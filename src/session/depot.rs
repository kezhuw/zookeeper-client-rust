use std::collections::VecDeque;
use std::io::{self, IoSlice};

use hashbrown::HashMap;
use strum::IntoEnumIterator;
use tokio::net::TcpStream;

use super::request::{MarshalledRequest, OpStat, Operation, SessionOperation, StateResponser};
use super::types::WatchMode;
use super::xid::Xid;
use super::SessionId;
use crate::error::Error;
use crate::proto::{OpCode, PredefinedXid, RemoveWatchesRequest};

#[derive(Default)]
pub struct Depot {
    xid: Xid,

    pending_authes: Vec<SessionOperation>,

    writing_slices: Vec<IoSlice<'static>>,
    writing_operations: VecDeque<Operation>,
    written_operations: HashMap<i32, SessionOperation>,

    watching_paths: HashMap<(&'static str, WatchMode), usize>,
    unwatching_paths: HashMap<(&'static str, WatchMode), SessionOperation>,
}

impl Depot {
    pub fn for_serving() -> Depot {
        let writing_capacity = 128usize;
        Depot {
            xid: Default::default(),
            pending_authes: Vec::with_capacity(5),
            writing_slices: Vec::with_capacity(writing_capacity),
            writing_operations: VecDeque::with_capacity(writing_capacity),
            written_operations: HashMap::with_capacity(128),
            watching_paths: HashMap::with_capacity(32),
            unwatching_paths: HashMap::with_capacity(32),
        }
    }

    pub fn for_connecting() -> Depot {
        Depot {
            xid: Default::default(),
            pending_authes: Default::default(),
            writing_slices: Vec::with_capacity(10),
            writing_operations: VecDeque::with_capacity(10),
            written_operations: HashMap::with_capacity(10),
            watching_paths: HashMap::new(),
            unwatching_paths: HashMap::new(),
        }
    }

    /// Clear all buffered operations from previous run.
    pub fn clear(&mut self) {
        self.pending_authes.clear();
        self.writing_slices.clear();
        self.watching_paths.clear();
        self.unwatching_paths.clear();
        self.writing_operations.clear();
        self.written_operations.clear();
    }

    /// Error out ongoing operations except authes.
    pub fn error(&mut self, err: &Error) {
        self.written_operations.drain().for_each(|(_, operation)| {
            if operation.request.get_code() == OpCode::Auth {
                self.pending_authes.push(operation);
                return;
            }
            operation.responser.send(Err(err.clone()));
        });
        self.writing_operations.drain(..).for_each(|operation| {
            if let Operation::Session(operation) = operation {
                if operation.request.get_code() == OpCode::Auth {
                    self.pending_authes.push(operation);
                    return;
                }
                operation.responser.send(Err(err.clone()));
            }
        });
        self.unwatching_paths.drain().for_each(|(_, operation)| {
            operation.responser.send(Err(err.clone()));
        });
        self.writing_slices.clear();
        self.watching_paths.clear();
    }

    /// Terminate all ongoing operations including authes.
    pub fn terminate(&mut self, err: Error) {
        self.error(&err);
        for SessionOperation { responser, .. } in self.pending_authes.drain(..) {
            responser.send(Err(err.clone()));
        }
    }

    /// Check whether there is any ongoing operations.
    pub fn is_empty(&self) -> bool {
        self.writing_operations.is_empty() && self.written_operations.is_empty()
    }

    pub fn pop_request(&mut self, xid: i32) -> Result<SessionOperation, Error> {
        match self.written_operations.remove(&xid) {
            None => Err(Error::UnexpectedError(format!("recv response with xid {} but no pending request", xid))),
            Some(operation) => Ok(operation),
        }
    }

    pub fn pop_ping(&mut self) -> Result<(), Error> {
        self.pop_request(PredefinedXid::Ping.into()).map(|_| ())
    }

    pub fn push_operation(&mut self, operation: Operation) {
        let buf = unsafe { std::mem::transmute::<&[u8], &'_ [u8]>(operation.get_data()) };
        self.writing_operations.push_back(operation);
        self.writing_slices.push(IoSlice::new(buf));
    }

    pub fn has_pending_writes(&self) -> bool {
        !self.writing_slices.is_empty()
    }

    pub fn start(&mut self) {
        let mut pending_authes = std::mem::take(&mut self.pending_authes);
        for operation in pending_authes.drain(..) {
            self.push_session(operation);
        }
        self.pending_authes = pending_authes;
    }

    fn cancel_unwatch(&mut self, path: &'static str, mode: WatchMode) {
        if let Some(SessionOperation { responser, .. }) = self.unwatching_paths.remove(&(path, mode)) {
            responser.send_empty();
        }
        if let Some(SessionOperation { responser, .. }) = self.unwatching_paths.remove(&(path, WatchMode::Any)) {
            responser.send_empty();
        }
    }

    pub fn fail_watch(&mut self, path: &str, mode: WatchMode) {
        let path = unsafe { std::mem::transmute::<&str, &'_ str>(path) };
        let count = self.watching_paths.get_mut(&(path, mode)).unwrap();
        *count -= 1;
        if *count == 0 {
            self.watching_paths.remove(&(path, mode));
            if let Some(operation) = self.unwatching_paths.remove(&(path, mode)) {
                self.push_operation(Operation::Session(operation));
            }
            if self.has_watching_requests(path) {
                return;
            }
            if let Some(operation) = self.unwatching_paths.remove(&(path, WatchMode::Any)) {
                self.push_operation(Operation::Session(operation));
            }
        }
    }

    pub fn succeed_watch(&mut self, path: &str, mode: WatchMode) {
        let path = unsafe { std::mem::transmute::<&str, &'_ str>(path) };
        let count = self.watching_paths.get_mut(&(path, mode)).unwrap();
        *count -= 1;
        if *count == 0 {
            self.watching_paths.remove(&(path, mode));
        }
        self.cancel_unwatch(path, mode);
    }

    pub fn push_session(&mut self, mut operation: SessionOperation) {
        let info = operation.request.get_operation_info();
        log::debug!("ZooKeeper operation request: {:?}", info);
        if let (op_code, OpStat::Watch { path, mode }) = info {
            let path = unsafe { std::mem::transmute::<&str, &'_ str>(path) };
            if op_code == OpCode::RemoveWatches {
                if self.watching_paths.contains_key(&(path, mode))
                    || (mode == WatchMode::Any && self.has_watching_requests(path))
                {
                    self.unwatching_paths.insert((path, mode), operation);
                    return;
                }
            } else {
                // Overwrite old paths as they could be invalidated after reply.
                // `HashMap::insert` does not update the key in case it is present, so we have to remove it first.
                let count = self.watching_paths.remove(&(path, mode)).unwrap_or(0) + 1;
                self.watching_paths.insert((path, mode), count);
            }
        }
        operation.request.set_xid(self.xid.next());
        self.push_operation(Operation::Session(operation));
    }

    pub fn push_remove_watch(&mut self, path: &str, mode: WatchMode, responser: StateResponser) {
        let record = RemoveWatchesRequest { path, mode: mode.into() };
        let operation = SessionOperation { request: MarshalledRequest::new(OpCode::RemoveWatches, &record), responser };
        self.push_session(operation);
    }

    pub fn has_watching_requests(&self, path: &str) -> bool {
        WatchMode::iter()
            .filter(|mode| *mode != WatchMode::Any)
            .any(|mode| self.watching_paths.contains_key(&(path, mode)))
    }

    pub fn write_operations(&mut self, sock: &TcpStream, session_id: SessionId) -> Result<(), Error> {
        let result = sock.try_write_vectored(self.writing_slices.as_slice());
        let mut written_bytes = match result {
            Err(err) => {
                if err.kind() == io::ErrorKind::WouldBlock {
                    return Ok(());
                }
                log::debug!("ZooKeeper session {} write failed {}", session_id, err);
                return Err(Error::ConnectionLoss);
            },
            Ok(written_bytes) => written_bytes,
        };
        let written_slices = self
            .writing_slices
            .iter()
            .position(|slice| {
                if written_bytes >= slice.len() {
                    written_bytes -= slice.len();
                    return false;
                }
                true
            })
            .unwrap_or(self.writing_slices.len());
        if written_slices != 0 {
            self.writing_slices.drain(..written_slices);
            self.writing_operations
                .drain(..written_slices)
                .filter_map(|operation| {
                    if let Operation::Session(operation) = operation {
                        return Some(operation);
                    }
                    None
                })
                .for_each(|operation| {
                    let xid = operation.request.get_xid();
                    self.written_operations.insert(xid, operation);
                });
        }
        if written_bytes != 0 {
            let (_, rest) = self.writing_slices[0].split_at(written_bytes);
            let rest = unsafe { std::mem::transmute::<&[u8], &'_ [u8]>(rest) };
            self.writing_slices[0] = IoSlice::new(rest);
        }
        Ok(())
    }
}
