use std::collections::VecDeque;
use std::io::{self, IoSlice};

use hashbrown::HashMap;
use strum::IntoEnumIterator;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use super::request::{self, MarshalledRequest, Operation, SessionOperation, StateResponser};
use super::types::WatchMode;
use super::xid::Xid;
use super::SessionId;
use crate::error::Error;
use crate::proto::{AuthPacket, OpCode, RemoveWatchesRequest};

pub type AuthResponser = oneshot::Sender<Result<(), Error>>;

#[derive(Default)]
pub struct Depot {
    xid: Xid,

    writing_slices: Vec<IoSlice<'static>>,
    writing_operations: VecDeque<Operation>,
    written_operations: VecDeque<SessionOperation>,
    pending_auth: Option<(AuthPacket, AuthResponser)>,

    watching_paths: HashMap<(&'static str, WatchMode), usize>,
    unwatching_paths: HashMap<(&'static str, WatchMode), SessionOperation>,
}

impl Depot {
    pub fn for_serving() -> Depot {
        let writing_capacity = 128usize;
        Depot {
            xid: Default::default(),
            writing_slices: Vec::with_capacity(writing_capacity),
            writing_operations: VecDeque::with_capacity(writing_capacity),
            written_operations: VecDeque::with_capacity(128),
            pending_auth: None,
            watching_paths: HashMap::with_capacity(32),
            unwatching_paths: HashMap::with_capacity(32),
        }
    }

    pub fn for_connecting() -> Depot {
        Depot {
            xid: Default::default(),
            writing_slices: Vec::with_capacity(10),
            writing_operations: VecDeque::with_capacity(10),
            written_operations: VecDeque::with_capacity(10),
            pending_auth: None,
            watching_paths: HashMap::new(),
            unwatching_paths: HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.writing_slices.clear();
        self.watching_paths.clear();
        self.writing_operations.clear();
        self.written_operations.clear();
    }

    pub fn error(&mut self, err: Error) {
        self.written_operations.drain(..).for_each(|operation| {
            operation.responser.send(Err(err.clone()));
        });
        self.writing_operations.drain(..).for_each(|operation| {
            if let Operation::Session(operation) = operation {
                operation.responser.send(Err(err.clone()));
            }
        });
        self.unwatching_paths.drain().for_each(|(_, operation)| {
            operation.responser.send(Err(err.clone()));
        });
        self.writing_slices.clear();
        self.watching_paths.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.writing_operations.is_empty() && self.written_operations.is_empty()
    }

    pub fn pop_pending_auth(&mut self) -> Option<(AuthPacket, AuthResponser)> {
        self.pending_auth.take()
    }

    pub fn has_pending_auth(&self) -> bool {
        self.pending_auth.is_some()
    }

    pub fn pop_reqeust(&mut self, xid: i32) -> Result<SessionOperation, Error> {
        match self.written_operations.pop_front() {
            None => Err(Error::UnexpectedError(format!("recv response with xid {} but no pending request", xid))),
            Some(operation) => {
                let request_xid = operation.request.get_xid();
                if xid == request_xid {
                    return Ok(operation);
                }
                self.written_operations.push_front(operation);
                Err(Error::UnexpectedError(format!("expect response xid {} but got {}", xid, request_xid)))
            },
        }
    }

    pub fn pop_ping(&mut self) -> Result<(), Error> {
        if let Some(operation) = self.written_operations.pop_front() {
            let op_code = operation.request.get_code();
            if op_code != OpCode::Ping {
                self.written_operations.push_front(operation);
                return Err(Error::UnexpectedError(format!("expect pending ping request, got {}", op_code)));
            }
            return Ok(());
        }
        Err(Error::UnexpectedError("expect pending ping request, got none".to_string()))
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
        if let Some((auth, responser)) = self.pending_auth.take() {
            self.push_auth(auth, responser);
        }
    }

    fn cancel_unwatch(&mut self, path: &'static str, mode: WatchMode) {
        if let Some(SessionOperation { responser, .. }) = self.unwatching_paths.remove(&(path, mode)) {
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
        if let (op_code, Some((path, mode))) = operation.request.get_operation_info() {
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
                let count = self.watching_paths.get(&(path, mode)).copied().unwrap_or(0) + 1;
                self.watching_paths.insert((path, mode), count);
            }
        }
        operation.request.set_xid(self.xid.next());
        self.push_operation(Operation::Session(operation));
    }

    pub fn push_remove_watch(&mut self, path: &str, mode: WatchMode, responser: StateResponser) {
        let record = RemoveWatchesRequest { path, mode: mode.into() };
        let operation =
            SessionOperation { request: MarshalledRequest::new_request(OpCode::RemoveWatches, &record), responser };
        self.push_session(operation);
    }

    pub fn has_watching_requests(&self, path: &str) -> bool {
        WatchMode::iter()
            .filter(|mode| *mode != WatchMode::Any)
            .any(|mode| self.watching_paths.contains_key(&(path, mode)))
    }

    pub fn push_auth(&mut self, auth: AuthPacket, responser: AuthResponser) {
        let operation = request::build_auth_operation(OpCode::Auth, &auth);
        self.pending_auth = Some((auth, responser));
        self.push_operation(Operation::Auth(operation));
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
            let written = self.writing_operations.drain(..written_slices).filter_map(|operation| {
                if let Operation::Session(operation) = operation {
                    return Some(operation);
                }
                None
            });
            self.written_operations.extend(written);
        }
        if written_bytes != 0 {
            let (_, rest) = self.writing_slices[0].split_at(written_bytes);
            let rest = unsafe { std::mem::transmute::<&[u8], &'_ [u8]>(rest) };
            self.writing_slices[0] = IoSlice::new(rest);
        }
        Ok(())
    }
}
