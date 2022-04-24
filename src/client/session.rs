use std::collections::VecDeque;
use std::io;
use std::io::IoSlice;
use std::time::Duration;

use hashbrown::HashMap;
use ignore_result::Ignore;
use strum::IntoEnumIterator;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Instant};

use self::watch::WatchManager;
use super::operation::{
    self,
    MarshalledRequest,
    Operation,
    SessionOperation,
    StateResponser,
    WatchReceiver,
    WatcherId,
};
use super::types::{SessionId, SessionState, WatchMode};
use crate::error::Error;
use crate::proto::{
    AuthPacket,
    ConnectRequest,
    ConnectResponse,
    ErrorCode,
    OpCode,
    PredefinedXid,
    RemoveWatchesRequest,
    ReplyHeader,
    RequestHeader,
    WatcherEvent,
};
use crate::record;

mod watch;

pub type AuthResponser = oneshot::Sender<Result<(), Error>>;

#[derive(Default)]
pub struct OperationState {
    writing_slices: Vec<IoSlice<'static>>,
    writing_operations: VecDeque<Operation>,
    written_operations: VecDeque<SessionOperation>,
    pending_auth: Option<(AuthPacket, AuthResponser)>,

    watching_paths: HashMap<(&'static str, WatchMode), usize>,
    unwatching_paths: HashMap<(&'static str, WatchMode), SessionOperation>,
}

impl OperationState {
    fn for_serving() -> OperationState {
        let writing_capacity = 128usize;
        OperationState {
            writing_slices: Vec::with_capacity(writing_capacity),
            writing_operations: VecDeque::with_capacity(writing_capacity),
            written_operations: VecDeque::with_capacity(128),
            pending_auth: None,
            watching_paths: HashMap::with_capacity(32),
            unwatching_paths: HashMap::with_capacity(32),
        }
    }

    pub fn for_connecting() -> OperationState {
        OperationState {
            writing_slices: Vec::with_capacity(10),
            writing_operations: VecDeque::with_capacity(10),
            written_operations: VecDeque::with_capacity(10),
            pending_auth: None,
            watching_paths: HashMap::new(),
            unwatching_paths: HashMap::new(),
        }
    }

    fn clear(&mut self) {
        self.writing_slices.clear();
        self.watching_paths.clear();
        self.writing_operations.clear();
        self.written_operations.clear();
    }

    fn error(&mut self, err: Error) {
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

    fn is_empty(&self) -> bool {
        self.writing_operations.is_empty() && self.written_operations.is_empty()
    }

    fn pop_ping(&mut self) -> Result<(), Error> {
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

    fn push_operation(&mut self, operation: Operation) {
        let buf = unsafe { std::mem::transmute::<&[u8], &'_ [u8]>(operation.get_data()) };
        self.writing_operations.push_back(operation);
        self.writing_slices.push(IoSlice::new(buf));
    }

    fn start(&mut self) {
        if let Some((auth, responser)) = self.pending_auth.take() {
            self.push_auth(auth, responser);
        }
    }

    fn cancel_unwatch(&mut self, path: &'static str, mode: WatchMode) {
        if let Some(SessionOperation { responser, .. }) = self.unwatching_paths.remove(&(path, mode)) {
            responser.send_empty();
        }
    }

    fn fail_watch(&mut self, path: &str, mode: WatchMode) {
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

    fn succeed_watch(&mut self, path: &str, mode: WatchMode) {
        let path = unsafe { std::mem::transmute::<&str, &'_ str>(path) };
        let count = self.watching_paths.get_mut(&(path, mode)).unwrap();
        *count -= 1;
        if *count == 0 {
            self.watching_paths.remove(&(path, mode));
        }
        self.cancel_unwatch(path, mode);
    }

    fn push_session(&mut self, operation: SessionOperation) {
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
        self.push_operation(Operation::Session(operation));
    }

    fn push_remove_watch(&mut self, path: &str, mode: WatchMode, responser: StateResponser) {
        let record = RemoveWatchesRequest { path, mode: mode.into() };
        let operation =
            SessionOperation { request: MarshalledRequest::new_request(OpCode::RemoveWatches, &record), responser };
        self.push_session(operation);
    }

    fn has_watching_requests(&self, path: &str) -> bool {
        WatchMode::iter()
            .filter(|mode| *mode != WatchMode::Any)
            .any(|mode| self.watching_paths.contains_key(&(path, mode)))
    }

    fn push_auth(&mut self, auth: AuthPacket, responser: AuthResponser) {
        let operation = operation::build_auth_operation(OpCode::Auth, &auth);
        self.pending_auth = Some((auth, responser));
        self.push_operation(Operation::Auth(operation));
    }

    fn write_operations(&mut self, sock: &TcpStream, state: &Session) -> Result<(), Error> {
        let result = sock.try_write_vectored(self.writing_slices.as_slice());
        let mut written_bytes = match result {
            Err(err) => {
                if err.kind() == io::ErrorKind::WouldBlock {
                    return Ok(());
                }
                log::debug!("ZooKeeper session {} write failed {}", state.session_id, err);
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

pub struct Session {
    timeout: Duration,
    readonly: bool,

    prev_xid: i32,

    last_zxid: i64,
    last_recv: Instant,
    last_send: Instant,
    last_ping: Option<Instant>,
    tick_timeout: Duration,
    ping_timeout: Duration,
    recv_timeout: Duration,

    pub session_id: SessionId,
    session_state: SessionState,
    pub session_timeout: Duration,
    pub session_password: Vec<u8>,
    session_readonly: bool,

    pub authes: Vec<AuthPacket>,
    state_sender: tokio::sync::watch::Sender<SessionState>,

    watch_manager: WatchManager,
    unwatch_receiver: Option<mpsc::UnboundedReceiver<(WatcherId, StateResponser)>>,
}

impl Session {
    pub fn new(
        timeout: Duration,
        authes: Vec<AuthPacket>,
        readonly: bool,
    ) -> (Session, tokio::sync::watch::Receiver<SessionState>) {
        let (state_sender, state_receiver) = tokio::sync::watch::channel(SessionState::Disconnected);
        let now = Instant::now();
        let (watch_manager, unwatch_receiver) = WatchManager::new();
        let mut state = Session {
            timeout,
            readonly,

            prev_xid: 0,

            last_zxid: 0,
            last_recv: now,
            last_send: now,
            last_ping: None,
            tick_timeout: Duration::ZERO,
            ping_timeout: Duration::ZERO,
            recv_timeout: Duration::ZERO,

            session_id: SessionId(0),
            session_timeout: timeout,
            session_state: SessionState::Disconnected,
            session_password: Vec::with_capacity(16),
            session_readonly: false,

            authes,
            state_sender,
            watch_manager,
            unwatch_receiver: Some(unwatch_receiver),
        };
        state.reset_session_timeout(timeout);
        (state, state_receiver)
    }

    async fn quit(&mut self, mut requester: mpsc::Receiver<SessionOperation>, err: &Error) {
        requester.close();
        while let Some(operation) = requester.recv().await {
            operation.responser.send(Err(err.clone()));
        }
    }

    pub async fn serve(
        &mut self,
        servers: Vec<(String, u16)>,
        sock: TcpStream,
        mut buf: Vec<u8>,
        mut connecting_state: OperationState,
        mut requester: mpsc::Receiver<SessionOperation>,
        mut auth_requester: mpsc::Receiver<(AuthPacket, AuthResponser)>,
    ) {
        let mut state = OperationState::for_serving();
        let mut unwatch_requester = self.unwatch_receiver.take().unwrap();
        self.serve_once(sock, &mut buf, &mut state, &mut requester, &mut auth_requester, &mut unwatch_requester).await;
        while !self.session_state.is_terminated() {
            let mut hosts = servers.iter().map(|(host, port)| (host.as_str(), *port));
            let sock = match self.start(&mut hosts, &mut buf, &mut connecting_state).await {
                Err(err) => {
                    log::warn!("fail to connect to cluster {:?} due to {}", servers, err);
                    self.resolve_start_error(&err);
                    break;
                },
                Ok(sock) => sock,
            };
            self.serve_once(sock, &mut buf, &mut state, &mut requester, &mut auth_requester, &mut unwatch_requester)
                .await;
        }
        let err = self.state_error();
        self.quit(requester, &err).await;
        if let Some((_, responser)) = state.pending_auth {
            responser.send(Err(err)).ignore();
        }
    }

    fn state_error(&self) -> Error {
        self.session_state.to_error()
    }

    fn change_state(&mut self, state: SessionState) {
        if state == self.session_state || self.session_state.is_terminated() {
            return;
        }
        self.session_state = state;
        self.watch_manager.dispatch_session_state(state);
        self.state_sender.send(state).ignore();
    }

    fn resolve_start_error(&mut self, err: &Error) {
        let state = match err {
            Error::SessionExpired | Error::SessionMoved => SessionState::Expired,
            Error::AuthFailed => SessionState::AuthFailed,
            _ => SessionState::Closed,
        };
        self.change_state(state);
    }

    fn resolve_serve_error(&mut self, err: &Error) {
        let state = match err {
            Error::SessionExpired | Error::SessionMoved => SessionState::Expired,
            Error::AuthFailed => SessionState::AuthFailed,
            Error::ClientClosed => SessionState::Closed,
            _ => SessionState::Disconnected,
        };
        self.change_state(state);
    }

    async fn serve_once(
        &mut self,
        sock: TcpStream,
        buf: &mut Vec<u8>,
        state: &mut OperationState,
        requester: &mut mpsc::Receiver<SessionOperation>,
        auth_requester: &mut mpsc::Receiver<(AuthPacket, AuthResponser)>,
        unwatch_requester: &mut mpsc::UnboundedReceiver<(WatcherId, StateResponser)>,
    ) {
        if let Err(err) = self.serve_session(&sock, buf, state, requester, auth_requester, unwatch_requester).await {
            self.resolve_serve_error(&err);
            log::debug!("ZooKeeper session {} state {} error {}", self.session_id, self.session_state, err);
            state.error(err);
        } else {
            self.change_state(SessionState::Disconnected);
            self.change_state(SessionState::Closed);
        }
    }

    fn handle_notification(&mut self, mut body: &[u8], state: &mut OperationState) -> Result<(), Error> {
        let event = record::unmarshal_entity::<WatcherEvent>(&"watch notification", &mut body)?;
        self.watch_manager.dispatch_server_event(event, state);
        Ok(())
    }

    fn handle_session_failure(&mut self, operation: SessionOperation, err: Error, state: &mut OperationState) {
        let SessionOperation { responser, request, .. } = operation;
        let (op_code, watch_info) = request.get_operation_info();
        if let Some((path, mode)) = watch_info {
            if op_code != OpCode::RemoveWatches {
                state.fail_watch(path, mode);
            }
        }
        responser.send(Err(err));
    }

    fn handle_session_watcher(
        &mut self,
        request: &MarshalledRequest,
        error_code: ErrorCode,
        state: &mut OperationState,
    ) -> (OpCode, WatchReceiver) {
        let (op_code, watch_info) = request.get_operation_info();
        if op_code == OpCode::RemoveWatches || watch_info.is_none() {
            return (op_code, WatchReceiver::None);
        }
        let (path, mode) = watch_info.unwrap();
        let watcher = self.watch_manager.create_watcher(path, mode, request.get_code(), error_code);
        if watcher.is_none() {
            state.fail_watch(path, mode);
        } else {
            state.succeed_watch(path, mode);
        }
        (op_code, watcher)
    }

    fn handle_session_reply(&mut self, operation: SessionOperation, rc: i32, body: &[u8], state: &mut OperationState) {
        let error_code = match ErrorCode::try_from(rc) {
            Ok(error_code) => error_code,
            Err(err) => {
                self.handle_session_failure(operation, Error::from(err), state);
                return;
            },
        };
        let SessionOperation { responser, request, .. } = operation;
        let (op_code, watcher) = self.handle_session_watcher(&request, error_code, state);
        if error_code == ErrorCode::Ok || (error_code == ErrorCode::NoNode && op_code == OpCode::Exists) {
            let mut buf = request.0;
            buf.clear();
            buf.extend_from_slice(body);
            responser.send(Ok((buf, watcher)));
        } else {
            assert!(watcher.is_none());
            responser.send(Err(Error::from(error_code)));
        }
    }

    fn handle_reply(&mut self, header: ReplyHeader, body: &[u8], state: &mut OperationState) -> Result<(), Error> {
        if header.err == ErrorCode::SessionExpired.into() {
            return Err(Error::SessionExpired);
        } else if header.err == ErrorCode::AuthFailed.into() {
            return Err(Error::AuthFailed);
        }
        if header.xid == PredefinedXid::Auth.into() {
            if let Some((auth, responser)) = state.pending_auth.take() {
                self.authes.push(auth);
                responser.send(Ok(())).ignore();
            }
            return Ok(());
        } else if header.xid == PredefinedXid::Notification.into() {
            self.handle_notification(body, state)?;
            return Ok(());
        } else if header.xid == PredefinedXid::Ping.into() {
            state.pop_ping()?;
            if let Some(last_ping) = self.last_ping.take() {
                let elapsed = Instant::now() - last_ping;
                log::debug!("ZooKeeper session {} got ping response after {}ms", self.session_id, elapsed.as_millis());
            }
            return Ok(());
        }
        let operation = match state.written_operations.pop_front() {
            None => {
                return Err(Error::UnexpectedError(format!(
                    "recv response with xid {} but no pending request",
                    header.xid
                )))
            },
            Some(operation) => operation,
        };
        let xid = operation.request.get_xid();
        if xid != header.xid {
            state.written_operations.push_front(operation);
            return Err(Error::UnexpectedError(format!("expect response xid {} but got {}", xid, header.xid)));
        }
        self.handle_session_reply(operation, header.err, body, state);
        Ok(())
    }

    fn reset_session_timeout(&mut self, timeout: Duration) {
        let tick = Duration::from_millis(1).max(timeout / 6);
        self.tick_timeout = tick;
        self.ping_timeout = 2 * tick;
        self.recv_timeout = 4 * tick;
        self.session_timeout = 6 * tick;
    }

    fn complete_connect(&mut self) {
        let state = if self.session_readonly { SessionState::ConnectedReadOnly } else { SessionState::SyncConnected };
        self.change_state(state);
    }

    fn handle_connect_response(&mut self, mut body: &[u8]) -> Result<(), Error> {
        let response = record::unmarshal::<ConnectResponse>(&mut body)?;
        if response.session_id == 0 {
            return Err(Error::SessionExpired);
        } else if !self.readonly && response.readonly {
            return Err(Error::ConnectionLoss);
        }
        self.session_id = SessionId(response.session_id);
        self.reset_session_timeout(Duration::from_millis(response.session_timeout as u64));
        self.session_password.clear();
        self.session_password.extend_from_slice(response.password);
        self.session_readonly = response.readonly;
        self.complete_connect();
        Ok(())
    }

    fn read_socket(&mut self, sock: &TcpStream, buf: &mut Vec<u8>) -> Result<(), Error> {
        match sock.try_read_buf(buf) {
            Ok(0) => {
                log::debug!("ZooKeeper session {} encounters server closed", self.session_id);
                return Err(Error::ConnectionLoss);
            },
            Err(err) => {
                if err.kind() != io::ErrorKind::WouldBlock {
                    log::debug!("ZooKeeper session {} encounters read err {}", self.session_id, err);
                    return Err(Error::ConnectionLoss);
                }
            },
            _ => {},
        }
        Ok(())
    }

    fn handle_recv_buf(&mut self, recved: &mut Vec<u8>, state: &mut OperationState) -> Result<(), Error> {
        let mut reading = recved.as_slice();
        if self.session_state == SessionState::Disconnected {
            if let Some(body) = record::try_deserialize::<&[u8]>(&mut reading)? {
                self.handle_connect_response(body)?;
            } else {
                return Ok(());
            }
        }
        while let Some(mut body) = record::try_deserialize::<&[u8]>(&mut reading)? {
            let header: ReplyHeader = record::unmarshal(&mut body)?;
            self.last_zxid = self.last_zxid.max(header.zxid);
            self.handle_reply(header, body, state)?;
        }
        let consumed_bytes = recved.len() - reading.len();
        recved.drain(..consumed_bytes);
        self.last_recv = Instant::now();
        Ok(())
    }

    fn next_xid(&mut self) -> i32 {
        if self.prev_xid == i32::MAX {
            self.prev_xid = 1;
        } else {
            self.prev_xid += 1;
        };
        self.prev_xid
    }

    async fn serve_connecting(
        &mut self,
        sock: &TcpStream,
        buf: &mut Vec<u8>,
        state: &mut OperationState,
    ) -> Result<(), Error> {
        let mut tick = time::interval(self.tick_timeout);
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        while !(self.session_state.is_connected() && state.is_empty()) {
            select! {
                _ = sock.readable() => {
                    self.read_socket(sock, buf)?;
                    self.handle_recv_buf(buf, state)?;
                },
                _ = sock.writable(), if !state.writing_slices.is_empty() => {
                    state.write_operations(sock, self)?;
                    self.last_send = Instant::now();
                },
                now = tick.tick() => {
                    if now >= self.last_recv + self.recv_timeout {
                        return Err(Error::SessionExpired);
                    }
                },
            }
        }
        Ok(())
    }

    async fn serve_session(
        &mut self,
        sock: &TcpStream,
        buf: &mut Vec<u8>,
        state: &mut OperationState,
        requester: &mut mpsc::Receiver<SessionOperation>,
        auth_requester: &mut mpsc::Receiver<(AuthPacket, AuthResponser)>,
        unwatch_requester: &mut mpsc::UnboundedReceiver<(WatcherId, StateResponser)>,
    ) -> Result<(), Error> {
        let mut tick = time::interval(self.tick_timeout);
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut channel_closed = false;
        let mut auth_closed = false;
        state.start();
        while !(channel_closed && state.is_empty()) {
            select! {
                _ = sock.readable() => {
                    self.read_socket(sock, buf)?;
                    self.handle_recv_buf(buf, state)?;
                },
                _ = sock.writable(), if !state.writing_slices.is_empty() => {
                    state.write_operations(sock, self)?;
                    self.last_send = Instant::now();
                },
                r = requester.recv(), if !channel_closed => {
                    let mut operation = if let Some(operation) = r {
                        operation
                    } else {
                        channel_closed = true;
                        continue;
                    };
                    operation.request.set_xid(self.next_xid());
                    state.push_session(operation);
                    state.write_operations(sock, self)?;
                    self.last_send = Instant::now();
                },
                r = auth_requester.recv(), if !auth_closed && state.pending_auth.is_none() => {
                    if let Some((auth, responser)) = r {
                        state.push_auth(auth, responser);
                    } else {
                        auth_closed = true;
                    };
                },
                Some((watcher_id, responser)) = unwatch_requester.recv() => self.watch_manager.drop_watcher(watcher_id, responser, state),
                now = tick.tick() => {
                    if now >= self.last_recv + self.recv_timeout {
                        return Err(Error::SessionExpired);
                    }
                    if self.last_ping.is_none() && now >= self.last_send + self.ping_timeout {
                        self.send_ping(state, now);
                        state.write_operations(sock, self)?;
                    }
                },
            }
        }
        Err(Error::ClientClosed)
    }

    async fn new_socket(
        &mut self,
        hosts: &mut impl Iterator<Item = (&str, u16)>,
        deadline: Instant,
    ) -> Result<TcpStream, Error> {
        let sleep = time::sleep_until(deadline);
        loop {
            let addr = match hosts.next() {
                None => return Err(Error::NoHosts),
                Some(addr) => addr,
            };
            select! {
                _ = sleep => return Err(Error::Timeout),
                r = TcpStream::connect(addr) => {
                    return match r {
                        Err(_) => Err(Error::ConnectionLoss),
                        Ok(sock) => Ok(sock),
                    };
                },
            }
        }
    }

    fn send_ping(&mut self, state: &mut OperationState, now: Instant) {
        let header = RequestHeader::with_code(OpCode::Ping);
        let operation = operation::build_session_operation(&header);
        state.push_operation(Operation::Session(operation));
        self.last_send = now;
        self.last_ping = Some(self.last_send);
    }

    fn send_connect(&self, state: &mut OperationState) {
        let request = ConnectRequest {
            protocol_version: 0,
            last_zxid_seen: 0,
            timeout: self.timeout.as_millis() as i32,
            session_id: self.session_id.0,
            password: self.session_password.as_slice(),
            readonly: self.readonly,
        };
        let operation = operation::build_connect_operation(&request);
        state.push_operation(Operation::Connect(operation));
    }

    fn send_authes(&self, state: &mut OperationState) {
        self.authes.iter().for_each(|auth| {
            let operation = operation::build_auth_operation(OpCode::Auth, auth);
            state.push_operation(Operation::Auth(operation));
        });
    }

    async fn start_once(
        &mut self,
        hosts: &mut impl Iterator<Item = (&str, u16)>,
        deadline: Instant,
        buf: &mut Vec<u8>,
        state: &mut OperationState,
    ) -> Result<TcpStream, Error> {
        let sock = self.new_socket(hosts, deadline).await?;
        state.clear();
        buf.clear();
        self.send_connect(state);
        // TODO: Sasl
        self.send_authes(state);
        self.watch_manager.resend_watches(self.last_zxid, state);
        self.last_send = Instant::now();
        self.last_recv = self.last_send;
        self.last_ping = None;
        self.serve_connecting(&sock, buf, state).await?;
        Ok(sock)
    }

    pub async fn start(
        &mut self,
        hosts: &mut impl Iterator<Item = (&str, u16)>,
        buf: &mut Vec<u8>,
        state: &mut OperationState,
    ) -> Result<TcpStream, Error> {
        let deadline = Instant::now() + self.session_timeout;
        let mut last_error = match self.start_once(hosts, deadline, buf, state).await {
            Err(err) => err,
            Ok(sock) => return Ok(sock),
        };
        while last_error != Error::NoHosts && last_error != Error::Timeout {
            match self.start_once(hosts, deadline, buf, state).await {
                Err(err) => {
                    last_error = err;
                    continue;
                },
                Ok(sock) => return Ok(sock),
            };
        }
        Err(last_error)
    }
}
