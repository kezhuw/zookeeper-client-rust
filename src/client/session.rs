use std::collections::{HashMap, VecDeque};
use std::io;
use std::io::IoSlice;
use std::time::Duration;

use compact_str::CompactStr;
use ignore_result::Ignore;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{self, Instant};

use super::operation::{
    self,
    OneshotReceiver,
    Operation,
    PersistentReceiver,
    SessionOperation,
    WatchMode,
    WatchReceiver,
};
use super::types::{EventType, SessionId, SessionState, WatchedEvent};
use crate::error::Error;
use crate::proto::{
    AuthPacket,
    ConnectRequest,
    ConnectResponse,
    ErrorCode,
    OpCode,
    PredefinedXid,
    ReplyHeader,
    RequestHeader,
    SetWatchesRequest,
    WatcherEvent,
};
use crate::record;

const SET_WATCHES_MAX_BYTES: usize = 128 * 1024;
pub type AuthResponser = oneshot::Sender<Result<(), Error>>;

type OneshotWatchSender = oneshot::Sender<WatchedEvent>;
type PersistentWatchSender = mpsc::UnboundedSender<WatchedEvent>;

#[derive(Default)]
pub struct OperationState {
    writing_slices: Vec<IoSlice<'static>>,
    writing_operations: VecDeque<Operation>,
    written_operations: VecDeque<SessionOperation>,
    pending_auth: Option<(AuthPacket, AuthResponser)>,
}

impl OperationState {
    fn for_serving() -> OperationState {
        let writing_capacity = 128usize;
        OperationState {
            writing_slices: Vec::with_capacity(writing_capacity),
            writing_operations: VecDeque::with_capacity(writing_capacity),
            written_operations: VecDeque::with_capacity(128),
            pending_auth: None,
        }
    }

    pub fn for_connecting() -> OperationState {
        OperationState {
            writing_slices: Vec::with_capacity(10),
            writing_operations: VecDeque::with_capacity(10),
            written_operations: VecDeque::with_capacity(10),
            pending_auth: None,
        }
    }

    fn clear(&mut self) {
        self.writing_slices.clear();
        self.writing_operations.clear();
        self.written_operations.clear();
    }

    fn error(&mut self, err: Error) {
        self.written_operations.drain(..).for_each(|operation| {
            operation.responser.send(Err(err.clone())).ignore();
        });
        self.writing_operations.drain(..).for_each(|operation| {
            if let Operation::Session(operation) = operation {
                operation.responser.send(Err(err.clone())).ignore();
            }
        });
        self.writing_slices.clear();
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
    state_sender: watch::Sender<SessionState>,

    data_watchers: HashMap<CompactStr, Vec<OneshotWatchSender>>,
    exist_watchers: HashMap<CompactStr, Vec<OneshotWatchSender>>,
    child_watchers: HashMap<CompactStr, Vec<OneshotWatchSender>>,

    persistent_watchers: HashMap<CompactStr, Vec<PersistentWatchSender>>,
    persistent_recursive_watchers: HashMap<CompactStr, Vec<PersistentWatchSender>>,
}

impl Session {
    pub fn new(timeout: Duration, authes: Vec<AuthPacket>, readonly: bool) -> (Session, watch::Receiver<SessionState>) {
        let (state_sender, state_receiver) = watch::channel(SessionState::Disconnected);
        let now = Instant::now();
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

            data_watchers: HashMap::new(),
            exist_watchers: HashMap::new(),
            child_watchers: HashMap::new(),

            persistent_watchers: HashMap::new(),
            persistent_recursive_watchers: HashMap::new(),
        };
        state.reset_session_timeout(timeout);
        (state, state_receiver)
    }

    async fn quit(&mut self, mut requester: mpsc::Receiver<SessionOperation>, err: &Error) {
        requester.close();
        while let Some(operation) = requester.recv().await {
            operation.responser.send(Err(err.clone())).ignore();
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
        self.serve_once(sock, &mut buf, &mut state, &mut requester, &mut auth_requester).await;
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
            self.serve_once(sock, &mut buf, &mut state, &mut requester, &mut auth_requester).await;
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
        self.dispatch_session_state(state);
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
    ) {
        if let Err(err) = self.serve_session(&sock, buf, state, requester, auth_requester).await {
            self.resolve_serve_error(&err);
            log::debug!("ZooKeeper session {} state {} error {}", self.session_id, self.session_state, err);
            state.error(err);
        } else {
            self.change_state(SessionState::Disconnected);
            self.change_state(SessionState::Closed);
        }
    }

    fn dispatch_oneshot_watcher(
        watches: &mut HashMap<CompactStr, Vec<OneshotWatchSender>>,
        path: &str,
        event: &WatchedEvent,
    ) {
        if let Some(senders) = watches.get_mut(path) {
            senders.drain(..).for_each(|sender| sender.send(event.clone()).ignore())
        }
    }

    fn dispatch_persistent_watcher(
        watches: &HashMap<CompactStr, Vec<PersistentWatchSender>>,
        path: &str,
        event: &WatchedEvent,
    ) {
        if let Some(senders) = watches.get(path) {
            senders.iter().for_each(|sender| sender.send(event.clone()).ignore());
        }
    }

    fn dispatch_recursive_watcher(
        watches: &HashMap<CompactStr, Vec<PersistentWatchSender>>,
        mut path: &str,
        event: &WatchedEvent,
    ) {
        Self::dispatch_persistent_watcher(watches, path, event);
        while path.len() > 1 {
            let i = path.rfind('/').unwrap_or(0).max(1);
            path = unsafe { path.get_unchecked(..i) };
            Self::dispatch_persistent_watcher(watches, path, event);
        }
    }

    fn add_oneshot_watcher(
        path: &str,
        watches: &mut HashMap<CompactStr, Vec<OneshotWatchSender>>,
    ) -> oneshot::Receiver<WatchedEvent> {
        let (sender, receiver) = oneshot::channel();
        if let Some(watchers) = watches.get_mut(path) {
            watchers.push(sender);
            return receiver;
        }
        let watchers = vec![sender];
        watches.insert(CompactStr::new(path), watchers);
        receiver
    }

    fn add_persistent_watcher(
        path: &str,
        watches: &mut HashMap<CompactStr, Vec<PersistentWatchSender>>,
    ) -> PersistentReceiver {
        let (sender, receiver) = mpsc::unbounded_channel();
        if let Some(watchers) = watches.get_mut(path) {
            watchers.push(sender);
            return receiver;
        }
        let watchers = vec![sender];
        watches.insert(CompactStr::new(path), watchers);
        receiver
    }

    fn add_data_watch(&mut self, path: &str) -> OneshotReceiver {
        Self::add_oneshot_watcher(path, &mut self.data_watchers)
    }

    fn add_exist_watch(&mut self, path: &str) -> OneshotReceiver {
        Self::add_oneshot_watcher(path, &mut self.exist_watchers)
    }

    fn add_child_watch(&mut self, path: &str) -> OneshotReceiver {
        Self::add_oneshot_watcher(path, &mut self.child_watchers)
    }

    fn create_watcher(&mut self, path: &str, watch_mode: WatchMode, op_code: OpCode, rc: ErrorCode) -> WatchReceiver {
        if rc != ErrorCode::Ok {
            if rc == ErrorCode::NoNode && op_code == OpCode::Exists {
                return WatchReceiver::Oneshot(self.add_exist_watch(path));
            }
            return WatchReceiver::None;
        }
        if op_code == OpCode::GetData || op_code == OpCode::Exists {
            return WatchReceiver::Oneshot(self.add_data_watch(path));
        } else if op_code == OpCode::GetChildren || op_code == OpCode::GetChildren2 {
            return WatchReceiver::Oneshot(self.add_child_watch(path));
        }
        if watch_mode == WatchMode::Persistent {
            return WatchReceiver::Persistent(Self::add_persistent_watcher(path, &mut self.persistent_watchers));
        }
        assert!(watch_mode == WatchMode::PersistentRecursive);
        WatchReceiver::Persistent(Self::add_persistent_watcher(path, &mut self.persistent_recursive_watchers))
    }

    fn dispatch_data_event(&mut self, event: WatchedEvent) {
        let path = event.path.as_str();
        Self::dispatch_oneshot_watcher(&mut self.data_watchers, path, &event);
        Self::dispatch_oneshot_watcher(&mut self.exist_watchers, path, &event);
        self.dispatch_persistent_event(&event);
    }

    fn dispatch_child_event(&mut self, event: WatchedEvent) {
        let path = event.path.as_str();
        Self::dispatch_oneshot_watcher(&mut self.child_watchers, path, &event);
        Self::dispatch_persistent_watcher(&self.persistent_watchers, path, &event);
    }

    fn dispatch_node_delete_event(&mut self, event: WatchedEvent) {
        let path = event.path.as_str();
        Self::dispatch_oneshot_watcher(&mut self.data_watchers, path, &event);
        Self::dispatch_oneshot_watcher(&mut self.exist_watchers, path, &event);
        Self::dispatch_oneshot_watcher(&mut self.child_watchers, path, &event);
        self.dispatch_persistent_event(&event);
    }

    fn dispatch_persistent_event(&mut self, event: &WatchedEvent) {
        let path = event.path.as_str();
        Self::dispatch_persistent_watcher(&self.persistent_watchers, path, event);
        Self::dispatch_recursive_watcher(&self.persistent_recursive_watchers, path, event);
    }

    fn dispatch_session_state(&mut self, state: SessionState) {
        let event = WatchedEvent { event_type: EventType::Session, session_state: state, path: String::default() };
        self.persistent_watchers
            .values()
            .flat_map(|senders| senders.iter())
            .for_each(|sender| sender.send(event.clone()).ignore());
        self.persistent_recursive_watchers
            .values()
            .flat_map(|senders| senders.iter())
            .for_each(|sender| sender.send(event.clone()).ignore());

        if event.session_state.is_terminated() {
            self.data_watchers
                .values_mut()
                .flat_map(|senders| senders.drain(..))
                .for_each(|sender| sender.send(event.clone()).ignore());
            self.exist_watchers
                .values_mut()
                .flat_map(|senders| senders.drain(..))
                .for_each(|sender| sender.send(event.clone()).ignore());
            self.child_watchers
                .values_mut()
                .flat_map(|senders| senders.drain(..))
                .for_each(|sender| sender.send(event.clone()).ignore());

            self.data_watchers.clear();
            self.exist_watchers.clear();
            self.child_watchers.clear();
            self.persistent_watchers.clear();
            self.persistent_recursive_watchers.clear();
        }
    }

    fn dispatch_server_event(&mut self, event: WatchedEvent) {
        use EventType::*;
        match event.event_type {
            NodeCreated | NodeDataChanged => self.dispatch_data_event(event),
            NodeChildrenChanged => self.dispatch_child_event(event),
            NodeDeleted => self.dispatch_node_delete_event(event),
            _ => unreachable!("unexpected server watch event {:?}", event),
        }
    }

    fn handle_notification(&mut self, mut body: &[u8]) -> Result<(), Error> {
        let watcher_event = record::unmarshal_entity::<WatcherEvent>(&"watch notification", &mut body)?;
        let session_state = SessionState::from_server(watcher_event.session_state)?;
        let event_type = EventType::from_server(watcher_event.event_type)?;
        let event = WatchedEvent { event_type, session_state, path: watcher_event.path };
        self.dispatch_server_event(event);
        Ok(())
    }

    fn handle_session_reply(&mut self, operation: SessionOperation, rc: i32, body: &[u8]) {
        let SessionOperation { responser, request, .. } = operation;
        let error_code = match ErrorCode::try_from(rc) {
            Ok(error_code) => error_code,
            Err(err) => {
                let err = Error::from(err);
                responser.send(Err(err)).ignore();
                return;
            },
        };
        let (op_code, watch_info) = request.get_operation_info();
        let watcher = if let Some((path, watch_mode)) = watch_info {
            self.create_watcher(path, watch_mode, request.get_code(), error_code)
        } else {
            WatchReceiver::None
        };
        if error_code == ErrorCode::Ok || (error_code == ErrorCode::NoNode && op_code == OpCode::Exists) {
            let mut buf = request.0;
            buf.clear();
            buf.extend_from_slice(body);
            responser.send(Ok((buf, watcher))).ignore();
        } else {
            assert!(watcher.is_none());
            responser.send(Err(Error::from(error_code))).ignore();
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
            self.handle_notification(body)?;
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
        self.handle_session_reply(operation, header.err, body);
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
                    state.push_operation(Operation::Session(operation));
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

    fn oneshot_watches(watchers: &HashMap<CompactStr, Vec<OneshotWatchSender>>) -> impl Iterator<Item = &str> {
        watchers.iter().filter_map(|(k, v)| if v.is_empty() { None } else { Some(k.as_str()) })
    }

    fn persistent_watches(watchers: &HashMap<CompactStr, Vec<PersistentWatchSender>>) -> impl Iterator<Item = &str> {
        watchers.iter().filter_map(|(k, v)| if v.is_empty() { None } else { Some(k.as_str()) })
    }

    fn send_and_clear_watches(&self, watches: &mut [Vec<&str>; 5], i: usize, state: &mut OperationState) {
        let (n, op_code) = if i <= 2 { (3, OpCode::SetWatches) } else { (5, OpCode::SetWatches2) };
        let request = SetWatchesRequest { relative_zxid: self.last_zxid, watches: &watches[..n] };
        let (operation, _) = operation::build_state_operation(op_code, &request);
        state.push_operation(Operation::Session(operation));
        watches[..=i].iter_mut().for_each(|v| v.clear());
    }

    fn resend_watches(&self, state: &mut OperationState) {
        use std::iter::repeat;
        let mut watches = [vec![], vec![], vec![], vec![], vec![]];
        let data_watchers = repeat(0usize).zip(Self::oneshot_watches(&self.data_watchers));
        let exist_watchers = repeat(1usize).zip(Self::oneshot_watches(&self.exist_watchers));
        let child_watchers = repeat(2usize).zip(Self::oneshot_watches(&self.child_watchers));
        let persistent_watchers = repeat(3usize).zip(Self::persistent_watches(&self.persistent_watchers));
        let persistent_recursive_watchers =
            repeat(4usize).zip(Self::persistent_watches(&self.persistent_recursive_watchers));

        let mut index = 0usize;
        let mut bytes = 0usize;
        data_watchers
            .chain(exist_watchers)
            .chain(child_watchers)
            .chain(persistent_watchers)
            .chain(persistent_recursive_watchers)
            .for_each(|(i, path)| {
                index = i;
                bytes += path.len();
                watches[i].push(path);
                if bytes > SET_WATCHES_MAX_BYTES {
                    self.send_and_clear_watches(&mut watches, i, state);
                    bytes = 0;
                }
            });
        if bytes != 0 {
            self.send_and_clear_watches(&mut watches, index, state);
        }
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
        self.resend_watches(state);
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
