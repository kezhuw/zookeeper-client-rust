mod depot;
mod event;
mod request;
mod types;
mod watch;

use std::io;
use std::time::Duration;

use ignore_result::Ignore;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};

pub use self::depot::{AuthResponser, Depot};
use self::event::WatcherEvent;
pub use self::request::{build_state_operation, MarshalledRequest, Operation, SessionOperation, StateResponser};
pub use self::types::{EventType, SessionId, SessionState, WatchedEvent};
pub use self::watch::{OneshotReceiver, PersistentReceiver, WatchReceiver};
use self::watch::{WatchManager, WatcherId};
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
};
use crate::record;

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
        let mut session = Session {
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
        session.reset_session_timeout(timeout);
        (session, state_receiver)
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
        mut connecting_trans: Depot,
        mut requester: mpsc::Receiver<SessionOperation>,
        mut auth_requester: mpsc::Receiver<(AuthPacket, AuthResponser)>,
    ) {
        let mut depot = Depot::for_serving();
        let mut unwatch_requester = self.unwatch_receiver.take().unwrap();
        self.serve_once(sock, &mut buf, &mut depot, &mut requester, &mut auth_requester, &mut unwatch_requester).await;
        while !self.session_state.is_terminated() {
            let mut hosts = servers.iter().map(|(host, port)| (host.as_str(), *port));
            let sock = match self.start(&mut hosts, &mut buf, &mut connecting_trans).await {
                Err(err) => {
                    log::warn!("fail to connect to cluster {:?} due to {}", servers, err);
                    self.resolve_start_error(&err);
                    break;
                },
                Ok(sock) => sock,
            };
            self.serve_once(sock, &mut buf, &mut depot, &mut requester, &mut auth_requester, &mut unwatch_requester)
                .await;
        }
        let err = self.state_error();
        self.quit(requester, &err).await;
        if let Some((_, responser)) = depot.pop_pending_auth() {
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
        depot: &mut Depot,
        requester: &mut mpsc::Receiver<SessionOperation>,
        auth_requester: &mut mpsc::Receiver<(AuthPacket, AuthResponser)>,
        unwatch_requester: &mut mpsc::UnboundedReceiver<(WatcherId, StateResponser)>,
    ) {
        if let Err(err) = self.serve_session(&sock, buf, depot, requester, auth_requester, unwatch_requester).await {
            self.resolve_serve_error(&err);
            log::debug!("ZooKeeper session {} state {} error {}", self.session_id, self.session_state, err);
            depot.error(err);
        } else {
            self.change_state(SessionState::Disconnected);
            self.change_state(SessionState::Closed);
        }
    }

    fn handle_notification(&mut self, mut body: &[u8], depot: &mut Depot) -> Result<(), Error> {
        let event = record::unmarshal_entity::<WatcherEvent>(&"watch notification", &mut body)?;
        self.watch_manager.dispatch_server_event(event, depot);
        Ok(())
    }

    fn handle_session_failure(&mut self, operation: SessionOperation, err: Error, depot: &mut Depot) {
        let SessionOperation { responser, request, .. } = operation;
        let (op_code, watch_info) = request.get_operation_info();
        if let Some((path, mode)) = watch_info {
            if op_code != OpCode::RemoveWatches {
                depot.fail_watch(path, mode);
            }
        }
        responser.send(Err(err));
    }

    fn handle_session_watcher(
        &mut self,
        request: &MarshalledRequest,
        error_code: ErrorCode,
        depot: &mut Depot,
    ) -> (OpCode, WatchReceiver) {
        let (op_code, watch_info) = request.get_operation_info();
        if op_code == OpCode::RemoveWatches || watch_info.is_none() {
            return (op_code, WatchReceiver::None);
        }
        let (path, mode) = watch_info.unwrap();
        let watcher = self.watch_manager.create_watcher(path, mode, request.get_code(), error_code);
        if watcher.is_none() {
            depot.fail_watch(path, mode);
        } else {
            depot.succeed_watch(path, mode);
        }
        (op_code, watcher)
    }

    fn handle_session_reply(&mut self, operation: SessionOperation, rc: i32, body: &[u8], depot: &mut Depot) {
        let error_code = match ErrorCode::try_from(rc) {
            Ok(error_code) => error_code,
            Err(err) => {
                self.handle_session_failure(operation, Error::from(err), depot);
                return;
            },
        };
        let SessionOperation { responser, request, .. } = operation;
        let (op_code, watcher) = self.handle_session_watcher(&request, error_code, depot);
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

    fn handle_reply(&mut self, header: ReplyHeader, body: &[u8], depot: &mut Depot) -> Result<(), Error> {
        if header.err == ErrorCode::SessionExpired.into() {
            return Err(Error::SessionExpired);
        } else if header.err == ErrorCode::AuthFailed.into() {
            return Err(Error::AuthFailed);
        }
        if header.xid == PredefinedXid::Auth.into() {
            if let Some((auth, responser)) = depot.pop_pending_auth() {
                self.authes.push(auth);
                responser.send(Ok(())).ignore();
            }
            return Ok(());
        } else if header.xid == PredefinedXid::Notification.into() {
            self.handle_notification(body, depot)?;
            return Ok(());
        } else if header.xid == PredefinedXid::Ping.into() {
            depot.pop_ping()?;
            if let Some(last_ping) = self.last_ping.take() {
                let elapsed = Instant::now() - last_ping;
                log::debug!("ZooKeeper session {} got ping response after {}ms", self.session_id, elapsed.as_millis());
            }
            return Ok(());
        }
        let operation = depot.pop_reqeust(header.xid)?;
        self.handle_session_reply(operation, header.err, body, depot);
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

    fn handle_recv_buf(&mut self, recved: &mut Vec<u8>, depot: &mut Depot) -> Result<(), Error> {
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
            self.handle_reply(header, body, depot)?;
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

    async fn serve_connecting(&mut self, sock: &TcpStream, buf: &mut Vec<u8>, depot: &mut Depot) -> Result<(), Error> {
        let mut tick = time::interval(self.tick_timeout);
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        while !(self.session_state.is_connected() && depot.is_empty()) {
            select! {
                _ = sock.readable() => {
                    self.read_socket(sock, buf)?;
                    self.handle_recv_buf(buf, depot)?;
                },
                _ = sock.writable(), if depot.has_pending_writes() => {
                    depot.write_operations(sock, self.session_id)?;
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
        depot: &mut Depot,
        requester: &mut mpsc::Receiver<SessionOperation>,
        auth_requester: &mut mpsc::Receiver<(AuthPacket, AuthResponser)>,
        unwatch_requester: &mut mpsc::UnboundedReceiver<(WatcherId, StateResponser)>,
    ) -> Result<(), Error> {
        let mut tick = time::interval(self.tick_timeout);
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut channel_closed = false;
        let mut auth_closed = false;
        depot.start();
        while !(channel_closed && depot.is_empty()) {
            select! {
                _ = sock.readable() => {
                    self.read_socket(sock, buf)?;
                    self.handle_recv_buf(buf, depot)?;
                },
                _ = sock.writable(), if depot.has_pending_writes() => {
                    depot.write_operations(sock, self.session_id)?;
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
                    depot.push_session(operation);
                    depot.write_operations(sock, self.session_id)?;
                    self.last_send = Instant::now();
                },
                r = auth_requester.recv(), if !auth_closed && !depot.has_pending_auth() => match r {
                    Some((auth, responser)) => depot.push_auth(auth, responser),
                    None => auth_closed = true,
                },
                r = unwatch_requester.recv() => if let Some((watcher_id, responser)) = r {
                    self.watch_manager.drop_watcher(watcher_id, responser, depot);
                },
                now = tick.tick() => {
                    if now >= self.last_recv + self.recv_timeout {
                        return Err(Error::SessionExpired);
                    }
                    if self.last_ping.is_none() && now >= self.last_send + self.ping_timeout {
                        self.send_ping(depot, now);
                        depot.write_operations(sock, self.session_id)?;
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

    fn send_ping(&mut self, depot: &mut Depot, now: Instant) {
        let header = RequestHeader::with_code(OpCode::Ping);
        let operation = request::build_session_operation(&header);
        depot.push_operation(Operation::Session(operation));
        self.last_send = now;
        self.last_ping = Some(self.last_send);
    }

    fn send_connect(&self, depot: &mut Depot) {
        let request = ConnectRequest {
            protocol_version: 0,
            last_zxid_seen: 0,
            timeout: self.timeout.as_millis() as i32,
            session_id: self.session_id.0,
            password: self.session_password.as_slice(),
            readonly: self.readonly,
        };
        let operation = request::build_connect_operation(&request);
        depot.push_operation(Operation::Connect(operation));
    }

    fn send_authes(&self, depot: &mut Depot) {
        self.authes.iter().for_each(|auth| {
            let operation = request::build_auth_operation(OpCode::Auth, auth);
            depot.push_operation(Operation::Auth(operation));
        });
    }

    async fn start_once(
        &mut self,
        hosts: &mut impl Iterator<Item = (&str, u16)>,
        deadline: Instant,
        buf: &mut Vec<u8>,
        depot: &mut Depot,
    ) -> Result<TcpStream, Error> {
        let sock = self.new_socket(hosts, deadline).await?;
        depot.clear();
        buf.clear();
        self.send_connect(depot);
        // TODO: Sasl
        self.send_authes(depot);
        self.watch_manager.resend_watches(self.last_zxid, depot);
        self.last_send = Instant::now();
        self.last_recv = self.last_send;
        self.last_ping = None;
        self.serve_connecting(&sock, buf, depot).await?;
        Ok(sock)
    }

    pub async fn start(
        &mut self,
        hosts: &mut impl Iterator<Item = (&str, u16)>,
        buf: &mut Vec<u8>,
        depot: &mut Depot,
    ) -> Result<TcpStream, Error> {
        let deadline = Instant::now() + self.session_timeout;
        let mut last_error = match self.start_once(hosts, deadline, buf, depot).await {
            Err(err) => err,
            Ok(sock) => return Ok(sock),
        };
        while last_error != Error::NoHosts && last_error != Error::Timeout {
            match self.start_once(hosts, deadline, buf, depot).await {
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
