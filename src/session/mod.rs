mod connection;
mod depot;
mod event;
mod request;
mod types;
mod watch;
mod xid;

use std::io;
use std::sync::Arc;
use std::time::Duration;

use ignore_result::Ignore;
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::{self, Instant, Sleep};
use tokio_rustls::TlsConnector;

use self::connection::Connection;
pub use self::depot::Depot;
use self::event::WatcherEvent;
pub use self::request::{
    ConnectOperation,
    MarshalledRequest,
    OpStat,
    Operation,
    SessionOperation,
    StateReceiver,
    StateResponser,
};
pub use self::types::{EventType, SessionId, SessionState, WatchedEvent};
pub use self::watch::{OneshotReceiver, PersistentReceiver, WatchReceiver};
use self::watch::{WatchManager, WatcherId};
use crate::error::Error;
use crate::proto::{AuthPacket, ConnectRequest, ConnectResponse, ErrorCode, OpCode, PredefinedXid, ReplyHeader};
use crate::record;
use crate::util::HostPort;

pub const PASSWORD_LEN: usize = 16;
pub const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(6);

trait RequestOperation {
    fn into_responser(self) -> StateResponser;
}

impl RequestOperation for SessionOperation {
    fn into_responser(self) -> StateResponser {
        self.responser
    }
}

impl RequestOperation for (WatcherId, StateResponser) {
    fn into_responser(self) -> StateResponser {
        self.1
    }
}

pub struct Session {
    readonly: bool,
    detached: bool,

    tls_connector: TlsConnector,

    configured_connection_timeout: Duration,

    last_zxid: i64,
    last_recv: Instant,
    last_send: Instant,
    last_ping: Option<Instant>,
    tick_timeout: Duration,
    ping_timeout: Duration,
    connection_timeout: Duration,
    session_expired_timeout: Duration,

    pub session_id: SessionId,
    session_state: SessionState,
    pub session_timeout: Duration,
    pub session_password: Vec<u8>,
    session_readonly: bool,

    pub authes: Vec<MarshalledRequest>,
    state_sender: tokio::sync::watch::Sender<SessionState>,

    watch_manager: WatchManager,
    unwatch_receiver: Option<mpsc::UnboundedReceiver<(WatcherId, StateResponser)>>,
}

impl Session {
    pub fn new(
        session: Option<(SessionId, Vec<u8>)>,
        authes: &[AuthPacket],
        readonly: bool,
        detached: bool,
        tls_config: ClientConfig,
        session_timeout: Duration,
        connection_timeout: Duration,
    ) -> (Session, tokio::sync::watch::Receiver<SessionState>) {
        let (session_id, session_password) =
            session.unwrap_or_else(|| (SessionId(0), Vec::with_capacity(PASSWORD_LEN)));
        let (state_sender, state_receiver) = tokio::sync::watch::channel(SessionState::Disconnected);
        let now = Instant::now();
        let (watch_manager, unwatch_receiver) = WatchManager::new();
        let mut session = Session {
            readonly,
            detached,
            tls_connector: TlsConnector::from(Arc::new(tls_config)),

            configured_connection_timeout: connection_timeout,

            last_zxid: 0,
            last_recv: now,
            last_send: now,
            last_ping: None,
            tick_timeout: Duration::ZERO,
            ping_timeout: Duration::ZERO,
            connection_timeout: Duration::ZERO,
            session_expired_timeout: Duration::ZERO,

            session_id,
            session_timeout,
            session_state: SessionState::Disconnected,
            session_password,
            session_readonly: false,

            authes: authes.iter().map(|auth| MarshalledRequest::new(OpCode::Auth, auth)).collect(),
            state_sender,
            watch_manager,
            unwatch_receiver: Some(unwatch_receiver),
        };
        let timeout = if session_timeout.is_zero() { DEFAULT_SESSION_TIMEOUT } else { session_timeout };
        session.reset_timeout(timeout);
        (session, state_receiver)
    }

    async fn close_requester<T: RequestOperation>(mut requester: mpsc::UnboundedReceiver<T>, err: &Error) {
        requester.close();
        while let Some(operation) = requester.recv().await {
            let responser = operation.into_responser();
            responser.send(Err(err.clone()));
        }
    }

    pub async fn serve(
        &mut self,
        servers: Vec<HostPort>,
        conn: Connection,
        mut buf: Vec<u8>,
        mut connecting_trans: Depot,
        mut requester: mpsc::UnboundedReceiver<SessionOperation>,
    ) {
        let mut depot = Depot::for_serving();
        let mut unwatch_requester = self.unwatch_receiver.take().unwrap();
        self.serve_once(conn, &mut buf, &mut depot, &mut requester, &mut unwatch_requester).await;
        while !self.session_state.is_terminated() {
            let mut hosts = servers.iter().cycle().map(|(host, port, tls)| (host.as_str(), *port, *tls));
            let conn = match self.start(&mut hosts, &mut buf, &mut connecting_trans).await {
                Err(err) => {
                    log::warn!("fail to connect to cluster {:?} due to {}", servers, err);
                    self.resolve_start_error(&err);
                    break;
                },
                Ok(conn) => conn,
            };
            self.serve_once(conn, &mut buf, &mut depot, &mut requester, &mut unwatch_requester).await;
        }
        let err = self.state_error();
        Self::close_requester(requester, &err).await;
        Self::close_requester(unwatch_requester, &err).await;
        depot.terminate(err);
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
            Error::SessionExpired | Error::SessionMoved | Error::Timeout => SessionState::Expired,
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
        mut conn: Connection,
        buf: &mut Vec<u8>,
        depot: &mut Depot,
        requester: &mut mpsc::UnboundedReceiver<SessionOperation>,
        unwatch_requester: &mut mpsc::UnboundedReceiver<(WatcherId, StateResponser)>,
    ) {
        if let Err(err) = self.serve_session(&mut conn, buf, depot, requester, unwatch_requester).await {
            self.resolve_serve_error(&err);
            log::info!("ZooKeeper session {} state {} error {}", self.session_id, self.session_state, err);
            depot.error(&err);
        } else {
            self.change_state(SessionState::Disconnected);
            self.change_state(SessionState::Closed);
        }
    }

    fn handle_notification(&mut self, zxid: i64, mut body: &[u8], depot: &mut Depot) -> Result<(), Error> {
        let event = record::unmarshal_entity::<WatcherEvent>(&"watch notification", &mut body)?;
        self.watch_manager.dispatch_server_event(event.with_zxid(zxid), depot);
        Ok(())
    }

    fn handle_session_failure(&mut self, operation: SessionOperation, err: Error, depot: &mut Depot) {
        let SessionOperation { responser, request, .. } = operation;
        let info = request.get_operation_info();
        log::debug!("ZooKeeper operation unknown failure: {:?}, {:?}", info, err);
        match info {
            (op_code, OpStat::Watch { path, mode }) if op_code != OpCode::RemoveWatches => depot.fail_watch(path, mode),
            _ => {},
        }
        responser.send(Err(err));
    }

    fn handle_session_watcher(
        &mut self,
        request: &MarshalledRequest,
        error_code: ErrorCode,
        depot: &mut Depot,
    ) -> (OpCode, WatchReceiver) {
        let info = request.get_operation_info();
        log::debug!("ZooKeeper operation get reply: {:?}, {:?}", info, error_code);
        let (op_code, path, mode) = match info {
            (op_code, OpStat::Watch { path, mode }) if op_code != OpCode::RemoveWatches => (op_code, path, mode),
            (op_code, _) => return (op_code, WatchReceiver::None),
        };
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
        if error_code == ErrorCode::Ok || (op_code == OpCode::Exists && error_code == ErrorCode::NoNode) {
            if op_code == OpCode::Auth {
                if responser.send_empty() {
                    self.authes.push(request);
                }
                return;
            }
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
        if header.xid == PredefinedXid::Notification.into() {
            self.handle_notification(header.zxid, body, depot)?;
            return Ok(());
        } else if header.xid == PredefinedXid::Ping.into() {
            depot.pop_ping()?;
            if let Some(last_ping) = self.last_ping.take() {
                let elapsed = Instant::now() - last_ping;
                log::debug!("ZooKeeper session {} got ping response after {}ms", self.session_id, elapsed.as_millis());
            }
            return Ok(());
        }
        let operation = depot.pop_request(header.xid)?;
        self.handle_session_reply(operation, header.err, body, depot);
        Ok(())
    }

    fn calc_tick_timeout(&self, session_timeout: Duration) -> Duration {
        let connection_timeout = self.configured_connection_timeout;
        let tick_timeout = if connection_timeout.is_zero() || connection_timeout > session_timeout * 3 / 5 {
            session_timeout / 20
        } else {
            connection_timeout / 8
        };
        tick_timeout.max(Duration::from_millis(1))
    }

    /// Resets connection and session related timeout values.
    ///
    /// Set ping timeout to `3/8` of connection timeout to fully cover one roundtrip. See also
    /// [PredefinedXid::Ping].
    ///
    /// Set connection timeout to `2/5` of session timeout for at least two tries after connection
    /// loss if there is no configured connection timeout.
    ///
    /// Set client side session expired timeout to `7/5` of negotiated session timeout so client
    /// can expire session on its behalf.
    ///
    /// Numerators are chose deliberately so that resulting timeout values are sensible to mental
    /// model and friendly to eyeballs(`tick` is likely to be a natural number of milliseconds) in
    /// best effort.
    ///
    /// See also [ZOOKEEPER-1751][] and [ZOOKEEPER-4508][].
    ///
    /// [ZOOKEEPER-1751]: https://issues.apache.org/jira/browse/ZOOKEEPER-1751
    /// [ZOOKEEPER-4508]: https://issues.apache.org/jira/browse/ZOOKEEPER-4508
    fn reset_timeout(&mut self, session_timeout: Duration) {
        let tick = self.calc_tick_timeout(session_timeout);
        self.tick_timeout = tick;
        self.ping_timeout = Duration::from_secs(10).min(3 * tick);
        self.connection_timeout = 8 * tick;
        self.session_timeout = session_timeout.max(self.connection_timeout);
        self.session_expired_timeout = self.session_timeout * 7 / 5;
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
        self.reset_timeout(Duration::from_millis(response.session_timeout as u64));
        self.session_password.clear();
        self.session_password.extend_from_slice(response.password);
        self.session_readonly = response.readonly;
        self.complete_connect();
        Ok(())
    }

    fn read_connection(&mut self, conn: &mut Connection, buf: &mut Vec<u8>) -> Result<(), Error> {
        match conn.try_read_buf(buf) {
            Ok(0) => {
                return Err(Error::ConnectionLoss);
            },
            Err(err) => {
                if err.kind() != io::ErrorKind::WouldBlock {
                    return Err(Error::other_from(err));
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

    async fn serve_connecting(
        &mut self,
        conn: &mut Connection,
        buf: &mut Vec<u8>,
        depot: &mut Depot,
    ) -> Result<(), Error> {
        let mut tick = time::interval(self.tick_timeout);
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        while !(self.session_state.is_connected() && depot.is_empty()) {
            select! {
                _ = conn.readable() => {
                    self.read_connection(conn, buf)?;
                    self.handle_recv_buf(buf, depot)?;
                },
                _ = conn.writable(), if depot.has_pending_writes() || conn.wants_write() => {
                    depot.write_operations(conn)?;
                    self.last_send = Instant::now();
                },
                now = tick.tick() => {
                    if now >= self.last_recv + self.connection_timeout {
                        return Err(Error::ConnectionLoss);
                    }
                },
            }
        }
        Ok(())
    }

    async fn serve_session(
        &mut self,
        conn: &mut Connection,
        buf: &mut Vec<u8>,
        depot: &mut Depot,
        requester: &mut mpsc::UnboundedReceiver<SessionOperation>,
        unwatch_requester: &mut mpsc::UnboundedReceiver<(WatcherId, StateResponser)>,
    ) -> Result<(), Error> {
        let mut tick = time::interval(self.tick_timeout);
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut channel_closed = false;
        depot.start();
        while !(channel_closed && depot.is_empty() && !conn.wants_write()) {
            select! {
                _ = conn.readable() => {
                    self.read_connection(conn, buf)?;
                    self.handle_recv_buf(buf, depot)?;
                },
                _ = conn.writable(), if depot.has_pending_writes() || conn.wants_write() => {
                    depot.write_operations(conn)?;
                    self.last_send = Instant::now();
                },
                r = requester.recv(), if !channel_closed => {
                    let operation = if let Some(operation) = r {
                        operation
                    } else {
                        if !self.detached {
                            depot.push_session(SessionOperation::new_without_body(OpCode::CloseSession));
                        }
                        channel_closed = true;
                        continue;
                    };
                    depot.push_session(operation);
                    depot.write_operations(conn)?;
                    self.last_send = Instant::now();
                },
                r = unwatch_requester.recv() => if let Some((watcher_id, responser)) = r {
                    self.watch_manager.remove_watcher(watcher_id, responser, depot);
                },
                now = tick.tick() => {
                    if now >= self.last_recv + self.connection_timeout {
                        return Err(Error::ConnectionLoss);
                    }
                    if self.last_ping.is_none() && now >= self.last_send + self.ping_timeout {
                        self.send_ping(depot, now);
                        depot.write_operations(conn)?;
                    }
                },
            }
        }
        Err(Error::ClientClosed)
    }

    async fn new_connection<'a>(
        &mut self,
        hosts: &mut impl Iterator<Item = (&'a str, u16, bool)>,
        deadline: &mut Sleep,
    ) -> Result<(Connection, (&'a str, u16, bool)), Error> {
        let (host, port, tls) = match hosts.next() {
            None => return Err(Error::NoHosts),
            Some(addr) => addr,
        };
        select! {
            _ = unsafe { Pin::new_unchecked(deadline) } => Err(Error::Timeout),
            _ = time::sleep(self.connection_timeout) => {
                log::debug!("ZooKeeper fails to connect to {}:{} in {}ms", host, port, self.connection_timeout.as_millis());
                Err(Error::ConnectionLoss)
            },
            r = TcpStream::connect((host, port)) => {
                match r {
                    Err(err) => {
                        log::debug!("ZooKeeper fails to connect to {}:{} due to {}", host, port, err);
                        Err(Error::ConnectionLoss)
                    },
                    Ok(sock) => {
                        let connection = if tls {
                            let domain = ServerName::try_from(host).unwrap().to_owned();
                            let stream = match self.tls_connector.connect(domain, sock).await {
                                Err(err) => {
                                    log::debug!("ZooKeeper fails to complete tls session to {}:{} due to {}", host, port, err);
                                    return Err(Error::ConnectionLoss);
                                },
                                Ok(stream) => stream,
                            };
                            Connection::new_tls(stream)
                        } else {
                            Connection::new_raw(sock)
                        };
                        log::debug!("ZooKeeper succeeds in connecting to {}:{}", host, port);
                        Ok((connection, (host, port, tls)))
                    },
                }
            },
        }
    }

    fn send_ping(&mut self, depot: &mut Depot, now: Instant) {
        let operation = SessionOperation::new_without_body(OpCode::Ping);
        depot.push_operation(Operation::Session(operation));
        self.last_send = now;
        self.last_ping = Some(self.last_send);
    }

    fn send_connect(&self, depot: &mut Depot) {
        let request = ConnectRequest {
            protocol_version: 0,
            last_zxid_seen: 0,
            timeout: self.session_timeout.as_millis() as i32,
            session_id: self.session_id.0,
            password: self.session_password.as_slice(),
            readonly: self.readonly,
        };
        let operation = ConnectOperation::new(&request);
        depot.push_operation(Operation::Connect(operation));
    }

    fn send_authes(&self, depot: &mut Depot) {
        self.authes.iter().for_each(|auth| {
            let operation = SessionOperation::from(auth.clone());
            depot.push_session(operation);
        });
    }

    async fn start_once(
        &mut self,
        hosts: &mut impl Iterator<Item = (&str, u16, bool)>,
        deadline: &mut Sleep,
        buf: &mut Vec<u8>,
        depot: &mut Depot,
    ) -> Result<Connection, Error> {
        let (mut conn, addr) = self.new_connection(hosts, deadline).await?;
        depot.clear();
        buf.clear();
        self.send_connect(depot);
        // TODO: Sasl
        self.send_authes(depot);
        self.watch_manager.resend_watches(self.last_zxid, depot);
        self.last_send = Instant::now();
        self.last_recv = self.last_send;
        self.last_ping = None;
        match self.serve_connecting(&mut conn, buf, depot).await {
            Err(err) => {
                log::warn!("ZooKeeper fails to establish session to {}:{} due to {}", addr.0, addr.1, err);
                Err(err)
            },
            _ => {
                log::info!("ZooKeeper succeeds to establish session to {}:{}", addr.0, addr.1);
                Ok(conn)
            },
        }
    }

    pub async fn start(
        &mut self,
        hosts: &mut impl Iterator<Item = (&str, u16, bool)>,
        buf: &mut Vec<u8>,
        depot: &mut Depot,
    ) -> Result<Connection, Error> {
        let session_timeout = if self.session_id.0 == 0 { self.session_timeout } else { self.session_expired_timeout };
        let mut deadline = time::sleep_until(self.last_recv + session_timeout);
        let mut last_error = match self.start_once(hosts, &mut deadline, buf, depot).await {
            Err(err) => err,
            Ok(conn) => return Ok(conn),
        };
        while last_error != Error::NoHosts && last_error != Error::Timeout && last_error != Error::SessionExpired {
            match self.start_once(hosts, &mut deadline, buf, depot).await {
                Err(err) => {
                    last_error = err;
                    continue;
                },
                Ok(conn) => return Ok(conn),
            };
        }
        Err(last_error)
    }
}
