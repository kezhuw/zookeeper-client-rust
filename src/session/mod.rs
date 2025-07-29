mod connection;
mod depot;
mod event;
mod request;
mod types;
mod watch;
mod xid;

use std::pin::pin;
use std::sync::Weak;
use std::time::{Duration, Instant};

use async_io::Timer;
use asyncs::{select, sync};
use futures::channel::mpsc;
use futures::{AsyncWriteExt, StreamExt};
use ignore_result::Ignore;
use tracing::field::display;
use tracing::{debug, info, instrument, warn, Span};

use self::connection::{AsyncReadToBuf, Connection, Connector};
pub use self::depot::Depot;
use self::event::WatcherEvent;
pub use self::request::{
    ConnectOperation,
    MarshalledRequest,
    OpStat,
    Operation,
    Request,
    SessionOperation,
    StateReceiver,
};
pub use self::types::{EventType, SessionId, SessionInfo, SessionState, WatchedEvent};
use self::watch::WatchManager;
pub use self::watch::{OneshotReceiver, PersistentReceiver, WatchReceiver};
use crate::deadline::Deadline;
use crate::endpoint::IterableEndpoints;
use crate::error::Error;
use crate::proto::{AuthPacket, ConnectRequest, ConnectResponse, ErrorCode, OpCode, PredefinedXid, ReplyHeader};
use crate::record;
#[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
use crate::sasl::{SaslInitiator, SaslOptions, SaslSession};
#[cfg(feature = "tls")]
use crate::tls::TlsOptions;

pub const PASSWORD_LEN: usize = 16;
pub const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(6);

#[derive(Default)]
pub struct Builder {
    #[cfg(feature = "tls")]
    tls: Option<TlsOptions>,
    #[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
    sasl: Option<SaslOptions>,
    authes: Vec<MarshalledRequest>,
    readonly: bool,
    detached: bool,
    session: Option<SessionInfo>,
    session_timeout: Duration,
    connection_timeout: Duration,
}

impl Builder {
    #[cfg(feature = "tls")]
    pub fn with_tls(self, tls: Option<TlsOptions>) -> Self {
        Self { tls, ..self }
    }

    #[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
    pub fn with_sasl(self, sasl: Option<SaslOptions>) -> Self {
        Self { sasl, ..self }
    }

    pub fn with_authes(self, authes: &[AuthPacket]) -> Self {
        Self { authes: authes.iter().map(|auth| MarshalledRequest::new(OpCode::Auth, auth)).collect(), ..self }
    }

    pub fn with_readonly(self, readonly: bool) -> Self {
        Self { readonly, ..self }
    }

    pub fn with_detached(self, detached: bool) -> Self {
        Self { detached, ..self }
    }

    pub fn with_session(self, session: Option<SessionInfo>) -> Self {
        Self { session, ..self }
    }

    pub fn with_session_timeout(self, session_timeout: Duration) -> Self {
        Self { session_timeout, ..self }
    }

    pub fn with_connection_timeout(self, connection_timeout: Duration) -> Self {
        Self { connection_timeout, ..self }
    }

    pub fn build(self, requester: Weak<mpsc::UnboundedSender<Request>>) -> Result<Session, Error> {
        let session = match self.session {
            Some(session) => {
                if session.is_readonly() {
                    return Err(Error::with_message(format!(
                        "can't reestablish readonly and hence local session {}",
                        session.id()
                    )));
                }
                Span::current().record("session", display(session.id()));
                session
            },
            None => SessionInfo::new(SessionId(0), Vec::with_capacity(PASSWORD_LEN)),
        };
        if self.session_timeout < Duration::ZERO {
            return Err(Error::BadArguments(&"session timeout must not be negative"));
        } else if self.connection_timeout < Duration::ZERO {
            return Err(Error::BadArguments(&"connection timeout must not be negative"));
        }
        #[cfg(feature = "tls")]
        let connector = match self.tls {
            Some(options) => Connector::with_tls(options.into_client()?),
            None => Connector::new(),
        };
        #[cfg(not(feature = "tls"))]
        let connector = Connector::new();
        let (state_sender, state_receiver) = sync::watch::channel(SessionState::Disconnected);
        let now = Instant::now();
        let watch_manager = WatchManager::new(requester, state_receiver);
        let mut session = Session {
            readonly: self.readonly,
            detached: self.detached,

            configured_connection_timeout: self.connection_timeout,

            last_zxid: session.last_zxid,
            last_recv: now,
            last_send: now,
            last_ping: None,
            tick_timeout: Duration::ZERO,
            ping_timeout: Duration::ZERO,
            session_expired_timeout: Duration::ZERO,
            connector,
            #[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
            sasl_options: self.sasl,
            #[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
            sasl_session: None,

            session,
            session_timeout: self.session_timeout,
            session_state: SessionState::Disconnected,

            authes: self.authes,
            state_sender,
            watch_manager,
        };
        let timeout = if self.session_timeout.is_zero() { DEFAULT_SESSION_TIMEOUT } else { self.session_timeout };
        session.reset_timeout(timeout);
        Ok(session)
    }
}

pub struct Session {
    readonly: bool,
    detached: bool,

    connector: Connector,

    configured_connection_timeout: Duration,

    last_zxid: i64,
    last_recv: Instant,
    last_send: Instant,
    last_ping: Option<Instant>,
    tick_timeout: Duration,
    ping_timeout: Duration,
    session_expired_timeout: Duration,

    pub session: SessionInfo,
    session_state: SessionState,
    pub session_timeout: Duration,

    #[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
    sasl_options: Option<SaslOptions>,
    #[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
    sasl_session: Option<SaslSession>,

    pub authes: Vec<MarshalledRequest>,
    state_sender: sync::watch::Sender<SessionState>,

    watch_manager: WatchManager,
}

impl Session {
    pub fn builder() -> Builder {
        Builder::default()
    }

    fn is_readonly_allowed(&self) -> bool {
        // Session downgrade is not allowed as partitioned session will expired finally by quorum.
        self.readonly && self.session.readonly
    }

    async fn close_requester(mut requester: mpsc::UnboundedReceiver<Request>, err: &Error) {
        requester.close();
        while let Some(request) = requester.next().await {
            let responser = request.into_responser();
            responser.send(Err(err.clone()));
        }
    }

    pub fn subscribe_state(&self) -> sync::watch::Receiver<SessionState> {
        self.state_sender.subscribe()
    }

    #[instrument(name = "serve", skip_all, fields(session = display(self.session.id)))]
    pub async fn serve(
        &mut self,
        mut endpoints: IterableEndpoints,
        conn: Connection,
        mut buf: Vec<u8>,
        mut depot: Depot,
        mut requester: mpsc::UnboundedReceiver<Request>,
    ) {
        endpoints.cycle();
        endpoints.reset();
        self.serve_once(conn, &mut endpoints, &mut buf, &mut depot, &mut requester).await;
        while !self.session_state.is_terminated() {
            let conn = match self.start(&mut endpoints, &mut buf, &mut depot).await {
                Err(err) => {
                    warn!("fail to connect to cluster {:?} due to {}", endpoints.endpoints(), err);
                    self.resolve_start_error(&err);
                    break;
                },
                Ok(conn) => conn,
            };
            endpoints.reset();
            self.serve_once(conn, &mut endpoints, &mut buf, &mut depot, &mut requester).await;
        }
        let err = self.state_error();
        Self::close_requester(requester, &err).await;
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
        endpoints: &mut IterableEndpoints,
        buf: &mut Vec<u8>,
        depot: &mut Depot,
        requester: &mut mpsc::UnboundedReceiver<Request>,
    ) {
        let err = self.serve_session(endpoints, &mut conn, buf, depot, requester).await.unwrap_err();
        self.resolve_serve_error(&err);
        info!("enter state {} due to {}", self.session_state, err);
        depot.error(&err);
    }

    fn handle_notification(&mut self, zxid: i64, mut body: &[u8], depot: &mut Depot) -> Result<(), Error> {
        let event = record::unmarshal_entity::<WatcherEvent>(&"watch notification", &mut body)?;
        self.watch_manager.dispatch_server_event(event.with_zxid(zxid), depot);
        Ok(())
    }

    fn handle_session_failure(&mut self, operation: SessionOperation, err: Error, depot: &mut Depot) {
        let SessionOperation { responser, request, .. } = operation;
        let info = request.get_operation_info();
        debug!("unknown operation failure: {:?}, {:?}", info, err);
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
        debug!("operation get reply: {:?}, {:?}", info, error_code);
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

    fn handle_session_reply(
        &mut self,
        operation: SessionOperation,
        rc: i32,
        #[allow(unused_mut)] mut body: &[u8],
        #[allow(unused_mut)] depot: &mut Depot,
    ) -> Result<(), Error> {
        let error_code = match ErrorCode::try_from(rc) {
            Ok(error_code) => error_code,
            Err(err) => {
                self.handle_session_failure(operation, Error::from(err), depot);
                return Ok(());
            },
        };
        let SessionOperation { responser, request, .. } = operation;
        let (op_code, watcher) = self.handle_session_watcher(&request, error_code, depot);
        if error_code == ErrorCode::Ok || (op_code == OpCode::Exists && error_code == ErrorCode::NoNode) {
            if op_code == OpCode::Auth {
                if responser.send_empty() {
                    self.authes.push(request);
                }
                return Ok(());
            }
            #[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
            if op_code == OpCode::Sasl {
                let Some(sasl) = self.sasl_session.as_mut() else {
                    return Err(Error::UnexpectedError("got sasl response while no client sasl session".to_string()));
                };
                match record::deserialize::<Option<&[u8]>>(&mut body)? {
                    None => depot.complete_sasl(),
                    Some(challenge) => match sasl.step(challenge)? {
                        None => depot.complete_sasl(),
                        Some(response) => depot.push_sasl(response),
                    },
                }
                return Ok(());
            }
            let mut buf = request.0;
            buf.clear();
            buf.extend_from_slice(body);
            responser.send(Ok((buf, watcher)));
        } else {
            assert!(watcher.is_none());
            responser.send(Err(Error::from(error_code)));
        }
        Ok(())
    }

    fn handle_reply(&mut self, header: ReplyHeader, body: &[u8], depot: &mut Depot) -> Result<(), Error> {
        if header.err == ErrorCode::SessionExpired as i32 {
            return Err(Error::SessionExpired);
        } else if header.err == ErrorCode::AuthFailed as i32
            || header.err == ErrorCode::SessionClosedRequireSaslAuth as i32
        {
            return Err(Error::AuthFailed);
        }
        if header.xid == PredefinedXid::Notification as i32 {
            self.handle_notification(header.zxid, body, depot)?;
            return Ok(());
        } else if header.xid == PredefinedXid::Ping as i32 {
            depot.pop_ping()?;
            if let Some(last_ping) = self.last_ping.take() {
                let elapsed = Instant::now() - last_ping;
                debug!("got ping response after {}ms", elapsed.as_millis());
            }
            return Ok(());
        }
        self.last_zxid = self.last_zxid.max(header.zxid);
        let operation = depot.pop_request(header.xid)?;
        self.handle_session_reply(operation, header.err, body, depot)
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
        self.connector.set_timeout(8 * tick);
        self.session_timeout = session_timeout.max(self.connector.timeout());
        self.session_expired_timeout = self.session_timeout * 7 / 5;
    }

    fn complete_connect(&mut self) {
        let state = if self.session.readonly { SessionState::ConnectedReadOnly } else { SessionState::SyncConnected };
        self.change_state(state);
    }

    fn handle_connect_response(&mut self, mut body: &[u8]) -> Result<(), Error> {
        let response = record::unmarshal::<ConnectResponse>(&mut body)?;
        debug!("received connect response: {response:?}");
        let session_id = SessionId(response.session_id);
        if session_id.0 == 0 {
            return Err(Error::SessionExpired);
        } else if !self.is_readonly_allowed() && response.readonly {
            return Err(Error::UnexpectedError(format!(
                "get readonly session {}, while is not allowed since readonly={}, old session readonly={}",
                session_id, self.readonly, self.session.readonly
            )));
        }
        if self.session.id != session_id {
            self.session.id = session_id;
            info!(
                "new session established: {} readonly={}, timeout={}ms",
                session_id, response.readonly as bool, response.session_timeout
            );
            let span = Span::current();
            span.record("session", display(session_id));
        }
        self.session.password.clear();
        self.session.password.extend_from_slice(response.password);
        self.session.readonly = response.readonly;
        self.reset_timeout(Duration::from_millis(response.session_timeout as u64));
        self.complete_connect();
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
        let mut pinged = false;
        let (mut reader, mut writer) = conn.split();
        let mut tick = pin!(Timer::interval(self.tick_timeout));
        while !(self.session_state.is_connected() && depot.is_empty()) {
            select! {
                r = reader.read_to_buf(buf) => match r.map_err(Error::other)? {
                    0 => return Err(Error::ConnectionLoss),
                    _ => self.handle_recv_buf(buf, depot)?,
                },
                r = depot.write_to(&mut writer) => {
                    r?;
                    self.last_send = Instant::now();
                },
                now = tick.as_mut() => {
                    if now >= self.last_recv + self.connector.timeout() {
                        return Err(Error::with_message(format!("no response from connection in {}ms", self.connector.timeout().as_millis())));
                    }
                },
            }
            if self.session_state.is_connected() && !pinged {
                // Send opcode other than `auth` and `sasl` to get possible AuthFailed if
                // "zookeeper.enforce.auth.enabled".
                pinged = true;
                self.send_ping(depot, Instant::now());
            }
        }
        Ok(())
    }

    async fn poll<T>(f: &mut Option<impl std::future::Future<Output = T>>) -> T {
        match f.as_mut() {
            None => std::future::pending().await,
            Some(f) => unsafe { std::pin::Pin::new_unchecked(f).await },
        }
    }

    async fn serve_session(
        &mut self,
        endpoints: &mut IterableEndpoints,
        conn: &mut Connection,
        buf: &mut Vec<u8>,
        depot: &mut Depot,
        requester: &mut mpsc::UnboundedReceiver<Request>,
    ) -> Result<(), Error> {
        #[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
        self.sasl_session.take();
        let mut seek_for_writable =
            if self.session.readonly { Some(self.connector.clone().seek_for_writable(endpoints)) } else { None };
        let mut tick = pin!(Timer::interval(self.tick_timeout));
        let mut err = None;
        let mut channel_halted = false;
        depot.start();
        let (mut reader, mut writer) = conn.split();
        while !(channel_halted && depot.is_empty()) {
            select! {
                Some(endpoint) = Self::poll(&mut seek_for_writable), if seek_for_writable.is_some() => {
                    seek_for_writable = None;
                    err = Some(Error::with_message(format!("encounter writable server {endpoint}")));
                    channel_halted = true;
                },
                r = reader.read_to_buf(buf) => match r.map_err(Error::other)? {
                    0 => return Err(Error::ConnectionLoss),
                    _ => self.handle_recv_buf(buf, depot)?,
                },
                r = depot.write_to(&mut writer) => {
                    r?;
                    self.last_send = Instant::now();
                },
                r = requester.next(), if !channel_halted => match r {
                    None => {
                        if !self.detached {
                            depot.push_session(SessionOperation::new_without_body(OpCode::CloseSession));
                        }
                        channel_halted = true;
                    }
                    Some(Request::Session(operation)) => depot.push_session(operation),
                    Some(Request::RemoveWatcher {
                        id, responser
                    }) => {
                        self.watch_manager.remove_watcher(id, responser, depot);
                    }
                },
                now = tick.as_mut() => {
                    if now >= self.last_recv + self.connector.timeout() {
                        return Err(Error::with_message(format!("no response from connection in {}ms", self.connector.timeout().as_millis())));
                    }
                    if self.last_ping.is_none() && now >= self.last_send + self.ping_timeout {
                        self.send_ping(depot, now);
                    }
                },
            }
        }
        writer.flush().await.map_err(Error::other)?;
        Err(err.unwrap_or(Error::ClientClosed))
    }

    fn send_ping(&mut self, depot: &mut Depot, now: Instant) {
        let operation = SessionOperation::new_without_body(OpCode::Ping);
        depot.push_session(operation);
        self.last_send = now;
        self.last_ping = Some(self.last_send);
    }

    fn send_connect(&self, depot: &mut Depot) {
        let request = ConnectRequest {
            protocol_version: 0,
            last_zxid_seen: self.last_zxid,
            timeout: self.session_timeout.as_millis() as i32,
            session_id: if self.session.readonly { 0 } else { self.session.id.0 },
            password: self.session.password.as_slice(),
            readonly: self.is_readonly_allowed(),
        };
        debug!("sending connect request: {request:?}");
        let operation = ConnectOperation::new(&request);
        depot.push_operation(Operation::Connect(operation));
    }

    #[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
    fn send_sasl(&mut self, depot: &mut Depot, host: &str) -> Result<(), Error> {
        self.sasl_session = None;
        let Some(options) = self.sasl_options.as_ref() else {
            return Ok(());
        };
        let session = options.new_session(host).inspect_err(|e| {
            warn!("fail to open sasl session for {host}: {e}");
        })?;
        depot.push_sasl(session.initial());
        self.sasl_session = Some(session);
        Ok(())
    }

    fn send_authes(&self, depot: &mut Depot) {
        self.authes.iter().for_each(|auth| {
            let operation = SessionOperation::from(auth.clone());
            depot.push_session(operation);
        });
    }

    async fn start_once(
        &mut self,
        endpoints: &mut IterableEndpoints,
        deadline: &mut Deadline,
        buf: &mut Vec<u8>,
        depot: &mut Depot,
    ) -> Result<Connection, Error> {
        let Some(endpoint) = endpoints.next(deadline.timeout()).await else {
            return Err(Error::NoHosts);
        };
        let mut conn = match self.connector.connect(endpoint, deadline).await {
            Err(err) => {
                debug!("fails in connecting to {} due to {:?}", endpoint, err);
                return Err(Error::ConnectionLoss);
            },
            Ok(conn) => {
                debug!("succeeds in connecting to {}", endpoint);
                conn
            },
        };
        depot.reset();
        buf.clear();
        self.send_connect(depot);
        #[cfg(any(feature = "sasl-digest-md5", feature = "sasl-gssapi"))]
        self.send_sasl(depot, endpoint.host)?;
        self.send_authes(depot);
        self.watch_manager.resend_watches(self.last_zxid, depot);
        self.last_send = Instant::now();
        self.last_recv = self.last_send;
        self.last_ping = None;
        self.change_state(SessionState::Disconnected);
        match self.serve_connecting(&mut conn, buf, depot).await {
            Err(err) => {
                warn!("fails to establish session to {} due to {}", endpoint, err);
                Err(err)
            },
            _ => {
                info!("succeeds to establish session to {}", endpoint);
                Ok(conn)
            },
        }
    }

    pub async fn start(
        &mut self,
        endpoints: &mut IterableEndpoints,
        buf: &mut Vec<u8>,
        depot: &mut Depot,
    ) -> Result<Connection, Error> {
        endpoints.start();
        let session_timeout = if self.session.id.0 == 0 { self.session_timeout } else { self.session_expired_timeout };
        debug!(
            session_timeout = session_timeout.as_millis(),
            connection_timeout = self.connector.timeout().as_millis(),
            "attempts new connections to {:?}",
            endpoints.endpoints()
        );
        let mut deadline = Deadline::until(self.last_recv + session_timeout);
        let mut last_error = match self.start_once(endpoints, &mut deadline, buf, depot).await {
            Err(err) => err,
            Ok(conn) => return Ok(conn),
        };
        while !last_error.is_terminated() {
            if deadline.elapsed() {
                return Err(Error::Timeout);
            }
            match self.start_once(endpoints, &mut deadline, buf, depot).await {
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
