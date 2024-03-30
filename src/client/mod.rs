mod watcher;

use std::borrow::Cow;
use std::fmt::Write as _;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::time::Duration;

use const_format::formatcp;
use either::{Either, Left, Right};
use ignore_result::Ignore;
use thiserror::Error;
use tokio::sync::{mpsc, watch};

pub use self::watcher::{OneshotWatcher, PersistentWatcher, StateWatcher};
use super::session::{Depot, MarshalledRequest, Session, SessionOperation, WatchReceiver};
use crate::acl::{Acl, Acls, AuthUser};
use crate::chroot::{Chroot, ChrootPath, OwnedChroot};
use crate::endpoint::{self, IterableEndpoints};
use crate::error::Error;
use crate::proto::{
    self,
    AuthPacket,
    CheckVersionRequest,
    CreateRequest,
    DeleteRequest,
    ExistsRequest,
    GetAclResponse,
    GetChildren2Response,
    GetChildrenRequest,
    GetRequest,
    MultiHeader,
    MultiReadResponse,
    MultiWriteResponse,
    OpCode,
    PersistentWatchRequest,
    ReconfigRequest,
    RequestBuffer,
    RequestHeader,
    SetAclRequest,
    SetDataRequest,
    SyncRequest,
};
pub use crate::proto::{EnsembleUpdate, Stat};
use crate::record::{self, Record, StaticRecord};
use crate::session::StateReceiver;
pub use crate::session::{EventType, SessionId, SessionInfo, SessionState, WatchedEvent};
use crate::tls::TlsOptions;
use crate::util;

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// CreateMode specifies ZooKeeper znode type. It covers all znode types with help from
/// [CreateOptions::with_ttl].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateMode {
    Persistent,
    PersistentSequential,
    Ephemeral,
    EphemeralSequential,
    Container,
}

impl CreateMode {
    /// Constructs [CreateOptions] with this mode and given acls.
    pub const fn with_acls(self, acls: Acls<'_>) -> CreateOptions<'_> {
        CreateOptions { mode: self, acls, ttl: None }
    }

    fn is_sequential(self) -> bool {
        self == CreateMode::PersistentSequential || self == CreateMode::EphemeralSequential
    }

    fn is_persistent(self) -> bool {
        self == Self::Persistent || self == Self::PersistentSequential
    }

    fn is_ephemeral(self) -> bool {
        self == Self::Ephemeral || self == Self::EphemeralSequential
    }

    fn is_container(self) -> bool {
        self == CreateMode::Container
    }

    fn as_flags(self, ttl: bool) -> i32 {
        use CreateMode::*;
        match self {
            Persistent => {
                if ttl {
                    5
                } else {
                    0
                }
            },
            PersistentSequential => {
                if ttl {
                    6
                } else {
                    2
                }
            },
            Ephemeral => 1,
            EphemeralSequential => 3,
            Container => 4,
        }
    }
}

/// Watch mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AddWatchMode {
    /// Combination of stat, data and child watches on watching node.
    Persistent,

    /// Combination of stat and data watches on watching node and its children.
    PersistentRecursive,
}

impl From<AddWatchMode> for proto::AddWatchMode {
    fn from(mode: AddWatchMode) -> proto::AddWatchMode {
        match mode {
            AddWatchMode::Persistent => proto::AddWatchMode::Persistent,
            AddWatchMode::PersistentRecursive => proto::AddWatchMode::PersistentRecursive,
        }
    }
}

/// Options for node creation, constructed from [CreateMode::with_acls].
#[derive(Clone, Debug)]
pub struct CreateOptions<'a> {
    mode: CreateMode,
    acls: Acls<'a>,
    ttl: Option<Duration>,
}

// Five bytes are available for milliseconds. See javadoc of EphemeralType in ZooKeeper for reference.
//
// https://github.com/apache/zookeeper/blob/ebcf18e52fa095773429348ce495d59c896f4a26/zookeeper-server/src/main/java/org/apache/zookeeper/server/EphemeralType.java#L46
const TTL_MAX_MILLIS: u128 = 0x00FFFFFFFFFF;

impl<'a> CreateOptions<'a> {
    /// Specifies ttl for persistent node.
    pub const fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    fn validate(&'a self) -> Result<()> {
        if let Some(ref ttl) = self.ttl {
            if !self.mode.is_persistent() {
                return Err(Error::BadArguments(&"ttl can only be specified with persistent node"));
            } else if ttl.is_zero() {
                return Err(Error::BadArguments(&"ttl is zero"));
            } else if ttl.as_millis() > TTL_MAX_MILLIS {
                return Err(Error::BadArguments(&formatcp!("ttl cannot larger than {}", TTL_MAX_MILLIS)));
            }
        }
        if self.acls.is_empty() {
            return Err(Error::InvalidAcl);
        }
        Ok(())
    }

    fn validate_as_directory(&self) -> Result<()> {
        self.validate()?;
        if self.mode.is_ephemeral() {
            return Err(Error::BadArguments(&"directory node must not be ephemeral"));
        } else if self.mode.is_sequential() {
            return Err(Error::BadArguments(&"directory node must not be sequential"));
        }
        Ok(())
    }
}

/// Thin wrapper to encapsulate sequential node's sequence number.
///
/// It prints in ten decimal digits with possible leading padding 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CreateSequence(i64);

impl std::fmt::Display for CreateSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Discussion for 64-bit ephemeral sequence number:
        // * https://lists.apache.org/thread/4o3rl49rdj5y0134df922zgc8clyt86s
        // * https://issues.apache.org/jira/browse/ZOOKEEPER-4706
        if self.0 <= i32::MAX.into() {
            write!(f, "{:010}", self.0)
        } else {
            write!(f, "{:019}", self.0)
        }
    }
}

impl CreateSequence {
    pub fn into_i64(self) -> i64 {
        self.0
    }
}

/// Client encapsulates ZooKeeper session to interact with ZooKeeper cluster.
///
/// Besides semantic errors, node operations could also fail due to cluster availability and
/// capabilities, e.g. [Error::ConnectionLoss], [Error::QuotaExceeded], [Error::Unimplemented] and
/// so on.
///
/// All remote operations will fail after session expired, failed or closed.
///
/// # Notable behaviors
/// * All cloned clients share same authentication identities.
/// * All methods construct resulting future by sending request synchronously and polling output
///   asynchronously. This guarantees that requests are sending to server in the order of method
///   call but not future evaluation.
#[derive(Clone, Debug)]
pub struct Client {
    chroot: OwnedChroot,
    version: Version,
    session: SessionInfo,
    session_timeout: Duration,
    requester: mpsc::UnboundedSender<SessionOperation>,
    state_watcher: StateWatcher,
}

impl Client {
    const CONFIG_NODE: &'static str = "/zookeeper/config";

    /// Connects to ZooKeeper cluster.
    pub async fn connect(cluster: &str) -> Result<Self> {
        Self::connector().connect(cluster).await
    }

    /// Creates a builder with configurable options in connecting to ZooKeeper cluster.
    #[deprecated(since = "0.7.0", note = "use Client::connector instead")]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Creates a builder with configurable options in connecting to ZooKeeper cluster.
    pub fn connector() -> Connector {
        Connector::new()
    }

    pub(crate) fn new(
        chroot: OwnedChroot,
        version: Version,
        session: SessionInfo,
        timeout: Duration,
        requester: mpsc::UnboundedSender<SessionOperation>,
        state_receiver: watch::Receiver<SessionState>,
    ) -> Client {
        let state_watcher = StateWatcher::new(state_receiver);
        Client { chroot, version, session, session_timeout: timeout, requester, state_watcher }
    }

    fn validate_path<'a>(&'a self, path: &'a str) -> Result<ChrootPath<'a>> {
        ChrootPath::new(self.chroot.as_ref(), path, false)
    }

    fn validate_sequential_path<'a>(&'a self, path: &'a str) -> Result<ChrootPath<'a>> {
        ChrootPath::new(self.chroot.as_ref(), path, true)
    }

    /// Path of chroot.
    pub fn path(&self) -> &str {
        self.chroot.path()
    }

    /// ZooKeeper session info.
    pub fn session(&self) -> &SessionInfo {
        &self.session
    }

    /// ZooKeeper session id.
    pub fn session_id(&self) -> SessionId {
        self.session().id()
    }

    /// Consumes this instance into session info.
    pub fn into_session(self) -> SessionInfo {
        self.session
    }

    /// Negotiated session timeout.
    pub fn session_timeout(&self) -> Duration {
        self.session_timeout
    }

    /// Latest session state.
    pub fn state(&self) -> SessionState {
        self.state_watcher.peek_state()
    }

    /// Creates a [StateWatcher] to track future session state updates.
    pub fn state_watcher(&self) -> StateWatcher {
        let mut watcher = self.state_watcher.clone();
        watcher.state();
        watcher
    }

    /// Changes root directory to given absolute path.
    ///
    /// # Errors
    /// In case of bad root path, old client is wrapped in [Result::Err].
    ///
    /// # Notable behaviors
    /// * Existing watchers are not affected.
    pub fn chroot<'a>(mut self, path: impl Into<Cow<'a, str>>) -> std::result::Result<Client, Client> {
        if self.chroot.chroot(path) {
            Ok(self)
        } else {
            Err(self)
        }
    }

    fn send_request(&self, code: OpCode, body: &impl Record) -> StateReceiver {
        let request = MarshalledRequest::new(code, body);
        self.send_marshalled_request(request)
    }

    fn send_marshalled_request(&self, request: MarshalledRequest) -> StateReceiver {
        let (operation, receiver) = SessionOperation::new_marshalled(request).with_responser();
        if let Err(mpsc::error::SendError(operation)) = self.requester.send(operation) {
            let state = self.state();
            operation.responser.send(Err(state.to_error()));
        }
        receiver
    }

    async fn wait<T, E, F>(result: std::result::Result<F, E>) -> std::result::Result<T, E>
    where
        F: Future<Output = std::result::Result<T, E>>, {
        match result {
            Err(err) => Err(err),
            Ok(future) => future.await,
        }
    }

    async fn resolve<T, E, F>(result: std::result::Result<Either<F, T>, E>) -> std::result::Result<T, E>
    where
        F: Future<Output = std::result::Result<T, E>>, {
        match result {
            Err(err) => Err(err),
            Ok(Right(r)) => Ok(r),
            Ok(Left(future)) => future.await,
        }
    }

    async fn map_wait<T, U, Fu, Fn>(result: Result<Fu>, f: Fn) -> Result<U>
    where
        Fu: Future<Output = Result<T>>,
        Fn: FnOnce(T) -> U, {
        match result {
            Err(err) => Err(err),
            Ok(future) => match future.await {
                Err(err) => Err(err),
                Ok(t) => Ok(f(t)),
            },
        }
    }

    async fn retry_on_connection_loss<T, F>(operation: impl Fn() -> F) -> Result<T>
    where
        F: Future<Output = Result<T>>, {
        loop {
            let future = operation();
            return match future.await {
                Err(Error::ConnectionLoss) => continue,
                result => result,
            };
        }
    }

    fn parse_sequence(client_path: &str, prefix: &str) -> Result<CreateSequence> {
        if let Some(sequence_path) = client_path.strip_prefix(prefix) {
            match sequence_path.parse::<i64>() {
                Err(_) => Err(Error::UnexpectedError(format!("sequential node get no i32 path {}", client_path))),
                Ok(i) => Ok(CreateSequence(i)),
            }
        } else {
            Err(Error::UnexpectedError(format!(
                "sequential path {} does not contain prefix path {}",
                client_path, prefix
            )))
        }
    }

    /// Makes directories up to path. Treats it as `mkdir -p`.
    ///
    /// # Notable behaviors
    /// * No atomic, so it could fail with only partial directories created.
    /// * Pure asynchronous, so there is no order guarantee.
    /// * No [Error::NodeExists].
    ///
    /// # Notable errors
    /// * [Error::NoNode] if chroot does not exist.
    /// * [Error::BadArguments] if [CreateMode] is ephemeral or sequential.
    /// * [Error::InvalidAcl] if acl is invalid or empty.
    pub async fn mkdir(&self, path: &str, options: &CreateOptions<'_>) -> Result<()> {
        options.validate_as_directory()?;
        self.mkdir_internally(path, options).await
    }

    async fn mkdir_internally(&self, path: &str, options: &CreateOptions<'_>) -> Result<()> {
        let mut j = path.len();
        loop {
            match self.create(&path[..j], Default::default(), options).await {
                Ok(_) | Err(Error::NodeExists) => {
                    if j >= path.len() {
                        return Ok(());
                    } else if let Some(i) = path[j + 1..].find('/') {
                        j = j + 1 + i;
                    } else {
                        j = path.len();
                    }
                },
                Err(Error::NoNode) => {
                    let i = path[..j].rfind('/').unwrap();
                    if i == 0 {
                        // chroot does not exist,
                        return Err(Error::NoNode);
                    }
                    j = i;
                },
                Err(err) => return Err(err),
            }
        }
    }

    /// Creates node with given path and data.
    ///
    /// # Notable errors
    /// * [Error::NodeExists] if a node with same path already exists.
    /// * [Error::NoNode] if parent node does not exist.
    /// * [Error::NoChildrenForEphemerals] if parent node is ephemeral.
    /// * [Error::InvalidAcl] if acl is invalid or empty.
    ///
    /// # Notable behaviors
    /// The resulting [Stat] will be [Stat::is_invalid] if assumed server version is 3.4 series or
    /// below. See [ClientBuilder::assume_server_version] and [ZOOKEEPER-1297][].
    ///
    /// [ZOOKEEPER-1297]: https://issues.apache.org/jira/browse/ZOOKEEPER-1297
    pub fn create<'a: 'f, 'b: 'f, 'f>(
        &'a self,
        path: &'b str,
        data: &[u8],
        options: &CreateOptions<'_>,
    ) -> impl Future<Output = Result<(Stat, CreateSequence)>> + Send + 'f {
        Self::wait(self.create_internally(path, data, options))
    }

    fn create_internally<'a: 'f, 'b: 'f, 'f>(
        &'a self,
        path: &'b str,
        data: &[u8],
        options: &CreateOptions<'_>,
    ) -> Result<impl Future<Output = Result<(Stat, CreateSequence)>> + Send + 'f> {
        options.validate()?;
        let create_mode = options.mode;
        let sequential = create_mode.is_sequential();
        let chroot_path = if sequential { self.validate_sequential_path(path)? } else { self.validate_path(path)? };
        if chroot_path.is_root() {
            return Err(Error::BadArguments(&"can not create root node"));
        }
        let ttl = options.ttl.map(|ttl| ttl.as_millis() as i64).unwrap_or(0);
        let op_code = if ttl != 0 {
            OpCode::CreateTtl
        } else if create_mode.is_container() {
            OpCode::CreateContainer
        } else if self.version >= Version(3, 5, 0) {
            OpCode::Create2
        } else {
            OpCode::Create
        };
        let flags = create_mode.as_flags(ttl != 0);
        let request = CreateRequest { path: chroot_path, data, acls: options.acls, flags, ttl };
        let receiver = self.send_request(op_code, &request);
        Ok(async move {
            let (body, _) = receiver.await?;
            let mut buf = body.as_slice();
            let server_path = record::unmarshal_entity::<&str>(&"server path", &mut buf)?;
            let client_path = util::strip_root_path(server_path, self.chroot.root())?;
            let sequence = if sequential { Self::parse_sequence(client_path, path)? } else { CreateSequence(-1) };
            let stat =
                if op_code == OpCode::Create { Stat::new_invalid() } else { record::unmarshal::<Stat>(&mut buf)? };
            Ok((stat, sequence))
        })
    }

    /// Deletes node with specified path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    /// * [Error::BadVersion] if such node exists but has different version.
    /// * [Error::NotEmpty] if such node exists but has children.
    pub fn delete(&self, path: &str, expected_version: Option<i32>) -> impl Future<Output = Result<()>> + Send {
        Self::wait(self.delete_internally(path, expected_version))
    }

    fn delete_internally(&self, path: &str, expected_version: Option<i32>) -> Result<impl Future<Output = Result<()>>> {
        let chroot_path = self.validate_path(path)?;
        if chroot_path.is_root() {
            return Err(Error::BadArguments(&"can not delete root node"));
        }
        let request = DeleteRequest { path: chroot_path, version: expected_version.unwrap_or(-1) };
        let receiver = self.send_request(OpCode::Delete, &request);
        Ok(async move {
            receiver.await?;
            Ok(())
        })
    }

    // TODO: move these to session side so to eliminate owned Client and String.
    fn delete_background(self, path: String) {
        tokio::spawn(async move {
            self.delete_foreground(&path).await;
        });
    }

    async fn delete_foreground(&self, path: &str) {
        Client::retry_on_connection_loss(|| self.delete(path, None)).await.ignore();
    }

    fn delete_ephemeral_background(self, prefix: String, unique: bool) {
        tokio::spawn(async move {
            let (parent, tree, name) = util::split_path(&prefix);
            let mut children = Self::retry_on_connection_loss(|| self.list_children(parent)).await?;
            if unique {
                if let Some(i) = children.iter().position(|s| s.starts_with(name)) {
                    self.delete_foreground(&children[i]).await;
                };
                return Ok::<(), Error>(());
            }
            children.retain(|s| s.starts_with(name));
            for child in children.iter_mut() {
                child.insert_str(0, tree);
            }
            let results = Self::retry_on_connection_loss(|| {
                let mut reader = self.new_multi_reader();
                for child in children.iter() {
                    reader.add_get_data(child).unwrap();
                }
                reader.commit()
            })
            .await?;
            for (i, result) in results.into_iter().enumerate() {
                let MultiReadResult::Data { stat, .. } = result else {
                    // It could be Error::NoNode.
                    continue;
                };
                if stat.ephemeral_owner == self.session_id().0 {
                    self.delete_foreground(&children[i]).await;
                    break;
                }
            }
            Ok(())
        });
    }

    fn get_data_internally(
        &self,
        chroot: Chroot,
        path: &str,
        watch: bool,
    ) -> Result<impl Future<Output = Result<(Vec<u8>, Stat, WatchReceiver)>> + Send> {
        let chroot_path = ChrootPath::new(chroot, path, false)?;
        let request = GetRequest { path: chroot_path, watch };
        let receiver = self.send_request(OpCode::GetData, &request);
        Ok(async move {
            let (mut body, watcher) = receiver.await?;
            let data_len = body.len() - Stat::record_len();
            let mut stat_buf = &body[data_len..];
            let stat = record::unmarshal(&mut stat_buf)?;
            body.truncate(data_len);
            drop(body.drain(..4));
            Ok((body, stat, watcher))
        })
    }

    /// Gets stat and data for node with given path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub fn get_data(&self, path: &str) -> impl Future<Output = Result<(Vec<u8>, Stat)>> + Send {
        let result = self.get_data_internally(self.chroot.as_ref(), path, false);
        Self::map_wait(result, |(data, stat, _)| (data, stat))
    }

    /// Gets stat and data for node with given path, and watches node deletion and data change.
    ///
    /// The watch will be triggered by:
    /// * Data change.
    /// * Node deletion.
    /// * Session expiration.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub fn get_and_watch_data(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<(Vec<u8>, Stat, OneshotWatcher)>> + Send + '_ {
        let result = self.get_data_internally(self.chroot.as_ref(), path, true);
        Self::map_wait(result, |(data, stat, watcher)| (data, stat, watcher.into_oneshot(&self.chroot)))
    }

    fn check_stat_internally(
        &self,
        path: &str,
        watch: bool,
    ) -> Result<impl Future<Output = Result<(Option<Stat>, WatchReceiver)>>> {
        let chroot_path = self.validate_path(path)?;
        let request = ExistsRequest { path: chroot_path, watch };
        let receiver = self.send_request(OpCode::Exists, &request);
        Ok(async move {
            let (body, watcher) = receiver.await?;
            let mut buf = body.as_slice();
            let stat = record::try_deserialize(&mut buf)?;
            Ok((stat, watcher))
        })
    }

    /// Checks stat for node with given path.
    pub fn check_stat(&self, path: &str) -> impl Future<Output = Result<Option<Stat>>> + Send {
        Self::map_wait(self.check_stat_internally(path, false), |(stat, _)| stat)
    }

    /// Checks stat for node with given path, and watches node creation, deletion and data change.
    ///
    /// The watch will be triggered by:
    /// * Data change.
    /// * Node creation and deletion.
    /// * Session expiration.
    pub fn check_and_watch_stat(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<(Option<Stat>, OneshotWatcher)>> + Send + '_ {
        let result = self.check_stat_internally(path, true);
        Self::map_wait(result, |(stat, watcher)| (stat, watcher.into_oneshot(&self.chroot)))
    }

    /// Sets data for node with given path and returns updated stat.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    /// * [Error::BadVersion] if such node exists but has different version.
    /// * [Error::NoAuth] if the client has insufficient authorization to such node.
    pub fn set_data(
        &self,
        path: &str,
        data: &[u8],
        expected_version: Option<i32>,
    ) -> impl Future<Output = Result<Stat>> + Send {
        Self::wait(self.set_data_internally(path, data, expected_version))
    }

    pub fn set_data_internally(
        &self,
        path: &str,
        data: &[u8],
        expected_version: Option<i32>,
    ) -> Result<impl Future<Output = Result<Stat>>> {
        let chroot_path = self.validate_path(path)?;
        let request = SetDataRequest { path: chroot_path, data, version: expected_version.unwrap_or(-1) };
        let receiver = self.send_request(OpCode::SetData, &request);
        Ok(async move {
            let (body, _) = receiver.await?;
            let mut buf = body.as_slice();
            let stat: Stat = record::unmarshal(&mut buf)?;
            Ok(stat)
        })
    }

    fn list_children_internally(
        &self,
        path: &str,
        watch: bool,
    ) -> Result<impl Future<Output = Result<(Vec<String>, WatchReceiver)>>> {
        let chroot_path = self.validate_path(path)?;
        let request = GetChildrenRequest { path: chroot_path, watch };
        let receiver = self.send_request(OpCode::GetChildren, &request);
        Ok(async move {
            let (body, watcher) = receiver.await?;
            let mut buf = body.as_slice();
            let children = record::unmarshal_entity::<Vec<String>>(&"children paths", &mut buf)?;
            Ok((children, watcher))
        })
    }

    /// Lists children for node with given path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub fn list_children(&self, path: &str) -> impl Future<Output = Result<Vec<String>>> + Send + '_ {
        Self::map_wait(self.list_children_internally(path, false), |(children, _)| children)
    }

    /// Lists children for node with given path, and watches node deletion, children creation and
    /// deletion.
    ///
    /// The watch will be triggered by:
    /// * Children creation and deletion.
    /// * Node deletion.
    /// * Session expiration.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub fn list_and_watch_children(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<(Vec<String>, OneshotWatcher)>> + Send + '_ {
        let result = self.list_children_internally(path, true);
        Self::map_wait(result, |(children, watcher)| (children, watcher.into_oneshot(&self.chroot)))
    }

    fn get_children_internally(
        &self,
        path: &str,
        watch: bool,
    ) -> Result<impl Future<Output = Result<(Vec<String>, Stat, WatchReceiver)>>> {
        let chroot_path = self.validate_path(path)?;
        let request = GetChildrenRequest { path: chroot_path, watch };
        let receiver = self.send_request(OpCode::GetChildren2, &request);
        Ok(async move {
            let (body, watcher) = receiver.await?;
            let mut buf = body.as_slice();
            let response = record::unmarshal::<GetChildren2Response>(&mut buf)?;
            Ok((response.children, response.stat, watcher))
        })
    }

    /// Gets stat and children for node with given path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub fn get_children(&self, path: &str) -> impl Future<Output = Result<(Vec<String>, Stat)>> + Send {
        let result = self.get_children_internally(path, false);
        Self::map_wait(result, |(children, stat, _)| (children, stat))
    }

    /// Gets stat and children for node with given path, and watches node deletion, children
    /// creation and deletion.
    ///
    /// The watch will be triggered by:
    /// * Children creation and deletion.
    /// * Node deletion.
    /// * Session expiration.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub fn get_and_watch_children(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<(Vec<String>, Stat, OneshotWatcher)>> + Send + '_ {
        let result = self.get_children_internally(path, true);
        Self::map_wait(result, |(children, stat, watcher)| (children, stat, watcher.into_oneshot(&self.chroot)))
    }

    /// Counts descendants number for node with given path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub fn count_descendants_number(&self, path: &str) -> impl Future<Output = Result<usize>> + Send {
        Self::wait(self.count_descendants_number_internally(path))
    }

    fn count_descendants_number_internally(&self, path: &str) -> Result<impl Future<Output = Result<usize>>> {
        let chroot_path = self.validate_path(path)?;
        let receiver = self.send_request(OpCode::GetAllChildrenNumber, &chroot_path);
        Ok(async move {
            let (body, _) = receiver.await?;
            let mut buf = body.as_slice();
            let n = record::unmarshal_entity::<i32>(&"all children number", &mut buf)?;
            Ok(n as usize)
        })
    }

    /// Lists all ephemerals nodes that created by current session and starts with given path.
    ///
    /// # Notable behaviors
    /// * No [Error::NoNode] if node with give path does not exist.
    /// * Result will include given path if that node is ephemeral.
    /// * Returned paths are located at chroot but not ZooKeeper root.
    pub fn list_ephemerals(&self, path: &str) -> impl Future<Output = Result<Vec<String>>> + Send + '_ {
        Self::wait(self.list_ephemerals_internally(path))
    }

    fn list_ephemerals_internally(&self, path: &str) -> Result<impl Future<Output = Result<Vec<String>>> + Send + '_> {
        let path = self.validate_path(path)?;
        let receiver = self.send_request(OpCode::GetEphemerals, &path);
        Ok(async move {
            let (body, _) = receiver.await?;
            let mut buf = body.as_slice();
            let mut ephemerals = record::unmarshal_entity::<Vec<String>>(&"ephemerals", &mut buf)?;
            for ephemeral_path in ephemerals.iter_mut() {
                util::drain_root_path(ephemeral_path, self.chroot.root())?;
            }
            Ok(ephemerals)
        })
    }

    /// Gets acl and stat for node with given path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub fn get_acl(&self, path: &str) -> impl Future<Output = Result<(Vec<Acl>, Stat)>> + Send + '_ {
        Self::wait(self.get_acl_internally(path))
    }

    fn get_acl_internally(&self, path: &str) -> Result<impl Future<Output = Result<(Vec<Acl>, Stat)>>> {
        let chroot_path = self.validate_path(path)?;
        let receiver = self.send_request(OpCode::GetACL, &chroot_path);
        Ok(async move {
            let (body, _) = receiver.await?;
            let mut buf = body.as_slice();
            let response: GetAclResponse = record::unmarshal(&mut buf)?;
            Ok((response.acl, response.stat))
        })
    }

    /// Sets acl for node with given path and returns updated stat.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    /// * [Error::BadVersion] if such node exists but has different acl version.
    pub fn set_acl(
        &self,
        path: &str,
        acl: &[Acl],
        expected_acl_version: Option<i32>,
    ) -> impl Future<Output = Result<Stat>> + Send + '_ {
        Self::wait(self.set_acl_internally(path, acl, expected_acl_version))
    }

    fn set_acl_internally(
        &self,
        path: &str,
        acl: &[Acl],
        expected_acl_version: Option<i32>,
    ) -> Result<impl Future<Output = Result<Stat>>> {
        let chroot_path = self.validate_path(path)?;
        let request = SetAclRequest { path: chroot_path, acl, version: expected_acl_version.unwrap_or(-1) };
        let receiver = self.send_request(OpCode::SetACL, &request);
        Ok(async move {
            let (body, _) = receiver.await?;
            let mut buf = body.as_slice();
            let stat: Stat = record::unmarshal(&mut buf)?;
            Ok(stat)
        })
    }

    /// Watches possible nonexistent path using specified mode.
    ///
    /// The watch will be triggered by:
    /// * Data change, children creation and deletion.
    /// * Session activities.
    ///
    /// # Cautions
    /// * Holds returned watcher without polling events may result in memory burst.
    /// * At the time of written, ZooKeeper [ZOOKEEPER-4466][] does not support oneshot and
    /// persistent watch on same path.
    /// * Persistent watch could lose events during reconnection due to [ZOOKEEPER-4698][].
    ///
    /// [ZOOKEEPER-4466]: https://issues.apache.org/jira/browse/ZOOKEEPER-4466
    /// [ZOOKEEPER-4698]: https://issues.apache.org/jira/browse/ZOOKEEPER-4698
    pub fn watch(&self, path: &str, mode: AddWatchMode) -> impl Future<Output = Result<PersistentWatcher>> + Send + '_ {
        Self::wait(self.watch_internally(path, mode))
    }

    fn watch_internally(
        &self,
        path: &str,
        mode: AddWatchMode,
    ) -> Result<impl Future<Output = Result<PersistentWatcher>> + Send + '_> {
        let chroot_path = self.validate_path(path)?;
        let proto_mode = proto::AddWatchMode::from(mode);
        let request = PersistentWatchRequest { path: chroot_path, mode: proto_mode.into() };
        let receiver = self.send_request(OpCode::AddWatch, &request);
        Ok(async move {
            let (_, watcher) = receiver.await?;
            Ok(watcher.into_persistent(&self.chroot))
        })
    }

    /// Syncs with ZooKeeper **leader**.
    ///
    /// # Cautions
    /// `sync + read` could not guarantee linearizable semantics as `sync` is not quorum acked and
    /// leader could change in between.
    ///
    /// See [ZOOKEEPER-1675][] and [ZOOKEEPER-2136][] for reference.
    ///
    /// [ZOOKEEPER-1675]: https://issues.apache.org/jira/browse/ZOOKEEPER-1675
    /// [ZOOKEEPER-2136]: https://issues.apache.org/jira/browse/ZOOKEEPER-2136
    pub fn sync(&self, path: &str) -> impl Future<Output = Result<()>> + Send + '_ {
        Self::wait(self.sync_internally(path))
    }

    fn sync_internally(&self, path: &str) -> Result<impl Future<Output = Result<()>>> {
        let chroot_path = self.validate_path(path)?;
        let request = SyncRequest { path: chroot_path };
        let receiver = self.send_request(OpCode::Sync, &request);
        Ok(async move {
            let (body, _) = receiver.await?;
            let mut buf = body.as_slice();
            record::unmarshal_entity::<&str>(&"server path", &mut buf)?;
            Ok(())
        })
    }

    /// Authenticates session using given scheme and auth identification. This affects only
    /// subsequent operations.
    ///
    /// # Errors
    /// * [Error::AuthFailed] if authentication failed.
    /// * Other terminal session errors.
    ///
    /// # Notable behaviors
    /// * Same auth will be resubmitted for authentication after session reestablished.
    /// * This method is resistant to temporary session unavailability, that means
    ///   [SessionState::Disconnected] will not end authentication.
    /// * It is ok to ignore resulting future of this method as request is sending synchronously
    ///   and auth failure will fail ZooKeeper session with [SessionState::AuthFailed].
    pub fn auth(&self, scheme: String, auth: Vec<u8>) -> impl Future<Output = Result<()>> + Send + '_ {
        let request = AuthPacket { scheme, auth };
        let receiver = self.send_request(OpCode::Auth, &request);
        async move {
            receiver.await?;
            Ok(())
        }
    }

    /// Gets all authentication informations attached to current session.
    ///
    /// # Requirements
    /// * ZooKeeper 3.7.0 and above
    ///
    /// # References
    /// * [ZOOKEEPER-3969][] Add whoami API and Cli command.
    ///
    /// [ZOOKEEPER-3969]: https://issues.apache.org/jira/browse/ZOOKEEPER-3969
    pub fn list_auth_users(&self) -> impl Future<Output = Result<Vec<AuthUser>>> + Send {
        let receiver = self.send_request(OpCode::WhoAmI, &());
        async move {
            let (body, _) = receiver.await?;
            let mut buf = body.as_slice();
            let authed_users = record::unmarshal_entity::<Vec<AuthUser>>(&"authed users", &mut buf)?;
            Ok(authed_users)
        }
    }

    /// Gets data for ZooKeeper config node, that is node with path "/zookeeper/config".
    pub fn get_config(&self) -> impl Future<Output = Result<(Vec<u8>, Stat)>> + Send {
        let result = self.get_data_internally(Chroot::default(), Self::CONFIG_NODE, false);
        Self::map_wait(result, |(data, stat, _)| (data, stat))
    }

    /// Gets stat and data for ZooKeeper config node, that is node with path "/zookeeper/config".
    pub fn get_and_watch_config(&self) -> impl Future<Output = Result<(Vec<u8>, Stat, OneshotWatcher)>> + Send {
        let result = self.get_data_internally(Chroot::default(), Self::CONFIG_NODE, true);
        Self::map_wait(result, |(data, stat, watcher)| (data, stat, watcher.into_oneshot(&OwnedChroot::default())))
    }

    /// Updates ZooKeeper ensemble.
    ///
    /// # Notable errors
    /// * [Error::ReconfigDisabled] if ZooKeeper reconfiguration is disabled.
    ///
    /// # References
    /// See [ZooKeeper Dynamic Reconfiguration](https://zookeeper.apache.org/doc/current/zookeeperReconfig.html).
    pub fn update_ensemble<'a, I: Iterator<Item = &'a str> + Clone>(
        &self,
        update: EnsembleUpdate<'a, I>,
        expected_zxid: Option<i64>,
    ) -> impl Future<Output = Result<(Vec<u8>, Stat)>> + Send {
        let request = ReconfigRequest { update, version: expected_zxid.unwrap_or(-1) };
        let receiver = self.send_request(OpCode::Reconfig, &request);
        async move {
            let (mut body, _) = receiver.await?;
            let mut buf = body.as_slice();
            let data: &str = record::unmarshal_entity(&"reconfig data", &mut buf)?;
            let stat = record::unmarshal_entity(&"reconfig stat", &mut buf)?;
            let data_len = data.len();
            body.truncate(data_len + 4);
            drop(body.drain(..4));
            Ok((body, stat))
        }
    }

    /// Creates a [MultiReader].
    pub fn new_multi_reader(&self) -> MultiReader<'_> {
        MultiReader::new(self)
    }

    /// Creates a [MultiWriter].
    pub fn new_multi_writer(&self) -> MultiWriter<'_> {
        MultiWriter::new(self)
    }

    /// Creates a [CheckWriter], which is similar to [MultiWriter] but additional path check when
    /// [CheckWriter::commit].
    pub fn new_check_writer(&self, path: &str, version: Option<i32>) -> Result<CheckWriter<'_>> {
        let mut writer = self.new_multi_writer();
        writer.add_check_version(path, version.unwrap_or(-1))?;
        Ok(CheckWriter { writer })
    }

    async fn create_lock(
        &self,
        prefix: LockPrefix<'_>,
        data: &[u8],
        options: LockOptions<'_>,
    ) -> Result<(String, usize)> {
        let kind = prefix.kind();
        let prefix = prefix.into();
        self.validate_sequential_path(&prefix)?;
        let (parent, _, _) = util::split_path(&prefix);
        let guard = LockingGuard { zk: self, prefix: &prefix, unique: kind.is_unique() };
        loop {
            let mut result = self.create(&prefix, data, &CreateMode::EphemeralSequential.with_acls(options.acls)).await;
            if result == Err(Error::NoNode) {
                if let Some(options) = &options.parent {
                    match Self::retry_on_connection_loss(|| self.mkdir_internally(parent, options)).await {
                        Ok(_) => continue,
                        Err(Error::NoNode) => result = Err(Error::NoNode),
                        Err(err) => return Err(err),
                    }
                }
            }
            let sequence = match result {
                Err(Error::ConnectionLoss) => {
                    if let Some(sequence) = self.find_lock(&prefix, kind).await? {
                        sequence
                    } else {
                        continue;
                    }
                },
                Err(err) => {
                    if err.has_no_data_change() {
                        std::mem::forget(guard);
                        return Err(err);
                    } else {
                        return Err(err);
                    }
                },
                Ok((_stat, sequence)) => sequence,
            };
            std::mem::forget(guard);
            let prefix_len = prefix.len();
            let mut path = prefix;
            write!(&mut path, "{}", sequence).unwrap();
            let sequence_len = path.len() - prefix_len;
            return Ok((path, sequence_len));
        }
    }

    async fn find_lock(&self, prefix: &str, kind: LockPrefixKind<'_>) -> Result<Option<CreateSequence>> {
        let (parent, tree, name) = util::split_path(prefix);
        let mut children = Self::retry_on_connection_loss(|| self.list_children(parent)).await?;
        if kind.is_unique() {
            let Some(i) = children.iter().position(|s| s.starts_with(name)) else {
                return Ok(None);
            };
            let sequence = Self::parse_sequence(&children[i], name)?;
            return Ok(Some(sequence));
        }
        children.retain(|s| s.starts_with(name));
        if children.is_empty() {
            return Ok(None);
        }
        for child in children.iter_mut() {
            child.insert_str(0, tree);
        }
        let results = Self::retry_on_connection_loss(|| {
            let mut reader = self.new_multi_reader();
            for child in children.iter() {
                reader.add_get_data(child).unwrap();
            }
            reader.commit()
        })
        .await?;
        for (i, result) in results.into_iter().enumerate() {
            let MultiReadResult::Data { stat, .. } = result else {
                // It could be Error::NoNode.
                continue;
            };
            if stat.ephemeral_owner == self.session_id().0 {
                let sequence = Self::parse_sequence(&children[i], name)?;
                return Ok(Some(sequence));
            }
        }
        Ok(None)
    }

    async fn wait_lock(&self, lock: &str, kind: LockPrefixKind<'_>, sequence_len: usize) -> Result<()> {
        let (parent, tree, this) = util::split_path(lock);
        loop {
            let mut children = Self::retry_on_connection_loss(|| self.list_children(parent)).await?;
            children.retain(|s| {
                s.len() >= sequence_len && kind.filter(s) && s[s.len() - sequence_len..].parse::<i32>().is_ok()
            });
            children.sort_unstable_by(|a, b| a[a.len() - sequence_len..].cmp(&b[b.len() - sequence_len..]));
            match children.binary_search_by(|a| a[a.len() - sequence_len..].cmp(&this[this.len() - sequence_len..])) {
                Ok(0) => return Ok(()),
                Ok(i) => {
                    let mut child = children.swap_remove(i - 1);
                    child.insert_str(0, tree);
                    let watcher = match Self::retry_on_connection_loss(|| self.get_and_watch_data(&child)).await {
                        Err(Error::NoNode) => continue,
                        Err(err) => return Err(err),
                        Ok((_data, _stat, watcher)) => watcher,
                    };
                    watcher.changed().await;
                },
                Err(_) => return Err(Error::RuntimeInconsistent),
            }
        }
    }

    /// Contends lock/leader/latch using given locking path pattern.
    ///
    /// # Notable errors
    /// * [Error::RuntimeInconsistent] if lock path is deleted during contention.
    /// * [Error::SessionExpired] if session expired before lock acquired.
    /// * [Error::NoNode] if ancestor nodes do not exist and no options to create them.
    /// * [Error::NoChildrenForEphemerals] if parent node is ephemeral.
    /// * [Error::InvalidAcl] if acl is invalid or empty.
    ///
    /// # Cancellation safety
    /// This method is cancellation safe, so you can free to cancel result future without fear to
    /// dangle lock. For example, a timed lock is easy to construct with `select!` and `sleep`.
    ///
    /// # Asynchronous ordering
    /// Comparing to other data operations, e.g. [Client::create], this operation is pure
    /// asynchronous, so there is no data order guarantee.
    ///
    /// # Error handling on [Error::ConnectionLoss]
    /// * If connection loss during lock path creation, this method will find out the created lock
    /// path if creation success by matching prefix for [LockPrefix::new_curator] or ephemeral
    /// owner for others.
    /// * Retry all other operations on connection loss.
    ///
    /// # Notable issues
    /// * [ZOOKEEPER-22][]: Automatic request retries on connection failover.
    ///
    /// # Notable docs
    /// * [ZooKeeper Recipes and Solutions](https://zookeeper.apache.org/doc/r3.8.2/recipes.html)
    ///
    /// [ZOOKEEPER-22]: https://issues.apache.org/jira/browse/ZOOKEEPER-22
    pub async fn lock(
        &self,
        prefix: LockPrefix<'_>,
        data: &[u8],
        options: impl Into<LockOptions<'_>>,
    ) -> Result<LockClient<'_>> {
        let options = options.into();
        if options.acls.is_empty() {
            return Err(Error::InvalidAcl);
        }
        let prefix_kind = prefix.kind();
        let (lock, sequence_len) = self.create_lock(prefix, data, options).await?;
        let client = LockClient { client: self, lock: Cow::from(lock) };
        match self.wait_lock(&client.lock, prefix_kind, sequence_len).await {
            Err(err @ (Error::RuntimeInconsistent | Error::SessionExpired)) => {
                std::mem::forget(client);
                Err(err)
            },
            Err(err) => Err(err),
            Ok(_) => Ok(client),
        }
    }
}

/// Options to cover [Acls] for lock path and [CreateOptions] for ancestor nodes if they don't
/// exist.
#[derive(Clone, Debug)]
pub struct LockOptions<'a> {
    acls: Acls<'a>,
    parent: Option<CreateOptions<'a>>,
}

impl<'a> LockOptions<'a> {
    pub fn new(acls: Acls<'a>) -> Self {
        Self { acls, parent: None }
    }

    /// Creates ancestor nodes if not exist using given options.
    ///
    /// # Notable errors
    /// * [Error::BadArguments] if [CreateMode] is ephemeral or sequential.
    /// * [Error::InvalidAcl] if acl is invalid or empty.
    pub fn with_ancestor_options(mut self, options: CreateOptions<'a>) -> Result<Self> {
        options.validate_as_directory()?;
        self.parent = Some(options);
        Ok(self)
    }
}

impl<'a> From<Acls<'a>> for LockOptions<'a> {
    fn from(acls: Acls<'a>) -> Self {
        LockOptions::new(acls)
    }
}

#[derive(Clone, Copy)]
enum LockPrefixKind<'a> {
    Curator { lock_name: &'a str },
    Custom { lock_name: &'a str },
    Shared { prefix: &'a str },
}

impl<'a> LockPrefixKind<'a> {
    fn filter(&self, name: &str) -> bool {
        match self {
            Self::Curator { lock_name } => name.contains(lock_name),
            Self::Custom { lock_name } => name.contains(lock_name),
            Self::Shared { prefix } => name.starts_with(prefix),
        }
    }

    fn is_unique(&self) -> bool {
        matches!(self, Self::Curator { .. })
    }
}

#[derive(Debug)]
enum LockPrefixInner<'a> {
    Curator { dir: &'a str, name: &'a str },
    Custom { prefix: String, name: &'a str },
    Shared { prefix: &'a str },
}

// It is intentional for this to not `Clone` as it is nonsense for [LockPrefix::new_custom], and I
// don't want to complicate this anymore, e.g. `LockPatternPrefix` and `LockCustomPrefix`. The side
// effect is that it is not easy to share `LockPrefix`. But, let the caller/tester to bore about that.
//
/// Prefix pattern for lock path creation.
///
/// This struct carries path prefix for ephemeral lock path, name filter for candidate contenders
/// and hint to find created lock path in case of [Error::ConnectionLoss].
#[derive(Debug)]
pub struct LockPrefix<'a> {
    inner: LockPrefixInner<'a>,
}

impl<'a> LockPrefix<'a> {
    /// Apache Curator compatible unique prefix pattern, the final lock path will be
    /// `{dir}/_c_{uuid}-{name}{ephemeral_sequence}`.
    ///
    /// # Notable lock names
    /// * `latch-` for `LeaderLatch`.
    /// * `lock-` for `LeaderSelector` and `InterProcessMutex`.
    pub fn new_curator(dir: &'a str, name: &'a str) -> Result<Self> {
        crate::util::validate_path(Chroot::default(), dir, false)?;
        if name.find('/').is_some() {
            return Err(Error::BadArguments(&"lock name must not contain /"));
        }
        Ok(Self { inner: LockPrefixInner::Curator { dir, name } })
    }

    /// Shared path prefix, the final lock path will be `{prefix}{ephemeral_sequence}`.
    ///
    /// # CAUTION
    /// All contenders share same prefix, so concurrent contenders must not share same session
    /// client. Otherwise, the lock could be ruined in case of [Error::ConnectionLoss] as there is
    /// no way to differentiate contenders using same session.
    ///
    /// # Notable usages
    /// * Uses "{dir}/n-" as `prefix` for ZooKeeper java client's [LeaderElectionSupport].
    ///
    /// [LeaderElectionSupport]: https://github.com/apache/zookeeper/blob/release-3.9.0/zookeeper-recipes/zookeeper-recipes-election/src/main/java/org/apache/zookeeper/recipes/leader/LeaderElectionSupport.java#L165
    pub fn new_shared(prefix: &'a str) -> Result<Self> {
        crate::util::validate_path(Chroot::default(), prefix, true)?;
        Ok(Self { inner: LockPrefixInner::Shared { prefix } })
    }

    /// Custom path prefix, the final lock path will be `{prefix}{ephemeral_sequence}`.
    ///
    /// # CAUTION
    /// Don't reuse same prefix among clients with same session. See [LockPrefix::new_shared] for
    /// details.
    ///
    /// # API
    /// It is intentional for `prefix` parameter to be `String` but not `impl Into<String>` nor
    /// `impl Into<Cow<'a, str>>`, so to attract attention for best wish.
    ///
    /// # Notable usages
    /// * Uses "{dir}/x-{session_id}-" as `prefix` and "x-" or "" as `name` for ZooKeeper java
    /// client's [WriteLock].
    ///
    /// [WriteLock]: https://github.com/apache/zookeeper/blob/release-3.9.0/zookeeper-recipes/zookeeper-recipes-lock/src/main/java/org/apache/zookeeper/recipes/lock/WriteLock.java#L212
    pub fn new_custom(prefix: String, name: &'a str) -> Result<Self> {
        crate::util::validate_path(Chroot::default(), &prefix, true)?;
        if !name.is_empty() {
            let (_dir, _tree, this) = util::split_path(&prefix);
            if !this.contains(name) {
                return Err(Error::BadArguments(&"lock path prefix must contain lock name"));
            }
        }
        Ok(Self { inner: LockPrefixInner::Custom { prefix, name } })
    }

    fn kind(&self) -> LockPrefixKind<'a> {
        match &self.inner {
            LockPrefixInner::Curator { name, .. } => LockPrefixKind::Curator { lock_name: name },
            LockPrefixInner::Shared { prefix } => {
                let (_parent, _tree, name) = util::split_path(prefix);
                LockPrefixKind::Shared { prefix: name }
            },
            LockPrefixInner::Custom { name, .. } => LockPrefixKind::Custom { lock_name: name },
        }
    }

    fn into(self) -> String {
        match self.inner {
            LockPrefixInner::Curator { dir, name } => format!("{}/_c_{}-{}", dir, uuid::Uuid::new_v4(), name),
            LockPrefixInner::Shared { prefix } => prefix.to_string(),
            LockPrefixInner::Custom { prefix, .. } => prefix,
        }
    }
}

struct LockingGuard<'a> {
    zk: &'a Client,
    prefix: &'a str,
    unique: bool,
}

impl Drop for LockingGuard<'_> {
    fn drop(&mut self) {
        self.zk.clone().delete_ephemeral_background(self.prefix.to_string(), self.unique);
    }
}

/// Guard client writes by owned ZooKeeper lock path which will be deleted in background when dropped.
#[derive(Debug)]
pub struct LockClient<'a> {
    client: &'a Client,
    lock: Cow<'a, str>,
}

impl<'a> LockClient<'a> {
    async fn resolve_one_write(
        future: impl Future<Output = std::result::Result<Vec<MultiWriteResult>, CheckWriteError>>,
    ) -> Result<MultiWriteResult> {
        let mut results = future.await?;
        Ok(results.remove(0))
    }

    /// Underlying client.
    pub fn client(&self) -> &'a Client {
        self.client
    }

    /// Lock path.
    ///
    /// Caller can watch this path to detect external lock path deletion. See also
    /// [ZOOKEEPER-91](https://issues.apache.org/jira/browse/ZOOKEEPER-91).
    pub fn lock_path(&self) -> &str {
        &self.lock
    }

    /// Similar to [Client::create] except [Error::RuntimeInconsistent] if lock lost.
    ///
    /// # BUG
    /// [Stat] will be [Stat::is_invalid] due to bugs in ZooKeeper version before 3.7.2, 3.8.2 and
    /// 3.9.0. See [ZOOKEEPER-4026][] for reference.
    ///
    /// [ZOOKEEPER-4026]: https://issues.apache.org/jira/browse/ZOOKEEPER-4026
    pub fn create(
        &self,
        path: &str,
        data: &[u8],
        options: &CreateOptions<'_>,
    ) -> impl Future<Output = Result<(Stat, CreateSequence)>> + Send + 'a {
        Client::wait(self.create_internally(path, data, options))
    }

    fn create_internally(
        &self,
        path: &str,
        data: &[u8],
        options: &CreateOptions<'_>,
    ) -> Result<impl Future<Output = Result<(Stat, CreateSequence)>> + Send + 'a> {
        let mut writer = self.client.new_check_writer(&self.lock, None)?;
        writer.add_create(path, data, options)?;
        let write = writer.commit();
        // XXX: Ideally, we should enforce strict ephemeral node check here, but that will
        // capture lifetime of `path` which fail to compile.
        //
        // See https://users.rust-lang.org/t/solved-future-lifetime-bounds/43664.
        let path_len = path.len();
        Ok(async move {
            let result = Self::resolve_one_write(write).await?;
            let (created_path, stat) = result.into_create()?;
            let sequence = if created_path.len() <= path_len {
                CreateSequence(-1)
            } else {
                Client::parse_sequence(&created_path, &created_path[..path_len])?
            };
            Ok((stat, sequence))
        })
    }

    /// Similar to [Client::set_data] except [Error::RuntimeInconsistent] if lock lost.
    pub fn set_data(
        &self,
        path: &str,
        data: &[u8],
        expected_version: Option<i32>,
    ) -> impl Future<Output = Result<Stat>> + Send + 'a {
        Client::wait(self.set_data_internally(path, data, expected_version))
    }

    fn set_data_internally(
        &self,
        path: &str,
        data: &[u8],
        expected_version: Option<i32>,
    ) -> Result<impl Future<Output = Result<Stat>> + Send + 'a> {
        let mut writer = self.new_check_writer();
        writer.add_set_data(path, data, expected_version)?;
        let write = writer.commit();
        Ok(async move {
            let result = Self::resolve_one_write(write).await?;
            let stat = result.into_set_data()?;
            Ok(stat)
        })
    }

    /// Similar to [Client::delete] except [Error::RuntimeInconsistent] if lock lost.
    pub fn delete(&self, path: &str, expected_version: Option<i32>) -> impl Future<Output = Result<()>> + Send + 'a {
        Client::wait(self.delete_internally(path, expected_version))
    }

    fn delete_internally(
        &self,
        path: &str,
        expected_version: Option<i32>,
    ) -> Result<impl Future<Output = Result<()>> + Send + 'a> {
        let mut writer = self.new_check_writer();
        writer.add_delete(path, expected_version)?;
        let write = writer.commit();
        Ok(async move {
            let result = Self::resolve_one_write(write).await?;
            result.into_delete()
        })
    }

    /// Similar to [Client::new_check_writer] with this lock path.
    pub fn new_check_writer(&self) -> CheckWriter<'a> {
        unsafe { self.client.new_check_writer(&self.lock, None).unwrap_unchecked() }
    }

    /// Converts to [OwnedLockClient].
    pub fn into_owned(self) -> OwnedLockClient {
        let client = self.client.clone();
        let mut drop = ManuallyDrop::new(self);
        let lock = std::mem::take(drop.lock.to_mut());
        OwnedLockClient { client: ManuallyDrop::new(client), lock }
    }
}

/// Deletes lock path in background.
impl Drop for LockClient<'_> {
    fn drop(&mut self) {
        let path = std::mem::take(self.lock.to_mut());
        let client = self.client.clone();
        client.delete_background(path);
    }
}

/// Owned version of [LockClient].
#[derive(Clone, Debug)]
pub struct OwnedLockClient {
    client: ManuallyDrop<Client>,
    lock: String,
}

impl OwnedLockClient {
    fn lock_client(&self) -> std::mem::ManuallyDrop<LockClient<'_>> {
        std::mem::ManuallyDrop::new(LockClient { client: &self.client, lock: Cow::from(&self.lock) })
    }

    /// Underlying client.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Same as [LockClient::lock_path].
    pub fn lock_path(&self) -> &str {
        &self.lock
    }

    /// Same as [LockClient::create].
    pub fn create<'a: 'f, 'b: 'f, 'f>(
        &'a self,
        path: &'b str,
        data: &[u8],
        options: &CreateOptions<'_>,
    ) -> impl Future<Output = Result<(Stat, CreateSequence)>> + Send + 'f {
        self.lock_client().create(path, data, options)
    }

    /// Same as [LockClient::set_data].
    pub fn set_data(
        &self,
        path: &str,
        data: &[u8],
        expected_version: Option<i32>,
    ) -> impl Future<Output = Result<Stat>> + Send + '_ {
        self.lock_client().set_data(path, data, expected_version)
    }

    /// Same as [LockClient::delete].
    pub fn delete(&self, path: &str, expected_version: Option<i32>) -> impl Future<Output = Result<()>> + Send + '_ {
        self.lock_client().delete(path, expected_version)
    }

    /// Same as [LockClient::new_check_writer].
    pub fn new_check_writer(&self) -> CheckWriter<'_> {
        unsafe { self.client.new_check_writer(&self.lock, None).unwrap_unchecked() }
    }
}

/// Deletes lock path in background.
impl Drop for OwnedLockClient {
    fn drop(&mut self) {
        let client = unsafe { ManuallyDrop::take(&mut self.client) };
        let path = std::mem::take(&mut self.lock);
        client.delete_background(path);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub(crate) struct Version(u32, u32, u32);

/// A builder for [Client].
#[derive(Clone, Debug)]
pub struct Connector {
    tls: Option<TlsOptions>,
    authes: Vec<AuthPacket>,
    session: Option<SessionInfo>,
    readonly: bool,
    detached: bool,
    fail_eagerly: bool,
    server_version: Version,
    session_timeout: Duration,
    connection_timeout: Duration,
}

/// Builder for [Client] with more options than [Client::connect].
impl Connector {
    fn new() -> Self {
        Self {
            tls: None,
            authes: Default::default(),
            session: None,
            readonly: false,
            detached: false,
            fail_eagerly: false,
            server_version: Version(u32::MAX, u32::MAX, u32::MAX),
            session_timeout: Duration::ZERO,
            connection_timeout: Duration::ZERO,
        }
    }

    /// Specifies target session timeout to negotiate with ZooKeeper server.
    ///
    /// Defaults to 6s.
    pub fn session_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.session_timeout = timeout;
        self
    }

    /// Specifies idle timeout to conclude a connection as loss.
    ///
    /// Defaults to `2/5` of session timeout.
    pub fn connection_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.connection_timeout = timeout;
        self
    }

    /// Specifies whether readonly session is allowed.
    pub fn readonly(&mut self, readonly: bool) -> &mut Self {
        self.readonly = readonly;
        self
    }

    /// Specifies auth info for given authentication scheme.
    pub fn auth(&mut self, scheme: String, auth: Vec<u8>) -> &mut Self {
        self.authes.push(AuthPacket { scheme, auth });
        self
    }

    /// Specifies session to reestablish.
    pub fn session(&mut self, session: SessionInfo) -> &mut Self {
        self.session = Some(session);
        self
    }

    /// Specifies target server version of ZooKeeper cluster.
    ///
    /// Client will issue server compatible protocol to avoid [Error::Unimplemented] for some
    /// operations. See [Client::create] for an example.
    ///
    /// See [ZOOKEEPER-1381][] and [ZOOKEEPER-3762][] for references.
    ///
    /// [ZOOKEEPER-1381]: https://issues.apache.org/jira/browse/ZOOKEEPER-1381
    /// [ZOOKEEPER-3762]: https://issues.apache.org/jira/browse/ZOOKEEPER-3762
    pub fn server_version(&mut self, major: u32, minor: u32, patch: u32) -> &mut Self {
        self.server_version = Version(major, minor, patch);
        self
    }

    /// Detaches created session so it will not be closed after all client instances dropped.
    pub fn detached(&mut self) -> &mut Self {
        self.detached = true;
        self
    }

    /// Specifies tls options for connections to ZooKeeper.
    pub fn tls(&mut self, options: TlsOptions) -> &mut Self {
        self.tls = Some(options);
        self
    }

    /// Fail session establishment eagerly with [Error::NoHosts] when all hosts has been tried.
    ///
    /// This permits fail-fast without wait up to [Self::session_timeout] in [Self::connect]. This
    /// is not suitable for situations where ZooKeeper cluster is accessible via a single virtual IP.
    pub fn fail_eagerly(&mut self) -> &mut Self {
        self.fail_eagerly = true;
        self
    }

    async fn connect_internally(&mut self, secure: bool, cluster: &str) -> Result<Client> {
        let (endpoints, chroot) = endpoint::parse_connect_string(cluster, secure)?;
        if let Some(session) = self.session.as_ref() {
            if session.is_readonly() {
                return Err(Error::new_other(
                    format!("can't reestablish readonly and hence local session {}", session.id()),
                    None,
                ));
            }
        }
        if self.session_timeout < Duration::ZERO {
            return Err(Error::BadArguments(&"session timeout must not be negative"));
        } else if self.connection_timeout < Duration::ZERO {
            return Err(Error::BadArguments(&"connection timeout must not be negative"));
        }
        let tls_config = self.tls.take().unwrap_or_default().into_config()?;
        let (mut session, state_receiver) = Session::new(
            self.session.take(),
            &self.authes,
            self.readonly,
            self.detached,
            tls_config,
            self.session_timeout,
            self.connection_timeout,
        );
        let mut endpoints = IterableEndpoints::from(endpoints.as_slice());
        endpoints.reset();
        if !self.fail_eagerly {
            endpoints.cycle();
        }
        let mut buf = Vec::with_capacity(4096);
        let mut connecting_depot = Depot::for_connecting();
        let conn = session.start(&mut endpoints, &mut buf, &mut connecting_depot).await?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let session_info = session.session.clone();
        let session_timeout = session.session_timeout;
        tokio::spawn(async move {
            session.serve(endpoints, conn, buf, connecting_depot, receiver).await;
        });
        let client =
            Client::new(chroot.to_owned(), self.server_version, session_info, session_timeout, sender, state_receiver);
        Ok(client)
    }

    /// Connects to ZooKeeper cluster.
    ///
    /// Same to [Self::connect] except that `server1` will use tls encrypted protocol given
    /// the connection string `server1,tcp://server2,tcp+tls://server3`.
    pub async fn secure_connect(&mut self, cluster: &str) -> Result<Client> {
        self.connect_internally(true, cluster).await
    }

    /// Connects to ZooKeeper cluster.
    ///
    /// Parameter `cluster` specifies connection string to ZooKeeper cluster. It has same syntax as
    /// Java client except that you can specifies protocol for server individually. For example,
    /// `server1,tcp://server2,tcp+tls://server3`. This claims that `server1` and `server2` use
    /// plaintext protocol, while `server3` uses tls encrypted protocol.
    ///
    /// # Notable errors
    /// * [Error::NoHosts] if no host is available and [Self::fail_eagerly] is turn on
    /// * [Error::SessionExpired] if specified session expired
    /// * [Error::Timeout] if no session established with in approximate [Self::session_timeout]
    ///
    /// # Notable behaviors
    /// The state of this connector is undefined after connection attempt no matter whether it is
    /// success or not.
    pub async fn connect(&mut self, cluster: &str) -> Result<Client> {
        self.connect_internally(false, cluster).await
    }
}

/// Builder for [Client] with more options than [Client::connect].
#[derive(Clone, Debug)]
pub struct ClientBuilder {
    connector: Connector,
}

impl ClientBuilder {
    fn new() -> Self {
        Self { connector: Connector::new() }
    }

    /// Specifies target session timeout to negotiate with ZooKeeper server.
    ///
    /// Defaults to 6s.
    pub fn with_session_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.connector.session_timeout(timeout);
        self
    }

    /// Specifies idle timeout to conclude a connection as loss.
    ///
    /// Defaults to `2/5` of session timeout.
    pub fn with_connection_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.connector.connection_timeout(timeout);
        self
    }

    /// Specifies whether readonly session is allowed.
    pub fn with_readonly(&mut self, readonly: bool) -> &mut ClientBuilder {
        self.connector.readonly = readonly;
        self
    }

    /// Specifies auth info for given authentication scheme.
    pub fn with_auth(&mut self, scheme: String, auth: Vec<u8>) -> &mut ClientBuilder {
        self.connector.auth(scheme, auth);
        self
    }

    /// Specifies client assumed server version of ZooKeeper cluster.
    ///
    /// Client will issue server compatible protocol to avoid [Error::Unimplemented] for some
    /// operations. See [Client::create] for an example.
    ///
    /// See [ZOOKEEPER-1381][] and [ZOOKEEPER-3762][] for references.
    ///
    /// [ZOOKEEPER-1381]: https://issues.apache.org/jira/browse/ZOOKEEPER-1381
    /// [ZOOKEEPER-3762]: https://issues.apache.org/jira/browse/ZOOKEEPER-3762
    pub fn assume_server_version(&mut self, major: u32, minor: u32, patch: u32) -> &mut Self {
        self.connector.server_version(major, minor, patch);
        self
    }

    /// Detaches creating session so it will not be closed after all client instances dropped.
    pub fn detach(&mut self) -> &mut Self {
        self.connector.detached();
        self
    }

    /// Connects to ZooKeeper cluster.
    ///
    /// # Notable errors
    /// * [Error::NoHosts] if no host is available
    /// * [Error::SessionExpired] if specified session expired
    pub async fn connect(&mut self, cluster: &str) -> Result<Client> {
        self.connector.connect(cluster).await
    }
}

trait MultiBuffer {
    fn buffer(&mut self) -> &mut Vec<u8>;

    fn op_code() -> OpCode;

    fn build_request(&mut self) -> MarshalledRequest {
        let buffer = self.buffer();
        if buffer.is_empty() {
            return Default::default();
        }
        let header = MultiHeader { op: OpCode::Error, done: true, err: -1 };
        buffer.append_record(&header);
        buffer.finish();
        MarshalledRequest(std::mem::take(buffer))
    }

    fn add_operation(&mut self, op: OpCode, request: &impl Record) {
        let buffer = self.buffer();
        if buffer.is_empty() {
            let n = RequestHeader::record_len() + MultiHeader::record_len() + request.serialized_len();
            buffer.prepare_and_reserve(n);
            buffer.append_record(&RequestHeader::with_code(Self::op_code()));
        }
        let header = MultiHeader { op, done: false, err: -1 };
        self.buffer().append_record2(&header, request);
    }
}

/// Individual result for one operation in [MultiReader].
#[non_exhaustive]
#[derive(Debug)]
pub enum MultiReadResult {
    /// Response for [`MultiReader::add_get_data`].
    Data { data: Vec<u8>, stat: Stat },

    /// Response for [`MultiReader::add_get_children`].
    Children { children: Vec<String> },

    /// Response for individual error.
    Error { err: Error },
}

/// MultiReader commits multiple read operations in one request to achieve snapshot like semantics.
pub struct MultiReader<'a> {
    client: &'a Client,
    buf: Vec<u8>,
}

impl MultiBuffer for MultiReader<'_> {
    fn buffer(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    fn op_code() -> OpCode {
        OpCode::MultiRead
    }
}

impl<'a> MultiReader<'a> {
    fn new(client: &'a Client) -> MultiReader<'a> {
        MultiReader { client, buf: Default::default() }
    }

    /// Adds operation to get stat and data for node with given path.
    ///
    /// See [Client::get_data] for more details.
    pub fn add_get_data(&mut self, path: &str) -> Result<()> {
        let chroot_path = self.client.validate_path(path)?;
        let request = GetRequest { path: chroot_path, watch: false };
        self.add_operation(OpCode::GetData, &request);
        Ok(())
    }

    /// Adds operation to get stat and children for node with given path.
    ///
    /// See [Client::get_children] for more details.
    pub fn add_get_children(&mut self, path: &str) -> Result<()> {
        let chroot_path = self.client.validate_path(path)?;
        let request = GetChildrenRequest { path: chroot_path, watch: false };
        self.add_operation(OpCode::GetChildren, &request);
        Ok(())
    }

    /// Commits multiple operations in one request to reach consistent read.
    ///
    /// # Notable behaviors
    /// Individual errors(e.g. [Error::NoNode]) are reported individually through [MultiReadResult::Error].
    pub fn commit(&mut self) -> impl Future<Output = Result<Vec<MultiReadResult>>> + Send + 'a {
        let request = self.build_request();
        Client::resolve(self.commit_internally(request))
    }

    fn commit_internally(
        &self,
        request: MarshalledRequest,
    ) -> Result<Either<impl Future<Output = Result<Vec<MultiReadResult>>> + Send + 'a, Vec<MultiReadResult>>> {
        if request.is_empty() {
            return Ok(Right(Vec::default()));
        }
        let receiver = self.client.send_marshalled_request(request);
        Ok(Left(async move {
            let (body, _) = receiver.await?;
            let response = record::unmarshal::<Vec<MultiReadResponse>>(&mut body.as_slice())?;
            let mut results = Vec::with_capacity(response.len());
            for result in response {
                match result {
                    MultiReadResponse::Data { data, stat } => results.push(MultiReadResult::Data { data, stat }),
                    MultiReadResponse::Children { children } => results.push(MultiReadResult::Children { children }),
                    MultiReadResponse::Error(err) => results.push(MultiReadResult::Error { err }),
                }
            }
            Ok(results)
        }))
    }

    /// Clears collected operations.
    pub fn abort(&mut self) {
        self.buf.clear();
    }
}

/// Individual result for one operation in [MultiWriter].
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq)]
pub enum MultiWriteResult {
    /// Response for [MultiWriter::add_check_version].
    Check,

    /// Response for [MultiWriter::add_delete].
    Delete,

    /// Response for [MultiWriter::add_create].
    Create {
        /// Path of created znode.
        path: String,

        /// Stat for newly created node which could be [Stat::is_invalid] due to bugs in ZooKeeper server.
        ///
        /// See [ZOOKEEPER-4026][] and [ZOOKEEPER-4667][] for reference.
        ///
        /// [ZOOKEEPER-4026]: https://issues.apache.org/jira/browse/ZOOKEEPER-4026
        /// [ZOOKEEPER-4667]: https://issues.apache.org/jira/browse/ZOOKEEPER-4667
        stat: Stat,
    },

    /// Response for [MultiWriter::add_set_data].
    SetData {
        /// Updated stat.
        stat: Stat,
    },
}

impl MultiWriteResult {
    fn kind(&self) -> &'static str {
        match self {
            MultiWriteResult::Check => "MultiWriteResult::Check",
            MultiWriteResult::Create { .. } => "MultiWriteResult::Create",
            MultiWriteResult::Delete => "MultiWriteResult::Delete",
            MultiWriteResult::SetData { .. } => "MultiWriteResult::SetData",
        }
    }

    fn into_check(self) -> Result<()> {
        match self {
            MultiWriteResult::Check => Ok(()),
            _ => Err(Error::UnexpectedError(format!("expect MultiWriteResult::Check, got {}", self.kind()))),
        }
    }

    fn into_create(self) -> Result<(String, Stat)> {
        match self {
            MultiWriteResult::Create { path, stat } => Ok((path, stat)),
            _ => Err(Error::UnexpectedError(format!("expect MultiWriteResult::Create, got {}", self.kind()))),
        }
    }

    fn into_set_data(self) -> Result<Stat> {
        match self {
            MultiWriteResult::SetData { stat } => Ok(stat),
            _ => Err(Error::UnexpectedError(format!("expect MultiWriteResult::SetData, got {}", self.kind()))),
        }
    }

    fn into_delete(self) -> Result<()> {
        match self {
            MultiWriteResult::Delete => Ok(()),
            _ => Err(Error::UnexpectedError(format!("expect MultiWriteResult::Delete, got {}", self.kind()))),
        }
    }
}

/// Error for [MultiWriter::commit].
#[derive(Error, Clone, Debug, PartialEq, Eq)]
pub enum MultiWriteError {
    #[error("{source}")]
    RequestFailed {
        #[from]
        source: Error,
    },

    #[error("operation at index {index} failed: {source}")]
    OperationFailed { index: usize, source: Error },
}

impl From<MultiWriteError> for Error {
    fn from(err: MultiWriteError) -> Self {
        match err {
            MultiWriteError::RequestFailed { source } => source,
            MultiWriteError::OperationFailed { source, .. } => source,
        }
    }
}

/// Error for [CheckWriter::commit].
#[derive(Error, Clone, Debug, PartialEq, Eq)]
pub enum CheckWriteError {
    #[error("request failed: {source}")]
    RequestFailed {
        #[from]
        source: Error,
    },

    #[error("path check failed: {source}")]
    CheckFailed { source: Error },

    #[error("operation at index {index} failed: {source}")]
    OperationFailed { index: usize, source: Error },
}

impl From<MultiWriteError> for CheckWriteError {
    fn from(err: MultiWriteError) -> Self {
        match err {
            MultiWriteError::RequestFailed { source } => CheckWriteError::RequestFailed { source },
            MultiWriteError::OperationFailed { index: 0, source } => CheckWriteError::CheckFailed { source },
            MultiWriteError::OperationFailed { index, source } => {
                CheckWriteError::OperationFailed { index: index - 1, source }
            },
        }
    }
}

impl From<CheckWriteError> for Error {
    fn from(err: CheckWriteError) -> Self {
        match err {
            CheckWriteError::RequestFailed { source } => source,
            CheckWriteError::CheckFailed { source: Error::NoNode | Error::BadVersion } => Error::RuntimeInconsistent,
            CheckWriteError::CheckFailed { source } => source,
            CheckWriteError::OperationFailed { source, .. } => source,
        }
    }
}

/// Similar to [MultiWriter] expect for [CheckWriter::commit].
pub struct CheckWriter<'a> {
    writer: MultiWriter<'a>,
}

impl<'a> CheckWriter<'a> {
    /// Same as [MultiWriter::add_check_version].
    pub fn add_check_version(&mut self, path: &str, version: i32) -> Result<()> {
        self.writer.add_check_version(path, version)
    }

    /// Same as [MultiWriter::add_create].
    pub fn add_create(&mut self, path: &str, data: &[u8], options: &CreateOptions<'_>) -> Result<()> {
        self.writer.add_create(path, data, options)
    }

    /// Same as [MultiWriter::add_set_data].
    pub fn add_set_data(&mut self, path: &str, data: &[u8], expected_version: Option<i32>) -> Result<()> {
        self.writer.add_set_data(path, data, expected_version)
    }

    /// Same as [MultiWriter::add_delete].
    pub fn add_delete(&mut self, path: &str, expected_version: Option<i32>) -> Result<()> {
        self.writer.add_delete(path, expected_version)
    }

    /// Similar to [MultiWriter::commit] except independent path check error.
    pub fn commit(
        mut self,
    ) -> impl Future<Output = std::result::Result<Vec<MultiWriteResult>, CheckWriteError>> + Send + 'a {
        let commit = self.writer.commit();
        async move {
            let mut results = commit.await?;
            if results.is_empty() {
                Err(CheckWriteError::RequestFailed {
                    source: Error::UnexpectedError("expect path check, got none".to_string()),
                })
            } else {
                results.remove(0).into_check()?;
                Ok(results)
            }
        }
    }
}

/// MultiWriter commits write and condition check operations in one request to achieve transaction like semantics.
pub struct MultiWriter<'a> {
    client: &'a Client,
    buf: Vec<u8>,
}

impl MultiBuffer for MultiWriter<'_> {
    fn buffer(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    fn op_code() -> OpCode {
        OpCode::Multi
    }
}

impl<'a> MultiWriter<'a> {
    fn new(client: &'a Client) -> MultiWriter<'a> {
        MultiWriter { client, buf: Default::default() }
    }

    /// Adds operation to check version for node with given path.
    ///
    /// # Notable behaviors
    /// Effects of changes to data of given path in preceding operations affect this operation.
    pub fn add_check_version(&mut self, path: &str, version: i32) -> Result<()> {
        let chroot_path = self.client.validate_path(path)?;
        let request = CheckVersionRequest { path: chroot_path, version };
        self.add_operation(OpCode::Check, &request);
        Ok(())
    }

    /// Adds operation to create node with given path and data.
    ///
    /// See [Client::create] for more details.
    ///
    /// # Notable behaviors
    /// [MultiWriteResult::Create::stat] could be [Stat::is_invalid] due to bugs in ZooKeeper server.
    /// See [ZOOKEEPER-4026][] and [ZOOKEEPER-4667][] for reference.
    ///
    /// [ZOOKEEPER-4026]: https://issues.apache.org/jira/browse/ZOOKEEPER-4026
    /// [ZOOKEEPER-4667]: https://issues.apache.org/jira/browse/ZOOKEEPER-4667
    pub fn add_create(&mut self, path: &str, data: &[u8], options: &CreateOptions<'_>) -> Result<()> {
        options.validate()?;
        let ttl = options.ttl.map(|ttl| ttl.as_millis() as i64).unwrap_or(0);
        let create_mode = options.mode;
        let sequential = create_mode.is_sequential();
        let chroot_path =
            if sequential { self.client.validate_sequential_path(path)? } else { self.client.validate_path(path)? };
        let op_code = if ttl != 0 {
            OpCode::CreateTtl
        } else if create_mode.is_container() {
            OpCode::CreateContainer
        } else {
            OpCode::Create2
        };
        let flags = create_mode.as_flags(ttl != 0);
        let request = CreateRequest { path: chroot_path, data, acls: options.acls, flags, ttl };
        self.add_operation(op_code, &request);
        Ok(())
    }

    /// Adds operation to set data for node with given path.
    ///
    /// See [Client::set_data] for more details.
    pub fn add_set_data(&mut self, path: &str, data: &[u8], expected_version: Option<i32>) -> Result<()> {
        let chroot_path = self.client.validate_path(path)?;
        let request = SetDataRequest { path: chroot_path, data, version: expected_version.unwrap_or(-1) };
        self.add_operation(OpCode::SetData, &request);
        Ok(())
    }

    /// Adds operation to delete node with given path.
    ///
    /// See [Client::delete] for more details.
    pub fn add_delete(&mut self, path: &str, expected_version: Option<i32>) -> Result<()> {
        let chroot_path = self.client.validate_path(path)?;
        if chroot_path.is_root() {
            return Err(Error::BadArguments(&"can not delete root node"));
        }
        let request = DeleteRequest { path: chroot_path, version: expected_version.unwrap_or(-1) };
        self.add_operation(OpCode::Delete, &request);
        Ok(())
    }

    /// Commits multiple operations in one request to write transactionally.
    ///
    /// # Notable behaviors
    /// Failure of individual operation will fail whole request and commit no effect in server.
    ///
    /// # Notable errors
    /// * [Error::BadVersion] if check version failed.
    pub fn commit(
        &mut self,
    ) -> impl Future<Output = std::result::Result<Vec<MultiWriteResult>, MultiWriteError>> + Send + 'a {
        let request = self.build_request();
        Client::resolve(self.commit_internally(request))
    }

    fn commit_internally(
        &self,
        request: MarshalledRequest,
    ) -> std::result::Result<
        Either<
            impl Future<Output = std::result::Result<Vec<MultiWriteResult>, MultiWriteError>> + Send + 'a,
            Vec<MultiWriteResult>,
        >,
        MultiWriteError,
    > {
        if request.is_empty() {
            return Ok(Right(Vec::default()));
        }
        let receiver = self.client.send_marshalled_request(request);
        let client = self.client;
        Ok(Left(async move {
            let (body, _) = receiver.await?;
            let response = record::unmarshal::<Vec<MultiWriteResponse>>(&mut body.as_slice())?;
            let failed = response.first().map(|r| matches!(r, MultiWriteResponse::Error(_))).unwrap_or(false);
            let mut results = if failed { Vec::new() } else { Vec::with_capacity(response.len()) };
            for (index, result) in response.into_iter().enumerate() {
                match result {
                    MultiWriteResponse::Check => results.push(MultiWriteResult::Check),
                    MultiWriteResponse::Delete => results.push(MultiWriteResult::Delete),
                    MultiWriteResponse::Create { mut path, stat } => {
                        path = util::strip_root_path(path, client.chroot.root())?;
                        results.push(MultiWriteResult::Create { path: path.to_string(), stat });
                    },
                    MultiWriteResponse::SetData { stat } => results.push(MultiWriteResult::SetData { stat }),
                    MultiWriteResponse::Error(Error::UnexpectedErrorCode(0)) => {},
                    MultiWriteResponse::Error(err) => {
                        return Err(MultiWriteError::OperationFailed { index, source: err })
                    },
                }
            }
            Ok(results)
        }))
    }

    /// Clears collected operations.
    pub fn abort(&mut self) {
        self.buf.clear();
    }
}

#[cfg(test)]
mod tests {
    use assertor::*;

    use super::*;

    #[test]
    fn test_create_options_validate() {
        assert_that!(CreateMode::Persistent.with_acls(Acls::new(Default::default())).validate().unwrap_err())
            .is_equal_to(Error::InvalidAcl);

        let acls = Acls::anyone_all();

        assert_that!(CreateMode::Ephemeral.with_acls(acls).with_ttl(Duration::from_secs(1)).validate().unwrap_err())
            .is_equal_to(Error::BadArguments(&"ttl can only be specified with persistent node"));

        assert_that!(CreateMode::Persistent.with_acls(acls).with_ttl(Duration::ZERO).validate().unwrap_err())
            .is_equal_to(Error::BadArguments(&"ttl is zero"));

        assert_that!(CreateMode::Persistent
            .with_acls(acls)
            .with_ttl(Duration::from_millis(0x01FFFFFFFFFF))
            .validate()
            .unwrap_err())
        .is_equal_to(Error::BadArguments(&"ttl cannot larger than 1099511627775"));

        assert_that!(CreateMode::Persistent.with_acls(acls).with_ttl(Duration::from_secs(5)).validate())
            .is_equal_to(Ok(()));
    }

    #[test]
    fn test_lock_options_with_ancestor_options() {
        let options = LockOptions::new(Acls::anyone_all());
        assert_that!(options
            .clone()
            .with_ancestor_options(CreateMode::Ephemeral.with_acls(Acls::anyone_all()))
            .unwrap_err())
        .is_equal_to(Error::BadArguments(&"directory node must not be ephemeral"));
        assert_that!(options
            .with_ancestor_options(CreateMode::PersistentSequential.with_acls(Acls::anyone_all()))
            .unwrap_err())
        .is_equal_to(Error::BadArguments(&"directory node must not be sequential"));
    }

    #[test_log::test(tokio::test)]
    async fn session_last_zxid_seen() {
        use testcontainers::clients::Cli as DockerCli;
        use testcontainers::core::{Healthcheck, WaitFor};
        use testcontainers::images::generic::GenericImage;

        let healthcheck = Healthcheck::default()
            .with_cmd(["./bin/zkServer.sh", "status"].iter())
            .with_interval(Duration::from_secs(2))
            .with_retries(60);
        let image =
            GenericImage::new("zookeeper", "3.9.0").with_healthcheck(healthcheck).with_wait_for(WaitFor::Healthcheck);
        let docker = DockerCli::default();
        let container = docker.run(image);
        let endpoint = format!("127.0.0.1:{}", container.get_host_port(2181));

        let client1 = Client::connector().detached().connect(&endpoint).await.unwrap();
        client1.create("/n1", b"", &CreateMode::Persistent.with_acls(Acls::anyone_all())).await.unwrap();

        let mut session = client1.into_session();

        // Fail to connect with large zxid.
        session.last_zxid = i64::MAX;
        assert_that!(Client::connector().fail_eagerly().session(session.clone()).connect(&endpoint).await.unwrap_err())
            .is_equal_to(Error::NoHosts);

        // Succeed to connect with small zxid.
        session.last_zxid = 0;
        let client2 = Client::connector().fail_eagerly().session(session.clone()).connect(&endpoint).await.unwrap();
        client2.create("/n2", b"", &CreateMode::Persistent.with_acls(Acls::anyone_all())).await.unwrap();
    }
}
