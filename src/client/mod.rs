mod watcher;

use std::future::Future;
use std::time::Duration;

use const_format::formatcp;
use thiserror::Error;
use tokio::sync::{mpsc, watch};

pub use self::watcher::{OneshotWatcher, PersistentWatcher, StateWatcher};
use super::session::{Depot, MarshalledRequest, Session, SessionOperation, WatchReceiver, PASSWORD_LEN};
use crate::acl::{Acl, AuthUser};
use crate::chroot::{Chroot, ChrootPath, OwnedChroot};
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
pub use crate::session::{EventType, SessionId, SessionState, WatchedEvent};
use crate::util::{self, Ref as _};

type Result<T> = std::result::Result<T, Error>;

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
    fn is_sequential(self) -> bool {
        self == CreateMode::PersistentSequential || self == CreateMode::EphemeralSequential
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

/// Options for node creation.
pub struct CreateOptions<'a> {
    mode: CreateMode,
    acls: &'a [Acl],
    ttl: Option<Duration>,
}

// Five bytes are avaiable for milliseconds. See javadoc of EphemeralType in ZooKeeper for reference.
//
// https://github.com/apache/zookeeper/blob/ebcf18e52fa095773429348ce495d59c896f4a26/zookeeper-server/src/main/java/org/apache/zookeeper/server/EphemeralType.java#L46
const TTL_MAX_MILLIS: u128 = 0x00FFFFFFFFFF;

impl<'a> CreateOptions<'a> {
    /// Constructs options with specified create mode and acls.
    pub fn new(mode: CreateMode, acls: &'a [Acl]) -> CreateOptions<'a> {
        CreateOptions { mode, acls, ttl: None }
    }

    /// Specifies ttl for persistent node.
    pub fn with_ttl(&'a mut self, ttl: Duration) -> &'a mut Self {
        self.ttl = Some(ttl);
        self
    }

    fn validate(&'a self) -> Result<()> {
        if let Some(ref ttl) = self.ttl {
            if self.mode != CreateMode::Persistent && self.mode != CreateMode::PersistentSequential {
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
}

/// Thin wrapper to encapsulate sequential node's sequence number.
///
/// It prints in ten decimal digits with possible leading padding 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CreateSequence(pub i32);

impl std::fmt::Display for CreateSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:010}", self.0)
    }
}

/// Client encapsulates ZooKeeper session to interact with ZooKeeper cluster.
///
/// Besides semantic errors, node operations could also fail due to cluster availability and
/// limitations, e.g. [Error::ConnectionLoss], [Error::QuotaExceeded] and so on.
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
    session: (SessionId, Vec<u8>),
    session_timeout: Duration,
    requester: mpsc::UnboundedSender<SessionOperation>,
    state_watcher: StateWatcher,
}

impl Client {
    const CONFIG_NODE: &'static str = "/zookeeper/config";

    /// Connects to ZooKeeper cluster.
    pub async fn connect(cluster: &str) -> Result<Self> {
        Self::builder().connect(cluster).await
    }

    /// Creates a builder with configurable options in connecting to ZooKeeper cluster.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub(crate) fn new(
        chroot: OwnedChroot,
        session: (SessionId, Vec<u8>),
        timeout: Duration,
        requester: mpsc::UnboundedSender<SessionOperation>,
        state_receiver: watch::Receiver<SessionState>,
    ) -> Client {
        let state_watcher = StateWatcher::new(state_receiver);
        Client { chroot, session, session_timeout: timeout, requester, state_watcher }
    }

    fn validate_path<'a>(&'a self, path: &'a str) -> Result<ChrootPath<'a>> {
        ChrootPath::new(self.chroot.as_ref(), path, false)
    }

    fn validate_sequential_path<'a>(&'a self, path: &'a str) -> Result<ChrootPath<'a>> {
        ChrootPath::new(self.chroot.as_ref(), path, true)
    }

    /// ZooKeeper session id.
    pub fn session_id(&self) -> SessionId {
        self.session.0
    }

    /// Session password.
    pub fn session_password(&self) -> &[u8] {
        self.session.1.as_slice()
    }

    /// Consumes this instance into session info.
    pub fn into_session(self) -> (SessionId, Vec<u8>) {
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
    pub fn chroot(mut self, path: &str) -> std::result::Result<Client, Client> {
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

    async fn wait<T, F>(result: Result<F>) -> Result<T>
    where
        F: Future<Output = Result<T>>, {
        match result {
            Err(err) => Err(err),
            Ok(future) => future.await,
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

    fn parse_sequence(client_path: &str, path: &str) -> Result<CreateSequence> {
        if let Some(sequence_path) = client_path.strip_prefix(path) {
            match sequence_path.parse::<i32>() {
                Err(_) => Err(Error::UnexpectedError(format!("sequential node get no i32 path {}", client_path))),
                Ok(i) => Ok(CreateSequence(i)),
            }
        } else {
            Err(Error::UnexpectedError(format!(
                "sequential path {} does not contain prefix path {}",
                client_path, path
            )))
        }
    }

    /// Creates node with given path and data.
    ///
    /// # Notable errors
    /// * [Error::NodeExists] if a node with same path already exists.
    /// * [Error::NoNode] if parent node does not exist.
    /// * [Error::NoChildrenForEphemerals] if parent node is ephemeral.
    /// * [Error::InvalidAcl] if acl is invalid or empty.
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
        let ttl = options.ttl.map(|ttl| ttl.as_millis() as i64).unwrap_or(0);
        let op_code = if ttl != 0 {
            OpCode::CreateTtl
        } else if create_mode.is_container() {
            OpCode::CreateContainer
        } else {
            OpCode::Create2
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
            let stat = record::unmarshal::<Stat>(&mut buf)?;
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
        Self::map_wait(result, |(data, stat, watcher)| (data, stat, watcher.into_oneshot(self.chroot.root())))
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
        Self::map_wait(result, |(stat, watcher)| (stat, watcher.into_oneshot(self.chroot.root())))
    }

    /// Sets data for node with given path and returns updated stat.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    /// * [Error::BadVersion] if such node exists but has different version.
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
        Self::map_wait(result, |(children, watcher)| (children, watcher.into_oneshot(self.chroot.root())))
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
        Self::map_wait(result, |(children, stat, watcher)| (children, stat, watcher.into_oneshot(self.chroot.root())))
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
    /// * Persistent watch could loss events during reconnection due to [ZOOKEEPER-4698][].
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
            Ok(watcher.into_persistent(self.chroot.root()))
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

    /// Authenticates session using given scheme and auth identication. This affects only
    /// subsequent operations.
    ///
    /// # Errors
    /// * [Error::AuthFailed] if authentication failed.
    /// * Other terminal session errors.
    ///
    /// # Notable behaviors
    /// * Same auth will be resubmitted for authentication after session reestablished.
    /// * This method is resistent to temporary session unavailability, that means
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
        Self::map_wait(result, |(data, stat, watcher)| (data, stat, watcher.into_oneshot("")))
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

    /// Creates a multi reader.
    pub fn new_multi_reader(&self) -> MultiReader<'_> {
        MultiReader::new(self)
    }

    /// Creates a multi writer.
    pub fn new_multi_writer(&self) -> MultiWriter<'_> {
        MultiWriter::new(self)
    }
}

/// Builder for [Client] with more options than [Client::connect].
#[derive(Clone, Debug)]
pub struct ClientBuilder {
    authes: Vec<AuthPacket>,
    session: Option<(SessionId, Vec<u8>)>,
    readonly: bool,
    detached: bool,
    session_timeout: Duration,
    connection_timeout: Duration,
}

impl ClientBuilder {
    fn new() -> Self {
        Self {
            authes: Default::default(),
            session: None,
            readonly: false,
            detached: false,
            session_timeout: Duration::ZERO,
            connection_timeout: Duration::ZERO,
        }
    }

    /// Specifies target session timeout to negotiate with ZooKeeper server.
    ///
    /// Defaults to 6s.
    pub fn with_session_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.session_timeout = timeout;
        self
    }

    /// Specifies idle timeout to conclude a connection as loss.
    ///
    /// Defaults to `2/5` of session timeout.
    pub fn with_connection_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.connection_timeout = timeout;
        self
    }

    /// Specifies whether readonly server is allowed.
    pub fn with_readonly(&mut self, readonly: bool) -> &mut ClientBuilder {
        self.readonly = readonly;
        self
    }

    /// Specifies auth info for given authentication scheme.
    pub fn with_auth(&mut self, scheme: String, auth: Vec<u8>) -> &mut ClientBuilder {
        self.authes.push(AuthPacket { scheme, auth });
        self
    }

    /// Specifies session to reestablish.
    pub fn with_session(&mut self, id: SessionId, password: Vec<u8>) -> &mut Self {
        self.session = Some((id, password));
        self
    }

    /// Detaches creating session so it will not be closed after all client instances dropped.
    pub fn detach(&mut self) -> &mut Self {
        self.detached = true;
        self
    }

    /// Connects to ZooKeeper cluster.
    pub async fn connect(&mut self, cluster: &str) -> Result<Client> {
        let (hosts, chroot) = util::parse_connect_string(cluster)?;
        let mut buf = Vec::with_capacity(4096);
        let mut connecting_depot = Depot::for_connecting();
        if let Some((id, password)) = &self.session {
            if id.0 == 0 {
                return Err(Error::BadArguments(&"session id must not be 0"));
            } else if password.is_empty() {
                return Err(Error::BadArguments(&formatcp!(
                    "session password is empty, it should have length of {}",
                    PASSWORD_LEN
                )));
            }
        }
        if self.session_timeout < Duration::ZERO {
            return Err(Error::BadArguments(&"session timeout must not be negative"));
        } else if self.connection_timeout < Duration::ZERO {
            return Err(Error::BadArguments(&"connection timeout must not be negative"));
        }
        let (mut session, state_receiver) = Session::new(
            self.session.take(),
            &self.authes,
            self.readonly,
            self.detached,
            self.session_timeout,
            self.connection_timeout,
        );
        let mut hosts_iter = hosts.iter().copied();
        let sock = session.start(&mut hosts_iter, &mut buf, &mut connecting_depot).await?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let servers = hosts.into_iter().map(|addr| addr.to_value()).collect();
        let session_info = (session.session_id, session.session_password.clone());
        let session_timeout = session.session_timeout;
        tokio::spawn(async move {
            session.serve(servers, sock, buf, connecting_depot, receiver).await;
        });
        let client = Client::new(chroot.to_owned(), session_info, session_timeout, sender, state_receiver);
        Ok(client)
    }
}

trait MultiBuffer {
    fn buffer(&mut self) -> &mut Vec<u8>;

    fn op_code() -> OpCode;

    fn build_request(&mut self) -> MarshalledRequest {
        let header = MultiHeader { op: OpCode::Error, done: true, err: -1 };
        let buffer = self.buffer();
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
    /// Individual errors(eg. [Error::NoNode]) are reported individually through [MultiReadResult::Error].
    pub async fn commit(&mut self) -> Result<Vec<MultiReadResult>> {
        if self.buf.is_empty() {
            return Ok(Default::default());
        }
        let request = self.build_request();
        let receiver = self.client.send_marshalled_request(request);
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
    pub async fn commit(&mut self) -> std::result::Result<Vec<MultiWriteResult>, MultiWriteError> {
        if self.buf.is_empty() {
            return Ok(Default::default());
        }
        let request = self.build_request();
        let receiver = self.client.send_marshalled_request(request);
        let (body, _) = receiver.await?;
        let response = record::unmarshal::<Vec<MultiWriteResponse>>(&mut body.as_slice())?;
        let failed = response.first().map(|r| matches!(r, MultiWriteResponse::Error(_))).unwrap_or(false);
        let mut results = if failed { Vec::new() } else { Vec::with_capacity(response.len()) };
        for (index, result) in response.into_iter().enumerate() {
            match result {
                MultiWriteResponse::Check => results.push(MultiWriteResult::Check),
                MultiWriteResponse::Delete => results.push(MultiWriteResult::Delete),
                MultiWriteResponse::Create { path, stat } => {
                    util::strip_root_path(path, self.client.chroot.root())?;
                    results.push(MultiWriteResult::Create { path: path.to_string(), stat });
                },
                MultiWriteResponse::SetData { stat } => results.push(MultiWriteResult::SetData { stat }),
                MultiWriteResponse::Error(Error::UnexpectedErrorCode(0)) => {},
                MultiWriteResponse::Error(err) => return Err(MultiWriteError::OperationFailed { index, source: err }),
            }
        }
        Ok(results)
    }

    /// Clears collected operations.
    pub fn abort(&mut self) {
        self.buf.clear();
    }
}
