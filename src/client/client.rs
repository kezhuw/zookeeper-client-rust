use std::time::Duration;

use const_format::formatcp;
use tokio::sync::{mpsc, oneshot, watch};

use super::operation::{self, SessionOperation, WatchReceiver};
use super::session::{AuthResponser, OperationState, Session};
use super::types::{SessionId, SessionState, WatchedEvent};
use crate::acl::{Acl, AuthUser};
use crate::error::{ConnectError, Error};
pub use crate::proto::EnsembleUpdate;
use crate::proto::{
    self,
    AuthPacket,
    CreateRequest,
    DeleteRequest,
    ExistsRequest,
    GetAclResponse,
    GetChildren2Response,
    GetChildrenRequest,
    GetRequest,
    OpCode,
    PersistentWatchRequest,
    ReconfigRequest,
    RootedPath,
    SetAclRequest,
    SetDataRequest,
    Stat,
    SyncRequest,
};
use crate::record::{self, Record, StaticRecord};
use crate::util::{self, Ref as _};

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
        return self == CreateMode::PersistentSequential || self == CreateMode::EphemeralSequential;
    }

    fn is_container(self) -> bool {
        return self == CreateMode::Container;
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
        return CreateOptions { mode, acls, ttl: None };
    }

    /// Specifies ttl for persistent node.
    pub fn with_ttl(&'a mut self, ttl: Duration) -> &'a mut Self {
        self.ttl = Some(ttl);
        self
    }

    fn validate(&'a self) -> Result<(), Error> {
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
        return Ok(());
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

/// StateWatcher tracks session state updates.
#[derive(Clone, Debug)]
pub struct StateWatcher {
    receiver: watch::Receiver<SessionState>,
}

impl StateWatcher {
    fn new(receiver: watch::Receiver<SessionState>) -> StateWatcher {
        return StateWatcher { receiver };
    }

    /// Returns and consumes most recently state.
    pub fn state(&mut self) -> SessionState {
        let state = self.receiver.borrow_and_update();
        return *state;
    }

    /// Waits until state changed and returns consumed state.
    ///
    /// This method will block indefinitely after one of terminal states consumed.
    pub async fn changed(&mut self) -> SessionState {
        self.receiver.changed().await.unwrap();
        return self.state();
    }

    /// Returns but not consumes most recently state.
    pub fn peek_state(&self) -> SessionState {
        let state = self.receiver.borrow();
        return *state;
    }
}

/// Watcher for stat, data and child event.
#[derive(Debug)]
pub struct OneshotWatcher {
    root_len: usize,
    receiver: oneshot::Receiver<WatchedEvent>,
}

impl OneshotWatcher {
    /// Waits for node event or session broken.
    pub async fn changed(self) -> WatchedEvent {
        let mut event = self.receiver.await.unwrap();
        event.drain_root_len(self.root_len);
        return event;
    }
}

/// Watcher for persistent and recursive watch.
#[derive(Debug)]
pub struct PersistentWatcher {
    root_len: usize,
    receiver: mpsc::UnboundedReceiver<WatchedEvent>,
}

impl PersistentWatcher {
    /// Waits for next event which could be node event or session activities.
    ///
    /// # Panics
    /// Panic after terminal session event received.
    pub async fn changed(&mut self) -> WatchedEvent {
        let mut event = self.receiver.recv().await.unwrap();
        event.drain_root_len(self.root_len);
        return event;
    }
}

impl WatchReceiver {
    pub fn into_oneshot(self, root: &str) -> OneshotWatcher {
        match self {
            WatchReceiver::None => unreachable!("expect oneshot watcher, got none watcher"),
            WatchReceiver::Oneshot(receiver) => OneshotWatcher { root_len: root.len(), receiver },
            WatchReceiver::Persistent(_) => {
                unreachable!("expect oneshot watcher, got persistent watcher")
            },
        }
    }

    pub fn into_persistent(self, root: &str) -> PersistentWatcher {
        match self {
            WatchReceiver::None => unreachable!("expect oneshot watcher, got none watcher"),
            WatchReceiver::Oneshot(_) => {
                unreachable!("expect oneshot watcher, got oneshot watcher")
            },
            WatchReceiver::Persistent(receiver) => PersistentWatcher { root_len: root.len(), receiver },
        }
    }
}

impl Drop for PersistentWatcher {
    // FIXME: remove watcher
    fn drop(&mut self) {}
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
#[derive(Clone, Debug)]
pub struct Client {
    root: String,
    session: (SessionId, Vec<u8>),
    session_timeout: Duration,
    requester: mpsc::Sender<SessionOperation>,
    auth_requester: mpsc::Sender<(AuthPacket, AuthResponser)>,
    state_watcher: StateWatcher,
}

impl Client {
    const CONFIG_NODE: &'static str = "/zookeeper/config";

    /// Connects to ZooKeeper cluster with specified session timeout.
    pub async fn connect(cluster: &str, timeout: Duration) -> Result<Client, ConnectError> {
        return ClientBuilder::new(timeout).connect(cluster).await;
    }

    pub(crate) fn new(
        root: String,
        session: (SessionId, Vec<u8>),
        timeout: Duration,
        requester: mpsc::Sender<SessionOperation>,
        auth_requester: mpsc::Sender<(AuthPacket, AuthResponser)>,
        state_receiver: watch::Receiver<SessionState>,
    ) -> Client {
        let state_watcher = StateWatcher::new(state_receiver);
        Client { root, session, session_timeout: timeout, requester, auth_requester, state_watcher }
    }

    fn validate_path<'a>(&self, path: &'a str) -> Result<(&'a str, bool), Error> {
        return util::validate_path(self.root.as_str(), path, false);
    }

    fn validate_sequential_path<'a>(&self, path: &'a str) -> Result<(&'a str, bool), Error> {
        return util::validate_path(&self.root, path, true);
    }

    /// ZooKeeper session id.
    pub fn session_id(&self) -> SessionId {
        return self.session.0;
    }

    /// Session password.
    pub fn session_password(&self) -> &[u8] {
        return self.session.1.as_slice();
    }

    /// Negotiated session timeout.
    pub fn session_timeout(&self) -> Duration {
        return self.session_timeout;
    }

    /// Latest session state.
    pub fn state(&self) -> SessionState {
        return self.state_watcher.peek_state();
    }

    /// Creates a [StateWatcher] to track state updates.
    pub fn state_watcher(&self) -> StateWatcher {
        return self.state_watcher.clone();
    }

    /// Changes root directory to given absolute path.
    ///
    /// # Errors
    /// In case of bad root path, old client is wrapped in [Result::Err].
    ///
    /// # Notable behaviors
    /// * Existing watchers are not affected.
    pub fn chroot(mut self, root: &str) -> Result<Client, Client> {
        let is_zookeeper_root = match util::validate_path("", root, false) {
            Err(_) => return Err(self),
            Ok((_, is_zookeeper_root)) => is_zookeeper_root,
        };
        self.root.clear();
        if !is_zookeeper_root {
            self.root.push_str(root);
        }
        return Ok(self);
    }

    async fn request(&self, code: OpCode, body: &impl Record) -> Result<(Vec<u8>, WatchReceiver), Error> {
        let (operation, receiver) = operation::build_state_operation(code, body);
        if self.requester.send(operation).await.is_err() {
            let state = self.state();
            return Err(state.to_error());
        }
        return receiver.await.unwrap();
    }

    fn parse_sequence(client_path: &str, path: &str) -> Result<CreateSequence, Error> {
        if let Some(sequence_path) = client_path.strip_prefix(path) {
            match sequence_path.parse::<i32>() {
                Err(_) => Err(Error::UnexpectedError(format!("sequential node get no i32 path {}", client_path))),
                Ok(i) => Ok(CreateSequence(i)),
            }
        } else {
            return Err(Error::UnexpectedError(format!(
                "sequential path {} does not contain prefix path {}",
                client_path, path
            )));
        }
    }

    /// Creates node with given path and data.
    ///
    /// # Notable errors
    /// * [Error::NodeExists] if a node with same path already exists.
    /// * [Error::NoNode] if parent node does not exist.
    /// * [Error::NoChildrenForEphemerals] if parent node is ephemeral.
    /// * [Error::InvalidAcl] if acl is invalid or empty.
    pub async fn create(
        &self,
        path: &str,
        data: &[u8],
        options: &CreateOptions<'_>,
    ) -> Result<(Stat, CreateSequence), Error> {
        options.validate()?;
        let create_mode = options.mode;
        let sequential = create_mode.is_sequential();
        let (leaf, _) = if sequential { self.validate_sequential_path(path)? } else { self.validate_path(path)? };
        let ttl = options.ttl.map(|ttl| ttl.as_millis() as i64).unwrap_or(0);
        let op_code = if ttl != 0 {
            OpCode::CreateTtl
        } else if create_mode.is_container() {
            OpCode::CreateContainer
        } else {
            OpCode::Create2
        };
        let flags = create_mode.as_flags(ttl != 0);
        let request = CreateRequest { path: RootedPath::new(&self.root, leaf), data, acls: options.acls, flags, ttl };
        let (body, _) = self.request(op_code, &request).await?;
        let mut buf = body.as_slice();
        let server_path = record::unmarshal_entity::<&str>(&"server path", &mut buf)?;
        let client_path = util::strip_root_path(server_path, &self.root)?;
        let sequence = if sequential { Self::parse_sequence(client_path, path)? } else { CreateSequence(-1) };
        let stat = record::unmarshal::<Stat>(&mut buf)?;
        return Ok((stat, sequence));
    }

    /// Deletes node with specified path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    /// * [Error::BadVersion] if such node exists but has different version.
    /// * [Error::NotEmpty] if such node exists but has children.
    pub async fn delete(&self, path: &str, expected_version: Option<i32>) -> Result<(), Error> {
        let (leaf, _) = self.validate_path(path)?;
        if leaf.is_empty() {
            return Err(Error::BadArguments(&"can not delete root node"));
        }
        let request =
            DeleteRequest { path: RootedPath::new(&self.root, leaf), version: expected_version.unwrap_or(-1) };
        self.request(OpCode::Delete, &request).await?;
        return Ok(());
    }

    async fn get_data_internally(
        &self,
        root: &str,
        leaf: &str,
        watch: bool,
    ) -> Result<(Vec<u8>, Stat, WatchReceiver), Error> {
        let request = GetRequest { path: RootedPath::new(root, leaf), watch };
        let (mut body, watcher) = self.request(OpCode::GetData, &request).await?;
        let data_len = body.len() - Stat::record_len();
        let mut stat_buf = &body[data_len..];
        let stat = record::unmarshal(&mut stat_buf)?;
        body.truncate(data_len);
        drop(body.drain(..4));
        return Ok((body, stat, watcher));
    }

    /// Gets stat and data for node with given path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub async fn get_data(&self, path: &str) -> Result<(Vec<u8>, Stat), Error> {
        let (leaf, _) = self.validate_path(path)?;
        let (data, stat, _) = self.get_data_internally(&self.root, leaf, false).await?;
        return Ok((data, stat));
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
    pub async fn get_and_watch_data(&self, path: &str) -> Result<(Vec<u8>, Stat, OneshotWatcher), Error> {
        let (leaf, _) = self.validate_path(path)?;
        let (data, stat, watch_receiver) = self.get_data_internally(&self.root, leaf, true).await?;
        return Ok((data, stat, watch_receiver.into_oneshot(&self.root)));
    }

    async fn check_stat_internally(&self, path: &str, watch: bool) -> Result<(Option<Stat>, WatchReceiver), Error> {
        let (leaf, _) = self.validate_path(path)?;
        let request = ExistsRequest { path: RootedPath::new(&self.root, leaf), watch };
        let (body, watcher) = self.request(OpCode::Exists, &request).await?;
        let mut buf = body.as_slice();
        let stat = record::try_deserialize(&mut buf)?;
        return Ok((stat, watcher));
    }

    /// Checks stat for node with given path.
    pub async fn check_stat(&self, path: &str) -> Result<Option<Stat>, Error> {
        let (stat, _) = self.check_stat_internally(path, false).await?;
        return Ok(stat);
    }

    /// Checks stat for node with given path, and watches node creation, deletion and data change.
    ///
    /// The watch will be triggered by:
    /// * Data change.
    /// * Node creation and deletion.
    /// * Session expiration.
    pub async fn check_and_watch_stat(&self, path: &str) -> Result<(Option<Stat>, OneshotWatcher), Error> {
        let (stat, watch_receiver) = self.check_stat_internally(path, true).await?;
        return Ok((stat, watch_receiver.into_oneshot(&self.root)));
    }

    /// Sets data for node with given path and returns updated stat.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    /// * [Error::BadVersion] if such node exists but has different version.
    pub async fn set_data(&self, path: &str, data: &[u8], expected_version: Option<i32>) -> Result<Stat, Error> {
        let (leaf, _) = self.validate_path(path)?;
        let request =
            SetDataRequest { path: RootedPath::new(&self.root, leaf), data, version: expected_version.unwrap_or(-1) };
        let (body, _) = self.request(OpCode::SetData, &request).await?;
        let mut buf = body.as_slice();
        let stat: Stat = record::unmarshal(&mut buf)?;
        return Ok(stat);
    }

    async fn list_children_internally(&self, path: &str, watch: bool) -> Result<(Vec<String>, WatchReceiver), Error> {
        let (leaf, _) = self.validate_path(path)?;
        let request = GetChildrenRequest { path: RootedPath::new(&self.root, leaf), watch };
        let (body, watcher) = self.request(OpCode::GetChildren, &request).await?;
        let mut buf = body.as_slice();
        let children = record::unmarshal_entity::<Vec<&str>>(&"children paths", &mut buf)?;
        let children = children.into_iter().map(|child| child.to_owned()).collect();
        return Ok((children, watcher));
    }

    /// Lists children for node with given path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub async fn list_children(&self, path: &str) -> Result<Vec<String>, Error> {
        let (children, _) = self.list_children_internally(path, false).await?;
        return Ok(children);
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
    pub async fn list_and_watch_children(&self, path: &str) -> Result<(Vec<String>, OneshotWatcher), Error> {
        let (children, watcher) = self.list_children_internally(path, true).await?;
        return Ok((children, watcher.into_oneshot(&self.root)));
    }

    async fn get_children_internally(
        &self,
        path: &str,
        watch: bool,
    ) -> Result<(Vec<String>, Stat, WatchReceiver), Error> {
        let (leaf, _) = self.validate_path(path)?;
        let request = GetChildrenRequest { path: RootedPath::new(&self.root, leaf), watch };
        let (body, watcher) = self.request(OpCode::GetChildren2, &request).await?;
        let mut buf = body.as_slice();
        let response = record::unmarshal::<GetChildren2Response>(&mut buf)?;
        let children = response.children.into_iter().map(|s| s.to_owned()).collect();
        return Ok((children, response.stat, watcher));
    }

    /// Gets stat and children for node with given path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub async fn get_children(&self, path: &str) -> Result<(Vec<String>, Stat), Error> {
        let (children, stat, _) = self.get_children_internally(path, false).await?;
        return Ok((children, stat));
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
    pub async fn get_and_watch_children(&self, path: &str) -> Result<(Vec<String>, Stat, OneshotWatcher), Error> {
        let (children, stat, watcher) = self.get_children_internally(path, true).await?;
        return Ok((children, stat, watcher.into_oneshot(&self.root)));
    }

    /// Counts descendants number for node with given path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub async fn count_descendants_number(&self, path: &str) -> Result<usize, Error> {
        let (leaf, _) = self.validate_path(path)?;
        let request = RootedPath::new(&self.root, leaf);
        let (body, _) = self.request(OpCode::GetAllChildrenNumber, &request).await?;
        let mut buf = body.as_slice();
        let n = record::unmarshal_entity::<i32>(&"all children number", &mut buf)?;
        return Ok(n as usize);
    }

    /// Lists all ephemerals nodes that created by current session and starts with given path.
    ///
    /// # Notable behaviors
    /// * No [Error::NoNode] if node with give path does not exist.
    /// * Result will include given path if that node is ephemeral.
    /// * Returned paths are located at chroot but not ZooKeeper root.
    pub async fn list_ephemerals(&self, path: &str) -> Result<Vec<String>, Error> {
        let (leaf, _) = self.validate_path(path)?;
        let request = RootedPath::new(&self.root, leaf);
        let (body, _) = self.request(OpCode::GetEphemerals, &request).await?;
        let mut buf = body.as_slice();
        let mut ephemerals = record::unmarshal_entity::<Vec<String>>(&"ephemerals", &mut buf)?;
        for ephemeral_path in ephemerals.iter_mut() {
            util::drain_root_path(ephemeral_path, &self.root)?;
        }
        return Ok(ephemerals);
    }

    /// Gets acl and stat for node with given path.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    pub async fn get_acl(&self, path: &str) -> Result<(Vec<Acl>, Stat), Error> {
        let (leaf, _) = self.validate_path(path)?;
        let request = RootedPath::new(&self.root, leaf);
        let (body, _) = self.request(OpCode::GetACL, &request).await?;
        let mut buf = body.as_slice();
        let response: GetAclResponse = record::unmarshal(&mut buf)?;
        return Ok((response.acl, response.stat));
    }

    /// Sets acl for node with given path and returns updated stat.
    ///
    /// # Notable errors
    /// * [Error::NoNode] if such node does not exist.
    /// * [Error::BadVersion] if such node exists but has different acl version.
    pub async fn set_acl(&self, path: &str, acl: &[Acl], expected_acl_version: Option<i32>) -> Result<Stat, Error> {
        let (leaf, _) = self.validate_path(path)?;
        let request =
            SetAclRequest { path: RootedPath::new(&self.root, leaf), acl, version: expected_acl_version.unwrap_or(-1) };
        let (body, _) = self.request(OpCode::SetACL, &request).await?;
        let mut buf = body.as_slice();
        let stat: Stat = record::unmarshal(&mut buf)?;
        return Ok(stat);
    }

    /// Watches possible nonexistent path using specified mode.
    ///
    /// The watch will be triggered by:
    /// * Data change, children creation and deletion.
    /// * Session activities.
    ///
    /// # Cautions
    /// * Holds returned watcher without polling events may result in memory burst.
    /// * See [ZOOKEEPER-4466](https://issues.apache.org/jira/browse/ZOOKEEPER-4466) for
    /// interferences among different watch modes for same path or paths with parent-child
    /// relationship.
    pub async fn watch(&self, path: &str, mode: AddWatchMode) -> Result<PersistentWatcher, Error> {
        let (leaf, _) = self.validate_path(path)?;
        let proto_mode = proto::AddWatchMode::from(mode);
        let request = PersistentWatchRequest { path: RootedPath::new(&self.root, leaf), mode: proto_mode.into() };
        let (_, watcher) = self.request(OpCode::AddWatch, &request).await?;
        return Ok(watcher.into_persistent(&self.root));
    }

    /// Syncs with ZooKeeper **leader**.
    ///
    /// # Cautions
    /// `sync + read` could not guarantee linearizable semantics as `sync` is not quorum acked and
    /// leader could change in between.
    ///
    /// See [ZOOKEEPER-1675](https://issues.apache.org/jira/browse/ZOOKEEPER-1675) and
    /// [ZOOKEEPER-2136](https://issues.apache.org/jira/browse/ZOOKEEPER-2136) for reference.
    pub async fn sync(&self, path: &str) -> Result<(), Error> {
        let (leaf, _) = self.validate_path(path)?;
        let request = SyncRequest { path: RootedPath::new(&self.root, leaf) };
        let (body, _) = self.request(OpCode::Sync, &request).await?;
        let mut buf = body.as_slice();
        record::unmarshal_entity::<&str>(&"server path", &mut buf)?;
        return Ok(());
    }

    /// Authenticates session using given scheme and auth identication.
    ///
    /// # Errors
    /// * [Error::AuthFailed] if authentication failed.
    /// * Other terminal session errors.
    ///
    /// # Notable behaviors
    /// * Same auth will be resubmitted for authentication after session reestablished.
    /// * This method is resistent to temporary session unavailability, that means
    /// [SessionState::Disconnected] will not end authentication.
    pub async fn auth(&self, scheme: String, auth: Vec<u8>) -> Result<(), Error> {
        let (sender, receiver) = oneshot::channel();
        let auth_packet = AuthPacket { scheme, auth };
        if self.auth_requester.send((auth_packet, sender)).await.is_err() {
            let state = self.state();
            return Err(state.to_error());
        }
        return receiver.await.unwrap();
    }

    /// Gets all authentication informations attached to current session.
    ///
    /// # Requirements
    /// * ZooKeeper 3.7.0 and above
    ///
    /// # References
    /// * [ZOOKEEPER-3969](https://issues.apache.org/jira/browse/ZOOKEEPER-3969) Add whoami API and
    /// Cli command.
    pub async fn list_auth_users(&self) -> Result<Vec<AuthUser>, Error> {
        let request = ();
        let (body, _) = self.request(OpCode::WhoAmI, &request).await?;
        let mut buf = body.as_slice();
        let authed_users = record::unmarshal_entity::<Vec<AuthUser>>(&"authed users", &mut buf)?;
        return Ok(authed_users);
    }

    /// Gets data for ZooKeeper config node, that is node with path "/zookeeper/config".
    pub async fn get_config(&self) -> Result<(Vec<u8>, Stat), Error> {
        let (data, stat, _) = self.get_data_internally(Self::CONFIG_NODE, Default::default(), false).await?;
        return Ok((data, stat));
    }

    /// Gets stat and data for ZooKeeper config node, that is node with path "/zookeeper/config".
    pub async fn get_and_watch_config(&self) -> Result<(Vec<u8>, Stat, OneshotWatcher), Error> {
        let (data, stat, watcher) = self.get_data_internally(Self::CONFIG_NODE, Default::default(), false).await?;
        return Ok((data, stat, watcher.into_oneshot(&self.root)));
    }

    /// Updates ZooKeeper ensemble.
    ///
    /// # Notable errors
    /// * [Error::ReconfigDisabled] if ZooKeeper reconfiguration is disabled.
    ///
    /// # References
    /// See [ZooKeeper Dynamic Reconfiguration](https://zookeeper.apache.org/doc/current/zookeeperReconfig.html).
    pub async fn update_ensemble<'a, I: Iterator<Item = &'a str> + Clone>(
        &self,
        update: EnsembleUpdate<'a, I>,
        expected_version: Option<i32>,
    ) -> Result<(Vec<u8>, Stat), Error> {
        let request = ReconfigRequest { update, version: expected_version.unwrap_or(-1) };
        let (mut body, _) = self.request(OpCode::Reconfig, &request).await?;
        let mut buf = body.as_slice();
        let data: &str = record::unmarshal_entity(&"reconfig data", &mut buf)?;
        let stat = record::unmarshal_entity(&"reconfig stat", &mut buf)?;
        let data_len = data.len();
        body.truncate(data_len + 4);
        drop(body.drain(..4));
        return Ok((body, stat));
    }
}

/// Builder for [Client] with more options than [Client::connect].
#[derive(Clone, Debug)]
pub struct ClientBuilder {
    timeout: Duration,
    authes: Vec<AuthPacket>,
    readonly: bool,
}

impl ClientBuilder {
    /// Constructs a builder with given session timeout.
    pub fn new(session_timeout: Duration) -> ClientBuilder {
        return ClientBuilder { timeout: session_timeout, authes: Default::default(), readonly: false };
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

    /// Connects to ZooKeeper cluster.
    ///
    /// # Notable behaviors
    /// * On success, authes were consumed.
    pub async fn connect(&mut self, cluster: &str) -> Result<Client, ConnectError> {
        let (hosts, root) = util::parse_connect_string(cluster)?;
        let mut buf = Vec::with_capacity(4096);
        let mut connecting_state = OperationState::for_connecting();
        let authes = std::mem::take(&mut self.authes);
        let (mut session, state_receiver) = Session::new(self.timeout, authes, self.readonly);
        let mut hosts_iter = hosts.iter().copied();
        let sock = match session.start(&mut hosts_iter, &mut buf, &mut connecting_state).await {
            Ok(sock) => sock,
            Err(err) => {
                self.authes = std::mem::take(&mut session.authes);
                return Err(ConnectError::from(err));
            },
        };
        let (sender, receiver) = mpsc::channel(512);
        let (auth_sender, auth_receiver) = mpsc::channel(10);
        let servers = hosts.into_iter().map(|addr| addr.to_value()).collect();
        let session_info = (session.session_id, session.session_password.clone());
        let session_timeout = session.session_timeout;
        tokio::spawn(async move {
            session.serve(servers, sock, buf, connecting_state, receiver, auth_receiver).await;
        });
        let client = Client::new(root.to_string(), session_info, session_timeout, sender, auth_sender, state_receiver);
        return Ok(client);
    }
}
