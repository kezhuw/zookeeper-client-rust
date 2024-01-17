use num_enum::{IntoPrimitive, TryFromPrimitive};
use strum::EnumIter;

use crate::error::Error;
use crate::proto::AddWatchMode;
use crate::util;

/// Thin wrapper for zookeeper session id. It prints in hex format headed with 0x.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SessionId(pub i64);

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#x}", self.0)
    }
}

/// ZooKeeper session states.
#[derive(Copy, Clone, Debug, PartialEq, Eq, strum::Display)]
pub enum SessionState {
    /// Intermediate state states that client is disconnected from zookeeper cluster.
    Disconnected,

    /// Intermediate state states that client has recovered from disconnected state.
    SyncConnected,

    /// Terminal state states that client failed in authentication.
    AuthFailed,

    /// Intermediate state states that client is connected to a readonly server.
    ConnectedReadOnly,

    /// Terminal state states that zookeeper session is expired.
    Expired,

    /// Terminal state states that zookeeper client has been closed.
    Closed,
}

impl SessionState {
    pub(crate) fn from_server(state: i32) -> Result<SessionState, Error> {
        let session_state = match state {
            3 => SessionState::SyncConnected,
            _ => return Err(Error::UnexpectedError(format!("keeper state value should not be {}", state))),
        };
        Ok(session_state)
    }

    pub fn is_terminated(self) -> bool {
        use SessionState::*;
        matches!(self, AuthFailed | Expired | Closed)
    }

    pub(crate) fn is_connected(self) -> bool {
        self == SessionState::SyncConnected || self == SessionState::ConnectedReadOnly
    }

    pub(crate) fn to_error(self) -> Error {
        match self {
            SessionState::Disconnected => Error::ConnectionLoss,
            SessionState::AuthFailed => Error::AuthFailed,
            SessionState::Expired => Error::SessionExpired,
            SessionState::Closed => Error::ClientClosed,
            _ => Error::UnexpectedError(format!("expect error state, got {:?}", self)),
        }
    }
}

/// WatchedEvent represents update to watched node or session.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WatchedEvent {
    pub event_type: EventType,

    /// Updated state for state event. No meaning in node events.
    ///
    /// # Caution
    /// There will be no more updates after terminal session state.
    pub session_state: SessionState,

    /// Node path from chroot. Empty if this is a state event.
    pub path: String,

    /// `zxid` of the transaction that triggered this node event, [WatchedEvent::NO_ZXID] for session event.
    ///
    /// # Notable behaviors
    /// * This feature was shipped in 3.9.0, `zxid` wil be [WatchedEvent::NO_ZXID] for earlier versions. See [ZOOKEEPER-4655].
    /// * It is possible to receive multiple events with same `zxid` and even same `path` due to [crate::MultiWriter]. See [ZOOKEEPER-4695].
    ///
    /// [ZOOKEEPER-4655]: https://issues.apache.org/jira/browse/ZOOKEEPER-4655
    /// [ZOOKEEPER-4695]: https://issues.apache.org/jira/browse/ZOOKEEPER-4695
    pub zxid: i64,
}

impl WatchedEvent {
    pub const NO_ZXID: i64 = -1;

    pub fn new(event: EventType, path: impl Into<String>) -> Self {
        debug_assert_ne!(event, EventType::Session);
        Self { event_type: event, session_state: SessionState::SyncConnected, path: path.into(), zxid: Self::NO_ZXID }
    }

    pub fn with_zxid(self, zxid: i64) -> Self {
        debug_assert_ne!(self.event_type, EventType::Session);
        Self { zxid, ..self }
    }

    pub fn new_session(state: SessionState) -> Self {
        Self { event_type: EventType::Session, session_state: state, path: Default::default(), zxid: Self::NO_ZXID }
    }

    pub(crate) fn drain_root_path(&mut self, root: &str) {
        if self.event_type != EventType::Session && !root.is_empty() {
            util::drain_root_path(&mut self.path, root).unwrap();
        }
    }
}

/// Event type for watch notifications.
#[derive(Copy, Clone, Debug, PartialEq, Eq, strum::Display)]
pub enum EventType {
    Session,
    NodeCreated,
    NodeDeleted,
    NodeDataChanged,
    NodeChildrenChanged,
}

impl EventType {
    pub(crate) fn from_server(i: i32) -> Result<EventType, Error> {
        let event_type = match i {
            1 => EventType::NodeCreated,
            2 => EventType::NodeDeleted,
            3 => EventType::NodeDataChanged,
            4 => EventType::NodeChildrenChanged,
            _ => return Err(Error::UnexpectedError(format!("event type should not be {}", i))),
        };
        Ok(event_type)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, IntoPrimitive, TryFromPrimitive, EnumIter)]
#[repr(i32)]
pub enum WatchMode {
    Child = 1,
    Data = 2,
    Any = 3,
    PersistentNode = 4,
    PersistentRecursive = 5,
}

impl From<AddWatchMode> for WatchMode {
    fn from(mode: AddWatchMode) -> Self {
        match mode {
            AddWatchMode::Persistent => WatchMode::PersistentNode,
            AddWatchMode::PersistentRecursive => WatchMode::PersistentRecursive,
        }
    }
}
