use crate::error::Error;
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
        return Ok(session_state);
    }

    pub fn is_terminated(self) -> bool {
        use SessionState::*;
        match self {
            AuthFailed | Expired | Closed => return true,
            _ => return false,
        };
    }

    pub(crate) fn is_connected(self) -> bool {
        return self == SessionState::SyncConnected || self == SessionState::ConnectedReadOnly;
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
pub struct WatchedEvent {
    pub event_type: EventType,

    /// Updated state for state event. No meaning in node events.
    ///
    /// # Caution
    /// There will be no more updates after terminal session state.
    pub session_state: SessionState,

    /// Node path from chroot. Empty if this is a state event.
    pub path: String,
}

impl WatchedEvent {
    pub(crate) fn drain_root_len(&mut self, root_len: usize) {
        if self.event_type != EventType::Session && root_len != 0 {
            util::drain_root_len(&mut self.path, root_len);
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
        return Ok(event_type);
    }
}
