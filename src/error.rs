use thiserror::Error;

/// Errors for ZooKeeper operations.
#[non_exhaustive]
#[derive(Error, Clone, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("unable to unmarshal {entity} due to {reason}")]
    UnmarshalError { entity: &'static str, reason: &'static &'static str },

    #[error("no available hosts")]
    NoHosts,

    #[error("timeout")]
    Timeout,

    #[error("unexpected error: {0}")]
    UnexpectedError(String),

    #[error("bad arguments: {0}")]
    BadArguments(&'static &'static str),

    #[error("node not exists")]
    NoNode,

    #[error("not authorized")]
    NoAuth,

    #[error("mismatch version")]
    BadVersion,

    #[error("ephemeral node can not have children")]
    NoChildrenForEphemerals,

    #[error("node already exists")]
    NodeExists,

    #[error("node has not empty children")]
    NotEmpty,

    #[error("session expired")]
    SessionExpired,

    #[error("invalid acls")]
    InvalidAcl,

    #[error("authentication failed")]
    AuthFailed,

    #[error("session moved")]
    SessionMoved,

    #[error("write request is sent to read only server")]
    NotReadOnly,

    #[error("no watcher")]
    NoWatcher,

    #[error("exceed path quota")]
    QuotaExceeded,

    #[error("request was throttled due to server heavy loading")]
    Throttled,

    #[error("server fail to marshal client request")]
    MarshallingError,

    #[error("connection to server has lost")]
    ConnectionLoss,

    #[error("ZooKeeper reconfiguration disabled")]
    ReconfigDisabled,

    #[error("unexpected error code: {0}")]
    UnexpectedErrorCode(i32),

    #[error("client has been closed")]
    ClientClosed,
}

impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Error {
        unreachable!();
    }
}

/// Errors for client connecting.
#[derive(thiserror::Error, Debug)]
pub enum ConnectError {
    #[error("bad arguments: {0}")]
    BadArguments(&'static &'static str),

    #[error("auth failed")]
    AuthFailed,

    #[error("no available hosts")]
    NoHosts,

    #[error("timeout")]
    Timeout,

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error>),
}

impl From<Error> for ConnectError {
    fn from(err: Error) -> ConnectError {
        match err {
            Error::BadArguments(reason) => ConnectError::BadArguments(reason),
            Error::AuthFailed => ConnectError::AuthFailed,
            Error::NoHosts => ConnectError::NoHosts,
            Error::Timeout => ConnectError::Timeout,
            _ => ConnectError::Other(Box::new(err)),
        }
    }
}
