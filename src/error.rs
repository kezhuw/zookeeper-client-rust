use std::sync::Arc;

use derive_where::derive_where;
use static_assertions::assert_impl_all;
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

    #[error("unimplemented operation")]
    Unimplemented,

    #[error("connection to server has lost")]
    ConnectionLoss,

    #[error("ZooKeeper reconfiguration disabled")]
    ReconfigDisabled,

    #[error("unexpected error code: {0}")]
    UnexpectedErrorCode(i32),

    #[error("client has been closed")]
    ClientClosed,

    #[error("runtime condition mismatch")]
    RuntimeInconsistent,

    #[error(transparent)]
    Other(OtherError),
}

#[derive(Error, Clone, Debug)]
#[derive_where(Eq, PartialEq)]
#[error("{message}")]
pub struct OtherError {
    message: Arc<String>,
    #[derive_where(skip(EqHashOrd))]
    source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
}

impl Error {
    pub(crate) fn is_terminated(&self) -> bool {
        matches!(self, Self::NoHosts | Self::SessionExpired | Self::AuthFailed | Self::ClientClosed)
    }

    pub(crate) fn has_no_data_change(&self) -> bool {
        match self {
            Self::NoNode
            | Self::NoAuth
            | Self::BadVersion
            | Self::NoChildrenForEphemerals
            | Self::NodeExists
            | Self::NotEmpty
            | Self::InvalidAcl
            | Self::AuthFailed
            | Self::SessionMoved
            | Self::NotReadOnly
            | Self::NoWatcher
            | Self::QuotaExceeded
            | Self::Throttled
            | Self::MarshallingError
            | Self::Unimplemented
            | Self::ReconfigDisabled
            | Self::UnexpectedErrorCode(_) => true,
            // We are expired anyway, any ephemeral nodes will be deleted by ZooKeeper soon.
            Self::SessionExpired => true,
            // We are closed anyway, the session will expire soon.
            Self::ClientClosed => true,
            _ => false,
        }
    }

    pub(crate) fn new_other(
        message: impl Into<Arc<String>>,
        source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::Other(OtherError { message: message.into(), source })
    }

    #[allow(dead_code)]
    pub(crate) fn other(message: impl Into<String>, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::new_other(message.into(), Some(Arc::new(source)))
    }

    pub(crate) fn other_from(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::new_other(source.to_string(), Some(Arc::new(source)))
    }
}

assert_impl_all!(Error: Send, Sync);

impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Error {
        unreachable!();
    }
}
