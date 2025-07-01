use std::borrow::Cow;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

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
    Custom(CustomError),
}

#[derive(Error, Clone, Debug)]
pub struct CustomError {
    message: Option<Arc<Cow<'static, str>>>,
    source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
}

impl Display for CustomError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match (self.message.as_ref(), self.source.as_ref()) {
            (Some(message), None) => f.write_str(message),
            (Some(message), Some(err)) => write!(f, "{message}: {err}"),
            (None, Some(err)) => err.fmt(f),
            _ => unreachable!("no error message or source"),
        }
    }
}

impl PartialEq for CustomError {
    fn eq(&self, other: &Self) -> bool {
        if !self.message.eq(&other.message) {
            return false;
        }
        match (self.source.as_ref(), other.source.as_ref()) {
            (Some(lhs), Some(rhs)) => Arc::ptr_eq(lhs, rhs),
            (None, None) => true,
            _ => false,
        }
    }
}

impl Eq for CustomError {}

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

    pub(crate) fn with_message(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Custom(CustomError { message: Some(Arc::new(message.into())), source: None })
    }

    #[allow(dead_code)]
    pub(crate) fn with_other(
        message: impl Into<Cow<'static, str>>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Custom(CustomError { message: Some(Arc::new(message.into())), source: Some(Arc::new(source)) })
    }

    pub(crate) fn other(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Custom(CustomError { message: None, source: Some(Arc::new(source)) })
    }
}

assert_impl_all!(Error: Send, Sync);

impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Error {
        unreachable!();
    }
}
