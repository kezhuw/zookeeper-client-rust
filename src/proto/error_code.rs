use bytes::BufMut;
use num_enum::{IntoPrimitive, TryFromPrimitive, TryFromPrimitiveError};

use crate::error::Error;
use crate::record::{InvalidData, ReadingBuf, SerializableRecord, StaticRecord, UnsafeBuf, UnsafeRead};

impl From<ErrorCode> for Error {
    fn from(ec: ErrorCode) -> Error {
        use ErrorCode::*;
        match ec {
            Ok => Error::UnexpectedErrorCode(0),
            MarshallingError => Error::MarshallingError,
            Unimplemented => Error::Unimplemented,
            BadArguments => Error::BadArguments(&"server error"),
            NoNode => Error::NoNode,
            NoAuth => Error::NoAuth,
            BadVersion => Error::BadVersion,
            NoChildrenForEphemerals => Error::NoChildrenForEphemerals,
            NodeExists => Error::NodeExists,
            NotEmpty => Error::NotEmpty,
            SessionExpired => Error::SessionExpired,
            InvalidAcl => Error::InvalidAcl,
            AuthFailed | SessionClosedRequireSaslAuth => Error::AuthFailed,
            SessionMoved => Error::SessionMoved,
            NotReadOnly => Error::NotReadOnly,
            NoWatcher => Error::NoWatcher,
            ReconfigDisabled => Error::ReconfigDisabled,
            QuotaExceeded => Error::QuotaExceeded,
            Throttled => Error::Throttled,
        }
    }
}

#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
pub enum ErrorCode {
    Ok = 0,
    MarshallingError = -5,
    Unimplemented = -6,
    BadArguments = -8,
    NoNode = -101,
    NoAuth = -102,
    BadVersion = -103,
    NoChildrenForEphemerals = -108,
    NodeExists = -110,
    NotEmpty = -111,
    SessionExpired = -112,
    InvalidAcl = -114,
    AuthFailed = -115,
    SessionMoved = -118,
    NotReadOnly = -119,
    NoWatcher = -121,
    ReconfigDisabled = -123,
    SessionClosedRequireSaslAuth = -124,
    QuotaExceeded = -125,
    Throttled = -127,
}

impl From<TryFromPrimitiveError<ErrorCode>> for Error {
    fn from(err: TryFromPrimitiveError<ErrorCode>) -> Error {
        Error::UnexpectedErrorCode(err.number)
    }
}

impl SerializableRecord for ErrorCode {
    fn serialize(&self, buf: &mut dyn BufMut) {
        let ec = i32::from(*self);
        ec.serialize(buf);
    }
}

impl StaticRecord for ErrorCode {
    fn record_len() -> usize {
        4
    }
}

impl UnsafeRead<'_> for ErrorCode {
    type Error = InvalidData;

    unsafe fn read(buf: &mut ReadingBuf<'_>) -> Result<Self, InvalidData> {
        match ErrorCode::try_from(buf.get_unchecked_i32()) {
            Ok(ec) => Ok(ec),
            Err(err) => Err(InvalidData::UnexpectedErrorCode(err.number)),
        }
    }
}
