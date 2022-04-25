use crate::record::{
    self,
    DeserializableRecord,
    HasInsufficientError,
    InsufficientBuf,
    NotInsufficientError,
    ReadingBuf,
    UnmarshalError,
    UnsafeBuf,
};
use crate::util::{Ref, ToRef};
use crate::{Error, EventType, SessionState, WatchedEvent};

#[derive(Copy, Clone, Debug)]
pub struct WatcherEvent<'a> {
    pub event_type: EventType,
    pub session_state: SessionState,
    pub path: &'a str,
}

impl<'a> Ref<'a> for WatcherEvent<'a> {
    type Value = WatchedEvent;

    fn to_value(&self) -> Self::Value {
        WatchedEvent { event_type: self.event_type, session_state: self.session_state, path: self.path.to_owned() }
    }
}

impl<'a> ToRef<'a, WatcherEvent<'a>> for WatchedEvent {
    fn to_ref(&'a self) -> WatcherEvent<'a> {
        WatcherEvent { event_type: self.event_type, session_state: self.session_state, path: &self.path }
    }
}

impl From<InsufficientBuf> for Error {
    fn from(_: InsufficientBuf) -> Error {
        Error::UnmarshalError { entity: "", reason: &"insufficient buf" }
    }
}

impl UnmarshalError for Error {
    fn with_context(self, context: &'static str) -> Error {
        match self {
            Error::UnmarshalError { reason, .. } => Error::UnmarshalError { entity: context, reason },
            err => err,
        }
    }
}

impl NotInsufficientError for Error {
    type DeserializeError = Error;

    fn insufficient() -> Self::DeserializeError {
        Error::UnmarshalError { entity: "", reason: &"insufficient buf" }
    }

    fn to_deserialize_error(self) -> Self::DeserializeError {
        self
    }
}

impl HasInsufficientError for Error {
    type DataError = Error;

    fn to_data_error(self) -> Option<Self::DataError> {
        if matches!(self, Error::UnmarshalError { .. }) {
            None
        } else {
            Some(self)
        }
    }
}

impl<'a> DeserializableRecord<'a> for WatcherEvent<'a> {
    type Error = Error;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<Self, Self::Error> {
        if buf.len() < 12 {
            return Err(Error::UnmarshalError { entity: "watch notification", reason: &"insufficient buf" });
        }
        let event_type = EventType::from_server(unsafe { buf.get_unchecked_i32() })?;
        let session_state = SessionState::from_server(unsafe { buf.get_unchecked_i32() })?;
        let path = record::unmarshal(buf)?;
        Ok(WatcherEvent { event_type, session_state, path })
    }
}
