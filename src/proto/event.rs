use crate::record::{self, DeserializableRecord, DeserializeError, ReadingBuf, UnsafeBuf};

pub struct WatcherEvent {
    pub event_type: i32,
    pub session_state: i32,
    pub path: String,
}

impl DeserializableRecord<'_> for WatcherEvent {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf) -> Result<Self, Self::Error> {
        if buf.len() < 12 {
            return Err(DeserializeError::InsufficientBuf);
        }
        let event_type = unsafe { buf.get_unchecked_i32() };
        let session_state = unsafe { buf.get_unchecked_i32() };
        let path = record::deserialize(buf)?;
        Ok(WatcherEvent { event_type, session_state, path })
    }
}
