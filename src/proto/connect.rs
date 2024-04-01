use bytes::BufMut;

use crate::record::{
    DeserializableRecord,
    DeserializeError,
    DynamicRecord,
    ReadingBuf,
    SerializableRecord,
    StaticRecord,
    UnsafeBuf,
};

pub struct ConnectRequest<'a> {
    pub protocol_version: i32,
    pub last_zxid_seen: i64,
    pub timeout: i32,
    pub session_id: i64,
    pub password: &'a [u8],
    pub readonly: bool,
}

impl SerializableRecord for ConnectRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        self.protocol_version.serialize(buf);
        self.last_zxid_seen.serialize(buf);
        self.timeout.serialize(buf);
        self.session_id.serialize(buf);
        self.password.serialize(buf);
        self.readonly.serialize(buf);
    }
}

impl DynamicRecord for ConnectRequest<'_> {
    fn serialized_len(&self) -> usize {
        2 * i32::record_len() + 2 * i64::record_len() + self.password.serialized_len() + bool::record_len()
    }
}

pub struct ConnectResponse<'a> {
    #[allow(dead_code)]
    pub protocol_version: i32,
    pub session_timeout: i32,
    pub session_id: i64,
    pub password: &'a [u8],
    pub readonly: bool,
}

impl<'a> DeserializableRecord<'a> for ConnectResponse<'a> {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<Self, Self::Error> {
        let min_record_len = 4 + 4 + 8 + 4 + 1;
        if buf.len() < min_record_len {
            return Err(DeserializeError::InsufficientBuf);
        }
        let protocol_version = unsafe { buf.get_unchecked_i32() };
        let session_timeout = unsafe { buf.get_unchecked_i32() };
        let session_id = unsafe { buf.get_unchecked_i64() };
        if protocol_version != 0 {
            return Err(DeserializeError::UnmarshalError(format!("unsupported server version {protocol_version}")));
        } else if session_timeout < 0 {
            return Err(DeserializeError::UnmarshalError(format!(
                "invalid negotiated session timeout {session_timeout}"
            )));
        }
        let len = unsafe { buf.get_unchecked_i32() };
        if len <= 0 || len > buf.len() as i32 {
            return Err(DeserializeError::UnmarshalError(format!("invalid session password length {len}")));
        }
        let len = len as usize;
        let password = unsafe { buf.get_unchecked(..len) };
        let readonly = if len == buf.len() { 0 } else { unsafe { *buf.get_unchecked(len) } };
        if readonly != 0 && readonly != 1 {
            return Err(DeserializeError::UnmarshalError(format!("invalid session readonly value {readonly}")));
        }
        Ok(ConnectResponse { protocol_version, session_timeout, session_id, password, readonly: readonly == 1 })
    }
}
