use bytes::BufMut;

use crate::acl::AuthUser;
use crate::record::{
    self,
    DeserializableRecord,
    DeserializeError,
    DynamicRecord,
    ReadingBuf,
    SerializableRecord,
    StaticRecord,
};

#[derive(Clone, Debug)]
pub struct AuthPacket<'a> {
    pub scheme: &'a str,
    pub auth: &'a [u8],
}

impl SerializableRecord for AuthPacket<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        buf.put_i32(0);
        self.scheme.serialize(buf);
        self.auth.serialize(buf);
    }
}

impl DynamicRecord for AuthPacket<'_> {
    fn serialized_len(&self) -> usize {
        i32::record_len() + self.scheme.serialized_len() + self.auth.serialized_len()
    }
}

impl DeserializableRecord<'_> for AuthUser {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf) -> Result<Self, Self::Error> {
        let scheme: &str = record::deserialize(buf)?;
        let user: &str = record::deserialize(buf)?;
        Ok(AuthUser::new(scheme, user))
    }
}
