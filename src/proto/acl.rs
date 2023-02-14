use bytes::BufMut;

use super::path::RootedPath;
use super::stat::Stat;
use crate::acl::{Acl, AuthId, Permission};
use crate::record::{
    self,
    DeserializableRecord,
    DeserializeError,
    DynamicRecord,
    InvalidData,
    ReadingBuf,
    SerializableRecord,
};

impl SerializableRecord for Acl {
    fn serialize(&self, buf: &mut dyn BufMut) {
        buf.put_i32(self.permission().into_raw());
        self.scheme().serialize(buf);
        self.id().serialize(buf);
    }
}

impl DynamicRecord for Acl {
    fn serialized_len(&self) -> usize {
        return 4 + self.scheme().serialized_len() + self.id().serialized_len();
    }
}

impl TryFrom<i32> for Permission {
    type Error = InvalidData;

    fn try_from(raw: i32) -> Result<Permission, Self::Error> {
        let all_bits = Permission::ALL.into_raw();
        if (raw & !all_bits) != 0 {
            return Err(InvalidData::UnmarshalError(format!("invalid permission bits: {:#b}", raw)));
        }
        Ok(Permission::from_raw(raw))
    }
}

impl DeserializableRecord<'_> for Acl {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf) -> Result<Self, Self::Error> {
        let perms = i32::deserialize(buf)?;
        let scheme: &str = record::deserialize(buf)?;
        let id: &str = record::deserialize(buf)?;
        let permission = Permission::try_from(perms)?;
        let auth_id = AuthId::new(scheme, id);
        Ok(Acl::new(permission, auth_id))
    }
}

pub struct SetAclRequest<'a> {
    pub path: RootedPath<'a>,
    pub acl: &'a [Acl],
    pub version: i32,
}

impl SerializableRecord for SetAclRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        self.path.serialize(buf);
        self.acl.serialize(buf);
        self.version.serialize(buf);
    }
}

impl DynamicRecord for SetAclRequest<'_> {
    fn serialized_len(&self) -> usize {
        self.path.serialized_len() + self.acl.serialized_len() + self.version.serialized_len()
    }
}

pub struct GetAclResponse {
    pub acl: Vec<Acl>,
    pub stat: Stat,
}

impl DeserializableRecord<'_> for GetAclResponse {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf) -> Result<Self, Self::Error> {
        let acl = record::deserialize::<Vec<Acl>>(buf)?;
        let stat = record::deserialize::<Stat>(buf)?;
        Ok(GetAclResponse { acl, stat })
    }
}
