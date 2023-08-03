use bytes::BufMut;

use super::stat::Stat;
use crate::acl::Acls;
use crate::chroot::ChrootPath;
use crate::record::{
    DeserializableRecord,
    DeserializeError,
    DynamicRecord,
    ReadingBuf,
    SerializableRecord,
    StaticRecord,
};

pub struct SyncRequest<'a> {
    pub path: ChrootPath<'a>,
}

impl SerializableRecord for SyncRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        self.path.serialize(buf)
    }
}

impl DynamicRecord for SyncRequest<'_> {
    fn serialized_len(&self) -> usize {
        self.path.serialized_len()
    }
}

pub struct CreateRequest<'a> {
    pub path: ChrootPath<'a>,
    pub data: &'a [u8],
    pub acls: Acls<'a>,
    pub flags: i32,
    pub ttl: i64,
}

impl SerializableRecord for CreateRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        self.path.serialize(buf);
        self.data.serialize(buf);
        self.acls.serialize(buf);
        buf.put_i32(self.flags);
        if self.ttl != 0 {
            buf.put_i64(self.ttl);
        }
    }
}

impl DynamicRecord for CreateRequest<'_> {
    fn serialized_len(&self) -> usize {
        self.path.serialized_len()
            + self.data.serialized_len()
            + self.acls.serialized_len()
            + i32::record_len()
            + if self.ttl != 0 { i64::record_len() } else { 0 }
    }
}

pub struct RemoveWatchesRequest<'a> {
    pub path: &'a str,
    pub mode: i32,
}

impl SerializableRecord for RemoveWatchesRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        self.path.serialize(buf);
        self.mode.serialize(buf)
    }
}

impl DynamicRecord for RemoveWatchesRequest<'_> {
    fn serialized_len(&self) -> usize {
        self.path.serialized_len() + self.mode.serialized_len()
    }
}

pub struct SetWatchesRequest<'a> {
    pub relative_zxid: i64,
    pub paths: &'a [Vec<&'a str>],
}

impl SerializableRecord for SetWatchesRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        buf.put_i64(self.relative_zxid);
        self.paths.iter().for_each(|paths| paths.serialize(buf));
    }
}

impl DynamicRecord for SetWatchesRequest<'_> {
    fn serialized_len(&self) -> usize {
        let n: usize = self.paths.iter().map(|paths| paths.serialized_len()).sum();
        8 + n
    }
}

pub struct OneshotWatchRequest<'a> {
    pub path: ChrootPath<'a>,
    pub watch: bool,
}

pub type GetRequest<'a> = OneshotWatchRequest<'a>;
pub type ExistsRequest<'a> = OneshotWatchRequest<'a>;
pub type GetChildrenRequest<'a> = OneshotWatchRequest<'a>;

impl SerializableRecord for OneshotWatchRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        self.path.serialize(buf);
        self.watch.serialize(buf);
    }
}

impl DynamicRecord for OneshotWatchRequest<'_> {
    fn serialized_len(&self) -> usize {
        self.path.serialized_len() + bool::record_len()
    }
}

pub struct GetChildren2Response {
    pub children: Vec<String>,
    pub stat: Stat,
}

impl DeserializableRecord<'_> for GetChildren2Response {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf<'_>) -> Result<GetChildren2Response, Self::Error> {
        let children = Vec::<String>::deserialize(buf)?;
        let stat = Stat::deserialize(buf)?;
        let response = GetChildren2Response { children, stat };
        Ok(response)
    }
}

pub struct SetDataRequest<'a> {
    pub path: ChrootPath<'a>,
    pub data: &'a [u8],
    pub version: i32,
}

impl SerializableRecord for SetDataRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        self.path.serialize(buf);
        self.data.serialize(buf);
        buf.put_i32(self.version);
    }
}

impl DynamicRecord for SetDataRequest<'_> {
    fn serialized_len(&self) -> usize {
        self.path.serialized_len() + self.data.serialized_len() + i32::record_len()
    }
}

pub struct DeleteRequest<'a> {
    pub path: ChrootPath<'a>,
    pub version: i32,
}

impl SerializableRecord for DeleteRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        self.path.serialize(buf);
        buf.put_i32(self.version);
    }
}

impl DynamicRecord for DeleteRequest<'_> {
    fn serialized_len(&self) -> usize {
        self.path.serialized_len() + i32::record_len()
    }
}

pub struct PersistentWatchRequest<'a> {
    pub path: ChrootPath<'a>,
    pub mode: i32,
}

impl SerializableRecord for PersistentWatchRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        self.path.serialize(buf);
        buf.put_i32(self.mode);
    }
}

impl DynamicRecord for PersistentWatchRequest<'_> {
    fn serialized_len(&self) -> usize {
        self.path.serialized_len() + i32::record_len()
    }
}
