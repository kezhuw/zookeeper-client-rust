use bytes::BufMut;

use super::path::RootedPath;
use super::stat::Stat;
use crate::acl::Acl;
use crate::record::{
    DeserializableRecord,
    DeserializeError,
    DynamicRecord,
    ReadingBuf,
    SerializableRecord,
    StaticRecord,
};

pub struct SyncRequest<'a> {
    pub path: RootedPath<'a>,
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
    pub path: RootedPath<'a>,
    pub data: &'a [u8],
    pub acls: &'a [Acl],
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

pub struct SetWatchesRequest<'a> {
    pub relative_zxid: i64,
    pub watches: &'a [Vec<&'a str>],
}

impl SerializableRecord for SetWatchesRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        buf.put_i64(self.relative_zxid);
        self.watches.iter().for_each(|watches| watches.serialize(buf));
    }
}

impl DynamicRecord for SetWatchesRequest<'_> {
    fn serialized_len(&self) -> usize {
        let n: usize = self.watches.iter().map(|watches| watches.serialized_len()).sum();
        8 + n
    }
}

pub struct OneshotWatchRequest<'a> {
    pub path: RootedPath<'a>,
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

pub struct GetChildren2Response<'a> {
    pub children: Vec<&'a str>,
    pub stat: Stat,
}

impl<'a> DeserializableRecord<'a> for GetChildren2Response<'a> {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<GetChildren2Response<'a>, Self::Error> {
        let children = Vec::<&'a str>::deserialize(buf)?;
        let stat = Stat::deserialize(buf)?;
        let response = GetChildren2Response { children, stat };
        Ok(response)
    }
}

pub struct SetDataRequest<'a> {
    pub path: RootedPath<'a>,
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
    pub path: RootedPath<'a>,
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
    pub path: RootedPath<'a>,
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
