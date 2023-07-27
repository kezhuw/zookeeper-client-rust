use bytes::BufMut;

use crate::chroot::ChrootPath;
use crate::record::{DynamicRecord, SerializableRecord};

pub struct CheckVersionRequest<'a> {
    pub path: ChrootPath<'a>,
    pub version: i32,
}

impl SerializableRecord for CheckVersionRequest<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        self.path.serialize(buf);
        self.version.serialize(buf);
    }
}

impl DynamicRecord for CheckVersionRequest<'_> {
    fn serialized_len(&self) -> usize {
        self.path.serialized_len() + self.version.serialized_len()
    }
}
