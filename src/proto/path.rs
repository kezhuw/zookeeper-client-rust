use bytes::BufMut;

use crate::chroot::ChrootPath;
use crate::record::{DynamicRecord, SerializableRecord, StaticRecord};

impl SerializableRecord for ChrootPath<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        let (root, path) = self.path();
        let n = root.len() + path.len();
        buf.put_i32(n as i32);
        buf.put_slice(root.as_bytes());
        buf.put_slice(path.as_bytes());
    }
}

impl DynamicRecord for ChrootPath<'_> {
    fn serialized_len(&self) -> usize {
        let (root, path) = self.path();
        i32::record_len() + root.len() + path.len()
    }
}
