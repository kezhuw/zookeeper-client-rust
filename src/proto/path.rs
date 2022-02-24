use bytes::BufMut;

use crate::record::{DynamicRecord, SerializableRecord, StaticRecord};

pub struct RootedPath<'a>(&'a str, &'a str);

impl RootedPath<'_> {
    pub fn new<'a>(root: &'a str, leaf: &'a str) -> RootedPath<'a> {
        RootedPath(root, leaf)
    }
}

impl SerializableRecord for RootedPath<'_> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        let n = self.0.len() + self.1.len();
        buf.put_i32(n as i32);
        buf.put_slice(self.0.as_bytes());
        buf.put_slice(self.1.as_bytes());
    }
}

impl DynamicRecord for RootedPath<'_> {
    fn serialized_len(&self) -> usize {
        i32::record_len() + self.0.len() + self.1.len()
    }
}
