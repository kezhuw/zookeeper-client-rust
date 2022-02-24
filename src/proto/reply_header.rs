use bytes::BufMut;

use crate::record::{ReadingBuf, SerializableRecord, StaticRecord, UnsafeBuf, UnsafeRead};

#[derive(Copy, Clone, Debug)]
pub struct ReplyHeader {
    pub xid: i32,
    pub zxid: i64,
    pub err: i32,
}

impl SerializableRecord for ReplyHeader {
    fn serialize(&self, buf: &mut dyn BufMut) {
        buf.put_i32(self.xid);
        buf.put_i64(self.zxid);
        buf.put_i32(self.err);
    }
}

impl StaticRecord for ReplyHeader {
    fn record_len() -> usize {
        16
    }
}

impl UnsafeRead<'_> for ReplyHeader {
    type Error = std::convert::Infallible;

    unsafe fn read(buf: &mut ReadingBuf) -> Result<Self, Self::Error> {
        let xid = buf.get_unchecked_i32();
        let zxid = buf.get_unchecked_i64();
        let err = buf.get_unchecked_i32();
        Ok(ReplyHeader { xid, zxid, err })
    }
}
