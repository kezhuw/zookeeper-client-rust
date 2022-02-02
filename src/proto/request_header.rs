use bytes::BufMut;

use super::consts::PredefinedXid;
use super::op_code::OpCode;
use crate::record::{InvalidData, ReadingBuf, SerializableRecord, StaticRecord, UnsafeBuf, UnsafeRead};

pub struct RequestHeader {
    pub xid: i32,
    pub code: OpCode,
}

impl RequestHeader {
    pub fn with_code(code: OpCode) -> RequestHeader {
        let xid = match code {
            OpCode::Ping => PredefinedXid::Ping.into(),
            OpCode::Auth => PredefinedXid::Auth.into(),
            OpCode::SetWatches | OpCode::SetWatches2 => PredefinedXid::SetWatches.into(),
            _ => 0,
        };
        return RequestHeader { xid, code };
    }
}

impl UnsafeRead<'_> for RequestHeader {
    type Error = InvalidData;

    unsafe fn read(buf: &mut ReadingBuf) -> Result<Self, InvalidData> {
        let xid = buf.get_unchecked_i32();
        let code = match buf.get_unchecked_i32().try_into() {
            Ok(code) => code,
            Err(_) => return Err(InvalidData(&"unexpected op code")),
        };
        return Ok(RequestHeader { xid, code });
    }
}

impl SerializableRecord for RequestHeader {
    fn serialize(&self, buf: &mut dyn BufMut) {
        buf.put_i32(self.xid);
        buf.put_i32(self.code.into());
    }
}

impl StaticRecord for RequestHeader {
    fn record_len() -> usize {
        return 8;
    }
}
