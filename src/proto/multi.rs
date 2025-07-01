use bytes::BufMut;

use super::error_code::ErrorCode;
use super::op_code::OpCode;
use super::stat::Stat;
use crate::error::Error;
use crate::record::{
    DeserializableRecord,
    DeserializeError,
    InvalidData,
    ReadingBuf,
    SerializableRecord,
    StaticRecord,
    UnsafeBuf,
    UnsafeRead,
};

pub struct MultiHeader {
    pub op: OpCode,
    pub done: bool,
    pub err: i32,
}

impl SerializableRecord for MultiHeader {
    fn serialize(&self, buf: &mut dyn BufMut) {
        self.op.serialize(buf);
        self.done.serialize(buf);
        self.err.serialize(buf);
    }
}

impl StaticRecord for MultiHeader {
    fn record_len() -> usize {
        OpCode::record_len() + bool::record_len() + i32::record_len()
    }
}

impl UnsafeRead<'_> for MultiHeader {
    type Error = InvalidData;

    unsafe fn read(buf: &mut ReadingBuf) -> Result<Self, Self::Error> {
        let op = OpCode::read(buf)?;
        let done = bool::read(buf)?;
        let err = buf.get_unchecked_i32();
        Ok(MultiHeader { op, done, err })
    }
}

pub enum MultiReadResponse {
    Data { data: Vec<u8>, stat: Stat },
    Children { children: Vec<String> },
    Error(Error),
}

impl DeserializableRecord<'_> for Vec<MultiReadResponse> {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf<'_>) -> Result<Vec<MultiReadResponse>, Self::Error> {
        let mut results = Vec::new();
        loop {
            let header = MultiHeader::deserialize(buf)?;
            if header.done {
                break;
            }
            match header.op {
                OpCode::GetData => {
                    let data = Vec::<u8>::deserialize(buf)?;
                    let stat = Stat::deserialize(buf)?;
                    results.push(MultiReadResponse::Data { data, stat });
                },
                OpCode::GetChildren => {
                    let children = Vec::<String>::deserialize(buf)?;
                    results.push(MultiReadResponse::Children { children });
                },
                OpCode::Error => {
                    let ec = i32::deserialize(buf)?;
                    let err = match ErrorCode::try_from(ec) {
                        Ok(ec) => Error::from(ec),
                        Err(err) => Error::UnexpectedErrorCode(err.number),
                    };
                    results.push(MultiReadResponse::Error(err));
                },
                op => {
                    return Err(DeserializeError::UnmarshalError(format!(
                        "unexpected op code {op} in multi read response"
                    )))
                },
            }
        }
        Ok(results)
    }
}

pub enum MultiWriteResponse<'a> {
    Check,
    Delete,
    Create { path: &'a str, stat: Stat },
    SetData { stat: Stat },
    Error(Error),
}

impl<'a> DeserializableRecord<'a> for Vec<MultiWriteResponse<'a>> {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<Vec<MultiWriteResponse<'a>>, Self::Error> {
        let mut results = Vec::new();
        loop {
            let header = MultiHeader::deserialize(buf)?;
            if header.done {
                break;
            }
            match header.op {
                OpCode::Check => results.push(MultiWriteResponse::Check),
                OpCode::Delete => results.push(MultiWriteResponse::Delete),
                OpCode::Create => {
                    let path = <&str>::deserialize(buf)?;
                    results.push(MultiWriteResponse::Create { path, stat: Stat::new_invalid() });
                },
                OpCode::Create2 => {
                    let path = <&str>::deserialize(buf)?;
                    let stat = Stat::deserialize(buf)?;
                    results.push(MultiWriteResponse::Create { path, stat });
                },
                OpCode::SetData => {
                    let stat = Stat::deserialize(buf)?;
                    results.push(MultiWriteResponse::SetData { stat });
                },
                OpCode::Error => {
                    let ec = i32::deserialize(buf)?;
                    let err = match ErrorCode::try_from(ec) {
                        Ok(ec) => Error::from(ec),
                        Err(err) => Error::UnexpectedErrorCode(err.number),
                    };
                    results.push(MultiWriteResponse::Error(err));
                },
                op => {
                    return Err(DeserializeError::UnmarshalError(format!(
                        "unexpected op code {op} in multi write response",
                    )))
                },
            }
        }
        Ok(results)
    }
}
