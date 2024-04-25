use std::convert::Infallible;

use bytes::{Buf, BufMut};

use super::error::Error as ZkError;

#[derive(thiserror::Error, Debug)]
pub enum DeserializeError {
    #[error("insufficient buf")]
    InsufficientBuf,

    #[error("unexpected op code: {0}")]
    UnexpectedOpCode(i32),

    #[error("unexpected error code: {0}")]
    UnexpectedErrorCode(i32),

    #[error("{0}")]
    UnmarshalError(String),
}

#[derive(thiserror::Error, Clone, Copy, Debug, PartialEq, Eq)]
pub struct InsufficientBuf;

#[derive(thiserror::Error, Debug)]
pub enum InvalidData {
    #[error("unexpected op code: {0}")]
    UnexpectedOpCode(i32),

    #[error("unexpected error code: {0}")]
    UnexpectedErrorCode(i32),

    #[error("{0}")]
    UnmarshalError(String),
}

pub trait UnmarshalError: Sized {
    fn with_context(self, context: &'static str) -> ZkError;
}

impl DeserializeError {
    pub fn with_entity(self, entity: &'static str) -> ZkError {
        match self {
            DeserializeError::InsufficientBuf => ZkError::UnmarshalError { entity, reason: &"insufficient buf" },
            DeserializeError::UnexpectedOpCode(code) => {
                ZkError::UnexpectedError(format!("unexpected op code {}", code))
            },
            DeserializeError::UnexpectedErrorCode(code) => ZkError::UnexpectedErrorCode(code),
            DeserializeError::UnmarshalError(err) => ZkError::UnexpectedError(err),
        }
    }
}

impl<T> UnmarshalError for T
where
    T: Into<DeserializeError>,
{
    fn with_context(self, context: &'static str) -> ZkError {
        let err: DeserializeError = self.into();
        err.with_entity(context)
    }
}

pub trait NotInsufficientError {
    type DeserializeError: HasInsufficientError + UnmarshalError + From<InsufficientBuf>;

    fn insufficient() -> Self::DeserializeError;

    fn to_deserialize_error(self) -> Self::DeserializeError;
}

impl NotInsufficientError for Infallible {
    type DeserializeError = InsufficientBuf;

    fn insufficient() -> Self::DeserializeError {
        InsufficientBuf
    }

    fn to_deserialize_error(self) -> Self::DeserializeError {
        InsufficientBuf
    }
}

impl NotInsufficientError for InvalidData {
    type DeserializeError = DeserializeError;

    fn insufficient() -> Self::DeserializeError {
        DeserializeError::InsufficientBuf
    }

    fn to_deserialize_error(self) -> Self::DeserializeError {
        DeserializeError::from(self)
    }
}

pub trait HasInsufficientError {
    type DataError: NotInsufficientError;

    fn to_data_error(self) -> Option<Self::DataError>;
}

impl HasInsufficientError for InsufficientBuf {
    type DataError = Infallible;

    fn to_data_error(self) -> Option<Self::DataError> {
        None
    }
}

impl HasInsufficientError for DeserializeError {
    type DataError = InvalidData;

    fn to_data_error(self) -> Option<Self::DataError> {
        match self {
            DeserializeError::InsufficientBuf => None,
            DeserializeError::UnexpectedOpCode(op_code) => Some(InvalidData::UnexpectedOpCode(op_code)),
            DeserializeError::UnexpectedErrorCode(ec) => Some(InvalidData::UnexpectedErrorCode(ec)),
            DeserializeError::UnmarshalError(reason) => Some(InvalidData::UnmarshalError(reason)),
        }
    }
}

impl std::fmt::Display for InsufficientBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "insufficient buf")
    }
}

impl From<InsufficientBuf> for DeserializeError {
    fn from(_: InsufficientBuf) -> DeserializeError {
        DeserializeError::InsufficientBuf
    }
}

impl From<InvalidData> for DeserializeError {
    fn from(err: InvalidData) -> DeserializeError {
        match err {
            InvalidData::UnexpectedOpCode(op_code) => DeserializeError::UnexpectedOpCode(op_code),
            InvalidData::UnexpectedErrorCode(ec) => DeserializeError::UnexpectedErrorCode(ec),
            InvalidData::UnmarshalError(reason) => DeserializeError::UnmarshalError(reason),
        }
    }
}

pub trait Record: SerializableRecord + DynamicRecord {}

impl<T> Record for T where T: SerializableRecord + DynamicRecord {}

pub trait SerializableRecord {
    fn serialize(&self, buf: &mut dyn BufMut);
}

pub trait StaticRecord {
    fn record_len() -> usize;
}

impl<T> DynamicRecord for T
where
    T: StaticRecord,
{
    fn serialized_len(&self) -> usize {
        T::record_len()
    }
}

pub trait DynamicRecord {
    fn serialized_len(&self) -> usize;
}

pub fn deserialize<'a, T>(buf: &mut ReadingBuf<'a>) -> Result<T, T::Error>
where
    T: DeserializableRecord<'a> + 'a, {
    T::deserialize(buf)
}

pub fn try_deserialize<'a, T>(
    buf: &mut ReadingBuf<'a>,
) -> Result<Option<T>, <<T as DeserializableRecord<'a>>::Error as HasInsufficientError>::DataError>
where
    T: DeserializableRecord<'a> + 'a, {
    T::try_deserialize(buf)
}

pub fn unmarshal<'a, T>(buf: &mut ReadingBuf<'a>) -> Result<T, ZkError>
where
    T: DeserializableRecord<'a> + 'a, {
    T::deserialize(buf).map_err(|e| e.with_context(std::any::type_name::<T>()))
}

pub fn unmarshal_entity<'a, T>(entity: &'static &'static str, buf: &mut ReadingBuf<'a>) -> Result<T, ZkError>
where
    T: DeserializableRecord<'a> + 'a, {
    T::deserialize(buf).map_err(|e| e.with_context(entity))
}

impl<T> SerializableRecord for &T
where
    T: SerializableRecord,
{
    fn serialize(&self, buf: &mut dyn BufMut) {
        (*self).serialize(buf)
    }
}

pub type ReadingBuf<'a> = &'a [u8];

pub trait UnsafeRead<'a> {
    type Error: NotInsufficientError;

    unsafe fn read(buf: &mut ReadingBuf<'a>) -> Result<Self, Self::Error>
    where
        Self: Sized + 'a;
}

impl<'a, T> DeserializableRecord<'a> for T
where
    T: UnsafeRead<'a> + StaticRecord + 'a,
{
    type Error = <<T as UnsafeRead<'a>>::Error as NotInsufficientError>::DeserializeError;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<Self, Self::Error> {
        if buf.len() < T::record_len() {
            return Err(T::Error::insufficient());
        }
        match unsafe { T::read(buf) } {
            Ok(v) => Ok(v),
            Err(err) => Err(err.to_deserialize_error()),
        }
    }
}

pub trait DeserializableRecord<'a> {
    type Error: HasInsufficientError + UnmarshalError + From<InsufficientBuf>;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<Self, Self::Error>
    where
        Self: Sized + 'a;

    fn try_deserialize(
        buf: &mut ReadingBuf<'a>,
    ) -> Result<Option<Self>, <<Self as DeserializableRecord<'a>>::Error as HasInsufficientError>::DataError>
    where
        Self: Sized + 'a, {
        let backup = *buf;
        match Self::deserialize(buf) {
            Ok(v) => Ok(Some(v)),
            Err(err) => {
                *buf = backup;
                match err.to_data_error() {
                    None => Ok(None),
                    Some(err) => Err(err),
                }
            },
        }
    }
}

impl SerializableRecord for () {
    fn serialize(&self, _buf: &mut dyn BufMut) {}
}

impl StaticRecord for () {
    fn record_len() -> usize {
        0
    }
}

impl SerializableRecord for bool {
    fn serialize(&self, buf: &mut dyn BufMut) {
        buf.put_u8(u8::from(*self));
    }
}

impl StaticRecord for bool {
    fn record_len() -> usize {
        1
    }
}

impl UnsafeRead<'_> for bool {
    type Error = InvalidData;

    unsafe fn read(buf: &mut ReadingBuf) -> Result<Self, Self::Error> {
        match buf.get_unchecked_u8() {
            0 => Ok(false),
            1 => Ok(true),
            u => Err(InvalidData::UnmarshalError(format!("invalid value {} for bool", u))),
        }
    }
}

impl SerializableRecord for i32 {
    fn serialize(&self, buf: &mut dyn BufMut) {
        buf.put_i32(*self);
    }
}

impl StaticRecord for i32 {
    fn record_len() -> usize {
        4
    }
}

impl<'a> DeserializableRecord<'a> for i32 {
    type Error = InsufficientBuf;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<Self, Self::Error> {
        if buf.len() < 4 {
            return Err(InsufficientBuf);
        }
        let v = unsafe { buf.get_unchecked_i32() };
        Ok(v)
    }
}

impl SerializableRecord for i64 {
    fn serialize(&self, buf: &mut dyn BufMut) {
        buf.put_i64(*self);
    }
}

impl StaticRecord for i64 {
    fn record_len() -> usize {
        8
    }
}

impl<'a> DeserializableRecord<'a> for i64 {
    type Error = InsufficientBuf;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<Self, Self::Error> {
        if buf.len() < 8 {
            return Err(InsufficientBuf);
        }
        let v = unsafe { buf.get_unchecked_i64() };
        Ok(v)
    }
}

impl SerializableRecord for str {
    fn serialize(&self, buf: &mut dyn BufMut) {
        let n = self.len() as i32;
        buf.put_i32(n);
        buf.put_slice(self.as_bytes());
    }
}

impl SerializableRecord for &str {
    fn serialize(&self, buf: &mut dyn BufMut) {
        let n = self.len() as i32;
        buf.put_i32(n);
        buf.put_slice(self.as_bytes());
    }
}

impl DynamicRecord for str {
    fn serialized_len(&self) -> usize {
        4 + self.len()
    }
}

impl DynamicRecord for &str {
    fn serialized_len(&self) -> usize {
        4 + self.len()
    }
}

impl<'a> DeserializableRecord<'a> for &'a str {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<&'a str, Self::Error> {
        let n = i32::deserialize(buf)?;
        if n <= 0 {
            return Ok(Default::default());
        } else if n > buf.len() as i32 {
            return Err(DeserializeError::InsufficientBuf);
        }
        let n = n as usize;
        let bytes = unsafe { buf.get_unchecked(..n) };
        let s = match std::str::from_utf8(bytes) {
            Err(err) => {
                let pos = err.valid_up_to();
                let err = match err.error_len() {
                    None => DeserializeError::UnmarshalError(format!("unexpected utf8 end after {:?}", &bytes[pos..])),
                    Some(n) => {
                        DeserializeError::UnmarshalError(format!("invalid utf8 bytes {:?}", &bytes[pos..pos + n]))
                    },
                };
                return Err(err);
            },
            Ok(s) => s,
        };
        unsafe { *buf = buf.get_unchecked(n..) };
        Ok(s)
    }
}

impl DeserializableRecord<'_> for String {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf) -> Result<String, Self::Error> {
        let s: &str = DeserializableRecord::deserialize(buf)?;
        Ok(s.to_string())
    }
}

impl SerializableRecord for [u8] {
    fn serialize(&self, buf: &mut dyn BufMut) {
        let n = self.len() as i32;
        buf.put_i32(n);
        buf.put_slice(self);
    }
}

impl DynamicRecord for [u8] {
    fn serialized_len(&self) -> usize {
        4 + self.len()
    }
}

impl<'a> DeserializableRecord<'a> for &'a [u8] {
    type Error = InsufficientBuf;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<&'a [u8], Self::Error> {
        Option::<&[u8]>::deserialize(buf).map(|opt| opt.unwrap_or_default())
    }
}

impl SerializableRecord for Option<&[u8]> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        match self {
            None => buf.put_i32(-1),
            Some(bytes) => bytes.serialize(buf),
        }
    }
}

impl DynamicRecord for Option<&[u8]> {
    fn serialized_len(&self) -> usize {
        match self {
            None => 4,
            Some(bytes) => 4 + bytes.len(),
        }
    }
}

impl<'a> DeserializableRecord<'a> for Option<&'a [u8]> {
    type Error = InsufficientBuf;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<Option<&'a [u8]>, Self::Error> {
        let n = i32::deserialize(buf)?;
        if n < 0 {
            return Ok(None);
        } else if n == 0 {
            return Ok(Some(Default::default()));
        } else if n > buf.len() as i32 {
            return Err(InsufficientBuf);
        }
        let n = n as usize;
        let bytes = unsafe { buf.get_unchecked(..n) };
        unsafe { *buf = buf.get_unchecked(n..) };
        Ok(Some(bytes))
    }
}

impl<T> SerializableRecord for [T]
where
    T: SerializableRecord,
{
    fn serialize(&self, buf: &mut dyn BufMut) {
        let n = self.len() as i32;
        n.serialize(buf);
        for v in self.iter() {
            v.serialize(buf);
        }
    }
}

impl<T> DynamicRecord for [T]
where
    T: DynamicRecord,
{
    fn serialized_len(&self) -> usize {
        let n: usize = self.iter().map(|v| v.serialized_len()).sum();
        4 + n
    }
}

impl<T> SerializableRecord for Vec<T>
where
    T: SerializableRecord,
{
    fn serialize(&self, buf: &mut dyn BufMut) {
        let n = self.len() as i32;
        n.serialize(buf);
        for v in self.iter() {
            v.serialize(buf);
        }
    }
}

impl<T> DynamicRecord for Vec<T>
where
    T: DynamicRecord,
{
    fn serialized_len(&self) -> usize {
        let n: usize = self.iter().map(|v| v.serialized_len()).sum();
        4 + n
    }
}

impl<'a, T> DeserializableRecord<'a> for Vec<T>
where
    T: DeserializableRecord<'a> + 'a,
{
    type Error = T::Error;

    fn deserialize(buf: &mut ReadingBuf<'a>) -> Result<Vec<T>, Self::Error> {
        let n = i32::deserialize(buf)?;
        if n <= 0 {
            return Ok(Default::default());
        }
        let n = n as usize;
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            v.push(T::deserialize(buf)?);
        }
        Ok(v)
    }
}

impl DeserializableRecord<'_> for Vec<u8> {
    type Error = DeserializeError;

    fn deserialize(buf: &mut ReadingBuf<'_>) -> Result<Vec<u8>, Self::Error> {
        let bytes = <&[u8]>::deserialize(buf)?;
        Ok(bytes.to_owned())
    }
}

pub trait UnsafeBuf: Buf {
    unsafe fn get_unchecked_u8(&mut self) -> u8;

    unsafe fn get_unchecked_i32(&mut self) -> i32;

    unsafe fn get_unchecked_i64(&mut self) -> i64;
}

impl UnsafeBuf for &[u8] {
    unsafe fn get_unchecked_u8(&mut self) -> u8 {
        let u = *self.get_unchecked(0);
        *self = self.get_unchecked(1..);
        u
    }

    unsafe fn get_unchecked_i32(&mut self) -> i32 {
        const LEN: usize = 4;
        let bytes = self.get_unchecked(..LEN) as *const _ as *const [_; LEN];
        *self = self.get_unchecked(LEN..);
        i32::from_be_bytes(*bytes)
    }

    unsafe fn get_unchecked_i64(&mut self) -> i64 {
        const LEN: usize = 8;
        let bytes = self.get_unchecked(..LEN) as *const _ as *const [_; LEN];
        *self = self.get_unchecked(LEN..);
        i64::from_be_bytes(*bytes)
    }
}
