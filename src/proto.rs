mod acl;
mod auth;
mod connect;
mod consts;
mod data;
mod error_code;
mod event;
mod op_code;
mod path;
mod reconfig;
mod reply_header;
mod request_header;
mod stat;

use bytes::BufMut;

pub use self::acl::{GetAclResponse, SetAclRequest};
pub use self::auth::AuthPacket;
pub use self::connect::{ConnectRequest, ConnectResponse};
pub use self::consts::{AddWatchMode, PredefinedXid};
pub use self::data::{
    CreateRequest,
    DeleteRequest,
    ExistsRequest,
    GetChildren2Response,
    GetChildrenRequest,
    GetRequest,
    OneshotWatchRequest,
    PersistentWatchRequest,
    RemoveWatchesRequest,
    SetDataRequest,
    SetWatchesRequest,
    SyncRequest,
};
pub use self::error_code::ErrorCode;
pub use self::event::WatcherEvent;
pub use self::op_code::OpCode;
pub use self::path::RootedPath;
pub use self::reconfig::{EnsembleUpdate, ReconfigRequest};
pub use self::reply_header::ReplyHeader;
pub use self::request_header::RequestHeader;
pub use self::stat::Stat;
use super::record::{DynamicRecord, SerializableRecord};
use crate::record::Record;

trait RequestBuffer {
    fn write_request(&mut self, header: &RequestHeader, record: &dyn Record) -> usize;
    fn write_lenght_prefixed_record(&mut self, record: &dyn Record) -> usize;
}

#[allow(clippy::uninit_vec)]
impl RequestBuffer for Vec<u8> {
    fn write_request(&mut self, header: &RequestHeader, record: &dyn Record) -> usize {
        let i = self.len();
        self.reserve(4);
        unsafe { self.set_len(i + 4) };
        header.serialize(self);
        record.serialize(self);
        let len = self.len() - i - 4;
        let mut buf = &mut self[i..i + 4];
        buf.put_i32(len as i32);
        len + 4
    }

    fn write_lenght_prefixed_record(&mut self, record: &dyn Record) -> usize {
        let i = self.len();
        self.reserve(4);
        unsafe { self.set_len(i + 4) };
        record.serialize(self);
        let len = self.len() - i - 4;
        let mut buf = &mut self[i..i + 4];
        buf.put_i32(len as i32);
        len + 4
    }
}

pub fn build_record_request(record: &dyn Record) -> Vec<u8> {
    let mut buf = Vec::with_capacity(record.serialized_len() + 4);
    buf.write_lenght_prefixed_record(record);
    buf
}

pub fn build_session_request(header: &RequestHeader, body: &dyn Record) -> Vec<u8> {
    let mut buf = Vec::with_capacity(header.serialized_len() + body.serialized_len() + 4);
    buf.write_request(header, body);
    buf
}
