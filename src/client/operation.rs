use bytes::{Buf, BufMut};
use tokio::sync::{mpsc, oneshot};

use super::types::WatchedEvent;
use crate::error::Error;
use crate::proto::{self, AddWatchMode, ConnectRequest, OpCode, RequestHeader};
use crate::record::{self, Record, StaticRecord};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchMode {
    Oneshot,
    Persistent,
    PersistentRecursive,
}

impl MarshalledRequest {
    pub fn new_request(code: OpCode, body: &dyn Record) -> MarshalledRequest {
        let header = RequestHeader::with_code(code);
        let buf = proto::build_session_request(&header, body);
        return MarshalledRequest(buf);
    }

    pub fn new_record(body: &dyn Record) -> MarshalledRequest {
        return MarshalledRequest(proto::build_record_request(body));
    }

    pub fn as_slice(&self) -> &[u8] {
        return self.0.as_slice();
    }

    pub fn get_code(&self) -> OpCode {
        let mut buf = &self.0[8..12];
        return buf.get_i32().try_into().unwrap();
    }

    pub fn get_xid(&self) -> i32 {
        let mut xid_buf = &self.0[4..8];
        return xid_buf.get_i32();
    }

    pub fn set_xid(&mut self, xid: i32) {
        let mut xid_buf = &mut self.0[4..8];
        xid_buf.put_i32(xid);
    }

    pub fn get_operation_info(&self) -> (OpCode, Option<(&str, WatchMode)>) {
        let op_code = self.get_code();
        let watcher_info = match op_code {
            OpCode::GetData | OpCode::Exists | OpCode::GetChildren | OpCode::GetChildren2 | OpCode::AddWatch => {
                let offset = 4 + RequestHeader::record_len();
                let mut body = &self.0[offset..];
                let server_path = record::deserialize::<&str>(&mut body).unwrap();
                if op_code == OpCode::AddWatch {
                    body.advance(3);
                }
                let watch = body.get_u8();
                assert!(watch == 0 || watch == 1);
                if op_code == OpCode::AddWatch {
                    if watch == i32::from(AddWatchMode::Persistent) as u8 {
                        Some((server_path, WatchMode::Persistent))
                    } else {
                        assert!(watch == i32::from(AddWatchMode::PersistentRecursive) as u8);
                        Some((server_path, WatchMode::PersistentRecursive))
                    }
                } else if watch == 1 {
                    Some((server_path, WatchMode::Oneshot))
                } else {
                    None
                }
            },
            _ => None,
        };
        return (op_code, watcher_info);
    }
}

pub enum Operation {
    Connect(ConnectOperation),
    Auth(AuthOperation),
    Session(SessionOperation),
}

impl Operation {
    pub fn get_data(&self) -> &[u8] {
        match self {
            Operation::Connect(operation) => operation.request.as_slice(),
            Operation::Session(operation) => operation.request.as_slice(),
            Operation::Auth(operation) => operation.request.as_slice(),
        }
    }
}

pub struct ConnectOperation {
    pub request: Vec<u8>,
}

pub struct AuthOperation {
    pub request: MarshalledRequest,
}

#[derive(Debug)]
pub struct MarshalledRequest(pub Vec<u8>);

#[derive(Debug)]
pub struct SessionOperation {
    pub request: MarshalledRequest,
    pub responser: StateResponser,
}

#[derive(Debug)]
pub enum WatchReceiver {
    None,
    Oneshot(oneshot::Receiver<WatchedEvent>),
    Persistent(mpsc::UnboundedReceiver<WatchedEvent>),
}

pub type OneshotReceiver = oneshot::Receiver<WatchedEvent>;
pub type PersistentReceiver = mpsc::UnboundedReceiver<WatchedEvent>;

impl WatchReceiver {
    pub fn is_none(&self) -> bool {
        matches!(self, WatchReceiver::None)
    }
}

pub type StateReceiver = oneshot::Receiver<Result<(Vec<u8>, WatchReceiver), Error>>;
pub type StateResponser = oneshot::Sender<Result<(Vec<u8>, WatchReceiver), Error>>;

pub fn build_connect_operation(request: &ConnectRequest) -> ConnectOperation {
    let buf = proto::build_record_request(request);
    return ConnectOperation { request: buf };
}

pub fn build_auth_operation(code: OpCode, body: &dyn Record) -> AuthOperation {
    let request = MarshalledRequest::new_request(code, body);
    return AuthOperation { request };
}

pub fn build_state_operation(code: OpCode, body: &dyn Record) -> (SessionOperation, StateReceiver) {
    let request = MarshalledRequest::new_request(code, body);
    let (sender, receiver) = oneshot::channel();
    let operation = SessionOperation { request, responser: sender };
    return (operation, receiver);
}

pub fn build_session_operation(request: &dyn Record) -> SessionOperation {
    let request = MarshalledRequest::new_record(request);
    let (sender, _) = oneshot::channel();
    let operation = SessionOperation { request, responser: sender };
    return operation;
}
