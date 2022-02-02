use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, IntoPrimitive, TryFromPrimitive, strum::Display)]
pub enum OpCode {
    Notification = 0,
    Create = 1,
    Delete = 2,
    Exists = 3,
    GetData = 4,
    SetData = 5,
    GetACL = 6,
    SetACL = 7,
    GetChildren = 8,
    Sync = 9,
    Ping = 11,
    GetChildren2 = 12,
    Check = 13,
    Multi = 14,
    Create2 = 15,
    Reconfig = 16,
    CheckWatches = 17,
    RemoveWatches = 18,
    CreateContainer = 19,
    DeleteContainer = 20,
    CreateTtl = 21,
    MultiRead = 22,
    Auth = 100,
    SetWatches = 101,
    Sasl = 102,
    GetEphemerals = 103,
    GetAllChildrenNumber = 104,
    SetWatches2 = 105,
    AddWatch = 106,
    WhoAmI = 107,
    CreateSession = -10,
    CloseSession = -11,
    Error = -1,
}

#[cfg(test)]
mod tests {
    use super::OpCode;

    #[test]
    fn test_display() {
        let s = format!("{}", OpCode::Auth);
        assert_eq!(s, "Auth");
    }
}
