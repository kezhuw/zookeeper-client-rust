use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, IntoPrimitive)]
pub enum PredefinedXid {
    Notification = -1,
    Ping = -2,
    Auth = -4,
    SetWatches = -8,
}

#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
pub enum AddWatchMode {
    Persistent = 0,
    PersistentRecursive = 1,
}
