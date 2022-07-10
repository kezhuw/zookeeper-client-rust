use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, IntoPrimitive)]
pub enum PredefinedXid {
    Notification = -1,
    /// ZooKeeper server [hard-code -2 as ping response xid][ping-xid], so we have to use this and make sure
    /// at most one ping in wire.
    ///
    /// ping-xid: https://github.com/apache/zookeeper/blob/de7c5869d372e46af43979134d0e30b49d2319b1/zookeeper-server/src/main/java/org/apache/zookeeper/server/FinalRequestProcessor.java#L215
    Ping = -2,

    /// Fortunately, ZooKeeper server [use xid from header](auth-xid) to reply auth request, so we can have
    /// multiple auth requets in network.
    ///
    /// auth-xid: https://github.com/apache/zookeeper/blob/de7c5869d372e46af43979134d0e30b49d2319b1/zookeeper-server/src/main/java/org/apache/zookeeper/server/ZooKeeperServer.java#L1621
    Auth = -4,
    SetWatches = -8,
}

#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
pub enum AddWatchMode {
    Persistent = 0,
    PersistentRecursive = 1,
}
