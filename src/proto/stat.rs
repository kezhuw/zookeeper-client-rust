use bytes::BufMut;

use crate::record::{ReadingBuf, SerializableRecord, StaticRecord, UnsafeBuf, UnsafeRead};

/// ZooKeeper node stat.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stat {
    /// The zxid of the change that caused this znode to be created.
    pub czxid: i64,

    /// The zxid of the change that last modified this znode.
    pub mzxid: i64,

    /// The zxid of the change that last modified children of this znode.
    pub pzxid: i64,

    /// The time in milliseconds from epoch when this znode was created.
    pub ctime: i64,

    /// The time in milliseconds from epoch when this znode was last modified.
    pub mtime: i64,

    /// The number of changes to the data of this znode.
    pub version: i32,

    /// The number of changes to the children of this znode.
    pub cversion: i32,

    /// The number of changes to the ACL of this znode.
    pub aversion: i32,

    /// The session id of the owner of this znode if the znode is an ephemeral node. If it is not an ephemeral node, it will be zero.
    pub ephemeral_owner: i64,

    /// The length of the data field of this znode.
    pub data_length: i32,

    /// The number of children of this znode.
    pub num_children: i32,
}

impl SerializableRecord for Stat {
    fn serialize(&self, buf: &mut dyn BufMut) {
        buf.put_i64(self.czxid);
        buf.put_i64(self.mzxid);
        buf.put_i64(self.ctime);
        buf.put_i64(self.mtime);
        buf.put_i32(self.version);
        buf.put_i32(self.cversion);
        buf.put_i32(self.aversion);
        buf.put_i64(self.ephemeral_owner);
        buf.put_i32(self.data_length);
        buf.put_i32(self.num_children);
        buf.put_i64(self.pzxid);
    }
}

impl StaticRecord for Stat {
    fn record_len() -> usize {
        return 68;
    }
}

impl UnsafeRead<'_> for Stat {
    type Error = std::convert::Infallible;

    unsafe fn read(buf: &mut ReadingBuf) -> Result<Self, Self::Error> {
        let czxid = buf.get_unchecked_i64();
        let mzxid = buf.get_unchecked_i64();
        let ctime = buf.get_unchecked_i64();
        let mtime = buf.get_unchecked_i64();
        let version = buf.get_unchecked_i32();
        let cversion = buf.get_unchecked_i32();
        let aversion = buf.get_unchecked_i32();
        let ephemeral_owner = buf.get_unchecked_i64();
        let data_length = buf.get_unchecked_i32();
        let num_children = buf.get_unchecked_i32();
        let pzxid = buf.get_unchecked_i64();
        return Ok(Stat {
            czxid,
            mzxid,
            ctime,
            mtime,
            version,
            cversion,
            aversion,
            ephemeral_owner,
            data_length,
            num_children,
            pzxid,
        });
    }
}

#[cfg(test)]
mod tests {
    use rand::distributions::Standard;
    use rand::{self, Rng};

    use super::Stat;
    use crate::record::{self, DeserializableRecord, SerializableRecord, StaticRecord};

    #[test]
    fn test_insufficient_buf() {
        let rng = rand::thread_rng();
        let data: Vec<u8> = rng.sample_iter(Standard).take(Stat::record_len() - 1).collect();
        let mut buf = data.as_slice();
        let err = Stat::deserialize(&mut buf).unwrap_err();
        assert_eq!(err, record::InsufficientBuf);
    }

    #[test]
    fn test_serde() {
        let mut rng = rand::thread_rng();
        let stat = Stat {
            czxid: rng.gen(),
            mzxid: rng.gen(),
            ctime: rng.gen(),
            mtime: rng.gen(),
            version: rng.gen(),
            cversion: rng.gen(),
            aversion: rng.gen(),
            ephemeral_owner: rng.gen(),
            data_length: rng.gen(),
            num_children: rng.gen(),
            pzxid: rng.gen(),
        };
        let mut data = Vec::new();
        stat.serialize(&mut data);
        assert_eq!(data.len(), Stat::record_len());
        let mut buf = data.as_slice();
        let deserialized = Stat::deserialize(&mut buf).unwrap();
        assert_eq!(deserialized, stat);
        assert!(buf.is_empty());
    }
}
