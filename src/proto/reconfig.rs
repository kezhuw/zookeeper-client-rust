use bytes::BufMut;

use crate::record::{DynamicRecord, SerializableRecord};

struct ServerList<'a, 'b: 'a, T: Iterator<Item = &'b str> + Clone>(&'a T);

impl<'a, 'b, T: Iterator<Item = &'b str> + Clone> SerializableRecord for ServerList<'a, 'b, T> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        let n = self.serialized_len();
        if n == 4 {
            buf.put_i32(-1);
            return;
        }
        buf.put_i32(n as i32 - 4);
        self.0.clone().filter(|s| !s.is_empty()).enumerate().for_each(|(i, s)| {
            if i != 0 {
                buf.put_u8(b',');
            }
            buf.put_slice(s.as_bytes());
        });
    }
}

impl<'a, 'b, T: Iterator<Item = &'b str> + Clone> DynamicRecord for ServerList<'a, 'b, T> {
    fn serialized_len(&self) -> usize {
        let n: usize = self.0.clone().filter(|s| !s.is_empty()).map(|s| s.len() + 1).sum();
        return 4 + if n > 0 { n - 1 } else { 0 };
    }
}

/// EnsembleUpdate specifies an update to ZooKeeper ensemble membership.
///
/// The item could be single server or comma separated server list.
pub enum EnsembleUpdate<'a, T: Iterator<Item = &'a str> + Clone> {
    Incremental {
        /// Joining servers.
        joinings: T,

        /// Leaving servers.
        leavings: T,
    },
    New {
        /// New ensemble.
        ensemble: T,
    },
}

pub struct ReconfigRequest<'a, T: Iterator<Item = &'a str> + Clone> {
    pub update: EnsembleUpdate<'a, T>,
    pub version: i32,
}

impl<'a, T: Iterator<Item = &'a str> + Clone> SerializableRecord for ReconfigRequest<'a, T> {
    fn serialize(&self, buf: &mut dyn BufMut) {
        match &self.update {
            EnsembleUpdate::Incremental { joinings, leavings } => {
                ServerList(joinings).serialize(buf);
                ServerList(leavings).serialize(buf);
                buf.put_i32(-1);
            },
            EnsembleUpdate::New { ensemble } => {
                buf.put_i32(-1);
                buf.put_i32(-1);
                ServerList(ensemble).serialize(buf);
            },
        };
        self.version.serialize(buf);
    }
}

impl<'a, T: Iterator<Item = &'a str> + Clone> DynamicRecord for ReconfigRequest<'a, T> {
    fn serialized_len(&self) -> usize {
        let n = match &self.update {
            EnsembleUpdate::Incremental { joinings, leavings } => {
                4 + ServerList(joinings).serialized_len() + ServerList(leavings).serialized_len()
            },
            EnsembleUpdate::New { ensemble } => {
                let servers = ServerList(ensemble);
                8 + servers.serialized_len()
            },
        };
        n + 4
    }
}
