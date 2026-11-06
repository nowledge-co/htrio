//! Bounded HTTP priority signaling (RFC 9218).

use crate::proto::varint::BufMutExt;
use crate::shared_state::{ConnectionState, SharedState};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;

/// HTTP urgency and incremental delivery preference.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Priority {
    urgency: u8,
    incremental: bool,
}

impl Default for Priority {
    fn default() -> Self {
        Self {
            urgency: 3,
            incremental: false,
        }
    }
}
impl Priority {
    /// Construct an urgency in 0..=7 (0 is most urgent).
    pub const fn new(urgency: u8, incremental: bool) -> Option<Self> {
        if urgency > 7 {
            None
        } else {
            Some(Self {
                urgency,
                incremental,
            })
        }
    }
    /// The urgency value.
    pub const fn urgency(self) -> u8 {
        self.urgency
    }
    /// Whether the response can be processed incrementally.
    pub const fn incremental(self) -> bool {
        self.incremental
    }
    /// A structured Priority field value.
    pub fn field_value(self) -> String {
        if self.incremental {
            format!("u={}, i", self.urgency)
        } else {
            format!("u={}", self.urgency)
        }
    }
    /// Mapping to a QUIC send scheduler where larger numbers run first.
    pub fn quic_priority(self) -> i32 {
        7 - i32::from(self.urgency)
    }
}

/// A request-scoped handle for reprioritization on the connection control stream.
#[derive(Clone, Debug)]
pub struct Handle {
    pub(crate) shared: Arc<SharedState>,
    pub(crate) id: u64,
}
impl Handle {
    /// Queue the latest priority. Returns false after closure or at the queue bound.
    pub fn update(&self, priority: Priority) -> bool {
        if self.shared.is_closing() || self.shared.get_conn_error().is_some() {
            return false;
        }
        let mut pending = self.shared.priorities.lock().unwrap();
        if pending.len() >= 256 && !pending.contains_key(&self.id) {
            return false;
        }
        pending.insert(self.id, priority);
        self.shared.waker().wake();
        true
    }
    /// The client-initiated request stream identifier.
    pub fn stream_id(&self) -> u64 {
        self.id
    }
}

pub(crate) fn encode(id: u64, priority: Priority) -> Bytes {
    let field = priority.field_value();
    let mut payload = BytesMut::new();
    payload.write_var(id);
    payload.extend_from_slice(field.as_bytes());
    let mut wire = BytesMut::new();
    wire.write_var(0xf0700);
    wire.write_var(payload.len() as u64);
    wire.extend_from_slice(&payload);
    wire.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::varint::BufExt;
    #[test]
    fn priority_update_wire_format_and_queue_coalescing() {
        let priority = Priority::new(0, true).unwrap();
        let mut wire = encode(4, priority);
        assert_eq!(wire.get_var().unwrap(), 0xf0700);
        assert_eq!(wire.get_var().unwrap(), 7);
        assert_eq!(wire.get_var().unwrap(), 4);
        assert_eq!(wire, "u=0, i");
        assert!(Priority::new(8, false).is_none());
        let shared = Arc::new(SharedState::default());
        let handle = Handle {
            shared: shared.clone(),
            id: 4,
        };
        for urgency in 0..8 {
            assert!(handle.update(Priority::new(urgency, false).unwrap()));
        }
        assert_eq!(shared.priorities.lock().unwrap().len(), 1);
        shared.set_closing();
        assert!(!handle.update(priority));
    }
}
