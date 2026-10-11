use log::debug;

use crate::{message::MessageId, replica::ReplicaId};

pub fn generate_snowflake_id(replica_id: ReplicaId) -> MessageId {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Since the new Id has space for 10 bits on the right,
    // a value greater or equal 2^10 = 1024 cannot be represented (would default to ten 0s).
    // This assertion mitigates the case that its overflown.
    assert!(replica_id < 1024);

    let new_id = (now << 10) | (replica_id as u64);
    debug!(
        "Generated new id: {new_id} (ts: {}, id: {})",
        (now << 10),
        replica_id as u64
    );
    new_id
}
