use log::info;

use crate::message::{CheckSum, Message};
use std::{collections::hash_map::DefaultHasher, hash::Hasher};

/// Builds the cryptographic checksum of a message (its id, sender_id and body)
pub fn compute_checksum(msg: Message) -> CheckSum {
    let mut s = DefaultHasher::new();
    s.write(&msg.header.id.to_le_bytes());
    s.write(&msg.header.sender_id.to_le_bytes());
    s.write(&msg.msg_type.to_bytes());
    let checksum = s.finish();
    info!("Generated checksum {checksum} for message {:?}", msg);
    checksum
}
