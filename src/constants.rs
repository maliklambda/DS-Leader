use std::{net::Ipv4Addr, time::Duration};

pub const MAX_CLUSTER_SIZE: usize = 5;

/// A new node must choose the first available port above this value.
/// So the highest port occupied by the system is PORT_START+MAX_CLUSTER_SIZE
pub const PORT_START: u16 = 8000;

/// The upper bound on the messages array for a replica.
/// Each replica may hold at most this many messages.
pub const MAX_MESSAGES: usize = 10;

/// The maximum number of messages that are received per tick.
pub const MAX_MESSAGES_PER_TICK: usize = 10;

/// Number of milliseconds a replica runs for
pub const MILLISECONDS: usize = 10;

// Localhost is used for virtually everything now.
pub const LOCALHOST: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);

/// set a timeout on receiving udp packets
pub const READ_TIMEOUT: Duration = Duration::from_millis(10);

/// Maximum required votes for a quroum.
/// A quroum cannot require more votes than this.
pub const MAX_REQUIRED_VOTES: usize = MAX_CLUSTER_SIZE / 2;
