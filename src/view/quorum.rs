use std::ops::Index;

use crate::{constants::MAX_REQUIRED_VOTES, replica::ReplicaId};

#[derive(Debug)]
pub enum VotingStatus {
    Idle,
    Voting(Quorum),
}

#[derive(Debug)]
pub struct Quorum {
    /// Number of votes required to reach quorum.
    /// Excluding the primary.
    required_votes: u16,

    /// Keep track of which ids voted to prevent double votes.
    /// This also provides the number of votes.
    ids_voted: [ReplicaId; MAX_REQUIRED_VOTES],

    /// Keep track of start time to allow timeouts.
    start: std::time::Instant,
}

impl Quorum {
    pub fn new(required_votes: u16) -> Self {
        Self {
            required_votes,
            ids_voted: [0; MAX_REQUIRED_VOTES],
            start: std::time::Instant::now(),
        }
    }

    pub fn register_new_vote(&mut self, voter: ReplicaId) {
        if !self.ids_voted.contains(&voter) {
            let pos = self
                .ids_voted
                .iter()
                .position(|id| *id == 0)
                .expect("No space in ids_voted array of quorum");
            self.ids_voted[pos] = voter;
        }
    }

    pub fn reached_quorum(&self) -> bool {
        let count = self.ids_voted.iter().filter(|id| **id == 0).count();
        count >= self.required_votes as usize
    }
}
