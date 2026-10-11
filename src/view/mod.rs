pub mod quorum;

use log::debug;

use crate::{
    constants::{MAX_CLUSTER_SIZE, PORT_START},
    replica::ReplicaId,
};

#[derive(Debug, Clone, Copy)]
pub struct View {
    /// Id of the current leader of the cluster.
    pub leader: ReplicaId,

    /// All known replicas in the cluster.
    pub replicas: [ReplicaId; MAX_CLUSTER_SIZE],

    /// Keep track of all opened ports in the cluster.
    /// Index is the same as in replicas
    pub occupied_ports: [u32; MAX_CLUSTER_SIZE],
}

impl View {
    /// View -> view info
    /// View info is what is sent over the network.
    pub fn to_view_info(&self) -> ViewInfo {
        ViewInfo {
            leader_id: self.leader,
            active_ports: self.occupied_ports,
        }
    }

    /// Add a new replica to the view.
    pub fn add_replica(&mut self, replica_id: ReplicaId) {
        self.replicas[replica_id as usize] = replica_id;
        self.occupied_ports[replica_id as usize] = replica_id + PORT_START as u32;
    }
}

impl From<ViewInfo> for View {
    fn from(value: ViewInfo) -> Self {
        debug!("active ports: {:?}", value.active_ports);
        let replicas: [ReplicaId; MAX_CLUSTER_SIZE] = value.active_ports.map(|port| {
            if port < PORT_START as u32 {
                port
            } else {
                port - PORT_START as u32
            }
        });
        View {
            leader: value.leader_id,
            occupied_ports: value.active_ports,
            replicas,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct ViewInfo {
    pub leader_id: ReplicaId,
    pub active_ports: [ReplicaId; MAX_CLUSTER_SIZE],
}

impl ViewInfo {
    pub const BYTE_LENGTH: usize = size_of::<ReplicaId>() * (MAX_CLUSTER_SIZE + 1);

    pub fn empty_view_info() -> Self {
        let zero_replicas = [0; MAX_CLUSTER_SIZE];
        Self::new(0, zero_replicas)
    }

    pub fn new(leader_id: ReplicaId, active_ports: [u32; MAX_CLUSTER_SIZE]) -> Self {
        Self {
            leader_id,
            active_ports,
        }
    }

    pub fn to_bytes(&self) -> [u8; Self::BYTE_LENGTH] {
        let mut bytes = [0_u8; Self::BYTE_LENGTH];
        // leader id
        bytes[..size_of::<ReplicaId>()].copy_from_slice(&self.leader_id.to_le_bytes());

        let mut idx = size_of::<ReplicaId>();
        for port in self.active_ports {
            if port != 0 {
                bytes[idx..idx + size_of::<ReplicaId>()].copy_from_slice(&port.to_le_bytes());
            }
            idx += size_of::<ReplicaId>();
        }
        bytes
    }

    pub fn from_bytes(bytes: [u8; Self::BYTE_LENGTH]) -> Option<Self> {
        let leader_id =
            ReplicaId::from_le_bytes(bytes[..size_of::<ReplicaId>()].try_into().unwrap());
        let mut replicas: [ReplicaId; MAX_CLUSTER_SIZE] = [0; MAX_CLUSTER_SIZE];
        for i in 1..=MAX_CLUSTER_SIZE {
            let idx = size_of::<ReplicaId>() * i;
            let replica = ReplicaId::from_le_bytes(
                bytes[idx..idx + size_of::<ReplicaId>()].try_into().unwrap(),
            );
            replicas[i - 1] = replica;
        }
        Some(Self::new(leader_id, replicas))
    }
}
