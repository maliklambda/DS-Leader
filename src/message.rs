use std::time::SystemTime;

use crate::{
    checksum::compute_checksum, constants::MAX_CLUSTER_SIZE, prepare::Prepare, replica::ReplicaId,
    view::ViewInfo,
};
use log::{debug, info};

pub type CheckSum = u64;

#[derive(Debug, Clone, Copy)]
pub struct Message {
    pub header: MessageHeader,
    pub msg_type: MessageType,
}

impl Message {
    pub const BYTE_LENGTH: usize = MessageHeader::BYTE_LENGTH + MessageType::BYTE_LENGTH;

    pub fn new(sender_id: ReplicaId, msg_type: MessageType, msg_id: u64) -> Self {
        let mut msg = Message {
            header: MessageHeader::new_empty_checksum(sender_id, msg_id),
            msg_type,
        };
        let checksum = compute_checksum(msg);
        msg.header.checksum = checksum;
        assert_ne!(msg.header.checksum, 0);
        msg
    }

    pub fn checksum_matches(&self) -> bool {
        let given = self.header.checksum;
        let actual = compute_checksum(*self);
        debug!("Checksum assertion - given: {given}, actual: {actual}");
        given == actual
    }

    pub fn to_bytes(&self) -> [u8; Self::BYTE_LENGTH] {
        let b_header = self.header.to_bytes();
        let b_message_type = self.msg_type.to_bytes();
        assert_eq!(b_header.len() + b_message_type.len(), Self::BYTE_LENGTH);
        let mut b_msg = [0_u8; Self::BYTE_LENGTH];
        b_msg[0..MessageHeader::BYTE_LENGTH].copy_from_slice(&b_header);
        b_msg[MessageHeader::BYTE_LENGTH..].copy_from_slice(&b_message_type);
        b_msg
    }

    pub fn from_bytes(bytes: [u8; Self::BYTE_LENGTH]) -> Option<Self> {
        let msg_header =
            MessageHeader::from_bytes(bytes[..MessageHeader::BYTE_LENGTH].try_into().unwrap());
        info!("header: {:?}", msg_header);
        let msg_type: MessageType =
            MessageType::from_bytes_full(bytes[MessageHeader::BYTE_LENGTH..].try_into().unwrap())
                .unwrap();
        info!("body: {:?}", msg_type);
        let msg = Message {
            header: msg_header,
            msg_type,
        };
        Some(msg)
    }

    pub fn zero_message() -> Self {
        Self {
            header: MessageHeader {
                id: 0,
                sender_id: 0,
                checksum: 0,
            },
            msg_type: MessageType::ViewInfoRequest,
        }
    }
}

pub type MessageId = u64;

#[derive(Debug, Clone, Copy)]
pub struct MessageHeader {
    /// Unique snowflake message id.
    /// computed using timestamp & replica id.
    pub id: MessageId,

    /// Id of the sender.
    pub sender_id: ReplicaId,

    /// checksum is computed for both the message id and the message type (or body).
    /// See function checksum(msg: Message) for more details
    pub checksum: CheckSum,
}

impl MessageHeader {
    pub const BYTE_LENGTH: usize =
        size_of::<u64>() + size_of::<ReplicaId>() + size_of::<CheckSum>();

    /// Returns a message header with an empty checksum
    pub fn new_empty_checksum(sender_id: ReplicaId, msg_id: MessageId) -> Self {
        Self {
            id: msg_id,
            sender_id,
            checksum: 0,
        }
    }

    pub fn to_bytes(&self) -> [u8; Self::BYTE_LENGTH] {
        let mut bytes = [0_u8; Self::BYTE_LENGTH];
        let mut idx = 0;
        {
            bytes[idx..idx + size_of::<MessageId>()].copy_from_slice(&self.id.to_le_bytes());
            idx += size_of::<MessageId>();
        }
        {
            bytes[idx..idx + size_of::<ReplicaId>()].copy_from_slice(&self.sender_id.to_le_bytes());
            idx += size_of::<ReplicaId>();
        }
        {
            bytes[idx..idx + size_of::<CheckSum>()].copy_from_slice(&self.checksum.to_le_bytes());
            idx += size_of::<CheckSum>();
        }
        assert_eq!(idx, Self::BYTE_LENGTH);
        bytes
    }

    pub fn from_bytes(bytes: &[u8; Self::BYTE_LENGTH]) -> Self {
        let mut idx = 0;
        let id = {
            let id = u64::from_le_bytes(bytes[idx..idx + size_of::<u64>()].try_into().unwrap());
            idx += size_of::<u64>();
            id
        };
        let sender_id = {
            let sender_id =
                u32::from_le_bytes(bytes[idx..idx + size_of::<u32>()].try_into().unwrap());
            idx += size_of::<u32>();
            sender_id
        };
        let checksum = {
            let checksum = CheckSum::from_le_bytes(
                bytes[idx..idx + size_of::<CheckSum>()].try_into().unwrap(),
            );
            idx += size_of::<CheckSum>();
            checksum
        };
        assert_eq!(idx, Self::BYTE_LENGTH);
        Self {
            id,
            sender_id,
            checksum,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageType {
    /// Broadcast message. Asks other replicas who the primary of the view is.
    ViewInfoRequest,

    /// Response to WhoIsPrimary. Contains the (view-) id of the node that is elected as leader.
    /// TODO: send more than just the leader's id -> send entire view info
    ViewInfoResponse(ViewInfo),

    /// Broadcast message. Asks other replicas to accept this node as a leader.
    IWantToBePrimary(u64),

    /// Response to IWantToBePrimary.
    /// Acknowledges the requesting node as leader.
    /// May not vote for anyone else after that.
    IAcceptYourAuthority,

    /// New node in cluster requests a view change
    /// (i.e. to be added to the cluster).
    /// That request is sent to the primary only.
    /// The primary must distribute it.
    /// If a quorum accepts it, the primary responds to the node with success.
    RequestViewChange(ViewInfo),

    /// Primary's response to a view change request from a replica.
    /// I sent only after primary receives a quorum.
    RequestViewChangeOk,

    /// Primary commands all other replicas to change the current view.
    ViewChange(ViewInfo),

    /// Replica responds to primary. View change is accepted.
    ViewChangeOk,

    /// Sent by primary to get quorum of replicas to accept a new prepare.
    NewPrepare(Prepare),

    /// Response to NewPrepare. Replica acknowledges the new prepare
    PrepareOk,

    /// Heartbeat and its sender
    /// TODO: Make this a separate type
    /// TODO: Add timestamp to it
    Heartbeat,
}

impl From<&MessageType> for u8 {
    fn from(value: &MessageType) -> Self {
        match value {
            MessageType::ViewInfoRequest => 1,
            MessageType::ViewInfoResponse(_) => 2,
            MessageType::IWantToBePrimary(_) => 3,
            MessageType::IAcceptYourAuthority => 4,
            MessageType::NewPrepare(_) => 5,
            MessageType::PrepareOk => 6,
            MessageType::RequestViewChange(_) => 7,
            MessageType::RequestViewChangeOk => 8,
            MessageType::ViewChange(_) => 9,
            MessageType::ViewChangeOk => 10,
            MessageType::Heartbeat => 255,
        }
    }
}

#[derive(Debug)]
pub struct UnknownMessageType {
    unknown_val: u8,
}

impl TryFrom<u8> for MessageType {
    type Error = UnknownMessageType;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let mt = match value {
            1 => MessageType::ViewInfoRequest,
            2 => MessageType::ViewInfoResponse(ViewInfo::empty_view_info()),
            3 => MessageType::IWantToBePrimary(0),
            4 => MessageType::IAcceptYourAuthority,
            5 => MessageType::NewPrepare(Prepare::empty_prepare()),
            6 => MessageType::PrepareOk,
            7 => MessageType::RequestViewChange(ViewInfo::empty_view_info()),
            8 => MessageType::RequestViewChangeOk,
            9 => MessageType::ViewChange(ViewInfo::empty_view_info()),
            10 => MessageType::ViewChangeOk,
            255 => MessageType::Heartbeat,
            unknown_val => return Err(UnknownMessageType { unknown_val }),
        };
        Ok(mt)
    }
}

impl MessageType {
    pub const BYTE_LENGTH: usize = 100;
    pub fn to_checksum(&self) -> CheckSum {
        todo!("msg type -> checksum")
    }

    pub fn to_bytes(&self) -> [u8; Self::BYTE_LENGTH] {
        let mut b = [0_u8; Self::BYTE_LENGTH];

        // set the first byte as message type
        b[0] = self.into();
        match self {
            Self::ViewInfoResponse(view_info) => {
                b[1..1 + size_of::<ViewInfo>()].copy_from_slice(&view_info.to_bytes())
            }
            Self::IWantToBePrimary(self_id) => {
                b[1..1 + size_of::<u64>()].copy_from_slice(&self_id.to_le_bytes())
            }
            Self::NewPrepare(prepare) => {
                b[1..1 + Prepare::BYTE_LENGTH].copy_from_slice(&prepare.to_bytes())
            }
            _ => (), // no further operation needed
        };
        b
    }

    pub fn from_bytes_full(bytes: [u8; Self::BYTE_LENGTH]) -> Option<Self> {
        let msg_type = MessageType::try_from(bytes[0]).ok()?;
        info!("Message is of type: {:?}", msg_type);
        match msg_type {
            Self::ViewInfoResponse(_) => Some(Self::ViewInfoResponse(ViewInfo::from_bytes(
                bytes[1..1 + size_of::<ViewInfo>()].try_into().unwrap(),
            )?)),
            Self::IWantToBePrimary(_) => Some(Self::IWantToBePrimary(u64::from_le_bytes(
                bytes[1..1 + size_of::<u64>()].try_into().unwrap(),
            ))),
            Self::NewPrepare(_) => Some(Self::NewPrepare(Prepare::from_bytes(
                bytes[1..1 + Prepare::BYTE_LENGTH].try_into().unwrap(),
            ))),
            Self::RequestViewChange(_) => Some(Self::RequestViewChange(ViewInfo::from_bytes(
                bytes[1..1 + size_of::<ViewInfo>()].try_into().unwrap(),
            )?)),
            Self::ViewChange(_) => Some(Self::ViewChange(ViewInfo::from_bytes(
                bytes[1..1 + size_of::<ViewInfo>()].try_into().unwrap(),
            )?)),
            _ => Some(msg_type), // no further operation needed
        }
    }
}
