/// This is the only message type.
/// The client requests a prepare that is accepted by the primary and replicated accross the
/// cluster.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Prepare {}

impl Prepare {
    pub const BYTE_LENGTH: usize = 10;

    pub fn to_bytes(&self) -> [u8; Self::BYTE_LENGTH] {
        todo!("prepare -> bytes")
    }

    pub fn from_bytes(_bytes: &[u8; Self::BYTE_LENGTH]) -> Self {
        todo!("bytes -> prepare")
    }

    pub fn empty_prepare() -> Self {
        Prepare {}
    }
}
