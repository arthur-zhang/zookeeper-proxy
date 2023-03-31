use crate::errors::ZkError;
use std::fmt::Debug;

pub trait Deserialize: Sized {
    fn deserialize(bytes: &mut bytes::BytesMut) -> Result<Self, ZkError>;
}

pub trait Serialize: Debug + Send + Sync {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError>;
    fn size(&self) -> usize;
}
