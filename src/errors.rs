#[derive(Debug)]
pub enum ZkError {
    SocketError(String),
    InvalidPacketLength(i32),
    InvalidOpCode(i32),
    DecodeError,
    EncodeError,
    InternalError,
    IoError,
    InvalidString,
}

impl From<std::io::Error> for ZkError {
    fn from(e: std::io::Error) -> Self {
        ZkError::SocketError(e.to_string())
    }
}
