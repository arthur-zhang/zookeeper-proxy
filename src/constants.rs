use num_derive::{FromPrimitive, ToPrimitive};

pub const BOOL_LENGTH: i32 = 1;
pub const INT_LENGTH: i32 = 4;
pub const LONG_LENGTH: i32 = 8;
pub const XID_LENGTH: i32 = 4;
pub const OPCODE_LENGTH: i32 = 4;
pub const ZXID_LENGTH: i32 = 8;
pub const TIMEOUT_LENGTH: i32 = 4;
pub const SESSION_LENGTH: i32 = 8;
pub const MULTI_HEADER_LENGTH: i32 = 9;
pub const PROTOCOL_VERSION_LENGTH: i32 = 4;
pub const SERVER_HEADER_LENGTH: i32 = 16;

#[derive(Debug, ToPrimitive, FromPrimitive, Clone)]
pub enum OpCodes {
    Connect = 0,
    Create = 1,
    Delete = 2,
    Exists = 3,
    GetData = 4,
    SetData = 5,
    GetAcl = 6,
    SetAcl = 7,
    GetChildren = 8,
    Sync = 9,
    Ping = 11,
    GetChildren2 = 12,
    Check = 13,
    Multi = 14,
    Create2 = 15,
    Reconfig = 16,
    CheckWatches = 17,
    RemoveWatches = 18,
    CreateContainer = 19,
    CreateTtl = 21,
    MultiRead = 22,
    SetAuth = 100,
    SetWatches = 101,
    Sasl = 102,
    GetEphemerals = 103,
    GetAllChildrenNumber = 104,
    SetWatches2 = 105,
    AddWatch = 106,
    WhoAmI = 107,
    CreateSession = -10,
    Close = -11,
    Error = -1,
}
