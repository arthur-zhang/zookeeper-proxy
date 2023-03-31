use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use dashmap::DashMap;
use lazy_static::lazy_static;
use log::{debug, error, info};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use tokio_util::codec::{Decoder, Encoder};

use crate::constants::*;
use crate::errors::ZkError;
use crate::length_codec::LengthDelimitedCodec;
use crate::proto::*;
use crate::record::{Deserialize, Serialize};
use crate::zk_errcode::ZooErrors;

#[derive(Debug)]
pub enum Request {
    Connect(ConnectRequest),
    Auth(AuthPacket),
    Create(CreateRequest),
    CreateTTL(CreateTTLRequest),
    Delete(DeleteRequest),
    Exists(ExistsRequest),
    GetData(GetDataRequest),
    SetData(SetDataRequest),
    GetAcl(GetACLRequest),
    SetAcl(SetACLRequest),
    GetChildren(GetChildrenRequest),
    GetChildren2(GetChildren2Request),
    Ping,
    Whoami,
    GetChildren3(GetChildrenRequest),
    Multi(MultiRequest),
    Close,
    SetWatches(SetWatches),
    SetWatches2(SetWatches2),
    CheckWatches(CheckWatchesRequest),
    Sync(SyncRequest),
    CheckVersion(CheckVersionRequest),
    ReConfig(ReconfigRequest),
    RemoveWatches(RemoveWatchesRequest),
    GetEphemerals(GetEphemeralsRequest),
    GetAllChildrenNumber(GetAllChildrenNumberRequest),
    AddWatch(AddWatchRequest),
    UnSupported,
}

impl Request {
    pub fn path(&self) -> Option<std::string::String> {
        return match self {
            Request::Ping | Request::Connect(_) | Request::Auth(_) | Request::Whoami | Request::Close | Request::ReConfig(_) | Request::UnSupported => {
                None
            }
            Request::Create(req) => {
                Some(req.path.clone())
            }
            Request::CreateTTL(req) => {
                Some(req.path.clone())
            }
            Request::Delete(req) => {
                Some(req.path.clone())
            }
            Request::Exists(req) => {
                Some(req.path.clone())
            }
            Request::GetData(req) => {
                Some(req.path.clone())
            }
            Request::SetData(req) => {
                Some(req.path.clone())
            }
            Request::GetAcl(req) => {
                Some(req.path.clone())
            }
            Request::SetAcl(req) => {
                Some(req.path.clone())
            }
            Request::GetChildren(req) => {
                Some(req.path.clone())
            }
            Request::GetChildren2(req) => {
                Some(req.path.clone())
            }
            Request::GetChildren3(req) => {
                Some(req.path.clone())
            }
            Request::Multi(_) => {
                None
            }
            Request::SetWatches(_) => {
                None
            }
            Request::SetWatches2(_) => {
                None
            }
            Request::CheckWatches(req) => {
                Some(req.path.clone())
            }
            Request::Sync(req) => {
                Some(req.path.clone())
            }
            Request::CheckVersion(req) => {
                Some(req.path.clone())
            }
            Request::RemoveWatches(req) => {
                Some(req.path.clone())
            }
            Request::GetEphemerals(_) => {
                None
            }
            Request::GetAllChildrenNumber(req) => {
                Some(req.path.clone())
            }
            Request::AddWatch(req) => {
                Some(req.path.clone())
            }
        };
    }
}

#[derive(Debug)]
pub struct MultiRequest {
    ops: Vec<(MultiHeader, Request)>,
    done_header: MultiHeader,
}

impl Serialize for MultiRequest {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        for (header, req) in &self.ops {
            header.serialize_into(buffer)?;
            req.serialize_into(buffer)?;
        }
        self.done_header.serialize_into(buffer)?;
        Ok(())
    }

    fn size(&self) -> usize {
        let mut size = 0;
        for (header, req) in &self.ops {
            size += header.size();
            size += req.size();
        }
        size += self.done_header.size();
        size
    }
}

impl Deserialize for MultiRequest {
    fn deserialize(buffer: &mut BytesMut) -> Result<Self, ZkError> {
        let mut ops = Vec::new();
        let done_header: MultiHeader;
        loop {
            let header = MultiHeader::deserialize(buffer)?;
            if header.done == 1 {
                done_header = header;
                break;
            }
            let opcode =
                OpCodes::from_i32(header.r#type).ok_or(ZkError::InvalidOpCode(header.r#type))?;

            let req = Request::deserialize(buffer, opcode)?;
            ops.push((header, req));
        }
        Ok(MultiRequest { ops, done_header })
    }
}

impl Request {
    fn deserialize(bytes: &mut BytesMut, op_type: OpCodes) -> Result<Self, ZkError> {
        match op_type {
            OpCodes::Create => {
                return Ok(Request::Create(CreateRequest::deserialize(bytes)?));
            }
            OpCodes::Create2 => {
                return Ok(Request::Create(CreateRequest::deserialize(bytes)?));
            }
            OpCodes::CreateTtl => {
                return Ok(Request::CreateTTL(CreateTTLRequest::deserialize(bytes)?));
            }
            OpCodes::CreateContainer => {
                return Ok(Request::Create(CreateRequest::deserialize(bytes)?));
            }
            OpCodes::Delete => {
                return Ok(Request::Delete(DeleteRequest::deserialize(bytes)?));
            }
            OpCodes::SetData => {
                return Ok(Request::SetData(SetDataRequest::deserialize(bytes)?));
            }
            OpCodes::Check => {
                return Ok(Request::CheckWatches(CheckWatchesRequest::deserialize(
                    bytes,
                )?));
            }
            OpCodes::GetChildren => {
                return Ok(Request::GetChildren(GetChildrenRequest::deserialize(
                    bytes,
                )?));
            }
            OpCodes::Exists => {
                return Ok(Request::Exists(ExistsRequest::deserialize(bytes)?));
            }
            OpCodes::GetData => {
                return Ok(Request::GetData(GetDataRequest::deserialize(bytes)?));
            }

            _ => {
                unreachable!("unimplemented opcode: {:?}", op_type)
            }
        }
    }
}

impl Serialize for Request {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        match self {
            Request::Create(req) => req.serialize_into(buffer),
            Request::Delete(req) => req.serialize_into(buffer),
            Request::Exists(req) => req.serialize_into(buffer),
            Request::GetData(req) => req.serialize_into(buffer),
            Request::SetData(req) => req.serialize_into(buffer),
            Request::GetAcl(req) => req.serialize_into(buffer),
            Request::SetAcl(req) => req.serialize_into(buffer),
            Request::GetChildren(req) => req.serialize_into(buffer),
            Request::GetChildren2(req) => req.serialize_into(buffer),
            Request::Ping => Ok(()),
            Request::GetChildren3(req) => req.serialize_into(buffer),
            Request::Close => Ok(()),
            Request::SetWatches(req) => req.serialize_into(buffer),
            Request::Connect(req) => req.serialize_into(buffer),
            Request::Auth(req) => req.serialize_into(buffer),
            Request::CreateTTL(req) => req.serialize_into(buffer),
            Request::CheckWatches(req) => req.serialize_into(buffer),
            Request::Whoami => Ok(()),
            Request::Multi(req) => req.serialize_into(buffer),
            Request::Sync(req) => req.serialize_into(buffer),
            Request::SetWatches2(req) => req.serialize_into(buffer),
            Request::CheckVersion(req) => req.serialize_into(buffer),
            Request::ReConfig(req) => req.serialize_into(buffer),
            Request::RemoveWatches(req) => req.serialize_into(buffer),
            Request::GetEphemerals(req) => req.serialize_into(buffer),
            Request::GetAllChildrenNumber(req) => req.serialize_into(buffer),
            Request::AddWatch(req) => req.serialize_into(buffer),
            Request::UnSupported => Ok(()),
        }
    }

    fn size(&self) -> usize {
        match self {
            Request::Create(req) => req.size(),
            Request::Delete(req) => req.size(),
            Request::Exists(req) => req.size(),
            Request::GetData(req) => req.size(),
            Request::SetData(req) => req.size(),
            Request::GetAcl(req) => req.size(),
            Request::SetAcl(req) => req.size(),
            Request::GetChildren(req) => req.size(),
            Request::GetChildren2(req) => req.size(),
            Request::Ping => 0,
            Request::GetChildren3(req) => req.size(),
            Request::Close => 0,
            Request::SetWatches(req) => req.size(),
            Request::Connect(req) => req.size(),
            Request::Auth(req) => req.size(),
            Request::CreateTTL(req) => req.size(),
            Request::CheckWatches(req) => req.size(),
            Request::Whoami => 0,
            Request::Multi(req) => req.size(),
            Request::Sync(req) => req.size(),
            Request::SetWatches2(req) => req.size(),
            Request::CheckVersion(req) => req.size(),
            Request::ReConfig(req) => req.size(),
            Request::RemoveWatches(req) => req.size(),
            Request::GetEphemerals(req) => req.size(),
            Request::GetAllChildrenNumber(req) => req.size(),
            Request::AddWatch(req) => req.size(),
            Request::UnSupported => 0,
        }
    }
}

#[derive(Debug)]
pub struct MultiResponse {
    results: Vec<Result<(ZkResponse, MultiHeader), ZooErrors>>,
    done_header: MultiHeader,
}

impl Serialize for MultiResponse {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        for result in &self.results {
            match result {
                Ok((response, header)) => {
                    header.serialize_into(buffer)?;
                    // response.serialize_into(buffer)?;
                }
                Err(err) => {
                    let header = MultiHeader {
                        r#type: -1,
                        done: 0,
                        err: err.to_i32().unwrap(),
                    };
                    header.serialize_into(buffer)?;
                }
            }
        }
        self.done_header.serialize_into(buffer)?;
        Ok(())
    }

    fn size(&self) -> usize {
        let mut size = 0;
        for result in &self.results {
            match result {
                Ok((response, header)) => {
                    size += header.size() + response.size();
                }
                Err(_) => {
                    size += 4 + 1 + 4;
                }
            }
        }
        size + self.done_header.size()
    }
}

impl Deserialize for MultiResponse {
    fn deserialize(bytes: &mut BytesMut) -> Result<Self, ZkError> {
        let mut results = vec![];
        let done_header: MultiHeader;
        loop {
            let header = MultiHeader::deserialize(bytes)?;
            if header.done == 1 {
                done_header = header;
                break;
            }
            let opcode = header.r#type;
            let err = header.err;
            if opcode == -1 {
                results.push(Ok((ZkResponse::Err(err), header)));
                continue;
            }
            let opcode_enum = OpCodes::from_i32(opcode);

            let response = ZkResponse::deserialize(bytes, opcode_enum)?;
            results.push(Ok((response, header)));
        }
        return Ok(MultiResponse {
            results,
            done_header,
        });
    }
}

#[derive(Debug)]
pub struct RequestPacket {
    pub request_header: Option<RequestHeader>,
    pub request: Request,
}

impl Serialize for RequestPacket {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        if let Some(header) = &self.request_header {
            header.serialize_into(buffer)?;
        }
        self.request.serialize_into(buffer)?;
        Ok(())
    }

    fn size(&self) -> usize {
        self.request_header.as_ref().map_or(0, |h| h.size()) + self.request.size()
    }
}

#[derive(Debug)]
pub struct ResponsePacket {
    pub response_header: Option<ReplyHeader>,
    pub response: ZkResponse,
}

impl Serialize for ResponsePacket {
    fn serialize_into(&self, buffer: &mut BytesMut) -> Result<(), ZkError> {
        if let Some(header) = &self.response_header {
            header.serialize_into(buffer)?;
        }
        self.response.serialize_into(buffer)?;
        Ok(())
    }

    fn size(&self) -> usize {
        self.response_header.as_ref().map_or(0, |h| h.size()) + self.response.size()
    }
}

#[derive(Debug)]
pub enum ZkResponse {
    Connect(ConnectResponse),
    GetData(GetDataResponse),
    SetData(SetDataResponse),
    Create(CreateResponse),
    Create2(Create2Response),
    WatchEvent(WatcherEvent),
    WhoAmI(WhoAmIResponse),
    GetChildren2 { children: Vec<String>, stat: Stat },
    GetAcl { acl: Vec<ACL>, stat: Stat },
    Ping,
    Stat(Stat),
    Empty,
    Strings(Vec<String>),
    String(String),
    Multi(MultiResponse),
    Err(i32),
}

impl ZkResponse {
    fn deserialize(src: &mut BytesMut, opcode: Option<OpCodes>) -> Result<Self, ZkError> {
        let opcode = opcode.ok_or(ZkError::InvalidOpCode(-1))?;

        match opcode {
            OpCodes::Connect => {
                unreachable!("should not be here");
            }

            OpCodes::Exists | OpCodes::SetData | OpCodes::SetAcl => {
                let stat = Stat::deserialize(src)?;

                return Ok(ZkResponse::Stat(stat));
            }
            OpCodes::GetData => {
                let data = Vec::<u8>::deserialize(src)?;
                let stat = Stat::deserialize(src)?;

                return Ok(ZkResponse::GetData(GetDataResponse { data, stat }));
            }

            OpCodes::Delete | OpCodes::Close => {
                return Ok(ZkResponse::Empty);
            }
            OpCodes::GetChildren => {
                let children = Vec::<String>::deserialize(src)?;

                return Ok(ZkResponse::Strings(children));
            }
            OpCodes::GetChildren2 => {
                let children = Vec::<String>::deserialize(src)?;
                let stat = Stat::deserialize(src)?;

                return Ok(ZkResponse::GetChildren2 { children, stat });
            }
            OpCodes::Create => {
                let resp = CreateResponse::deserialize(src)?;

                return Ok(ZkResponse::Create(resp));
            }
            OpCodes::CreateTtl => {
                let path = String::deserialize(src)?;

                return Ok(ZkResponse::String(path));
            }
            OpCodes::GetAcl => {
                let acl = Vec::<ACL>::deserialize(src)?;
                let stat = Stat::deserialize(src)?;

                return Ok(ZkResponse::GetAcl { acl, stat });
            }
            OpCodes::Check => {
                return Ok(ZkResponse::Empty);
            }
            OpCodes::CreateContainer => {
                let path = String::deserialize(src)?;

                return Ok(ZkResponse::String(path));
            }
            OpCodes::SetAuth => {
                return Ok(ZkResponse::Empty);
            }
            OpCodes::SetWatches => {
                return Ok(ZkResponse::Empty);
            }
            OpCodes::Create2 => {
                let res = Create2Response::deserialize(src)?;

                return Ok(ZkResponse::Create2(res));
            }
            OpCodes::CheckWatches => {
                return Ok(ZkResponse::Empty);
            }
            OpCodes::WhoAmI => {
                let res = WhoAmIResponse::deserialize(src)?;

                return Ok(ZkResponse::WhoAmI(res));
            }
            OpCodes::Multi => {
                let res = MultiResponse::deserialize(src)?;

                return Ok(ZkResponse::Multi(res));
            }
            OpCodes::Error => {
                return Ok(ZkResponse::Empty);
            }
            _ => {
                info!("unhandled opcode: {:?}", opcode);
                todo!("{:?}", opcode);
            }
        }
    }
}

impl Serialize for ZkResponse {
    fn serialize_into(&self, buffer: &mut bytes::BytesMut) -> Result<(), ZkError> {
        match self {
            ZkResponse::Connect(resp) => {
                resp.serialize_into(buffer)?;
            }
            ZkResponse::GetData(r) => {
                r.serialize_into(buffer)?;
            }
            ZkResponse::Ping => {}
            ZkResponse::GetAcl { acl, stat } => {
                buffer.put_i32(acl.len() as i32);
                for a in acl {
                    a.serialize_into(buffer)?;
                }
                stat.serialize_into(buffer)?;
            }
            ZkResponse::Stat(stat) => {
                stat.serialize_into(buffer)?;
            }
            ZkResponse::Empty => {}
            ZkResponse::Strings(strs) => {
                buffer.put_i32(strs.len() as i32);
                for s in strs {
                    buffer.put_i32(s.len() as i32);
                    buffer.put_slice(s.as_bytes());
                }
            }
            ZkResponse::String(str) => {
                buffer.put_i32(str.len() as i32);
                buffer.put_slice(str.as_bytes());
            }
            ZkResponse::Multi(_) => {
                todo!()
            }
            ZkResponse::GetChildren2 { children, stat } => {
                children.serialize_into(buffer)?;
                stat.serialize_into(buffer)?;
            }
            ZkResponse::WatchEvent(req) => {
                req.serialize_into(buffer)?;
            }
            ZkResponse::Create2(req) => {
                req.serialize_into(buffer)?;
            }
            ZkResponse::WhoAmI(res) => {
                res.serialize_into(buffer)?;
            }
            ZkResponse::SetData(res) => {
                res.serialize_into(buffer)?;
            }
            ZkResponse::Create(res) => {
                res.serialize_into(buffer)?;
            }
            ZkResponse::Err(err) => {
                buffer.put_i32(*err);
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        match self {
            ZkResponse::Connect(r) => 4 + 4 + 8 + 4 + r.passwd.len() + 1,
            ZkResponse::GetData(r) => 4 + r.data.len() + 6 * 8 + 4 * 5,
            ZkResponse::Ping => 0,
            ZkResponse::GetAcl { acl, stat } => 4 + acl.len() * 8 + stat.size(),
            ZkResponse::Stat(stat) => stat.size(),
            ZkResponse::Empty => 0,
            ZkResponse::Strings(strs) => 4 + strs.iter().map(|s| 4 + s.len()).sum::<usize>(),
            ZkResponse::String(str) => 4 + str.len(),
            ZkResponse::Multi(_) => {
                todo!()
            }
            ZkResponse::GetChildren2 { children, stat } => children.size() + stat.size(),
            ZkResponse::WatchEvent(req) => req.size(),
            ZkResponse::Create2(req) => req.size(),
            ZkResponse::WhoAmI(req) => req.size(),
            ZkResponse::SetData(req) => req.size(),
            ZkResponse::Create(req) => req.size(),
            ZkResponse::Err(_) => 4,
        }
    }
}

#[derive(Debug, FromPrimitive, ToPrimitive)]
pub enum XidCodes {
    ConnectXid = 0,
    WatchXid = -1,
    PingXid = -2,
    AuthXid = -4,
    SetWatchesXid = -8,
}

lazy_static! {
    static ref LENGTH_DELIMITED_CODEC: LengthDelimitedCodec = LengthDelimitedCodec::builder()
        .max_frame_length(1 * 1_024 * 1_024)
        .length_field_length(4)
        .length_field_offset(0)
        .length_adjustment(0)
        .big_endian()
        .new_codec();
}

pub struct ClientPacketCodec {
    inner: LengthDelimitedCodec,
    requests_by_xid: Arc<DashMap<i32, OpCodes>>,
}

impl ClientPacketCodec {
    pub fn new(map: Arc<DashMap<i32, OpCodes>>) -> Self {
        Self {
            inner: LENGTH_DELIMITED_CODEC.clone(),
            requests_by_xid: map,
        }
    }
}

impl Encoder<RequestPacket> for ClientPacketCodec {
    type Error = ZkError;

    fn encode(
        &mut self,
        item: RequestPacket,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        if let Some(header) = &item.request_header {
            let xid = header.xid;
            let op = header.r#type;
            if let Some(opcode) = OpCodes::from_i32(op) {
                // self.requests_by_xid.insert(xid, opcode);
            }
        }

        let n = item.size();
        dst.reserve(n + 4);
        dst.put_i32(n as i32);
        item.serialize_into(dst)?;
        Ok(())
    }
}

// b->p message decode
impl Decoder for ClientPacketCodec {
    type Item = ResponsePacket;
    type Error = ZkError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let src = self.inner.decode(src).map_err(|_| ZkError::DecodeError)?;

        return match src {
            None => {
                Ok(None)
            }
            Some((mut src, n)) => {
                ensure_min_length_bytes(&src, 4)?;
                let xid = i32::from_be_bytes(src[..4].try_into().unwrap());
                let xid_enum = XidCodes::from_i32(xid);

                if let Some(XidCodes::ConnectXid) = xid_enum {
                    let resp = ConnectResponse::deserialize(&mut src)?;
                    return Ok(Some(ResponsePacket {
                        response_header: None,
                        response: ZkResponse::Connect(resp),
                    }));
                }

                let xid = src.get_i32();
                let zxid = src.get_i64();
                let err = src.get_i32();
                let reply_header = ReplyHeader { xid, zxid, err };

                info!("xid: {}, zxid: {}, err: {}", xid, zxid, err);
                match xid_enum {
                    Some(XidCodes::WatchXid) => {
                        let resp = WatcherEvent::deserialize(&mut src)?;
                        return Ok(Some(ResponsePacket {
                            response_header: Some(reply_header),
                            response: ZkResponse::WatchEvent(resp),
                        }));
                    }
                    Some(XidCodes::PingXid) => {
                        return Ok(Some(ResponsePacket {
                            response_header: Some(reply_header),
                            response: ZkResponse::Empty,
                        }));
                    }
                    Some(XidCodes::AuthXid) => {
                        return Ok(Some(ResponsePacket {
                            response_header: Some(reply_header),
                            response: ZkResponse::Empty,
                        }));
                    }

                    Some(XidCodes::SetWatchesXid) => {
                        error!("SetWatchesXid should not happen here");
                    }
                    _ => {}
                }

                info!("reply_header: {:?}", reply_header);

                if err != 0 {
                    return Ok(Some(ResponsePacket {
                        response_header: Some(reply_header),
                        response: ZkResponse::Empty,
                    }));
                }
                if xid_enum.is_some() {
                    match xid_enum.unwrap() {
                        XidCodes::ConnectXid => {
                            unreachable!()
                        }
                        XidCodes::WatchXid => {
                            return Ok(Some(ResponsePacket {
                                response_header: Some(reply_header),
                                response: ZkResponse::Empty,
                            }));
                        }
                        XidCodes::PingXid => {
                            return Ok(Some(ResponsePacket {
                                response_header: Some(reply_header),
                                response: ZkResponse::Ping,
                            }));
                        }
                        XidCodes::AuthXid => {
                            todo!()
                        }
                        XidCodes::SetWatchesXid => {
                            todo!()
                        }
                    }
                }
                let opcode = self.requests_by_xid.get(&xid).ok_or(ZkError::DecodeError)?;
                let opcode = opcode.clone();

                debug!(
                    "ClientPacketCodec decoder, opcode: {:?}, remaining len: {}",
                    opcode,
                    src.len()
                );
                let resp = ZkResponse::deserialize(&mut src, Some(opcode))?;
                Ok(Some(ResponsePacket {
                    response_header: Some(reply_header),
                    response: resp,
                }))
            }
        };
    }
}

pub struct ServerPacketCodec {
    inner: LengthDelimitedCodec,
}

impl ServerPacketCodec {
    pub fn new() -> Self {
        Self {
            inner: LENGTH_DELIMITED_CODEC.clone(),
        }
    }

    fn decode_inner(
        &self,
        src: &mut bytes::BytesMut,
        len: usize,
    ) -> Result<Option<RequestPacket>, ZkError> {
        let len = src.len() as i32;
        debug!("zookeeper_proxy: decoding inner, len: {}", len);
        ensure_min_length_bytes(src, XID_LENGTH + OPCODE_LENGTH)?; // xid + opcode

        // Control requests, with XIDs <= 0.
        // These are meant to control the state of a session:
        // connect, keep-alive, authenticate and set initial watches.
        //
        // Note: setWatches is a command historically used to set watches
        //       right after connecting, typically used when roaming from one
        //       ZooKeeper server to the next. Thus, the special xid.
        //       However, some client implementations might expose setWatches
        //       as a regular data request, so we support that as well.

        let xid = i32::from_be_bytes(src[..4].try_into().unwrap());

        let xid_enum = XidCodes::from_i32(xid);
        debug!("xid_enum = {:?}", xid_enum);
        if let Some(xid_enum) = xid_enum {
            match xid_enum {
                XidCodes::ConnectXid => {
                    let req = ConnectRequest::deserialize(src)?;
                    return Ok(Some(RequestPacket {
                        request_header: None,
                        request: Request::Connect(req),
                    }));
                }
                XidCodes::PingXid => {
                    _ = src.get_i32(); // skip xid
                    let r#type = src.get_i32();

                    return Ok(Some(RequestPacket {
                        request_header: Some(RequestHeader { xid, r#type }),
                        request: Request::Ping,
                    }));
                }
                XidCodes::AuthXid => {
                    _ = src.get_i32(); // skip xid
                    let r#type = src.get_i32();
                    let req = AuthPacket::deserialize(src)?;
                    return Ok(Some(RequestPacket {
                        request_header: Some(RequestHeader { xid, r#type }),
                        request: Request::Auth(req),
                    }));
                }
                XidCodes::SetWatchesXid => {
                    _ = src.get_i32(); // skip xid
                    let r#type = src.get_i32();
                    let req = SetWatches::deserialize(src)?;

                    return Ok(Some(RequestPacket {
                        request_header: Some(RequestHeader { xid, r#type }),
                        request: Request::SetWatches(req),
                    }));
                }
                XidCodes::WatchXid => {
                    //  WATCH_XID is generated by the server, so that and everything else can be ignored here.
                }
            }
        }
        let xid = src.get_i32();

        // Data requests, with XIDs > 0.
        let r#type = src.get_i32(); // opcode

        let opcode_enum = OpCodes::from_i32(r#type);
        debug!("zookeeper_proxy: decoding request with opcode {:?}", opcode_enum);
        if opcode_enum.is_none() {
            error!(
                "zookeeper_proxy: decoding request error, invalid opcode: {}",
                r#type
            );
            return Err(ZkError::InvalidOpCode(r#type));
        }
        let opcode_enum = opcode_enum.unwrap();

        return match opcode_enum {
            OpCodes::GetData => {
                let req = GetDataRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::GetData(req),
                }))
            }
            OpCodes::CreateTtl => {
                let req = CreateTTLRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::CreateTTL(req),
                }))
            }
            OpCodes::Create | OpCodes::Create2 | OpCodes::CreateContainer => {
                let req = CreateRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::Create(req),
                }))
            }
            OpCodes::SetData => {
                let req = SetDataRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::SetData(req),
                }))
            }
            OpCodes::GetChildren => {
                let req = GetChildrenRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::GetChildren(req),
                }))
            }
            OpCodes::GetChildren2 => {
                let req = GetChildren2Request::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::GetChildren2(req),
                }))
            }
            OpCodes::Delete => {
                let req = DeleteRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::Delete(req),
                }))
            }
            OpCodes::Exists => {
                let req = ExistsRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::Exists(req),
                }))
            }
            OpCodes::GetAcl => {
                let req = GetACLRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::GetAcl(req),
                }))
            }
            OpCodes::SetAcl => {
                let req = SetACLRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::SetAcl(req),
                }))
            }
            OpCodes::Close => Ok(Some(RequestPacket {
                request_header: Some(RequestHeader { xid, r#type }),
                request: Request::Close,
            })),
            OpCodes::SetAuth => {
                let req = AuthPacket::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::Auth(req),
                }))
            }
            OpCodes::SetWatches => {
                let req = SetWatches::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::SetWatches(req),
                }))
            }
            OpCodes::CheckWatches => {
                let req = CheckWatchesRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::CheckWatches(req),
                }))
            }
            OpCodes::SetWatches2 => {
                let req = SetWatches2::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::SetWatches2(req),
                }))
            }
            OpCodes::WhoAmI => Ok(Some(RequestPacket {
                request_header: Some(RequestHeader { xid, r#type }),
                request: Request::Whoami,
            })),
            OpCodes::Multi => {
                let req = MultiRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::Multi(req),
                }))
            }
            OpCodes::Sync => {
                let req = SyncRequest::deserialize(src)?;

                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::Sync(req),
                }))
            }

            OpCodes::Check => {
                let req = CheckVersionRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::CheckVersion(req),
                }))
            }
            OpCodes::Reconfig => {
                let req = ReconfigRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::ReConfig(req),
                }))
            }
            OpCodes::RemoveWatches => {
                let req = RemoveWatchesRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::RemoveWatches(req),
                }))
            }
            OpCodes::GetEphemerals => {
                let req = GetEphemeralsRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::GetEphemerals(req),
                }))
            }
            OpCodes::GetAllChildrenNumber => {
                let req = GetAllChildrenNumberRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::GetAllChildrenNumber(req),
                }))
            }
            OpCodes::AddWatch => {
                let req = AddWatchRequest::deserialize(src)?;
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::AddWatch(req),
                }))
            }
            OpCodes::Connect | OpCodes::Ping | OpCodes::Error => {
                error!("should not reach here: {:?}", r#type);
                let n = src.len();
                src.advance(n);
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::UnSupported,
                }))
            }
            OpCodes::MultiRead | OpCodes::CreateSession | OpCodes::Sasl => {
                error!("unsupported opcode: {:?}", r#type);
                let n = src.len();
                src.advance(n);
                Ok(Some(RequestPacket {
                    request_header: Some(RequestHeader { xid, r#type }),
                    request: Request::UnSupported,
                }))
            }
        };
    }
}

impl Decoder for ServerPacketCodec {
    type Item = RequestPacket;
    type Error = ZkError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        return match self.inner.decode(src) {
            Err(_) => Err(ZkError::DecodeError),
            Ok(res) => match res {
                None => Ok(None),
                Some((mut src, n)) => self.decode_inner(&mut src, n),
            },
        };
    }
}

impl Encoder<ResponsePacket> for ServerPacketCodec {
    type Error = ZkError;

    fn encode(&mut self, item: ResponsePacket, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut buf = BytesMut::new();
        buf.reserve(item.size());
        item.serialize_into(&mut buf)?;
        self.inner
            .encode(buf.freeze(), dst)
            .map_err(|_| ZkError::EncodeError)
    }
}

fn ensure_min_length(len: i32, min: i32) -> Result<(), ZkError> {
    if len < min {
        // return Err(ZkError::InvalidPacketLength(len));
        panic!("ensure_min_length: len: {}, require min: {}", len, min);
    }
    Ok(())
}

fn ensure_min_length_bytes(bytes: &bytes::BytesMut, min: i32) -> Result<(), ZkError> {
    return ensure_min_length(bytes.len() as i32, min);
}

fn ensure_max_length_bytes(bytes: &bytes::BytesMut) -> Result<(), ZkError> {
    // if len > MAX_PACKET_LENGTH {
    //     return Err(ZkError::InvalidPacketLength(len));
    // }
    Ok(())
}

fn ensure_max_len(len: usize) -> Result<(), ZkError> {
    // if len > MAX_PACKET_LENGTH {
    //     return Err(ZkError::InvalidPacketLength(len));
    // }
    Ok(())
}

fn maybe_read_bool(bytes: &mut bytes::BytesMut) -> bool {
    return if bytes.remaining() >= 1 {
        bytes.get_u8() == 1
    } else {
        false
    };
}
