use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use futures::SinkExt;
use log::{debug, error, info};
use num_traits::ToPrimitive;
use regex::Regex;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

use crate::codec::{ClientPacketCodec, Request, ResponsePacket, ServerPacketCodec, ZkResponse};
use crate::config::Config;
use crate::constants::OpCodes;
use crate::errors::ZkError;
use crate::proto::ReplyHeader;
use crate::record::Serialize;
use crate::zk_errcode::ZooErrors;

// c->p->b
pub struct UpStreamConnection {
    c2p_read_half: OwnedReadHalf,
    p2b_write_half: OwnedWriteHalf,
    tx: UnboundedSender<Bytes>,
    config: Config,
}

impl UpStreamConnection {
    fn new(
        config: Config,
        c2p_read_half: OwnedReadHalf,
        p2b_write_half: OwnedWriteHalf,
        tx: UnboundedSender<Bytes>,
    ) -> Self {
        return Self {
            config,
            c2p_read_half,
            p2b_write_half,
            tx,
        };
    }


    async fn pipe(&mut self, map: Arc<DashMap<i32, OpCodes>>) -> std::io::Result<()> {
        let mut c2p_framed = FramedRead::new(&mut self.c2p_read_half, ServerPacketCodec::new());
        let mut p2b_framed = FramedWrite::new(
            &mut self.p2b_write_half,
            ClientPacketCodec::new(map.clone()),
        );

        while let Some(Ok(r)) = c2p_framed.next().await {
            if r.request_header.is_none() {
                let _ = p2b_framed.send(r).await;
                continue;
            }


            let xid = r
                .request_header
                .as_ref()
                .and_then(|it| Some(it.xid))
                .clone();

            let opcode = r.request_header.as_ref().unwrap().r#type;

            let mut has_match = false;
            let mut bytes = BytesMut::new();
            for rule in &self.config.circuit_break {
                if let Ok(regex) = Regex::new(&rule.regex) {
                    if rule.opcode.contains(&opcode) && r.request.path().is_some() && regex.is_match(&r.request.path().clone().unwrap()) {
                        has_match = true;
                        debug!("rule matched");
                        let resp = ResponsePacket {
                            response_header: Some(ReplyHeader {
                                xid: xid.unwrap(),
                                zxid: 0,
                                err: ZooErrors::ZNOAUTH.to_i32().unwrap(),
                            }),
                            response: ZkResponse::Empty,
                        };
                        bytes = BytesMut::with_capacity(resp.size() + 4);
                        bytes.put_i32(resp.size() as i32);
                        resp.serialize_into(&mut bytes).unwrap();
                        break;
                    }
                }
            }
            if has_match {
                let _ = self.tx.send(bytes.freeze());
                continue;
            }

            let _ = p2b_framed.send(r).await;
        }
        Ok(())
    }
}

// c<-p
struct P2CDownStreamConnection {
    p2c_write_half: OwnedWriteHalf,
    rx: UnboundedReceiver<Bytes>,
}

impl P2CDownStreamConnection {
    fn new(p2c_write_half: OwnedWriteHalf, rx: UnboundedReceiver<Bytes>) -> Self {
        return Self { p2c_write_half, rx };
    }
    async fn pipe(&mut self, map: Arc<DashMap<i32, OpCodes>>) -> std::io::Result<()> {
        let mut writer = FramedWrite::new(&mut self.p2c_write_half, BytesCodec::new());

        while let Some(res) = self.rx.recv().await {
            let _ = writer.send(res).await;
            // let xid = res.response_header.as_ref().and_then(|it| Some(it.xid)).clone();
            // let _ = writer.send(res).await;
            // if let Some(xid) = xid {
            //     map.remove(&xid);
            // }
        }
        Ok(())
    }
}

pub struct B2PDownStreamConnection {
    b2p_read_half: OwnedReadHalf,
    tx: UnboundedSender<Bytes>,
}

impl B2PDownStreamConnection {
    fn new(b2p_read_half: OwnedReadHalf, tx: UnboundedSender<Bytes>) -> Self {
        return Self { b2p_read_half, tx };
    }
    async fn pipe(&mut self, map: Arc<DashMap<i32, OpCodes>>) -> std::io::Result<()> {
        let mut framed_reader = FramedRead::new(&mut self.b2p_read_half, BytesCodec::new());

        while let Some(Ok(res)) = framed_reader.next().await {
            let _ = self.tx.send(res.freeze());
        }
        Ok(())
    }
}

pub struct ZkServer {
    config: Config,
}

impl ZkServer {
    pub fn new(config: Config) -> ZkServer {
        ZkServer { config }
    }
    pub async fn handle_conn(upstream_addr: String, config: Config, c2p_stream: TcpStream) -> Result<(), ZkError> {
        let p2b_stream = TcpStream::connect(upstream_addr).await.unwrap();

        let (c2p_read_half, p2c_write_half) = c2p_stream.into_split();
        let (b2p_read_half, p2b_write_half) = p2b_stream.into_split();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut upstream_conn = UpStreamConnection::new(config.clone(), c2p_read_half, p2b_write_half, tx.clone());
        let mut p2c_downstream_conn = P2CDownStreamConnection::new(p2c_write_half, rx);
        let mut b2p_downstream_conn = B2PDownStreamConnection::new(b2p_read_half, tx.clone());

        let map = Arc::new(DashMap::new());
        tokio::select! {
            _ = upstream_conn.pipe(map.clone()) => {
            }
            _ = p2c_downstream_conn.pipe(map.clone()) => {
            }
            _ = b2p_downstream_conn.pipe(map.clone()) => {
            }
        }
        Ok(())
    }

    pub async fn start(&self) -> Result<(), ZkError> {
        let listen_addr = format!("{}:{}", self.config.server.address, self.config.server.port);
        let upstream_addr = format!(
            "{}:{}",
            self.config.upstream.address, self.config.upstream.port
        );

        let listener = TcpListener::bind(listen_addr).await.unwrap();
        loop {
            let (c2p_socket, peer_addr) = listener.accept().await.unwrap();
            info!("peer_addr: {:?}", peer_addr);
            let upstream_addr = upstream_addr.clone();
            let config = self.config.clone();
            tokio::spawn(async move {
                if let Err(err) = Self::handle_conn(upstream_addr, config, c2p_socket).await {
                    error!("handle_conn error: {:?}", err);
                }
            });
        }
    }
}
