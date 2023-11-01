use std::{
    collections::{hash_map::Entry, HashMap},
    io,
};

use crossbeam::channel::{Receiver, Sender};
use futures::{SinkExt, StreamExt};
use mephisto_raft::Peer;
use prost::{
    bytes::Buf,
    encoding::{decode_varint, encode_varint},
    Message,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::{
    bytes::BytesMut,
    codec::{Decoder, Encoder, Framed},
};
use tracing::{error, error_span, info};

use crate::RaftMessage;

#[allow(dead_code)] // hold the fields
pub struct RaftService {
    inbound: InboundManager,
    outbound: OutboundManager,

    tx_inbound: Sender<RaftMessage>,
    rx_inbound: Receiver<RaftMessage>,
    tx_outbound: Sender<RaftMessage>,
    rx_outbound: Receiver<RaftMessage>,
}

impl RaftService {
    pub fn start(this: Peer, peers: Vec<Peer>) -> io::Result<RaftService> {
        let (tx_inbound, rx_inbound) = crossbeam::channel::unbounded();
        let (tx_outbound, rx_outbound) = crossbeam::channel::unbounded();

        let inbound = InboundManager::start(this.clone(), tx_inbound.clone())?;
        let outbound = OutboundManager::start(this, peers, rx_outbound.clone())?;

        Ok(RaftService {
            inbound,
            outbound,
            tx_inbound,
            rx_inbound,
            tx_outbound,
            rx_outbound,
        })
    }

    pub fn tx_outbound(&self) -> Sender<RaftMessage> {
        self.tx_outbound.clone()
    }

    pub fn rx_inbound(&self) -> Receiver<RaftMessage> {
        self.rx_inbound.clone()
    }
}

struct InboundManager {
    #[allow(dead_code)] // hold the field
    runtime: tokio::runtime::Runtime,
}

impl InboundManager {
    pub fn start(this: Peer, tx_inbound: Sender<RaftMessage>) -> io::Result<InboundManager> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name(format!("InboundManager-{}", this.id))
            .enable_all()
            .build()?;

        async fn accept(this: Peer, tx_inbound: Sender<RaftMessage>) -> io::Result<()> {
            let listener = TcpListener::bind(&this.address).await?;
            loop {
                let (socket, _) = listener.accept().await?;
                let mut socket = Framed::new(socket, RaftMessageServerCodec::new());
                let tx_inbound = tx_inbound.clone();
                tokio::spawn(async move {
                    while let Some(Ok(msg)) = socket.next().await {
                        tx_inbound
                            .send(msg)
                            .expect("inbound channel has been closed");
                    }
                });
            }
        }

        runtime.spawn(async move {
            let span = error_span!("InboundManager", id = this.id);
            let _guard = span.enter();
            match accept(this, tx_inbound).await {
                Ok(()) => info!("InboundManager shutdown normally"),
                Err(err) => error!(?err, "InboundManager shutdown improperly"),
            }
        });

        Ok(InboundManager { runtime })
    }
}

struct OutboundManager {
    #[allow(dead_code)] // hold the field
    runtime: tokio::runtime::Runtime,
}

impl OutboundManager {
    pub fn start(
        this: Peer,
        peers: Vec<Peer>,
        rx_outbound: Receiver<RaftMessage>,
    ) -> io::Result<OutboundManager> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name(format!("OutboundManager-{}", this.id))
            .enable_all()
            .build()?;

        async fn send(peers: Vec<Peer>, rx_outbound: Receiver<RaftMessage>) -> io::Result<()> {
            let peers = peers
                .into_iter()
                .map(|p| (p.id, p))
                .collect::<HashMap<_, _>>();

            let mut sockets = HashMap::new();

            loop {
                let msg = rx_outbound.recv().unwrap();
                let socket = match sockets.entry(msg.to) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        let peer = peers.get(&msg.to).expect("unknown peer");
                        let socket = TcpStream::connect(&peer.address).await?;
                        let socket = Framed::new(socket, RaftMessageServerCodec::new());
                        entry.insert(socket)
                    }
                };
                socket.send(msg).await?;
            }
        }

        runtime.spawn(async move {
            let span = error_span!("OutboundManager", id = this.id);
            let _guard = span.enter();
            match send(peers, rx_outbound).await {
                Ok(()) => info!("OutboundManager shutdown normally"),
                Err(err) => error!(?err, "OutboundManager shutdown improperly"),
            }
        });

        Ok(OutboundManager { runtime })
    }
}

pub struct RaftMessageServerCodec {
    peek_len: u64,
}

impl RaftMessageServerCodec {
    pub fn new() -> RaftMessageServerCodec {
        RaftMessageServerCodec { peek_len: 0 }
    }
}

impl Encoder<RaftMessage> for RaftMessageServerCodec {
    type Error = io::Error;

    fn encode(&mut self, item: RaftMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        encode_varint(item.encoded_len() as u64, dst);
        item.encode(dst).map_err(io::Error::other)
    }
}

impl Decoder for RaftMessageServerCodec {
    type Item = RaftMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.has_remaining() {
            if self.peek_len == 0 {
                self.peek_len = decode_varint(src).map_err(io::Error::other)?;
            }

            if src.remaining() >= self.peek_len as usize {
                let msg = RaftMessage::decode(src).map_err(io::Error::other)?;
                return Ok(Some(msg));
            }
        }

        return Ok(None);
    }
}