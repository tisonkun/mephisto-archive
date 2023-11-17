// Copyright 2023 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::{hash_map::Entry, HashMap},
    io,
};

use crossbeam::channel::{Receiver, Select, Sender, TryRecvError};
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
use tracing::{error, error_span, info, trace, Instrument};

use crate::raftserver::RaftMessage;

pub struct RaftService {
    inbound: InboundManager,
    outbound: OutboundManager,

    rx_inbound: Receiver<RaftMessage>,
    tx_outbound: Sender<RaftMessage>,
    tx_shutdown: Sender<()>,
}

impl RaftService {
    pub fn start(this: Peer, peers: Vec<Peer>) -> io::Result<RaftService> {
        let (tx_inbound, rx_inbound) = crossbeam::channel::unbounded();
        let (tx_outbound, rx_outbound) = crossbeam::channel::unbounded();
        let (tx_shutdown, rx_shutdown) = crossbeam::channel::bounded(1);

        let inbound = InboundManager::start(this.clone(), tx_inbound.clone())?;
        let outbound = OutboundManager::start(this, peers, rx_outbound.clone(), rx_shutdown)?;

        Ok(RaftService {
            inbound,
            outbound,
            rx_inbound,
            tx_outbound,
            tx_shutdown,
        })
    }

    pub fn tx_outbound(&self) -> Sender<RaftMessage> {
        self.tx_outbound.clone()
    }

    pub fn rx_inbound(&self) -> Receiver<RaftMessage> {
        self.rx_inbound.clone()
    }

    pub fn shutdown(self) {
        self.inbound.runtime.shutdown_background();
        self.outbound.runtime.shutdown_background();
        let _ = self.tx_shutdown.send(());
    }
}

struct InboundManager {
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
                let (socket, addr) = listener.accept().await?;
                let mut socket = Framed::new(socket, RaftMessageServerCodec::new());
                let tx_inbound = tx_inbound.clone();
                tokio::spawn(async move {
                    loop {
                        let msg = socket.next().await;
                        trace!("Received msg {msg:?}");
                        match msg {
                            None => continue,
                            Some(Ok(msg)) => {
                                tx_inbound
                                    .send(msg)
                                    .expect("inbound channel has been closed");
                            }
                            Some(Err(err)) => {
                                error!(?err, "socket on {addr} encounters error");
                                break;
                            }
                        }
                    }
                });
            }
        }

        let span = error_span!("InboundManager", id = this.id);
        runtime.spawn(
            async move {
                match accept(this, tx_inbound).await {
                    Ok(()) => info!("InboundManager shutdown normally"),
                    Err(err) => error!(?err, "InboundManager shutdown improperly"),
                }
            }
            .instrument(span),
        );

        Ok(InboundManager { runtime })
    }
}

struct OutboundManager {
    runtime: tokio::runtime::Runtime,
}

impl OutboundManager {
    pub fn start(
        this: Peer,
        peers: Vec<Peer>,
        rx_outbound: Receiver<RaftMessage>,
        rx_shutdown: Receiver<()>,
    ) -> io::Result<OutboundManager> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name(format!("OutboundManager-{}", this.id))
            .enable_all()
            .build()?;

        async fn send(
            peers: Vec<Peer>,
            rx_outbound: Receiver<RaftMessage>,
            rx_shutdown: Receiver<()>,
        ) -> io::Result<()> {
            let peers = peers
                .into_iter()
                .map(|p| (p.id, p))
                .collect::<HashMap<_, _>>();

            let mut sockets = HashMap::new();

            loop {
                let mut select = Select::new();
                select.recv(&rx_shutdown);
                select.recv(&rx_outbound);
                select.ready();

                if !matches!(rx_shutdown.try_recv(), Err(TryRecvError::Empty)) {
                    return Ok(());
                }

                if let Ok(msg) = rx_outbound.try_recv() {
                    let mut socket = match sockets.entry(msg.to) {
                        Entry::Occupied(entry) => entry,
                        Entry::Vacant(entry) => {
                            let peer = peers.get(&msg.to).expect("unknown peer");
                            let socket = TcpStream::connect(&peer.address).await?;
                            let socket = Framed::new(socket, RaftMessageServerCodec::new());
                            entry.insert_entry(socket)
                        }
                    };
                    trace!("Sending msg {msg:?}");
                    if let Err(err) = socket.get_mut().send(msg).await {
                        let to = socket.key();
                        error!(?err, "socket to {to} encounters error");
                        socket.remove();
                    }
                    // socket.mut.send(msg).await?;
                }
            }
        }

        let span = error_span!("OutboundManager", id = this.id);
        runtime.spawn(
            async move {
                match send(peers, rx_outbound, rx_shutdown).await {
                    Ok(()) => info!("OutboundManager shutdown normally"),
                    Err(err) => error!(?err, "OutboundManager shutdown improperly"),
                }
            }
            .instrument(span),
        );

        Ok(OutboundManager { runtime })
    }
}

pub struct RaftMessageServerCodec {
    peek_len: u64,
}

#[allow(clippy::new_without_default)]
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
                self.peek_len = 0;
                return Ok(Some(msg));
            }
        }

        Ok(None)
    }
}
