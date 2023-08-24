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

use std::{collections::HashMap, io, io::Write, net::TcpStream};

use bytes::BufMut;
use crossbeam::{
    channel::{Receiver, Select, Sender, TryRecvError},
    sync::WaitGroup,
};
use mephisto_raft::Peer;
use prost::Message;
use tracing::{error, error_span, info, trace};

use crate::{transport::Transport, RaftMessage};

pub struct ShutdownRouter {
    shutdown_signals: Vec<Sender<()>>,
    shutdown_waiters: WaitGroup,
}

impl ShutdownRouter {
    pub fn shutdown(self) {
        for shutdown in self.shutdown_signals {
            if let Err(err) = shutdown.send(()) {
                // receiver closed - already shutdown
                trace!(?err, "shutdown router connection cannot send signal");
            }
        }
        self.shutdown_waiters.wait();
    }
}

pub struct RaftRouter {
    this: Peer,
    peers: HashMap<u64, Sender<RaftMessage>>,
}

impl RaftRouter {
    pub fn new(this: Peer, peer_list: Vec<Peer>) -> (RaftRouter, ShutdownRouter) {
        let mut shutdown_signals = vec![];
        let shutdown_waiters = WaitGroup::new();

        let mut peers = HashMap::new();
        for peer in peer_list {
            let peer_id = peer.id;
            if this.id == peer_id {
                continue;
            }

            let (tx_message, rx_message) = crossbeam::channel::unbounded();
            let (tx_shutdown, rx_shutdown) = crossbeam::channel::bounded(1);
            peers.insert(peer.id, tx_message);
            shutdown_signals.push(tx_shutdown);

            let c = Connection {
                peer,
                rx_message,
                rx_shutdown,
            };
            c.do_main(this.id, peer_id, shutdown_waiters.clone());
        }

        let router = RaftRouter { this, peers };
        let shutdown = ShutdownRouter {
            shutdown_signals,
            shutdown_waiters,
        };
        (router, shutdown)
    }

    pub fn send(&self, to: u64, msg: RaftMessage) {
        debug_assert_ne!(to, self.this.id);
        let peer = self.peers.get(&to).unwrap();
        peer.send(msg).unwrap();
    }
}

struct Connection {
    peer: Peer,
    rx_message: Receiver<RaftMessage>,
    rx_shutdown: Receiver<()>,
}

// tolerate maybe server not yet started
#[derive(Debug, Default)]
struct RetryRefused {
    times: i32,
}

impl RetryRefused {
    fn retry(&mut self, err: &io::Error) -> bool {
        self.times += 1;
        matches!(err.kind(), io::ErrorKind::ConnectionRefused) && self.times <= 10
    }

    fn reset(&mut self) {
        self.times = 0;
    }
}

impl Connection {
    fn do_main(self, this: u64, peer: u64, wg: WaitGroup) {
        std::thread::spawn(move || {
            error_span!("router", this, peer).in_scope(|| match self.internal_do_main() {
                Ok(()) => info!("router closed"),
                Err(err) => error!(?err, "router failed"),
            });
            drop(wg);
        });
    }

    fn internal_do_main(self) -> io::Result<()> {
        let mut retry_refused = RetryRefused::default();
        let mut transport = Transport::default();
        let mut socket = None;

        loop {
            if transport.write_context().buffer().is_empty() {
                let mut select = Select::new();
                select.recv(&self.rx_shutdown);
                select.recv(&self.rx_message);
                select.ready();
            }

            if !matches!(self.rx_shutdown.try_recv(), Err(TryRecvError::Empty)) {
                return Ok(());
            }

            for msg in self.rx_message.try_iter() {
                let buf = msg.encode_to_vec();
                {
                    let n = buf.len();
                    let buf = u32::to_be_bytes(n as u32);
                    transport.write_context().buffer().put_slice(buf.as_slice());
                }
                transport.write_context().buffer().put_slice(buf.as_slice());
            }

            let stream = match socket {
                None => match TcpStream::connect(self.peer.address.clone()) {
                    Ok(stream) => {
                        retry_refused.reset();
                        stream.set_nonblocking(true)?;
                        stream.set_nodelay(true)?;
                        socket = Some(stream);
                        socket.as_mut().unwrap()
                    }
                    Err(ref err) if retry_refused.retry(err) => continue,
                    Err(err) => return Err(err),
                },
                Some(ref mut stream) => stream,
            };

            transport.write_bytes(stream)?;
            stream.flush()?;
        }
    }
}
