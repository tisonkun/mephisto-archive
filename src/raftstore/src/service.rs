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
    io,
    mem::size_of,
    net::{TcpListener, TcpStream},
    ops::DerefMut,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bytes::Buf;
use crossbeam::{channel::Sender, sync::WaitGroup};
use mephisto_raft::Peer;
use polling::{Event, Poller};
use tracing::{debug, error, error_span, info, trace};

use crate::{
    shutdown::ShutdownIO,
    transport::{ReadContext, Transport},
    RaftMessage,
};

fn interrupted(err: &io::Error) -> bool {
    matches!(err.kind(), io::ErrorKind::Interrupted)
}

fn would_block(err: &io::Error) -> bool {
    matches!(err.kind(), io::ErrorKind::WouldBlock)
}

pub struct RaftService {
    this: Peer,
    tx_message: Sender<RaftMessage>,
    poller: Arc<Poller>,
    is_shutdown: Arc<AtomicBool>,
}

impl RaftService {
    pub fn shutdown(&self) -> ShutdownIO {
        ShutdownIO::new(self.poller.clone(), self.is_shutdown.clone())
    }

    pub fn new(this: Peer, tx_message: Sender<RaftMessage>) -> io::Result<RaftService> {
        Ok(RaftService {
            this,
            tx_message,
            poller: Arc::new(Poller::new()?),
            is_shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn do_main(self, id: u64, wg: WaitGroup) {
        std::thread::spawn(move || {
            error_span!("service", id).in_scope(|| match self.internal_do_main() {
                Ok(()) => info!("raft service closed"),
                Err(err) => error!(?err, "raft service failed"),
            });
            drop(wg);
        });
    }

    fn internal_do_main(self) -> io::Result<()> {
        info!(
            address = self.this.address,
            "raft service is serving requests"
        );

        let listener = TcpListener::bind(self.this.address)?;
        listener.set_nonblocking(true)?;
        self.poller.add(&listener, Event::readable(1))?;

        let mut guard = scopeguard::guard(
            (vec![], WaitGroup::new()),
            |(signals, waiters): (Vec<ShutdownIO>, WaitGroup)| {
                for shutdown in signals {
                    shutdown.shutdown();
                }
                waiters.wait();
            },
        );

        let (shutdown_signals, shutdown_waiters) = guard.deref_mut();

        loop {
            let mut events = vec![];
            self.poller.wait(&mut events, None)?;

            if self.is_shutdown.load(Ordering::SeqCst) {
                return Ok(());
            }

            while let Some(socket) = match listener.incoming().next() {
                None => None,
                Some(Err(ref err)) if would_block(err) => None,
                Some(Err(ref err)) if interrupted(err) => None,
                Some(Err(err)) => return Err(err),
                Some(Ok(socket)) => Some(socket),
            } {
                let address = socket.peer_addr()?;
                trace!("accepted connection to {}", address);
                let c = Connection::new(socket, self.tx_message.clone())?;
                shutdown_signals.push(c.shutdown());
                c.do_main(self.this.id, format!("{address}"), shutdown_waiters.clone());
            }

            self.poller.modify(&listener, Event::readable(1))?;
        }
    }
}

struct Connection {
    socket: TcpStream,
    transport: Transport,
    tx_message: Sender<RaftMessage>,
    poller: Arc<Poller>,
    is_shutdown: Arc<AtomicBool>,
}

impl Connection {
    fn new(socket: TcpStream, tx_message: Sender<RaftMessage>) -> io::Result<Connection> {
        Ok(Connection {
            socket,
            tx_message,
            transport: Transport::default(),
            poller: Arc::new(Poller::new()?),
            is_shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    fn shutdown(&self) -> ShutdownIO {
        ShutdownIO::new(self.poller.clone(), self.is_shutdown.clone())
    }

    fn do_main(self, id: u64, address: String, wg: WaitGroup) {
        std::thread::spawn(move || {
            error_span!("srvconn", id, address).in_scope(|| match self.internal_do_main() {
                Ok(()) => info!("srvconn closed"),
                Err(err) => error!(?err, "srvconn failed"),
            });
            drop(wg);
        });
    }

    fn internal_do_main(mut self) -> io::Result<()> {
        self.socket.set_nodelay(true)?;
        self.socket.set_nonblocking(true)?;
        self.poller.add(&self.socket, Event::readable(1))?;

        loop {
            let mut events = vec![];
            self.poller.wait(&mut events, None)?;
            if self.is_shutdown.load(Ordering::SeqCst) {
                return Ok(());
            }

            let mut read_ctx = self.transport.read_bytes(&mut self.socket)?;

            while let Some(req) = try_read_one_message(&mut read_ctx)? {
                debug!(?req, "receive service message");
                self.tx_message.send(req).unwrap();
            }

            if read_ctx.is_closed() {
                return Ok(());
            }

            self.poller.modify(&self.socket, Event::readable(1))?;
        }
    }
}

fn try_read_one_message(read_ctx: &mut ReadContext) -> io::Result<Option<RaftMessage>> {
    use prost::Message;

    let read_buf = read_ctx.buffer();

    let mut peek_buf = read_buf.chunk();
    if peek_buf.remaining() < size_of::<u32>() {
        return Ok(None);
    }
    let n = peek_buf.get_u32() as usize;
    if read_buf.remaining() < size_of::<u32>() + n {
        return Ok(None);
    }

    read_buf.advance(size_of::<u32>());
    match RaftMessage::decode(&read_buf.chunk()[..n]) {
        Ok(msg) => {
            read_buf.advance(n);
            Ok(Some(msg))
        }
        Err(err) => Err(io::Error::other(err)),
    }
}
