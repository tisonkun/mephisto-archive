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

use std::time::{Duration, Instant};

use crossbeam::{
    channel::{Receiver, Select, Sender, TryRecvError},
    sync::WaitGroup,
};
use mephisto_raft::{eraftpb, storage::MemStorage, Config, Peer, RawNode, SoftState};
use tracing::{error, error_span, info, trace};

use crate::{router::RaftRouter, service::RaftService, RaftMessage};

#[derive(Default)]
struct RaftState {
    state: SoftState,
    // applied_index: u64,
}

pub struct ShutdownNode {
    tx_shutdown: Sender<()>,
}

impl ShutdownNode {
    pub fn shutdown(self) {
        if let Err(err) = self.tx_shutdown.send(()) {
            // receiver closed - already shutdown
            trace!(?err, "shutdown node cannot send signal");
        }
    }
}

pub struct RaftNode {
    this: Peer,
    peers: Vec<Peer>,
    node: RawNode<MemStorage>,
    state: RaftState,

    tx_shutdown: Sender<()>,
    rx_shutdown: Receiver<()>,
    tx_message: Sender<RaftMessage>,
    rx_message: Receiver<RaftMessage>,
    tick: Receiver<Instant>,
}

impl RaftNode {
    pub fn new(this: Peer, peers: Vec<Peer>) -> RaftNode {
        let id = this.id;

        let config = {
            let mut config = Config::new(id);
            config.pre_vote = true;
            config.priority = (1 << id) as i64;
            config.election_tick = 10;
            config.heartbeat_tick = 1;
            config.max_size_per_msg = 1024 * 1024 * 1024;
            config.validate().unwrap();
            config
        };

        let voters = peers.iter().map(|p| p.id).collect::<Vec<_>>();
        let storage = MemStorage::new_with_conf_state((voters, vec![]));
        storage.wl().mut_hard_state().term = 1;
        let node = RawNode::new(&config, storage).unwrap();

        let (tx_message, rx_message) = crossbeam::channel::unbounded();
        let (tx_shutdown, rx_shutdown) = crossbeam::channel::bounded(1);
        let tick = crossbeam::channel::tick(Duration::from_millis(100));

        RaftNode {
            this,
            peers,
            node,
            state: RaftState::default(),
            tx_shutdown,
            rx_shutdown,
            tx_message,
            rx_message,
            tick,
        }
    }

    pub fn shutdown(&self) -> ShutdownNode {
        ShutdownNode {
            tx_shutdown: self.tx_shutdown.clone(),
        }
    }

    pub fn do_main(mut self) -> anyhow::Result<()> {
        let service = RaftService::new(self.this.clone(), self.tx_message.clone())?;
        let shutdown_service = service.shutdown();
        let shutdown_waiters = WaitGroup::new();
        let wg = shutdown_waiters.clone();
        std::thread::spawn(move || {
            error_span!("service", id = self.this.id).in_scope(|| match service.do_main() {
                Ok(()) => info!("raft service closed"),
                Err(err) => error!(?err, "raft service failed"),
            });
            drop(wg);
        });

        let router = RaftRouter::new(self.this.clone(), self.peers.clone());

        loop {
            if self.process_inbound()? {
                break;
            }
            self.process_ready(&router)?;
        }

        router.shutdown();
        shutdown_service.shutdown()?;
        shutdown_waiters.wait();

        Ok(())
    }

    // return true if shutdown
    fn process_inbound(&mut self) -> anyhow::Result<bool> {
        let mut select = Select::new();
        select.recv(&self.rx_shutdown);
        select.recv(&self.rx_message);
        select.recv(&self.tick);
        select.ready();

        if !matches!(self.rx_shutdown.try_recv(), Err(TryRecvError::Empty)) {
            return Ok(true);
        }

        for _ in self.tick.try_iter() {
            self.node.tick();
        }

        for msg in self.rx_message.try_iter() {
            self.node.step(msg)?;
        }

        Ok(false)
    }

    fn process_ready(&mut self, router: &RaftRouter) -> anyhow::Result<()> {
        if !self.node.has_ready() {
            return Ok(());
        }

        let mut ready = self.node.ready();

        if let Some(ss) = ready.ss() {
            if ss.raft_state != self.state.state.raft_state {
                info!(
                    "changing raft node role from {:?} to {:?}",
                    self.state.state.raft_state, ss.raft_state
                );
            }
            self.state.state = *ss;
        }

        for msg in ready.take_messages() {
            self.process_message(msg, router);
        }

        if let Some(hs) = ready.hs() {
            self.node.store().wl().set_hard_state(hs.clone());
        }

        for msg in ready.take_persisted_messages() {
            self.process_message(msg, router);
        }

        self.node.advance(ready);
        Ok(())
    }

    fn process_message(&self, msg: RaftMessage, router: &RaftRouter) {
        match msg.msg_type() {
            eraftpb::MessageType::MsgAppend
            | eraftpb::MessageType::MsgRequestPreVote
            | eraftpb::MessageType::MsgAppendResponse
            | eraftpb::MessageType::MsgRequestPreVoteResponse
            | eraftpb::MessageType::MsgRequestVote
            | eraftpb::MessageType::MsgRequestVoteResponse
            | eraftpb::MessageType::MsgHeartbeat
            | eraftpb::MessageType::MsgHeartbeatResponse
            | eraftpb::MessageType::MsgPropose
            | eraftpb::MessageType::MsgReadIndex
            | eraftpb::MessageType::MsgReadIndexResp => {
                if msg.to != self.this.id {
                    router.send(msg.to, msg);
                } else {
                    unreachable!("outbound message send to self {msg:?}")
                }
            }
            _ => unimplemented!("unimplemented {msg:?}"),
        }
    }
}
