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

use crossbeam::channel::Sender;
use mephisto_raft::Peer;

use crate::raftserver::{node::RaftNode, service::RaftService};

pub mod node;
pub mod service;

pub type RaftMessage = mephisto_raft::proto::eraftpb::Message;

pub struct RaftServer {
    service: Option<RaftService>,
    shutdown_node: Sender<()>,
}

impl RaftServer {
    pub fn start(this: Peer, peers: Vec<Peer>) -> anyhow::Result<RaftServer> {
        let service = RaftService::start(this.clone(), peers.clone())?;

        let node = RaftNode::new(this, peers, service.rx_inbound(), service.tx_outbound())?;
        let shutdown_node = node.tx_shutdown();
        node.run();

        Ok(RaftServer {
            service: Some(service),
            shutdown_node,
        })
    }
}

impl Drop for RaftServer {
    fn drop(&mut self) {
        let _ = self.shutdown_node.send(());
        if let Some(service) = self.service.take() {
            service.shutdown();
        }
    }
}
