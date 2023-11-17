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
