use crossbeam::channel::Sender;
use mephisto_raft::Peer;

use crate::{node::RaftNode, service::RaftService};

#[allow(dead_code)] // hold the fields
pub struct RaftServer {
    service: RaftService,
    shutdown_service: Sender<()>,
    shutdown_node: Sender<()>,
}

impl RaftServer {
    pub fn start(this: Peer, peers: Vec<Peer>) -> anyhow::Result<RaftServer> {
        let (tx_shutdown_service, rx_shutdown_service) = crossbeam::channel::bounded(0);
        let service = RaftService::start(this.clone(), peers.clone(), rx_shutdown_service)?;

        let node = RaftNode::new(this, peers, service.rx_inbound(), service.tx_outbound())?;
        let shutdown_node = node.tx_shutdown();
        node.run();

        Ok(RaftServer {
            service,
            shutdown_service: tx_shutdown_service,
            shutdown_node,
        })
    }
}
