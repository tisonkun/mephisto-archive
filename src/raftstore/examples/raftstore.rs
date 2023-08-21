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

use std::time::Duration;

use crossbeam::sync::WaitGroup;
use mephisto_raft::Peer;
use mephisto_raftstore::node::RaftNode;
use tracing::{error, error_span, info, Level};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    let peers = vec![
        Peer {
            id: 1,
            address: "127.0.0.1:9746".to_string(),
        },
        Peer {
            id: 2,
            address: "127.0.0.1:9846".to_string(),
        },
        Peer {
            id: 3,
            address: "127.0.0.1:9946".to_string(),
        },
    ];

    let mut shutdown_nodes = vec![];
    let shutdown_waiters = WaitGroup::new();

    for peer in peers.iter() {
        let node = RaftNode::new(peer.clone(), peers.clone());
        shutdown_nodes.push(node.shutdown());
        let wg = shutdown_waiters.clone();
        let peer_id = peer.id;
        std::thread::spawn(move || {
            error_span!("node", id = peer_id).in_scope(|| match node.do_main() {
                Ok(()) => info!("node shutdown"),
                Err(err) => error!(?err, "node crashed"),
            });
            drop(wg);
        });
    }

    for _ in 0..10 {
        std::thread::sleep(Duration::from_secs(1));
    }

    for shutdown in shutdown_nodes {
        shutdown.shutdown();
    }
    shutdown_waiters.wait();

    Ok(())
}
