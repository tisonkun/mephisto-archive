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
use mephisto_raftsrv::server::RaftServer;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use tracing::Level;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

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

    let mut servers = vec![];

    for peer in peers.iter() {
        let server = RaftServer::start(peer.clone(), peers.clone())?;
        servers.push(server);
    }

    for _ in 0..10 {
        std::thread::sleep(Duration::from_secs(1));
    }

    let wg = WaitGroup::new();
    let wgs = (0..servers.len()).map(|_| wg.clone()).collect::<Vec<_>>();
    servers.into_par_iter().zip(wgs).for_each(|(srv, wg)| {
        drop(srv);
        drop(wg);
    });
    wg.wait();

    Ok(())
}
