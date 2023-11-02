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
