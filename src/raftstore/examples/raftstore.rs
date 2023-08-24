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

use crossbeam::{channel::Receiver, sync::WaitGroup};
use mephisto_raft::Peer;
use mephisto_raftstore::{
    node::RaftNode,
    proto::{
        datatree::{CreateRequest, DataTreeRequest, GetDataRequest, RequestHeader, RequestType},
        FatReply, FatRequest, ReqId,
    },
};
use tracing::{info, Level};
use tracing_subscriber::{filter::FilterFn, layer::SubscriberExt, util::SubscriberInitExt, Layer};

fn make_create_req(req_id: ReqId, path: String, data: String) -> (FatRequest, Receiver<FatReply>) {
    let (tx, rx) = crossbeam::channel::bounded(1);
    let req = FatRequest {
        req_id,
        header: RequestHeader {
            req_id: req_id.req_id,
            req_type: RequestType::ReqCreate as i32,
        },
        request: DataTreeRequest::Create(CreateRequest {
            path,
            data: data.into(),
        }),
        reply: tx,
    };
    (req, rx)
}

fn make_get_data_req(req_id: ReqId, path: String) -> (FatRequest, Receiver<FatReply>) {
    let (tx, rx) = crossbeam::channel::bounded(1);
    let req = FatRequest {
        req_id,
        header: RequestHeader {
            req_id: req_id.req_id,
            req_type: RequestType::ReqGetData as i32,
        },
        request: DataTreeRequest::GetData(GetDataRequest { path }),
        reply: tx,
    };
    (req, rx)
}

fn main() -> anyhow::Result<()> {
    let filter = FilterFn::new(|meta| {
        // filter out logs of the "polling" crate
        !meta.target().starts_with("polling")
    })
    .with_max_level_hint(Level::INFO);
    let layer = tracing_subscriber::fmt::layer();

    tracing_subscriber::registry()
        .with(layer.with_filter(filter))
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

    let mut requesters = vec![];

    for peer in peers.iter() {
        let node = RaftNode::new(peer.clone(), peers.clone());
        requesters.push(node.tx_request());
        shutdown_nodes.push(node.shutdown());
        let wg = shutdown_waiters.clone();
        let peer_id = peer.id;
        node.do_main(peer_id, wg);
    }

    let client_id = uuid::Uuid::new_v4();
    let mut results = vec![];
    for i in 0..10 {
        let (req, rx) = make_create_req(
            ReqId {
                client_id,
                req_id: 2 * i,
            },
            format!("mephisto-{}", i / 2),
            format!("value-{}", i),
        );
        requesters[0].send(req).unwrap();
        results.push(rx);

        let (req, rx) = make_get_data_req(
            ReqId {
                client_id,
                req_id: 2 * i + 1,
            },
            format!("mephisto-{}", i / 2),
        );
        requesters[0].send(req).unwrap();
        results.push(rx);
    }

    for (i, result) in results.iter().enumerate() {
        info!("rx.recv()[{i}]={:?}", result.recv());
    }

    for shutdown in shutdown_nodes {
        shutdown.shutdown();
    }
    shutdown_waiters.wait();

    Ok(())
}
