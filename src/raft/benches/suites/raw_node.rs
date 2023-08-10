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

use criterion::{BatchSize, Bencher, BenchmarkId, Criterion, Throughput};
use mephisto_raft::{
    eraftpb::{Entry, Message, Snapshot, SnapshotMetadata},
    storage::MemStorage,
    Config, RawNode,
};

pub fn bench_raw_node(c: &mut Criterion) {
    bench_raw_node_new(c);
    bench_raw_node_leader_propose(c);
    bench_raw_node_new_ready(c);
}

fn quick_raw_node() -> RawNode<MemStorage> {
    let id = 1;
    let storage = MemStorage::new_with_conf_state((vec![1], vec![]));
    let config = Config::new(id);
    RawNode::new(&config, storage).unwrap()
}

pub fn bench_raw_node_new(c: &mut Criterion) {
    let bench = |b: &mut Bencher| {
        b.iter(quick_raw_node);
    };

    c.bench_function("RawNode::new", bench);
}

pub fn bench_raw_node_leader_propose(c: &mut Criterion) {
    static KB: usize = 1024;
    let mut test_sets = vec![
        0,
        32,
        128,
        512,
        KB,
        4 * KB,
        16 * KB,
        128 * KB,
        512 * KB,
        KB * KB,
    ];
    let mut group = c.benchmark_group("RawNode::leader_propose");
    for size in test_sets.drain(..) {
        // Calculate measurement time in seconds according to the input size.
        // The approximate time might not be the best but should work fine.
        let mtime = if size < KB {
            1
        } else if size < 128 * KB {
            3
        } else {
            7
        };
        group
            .measurement_time(Duration::from_secs(mtime))
            .throughput(Throughput::Bytes(size as u64))
            .bench_with_input(
                BenchmarkId::from_parameter(size),
                &size,
                |b: &mut Bencher, size| {
                    let mut node = quick_raw_node();
                    node.raft.become_candidate();
                    node.raft.become_leader();
                    b.iter_batched(
                        || (vec![0; 8], vec![0; *size]),
                        |(context, value)| node.propose(context, value).expect(""),
                        BatchSize::SmallInput,
                    );
                },
            );
    }
}

pub fn bench_raw_node_new_ready(c: &mut Criterion) {
    let mut group = c.benchmark_group("RawNode::ready");
    group
        // TODO: The proper measurement time could be affected by the system and machine.
        .measurement_time(Duration::from_secs(20))
        .bench_function("Default", |b: &mut Bencher| {
            b.iter_batched(
                test_ready_raft_node,
                |mut node| {
                    let _ = node.ready();
                },
                // NOTICE: SmallInput accumulates (iters + 10 - 1) / 10 samples per batch
                BatchSize::SmallInput,
            );
        });
}

// Create a raft node calling `ready()` with things below:
//  - 100 new entries with 32KB data each
//  - 100 committed entries with 32KB data each
//  - 100 raft messages
//  - A snapshot with 8MB data
// TODO: Maybe gathering all the things we need into a struct(e.g. something like
//  `ReadyBenchOption`) and use it to customize the output.
fn test_ready_raft_node() -> RawNode<MemStorage> {
    let mut node = quick_raw_node();
    node.raft.become_candidate();
    node.raft.become_leader();
    let unstable = node.raft.raft_log.unstable_entries().to_vec();
    node.raft.raft_log.stable_entries(1, 1);
    node.raft.raft_log.store.wl().append(&unstable).expect("");
    node.raft.on_persist_entries(1, 1);
    node.raft.commit_apply(1);
    let mut entries = vec![];
    for i in 1..101 {
        entries.push(Entry {
            term: 1,
            index: i,
            data: vec![0; 32 * 1024],
            ..Default::default()
        });
    }
    let _ = node.raft.append_entry(&mut entries);
    let unstable = node.raft.raft_log.unstable_entries().to_vec();
    node.raft.raft_log.stable_entries(101, 1);
    node.raft.raft_log.store.wl().append(&unstable).expect("");
    // This increases 'committed_index' to `last_index` because there is only one node in quorum.
    node.raft.on_persist_entries(101, 1);

    let _ = Snapshot {
        data: vec![0; 8 * 1024 * 1024],
        // We don't care about the contents in snapshot here since it won't be applied.
        metadata: Some(SnapshotMetadata::default()),
    };
    for _ in 0..100 {
        node.raft.msgs.push(Message::default());
    }
    // Force reverting committed index to provide us some entries to be stored from next `Ready`
    node.raft.raft_log.committed = 1;
    node
}
