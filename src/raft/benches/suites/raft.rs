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

use criterion::Criterion;
use mephisto_raft::{eraftpb::ConfState, storage::MemStorage, Config, Raft};

use crate::DEFAULT_RAFT_SETS;

pub fn bench_raft(c: &mut Criterion) {
    bench_raft_new(c);
    bench_raft_campaign(c);
}

fn new_storage(voters: usize, learners: usize) -> MemStorage {
    let mut cc = ConfState::default();
    for i in 1..=voters {
        cc.voters.push(i as u64);
    }
    for i in 1..=learners {
        cc.learners.push(voters as u64 + i as u64);
    }
    MemStorage::new_with_conf_state(cc)
}

fn quick_raft(storage: MemStorage) -> Raft<MemStorage> {
    let id = 1;
    let config = Config::new(id);
    Raft::new(&config, storage).unwrap()
}

pub fn bench_raft_new(c: &mut Criterion) {
    DEFAULT_RAFT_SETS.iter().for_each(|(voters, learners)| {
        c.bench_function(&format!("Raft::new ({}, {})", voters, learners), move |b| {
            let storage = new_storage(*voters, *learners);
            b.iter(|| quick_raft(storage.clone()))
        });
    });
}

pub fn bench_raft_campaign(c: &mut Criterion) {
    DEFAULT_RAFT_SETS
        .iter()
        .skip(1)
        .for_each(|(voters, learners)| {
            // We don't want to make `raft::raft` public at this point.
            let msgs = &[
                "CampaignPreElection",
                "CampaignElection",
                "CampaignTransfer",
            ];
            // Skip the first since it's 0,0
            for msg in msgs {
                c.bench_function(
                    &format!("Raft::campaign ({}, {}, {})", voters, learners, msg),
                    move |b| {
                        let storage = new_storage(*voters, *learners);
                        b.iter(|| {
                            let mut raft = quick_raft(storage.clone());
                            raft.campaign(msg.as_bytes());
                        })
                    },
                );
            }
        });
}
