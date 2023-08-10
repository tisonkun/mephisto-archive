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

use mephisto_raft::{
    eraftpb::{
        ConfChange, ConfChangeSingle, ConfChangeType, ConfState, Entry, HardState, Message,
        MessageType, Snapshot, SnapshotMetadata,
    },
    proto::new_conf_change_single,
    storage::MemStorage,
    Config, Raft, RaftLog, SoftState, StateRole, Storage, NO_LIMIT,
};

use crate::harness::interface::Interface;

#[allow(clippy::declare_interior_mutable_const)]
pub const NOP_STEPPER: Option<Interface> = Some(Interface { raft: None });

pub fn ltoa(raft_log: &RaftLog<MemStorage>) -> String {
    let mut s = format!("committed: {}\n", raft_log.committed);
    s = s + &format!("applied: {}\n", raft_log.applied);
    for (i, e) in raft_log.all_entries().iter().enumerate() {
        s = s + &format!("#{}: {:?}\n", i, e);
    }
    s
}

pub fn new_storage() -> MemStorage {
    MemStorage::new()
}

pub fn new_test_config(id: u64, election_tick: usize, heartbeat_tick: usize) -> Config {
    Config {
        id,
        election_tick,
        heartbeat_tick,
        max_size_per_msg: NO_LIMIT,
        max_inflight_msgs: 256,
        ..Default::default()
    }
}

pub fn new_test_raft(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
) -> Interface {
    let config = new_test_config(id, election, heartbeat);
    if storage.initial_state().unwrap().initialized() && peers.is_empty() {
        panic!("new_test_raft with empty peers on initialized store");
    }
    if !peers.is_empty() && !storage.initial_state().unwrap().initialized() {
        storage.initialize_with_conf_state((peers, vec![]));
    }
    new_test_raft_with_config(&config, storage)
}

pub fn new_test_raft_with_prevote(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
    pre_vote: bool,
) -> Interface {
    let mut config = new_test_config(id, election, heartbeat);
    config.pre_vote = pre_vote;
    if storage.initial_state().unwrap().initialized() && peers.is_empty() {
        panic!("new_test_raft with empty peers on initialized store");
    }
    if !peers.is_empty() && !storage.initial_state().unwrap().initialized() {
        storage.initialize_with_conf_state((peers, vec![]));
    }
    new_test_raft_with_config(&config, storage)
}

pub fn new_test_raft_with_logs(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
    logs: &[Entry],
) -> Interface {
    let config = new_test_config(id, election, heartbeat);
    if storage.initial_state().unwrap().initialized() && peers.is_empty() {
        panic!("new_test_raft with empty peers on initialized store");
    }
    if !peers.is_empty() && !storage.initial_state().unwrap().initialized() {
        storage.initialize_with_conf_state((peers, vec![]));
    }
    storage.wl().append(logs).unwrap();
    new_test_raft_with_config(&config, storage)
}

pub fn new_test_raft_with_config(config: &Config, storage: MemStorage) -> Interface {
    Interface::new(Raft::new(config, storage).unwrap())
}

pub fn hard_state(term: u64, commit: u64, vote: u64) -> HardState {
    HardState { term, vote, commit }
}

pub fn soft_state(leader_id: u64, raft_state: StateRole) -> SoftState {
    SoftState {
        leader_id,
        raft_state,
    }
}

pub const SOME_DATA: Option<&'static str> = Some("somedata");

pub fn new_message_with_entries(from: u64, to: u64, ty: MessageType, ents: Vec<Entry>) -> Message {
    Message {
        msg_type: ty as i32,
        to,
        from,
        entries: ents,
        ..Default::default()
    }
}

pub fn new_message(from: u64, to: u64, t: MessageType, n: usize) -> Message {
    let mut m = new_message_with_entries(from, to, t, vec![]);
    if n > 0 {
        let mut ents = Vec::with_capacity(n);
        for _ in 0..n {
            ents.push(new_entry(0, 0, SOME_DATA));
        }
        m.entries = ents;
    }
    m
}

pub fn new_entry(term: u64, index: u64, data: Option<&str>) -> Entry {
    let mut e = Entry {
        term,
        index,
        ..Default::default()
    };
    if let Some(d) = data {
        e.data = d.as_bytes().to_vec();
    }
    e
}

pub fn empty_entry(term: u64, index: u64) -> Entry {
    new_entry(term, index, None)
}

pub fn new_snapshot(index: u64, term: u64, voters: Vec<u64>) -> Snapshot {
    Snapshot {
        metadata: Some(SnapshotMetadata {
            conf_state: Some(ConfState {
                voters,
                ..Default::default()
            }),
            index,
            term,
        }),
        ..Default::default()
    }
}

pub fn conf_change_with_single(ty: ConfChangeType, node_id: u64) -> ConfChange {
    ConfChange {
        changes: vec![new_conf_change_single(node_id, ty)],
        ..Default::default()
    }
}

pub fn remove_node(node_id: u64) -> ConfChange {
    conf_change_with_single(ConfChangeType::RemoveNode, node_id)
}

pub fn add_node(node_id: u64) -> ConfChange {
    conf_change_with_single(ConfChangeType::AddNode, node_id)
}

pub fn add_learner(node_id: u64) -> ConfChange {
    conf_change_with_single(ConfChangeType::AddLearnerNode, node_id)
}

pub fn conf_state_simple(voters: Vec<u64>, learners: Vec<u64>) -> ConfState {
    ConfState {
        voters,
        learners,
        ..Default::default()
    }
}

pub fn conf_state(
    voters: Vec<u64>,
    learners: Vec<u64>,
    voters_outgoing: Vec<u64>,
    learners_next: Vec<u64>,
    auto_leave: bool,
) -> ConfState {
    let mut cs = conf_state_simple(voters, learners);
    cs.voters_outgoing = voters_outgoing;
    cs.learners_next = learners_next;
    cs.auto_leave = auto_leave;
    cs
}

pub fn conf_change(steps: Vec<ConfChangeSingle>) -> ConfChange {
    ConfChange {
        changes: steps,
        ..Default::default()
    }
}
