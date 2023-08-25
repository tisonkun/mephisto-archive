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

use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};

use bytes::Bytes;
use crossbeam::{
    channel::{Receiver, Select, Sender, TryRecvError},
    sync::WaitGroup,
};
use mephisto_raft::{
    eraftpb,
    eraftpb::{Entry, EntryType},
    storage::MemStorage,
    Config, Peer, RawNode, SoftState, INVALID_ID,
};
use prost::Message;
use tracing::{error, error_span, info, trace};

use crate::{
    datatree::DataTree,
    proto::{
        datatree::{CreateRequest, DataTreeRequest, ReplyHeader, RequestHeader, RequestType},
        FatReply, FatRequest, ReqId,
    },
    router::RaftRouter,
    service::RaftService,
    RaftMessage,
};

#[derive(Default)]
struct RaftState {
    state: SoftState,
    applied_index: u64,
}

pub struct ShutdownNode {
    tx_shutdown: Sender<()>,
}

impl ShutdownNode {
    pub fn shutdown(self) {
        if let Err(err) = self.tx_shutdown.send(()) {
            // receiver closed - already shutdown
            trace!(?err, "shutdown node cannot send signal");
        }
    }
}

#[derive(Debug, Default)]
struct PendingReads {
    head_write_seq: u64,
    seen_write_seq: u64,
    reads: VecDeque<PendingRead>,
}

#[derive(Debug)]
struct PendingRead {
    read_index: u64,
    wait_write_seq: u64,
    request: FatRequest,
}

pub struct RaftNode {
    this: Peer,
    peers: Vec<Peer>,
    node: RawNode<MemStorage>,
    state: RaftState,

    // mock datatree
    datatree: DataTree,

    sending_requests: VecDeque<FatRequest>,
    pending_requests: BTreeMap<ReqId, FatRequest>,
    pending_reads: BTreeMap<uuid::Uuid /* client_id */, PendingReads>,

    tx_shutdown: Sender<()>,
    rx_shutdown: Receiver<()>,
    tx_request: Sender<FatRequest>,
    rx_request: Receiver<FatRequest>,
    tx_message: Sender<RaftMessage>,
    rx_message: Receiver<RaftMessage>,
    tick: Receiver<Instant>,
}

impl RaftNode {
    pub fn new(this: Peer, peers: Vec<Peer>) -> RaftNode {
        let id = this.id;

        let config = {
            let mut config = Config::new(id);
            config.pre_vote = true;
            config.priority = (1 << id) as i64;
            config.election_tick = 10;
            config.heartbeat_tick = 1;
            config.max_size_per_msg = 1024 * 1024 * 1024;
            config.validate().unwrap();
            config
        };

        let voters = peers.iter().map(|p| p.id).collect::<Vec<_>>();
        let storage = MemStorage::new_with_conf_state((voters, vec![]));
        storage.wl().mut_hard_state().term = 1;
        let node = RawNode::new(&config, storage).unwrap();

        let (tx_message, rx_message) = crossbeam::channel::unbounded();
        let (tx_request, rx_request) = crossbeam::channel::unbounded();
        let (tx_shutdown, rx_shutdown) = crossbeam::channel::bounded(1);
        let tick = crossbeam::channel::tick(Duration::from_millis(100));

        RaftNode {
            this,
            peers,
            node,
            state: RaftState::default(),
            datatree: Default::default(),
            sending_requests: VecDeque::new(),
            pending_requests: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            tx_shutdown,
            rx_shutdown,
            tx_request,
            rx_request,
            tx_message,
            rx_message,
            tick,
        }
    }

    pub fn tx_request(&self) -> Sender<FatRequest> {
        self.tx_request.clone()
    }

    pub fn shutdown(&self) -> ShutdownNode {
        ShutdownNode {
            tx_shutdown: self.tx_shutdown.clone(),
        }
    }

    pub fn do_main(self, id: u64, wg: WaitGroup) {
        std::thread::spawn(move || {
            error_span!("node", id).in_scope(|| match self.internal_do_main() {
                Ok(()) => info!("node shutdown"),
                Err(err) => error!(?err, "node crashed"),
            });
            drop(wg);
        });
    }

    pub fn internal_do_main(mut self) -> anyhow::Result<()> {
        let service = RaftService::new(self.this.clone(), self.tx_message.clone())?;
        let shutdown_service = service.shutdown();
        let shutdown_waiters = WaitGroup::new();
        service.do_main(self.this.id, shutdown_waiters.clone());

        let (router, shutdown_router) = RaftRouter::new(self.this.clone(), self.peers.clone());
        let _guard = scopeguard::guard((), move |_| {
            shutdown_router.shutdown();
            shutdown_service.shutdown();
            shutdown_waiters.wait();
        });

        loop {
            if self.process_inbound()? {
                return Ok(());
            }

            self.process_ready(&router)?;
        }
    }

    // return true if shutdown
    fn process_inbound(&mut self) -> anyhow::Result<bool> {
        let mut select = Select::new();
        select.recv(&self.rx_shutdown);
        select.recv(&self.rx_message);
        select.recv(&self.rx_request);
        select.recv(&self.tick);
        select.ready();

        if !matches!(self.rx_shutdown.try_recv(), Err(TryRecvError::Empty)) {
            return Ok(true);
        }

        for _ in self.tick.try_iter() {
            if self.node.tick() {
                return Ok(false);
            }
        }

        for req in self.rx_request.try_iter() {
            self.sending_requests.push_back(req);
        }

        if self.state.state.leader_id != INVALID_ID {
            for request in self.sending_requests.drain(..) {
                match request.request {
                    DataTreeRequest::Create(ref req) => {
                        let mut data = request.header.encode_length_delimited_to_vec();
                        data.append(&mut req.encode_length_delimited_to_vec());
                        self.node.propose(request.req_id.serialize(), data)?;
                        let entry = self
                            .pending_reads
                            .entry(request.req_id.client_id)
                            .or_default();
                        entry.head_write_seq = request.req_id.seq_id;
                        self.pending_requests.insert(request.req_id, request);
                    }
                    DataTreeRequest::GetData(_) => {
                        self.node.read_index(request.req_id.serialize());
                        let entry = self
                            .pending_reads
                            .entry(request.req_id.client_id)
                            .or_default();
                        entry.reads.push_back(PendingRead {
                            read_index: u64::MAX,
                            wait_write_seq: entry.head_write_seq,
                            request,
                        });
                    }
                }
            }
        }

        for reads in self.pending_reads.values_mut() {
            for read in reads.reads.iter_mut() {
                if read.read_index == u64::MAX {
                    self.node.read_index(read.request.req_id.serialize());
                }
            }
        }

        for msg in self.rx_message.try_iter() {
            self.node.step(msg)?;
        }

        Ok(false)
    }

    fn process_ready(&mut self, router: &RaftRouter) -> anyhow::Result<()> {
        if !self.node.has_ready() {
            return Ok(());
        }

        let mut ready = self.node.ready();

        if let Some(ss) = ready.ss() {
            if ss.raft_state != self.state.state.raft_state {
                info!(
                    "changing raft node role from {:?} to {:?}",
                    self.state.state.raft_state, ss.raft_state
                );
            }
            self.state.state = *ss;
        }

        for msg in ready.take_messages() {
            self.process_message(msg, router);
        }

        if let Some(hs) = ready.hs() {
            self.node.store().wl().set_hard_state(hs.clone());
        }

        if !ready.entries().is_empty() {
            self.node.store().wl().append(ready.entries())?;
        }

        for msg in ready.take_persisted_messages() {
            self.process_message(msg, router);
        }

        for read_state in ready.take_read_states() {
            let req_id = ReqId::deserialize(read_state.request_ctx.as_slice());
            if let Some(req) = self.pending_reads.get_mut(&req_id.client_id) {
                for read in req.reads.iter_mut() {
                    let ReqId { seq_id, client_id } = read.request.req_id;
                    debug_assert_eq!(req_id.client_id, client_id);
                    if seq_id > req_id.seq_id {
                        break;
                    }
                    read.read_index = read.read_index.min(read_state.index);
                }
            }
        }

        self.process_pending_reads(None);

        for entry in ready.take_committed_entries() {
            self.process_committed_entry(entry)?;
            self.process_pending_reads(None);
        }

        self.node.advance(ready);
        Ok(())
    }

    fn process_message(&self, msg: RaftMessage, router: &RaftRouter) {
        match msg.msg_type() {
            eraftpb::MessageType::MsgAppend
            | eraftpb::MessageType::MsgRequestPreVote
            | eraftpb::MessageType::MsgAppendResponse
            | eraftpb::MessageType::MsgRequestPreVoteResponse
            | eraftpb::MessageType::MsgRequestVote
            | eraftpb::MessageType::MsgRequestVoteResponse
            | eraftpb::MessageType::MsgHeartbeat
            | eraftpb::MessageType::MsgHeartbeatResponse
            | eraftpb::MessageType::MsgPropose
            | eraftpb::MessageType::MsgReadIndex
            | eraftpb::MessageType::MsgReadIndexResp => {
                if msg.to != self.this.id {
                    router.send(msg.to, msg);
                } else {
                    unreachable!("outbound message send to self {msg:?}")
                }
            }
            _ => unimplemented!("unimplemented {msg:?}"),
        }
    }

    // process pending reads if applied_index >= read_index
    fn process_pending_reads(&mut self, req_id: Option<ReqId>) {
        let reads = match req_id {
            None => self.pending_reads.values_mut().collect(),
            Some(ReqId { client_id, .. }) => {
                if let Some(reads) = self.pending_reads.get_mut(&client_id) {
                    vec![reads]
                } else {
                    vec![]
                }
            }
        };

        for reads in reads {
            let seen_write_seq = reads.seen_write_seq;
            let seen_apply_index = self.state.applied_index;
            while let Some(read) = reads.reads.front() {
                if read.wait_write_seq <= seen_write_seq && read.read_index <= seen_apply_index {
                    let read = reads.reads.pop_front().unwrap();
                    match read.request.request {
                        DataTreeRequest::GetData(request) => {
                            let reply = self.datatree.get_data(request);
                            let reply = FatReply {
                                header: ReplyHeader {
                                    req_id: read.request.req_id.seq_id,
                                    txn_id: self.state.applied_index,
                                    err: 0,
                                },
                                reply,
                            };
                            read.request.reply.send(reply).unwrap();
                        }
                        r => unreachable!("illegal request {:?}", r),
                    }
                } else {
                    break;
                }
            }
        }
    }

    fn process_committed_entry(&mut self, entry: Entry) -> anyhow::Result<()> {
        let applied_index = self.state.applied_index.max(entry.index);
        match entry.entry_type() {
            EntryType::EntryNormal => {
                if entry.data.is_empty() {
                    // empty entry indicate a becoming leader event
                    self.state.applied_index = applied_index;
                    return Ok(());
                }

                let req_id = ReqId::deserialize(entry.context.as_slice());
                let (hdr, request) = {
                    let mut payload = Bytes::from(entry.data);
                    let hdr = RequestHeader::decode_length_delimited(&mut payload)?;
                    let request = match hdr.req_type() {
                        RequestType::ReqCreate => DataTreeRequest::Create(
                            CreateRequest::decode_length_delimited(&mut payload)?,
                        ),
                        r => unreachable!("illegal request type {:?}", r),
                    };
                    (hdr, request)
                };

                match request {
                    DataTreeRequest::Create(request) => {
                        let reply = self.datatree.create(request);
                        if let Some(reads) = self.pending_reads.get_mut(&req_id.client_id) {
                            for read in reads.reads.iter_mut() {
                                if read.wait_write_seq < req_id.seq_id {
                                    // seen latter write, must flush
                                    read.read_index = 0;
                                } else {
                                    break;
                                }
                            }
                            self.process_pending_reads(Some(req_id));
                        }
                        if let Some(sending_reply) = self.pending_requests.remove(&req_id) {
                            let reply = FatReply {
                                header: ReplyHeader {
                                    req_id: req_id.seq_id,
                                    txn_id: applied_index,
                                    err: 0,
                                },
                                reply,
                            };
                            sending_reply.reply.send(reply).unwrap();
                        }
                    }
                    _ => unreachable!("illegal request type {:?}", hdr.req_type()),
                }

                if let Some(reads) = self.pending_reads.get_mut(&req_id.client_id) {
                    reads.seen_write_seq = req_id.seq_id;
                }
            }
            EntryType::EntryConfChange => unimplemented!("EntryConfChange"),
        }

        self.state.applied_index = applied_index;
        Ok(())
    }
}
