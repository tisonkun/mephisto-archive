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
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use crossbeam::channel::{Receiver, Sender};
use tracing::info;

use crate::{
    commander::Commander,
    env::Env,
    message::PaxosMessage,
    process::{InboxMessage, Process},
    scout::Scout,
    BallotNumber, Command, Config, ProcessId,
};

/// Leader receives requests from replicas, serializes requests and responds to replicas. Leader
/// maintains four state variables:
///
/// - ballot_number: a monotonically increasing ballot number
/// - active: a boolean flag, initially false
/// - proposals: a map of slot numbers to proposed commands in the form of a set of (slot number,
///   command) pairs, initially empty. At any time, there is at most one entry per slot number in
///   the set.
/// - config: current cluster membership to lookup replicas and acceptors.
pub struct Leader {
    env: Arc<Mutex<Env>>,
    me: ProcessId,
    ballot_number: BallotNumber,
    config: Config,
    active: bool,
    proposals: HashMap<u64, Command>,

    tx_inbox: Sender<InboxMessage>,
    rx_inbox: Receiver<InboxMessage>,
}

impl Leader {
    pub fn start(env: Arc<Mutex<Env>>, me: ProcessId, config: Config) {
        let (tx_inbox, rx_inbox) = crossbeam::channel::unbounded();
        let this = Leader {
            env: env.clone(),
            me: me.clone(),
            config,
            active: false,
            proposals: HashMap::new(),
            ballot_number: BallotNumber {
                round: 0,
                leader_id: me,
            },
            tx_inbox,
            rx_inbox,
        };
        let mut env = env.lock().unwrap();
        env.add_proc(this);
    }

    fn start_scout(&self) {
        Scout::start(
            self.env.clone(),
            ProcessId::new(format!("scout:{}:{}", self.me, self.ballot_number)),
            self.me.clone(),
            self.config.acceptors.clone(),
            self.ballot_number.clone(),
        );
    }
}

impl Process for Leader {
    fn env(&self) -> Arc<Mutex<Env>> {
        self.env.clone()
    }

    fn id(&self) -> ProcessId {
        self.me.clone()
    }

    fn inbox(&self) -> Sender<InboxMessage> {
        self.tx_inbox.clone()
    }

    /// The leader starts by spawning a scout for its initial ballot number, and then enters into a
    /// loop awaiting messages. There are three types of messages that cause transitions:
    ///
    /// - Propose: A replica proposes given command for given slot number.
    /// - Adopted: Sent by a scout, this message signifies that the current ballot number has been
    ///   adopted by a majority of acceptors. (If an adopted message arrives for an old ballot
    ///   number, it is ignored.) The set pvalues contains all pvalues accepted by these acceptors
    ///   prior to the adopted ballot number.
    /// - Preempted: Sent by either a scout or a commander, it means that some acceptor has adopted
    ///   the ballot number that is included in the message. If this ballot number is higher than
    ///   the current ballot number of the leader, it may no longer be possible to use the current
    ///   ballot number to choose a command.
    fn do_run(mut self) -> anyhow::Result<()> {
        info!("Leader started.");

        self.start_scout();

        loop {
            let msg = match self.rx_inbox.recv()? {
                InboxMessage::Shutdown => {
                    info!("Leader shutting down ...");
                    return Ok(());
                }
                InboxMessage::Paxos(msg) => msg,
            };

            match msg {
                PaxosMessage::ProposeMessage(msg) => {
                    if let Entry::Vacant(proposal) = self.proposals.entry(msg.slot_number) {
                        proposal.insert(msg.command.clone());
                        if self.active {
                            Commander::start(
                                self.env.clone(),
                                ProcessId::new(format!(
                                    "commander:{}:{}:{}",
                                    self.me, self.ballot_number, msg.slot_number
                                )),
                                self.me.clone(),
                                self.config.acceptors.clone(),
                                self.config.replicas.clone(),
                                self.ballot_number.clone(),
                                msg.slot_number,
                                msg.command,
                            );
                        }
                    }
                }
                PaxosMessage::AdoptedMessage(msg) => {
                    if self.ballot_number == msg.ballot_number {
                        for (slot_number, value) in msg.accepted.values() {
                            self.proposals.insert(*slot_number, value.command.clone());
                        }
                        for (slot_number, proposal) in self.proposals.iter() {
                            Commander::start(
                                self.env.clone(),
                                ProcessId::new(format!(
                                    "commander:{}:{}:{}",
                                    self.me, self.ballot_number, slot_number
                                )),
                                self.me.clone(),
                                self.config.acceptors.clone(),
                                self.config.replicas.clone(),
                                self.ballot_number.clone(),
                                *slot_number,
                                proposal.clone(),
                            );
                        }
                        self.active = true;
                    }
                }
                PaxosMessage::PreemptedMessage(msg) => {
                    if self.ballot_number < msg.ballot_number {
                        self.active = false;
                        self.ballot_number = BallotNumber {
                            round: msg.ballot_number.round + 1,
                            leader_id: self.me.clone(),
                        };
                        self.start_scout();
                    }
                }
                msg => unreachable!("Leader cannot process paxos message: {:?}", msg),
            }
        }
    }
}
