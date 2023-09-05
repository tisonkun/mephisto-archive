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
    env::Env,
    message::{PaxosMessage, ProposeMessage},
    process::{InboxMessage, Process},
    Command, Config, ProcessId, WINDOW,
};

pub struct Replica {
    env: Arc<Mutex<Env>>,
    me: ProcessId,
    slot_in: u64,
    slot_out: u64,
    config: Config,
    requests: Vec<Command>,
    proposals: HashMap<u64, Command>,
    decisions: HashMap<u64, Command>,

    tx_inbox: Sender<InboxMessage>,
    rx_inbox: Receiver<InboxMessage>,
}

impl Replica {
    pub fn start(env: Arc<Mutex<Env>>, me: ProcessId, config: Config) {
        let (tx_inbox, rx_inbox) = crossbeam::channel::unbounded();
        let this = Replica {
            env: env.clone(),
            me,
            slot_in: 1,
            slot_out: 1,
            config,
            requests: vec![],
            proposals: Default::default(),
            decisions: Default::default(),
            tx_inbox,
            rx_inbox,
        };
        let mut env = env.lock().unwrap();
        env.add_proc(this);
    }

    /// This function tries to transfer requests from the set requests to proposals. It uses slot_in
    /// to look for unused slots within the window of slots with known configurations. For each such
    /// slot, it first checks if the configuration for that slot is different from the prior slot by
    /// checking if the decision in (slot_in - WINDOW) is a reconfiguration command. If so, the
    /// function updates the configuration for slot s. Then the function pops a request from
    /// requests and adds it as a proposal for slot_in to the set proposals. Finally, it sends a
    /// Propose message to all leaders in the configuration of slot_in.
    pub fn propose(&mut self) {
        while !self.requests.is_empty() && self.slot_in < self.slot_out + WINDOW {
            if !self.decisions.contains_key(&self.slot_in) {
                let command = self.requests.pop().unwrap();
                self.proposals.insert(self.slot_in, command.clone());
                let env = self.env.lock().unwrap();
                for leader in self.config.leaders.iter().cloned() {
                    env.send_msg(
                        leader,
                        PaxosMessage::ProposeMessage(ProposeMessage {
                            src: self.me.clone(),
                            slot_number: self.slot_in,
                            command: command.clone(),
                        }),
                    );
                }
            }

            self.slot_in += 1;

            if self.slot_in <= WINDOW {
                continue;
            }
            let decision = match self.decisions.get(&(self.slot_in - WINDOW)) {
                None => continue,
                Some(Command::Operation(_)) => continue,
                Some(Command::Config(config)) => config.config.clone(),
            };
            self.config = decision;
            info!("Updated config: {:?}", self.config);
        }
    }

    /// This function is invoked with the same sequence of commands at all replicas. First, it
    /// checks to see if it has already performed the command. Different replicas may end up
    /// proposing the same command for different slots, and thus the same command may be decided
    /// multiple times. The corresponding operation is evaluated only if the command is new and it
    /// is not a reconfiguration request. If so, perform() applies the requested operation to the
    /// application state. In either case, the function increments slot out.
    pub fn perform(&mut self, command: Command) {
        for s in 1..self.slot_out {
            let exist = match self.decisions.get(&s) {
                None => false,
                Some(cmd) => command.eq(cmd),
            };
            if exist {
                self.slot_out += 1;
                return;
            }
        }

        if matches!(command, Command::Config(_)) {
            self.slot_out += 1;
            return;
        }

        // Mock implementation for real command perform
        info!("Performing {} : {:?}", self.slot_out - 1, command);
        self.slot_out += 1;
    }
}

impl Process for Replica {
    fn env(&self) -> Arc<Mutex<Env>> {
        self.env.clone()
    }

    fn id(&self) -> ProcessId {
        self.me.clone()
    }

    fn inbox(&self) -> Sender<InboxMessage> {
        self.tx_inbox.clone()
    }

    /// A replica runs in an infinite loop, receiving messages. Replicas receive two kinds of
    /// messages:
    ///
    /// - Requests: When it receives a request from a client, the replica adds the request to set
    ///   requests. Next, the replica invokes the function propose().
    /// - Decisions: Decisions may arrive out-of-order and multiple times. For each decision
    ///   message, the replica adds the decision to the set decisions. Then, in a loop, it considers
    ///   which decisions are ready for execution before trying to receive more messages. If there
    ///   is a decision corresponding to the current slot out, the replica first checks to see if it
    ///   has proposed a different command for that slot. If so, the replica removes that command
    ///   from the set proposals and returns it to set requests so it can be proposed again at a
    ///   later time. Next, the replica invokes perform().
    fn do_run(mut self) -> anyhow::Result<()> {
        info!("Replica started.");
        loop {
            let msg = match self.rx_inbox.recv()? {
                InboxMessage::Shutdown => {
                    info!("Replica shutting down ...");
                    return Ok(());
                }
                InboxMessage::Paxos(msg) => msg,
            };
            match msg {
                PaxosMessage::RequestMessage(msg) => self.requests.push(msg.command),
                PaxosMessage::DecisionMessage(msg) => {
                    self.decisions.insert(msg.slot_number, msg.command);
                    while let Some(decision) = self.decisions.get(&self.slot_out) {
                        if let Entry::Occupied(proposal) = self.proposals.entry(self.slot_out) {
                            let cmd = proposal.remove();
                            if cmd.ne(decision) {
                                self.requests.push(cmd);
                            }
                        }
                        self.perform(decision.clone());
                    }
                }
                msg => unreachable!("Replica cannot process paxos message: {:?}", msg),
            }
            self.propose();
        }
    }
}
