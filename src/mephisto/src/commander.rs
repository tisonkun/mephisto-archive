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
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use crossbeam::channel::{Receiver, Sender};
use tracing::info;

use crate::{
    env::Env,
    message::{DecisionMessage, P2aMessage, PaxosMessage, PreemptedMessage},
    process::{InboxMessage, Process},
    BallotNumber, Command, ProcessId,
};

/// The commander runs what is known as phase 2 of the Synod protocol.  Every commander is created
/// for a specific ballot number, slot number and command triple.
pub struct Commander {
    env: Arc<Mutex<Env>>,
    me: ProcessId,
    leader: ProcessId,
    acceptors: Vec<ProcessId>,
    replicas: Vec<ProcessId>,
    ballot_number: BallotNumber,
    slot_number: u64,
    command: Command,

    tx_inbox: Sender<InboxMessage>,
    rx_inbox: Receiver<InboxMessage>,
}

impl Commander {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        env: Arc<Mutex<Env>>,
        me: ProcessId,
        leader: ProcessId,
        acceptors: Vec<ProcessId>,
        replicas: Vec<ProcessId>,
        ballot_number: BallotNumber,
        slot_number: u64,
        command: Command,
    ) {
        let (tx_inbox, rx_inbox) = crossbeam::channel::unbounded();
        let this = Commander {
            env: env.clone(),
            me,
            leader,
            acceptors,
            replicas,
            ballot_number,
            slot_number,
            command,
            tx_inbox,
            rx_inbox,
        };
        let mut env = env.lock().unwrap();
        env.add_proc(this);
    }
}

impl Process for Commander {
    fn env(&self) -> Arc<Mutex<Env>> {
        self.env.clone()
    }

    fn id(&self) -> ProcessId {
        self.me.clone()
    }

    fn inbox(&self) -> Sender<InboxMessage> {
        self.tx_inbox.clone()
    }

    /// A commander sends a p2a message to all acceptors, and waits for p2b responses. In each such
    /// response the ballot number in the message will be greater than the ballot number of the
    /// commander. There are two cases:
    ///
    /// - If a commander receives p2b messages with its ballot number from all acceptors in a
    ///   majority of acceptors, then the commander learns that the command has been chosen for the
    ///   slot. In this case, the commander notifies all replicas and exits.
    /// - If a commander receives a p2b message with a different ballot number from some acceptor,
    ///   then it learns that a higher ballot is active. This means that the commander's ballot
    ///   number may no longer be able to make progress. In this case, the commander notifies its
    ///   leader about the existence of the higher ballot number, and exits.
    fn do_run(self) -> anyhow::Result<()> {
        let mut waiting = BTreeSet::new();
        {
            let env = self.env.lock().unwrap();
            for acceptor in self.acceptors.iter().cloned() {
                env.send_msg(
                    acceptor.clone(),
                    PaxosMessage::P2aMessage(P2aMessage {
                        src: self.me.clone(),
                        ballot_number: self.ballot_number.clone(),
                        slot_number: self.slot_number,
                        command: self.command.clone(),
                    }),
                );
                waiting.insert(acceptor);
            }
        }

        loop {
            let msg = match self.rx_inbox.recv()? {
                InboxMessage::Shutdown => {
                    info!("Commander shutting down ...");
                    return Ok(());
                }
                InboxMessage::Paxos(PaxosMessage::P2bMessage(msg)) => msg,
                InboxMessage::Paxos(msg) => {
                    unreachable!("Commander cannot process paxos message: {:?}", msg)
                }
            };

            if self.ballot_number != msg.ballot_number {
                let env = self.env.lock().unwrap();
                env.send_msg(
                    self.leader,
                    PaxosMessage::PreemptedMessage(PreemptedMessage {
                        src: self.me.clone(),
                        ballot_number: msg.ballot_number,
                    }),
                );
                return Ok(());
            } else {
                waiting.remove(&msg.src);
                if 2 * waiting.len() < self.acceptors.len() {
                    let env = self.env.lock().unwrap();
                    for replica in self.replicas.iter().cloned() {
                        env.send_msg(
                            replica,
                            PaxosMessage::DecisionMessage(DecisionMessage {
                                src: self.me.clone(),
                                slot_number: self.slot_number,
                                command: self.command.clone(),
                            }),
                        );
                    }
                    return Ok(());
                }
            }
        }
    }
}
