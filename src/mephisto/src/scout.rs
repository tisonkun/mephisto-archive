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
    message::{AdoptedMessage, P1aMessage, PaxosMessage, PreemptedMessage},
    process::{InboxMessage, Process},
    pvalue::PValueSet,
    BallotNumber, ProcessId,
};

/// The scout runs what is known as phase 1 of the Synod protocol. Every scout is created for a
/// specific ballot number.
pub struct Scout {
    env: Arc<Mutex<Env>>,
    me: ProcessId,
    leader: ProcessId,
    acceptors: Vec<ProcessId>,
    ballot_number: BallotNumber,

    tx_inbox: Sender<InboxMessage>,
    rx_inbox: Receiver<InboxMessage>,
}

impl Scout {
    pub fn start(
        env: Arc<Mutex<Env>>,
        me: ProcessId,
        leader: ProcessId,
        acceptors: Vec<ProcessId>,
        ballot_number: BallotNumber,
    ) {
        let (tx_inbox, rx_inbox) = crossbeam::channel::unbounded();
        let this = Scout {
            env: env.clone(),
            me,
            leader,
            acceptors,
            ballot_number,
            tx_inbox,
            rx_inbox,
        };
        let mut env = env.lock().unwrap();
        env.add_proc(this);
    }
}

impl Process for Scout {
    fn env(&self) -> Arc<Mutex<Env>> {
        self.env.clone()
    }

    fn id(&self) -> ProcessId {
        self.me.clone()
    }

    fn inbox(&self) -> Sender<InboxMessage> {
        self.tx_inbox.clone()
    }

    /// A scout sends a p1a message to all acceptors, and waits for p1b responses. In each such
    /// response the ballot number in the message will be greater than the ballot number of the
    /// scout. There are two cases:
    ///
    /// - When a scout receives a p1b message it records all the values that are accepted by the
    ///   acceptor that sent it. If the scout receives such p1b messages from all acceptors in a
    ///   majority of acceptors, then the scout learns that its ballot number is adopted. In this
    ///   case, the commander notifies its leader and exits.
    /// - If a scout receives a p1b message with a different ballot number from some acceptor, then
    ///   it learns that a higher ballot is active. This means that the scout's ballot number may no
    ///   longer be able to make progress. In this case, the scout notifies its leader about the
    ///   existence of the higher ballot number, and exits.
    fn do_run(self) -> anyhow::Result<()> {
        let mut waiting = BTreeSet::new();
        {
            let env = self.env.lock().unwrap();
            for acceptor in self.acceptors.iter().cloned() {
                env.send_msg(
                    acceptor.clone(),
                    PaxosMessage::P1aMessage(P1aMessage {
                        src: self.me.clone(),
                        ballot_number: self.ballot_number.clone(),
                    }),
                );
                waiting.insert(acceptor);
            }
        }

        let mut pvalues = PValueSet::default();
        loop {
            let msg = match self.rx_inbox.recv()? {
                InboxMessage::Shutdown => {
                    info!("Scout shutting down ...");
                    return Ok(());
                }
                InboxMessage::Paxos(PaxosMessage::P1bMessage(msg)) => msg,
                InboxMessage::Paxos(msg) => {
                    unreachable!("Commander cannot process paxos message: {:?}", msg)
                }
            };

            if self.ballot_number == msg.ballot_number && waiting.contains(&msg.src) {
                pvalues.update(msg.accepted);
                waiting.remove(&msg.src);
                if 2 * waiting.len() > self.acceptors.len() {
                    let env = self.env.lock().unwrap();
                    env.send_msg(
                        self.leader,
                        PaxosMessage::AdoptedMessage(AdoptedMessage {
                            src: self.me,
                            ballot_number: self.ballot_number,
                            accepted: pvalues,
                        }),
                    );
                    return Ok(());
                }
            } else {
                let env = self.env.lock().unwrap();
                env.send_msg(
                    self.leader,
                    PaxosMessage::PreemptedMessage(PreemptedMessage {
                        src: self.me,
                        ballot_number: msg.ballot_number,
                    }),
                );
                return Ok(());
            }
        }
    }
}
