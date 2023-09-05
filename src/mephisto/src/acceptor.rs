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

use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender};
use tracing::info;

use crate::{
    env::Env,
    message::{P1bMessage, P2bMessage, PaxosMessage},
    process::{InboxMessage, Process},
    pvalue::{PValue, PValueSet},
    BallotNumber, ProcessId,
};

/// Acceptors in Paxos maintain the fault tolerant memory of Paxos and reply to p1a and p2a messages
/// received from leaders. The Acceptor state consists of two variables:
///
/// - ballot_number: a ballot number, initially [None].
/// - accepted: a set of pvalues, initially empty.
pub struct Acceptor {
    env: Arc<Mutex<Env>>,
    me: ProcessId,
    accepted: PValueSet,
    ballot_number: Option<BallotNumber>,

    tx_inbox: Sender<InboxMessage>,
    rx_inbox: Receiver<InboxMessage>,
}

impl Acceptor {
    pub fn start(env: Arc<Mutex<Env>>, me: ProcessId) {
        let (tx_inbox, rx_inbox) = crossbeam::channel::unbounded();
        let this = Acceptor {
            env: env.clone(),
            accepted: PValueSet::default(),
            ballot_number: None,
            me,
            tx_inbox,
            rx_inbox,
        };
        let mut env = env.lock().unwrap();
        env.add_proc(this);
    }
}

impl Process for Acceptor {
    fn env(&self) -> Arc<Mutex<Env>> {
        self.env.clone()
    }

    fn id(&self) -> ProcessId {
        self.me.clone()
    }

    fn inbox(&self) -> Sender<InboxMessage> {
        self.tx_inbox.clone()
    }

    /// Acceptor receives either p1a or p2a messages:
    ///
    /// - Upon receiving a p1a request message from a leader for a ballot number msg.ballot_number,
    ///   an acceptor makes the following transition. First, the acceptor adopts msg.ballot_number
    ///   if and only if it exceeds its current ballot number. Then it returns to the leader a p1b
    ///   response message containing its current ballot number and all pvalues accepted thus far by
    ///   the acceptor.
    /// - Upon receiving a p2a request message from a leader with pvalue (b, s, c), an acceptor
    ///   makes the following transition. If its current ballot number equals b, then the acceptor
    ///   accepts (b, s, c). The acceptor returns to the leader a p2b response message containing
    ///   its current ballot number.
    fn do_run(mut self) -> anyhow::Result<()> {
        info!("Acceptor started.");
        loop {
            let msg = match self.rx_inbox.recv()? {
                InboxMessage::Shutdown => {
                    info!("Acceptor shutting down ...");
                    return Ok(());
                }
                InboxMessage::Paxos(msg) => msg,
            };

            match msg {
                PaxosMessage::P1aMessage(msg) => {
                    let ballot_number = self.ballot_number.clone();
                    let update = match ballot_number {
                        None => true,
                        Some(ballot_number) => msg.ballot_number > ballot_number,
                    };
                    if update {
                        self.ballot_number = Some(msg.ballot_number);
                    }
                    let env = self.env.lock().unwrap();
                    env.send_msg(
                        msg.src,
                        PaxosMessage::P1bMessage(P1bMessage {
                            src: self.me.clone(),
                            ballot_number: self.ballot_number.clone().unwrap(),
                            accepted: self.accepted.clone(),
                        }),
                    );
                }
                PaxosMessage::P2aMessage(msg) => {
                    let ballot_number = self.ballot_number.clone();
                    let update = match ballot_number {
                        None => true,
                        Some(ballot_number) => msg.ballot_number >= ballot_number,
                    };
                    if update {
                        self.ballot_number = Some(msg.ballot_number.clone());
                    }
                    self.accepted.add(PValue {
                        ballot_number: msg.ballot_number,
                        slot_number: msg.slot_number,
                        command: msg.command,
                    });
                    let env = self.env.lock().unwrap();
                    env.send_msg(
                        msg.src,
                        PaxosMessage::P2bMessage(P2bMessage {
                            src: self.me.clone(),
                            ballot_number: self.ballot_number.clone().unwrap(),
                            slot_number: msg.slot_number,
                        }),
                    );
                }
                msg => unreachable!("Acceptor cannot process paxos message: {:?}", msg),
            }
        }
    }
}
