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

use std::collections::HashMap;

use crossbeam::channel::Sender;
use tracing::{debug, error, error_span, trace};

use crate::{
    message::PaxosMessage,
    process::{InboxMessage, Process},
    ProcessId,
};

#[derive(Default, Debug, Clone)]
pub struct Env {
    processes: HashMap<ProcessId, Sender<InboxMessage>>,
}

impl Env {
    pub fn send_msg(&self, dst: ProcessId, msg: PaxosMessage) {
        if let Some(proc) = self.processes.get(&dst) {
            if let Err(err) = proc.send(InboxMessage::Paxos(msg)) {
                if dst.0.contains("commander") || dst.0.contains("scout") {
                    // Commander and Scout may exit early when the majority has replied.
                    debug!(?err, "Cannot send msg {:?} to dst {dst}.", err.0);
                } else {
                    error!(?err, "Cannot send msg {:?} to dst {dst}.", err.0);
                }
            }
        }
    }

    pub fn add_proc(&mut self, proc: impl Process + 'static) {
        if self.processes.insert(proc.id(), proc.inbox()).is_some() {
            panic!("duplicate process id: {:?}", proc.id());
        }

        std::thread::spawn(move || {
            error_span!("process", id = ?proc.id()).in_scope(|| match proc.run() {
                Ok(()) => debug!("Process normally exited."),
                Err(err) => error!(?err, "Process failed."),
            })
        });
    }

    pub fn remove_proc(&mut self, dst: ProcessId) {
        if let Some(proc) = self.processes.remove(&dst) {
            if let Err(err) = proc.send(InboxMessage::Shutdown) {
                trace!(?err, "{dst} has been already shutdown.");
            }
        }
    }
}
