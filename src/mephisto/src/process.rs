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

use crossbeam::channel::Sender;

use crate::{env::Env, message::PaxosMessage, ProcessId};

pub trait Process: Send {
    fn env(&self) -> Arc<Mutex<Env>>;
    fn id(&self) -> ProcessId;
    fn inbox(&self) -> Sender<InboxMessage>;
    fn do_run(self) -> anyhow::Result<()>;

    fn run(self) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let env = self.env();
        let id = self.id();
        let result = self.do_run();
        let mut env = env.lock().unwrap();
        env.remove_proc(id);
        result
    }
}

#[derive(Debug)]
pub enum InboxMessage {
    /// Cause the process to exit.
    Shutdown,
    /// All messages used in Paxos.
    Paxos(PaxosMessage),
}
