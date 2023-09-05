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
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

pub mod acceptor;
pub mod commander;
pub mod env;
pub mod leader;
pub mod message;
pub mod process;
pub mod pvalue;
pub mod replica;
pub mod scout;

pub const WINDOW: u64 = 5;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct ProcessId(Arc<String>);

impl ProcessId {
    pub fn new(id: String) -> ProcessId {
        ProcessId(Arc::new(id))
    }
}

impl Display for ProcessId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A ballot number is a lexicographically ordered pair of an integer and the identifier of the
/// ballot's leader.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct BallotNumber {
    round: u64,
    leader_id: ProcessId,
}

impl Display for BallotNumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BN({},{})", self.round, self.leader_id)
    }
}

/// A configuration consists of a list of replicas, a list of acceptors and a list of leaders.
#[derive(Default, Debug, Clone)]
pub struct Config {
    pub replicas: Vec<ProcessId>,
    pub acceptors: Vec<ProcessId>,
    pub leaders: Vec<ProcessId>,
}

#[derive(Debug, Clone)]
pub enum Command {
    Operation(OperationCommand),
    Config(ConfigCommand),
}

impl Command {
    pub fn id(&self) -> u64 {
        match self {
            Command::Operation(cmd) => cmd.req_id,
            Command::Config(cmd) => cmd.req_id,
        }
    }
}

impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        self.id().eq(&other.id())
    }
}

impl Eq for Command {}

/// A command consists of the process identifier of the client submitting the request, a
/// client-local request identifier, and an operation (which can be anything).
#[derive(Clone)]
pub struct OperationCommand {
    pub client: ProcessId,
    pub req_id: u64,
    pub op: Vec<u8>,
}

impl Debug for OperationCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("OperationCommand");
        de.field("client", &self.client);
        de.field("req_id", &self.req_id);
        de.field("op", &String::from_utf8_lossy(self.op.as_slice()));
        de.finish()
    }
}

/// A reconfiguration command is a command sent by a client to reconfigure the system.  A
/// reconfiguration command consists of the process identifier of the client submitting the request,
/// a client-local request identifier, and a configuration.
#[derive(Debug, Clone)]
pub struct ConfigCommand {
    pub client: ProcessId,
    pub req_id: u64,
    pub config: Config,
}
