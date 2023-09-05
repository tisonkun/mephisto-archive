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

use crate::{pvalue::PValueSet, BallotNumber, Command, ProcessId};

/// Enumeration for all messages used in Paxos.
/// Every message has a source.
#[derive(Debug)]
pub enum PaxosMessage {
    P1aMessage(P1aMessage),
    P1bMessage(P1bMessage),
    P2aMessage(P2aMessage),
    P2bMessage(P2bMessage),
    PreemptedMessage(PreemptedMessage),
    AdoptedMessage(AdoptedMessage),
    DecisionMessage(DecisionMessage),
    RequestMessage(RequestMessage),
    ProposeMessage(ProposeMessage),
}

/// Sent by Scouts to Acceptors in Phase 1 of Paxos.
/// Carries a ballot number.
#[derive(Debug)]
pub struct P1aMessage {
    pub src: ProcessId,
    pub ballot_number: BallotNumber,
}

/// Sent by Acceptors to Scouts in Phase 1 of Paxos.
/// Carries a ballot number and the set of accepted pvalues.
#[derive(Debug)]
pub struct P1bMessage {
    pub src: ProcessId,
    pub ballot_number: BallotNumber,
    pub accepted: PValueSet,
}

/// Sent by Commanders to Acceptors in Phase 2 of Paxos.
/// Carries a ballot number, a slot number and a command.
#[derive(Debug)]
pub struct P2aMessage {
    pub src: ProcessId,
    pub ballot_number: BallotNumber,
    pub slot_number: u64,
    pub command: Command,
}

/// Sent by Acceptors to Commanders in Phase 2 of Paxos.
/// Carries a ballot number and a slot number.
#[derive(Debug)]
pub struct P2bMessage {
    pub src: ProcessId,
    pub ballot_number: BallotNumber,
    pub slot_number: u64,
}

/// Sent by Scouts or Commanders to Leaders.
/// Carries a ballot number.
#[derive(Debug)]
pub struct PreemptedMessage {
    pub src: ProcessId,
    pub ballot_number: BallotNumber,
}

/// Sent by Scouts to Leaders.
/// Carries a ballot number and the set of accepted pvalues.
#[derive(Debug)]
pub struct AdoptedMessage {
    pub src: ProcessId,
    pub ballot_number: BallotNumber,
    pub accepted: PValueSet,
}

/// Sent by Commanders to Replicas.
/// Carries a slot number and a command.
#[derive(Debug)]
pub struct DecisionMessage {
    pub src: ProcessId,
    pub slot_number: u64,
    pub command: Command,
}

/// Sent by Clients to Replicas.
/// Carries a command.
#[derive(Debug)]
pub struct RequestMessage {
    pub src: ProcessId,
    pub command: Command,
}

/// Sent by Replicas to Leaders.
/// Carries a slot number and a command.
#[derive(Debug)]
pub struct ProposeMessage {
    pub src: ProcessId,
    pub slot_number: u64,
    pub command: Command,
}
