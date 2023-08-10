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

use mephisto_raft::eraftpb::MessageType;

use crate::harness::*;

// test_reject_stale_term_message tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
#[test]
fn test_reject_stale_term_message() {
    let scenario = fail::FailScenario::setup();
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    fail::cfg("before_step", "panic").unwrap();
    r.load_state(&hard_state(2, 0, 0));

    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.term = r.term - 1;
    r.step(m).expect("");
    scenario.teardown();
}

// ensure that the Step function ignores the message from old term and does not pass it to the
// actual stepX function.
#[test]
fn test_step_ignore_old_term_msg() {
    let scenario = fail::FailScenario::setup();
    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
    fail::cfg("before_step", "panic").unwrap();
    sm.term = 2;
    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.term = 1;
    sm.step(m).expect("");
    scenario.teardown();
}
