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
    fmt::{Display, Formatter},
    io::Write,
};

use goldenfile::Mint;
use itertools::Itertools;

use crate::{
    eraftpb::ConfChangeSingle,
    proto::{parse_conf_change, stringify_conf_change},
    Changer, ProgressTracker,
};

enum Command {
    Simple {
        ccs: Vec<ConfChangeSingle>,
    },
    EnterJoint {
        auto_leave: bool,
        ccs: Vec<ConfChangeSingle>,
    },
    LeaveJoint,
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Simple { ccs } => {
                writeln!(f, "simple (ccs={})", stringify_conf_change(ccs))
            }
            Command::EnterJoint { auto_leave, ccs } => {
                writeln!(
                    f,
                    "enter-joint (auto_leave={auto_leave}, ccs={})",
                    stringify_conf_change(ccs)
                )
            }
            Command::LeaveJoint => writeln!(f, "leave-joint"),
        }
    }
}

fn run_goldenfiles_test(case: &str, cmds: Vec<Command>) -> anyhow::Result<()> {
    let mut mint = Mint::new("src/confchange/goldenfiles");
    let mut file = mint.new_goldenfile(case)?;

    let mut tr = ProgressTracker::new(10);
    let mut idx = 0;

    for (i, cmd) in cmds.into_iter().enumerate() {
        if i > 0 {
            writeln!(file)?;
        }
        write!(file, "{cmd}")?;

        let mut changer = Changer::new(&tr);
        let res = match cmd {
            Command::Simple { ccs } => changer.simple(&ccs),
            Command::EnterJoint { auto_leave, ccs } => changer.enter_joint(auto_leave, &ccs),
            Command::LeaveJoint => changer.leave_joint(),
        };
        match res {
            Ok((conf, changes)) => {
                tr.apply_conf(conf, changes, idx);
                idx += 1;
            }
            Err(err) => {
                idx += 1;
                writeln!(file, "{err}")?;
                continue;
            }
        }
        let conf = tr.conf();
        writeln!(file, "{conf}")?;

        let prs = tr.progress();

        // output with peer_id sorted
        for (k, v) in prs.iter().sorted_by(|&(k1, _), &(k2, _)| k1.cmp(k2)) {
            write!(
                file,
                "{}: {} match={} next={}",
                k, v.state, v.matched, v.next_idx
            )?;
            if conf.learners.contains(k) {
                write!(file, " learner")?;
            }
            writeln!(file)?;
        }
    }

    Ok(())
}

fn simple(confchange: &str) -> Command {
    Command::Simple {
        ccs: parse_conf_change(confchange).unwrap(),
    }
}

fn entry_joint(auto_leave: bool, confchange: &str) -> Command {
    Command::EnterJoint {
        auto_leave,
        ccs: parse_conf_change(confchange).unwrap(),
    }
}

fn leave_joint() -> Command {
    Command::LeaveJoint
}

// NodeID zero is ignored.
#[test]
fn test_zero() -> anyhow::Result<()> {
    run_goldenfiles_test("zero.txt", vec![simple("v1 r0 v0 l0")])
}

// Test the autoleave argument to EnterJoint. The flag has no associated semantics in this
// package, it is simply passed through.
#[test]
fn test_joint_autoleave() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "joint_autoleave.txt",
        vec![
            simple("v1"),
            // Autoleave is reflected in the config.
            entry_joint(true, "v2 v3"),
            // Cannot enter-joint twice, even if autoleave changes.
            entry_joint(false, ""),
            leave_joint(),
        ],
    )
}

// Verify that operations upon entering the joint state are idempotent, i.e., removing an absent
// node is fine, etc.
#[test]
fn test_joint_idempotency() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "joint_idempotency.txt",
        vec![
            simple("v1"),
            entry_joint(false, "r1 r2 r9 v2 v3 v4 v2 v3 v4 l2 l2 r4 r4 l1 l1"),
            leave_joint(),
        ],
    )
}

// Verify that when a voter is demoted in a joint config, it will show up in learners_next until
// the joint config is left, and only then will the progress turn into that of a learner,
// without resetting the progress. Note that this last fact is verified by `next`, which
// can tell us which "round" the progress was originally created in.
#[test]
fn test_joint_learners_next() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "joint_learners_next.txt",
        vec![simple("v1"), entry_joint(false, "v2 l1"), leave_joint()],
    )
}

#[test]
fn test_joint_safety() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "joint_safety.txt",
        vec![
            leave_joint(),
            entry_joint(false, ""),
            entry_joint(false, "v1"),
            simple("v1"),
            leave_joint(),
            entry_joint(false, ""),
            entry_joint(false, ""),
            leave_joint(),
            leave_joint(),
            entry_joint(false, "r1 v2 v3 l4"),
            entry_joint(false, ""),
            entry_joint(false, "v12"),
            simple("l15"),
            leave_joint(),
            simple("l9"),
        ],
    )
}

#[test]
fn test_simple_idempotency() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "simple_idempotency.txt",
        vec![
            simple("v1"),
            simple("v1"),
            simple("v2"),
            simple("l1"),
            simple("l1"),
            simple("r1"),
            simple("r1"),
            simple("v3"),
            simple("r3"),
            simple("r3"),
            simple("r4"),
        ],
    )
}

#[test]
fn test_simple_promote_demote() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "simple_promote_demote.txt",
        vec![
            // Set up three voters for this test.
            simple("v1"),
            simple("v2"),
            simple("v3"),
            // Can atomically demote and promote without a hitch.
            simple("l1 v1"),
            // Can demote a voter.
            simple("l2"),
            // Can atomically promote and demote the same voter.
            simple("v2 l2"),
            // Can promote a voter.
            simple("v2"),
        ],
    )
}

#[test]
fn test_simple_safety() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "simple_safety.txt",
        vec![
            simple("l1"),
            simple("v1"),
            simple("v2 l3"),
            simple("r1 v5"),
            simple("r1 r2"),
            simple("v3 v4"),
            simple("l1 v5"),
            simple("l1 l2"),
            simple("l2 l3 l4 l5"),
            simple("r1"),
            simple("r2 r3 r4 r5"),
        ],
    )
}
