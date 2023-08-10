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

use std::io::Write;

use goldenfile::Mint;

use crate::{
    quorum::{AckIndexer, AckedIndexer, Index},
    HashMap, HashSet, JointConfig, MajorityConfig,
};

#[derive(Debug)]
enum Command {
    Committed {
        joint: bool,
        ids: Vec<u64>,
        idsj: Vec<u64>,
        idxs: Vec<Index>,
    },
    GroupCommitted {
        joint: bool,
        ids: Vec<u64>,
        idsj: Vec<u64>,
        idxs: Vec<Index>,
        /// group id of each nodes in the config
        gids: Vec<u64>,
    },
    Vote {
        joint: bool,
        ids: Vec<u64>,
        idsj: Vec<u64>,
        votes: Vec<Index>,
    },
}

impl Command {
    // majority config
    fn ids(&self) -> &[u64] {
        match self {
            Command::Committed { ids, .. } => ids,
            Command::GroupCommitted { ids, .. } => ids,
            Command::Vote { ids, .. } => ids,
        }
    }

    // majority configs for joint consensus
    fn idsj(&self) -> &[u64] {
        match self {
            Command::Committed { idsj, .. } => idsj,
            Command::GroupCommitted { idsj, .. } => idsj,
            Command::Vote { idsj, .. } => idsj,
        }
    }

    // The committed indexes for the nodes in the config in the order in which they appear in
    // (ids, idsj), without repetition. '0' denotes an omission (i.e. no information for this
    // voter).
    //
    // For example,
    // ids=(1,2) idsj=(2,3,4) idxs=(0,5,0,7) initializes the idx for voter 2 to 5 and that for
    // voter 4 to 7 (and no others).
    fn idxs(&self) -> &[Index] {
        match self {
            Command::Committed { idxs, .. } => idxs,
            Command::GroupCommitted { idxs, .. } => idxs,
            // Votes. These are initialized similar to idxs except the only values used are 1
            // (voted against) and 2 (voted for). This looks awkward, but is convenient
            // because it allows sharing code between the two.
            Command::Vote { votes, .. } => votes,
        }
    }
}

fn make_lookup(idxs: &[Index], ids: &[u64], idsj: &[u64]) -> AckIndexer {
    let mut l = AckIndexer::default();
    // next to consume from idxs
    let mut p: usize = 0;
    for id in ids.iter().chain(idsj) {
        if !l.contains_key(id) && p < idxs.len() {
            l.insert(*id, idxs[p]);
            p += 1;
        }
    }

    // Zero entries are created by _ placeholders; we don't want
    // them in the lookuper because "no entry" is different from
    // "zero entry". Note that we prevent tests from specifying
    // zero commit indexes, so that there's no confusion between
    // the two concepts.
    l.retain(|_, index| index.index > 0);
    l
}

fn run_goldenfiles_test(case: &str, cmds: Vec<Command>) -> anyhow::Result<()> {
    let mut mint = Mint::new("src/quorum/goldenfiles");
    let mut file = mint.new_goldenfile(case)?;

    for (i, cmd) in cmds.into_iter().enumerate() {
        if i > 0 {
            writeln!(file)?;
        }
        writeln!(file, "{cmd:?}")?;

        // Build the two majority configs.
        let c = MajorityConfig::new(HashSet::from_iter(cmd.ids().iter().cloned()));
        let cj = MajorityConfig::new(HashSet::from_iter(cmd.idsj().iter().cloned()));

        let voters = JointConfig::new_joint_from_majorities(c.clone(), cj.clone())
            .ids()
            .len();

        let mut idxs = cmd.idxs().to_vec();

        // verify length of voters
        if voters != idxs.len() {
            writeln!(
                file,
                "error: mismatched input (explicit or _) for voters {:?}: {:?}",
                voters, idxs
            )?;
            continue;
        }

        // verify length of group ids
        if let Command::GroupCommitted { ref gids, .. } = cmd {
            if gids.len() != voters {
                writeln!(
                    file,
                    "error: mismatched input (explicit or _) for group ids {:?}: {:?}",
                    voters,
                    gids.len()
                )?;
                continue;
            } else {
                // assign group ids to idxs
                for (idx, gid) in idxs.iter_mut().zip(gids) {
                    idx.group_id = *gid;
                }
            }
        }

        match cmd {
            Command::Committed {
                ids, idsj, joint, ..
            } => {
                let use_group_commit = false;

                let mut l = make_lookup(&idxs, &ids, &idsj);

                let idx;

                // Branch based on whether this is a majority or joint quorum
                // test case.
                if joint {
                    let cc = JointConfig::new_joint_from_majorities(c.clone(), cj.clone());
                    write!(file, "{}", &cc.describe(&l))?;
                    idx = cc.committed_index(use_group_commit, &l);
                    // Interchanging the majorities shouldn't make a difference. If it does, print.
                    let a_idx = JointConfig::new_joint_from_majorities(cj, c)
                        .committed_index(use_group_commit, &l);
                    if a_idx != idx {
                        writeln!(file, "{} <-- via symmetry", a_idx.0)?;
                    }
                } else {
                    idx = c.committed_index(use_group_commit, &l);
                    write!(file, "{}", &c.describe(&l))?;

                    // Joining a majority with the empty majority should give same result.
                    let a_idx = JointConfig::new_joint_from_majorities(
                        c.clone(),
                        MajorityConfig::default(),
                    )
                    .committed_index(use_group_commit, &l);
                    if a_idx != idx {
                        writeln!(file, "{} <-- via zero-joint quorum", a_idx.0)?;
                    }

                    // Joining a majority with itself should give same result.
                    let a_idx = JointConfig::new_joint_from_majorities(c.clone(), c.clone())
                        .committed_index(use_group_commit, &l);
                    if a_idx != idx {
                        writeln!(file, "{} <-- via self-joint quorum", a_idx.0)?;
                    }

                    // test overlaying
                    // If the committed index was definitely above the currently inspected idx,
                    // the result shouldn't change if we lower it further.
                    for &id in c.ids() {
                        if let Some(iidx) = l.acked_index(id) {
                            if idx.0 > iidx.index {
                                // try index - 1
                                l.insert(
                                    id,
                                    Index {
                                        index: iidx.index - 1,
                                        group_id: iidx.group_id,
                                    },
                                );

                                let a_idx = c.committed_index(use_group_commit, &l);
                                if a_idx != idx {
                                    writeln!(
                                        file,
                                        "{} <-- overlaying {}->{}",
                                        a_idx.0,
                                        id,
                                        iidx.index - 1
                                    )?;
                                }
                                // try 0
                                l.insert(
                                    id,
                                    Index {
                                        index: 0,
                                        group_id: iidx.group_id,
                                    },
                                );

                                let a_idx = c.committed_index(use_group_commit, &l);
                                if a_idx != idx {
                                    writeln!(file, "{} <-- overlaying {}->{}", a_idx.0, id, 0)?;
                                }
                                // recovery
                                l.insert(id, iidx);
                            }
                        }
                    }
                }
                writeln!(
                    file,
                    "{}",
                    Index {
                        index: idx.0,
                        group_id: 0
                    }
                )?;
            }
            Command::GroupCommitted {
                ids, idsj, joint, ..
            } => {
                let use_group_commit = true;

                let l = make_lookup(&idxs, &ids, &idsj);

                let idx;

                if joint {
                    let cc = JointConfig::new_joint_from_majorities(c.clone(), cj.clone());
                    // `describe` doesn't seem to be useful for group commit.
                    // buf.push_str(&cc.describe(&l));
                    idx = cc.committed_index(use_group_commit, &l);
                    // Interchanging the majorities shouldn't make a difference. If it does, print.
                    let a_idx = JointConfig::new_joint_from_majorities(cj, c)
                        .committed_index(use_group_commit, &l);
                    if a_idx != idx {
                        writeln!(file, "{} <-- via symmetry", a_idx.0)?;
                    }
                } else {
                    todo!("majority group commit");
                }

                writeln!(
                    file,
                    "{}",
                    Index {
                        index: idx.0,
                        group_id: 0
                    }
                )?;
            }
            Command::Vote {
                ids, idsj, joint, ..
            } => {
                let ll = make_lookup(&idxs, &ids, &idsj);
                let mut l = HashMap::default();
                for (id, v) in ll {
                    l.insert(id, v.index != 1);
                }

                let r;
                if joint {
                    // Run a joint quorum test case.
                    r = JointConfig::new_joint_from_majorities(c.clone(), cj.clone())
                        .vote_result(|id| l.get(&id).cloned());
                    // Interchanging the majorities shouldn't make a difference. If it does, print.
                    let ar = JointConfig::new_joint_from_majorities(cj, c)
                        .vote_result(|id| l.get(&id).cloned());
                    if ar != r {
                        writeln!(file, "{} <-- via symmetry", ar)?;
                    }
                } else {
                    r = c.vote_result(|id| l.get(&id).cloned());
                }
                writeln!(file, "{}", r)?;
            }
        }
    }

    Ok(())
}

fn committed(joint: bool, ids: Vec<u64>, idsj: Vec<u64>, idxs: Vec<u64>) -> Command {
    if !joint {
        assert!(
            idsj.is_empty(),
            "non-joint command has non-empty joint majority"
        );
    }

    Command::Committed {
        joint,
        ids,
        idsj,
        idxs: idxs
            .into_iter()
            .map(|i| Index {
                index: i,
                group_id: 0,
            })
            .collect(),
    }
}

fn group_committed(
    joint: bool,
    ids: Vec<u64>,
    idsj: Vec<u64>,
    idxs: Vec<u64>,
    gids: Vec<u64>,
) -> Command {
    if !joint {
        assert!(
            idsj.is_empty(),
            "non-joint command has non-empty joint majority"
        );
    }

    Command::GroupCommitted {
        joint,
        ids,
        idsj,
        idxs: idxs
            .into_iter()
            .map(|i| Index {
                index: i,
                group_id: 0,
            })
            .collect(),
        gids,
    }
}

fn vote(joint: bool, ids: Vec<u64>, idsj: Vec<u64>, votes: Vec<&str>) -> Command {
    if !joint {
        assert!(
            idsj.is_empty(),
            "non-joint command has non-empty joint majority"
        );
    }

    Command::Vote {
        joint,
        ids,
        idsj,
        votes: votes
            .into_iter()
            .map(|i| Index {
                index: match i {
                    "y" => 2,
                    "n" => 1,
                    "_" => 0,
                    _ => unreachable!("malformed vote {i}"),
                },
                group_id: 0,
            })
            .collect(),
    }
}

#[test]
fn test_joint_group_commit() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "joint_group_commit.txt",
        vec![
            // the same result of joint single group commit
            group_committed(
                true,
                vec![1, 2, 3],
                vec![],
                vec![100, 101, 99],
                vec![1, 1, 1],
            ),
            // min(quorum_commit_index = 100, first index that appears in second group = 99)
            group_committed(
                true,
                vec![1, 2, 3],
                vec![],
                vec![100, 101, 99],
                vec![1, 1, 2],
            ),
            // min(quorum_commit_index = 100, first index that appears in second group = 101)
            group_committed(
                true,
                vec![1, 2, 3],
                vec![],
                vec![100, 101, 99],
                vec![2, 1, 1],
            ),
            // minimum index = 99
            group_committed(
                true,
                vec![1, 2, 3],
                vec![],
                vec![100, 101, 99],
                vec![0, 1, 1],
            ),
            // min(quorum_commit_index = 100, first index that appears in second group = 99)
            group_committed(
                true,
                vec![1, 2, 3],
                vec![],
                vec![100, 101, 99],
                vec![0, 1, 2],
            ),
            // minimum index = 98
            group_committed(
                true,
                vec![1, 2, 3, 4, 5],
                vec![],
                vec![100, 101, 99, 102, 98],
                vec![0, 0, 0, 0, 1],
            ),
            // cfg1 = min(quorum_commit_index = 100, first index that appears in second group = 99)
            // cfg2 = min(quorum_commit_index = 100, first index that appears in second group = 1)
            group_committed(
                true,
                vec![1, 2, 3, 4],
                vec![3, 4, 5, 6],
                vec![101, 99, 100, 102, 103, 1],
                vec![1, 0, 1, 1, 0, 2],
            ),
            // cfg 1 = min(quorum_commit_index = 100, first index that appears in second group =
            // 101) cfg 2 = min(quorum_commit_index = 100, first index that appears in
            // second group = 101)
            group_committed(
                true,
                vec![1, 2, 3],
                vec![4, 5, 6],
                vec![99, 100, 101, 99, 100, 101],
                vec![1, 1, 2, 1, 2, 1],
            ),
            // cfg 1 = min(quorum_commit_index = 100, first index that appears in second group =
            // 101) cfg 2 = minimum index = 99
            group_committed(
                true,
                vec![1, 2, 3],
                vec![4, 5, 6],
                vec![99, 100, 101, 99, 100, 101],
                vec![1, 1, 2, 1, 1, 0],
            ),
            // min(quorum_commit_index = 101, first index that appears in second group = 103)
            group_committed(
                true,
                vec![1, 2, 3, 4, 5],
                vec![],
                vec![99, 100, 101, 102, 103],
                vec![1, 1, 1, 1, 2],
            ),
            // cfg 1 = minimum index = 1
            // cfg 2 = minimum index = 2
            group_committed(
                true,
                vec![1, 2, 3, 4, 5],
                vec![2, 3, 4, 5, 6],
                vec![1, 100, 101, 102, 103, 2],
                vec![1, 0, 1, 1, 1, 1],
            ),
            // cfg 1 = minimum index = 3
            // cfg 2 = quorum_commit_index = 101
            group_committed(
                true,
                vec![1, 2, 3, 4, 5],
                vec![2, 3, 4, 5, 6],
                vec![3, 100, 101, 102, 103, 2],
                vec![0, 1, 1, 1, 1, 1],
            ),
            // cfg 1 = min(quorum_commit_index = 101, first index that appears in second group =
            // 103) cfg 2 = min(quorum_commit_index = 101, first index that appears in
            // second group = 103)
            group_committed(
                true,
                vec![1, 2, 3, 4, 5],
                vec![2, 3, 4, 5, 6],
                vec![3, 100, 101, 102, 103, 2],
                vec![0, 1, 1, 1, 3, 1],
            ),
            // cfg 1 = minimum index = 3
            // cfg 2 = min(quorum_commit_index = 101, first index that appears in second group = 2)
            group_committed(
                true,
                vec![1, 2, 3, 4, 5],
                vec![2, 3, 4, 5, 6],
                vec![3, 100, 101, 102, 103, 2],
                vec![0, 1, 1, 1, 1, 3],
            ),
        ],
    )
}

#[test]
fn test_joint_vote() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "joint_vote.txt",
        vec![
            // Empty joint config wins all votes. This isn't used in production. Note that
            // by specifying cfgj explicitly we tell the test harness to treat the input as
            // a joint quorum and not a majority quorum.
            vote(true, vec![], vec![], vec![]),
            // More examples with close to trivial configs.
            vote(true, vec![1], vec![], vec!["_"]),
            vote(true, vec![1], vec![], vec!["y"]),
            vote(true, vec![1], vec![], vec!["n"]),
            vote(true, vec![1], vec![1], vec!["_"]),
            vote(true, vec![1], vec![1], vec!["y"]),
            vote(true, vec![1], vec![1], vec!["n"]),
            vote(true, vec![1], vec![2], vec!["_", "_"]),
            vote(true, vec![1], vec![2], vec!["y", "_"]),
            vote(true, vec![1], vec![2], vec!["y", "y"]),
            vote(true, vec![1], vec![2], vec!["y", "n"]),
            vote(true, vec![1], vec![2], vec!["n", "_"]),
            vote(true, vec![1], vec![2], vec!["n", "n"]),
            vote(true, vec![1], vec![2], vec!["n", "y"]),
            // Two node configs.
            vote(true, vec![1, 2], vec![3, 4], vec!["_", "_", "_", "_"]),
            vote(true, vec![1, 2], vec![3, 4], vec!["y", "_", "_", "_"]),
            vote(true, vec![1, 2], vec![3, 4], vec!["y", "y", "_", "_"]),
            vote(true, vec![1, 2], vec![3, 4], vec!["y", "y", "n", "_"]),
            vote(true, vec![1, 2], vec![3, 4], vec!["y", "y", "n", "n"]),
            vote(true, vec![1, 2], vec![3, 4], vec!["y", "y", "y", "n"]),
            vote(true, vec![1, 2], vec![3, 4], vec!["y", "y", "y", "y"]),
            vote(true, vec![1, 2], vec![2, 3], vec!["_", "_", "_"]),
            vote(true, vec![1, 2], vec![2, 3], vec!["_", "n", "_"]),
            vote(true, vec![1, 2], vec![2, 3], vec!["y", "y", "_"]),
            vote(true, vec![1, 2], vec![2, 3], vec!["y", "y", "n"]),
            vote(true, vec![1, 2], vec![2, 3], vec!["y", "y", "y"]),
            vote(true, vec![1, 2], vec![1, 2], vec!["_", "_"]),
            vote(true, vec![1, 2], vec![1, 2], vec!["y", "_"]),
            vote(true, vec![1, 2], vec![1, 2], vec!["y", "n"]),
            vote(true, vec![1, 2], vec![1, 2], vec!["n", "_"]),
            vote(true, vec![1, 2], vec![1, 2], vec!["n", "n"]),
            // Simple example for overlapping three node configs.
            vote(true, vec![1, 2, 3], vec![2, 3, 4], vec!["_", "_", "_", "_"]),
            vote(true, vec![1, 2, 3], vec![2, 3, 4], vec!["_", "n", "_", "_"]),
            vote(true, vec![1, 2, 3], vec![2, 3, 4], vec!["_", "n", "n", "_"]),
            vote(true, vec![1, 2, 3], vec![2, 3, 4], vec!["_", "y", "y", "_"]),
            vote(true, vec![1, 2, 3], vec![2, 3, 4], vec!["y", "y", "_", "_"]),
            vote(true, vec![1, 2, 3], vec![2, 3, 4], vec!["y", "y", "n", "_"]),
            vote(true, vec![1, 2, 3], vec![2, 3, 4], vec!["y", "y", "n", "n"]),
            vote(true, vec![1, 2, 3], vec![2, 3, 4], vec!["y", "y", "n", "y"]),
        ],
    )
}

#[test]
fn test_majority_vote() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "majority_vote.txt",
        vec![
            // The empty config always announces a won vote.
            vote(false, vec![], vec![], vec![]),
            vote(false, vec![1], vec![], vec!["_"]),
            vote(false, vec![1], vec![], vec!["n"]),
            vote(false, vec![123], vec![], vec!["y"]),
            vote(false, vec![4, 8], vec![], vec!["_", "_"]),
            // With two voters, a single rejection loses the vote.
            vote(false, vec![4, 8], vec![], vec!["n", "_"]),
            vote(false, vec![4, 8], vec![], vec!["y", "_"]),
            vote(false, vec![4, 8], vec![], vec!["n", "y"]),
            vote(false, vec![4, 8], vec![], vec!["y", "y"]),
            vote(false, vec![2, 4, 7], vec![], vec!["_", "_", "_"]),
            vote(false, vec![2, 4, 7], vec![], vec!["n", "_", "_"]),
            vote(false, vec![2, 4, 7], vec![], vec!["y", "_", "_"]),
            vote(false, vec![2, 4, 7], vec![], vec!["n", "n", "_"]),
            vote(false, vec![2, 4, 7], vec![], vec!["y", "n", "_"]),
            vote(false, vec![2, 4, 7], vec![], vec!["y", "y", "_"]),
            vote(false, vec![2, 4, 7], vec![], vec!["y", "y", "n"]),
            vote(false, vec![2, 4, 7], vec![], vec!["n", "y", "n"]),
            // Test some random example with seven nodes (why not).
            vote(
                false,
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![],
                vec!["y", "y", "n", "y", "_", "_", "_"],
            ),
            vote(
                false,
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![],
                vec!["_", "y", "y", "_", "n", "y", "n"],
            ),
            vote(
                false,
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![],
                vec!["y", "y", "n", "y", "_", "n", "y"],
            ),
            vote(
                false,
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![],
                vec!["y", "y", "_", "n", "y", "n", "n"],
            ),
            vote(
                false,
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![],
                vec!["y", "y", "n", "y", "n", "n", "n"],
            ),
        ],
    )
}

#[test]
fn test_joint_commit() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "joint_commit.txt",
        vec![
            // No difference between a simple majority quorum and a simple majority quorum
            // joint with an empty majority quorum.
            committed(true, vec![1, 2, 3], vec![], vec![100, 101, 99]),
            // Joint nonoverlapping singleton quorums.
            committed(true, vec![1], vec![2], vec![0, 0]),
            // Voter 1 has 100 committed, 2 nothing. This means we definitely won't commit
            // past 100.
            committed(true, vec![1], vec![2], vec![100, 0]),
            // Committed index collapses once both majorities do, to the lower index.
            committed(true, vec![1], vec![2], vec![13, 100]),
            // Joint overlapping (i.e. identical) singleton quorum.
            committed(true, vec![1], vec![1], vec![0]),
            committed(true, vec![1], vec![1], vec![100]),
            // Two-node config joint with non-overlapping single node config
            committed(true, vec![1, 3], vec![2], vec![0, 0, 0]),
            committed(true, vec![1, 3], vec![2], vec![100, 0, 0]),
            // 1 has 100 committed, 2 has 50 (collapsing half of the joint quorum to 50).
            committed(true, vec![1, 3], vec![2], vec![100, 0, 50]),
            // 2 reports 45, collapsing the other half (to 45).
            committed(true, vec![1, 3], vec![2], vec![100, 45, 50]),
            // Two-node config with overlapping single-node config.
            committed(true, vec![1, 2], vec![2], vec![0, 0]),
            // 1 reports 100.
            committed(true, vec![1, 2], vec![2], vec![100, 0]),
            // 2 reports 100.
            committed(true, vec![1, 2], vec![2], vec![0, 100]),
            committed(true, vec![1, 2], vec![2], vec![50, 100]),
            committed(true, vec![1, 2], vec![2], vec![100, 50]),
            // Joint non-overlapping two-node configs.
            committed(true, vec![1, 2], vec![3, 4], vec![50, 0, 0, 0]),
            committed(true, vec![1, 2], vec![3, 4], vec![50, 0, 49, 0]),
            committed(true, vec![1, 2], vec![3, 4], vec![50, 48, 49, 0]),
            committed(true, vec![1, 2], vec![3, 4], vec![50, 48, 49, 47]),
            // Joint overlapping two-node configs.
            committed(true, vec![1, 2], vec![2, 3], vec![0, 0, 0]),
            committed(true, vec![1, 2], vec![2, 3], vec![100, 0, 0]),
            committed(true, vec![1, 2], vec![2, 3], vec![0, 100, 0]),
            committed(true, vec![1, 2], vec![2, 3], vec![0, 100, 99]),
            committed(true, vec![1, 2], vec![2, 3], vec![101, 100, 99]),
            // Joint identical two-node configs.
            committed(true, vec![1, 2], vec![1, 2], vec![0, 0]),
            committed(true, vec![1, 2], vec![1, 2], vec![0, 40]),
            committed(true, vec![1, 2], vec![1, 2], vec![41, 40]),
            // Joint disjoint three-node configs.
            committed(true, vec![1, 2, 3], vec![4, 5, 6], vec![0, 0, 0, 0, 0, 0]),
            committed(true, vec![1, 2, 3], vec![4, 5, 6], vec![100, 0, 0, 0, 0, 0]),
            committed(
                true,
                vec![1, 2, 3],
                vec![4, 5, 6],
                vec![100, 0, 0, 90, 0, 0],
            ),
            committed(
                true,
                vec![1, 2, 3],
                vec![4, 5, 6],
                vec![100, 99, 0, 0, 0, 0],
            ),
            // First quorum <= 99, second one <= 97. Both quorums guarantee that 90 is
            // committed.
            committed(
                true,
                vec![1, 2, 3],
                vec![4, 5, 6],
                vec![0, 99, 90, 97, 95, 0],
            ),
            // First quorum collapsed to 92. Second one already had at least 95 committed,
            // so the result also collapses.
            committed(
                true,
                vec![1, 2, 3],
                vec![4, 5, 6],
                vec![92, 99, 90, 97, 95, 0],
            ),
            // Second quorum collapses, but nothing changes in the output.
            committed(
                true,
                vec![1, 2, 3],
                vec![4, 5, 6],
                vec![92, 99, 90, 97, 95, 77],
            ),
            // Joint overlapping three-node configs.
            committed(true, vec![1, 2, 3], vec![1, 4, 5], vec![0, 0, 0, 0, 0]),
            committed(true, vec![1, 2, 3], vec![1, 4, 5], vec![100, 0, 0, 0, 0]),
            committed(true, vec![1, 2, 3], vec![1, 4, 5], vec![100, 101, 0, 0, 0]),
            committed(
                true,
                vec![1, 2, 3],
                vec![1, 4, 5],
                vec![100, 101, 100, 0, 0],
            ),
            // Second quorum could commit either 98 or 99, but first quorum is open.
            committed(true, vec![1, 2, 3], vec![1, 4, 5], vec![0, 100, 0, 99, 98]),
            // Additionally, first quorum can commit either 100 or 99
            committed(true, vec![1, 2, 3], vec![1, 4, 5], vec![0, 100, 99, 99, 98]),
            committed(true, vec![1, 2, 3], vec![1, 4, 5], vec![1, 100, 99, 99, 98]),
            committed(
                true,
                vec![1, 2, 3],
                vec![1, 4, 5],
                vec![100, 100, 99, 99, 98],
            ),
            // More overlap.
            committed(true, vec![1, 2, 3], vec![2, 3, 4], vec![0, 0, 0, 0]),
            committed(true, vec![1, 2, 3], vec![2, 3, 4], vec![0, 100, 99, 0]),
            committed(true, vec![1, 2, 3], vec![2, 3, 4], vec![98, 100, 99, 0]),
            committed(true, vec![1, 2, 3], vec![2, 3, 4], vec![100, 100, 99, 0]),
            committed(true, vec![1, 2, 3], vec![2, 3, 4], vec![100, 100, 99, 98]),
            committed(true, vec![1, 2, 3], vec![2, 3, 4], vec![100, 0, 0, 101]),
            committed(true, vec![1, 2, 3], vec![2, 3, 4], vec![100, 99, 0, 101]),
            // Identical. This is also exercised in the test harness, so it's listed here
            // only briefly.
            committed(true, vec![1, 2, 3], vec![1, 2, 3], vec![50, 45, 0]),
        ],
    )
}

#[test]
fn test_majority_commit() -> anyhow::Result<()> {
    run_goldenfiles_test(
        "majority_commit.txt",
        vec![
            // The empty quorum commits "everything". This is useful for its use in joint
            // quorums.
            committed(false, vec![], vec![], vec![]),
            // A single voter quorum is not final when no index is known.
            committed(false, vec![1], vec![], vec![0]),
            // When an index is known, that's the committed index, and that's final.
            committed(false, vec![1], vec![], vec![12]),
            // With two nodes, start out similarly.
            committed(false, vec![1, 2], vec![], vec![0, 0]),
            // The first committed index becomes known (for n1). Nothing changes in the
            // output because idx=12 is not known to be on a quorum (which is both nodes).
            committed(false, vec![1, 2], vec![], vec![12, 0]),
            // The second index comes in and finalize the decision. The result will be the
            // smaller of the two indexes.
            committed(false, vec![1, 2], vec![], vec![12, 5]),
            // No surprises for three nodes.
            committed(false, vec![1, 2, 3], vec![], vec![0, 0, 0]),
            committed(false, vec![1, 2, 3], vec![], vec![12, 0, 0]),
            // We see a committed index, but a higher committed index for the last pending
            // votes could change (increment) the outcome, so not final yet.
            committed(false, vec![1, 2, 3], vec![], vec![12, 5, 0]),
            // a) the case in which it does:
            committed(false, vec![1, 2, 3], vec![], vec![12, 5, 6]),
            // b) the case in which it does not:
            committed(false, vec![1, 2, 3], vec![], vec![12, 5, 4]),
            // c) a different case in which the last index is pending but it has no chance of
            // swaying the outcome (because nobody in the current quorum agrees on anything
            // higher than the candidate):
            committed(false, vec![1, 2, 3], vec![], vec![5, 5, 0]),
            // c) continued: Doesn't matter what shows up last. The result is final.
            committed(false, vec![1, 2, 3], vec![], vec![5, 5, 12]),
            // With all committed idx known, the result is final.
            committed(false, vec![1, 2, 3], vec![], vec![100, 101, 103]),
            // Some more complicated examples. Similar to case c) above. The result is
            // already final because no index higher than 103 is one short of quorum.
            committed(
                false,
                vec![1, 2, 3, 4, 5],
                vec![],
                vec![101, 104, 103, 103, 0],
            ),
            // A similar case which is not final because another vote for >= 103 would change
            // the outcome.
            committed(
                false,
                vec![1, 2, 3, 4, 5],
                vec![],
                vec![101, 102, 103, 103, 0],
            ),
        ],
    )
}
