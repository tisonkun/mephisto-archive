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

use std::fmt::Write;

use crate::{
    eraftpb::{ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType},
    Error, Result,
};

/// Creates a `ConfChangeSingle`.
pub fn new_conf_change_single(node_id: u64, ty: ConfChangeType) -> ConfChangeSingle {
    ConfChangeSingle {
        node_id,
        change_type: ty as i32,
    }
}

/// Parses a Space-delimited sequence of operations into a slice of ConfChangeSingle.
/// The supported operations are:
/// - vn: make n a voter,
/// - ln: make n a learner,
/// - rn: remove n
pub fn parse_conf_change(s: &str) -> Result<Vec<ConfChangeSingle>> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(vec![]);
    }
    let mut ccs = vec![];
    let splits = s.split_ascii_whitespace();
    for tok in splits {
        if tok.len() < 2 {
            return Err(Error::ConfChangeError(format!("unknown token {}", tok)));
        }
        let mut cc = ConfChangeSingle::default();
        let mut chars = tok.chars();
        cc.set_change_type(match chars.next().unwrap() {
            'v' => ConfChangeType::AddNode,
            'l' => ConfChangeType::AddLearnerNode,
            'r' => ConfChangeType::RemoveNode,
            _ => return Err(Error::ConfChangeError(format!("unknown token {}", tok))),
        });
        cc.node_id = match chars.as_str().parse() {
            Ok(id) => id,
            Err(e) => {
                return Err(Error::ConfChangeError(format!(
                    "parse token {} fail: {}",
                    tok, e
                )))
            }
        };
        ccs.push(cc);
    }
    Ok(ccs)
}

/// The inverse to `parse_conf_change`.
pub fn stringify_conf_change(ccs: &[ConfChangeSingle]) -> String {
    let mut s = String::new();
    for (i, cc) in ccs.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        match cc.change_type() {
            ConfChangeType::AddNode => s.push('v'),
            ConfChangeType::AddLearnerNode => s.push('l'),
            ConfChangeType::RemoveNode => s.push('r'),
        }
        write!(&mut s, "{}", cc.node_id).unwrap();
    }
    s
}

impl ConfChange {
    /// Checks if uses Joint Consensus.
    ///
    /// It will return Some if and only if this config change will use Joint Consensus,
    /// which is the case if it contains more than one change or if the use of Joint
    /// Consensus was requested explicitly. The bool indicates whether the Joint State
    /// will be left automatically.
    pub fn enter_joint(&self) -> Option<bool> {
        // NB: in theory, more config changes could qualify for the "simple"
        // protocol but it depends on the config on top of which the changes apply.
        // For example, adding two learners is not OK if both nodes are part of the
        // base config (i.e. two voters are turned into learners in the process of
        // applying the conf change). In practice, these distinctions should not
        // matter, so we keep it simple and use Joint Consensus liberally.
        if self.transition() != ConfChangeTransition::Auto || self.changes.len() > 1 {
            match self.transition() {
                ConfChangeTransition::Auto | ConfChangeTransition::Implicit => Some(true),
                ConfChangeTransition::Explicit => Some(false),
            }
        } else {
            None
        }
    }

    /// Checks if the configuration change leaves a joint configuration.
    ///
    /// This is the case if the ConfChange is zero, with the possible exception of the Context
    /// field.
    pub fn leave_joint(&self) -> bool {
        self.transition() == ConfChangeTransition::Auto && self.changes.is_empty()
    }
}
