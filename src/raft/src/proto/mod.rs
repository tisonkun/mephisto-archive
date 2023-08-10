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

//! Raft proto structure definitions and utilities.

mod confchange;
mod confstate;

pub use confchange::{new_conf_change_single, parse_conf_change, stringify_conf_change};
pub use confstate::conf_state_eq;

pub mod eraftpb {
    // We have no control of generated files.
    #![allow(missing_docs)]
    include!(concat!(env!("OUT_DIR"), "/eraftpb.rs"));

    impl Message {
        #[inline]
        pub fn get_snapshot(&self) -> &Snapshot {
            static DEFAULT_SNAPSHOT: Snapshot = Snapshot {
                data: vec![],
                metadata: None,
            };

            match self.snapshot.as_ref() {
                Some(v) => v,
                None => &DEFAULT_SNAPSHOT,
            }
        }
    }

    impl Snapshot {
        /// For a given snapshot, determine if it's empty or not.
        pub fn is_empty(&self) -> bool {
            match &self.metadata {
                None => true,
                Some(metadata) => metadata.index == 0,
            }
        }

        #[inline]
        pub fn get_metadata(&self) -> &SnapshotMetadata {
            static DEFAULT_METADATA: SnapshotMetadata = SnapshotMetadata {
                conf_state: None,
                index: 0,
                term: 0,
            };

            match self.metadata.as_ref() {
                Some(v) => v,
                None => &DEFAULT_METADATA,
            }
        }
    }

    impl SnapshotMetadata {
        #[inline]
        pub fn get_conf_state(&self) -> &ConfState {
            static DEFAULT_CONF_STATE: ConfState = ConfState {
                voters: vec![],
                learners: vec![],
                voters_outgoing: vec![],
                learners_next: vec![],
                auto_leave: false,
            };
            match self.conf_state.as_ref() {
                Some(v) => v,
                None => &DEFAULT_CONF_STATE,
            }
        }
    }
}

pub mod util {
    //! This module contains utilities to work with Raft proto structures.

    use crate::eraftpb::ConfState;

    impl<Iter1, Iter2> From<(Iter1, Iter2)> for ConfState
    where
        Iter1: IntoIterator<Item = u64>,
        Iter2: IntoIterator<Item = u64>,
    {
        fn from((voters, learners): (Iter1, Iter2)) -> Self {
            let mut conf_state = ConfState::default();
            conf_state.voters.extend(voters);
            conf_state.learners.extend(learners);
            conf_state
        }
    }
}
