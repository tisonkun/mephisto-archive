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

use std::collections::{btree_map::Entry, BTreeMap};

use crate::{BallotNumber, Command};

#[derive(Debug, Clone)]
pub struct PValue {
    pub ballot_number: BallotNumber,
    pub slot_number: u64,
    pub command: Command,
}

#[derive(Default, Debug, Clone)]
pub struct PValueSet {
    /// PValues map that is indexed by slot_number: pvalue.
    values: BTreeMap<u64, PValue>,
}

impl PValueSet {
    /// Adds given PValue to the PValueSet; overwriting matching (command_number, proposal)
    /// if it exists and has a smaller ballot_number
    pub fn add(&mut self, value: PValue) {
        match self.values.entry(value.slot_number) {
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
            Entry::Occupied(mut entry) => {
                let prev = entry.get();
                if prev.ballot_number < value.ballot_number {
                    entry.insert(value);
                }
            }
        }
    }

    /// Removes given pvalue.
    pub fn remove(&mut self, value: &PValue) {
        self.values.remove(&value.slot_number);
    }

    /// Updates the pvalues of given pvalueset with the pvalues of the pvalueset overwriting the
    /// slot_numbers with lower ballot_number.
    pub fn update(&mut self, pvalueset: PValueSet) {
        for value in pvalueset.values.into_values() {
            self.add(value);
        }
    }

    pub fn values(&self) -> &BTreeMap<u64, PValue> {
        &self.values
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the number of PValues in the PValueSet.
    pub fn len(&self) -> usize {
        self.values.len()
    }
}
