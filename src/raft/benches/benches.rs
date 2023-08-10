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

#![allow(dead_code)] // Due to criterion we need this to avoid warnings.
#![cfg_attr(feature = "cargo-clippy", allow(clippy::let_and_return))] // Benches often artificially return values. Allow it.

use std::time::Duration;

use criterion::Criterion;

mod suites;

pub const DEFAULT_RAFT_SETS: [(usize, usize); 4] = [(0, 0), (3, 1), (5, 2), (7, 3)];

fn main() {
    let mut c = Criterion::default()
        // Configure defaults before overriding with args.
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1))
        .configure_from_args();

    suites::bench_raft(&mut c);
    suites::bench_raw_node(&mut c);
    suites::bench_progress(&mut c);

    c.final_summary();
}
