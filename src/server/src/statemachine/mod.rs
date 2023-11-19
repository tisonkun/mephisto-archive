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

use bytes::BufMut;

#[derive(Default, Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub struct Revision {
    main: u64,
    sub: u64,
}

impl Revision {
    pub fn new(main: u64, sub: u64) -> Revision {
        Revision { main, sub }
    }

    pub fn to_bytes(&self, tombstone: bool) -> Vec<u8> {
        let mut bs = vec![0; 18]; // long(8) + _(1) + long(8) + (optional) t(1)
        bs.put_u64(self.main);
        bs.put_u8(b'_');
        bs.put_u64(self.sub);
        if tombstone {
            bs.put_u8(b't');
        }
        bs
    }
}

#[derive(Debug)]
pub struct Generation {
    version: u64,
    created: Revision,
    revisions: Vec<Revision>,
}

impl Generation {
    pub fn new(version: u64, created: Revision, revisions: Vec<Revision>) -> Generation {
        Generation {
            version,
            created,
            revisions,
        }
    }

    pub fn get_revision(&self, n: usize) -> Revision {
        self.revisions[n]
    }

    pub fn get_first_version(&self) -> Revision {
        self.revisions[0]
    }

    pub fn get_last_revision(&self) -> Revision {
        self.revisions[self.revisions.len() - 1]
    }

    pub fn add_revision(&mut self, revision: Revision) {
        self.revisions.push(revision);
        self.version += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.revisions.is_empty()
    }

    pub fn walk(&self, predicate: impl Fn(Revision) -> bool) -> Option<usize> {
        let len = self.revisions.len();
        for i in 0..len {
            let idx = len - i - 1;
            if !predicate(self.revisions[idx]) {
                return Some(idx);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::statemachine::{Generation, Revision};

    #[test]
    fn test_revisions() {
        let revisions = vec![
            Revision::default(),
            Revision::new(1, 0),
            Revision::new(1, 1),
            Revision::new(2, 0),
            Revision::new(u64::MAX, u64::MAX),
        ];

        for i in 0..revisions.len() - 1 {
            assert!(revisions[i] < revisions[i + 1])
        }
    }

    #[test]
    fn test_generation_walk() {
        let revisions = vec![
            Revision::new(2, 0),
            Revision::new(4, 0),
            Revision::new(6, 0),
        ];
        let generation = Generation::new(3, revisions[0], revisions);

        struct TestCase {
            predicate: fn(Revision) -> bool,
            result: Option<usize>,
        }

        let cases = vec![
            TestCase {
                predicate: |r| r.main >= 7,
                result: Some(2),
            },
            TestCase {
                predicate: |r| r.main >= 6,
                result: Some(1),
            },
            TestCase {
                predicate: |r| r.main >= 5,
                result: Some(1),
            },
            TestCase {
                predicate: |r| r.main >= 4,
                result: Some(0),
            },
            TestCase {
                predicate: |r| r.main >= 3,
                result: Some(0),
            },
            TestCase {
                predicate: |r| r.main >= 2,
                result: None,
            },
        ];

        for case in cases {
            assert_eq!(generation.walk(case.predicate), case.result);
        }
    }
}
