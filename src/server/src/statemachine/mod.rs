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

#[derive(Debug, Copy, Clone)]
pub struct RevisionNotFound;

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

#[derive(Debug, Default, Eq, PartialEq)]
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

pub struct KeyIndex {
    key: Vec<u8>,
    modified: Revision,
    generations: Vec<Generation>,
}

impl KeyIndex {
    pub fn new(key: Vec<u8>) -> KeyIndex {
        KeyIndex {
            key,
            modified: Revision::default(),
            generations: vec![],
        }
    }

    pub fn put(&mut self, revision: Revision) {
        if revision <= self.modified {
            panic!(
                "'put' with an unexpected smaller revision (given: {revision:?}, modified: {:?})",
                self.modified,
            );
        }

        if self.generations.is_empty() {
            self.generations.push(Generation::default());
        }

        let g = {
            let idx = self.generations.len() - 1;
            &mut self.generations[idx]
        };
        if g.revisions.is_empty() {
            g.created = revision;
        }
        g.revisions.push(revision);
        g.version += 1;
        self.modified = revision;
    }

    /// `tombstone` puts a revision, pointing to a tombstone, to the [keyIndex].
    /// It also creates a new empty generation in the keyIndex.
    /// It returns `Err(RevisionNotFound)` when tombstone on an empty generation.
    pub fn tombstone(&mut self, revision: Revision) -> Result<(), RevisionNotFound> {
        if self.is_empty() {
            panic!(
                "'tombstone' got an unexpected empty keyIndex (key: {})",
                String::from_utf8_lossy(&self.key),
            );
        }

        if self.generations[self.generations.len() - 1].is_empty() {
            return Err(RevisionNotFound);
        }

        self.put(revision);
        self.generations.push(Generation::default());
        Ok(())
    }

    pub fn get(
        &self,
        at_rev: u64,
    ) -> Result<
        (
            Revision, // modified
            Revision, // created
            u64,      // version
        ),
        RevisionNotFound,
    > {
        if self.is_empty() {
            panic!(
                "'get' got an unexpected empty keyIndex (key: {})",
                String::from_utf8_lossy(&self.key),
            );
        }

        let g = match self.find_generation(at_rev) {
            None => return Err(RevisionNotFound),
            Some(g) => g,
        };

        match g.walk(|rev| rev.main > at_rev) {
            None => Err(RevisionNotFound),
            Some(n) => Ok((
                g.revisions[n],
                g.created,
                g.version - ((g.revisions.len() - n - 1) as u64),
            )),
        }
    }

    fn find_generation(&self, rev: u64) -> Option<&Generation> {
        let lastg = (self.generations.len() - 1) as i64;
        let mut cg = lastg;
        while cg >= 0 {
            if self.generations[cg as usize].revisions.is_empty() {
                cg -= 1;
                continue;
            }
            let g = &self.generations[cg as usize];
            if cg != lastg {
                let tomb = g.get_last_revision().main;
                if tomb <= rev {
                    return None;
                }
            }
            if g.get_first_version().main <= rev {
                return Some(g);
            }
            cg -= 1;
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.generations
            .first()
            .map(|g| g.is_empty())
            .unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::statemachine::{Generation, KeyIndex, Revision};

    #[test]
    fn test_revisions() {
        let revisions = [Revision::default(),
            Revision::new(1, 0),
            Revision::new(1, 1),
            Revision::new(2, 0),
            Revision::new(u64::MAX, u64::MAX)];

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

    #[test]
    fn test_find_generation() {
        let key_index = new_test_key_index();

        let g0 = &key_index.generations[0];
        let g1 = &key_index.generations[1];

        assert_eq!(key_index.find_generation(0), None);
        assert_eq!(key_index.find_generation(1), None);
        assert_eq!(key_index.find_generation(2), Some(g0));
        assert_eq!(key_index.find_generation(3), Some(g0));
        assert_eq!(key_index.find_generation(4), Some(g0));
        assert_eq!(key_index.find_generation(5), Some(g0));
        assert_eq!(key_index.find_generation(6), None);
        assert_eq!(key_index.find_generation(7), None);
        assert_eq!(key_index.find_generation(8), Some(g1));
        assert_eq!(key_index.find_generation(9), Some(g1));
        assert_eq!(key_index.find_generation(10), Some(g1));
        assert_eq!(key_index.find_generation(11), Some(g1));
        assert_eq!(key_index.find_generation(12), None);
        assert_eq!(key_index.find_generation(13), None);
    }

    fn new_test_key_index() -> KeyIndex {
        // key: "foo"
        // modified: 16
        // generations:
        //    {empty}
        //    {{14, 0}[1], {14, 1}[2], {16, 0}(t)[3]}
        //    {{8, 0}[1], {10, 0}[2], {12, 0}(t)[3]}
        //    {{2, 0}[1], {4, 0}[2], {6, 0}(t)[3]}

        let mut ki = KeyIndex::new("foo".as_bytes().to_vec());
        ki.put(Revision::new(2, 0));
        ki.put(Revision::new(4, 0));
        ki.tombstone(Revision::new(6, 0)).unwrap();
        ki.put(Revision::new(8, 0));
        ki.put(Revision::new(10, 0));
        ki.tombstone(Revision::new(12, 0)).unwrap();
        ki.put(Revision::new(14, 0));
        ki.put(Revision::new(14, 1));
        ki.tombstone(Revision::new(16, 0)).unwrap();
        ki
    }
}
