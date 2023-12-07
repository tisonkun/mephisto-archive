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

use std::collections::{btree_map::Entry, BTreeMap, Bound};

use bytes::BufMut;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RevisionNotFound;

#[derive(Default, Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub struct Revision {
    main: u64,
    sub: u64,
}

impl Revision {
    pub fn new(main: u64) -> Revision {
        Revision { main, sub: 0 }
    }

    pub fn sub(main: u64, sub: u64) -> Revision {
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexGet {
    created: Revision,
    modified: Revision,
    version: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexRange {
    revisions: Vec<Revision>,
    keys: Vec<Vec<u8>>,
    total: u64,
}

#[derive(Debug)]
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

    pub fn get(&self, at_rev: u64) -> Result<IndexGet, RevisionNotFound> {
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
            Some(n) => Ok(IndexGet {
                modified: g.revisions[n],
                created: g.created,
                version: g.version - ((g.revisions.len() - n - 1) as u64),
            }),
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

#[derive(Debug, Default)]
pub struct TreeIndex {
    tree: BTreeMap<Vec<u8>, KeyIndex>,
}

impl TreeIndex {
    pub fn put(&mut self, key: Vec<u8>, revision: Revision) {
        match self.tree.entry(key) {
            Entry::Vacant(ent) => {
                let ki = KeyIndex::new(ent.key().clone());
                ent.insert(ki).put(revision)
            }
            Entry::Occupied(mut ent) => ent.get_mut().put(revision),
        }
    }

    pub fn tombstone(&mut self, key: Vec<u8>, revision: Revision) -> Result<(), RevisionNotFound> {
        let ki = self.tree.get_mut(&key).ok_or(RevisionNotFound)?;
        ki.tombstone(revision)
    }

    pub fn get(&self, key: Vec<u8>, rev: u64) -> Result<IndexGet, RevisionNotFound> {
        self.unsafe_get(key, rev)
    }

    pub fn range(&self, key: Vec<u8>, end: Option<Vec<u8>>, rev: u64) -> IndexRange {
        self.range_limited(key, end, rev, 0)
    }

    pub fn range_limited(
        &self,
        key: Vec<u8>,
        end: Option<Vec<u8>>,
        rev: u64,
        limit: usize,
    ) -> IndexRange {
        let mut revisions = vec![];
        let mut keys = vec![];
        let mut total = 0;

        match end {
            None => {
                if let Ok(res) = self.unsafe_get(key.clone(), rev) {
                    revisions.push(res.modified);
                    keys.push(key);
                    total += 1;
                }
                // else not found - return empty result
            }
            Some(end) => {
                self.unsafe_visit(key, end, |ki| {
                    if let Ok(res) = ki.get(rev) {
                        if limit == 0 || revisions.len() < limit {
                            revisions.push(res.modified);
                            keys.push(ki.key.clone());
                        }
                        total += 1;
                    }
                    // else not found - skip
                    true
                });
            }
        }

        IndexRange {
            revisions,
            keys,
            total,
        }
    }

    pub fn unsafe_get(&self, key: Vec<u8>, rev: u64) -> Result<IndexGet, RevisionNotFound> {
        let ki = self.tree.get(&key).ok_or(RevisionNotFound)?;
        ki.get(rev)
    }

    fn unsafe_visit(&self, key: Vec<u8>, end: Vec<u8>, mut f: impl FnMut(&KeyIndex) -> bool) {
        let mut cursor = self.tree.lower_bound(Bound::Included(&key));
        loop {
            let (k, v) = match cursor.key_value() {
                None => break,
                Some((k, v)) => (k, v),
            };

            if !is_infinite(&end) && k >= &end {
                break;
            }

            if !f(v) {
                break;
            }

            cursor.move_next();
        }
    }
}

pub fn is_infinite(key: &Vec<u8>) -> bool {
    // encode {0} as infinite
    key.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_revisions() {
        let revisions = [
            Revision::default(),
            Revision::new(1),
            Revision::sub(1, 1),
            Revision::new(2),
            Revision::sub(u64::MAX, u64::MAX),
        ];

        for i in 0..revisions.len() - 1 {
            assert!(revisions[i] < revisions[i + 1])
        }
    }

    #[test]
    fn test_generation_walk() {
        let revisions = vec![Revision::new(2), Revision::new(4), Revision::new(6)];
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
        ki.put(Revision::new(2));
        ki.put(Revision::new(4));
        ki.tombstone(Revision::new(6)).unwrap();
        ki.put(Revision::new(8));
        ki.put(Revision::new(10));
        ki.tombstone(Revision::new(12)).unwrap();
        ki.put(Revision::new(14));
        ki.put(Revision::sub(14, 1));
        ki.tombstone(Revision::new(16)).unwrap();
        ki
    }

    #[test]
    fn test_tree_index_get() {
        let mut ti = TreeIndex::default();
        let key = "foo".as_bytes().to_vec();
        let created = Revision::new(2);
        let modified = Revision::new(4);
        let deleted = Revision::new(6);
        ti.put(key.clone(), created);
        ti.put(key.clone(), modified);
        ti.tombstone(key.clone(), deleted).unwrap();

        assert_eq!(Err(RevisionNotFound), ti.get(key.clone(), 0));
        assert_eq!(Err(RevisionNotFound), ti.get(key.clone(), 1));
        assert_eq!(
            Ok(IndexGet {
                created,
                modified: created,
                version: 1,
            }),
            ti.get(key.clone(), 2)
        );
        assert_eq!(
            Ok(IndexGet {
                created,
                modified: created,
                version: 1,
            }),
            ti.get(key.clone(), 3)
        );
        assert_eq!(
            Ok(IndexGet {
                created,
                modified,
                version: 2,
            }),
            ti.get(key.clone(), 4)
        );
        assert_eq!(
            Ok(IndexGet {
                created,
                modified,
                version: 2,
            }),
            ti.get(key.clone(), 5)
        );
        assert_eq!(Err(RevisionNotFound), ti.get(key.clone(), 6));
    }

    #[test]
    fn test_tree_index_tombstone() {
        let mut ti = TreeIndex::default();
        let key = "foo".as_bytes().to_vec();
        ti.put(key.clone(), Revision::new(1));
        ti.tombstone(key.clone(), Revision::new(2)).unwrap();
        assert_eq!(Err(RevisionNotFound), ti.get(key.clone(), 2));
        assert_eq!(
            Err(RevisionNotFound),
            ti.tombstone(key.clone(), Revision::new(3))
        );
    }

    #[test]
    fn test_tree_index_revisions() {
        let mut ti = TreeIndex::default();
        ti.put("foo".as_bytes().to_vec(), Revision::new(1));
        ti.put("foo1".as_bytes().to_vec(), Revision::new(2));
        ti.put("foo2".as_bytes().to_vec(), Revision::new(3));
        ti.put("foo2".as_bytes().to_vec(), Revision::new(4));
        ti.put("foo1".as_bytes().to_vec(), Revision::new(5));
        ti.put("foo".as_bytes().to_vec(), Revision::new(6));

        struct TestCase {
            key: Vec<u8>,
            end: Option<Vec<u8>>,
            rev: u64,
            limit: usize,
            revisions: Vec<Revision>,
            total: u64,
        }

        impl TestCase {
            fn new(
                key: &str,
                end: Option<&str>,
                rev: u64,
                limit: usize,
                total: u64,
                revisions: Vec<u64>,
            ) -> TestCase {
                TestCase {
                    key: key.as_bytes().to_vec(),
                    end: end.map(|v| v.as_bytes().to_vec()),
                    rev,
                    limit,
                    revisions: revisions.into_iter().map(Revision::new).collect(),
                    total,
                }
            }
        }

        let cases = vec![
            // single key that not found
            TestCase::new("bar", None, 6, 0, 0, vec![]),
            // single key that found
            TestCase::new("foo", None, 6, 0, 1, vec![6]),
            // various range keys, fixed atRev, unlimited
            TestCase::new("foo", Some("foo1"), 6, 0, 1, vec![6]),
            TestCase::new("foo", Some("foo2"), 6, 0, 2, vec![6, 5]),
            TestCase::new("foo", Some("fop"), 6, 0, 3, vec![6, 5, 4]),
            TestCase::new("foo1", Some("fop"), 6, 0, 2, vec![5, 4]),
            TestCase::new("foo2", Some("fop"), 6, 0, 1, vec![4]),
            TestCase::new("foo3", Some("fop"), 6, 0, 0, vec![]),
            // fixed range keys, various atRev, unlimited
            TestCase::new("foo1", Some("fop"), 1, 0, 0, vec![]),
            TestCase::new("foo1", Some("fop"), 2, 1, 1, vec![2]),
            TestCase::new("foo1", Some("fop"), 3, 2, 2, vec![2, 3]),
            TestCase::new("foo1", Some("fop"), 4, 2, 2, vec![2, 4]),
            TestCase::new("foo1", Some("fop"), 5, 2, 2, vec![5, 4]),
            TestCase::new("foo1", Some("fop"), 6, 2, 2, vec![5, 4]),
            // fixed range keys, fixed atRev, various limit
            TestCase::new("foo", Some("fop"), 6, 1, 3, vec![6]),
            TestCase::new("foo", Some("fop"), 6, 2, 3, vec![6, 5]),
            TestCase::new("foo", Some("fop"), 6, 3, 3, vec![6, 5, 4]),
            TestCase::new("foo", Some("fop"), 3, 1, 3, vec![1]),
            TestCase::new("foo", Some("fop"), 3, 2, 3, vec![1, 2]),
            TestCase::new("foo", Some("fop"), 3, 3, 3, vec![1, 2, 3]),
        ];

        for case in cases {
            let TestCase {
                key,
                end,
                rev,
                limit,
                revisions,
                total,
            } = case;
            let res = ti.range(key.clone(), end.clone(), rev);
            assert_eq!(total, res.total);
            let res = ti.range_limited(key.clone(), end.clone(), rev, limit);
            assert_eq!(total, res.total);
            assert_eq!(revisions, res.revisions)
        }
    }
}
