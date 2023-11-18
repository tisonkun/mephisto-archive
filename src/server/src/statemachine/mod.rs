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

#[derive(Default, Debug, Ord, PartialOrd, Eq, PartialEq)]
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
