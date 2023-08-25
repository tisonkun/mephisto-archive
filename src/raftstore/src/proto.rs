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

use std::mem::size_of;

use bytes::{Buf, BufMut, BytesMut};
use crossbeam::channel::Sender;

use crate::proto::datatree::{DataTreeReply, DataTreeRequest, ReplyHeader, RequestHeader};

pub mod datatree {
    include!(concat!(env!("OUT_DIR"), "/datatree.rs"));

    #[derive(Debug, Clone)]
    pub enum DataTreeRequest {
        Create(CreateRequest),
        GetData(GetDataRequest),
    }

    #[derive(Debug, Clone)]
    pub enum DataTreeReply {
        Error(i32),
        Create(CreateReply),
        GetData(GetDataReply),
    }
}

#[derive(Debug, Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub struct ReqId {
    pub client_id: uuid::Uuid,
    pub seq_id: u64,
}

impl ReqId {
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = BytesMut::with_capacity(3 * size_of::<u64>());
        let (msb, lsb) = self.client_id.as_u64_pair();
        data.put_u64(msb);
        data.put_u64(lsb);
        data.put_u64(self.seq_id);
        data.to_vec()
    }

    pub fn deserialize<B: Buf>(mut data: B) -> ReqId {
        let msb = data.get_u64();
        let lsb = data.get_u64();
        let client_id = uuid::Uuid::from_u64_pair(msb, lsb);
        let seq_id = data.get_u64();
        ReqId { client_id, seq_id }
    }
}

#[derive(Debug)]
pub struct FatRequest {
    pub req_id: ReqId,
    pub header: RequestHeader,
    pub request: DataTreeRequest,
    pub reply: Sender<FatReply>,
}

#[derive(Debug)]
pub struct FatReply {
    pub header: ReplyHeader,
    pub reply: DataTreeReply,
}
