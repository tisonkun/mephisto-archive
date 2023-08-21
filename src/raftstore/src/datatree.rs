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

use std::collections::HashMap;

use bytes::Bytes;

use crate::proto::datatree::{
    CreateReply, CreateRequest, DataTreeReply, GetDataReply, GetDataRequest,
};

#[derive(Debug, Default)]
pub struct DataTree {
    nodes: HashMap<String, Bytes>,
}

impl DataTree {
    pub fn create(&mut self, req: CreateRequest) -> DataTreeReply {
        self.nodes.insert(req.path.clone(), req.data);
        DataTreeReply::Create(CreateReply { path: req.path })
    }

    pub fn get_data(&mut self, req: GetDataRequest) -> DataTreeReply {
        let data = self
            .nodes
            .get(req.path.as_str())
            .cloned()
            .unwrap_or_default();
        DataTreeReply::GetData(GetDataReply { data })
    }
}
