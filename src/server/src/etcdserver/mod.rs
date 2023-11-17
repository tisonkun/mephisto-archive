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

use tonic::{Request, Response, Status};

use crate::etcdserver::proto::{
    etcdserverpb::{
        kv_server::Kv, CompactionRequest, CompactionResponse, DeleteRangeRequest,
        DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse, TxnRequest,
        TxnResponse,
    },
    mvccpb::KeyValue,
};

pub mod proto;

#[derive(Debug, Default)]
pub struct EtcdServer {}

#[tonic::async_trait]
impl Kv for EtcdServer {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let RangeRequest { key, .. } = request.into_inner();
        Ok(Response::new(RangeResponse {
            header: None,
            kvs: vec![KeyValue {
                key,
                create_revision: 0,
                mod_revision: 0,
                version: 0,
                value: "vec![]".as_bytes().to_vec(),
                lease: 0,
            }],
            more: false,
            count: 1,
        }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        unimplemented!("put")
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        unimplemented!("delete")
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        unimplemented!("txn")
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        unimplemented!("compact")
    }
}
