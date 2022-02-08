// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use tonic::{Response, Status};

use crate::client::apipb::{self, value::Content, Object, QueryResponse};

pub struct CacheReader {}

#[async_trait]
impl apipb::reader_server::Reader for CacheReader {
    async fn query(
        &self,
        _request: tonic::Request<apipb::QueryRequest>,
    ) -> Result<Response<apipb::QueryResponse>, Status> {
        // TODO: do real read.
        let objects = vec![Object {
            key: b"2".to_vec(),
            value: Some(apipb::Value {
                content: Some(Content::BytesValue(b"content1".to_vec())),
            }),
        }];
        Ok(Response::new(QueryResponse { objects }))
    }
}
