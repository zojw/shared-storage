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

use super::cachepb;
use crate::client::apipb;

pub struct Uploader {}

#[async_trait]
impl apipb::blob_uploader_server::BlobUploader for Uploader {
    async fn upload(
        &self,
        request: tonic::Request<apipb::BlobRequest>,
    ) -> Result<tonic::Response<apipb::BlobResponse>, tonic::Status> {
        todo!()
    }
}

#[async_trait]
impl cachepb::cache_node_service_server::CacheNodeService for Uploader {
    async fn heartbeat(
        &self,
        _request: tonic::Request<cachepb::HeartbeatRequest>,
    ) -> Result<tonic::Response<cachepb::HeartbeatResponse>, tonic::Status> {
        todo!()
    }
}
