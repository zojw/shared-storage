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

use crate::serverpb;

struct Server {
}

#[async_trait]
impl serverpb::cache_node_service_server::CacheNodeService for Server {
    async fn heartbeat(
        &self,
        _request: tonic::Request<serverpb::HeartbeatRequest>,
    ) -> Result<tonic::Response<serverpb::HeartbeatResponse>, tonic::Status> {
        todo!()
    }
}

#[async_trait]
impl serverpb::object_service_server::ObjectService for Server {
    async fn upload_object(
        &self,
        _request: tonic::Request<tonic::Streaming<serverpb::UploadObjectRequest>>,
    ) -> Result<tonic::Response<serverpb::UploadObjectResponse>, tonic::Status> {
        todo!()
    }

    async fn read_object(
        &self,
        _request: tonic::Request<serverpb::ReadObjectRequest>,
    ) -> Result<tonic::Response<serverpb::ReadObjectResponse>, tonic::Status> {
        todo!()
    }

    async fn delete_object(
        &self,
        _request: tonic::Request<serverpb::DeleteObjectRequest>,
    ) -> Result<tonic::Response<serverpb::DeleteObjectResponse>, tonic::Status> {
        todo!()
    }
}
