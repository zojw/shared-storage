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

use crate::client::apipb;

pub struct Server {}

#[async_trait]
impl apipb::blob_upload_control_server::BlobUploadControl for Server {
    async fn prepare_upload(
        &self,
        request: tonic::Request<apipb::PrepareUploadRequest>,
    ) -> Result<tonic::Response<apipb::PrepareUploadResponse>, tonic::Status> {
        todo!()
    }

    async fn finish_upload(
        &self,
        request: tonic::Request<apipb::FinishUploadRequest>,
    ) -> Result<tonic::Response<apipb::FinishUploadResponse>, tonic::Status> {
        todo!()
    }

    async fn rollback_upload(
        &self,
        request: tonic::Request<apipb::RollbackUploadRequest>,
    ) -> Result<tonic::Response<apipb::RollbackUploadResponse>, tonic::Status> {
        todo!()
    }
}
