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
use tonic::{Request, Response, Status};

use super::storage;
use crate::client::apipb::{self, PrepareUploadResponse};

pub struct BlobControl<S>
where
    S: storage::MetaStorage,
{
    meta_storage: S,
}

impl<S> BlobControl<S>
where
    S: storage::MetaStorage,
{
    pub fn new(meta_storage: S) -> Self {
        Self { meta_storage }
    }
}

#[async_trait]
impl<S> apipb::blob_upload_control_server::BlobUploadControl for BlobControl<S>
where
    S: storage::MetaStorage + Sync + Send + 'static,
{
    async fn prepare_upload(
        &self,
        request: Request<apipb::PrepareUploadRequest>,
    ) -> Result<Response<PrepareUploadResponse>, Status> {
        todo!()
    }

    async fn finish_upload(
        &self,
        request: Request<apipb::FinishUploadRequest>,
    ) -> Result<Response<apipb::FinishUploadResponse>, Status> {
        todo!()
    }

    async fn rollback_upload(
        &self,
        request: Request<apipb::RollbackUploadRequest>,
    ) -> Result<Response<apipb::RollbackUploadResponse>, Status> {
        todo!()
    }
}
