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

use super::{storage, versions::VersionSet};
use crate::{
    client::apipb::{self, PrepareUploadResponse},
    manifest::storage::{StagingBlob, StagingOperation, VersionEdit},
};

pub struct BlobControl<S>
where
    S: storage::MetaStorage,
{
    version_set: VersionSet<S>,
}

impl<S> BlobControl<S>
where
    S: storage::MetaStorage,
{
    pub fn new(version_set: VersionSet<S>) -> Self {
        Self { version_set }
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
        let blobs: Vec<StagingBlob> = request
            .get_ref()
            .blobs
            .to_owned()
            .iter()
            .map(|b| StagingBlob {
                bucket: b.bucket.to_owned(),
                blob: b.blob.to_owned(),
                stats: b.stats.to_owned(),
            })
            .collect();
        let locs = vec![]; // TODO: give a writable location.
        let token = "11233"; // TODO: real token
        let stg = StagingOperation {
            token: token.to_owned(),
            deadline_ts: 0, // TODO:.... handle cleanup logic.
            locations: locs.to_owned(),
            add_bucket: vec![],
            add_blob: blobs,
        };
        self.version_set
            .log_and_apply(vec![VersionEdit {
                add_buckets: vec![],
                remove_buckets: vec![],
                add_blobs: vec![],
                remove_blobs: vec![],
                add_staging: vec![stg],
                remove_staging: vec![],
            }])
            .await?;
        Ok(Response::new(PrepareUploadResponse {
            upload_token: token.to_string(), // TODO..
            locations: vec![],
        }))
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
