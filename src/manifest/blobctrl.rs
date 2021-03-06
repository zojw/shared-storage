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

use std::sync::Arc;

use async_trait::async_trait;
use tonic::{Request, Response, Status};

use super::{placement, storage, versions::VersionSet};
use crate::{
    client::apipb::{
        self, FinishUploadResponse, KeyRange, Location, PrepareUploadResponse, SpanLoc,
    },
    manifest::storage::{NewBlob, StagingBlob, StagingOperation, VersionEdit},
};

pub struct BlobControl<S, P>
where
    S: storage::MetaStorage,
    P: placement::Placement,
{
    version_set: Arc<VersionSet<S>>,
    placement: Arc<P>,
}

impl<S, P> BlobControl<S, P>
where
    S: storage::MetaStorage,
    P: placement::Placement,
{
    pub fn new(version_set: Arc<VersionSet<S>>, placement: Arc<P>) -> Self {
        Self {
            version_set,
            placement,
        }
    }
}

#[async_trait]
impl<S, P> apipb::blob_upload_control_server::BlobUploadControl for BlobControl<S, P>
where
    S: storage::MetaStorage + Sync + Send + 'static,
    P: placement::Placement + Sync + Send + 'static,
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
                desc: Some(b.to_owned()),
            })
            .collect();

        // FIXME!!!!: split range by span bondary provider by locator
        let stores = vec![1, 2];
        let locs: Vec<Location> = request
            .get_ref()
            .blobs
            .iter()
            .map(|b| Location {
                range: Some(KeyRange {
                    bucket: b.bucket.to_owned(),
                    start: b"1".to_vec(),
                    end: b"3".to_vec(),
                }),
                bucket: b.bucket.to_owned(),
                blob: b.blob.to_owned(),
                spans: vec![SpanLoc {
                    // FIXME: fix mock data!!!
                    span_id: 1,
                    server_id: 1,
                }],
                level: b.level.to_owned(),
            })
            .collect();

        let token = "11233"; // TODO: real token
        let stg = StagingOperation {
            token: token.to_owned(),
            deadline_ts: 0, // TODO:.... handle cleanup logic.
            locations: stores.to_owned(),
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
                add_spans: vec![],
                remove_spans: vec![],
            }])
            .await?;

        Ok(Response::new(PrepareUploadResponse {
            upload_token: token.to_string(), // TODO..
            locations: locs.to_owned(),
        }))
    }

    async fn finish_upload(
        &self,
        request: Request<apipb::FinishUploadRequest>,
    ) -> Result<Response<apipb::FinishUploadResponse>, Status> {
        let token = request.get_ref().upload_token.to_owned();
        let current = self.version_set.current_version().await;
        let op = current.get_stage(&token).unwrap();
        let mut add_blobs: Vec<NewBlob> = op
            .add_blob
            .iter()
            .map(|ab| ab.desc.as_ref().unwrap().to_owned())
            .collect();

        for b in add_blobs.iter_mut() {
            let new_blob = self.placement.add_new_blob(b.to_owned()).await?;
            b.span_ids = new_blob.span_ids;
        }

        self.version_set
            .log_and_apply(vec![VersionEdit {
                add_buckets: vec![],
                remove_buckets: vec![],
                add_blobs,
                remove_blobs: vec![],
                add_staging: vec![],
                remove_staging: vec![token.to_owned()],
                add_spans: vec![],
                remove_spans: vec![],
            }])
            .await?;

        Ok(Response::new(FinishUploadResponse {}))
    }

    async fn rollback_upload(
        &self,
        _request: Request<apipb::RollbackUploadRequest>,
    ) -> Result<Response<apipb::RollbackUploadResponse>, Status> {
        todo!()
    }
}
