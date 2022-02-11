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

use super::cachepb::{self, HeartbeatRequest, HeartbeatResponse};
use crate::client::apipb::{self, BlobRequest, BlobResponse};

pub struct Uploader<C, B, R>
where
    C: crate::cache::storage::CacheStorage,
    B: crate::blobstore::BlobStore,
    R: crate::cache::storage::CacheStorage,
{
    local_cache: Arc<C>,
    blob_store: Arc<B>,
    replica_cache: Option<Arc<R>>,
}

impl<C, B, R> Uploader<C, B, R>
where
    C: crate::cache::storage::CacheStorage,
    B: crate::blobstore::BlobStore,
    R: crate::cache::storage::CacheStorage,
{
    pub fn new(local_cache: Arc<C>, blob_store: Arc<B>, replica_cache: Option<Arc<R>>) -> Self {
        Self {
            local_cache,
            blob_store,
            replica_cache,
        }
    }
}

#[async_trait]
impl<C, B, R> apipb::blob_uploader_server::BlobUploader for Uploader<C, B, R>
where
    C: crate::cache::storage::CacheStorage + Send + Sync + 'static,
    B: crate::blobstore::BlobStore + Send + Sync + 'static,
    R: crate::cache::storage::CacheStorage + Send + Sync + 'static,
{
    async fn upload(
        &self,
        request: Request<BlobRequest>,
    ) -> Result<Response<BlobResponse>, Status> {
        let request = request.get_ref().to_owned();
        self.blob_store
            .put_object(&request.bucket, &request.blob, request.content.to_owned())
            .await?;
        self.local_cache
            .put_object(&request.bucket, &request.blob, request.content.to_owned())
            .await?;
        if let Some(replica) = &self.replica_cache {
            replica
                .put_object(&request.bucket, &request.blob, request.content.to_owned())
                .await?;
        }
        Ok(Response::new(BlobResponse {}))
    }
}

#[async_trait]
impl<C, B, R> cachepb::cache_node_service_server::CacheNodeService for Uploader<C, B, R>
where
    C: crate::cache::storage::CacheStorage + Send + Sync + 'static,
    B: crate::blobstore::BlobStore + Send + Sync + 'static,
    R: crate::cache::storage::CacheStorage + Send + Sync + 'static,
{
    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        todo!()
    }
}
