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
use tonic::{Response, Status};

use super::{
    cachepb::{
        self, HeartbeatRequest, HeartbeatResponse, RefillCacheRequest, RefillCacheResponse,
        RemoveCacheRequest, RemoveCacheResponse,
    },
    CacheStatus, CacheStorage,
};
use crate::blobstore::BlobStore;

pub struct NodeCacheManager<S, B>
where
    S: CacheStorage,
    B: BlobStore,
{
    status: Arc<CacheStatus<S>>,
    blob_store: Arc<B>,
    local_store: Arc<S>,
}

impl<S, B> NodeCacheManager<S, B>
where
    S: CacheStorage,
    B: BlobStore,
{
    pub fn new(status: Arc<CacheStatus<S>>, blob_store: Arc<B>, local_store: Arc<S>) -> Self {
        Self {
            status,
            blob_store,
            local_store,
        }
    }
}

#[async_trait]
impl<S, B> cachepb::node_cache_manage_service_server::NodeCacheManageService
    for NodeCacheManager<S, B>
where
    S: CacheStorage + Sync + Send + 'static,
    B: BlobStore + Sync + Send + 'static,
{
    async fn heartbeat(
        &self,
        request: tonic::Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let HeartbeatRequest {
            server_id,
            last_seq,
            current_seq,
        } = request.get_ref().to_owned();

        let events = Some(self.status.fetch_change_event(last_seq, current_seq).await);

        Ok(Response::new(HeartbeatResponse {
            current_seq,
            status: Some(cachepb::Status { server_id, events }),
        }))
    }

    async fn refill_cache(
        &self,
        request: tonic::Request<RefillCacheRequest>,
    ) -> Result<tonic::Response<RefillCacheResponse>, tonic::Status> {
        let RefillCacheRequest { bucket, blob } = request.get_ref();
        let content = self.blob_store.read_object(bucket, blob).await?;
        self.local_store
            .put_object(bucket, blob, content, None)
            .await?;
        Ok(Response::new(RefillCacheResponse {}))
    }

    async fn remove_cache(
        &self,
        request: tonic::Request<RemoveCacheRequest>,
    ) -> Result<tonic::Response<RemoveCacheResponse>, tonic::Status> {
        let RemoveCacheRequest { bucket, blob } = request.get_ref();
        self.local_store.delete_object(bucket, blob).await?;
        Ok(Response::new(RemoveCacheResponse {}))
    }
}
