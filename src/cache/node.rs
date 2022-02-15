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
    cachepb::{self, HeartbeatRequest, HeartbeatResponse},
    CacheStatus, CacheStorage,
};

pub struct NodeCacheManager<S>
where
    S: CacheStorage,
{
    status: Arc<CacheStatus<S>>,
}

impl<S> NodeCacheManager<S>
where
    S: CacheStorage,
{
    pub fn new(status: Arc<CacheStatus<S>>) -> Self {
        Self { status }
    }
}

#[async_trait]
impl<S> cachepb::node_cache_manage_service_server::NodeCacheManageService for NodeCacheManager<S>
where
    S: CacheStorage + Sync + Send + 'static,
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
}
