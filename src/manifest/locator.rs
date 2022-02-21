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
use tonic::Response;

use super::{status::ManifestStatus, storage::MetaStorage, VersionSet};
use crate::{client::apipb, discover::Discover};

pub struct CacheServerLocator<S, D>
where
    S: MetaStorage,
    D: Discover,
{
    version_set: Arc<VersionSet<S>>,
    manifest_status: Arc<ManifestStatus<D>>,
}

impl<S, D> CacheServerLocator<S, D>
where
    S: MetaStorage,
    D: Discover,
{
    pub fn new(version_set: Arc<VersionSet<S>>, manifest_status: Arc<ManifestStatus<D>>) -> Self {
        Self {
            version_set,
            manifest_status,
        }
    }
}

#[async_trait]
impl<S, D> apipb::locator_server::Locator for CacheServerLocator<S, D>
where
    S: MetaStorage + Sync + Send + 'static,
    D: Discover + Sync + Send + 'static,
{
    async fn locate_for_read(
        &self,
        request: tonic::Request<apipb::LocateRequest>,
    ) -> Result<tonic::Response<apipb::LocateResponse>, tonic::Status> {
        let ranges = request.get_ref().ranges.to_owned();
        let current = self.version_set.current_version().await;
        let mut locations = current.get_location(ranges);
        for l in locations.iter_mut() {
            if let Some(span_nodes) = self
                .manifest_status
                .locate_span_server(&l.bucket, &l.blob)
                .await
            {
                let mut spans = Vec::new();
                for (span, node) in span_nodes {
                    spans.push(apipb::SpanLoc {
                        span_id: span,
                        server_id: node,
                    })
                }
                l.spans = spans;
            }
        }
        Ok(Response::new(apipb::LocateResponse { locations }))
    }

    async fn locate_for_compact(
        &self,
        _request: tonic::Request<apipb::LocateRequest>,
    ) -> Result<tonic::Response<apipb::LocateResponse>, tonic::Status> {
        todo!()
    }
}
