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

use super::{storage::MetaStorage, VersionSet};
use crate::client::apipb;

pub struct CacheServerLocator<S>
where
    S: MetaStorage,
{
    version_set: Arc<VersionSet<S>>,
}

impl<S> CacheServerLocator<S>
where
    S: MetaStorage,
{
    pub fn new(version_set: Arc<VersionSet<S>>) -> Self {
        Self { version_set }
    }
}

#[async_trait]
impl<S> apipb::locator_server::Locator for CacheServerLocator<S>
where
    S: MetaStorage + Sync + Send + 'static,
{
    async fn locate_for_read(
        &self,
        request: tonic::Request<apipb::LocateRequest>,
    ) -> Result<tonic::Response<apipb::LocateResponse>, tonic::Status> {
        let ranges = request.get_ref().ranges.to_owned();
        let current = self.version_set.current_version().await;
        let locations = current.get_location(ranges);
        Ok(Response::new(apipb::LocateResponse { locations }))
    }

    async fn locate_for_compact(
        &self,
        request: tonic::Request<apipb::LocateRequest>,
    ) -> Result<tonic::Response<apipb::LocateResponse>, tonic::Status> {
        todo!()
    }
}
