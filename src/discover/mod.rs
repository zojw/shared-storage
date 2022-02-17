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

mod local;

use async_trait::async_trait;
pub use local::LocalSvcDiscover;
use tonic::transport::Channel;

use crate::error::Result;

#[async_trait]
pub trait Discover {
    async fn list(&self, svc_type: ServiceType) -> Result<Vec<Svc>>;
    async fn find(&self, svc_type: ServiceType, srv_ids: Vec<u32>) -> Result<Vec<Svc>>;
    // async fn subscribe(&self) -> Stream<()>;
}

pub enum ServiceType {
    NodeCacheManageSvc = 1,
    NodeBucketSvc = 2,
    NodeUploadSvc = 3,
    NodeReadSvc = 4,
    ManifestBlobCtrl = 5,
    ManifestBucketSvc = 6,
    ManifestLocatorSvc = 7,
}

#[derive(Clone)]
pub struct Svc {
    pub server_id: u32,
    pub channel: Channel,
}
