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
use tonic::Request;

use crate::{
    cache::storage::PutOptions,
    client::apipb::{blob_uploader_client::BlobUploaderClient, BlobRequest},
    discover::{Discover, ServiceType},
    error::Result,
};

pub struct CacheReplica<D>
where
    D: Discover,
{
    current_srv_id: u32,
    discover: Arc<D>,
}

#[allow(dead_code)]
impl<D> CacheReplica<D>
where
    D: Discover,
{
    pub fn new(current_srv_id: u32, discover: Arc<D>) -> Self {
        Self {
            current_srv_id,
            discover,
        }
    }
}

#[async_trait]
impl<D> super::ObjectPutter for CacheReplica<D>
where
    D: Discover + Send + Sync + 'static,
{
    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        span: u64,
        content: Vec<u8>,
        opt: Option<PutOptions>,
    ) -> Result<()> {
        let mut replica = opt.unwrap().replica_srv;
        replica.retain(|e| *e != self.current_srv_id);

        if replica.is_empty() {
            return Ok(());
        }

        let req_svc = self
            .discover
            .find(ServiceType::NodeUploadSvc, vec![replica[0]])
            .await?;

        if req_svc.is_empty() {
            todo!("retry route?")
        }

        let mut replica_uploader = BlobUploaderClient::new(req_svc[0].channel.clone());
        let req = Request::new(BlobRequest {
            bucket: bucket.to_owned(),
            blob: object.to_owned(),
            span: span.to_owned(),
            content: content.to_owned(),
            request_server_id: req_svc[0].server_id,
            replica_servers: replica,
        });
        replica_uploader.upload(req).await?;

        Ok(())
    }
}
