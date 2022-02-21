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

use tonic::{transport::Channel, Request};

use super::apipb::{
    self, blob_upload_control_client::BlobUploadControlClient,
    blob_uploader_client::BlobUploaderClient, locator_client::LocatorClient,
    reader_client::ReaderClient, KeyRange, PrepareUploadResponse,
};
use crate::{
    discover::{Discover, ServiceType},
    error::Result,
    manifest::{
        manifestpb::{bucket_service_client::BucketServiceClient, CreateBucketRequest},
        storage::{BlobStats, NewBlob},
    },
};

pub struct Client<D>
where
    D: Discover,
{
    discover: Arc<D>,
}

impl<D> Client<D>
where
    D: Discover,
{
    pub fn new(discover: Arc<D>) -> Self {
        Self { discover }
    }

    pub async fn flush(
        &mut self,
        bucket: &str,
        blob: &str,
        content: Vec<u8>,
        replica_count: u32,
    ) -> Result<()> {
        let blob_ctrl_svc = self.discover.list(ServiceType::ManifestBlobCtrl).await?;
        let blob_controller = BlobUploadControlClient::new(blob_ctrl_svc[0].channel.clone());

        // prepare blob updload.
        let base_level: u32 = 0; // TODO: smart choose base level.
        let new_blob = NewBlob {
            bucket: bucket.to_owned(),
            blob: blob.to_owned(),
            level: base_level,
            stats: Some(BlobStats {
                smallest: b"2".to_vec(),
                largest: b"3".to_vec(),
                smallest_sequence: 0,
                largest_sequence: 0,
                object_num: 1,
                deletion_num: 0,
            }),
            replica_count,
            size: 1,
            objects: 1,
            span_ids: vec![],
        };
        let prep = Request::new(apipb::PrepareUploadRequest {
            blobs: vec![new_blob],
        });
        let resp = blob_controller.clone().prepare_upload(prep).await?;
        let PrepareUploadResponse {
            upload_token,
            locations,
        } = resp.get_ref().to_owned();

        // upload blob data.
        let object_loc = &locations[0];
        let up = Request::new(apipb::BlobRequest {
            bucket: bucket.to_owned(),
            span: object_loc.spans[0].span_id,
            blob: blob.to_owned(),
            content, // TODO: split content if it over boundary?
            request_server_id: object_loc.spans[0].server_id,
            replica_servers: vec![],
        });
        self.get_uploader(up.get_ref().request_server_id)
            .await?
            .upload(up)
            .await?;

        // commit blob data.
        let fin = Request::new(apipb::FinishUploadRequest { upload_token });
        blob_controller.clone().finish_upload(fin).await?;

        Ok(())
    }

    pub async fn query(&mut self, _query: apipb::QueryExp) -> Result<Vec<apipb::Object>> {
        // TODO: extract keys from expression.
        let loc_req = Request::new(apipb::LocateRequest {
            ranges: vec![KeyRange {
                bucket: "b1".to_string(),
                start: b"1".to_vec(),
                end: b"9".to_vec(),
            }],
        });
        let mut result = Vec::new();
        let locator_svc = self.discover.list(ServiceType::ManifestLocatorSvc).await?;
        let mut locator = LocatorClient::new(locator_svc[0].channel.clone());
        let loc_resp = locator.locate_for_read(loc_req).await?;
        let locations = loc_resp.get_ref().locations.to_owned();

        for loc in locations {
            let mut reader = self.get_reader(loc.spans[0].server_id).await?;
            let condition = Some(apipb::QueryExp {}); // TODO: detach exp to store request.
            let query_req = Request::new(apipb::QueryRequest { condition });
            let query_resp = reader.query(query_req).await?;
            for obj in &query_resp.get_ref().objects {
                result.push(obj.to_owned())
            }
        }
        Ok(result)
    }

    async fn get_uploader(&self, server_id: u32) -> Result<BlobUploaderClient<Channel>> {
        let upload_svc = self
            .discover
            .find(ServiceType::NodeUploadSvc, vec![server_id])
            .await?;
        if upload_svc.is_empty() {
            todo!("handle server id not found")
        }
        let blob_uploader = crate::client::apipb::blob_uploader_client::BlobUploaderClient::new(
            upload_svc[0].channel.clone(),
        );
        Ok(blob_uploader)
    }

    async fn get_reader(&self, srv_id: u32) -> Result<ReaderClient<Channel>> {
        let read_svc = self
            .discover
            .find(ServiceType::NodeReadSvc, vec![srv_id])
            .await?;
        if read_svc.is_empty() {
            todo!("handle server id not found")
        }
        let reader = ReaderClient::new(read_svc[0].channel.clone());
        Ok(reader)
    }

    pub async fn create_bucket(&mut self, bucket_name: &str) -> Result<()> {
        let req = Request::new(CreateBucketRequest {
            bucket: bucket_name.to_owned(),
        });
        let bucket_mng_svc = self.discover.list(ServiceType::ManifestBucketSvc).await?;
        let mut bucket_mng = BucketServiceClient::new(bucket_mng_svc[0].channel.clone());
        bucket_mng.create_bucket(req).await?;
        Ok(())
    }
}
