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

use tonic::{transport::Channel, Request};

use super::apipb::{
    self, blob_upload_control_client::BlobUploadControlClient,
    blob_uploader_client::BlobUploaderClient, locator_client::LocatorClient,
    reader_client::ReaderClient, KeyRange, Location, PrepareUploadResponse,
};
use crate::{
    error::Result,
    manifest::{
        manifestpb::{bucket_service_client::BucketServiceClient, CreateBucketRequest},
        storage::{BlobStats, NewBlob},
    },
};

pub struct Client {
    blob_controller: BlobUploadControlClient<Channel>,
    blob_uploaders: Vec<BlobUploaderClient<Channel>>,
    locator: LocatorClient<Channel>,
    readers: Vec<ReaderClient<Channel>>,
    bucket_mng: BucketServiceClient<Channel>,
}

impl Client {
    pub fn new(
        blob_controller: BlobUploadControlClient<Channel>,
        blob_uploaders: Vec<BlobUploaderClient<Channel>>,
        locator: LocatorClient<Channel>,
        readers: Vec<ReaderClient<Channel>>,
        bucket_mng: BucketServiceClient<Channel>,
    ) -> Self {
        Self {
            blob_controller,
            blob_uploaders,
            locator,
            readers,
            bucket_mng,
        }
    }

    pub async fn flush(&mut self, bucket: &str, blob: &str, content: Vec<u8>) -> Result<()> {
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
        };
        let prep = Request::new(apipb::PrepareUploadRequest {
            blobs: vec![new_blob],
        });
        let resp = self.blob_controller.prepare_upload(prep).await?;
        let PrepareUploadResponse {
            upload_token,
            locations,
        } = resp.get_ref().to_owned();

        // upload blob data.
        let up = Request::new(apipb::BlobRequest {
            bucket: bucket.to_owned(),
            blob: blob.to_owned(),
            content, // TODO: split content if it over boundary?
        });
        self.get_uploader(&locations[0]).await?.upload(up).await?;

        // commit blob data.
        let fin = Request::new(apipb::FinishUploadRequest { upload_token });
        self.blob_controller.finish_upload(fin).await?;

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
        let loc_resp = self.locator.locate_for_read(loc_req).await?;
        let locations = loc_resp.get_ref().locations.to_owned();

        for loc in locations {
            let mut reader = self.get_reader(loc.stores[0]).await?;
            let condition = Some(apipb::QueryExp {}); // TODO: detach exp to store request.
            let query_req = Request::new(apipb::QueryRequest { condition });
            let query_resp = reader.query(query_req).await?;
            for obj in &query_resp.get_ref().objects {
                result.push(obj.to_owned())
            }
        }
        Ok(result)
    }

    pub async fn get_uploader(&self, _loc: &Location) -> Result<BlobUploaderClient<Channel>> {
        // TODO: it should be factorization and route right uploader by _loc
        Ok(self.blob_uploaders[0].clone())
    }

    pub async fn get_reader(&self, _store: u32) -> Result<ReaderClient<Channel>> {
        // TODO: establish & cache reader for store_id.
        Ok(self.readers[0].clone())
    }

    pub async fn create_bucket(&mut self, bucket_name: &str) -> Result<()> {
        let req = Request::new(CreateBucketRequest {
            bucket: bucket_name.to_owned(),
        });
        self.bucket_mng.create_bucket(req).await?;
        Ok(())
    }
}
