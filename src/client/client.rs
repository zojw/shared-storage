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

use tonic::{
    transport::{Channel, Endpoint, Server, Uri},
    Request,
};

use super::{
    apipb::{
        self, blob_upload_control_client::BlobUploadControlClient,
        blob_uploader_client::BlobUploaderClient, locator_client::LocatorClient,
        reader_client::ReaderClient, PrepareUploadResponse, WriteLocation,
    },
    blob_writer::BlobStoreWriter,
    MockStream,
};
use crate::{blobstore::MemBlobStore, error::Result, manifest::storage::NewBlob};

pub struct Client {
    blob_controller: BlobUploadControlClient<Channel>,
    locator: LocatorClient<Channel>,
    blob_store: MemBlobStore,
}

impl Client {
    pub fn new(
        blob_controller: BlobUploadControlClient<Channel>,
        locator: LocatorClient<Channel>,
        blob_store: MemBlobStore,
    ) -> Self {
        Self {
            blob_controller,
            locator,
            blob_store,
        }
    }

    pub async fn flush(&mut self, bucket: &str, object: &str, content: Vec<u8>) -> Result<()> {
        // prepare blob updload.
        let base_level: i32 = 0; // TODO: smart choose base level.
        let blob = NewBlob {
            bucket: bucket.to_owned(),
            blob: object.to_owned(),
            level: base_level,
            stats: None,
        };
        let prep = Request::new(apipb::PrepareUploadRequest { blobs: vec![blob] });
        let resp = self.blob_controller.prepare_upload(prep).await?;
        let PrepareUploadResponse {
            upload_token,
            locations,
        } = resp.get_ref().to_owned();

        // upload blob data.
        let up = Request::new(apipb::BlobRequest {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            content,
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
            keys: vec![b"1".to_vec()],
        });
        let mut result = Vec::new();
        let loc_resp = self.locator.locate_keys(loc_req).await?;
        let locations = loc_resp.get_ref().locations.to_owned();

        for loc in locations {
            let mut reader = self.get_reader(loc.store).await?;
            let condition = Some(apipb::QueryExp {}); // TODO: detach exp to store request.
            let query_req = Request::new(apipb::QueryRequest { condition });
            let query_resp = reader.query(query_req).await?;
            for obj in &query_resp.get_ref().objects {
                result.push(obj.to_owned())
            }
        }
        Ok(result)
    }

    pub async fn get_uploader(&self, _loc: &WriteLocation) -> Result<BlobUploaderClient<Channel>> {
        // TODO: use real instead of mock one
        Self::build_local_blob_writer(self.blob_store.clone()).await
    }

    async fn build_local_blob_writer(
        blob_store: MemBlobStore,
    ) -> Result<BlobUploaderClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let blob_writer = BlobStoreWriter { blob_store };
        tokio::spawn(async move {
            Server::builder()
                .add_service(apipb::blob_uploader_server::BlobUploaderServer::new(
                    blob_writer,
                ))
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                    MockStream(server),
                )]))
                .await
        });
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take().unwrap();
                async move { Ok::<_, std::io::Error>(MockStream(client)) }
            }))
            .await?;
        let client = BlobUploaderClient::new(channel);
        Ok(client)
    }

    pub async fn get_reader(&self, _store: u64) -> Result<ReaderClient<Channel>> {
        // TODO: establish & cache reader for store_id.
        Self::build_cache_reader().await
    }

    async fn build_cache_reader() -> Result<ReaderClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let reader = crate::cache::CacheReader {};
        tokio::spawn(async move {
            Server::builder()
                .add_service(apipb::reader_server::ReaderServer::new(reader))
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                    MockStream(server),
                )]))
                .await
        });
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://[::]:50053")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take().unwrap();
                async move { Ok::<_, std::io::Error>(MockStream(client)) }
            }))
            .await?;
        let client = apipb::reader_client::ReaderClient::new(channel);
        Ok(client)
    }
}
