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

use super::{
    manifestpb::{
        CreateBucketRequest, CreateBucketResponse, DeleteBlobRequest, DeleteBlobResponse,
        DeleteBucketRequest, DeleteBucketResponse, ListBlobsRequest, ListBlobsResponse,
        ListBucketsRequest, ListBucketsResponse,
    },
    storage::{DeleteBlob, MetaStorage, StagingBucket, StagingOperation, VersionEdit},
    VersionSet,
};
use crate::{
    blobstore::BlobStore,
    cache::cachepb::bucket_service_client::BucketServiceClient,
    discover::{Discover, Svc},
    manifest::manifestpb,
};

// Endpoint for manage bucket information.
// It will keep consistent between blobstore and localstores(both old or new
// addded).
pub struct BucketService<B, M, D>
where
    M: MetaStorage,
    B: BlobStore,
    D: Discover,
{
    blob_store: Arc<B>,
    version_set: Arc<VersionSet<M>>,
    discover: Arc<D>,
}

impl<B, M, D> BucketService<B, M, D>
where
    M: MetaStorage,
    B: BlobStore,
    D: Discover,
{
    pub fn new(blob_store: Arc<B>, version_set: Arc<VersionSet<M>>, discover: Arc<D>) -> Self {
        Self {
            blob_store,
            version_set,
            discover,
        }
    }

    async fn get_bucket_mng_for_all_cache_node(&self) -> crate::error::Result<Vec<Svc>> {
        self.discover
            .list(crate::discover::ServiceType::NodeBucketSvc)
            .await
    }
}

#[async_trait]
impl<B, M, D> manifestpb::bucket_service_server::BucketService for BucketService<B, M, D>
where
    M: MetaStorage + Sync + Send + 'static,
    B: BlobStore + Sync + Send + 'static,
    D: Discover + Sync + Send + 'static,
{
    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        let bucket_name = request.get_ref().bucket.to_owned();

        // prepare create bucket and put log as staging.
        let token = "cb_fake_token".to_string();
        let stg_op = StagingOperation {
            token: token.to_owned(), // TODO:...
            deadline_ts: 0,          // TODO:...
            locations: vec![],
            add_bucket: vec![StagingBucket {
                bucket: bucket_name.to_owned(),
            }],
            add_blob: vec![],
        };
        self.version_set
            .log_and_apply(vec![VersionEdit {
                add_buckets: vec![],
                remove_buckets: vec![],
                add_blobs: vec![],
                remove_blobs: vec![],
                add_staging: vec![stg_op],
                remove_staging: vec![],
                add_spans: vec![],
                remove_spans: vec![],
            }])
            .await?;

        // creat bucket in blob store.
        self.blob_store.create_bucket(&bucket_name).await?;

        // create bucket in each cache nodes.
        for svc in &self.get_bucket_mng_for_all_cache_node().await? {
            let mut client = BucketServiceClient::new(svc.channel.clone());
            let req = Request::new(crate::cache::cachepb::CreateBucketRequest {
                bucket: bucket_name.to_owned(),
            });
            client.create_bucket(req).await?;
        }

        // commit create bucket in meta.
        self.version_set
            .log_and_apply(vec![VersionEdit {
                add_buckets: vec![bucket_name],
                remove_buckets: vec![],
                add_blobs: vec![],
                remove_blobs: vec![],
                add_staging: vec![],
                remove_staging: vec![token],
                add_spans: vec![],
                remove_spans: vec![],
            }])
            .await?;

        Ok(Response::new(CreateBucketResponse {}))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        let bucket_name = request.get_ref().bucket.to_owned(); // TODO: use id instead of name #6

        // Remove bucket from meta first and can return success after this step.
        self.version_set
            .log_and_apply(vec![VersionEdit {
                add_buckets: vec![],
                remove_buckets: vec![bucket_name.to_owned()],
                add_blobs: vec![],
                remove_blobs: vec![],
                add_staging: vec![],
                remove_staging: vec![],
                add_spans: vec![],
                remove_spans: vec![],
            }])
            .await?;
        // TODO: maybe need mantain "ZombieBucket" info to cleanup.

        let _ = {
            self.blob_store.delete_bucket(&bucket_name).await?;

            for svc in self.get_bucket_mng_for_all_cache_node().await? {
                let mut client = BucketServiceClient::new(svc.channel.clone());
                let req = Request::new(crate::cache::cachepb::DeleteBucketRequest {
                    bucket: bucket_name.to_owned(),
                });
                client.delete_bucket(req).await?;
            }
        };

        Ok(Response::new(DeleteBucketResponse {}))
    }

    async fn list_buckets(
        &self,
        _request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
        let current = self.version_set.current_version().await;
        let buckets = current.list_buckets();
        Ok(Response::new(ListBucketsResponse { buckets }))
    }

    async fn list_blobs(
        &self,
        request: Request<ListBlobsRequest>,
    ) -> Result<Response<ListBlobsResponse>, Status> {
        let bucket = request.get_ref().bucket.to_owned();
        let current = self.version_set.current_version().await;
        let blobs = current.list_blobs(&bucket);
        Ok(Response::new(ListBlobsResponse { objects: blobs }))
    }

    async fn delete_blob(
        &self,
        request: Request<DeleteBlobRequest>,
    ) -> Result<Response<DeleteBlobResponse>, Status> {
        let DeleteBlobRequest { bucket, blob } = &request.get_ref().to_owned();

        let current = self.version_set.current_version().await;

        if let Some(blob_desc) = current.get_blob(bucket, blob) {
            // Remove blob from meta first and can return success after this step.
            self.version_set
                .log_and_apply(vec![VersionEdit {
                    add_buckets: vec![],
                    remove_buckets: vec![],
                    add_blobs: vec![],
                    remove_blobs: vec![DeleteBlob {
                        bucket: bucket.to_owned(),
                        blob: blob.to_owned(),
                        level: blob_desc.level,
                        smallest: blob_desc.smallest,
                    }],
                    add_staging: vec![],
                    remove_staging: vec![],
                    add_spans: vec![],
                    remove_spans: vec![],
                }])
                .await?;
            // TODO: maybe need mantain "ZombieBlob" info to cleanup.

            let _ = {
                self.blob_store.delete_object(bucket, blob).await?;

                for svc in self.get_bucket_mng_for_all_cache_node().await? {
                    let mut client = BucketServiceClient::new(svc.channel.clone());
                    for span in &blob_desc.span_ids {
                        let req = Request::new(crate::cache::cachepb::DeleteBlobRequest {
                            bucket: bucket.to_owned(),
                            blob: blob.to_owned(),
                            span: span.to_owned(),
                        });
                        client.delete_blob(req).await?;
                    }
                }
            };
        }

        Ok(Response::new(DeleteBlobResponse {}))
    }
}
