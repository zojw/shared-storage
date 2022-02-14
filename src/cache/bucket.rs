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
    cachepb::{
        CreateBucketRequest, CreateBucketResponse, DeleteBlobRequest, DeleteBlobResponse,
        DeleteBucketRequest, DeleteBucketResponse, ListBucketsRequest, ListBucketsResponse,
        ListObjectsRequest, ListObjectsResponse,
    },
    status::CacheStatus,
    CacheStorage,
};
use crate::cache::cachepb::bucket_service_server::BucketService;

pub struct CacheNodeBucketService<L>
where
    L: CacheStorage,
{
    local_store: Arc<L>,
    status: Arc<CacheStatus<L>>,
}

impl<L> CacheNodeBucketService<L>
where
    L: CacheStorage,
{
    pub fn new(local_store: Arc<L>, status: Arc<CacheStatus<L>>) -> Self {
        Self {
            local_store,
            status,
        }
    }
}

#[async_trait]
impl<L> BucketService for CacheNodeBucketService<L>
where
    L: CacheStorage + Sync + Send + 'static,
{
    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        let bucket = request.get_ref().bucket.to_owned();
        self.local_store.create_bucket(&bucket).await?;
        self.status.add_bucket(&bucket).await;
        Ok(Response::new(CreateBucketResponse {}))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        let bucket = request.get_ref().bucket.to_owned();
        self.local_store.delete_bucket(&bucket).await?;
        Ok(Response::new(DeleteBucketResponse {}))
    }

    async fn delete_blob(
        &self,
        request: Request<DeleteBlobRequest>,
    ) -> Result<Response<DeleteBlobResponse>, Status> {
        let DeleteBlobRequest { bucket, blob } = request.get_ref().to_owned();
        self.local_store.delete_object(&bucket, &blob).await?;
        self.status.delete_blob(&bucket, &blob).await;
        Ok(Response::new(DeleteBlobResponse {}))
    }

    async fn list_buckets(
        &self,
        _request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
        let buckets = self.local_store.list_buckets().await?;
        Ok(Response::new(ListBucketsResponse { buckets }))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let bucket = request.get_ref().bucket.to_owned();
        let blobs = self.local_store.list_objects(&bucket).await?;
        Ok(Response::new(ListObjectsResponse { objects: blobs }))
    }
}
