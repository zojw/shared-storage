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

use async_trait::async_trait;

use crate::masterpb;

struct Master {}

#[async_trait]
impl masterpb::bucket_service_server::BucketService for Master {
    async fn create_bucket(
        &self,
        _request: tonic::Request<masterpb::CreateBucketRequest>,
    ) -> Result<tonic::Response<masterpb::CreateBucketResponse>, tonic::Status> {
        todo!()
    }

    async fn delete_bucket(
        &self,
        _request: tonic::Request<masterpb::DeleteBucketRequest>,
    ) -> Result<tonic::Response<masterpb::DeleteBucketResponse>, tonic::Status> {
        todo!()
    }

    async fn list_buckets(
        &self,
        _request: tonic::Request<masterpb::ListBucketsRequest>,
    ) -> Result<tonic::Response<masterpb::ListBucketsResponse>, tonic::Status> {
        todo!()
    }

    async fn list_objects(
        &self,
        _request: tonic::Request<masterpb::ListObjectsRequest>,
    ) -> Result<tonic::Response<masterpb::ListObjectsResponse>, tonic::Status> {
        todo!()
    }
}
