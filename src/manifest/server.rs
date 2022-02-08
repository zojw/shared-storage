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

use crate::manifest::manifestpb;

struct Master {}

#[async_trait]
impl manifestpb::bucket_service_server::BucketService for Master {
    async fn create_bucket(
        &self,
        _request: tonic::Request<manifestpb::CreateBucketRequest>,
    ) -> Result<tonic::Response<manifestpb::CreateBucketResponse>, tonic::Status> {
        todo!()
    }

    async fn delete_bucket(
        &self,
        _request: tonic::Request<manifestpb::DeleteBucketRequest>,
    ) -> Result<tonic::Response<manifestpb::DeleteBucketResponse>, tonic::Status> {
        todo!()
    }

    async fn list_buckets(
        &self,
        _request: tonic::Request<manifestpb::ListBucketsRequest>,
    ) -> Result<tonic::Response<manifestpb::ListBucketsResponse>, tonic::Status> {
        todo!()
    }

    async fn list_objects(
        &self,
        _request: tonic::Request<manifestpb::ListObjectsRequest>,
    ) -> Result<tonic::Response<manifestpb::ListObjectsResponse>, tonic::Status> {
        todo!()
    }
}
