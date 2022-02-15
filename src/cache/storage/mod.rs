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

mod mem;

use async_trait::async_trait;
pub use mem::MemCacheStore;

use crate::error::Result;

#[async_trait]
pub trait CacheStorage: ObjectPutter {
    async fn create_bucket(&self, bucket: &str) -> Result<()>;
    async fn delete_bucket(&self, bucket: &str) -> Result<()>;
    async fn list_buckets(&self) -> Result<Vec<String>>;
    async fn list_objects(&self, bucket: &str) -> Result<Vec<String>>;
    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()>;

    async fn read_object(&self, bucket: &str, object: &str) -> Result<Vec<u8>>;
}

pub struct PutOptions {
    pub replica_srv: Vec<u32>,
}

#[async_trait]
pub trait ObjectPutter {
    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        content: Vec<u8>,
        opt: Option<PutOptions>,
    ) -> Result<()>;
}
