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

mod mock;
mod s3;

use async_trait::async_trait;
pub use mock::MemBlobStore as MockBlobStore;

use crate::error::Result;

#[async_trait]
pub trait BlobStore {
    async fn create_bucket(&self, bucket: &str) -> Result<()>;
    async fn delete_bucket(&self, bucket: &str) -> Result<()>;
    async fn list_buckets(&self) -> Result<Vec<String>>;
    async fn list_objects(&self, bucket: &str) -> Result<Vec<String>>;

    async fn put_object(&self, bucket: &str, object: &str, content: Vec<u8>) -> Result<()>;
    async fn read_object(&self, bucket: &str, object: &str) -> Result<Vec<u8>>;
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
