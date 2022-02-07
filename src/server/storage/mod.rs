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

mod file;

use anyhow::Result;
use async_trait::async_trait;
use tokio::io::AsyncWrite;

#[async_trait]
trait Storage {
    type Writer: AsyncWrite;

    async fn create_bucket(&self, bucket: &str) -> Result<()>;
    async fn delete_bucket(&self, bucket: &str) -> Result<()>;
    async fn list_buckets(&self) -> Result<Vec<String>>;
    fn put_object(&self, bucket: &str, object: &str) -> Self::Writer;
    async fn read_object(bucket: &str, object: &str, pos: i32, len: i32) -> Result<Vec<u8>>;
    async fn list_objects(&self, bucket: &str) -> Result<Vec<String>>;
}
