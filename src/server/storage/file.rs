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

use std::path::{Path, PathBuf};

use anyhow::Result;
use async_trait::async_trait;
use tokio::{
    fs::{create_dir_all, read_dir, remove_dir, DirBuilder, OpenOptions},
    io::AsyncWrite,
};

struct FileStorage {
    root: PathBuf,
}

impl FileStorage {
    pub async fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        DirBuilder::new().recursive(true).create(&root).await?;
        Ok(Self { root })
    }

    fn bucket_path(&self, bucket: impl AsRef<Path>) -> PathBuf {
        self.root.join(bucket)
    }

    fn object_path(&self, bucket: impl AsRef<Path>, object: impl AsRef<Path>) -> PathBuf {
        self.bucket_path(bucket).join(object)
    }
}

#[async_trait]
impl super::Storage for FileStorage {
    type Writer = FileWriter;

    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        let path = self.bucket_path(bucket);
        create_dir_all(&path).await?;
        todo!()
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        let path = self.bucket_path(bucket);
        remove_dir(path).await?;
        todo!()
    }

    async fn list_buckets(&self) -> Result<Vec<String>> {
        let path = self.root.clone();
        let mut entries = read_dir(path).await?;
        let mut bs = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            bs.push(entry.file_name().to_str().unwrap().to_owned())
        }
        return Ok(bs);
    }

    fn put_object(&self, bucket: &str, object: &str) -> Self::Writer {
        todo!()
    }

    async fn read_object(bucket: &str, object: &str, pos: i32, len: i32) -> Result<Vec<u8>> {
        todo!()
    }

    async fn list_objects(&self, bucket: &str) -> Result<Vec<String>> {
        let path = self.bucket_path(bucket);
        let mut entries = read_dir(path).await?;
        let mut fs = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            fs.push(entry.file_name().to_str().unwrap().to_owned())
        }
        return Ok(fs);
    }
}

struct FileWriter {}

impl AsyncWrite for FileWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        todo!()
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        todo!()
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        todo!()
    }
}
