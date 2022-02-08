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

use std::{
    borrow::BorrowMut,
    future::Future,
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
    pin::Pin,
    task::Poll,
};

use anyhow::Result;
use async_trait::async_trait;
use tokio::{
    fs::{create_dir_all, read_dir, remove_dir, DirBuilder, File, OpenOptions},
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
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
        Ok(())
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        let path = self.bucket_path(bucket);
        remove_dir(path).await?;
        Ok(())
    }

    async fn list_buckets(&self) -> Result<Vec<String>> {
        let path = self.root.clone();
        let mut entries = read_dir(path).await?;
        let mut bs = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            bs.push(entry.file_name().to_str().unwrap().to_owned())
        }
        Ok(bs)
    }

    fn put_object(&self, bucket: &str, object: &str) -> Result<Self::Writer> {
        let path = self.object_path(bucket, object);
        let buf = Vec::new();
        Ok(FileWriter {
            path,
            buf,
            close_fut: None,
        })
    }

    async fn read_object(&self, bucket: &str, object: &str, pos: i32, len: i32) -> Result<Vec<u8>> {
        let path = self.object_path(bucket, object);
        let f: File = OpenOptions::new().read(true).open(&path).await?;
        let std: std::fs::File = f.into_std().await;
        let mut res = Vec::new();
        let mut pos = pos.to_owned() as u64;
        loop {
            let mut buf = vec![0u8; len as usize];
            let read_len = std.read_at(&mut buf, pos)?;
            let buf = &buf[..read_len];
            res.extend_from_slice(buf);
            if buf.len() == len as usize {
                break;
            }
            pos += read_len as u64;
        }
        Ok(res)
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

type PinnedFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + 'static + Send>>;

struct FileWriter {
    path: PathBuf,
    buf: Vec<u8>,
    close_fut: Option<PinnedFuture<()>>,
}

impl FileWriter {
    async fn write_all(path: impl AsRef<Path>, src: Vec<u8>) -> Result<()> {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await?;
        f.write_all(&src).await?;
        Ok(())
    }
}

impl AsyncWrite for FileWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        let len = buf.len();
        self.buf.extend_from_slice(buf);
        Poll::Ready(Ok(len))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        let me = self.get_mut();
        let mut fut = me
            .close_fut
            .take()
            .unwrap_or_else(|| Box::pin(Self::write_all(&me.path, me.buf)));

        match fut.as_mut().poll(cx) {
            Poll::Pending => {
                me.close_fut = Some(fut);
                Poll::Pending
            }
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))),
        }
    }
}
