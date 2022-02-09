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
use prost::Message;
use tokio::sync::Mutex;

use crate::{
    blobstore::BlobStore,
    error::{Error, Result},
};

const META_BLOB_NAME: &str = "engula-meta-blob";
const META_BUCKET_NAME: &str = "engula-meta-bucket";

// Simple manifest storage base on blobstore.
pub struct MemBlobMetaStore<B>
where
    B: BlobStore,
{
    blob_store: B,
    versions: Arc<Mutex<Vec<super::VersionEdit>>>,
}

impl<B> MemBlobMetaStore<B>
where
    B: BlobStore,
{
    pub async fn new(blob_store: B) -> Result<Self> {
        let s = Self {
            blob_store,
            versions: Arc::new(Mutex::new(Vec::new())),
        };
        s.create_internel_bucket_if_not_exist().await?;
        s.recover().await?;
        Ok(s)
    }

    async fn create_internel_bucket_if_not_exist(&self) -> Result<()> {
        let r = self.blob_store.create_bucket(META_BUCKET_NAME).await;
        match r {
            Err(Error::AlreadyExists(_)) => Ok(()),
            o @ _ => o,
        }?;
        Ok(())
    }

    async fn recover(&self) -> Result<()> {
        let vs = {
            let r = self
                .blob_store
                .read_object(META_BUCKET_NAME, META_BLOB_NAME)
                .await;
            let content = match r {
                Err(Error::NotFound(_)) => Ok(vec![]),
                o @ _ => o,
            }?;
            super::VersionEditList::decode(&content[..])?
        };
        let mut versions = self.versions.lock().await;
        *versions = vs.versions.clone();
        Ok(())
    }

    async fn persist_all(&self) -> Result<()> {
        let versions = self.versions.lock().await;
        let vs = super::VersionEditList {
            versions: versions.to_vec(),
        };
        let content = vs.encode_to_vec();
        self.blob_store
            .put_object(META_BUCKET_NAME, META_BLOB_NAME, content)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<B> super::MetaStorage for MemBlobMetaStore<B>
where
    B: BlobStore + Sync + Send,
{
    async fn append(&self, ve: super::VersionEdit) -> Result<()> {
        // TODO: how to avoid fake leader append?
        async {
            let versions = self.versions.clone();
            let mut versions = versions.lock().await;
            versions.push(ve.to_owned());
        }
        .await;
        self.persist_all().await?;
        Ok(())
    }

    async fn read_all(&self) -> Result<Vec<super::VersionEdit>> {
        let vs = {
            let versions = self.versions.clone();
            let vs = versions.lock().await;
            vs.clone()
        };
        Ok(vs)
    }
}
