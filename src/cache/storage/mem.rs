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
    collections::{hash_map, HashMap},
    sync::Arc,
};

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::PutOptions;
use crate::error::{Error, Result};

type Object = Arc<Vec<u8>>;
type Bucket = Arc<Mutex<HashMap<String, Object>>>;

#[derive(Clone)]
pub struct MemCacheStore {
    buckets: Arc<Mutex<HashMap<String, Bucket>>>,
}

impl Default for MemCacheStore {
    fn default() -> Self {
        Self {
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl MemCacheStore {
    async fn bucket(&self, bucket_name: &str) -> Option<Bucket> {
        let buckets = self.buckets.lock().await;
        buckets.get(bucket_name).cloned()
    }
}

#[async_trait]
impl super::CacheStorage for MemCacheStore {
    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        let mut buckets = self.buckets.lock().await;
        match buckets.entry(bucket.to_owned()) {
            hash_map::Entry::Vacant(ent) => {
                let bucket = Arc::new(Mutex::new(HashMap::new()));
                ent.insert(bucket);
                Ok(())
            }
            hash_map::Entry::Occupied(ent) => {
                Err(Error::AlreadyExists(format!("bucket '{}'", ent.key())))
            }
        }
    }

    async fn delete_bucket(&self, bucket_name: &str) -> Result<()> {
        let mut buckets = self.buckets.lock().await;
        match buckets.remove(bucket_name) {
            Some(_) => Ok(()),
            None => Err(Error::NotFound(format!("bucket '{}'", bucket_name))),
        }
    }

    async fn list_buckets(&self) -> Result<Vec<String>> {
        let buckets = self.buckets.lock().await;
        Ok(buckets.keys().cloned().collect())
    }

    async fn list_objects(&self, bucket_name: &str) -> Result<Vec<String>> {
        if let Some(bucket) = self.bucket(bucket_name).await {
            let bucket = bucket.lock().await;
            Ok(bucket.keys().cloned().collect())
        } else {
            Err(Error::NotFound(format!("bucket '{}'", bucket_name)))
        }
    }

    async fn read_object(&self, bucket_name: &str, object_name: &str) -> Result<Vec<u8>> {
        if let Some(bucket) = self.bucket(bucket_name).await {
            match bucket.lock().await.get(object_name) {
                Some(object) => {
                    let mut dst = vec![0u8; object.len()];
                    dst.copy_from_slice(object);
                    Ok(dst)
                }
                None => Err(Error::NotFound(format!("object '{}'", object_name))),
            }
        } else {
            Err(Error::NotFound(format!("bucket '{}'", bucket_name)))
        }
    }

    async fn delete_object(&self, bucket_name: &str, object_name: &str) -> Result<()> {
        if let Some(bucket) = self.bucket(bucket_name).await {
            let mut bucket = bucket.lock().await;
            bucket.remove(object_name);
        }
        Ok(())
    }
}

#[async_trait]
impl super::ObjectPutter for MemCacheStore {
    async fn put_object(
        &self,
        bucket_name: &str,
        object_name: &str,
        content: Vec<u8>,
        _opt: Option<PutOptions>,
    ) -> Result<()> {
        if let Some(bucket) = self.bucket(bucket_name).await {
            let mut bucket = bucket.lock().await;
            bucket.insert(object_name.to_owned(), Arc::new(content));
            Ok(())
        } else {
            Err(Error::NotFound(format!("bucket '{}'", bucket_name)))
        }
    }
}
