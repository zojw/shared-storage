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

use std::{collections::HashSet, sync::Arc};

use tokio::sync::Mutex;

use super::{
    cachepb::{self, cache_event::EventType},
    storage,
};
use crate::{cache::cachepb::cache_events, error::Result};

#[derive(Hash, PartialEq, Eq, Clone)]
struct BucketBlob {
    bucket: String,
    blob: String,
}

// CacheStatus maintains the memory status for current cache node.
pub struct CacheStatus<S>
where
    S: storage::CacheStorage,
{
    inner: Arc<Mutex<Inner>>,
    store: Arc<S>,
}

struct Inner {
    buckets: HashSet<String>,
    blobs: HashSet<BucketBlob>,
    last_heartbeat: u64,
    delta: Vec<Event>,
}

enum Event {
    AddBucket { bucket: String },
    DeleteBucket { bucket: String },
    AddBlob { bucket: String, blob: String },
    DeleteBlob { bucket: String, blob: String },
}

impl<S> CacheStatus<S>
where
    S: storage::CacheStorage,
{
    pub async fn new(store: Arc<S>) -> Result<Self> {
        let s = Self {
            inner: Arc::new(Mutex::new(Inner {
                buckets: HashSet::new(),
                blobs: HashSet::new(),
                last_heartbeat: 0,
                delta: Vec::new(),
            })),
            store,
        };
        s.recovery().await?;
        Ok(s)
    }

    async fn recovery(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.buckets = self.store.list_buckets().await?.iter().cloned().collect();
        let mut blobs = HashSet::new();
        for bucket in &inner.buckets {
            let bs = self.store.list_objects(bucket).await?;
            for b in bs {
                blobs.insert(BucketBlob {
                    bucket: b.to_owned(),
                    blob: b.to_owned(),
                });
            }
        }
        inner.blobs = blobs;
        Ok(())
    }

    pub async fn add_bucket(&self, bucket: &str) {
        let mut inner = self.inner.lock().await;
        inner.buckets.insert(bucket.to_owned());
        inner.delta.push(Event::AddBucket {
            bucket: bucket.to_owned(),
        });
    }

    pub async fn delete_bucket(&self, bucket: &str) {
        let mut inner = self.inner.lock().await;
        inner.buckets.remove(bucket);
        inner.delta.push(Event::DeleteBucket {
            bucket: bucket.to_owned(),
        });
    }

    pub async fn add_blob(&self, bucket: &str, blob: &str) {
        let mut inner = self.inner.lock().await;
        assert!(inner.buckets.contains(bucket));
        inner.blobs.insert(BucketBlob {
            bucket: bucket.to_owned(),
            blob: blob.to_owned(),
        });
        inner.delta.push(Event::AddBlob {
            bucket: bucket.to_owned(),
            blob: blob.to_owned(),
        });
    }

    pub async fn delete_blob(&self, bucket: &str, blob: &str) {
        let mut inner = self.inner.lock().await;
        assert!(inner.buckets.contains(bucket));
        let bo = BucketBlob {
            bucket: bucket.to_owned(),
            blob: blob.to_owned(),
        };
        inner.blobs.remove(&bo);
        inner.delta.push(Event::DeleteBlob {
            bucket: bucket.to_owned(),
            blob: blob.to_owned(),
        });
    }

    pub async fn fetch_change_event(
        &self,
        last_seq: u64,
        current_seq: u64,
    ) -> cachepb::CacheEvents {
        let mut inner = self.inner.lock().await;
        let ev = if last_seq != inner.last_heartbeat {
            // new started or fetch but fail to apply manifest-service.
            let bucket_added = inner.buckets.iter().cloned().map(|b| cachepb::CacheEvent {
                typ: cachepb::cache_event::EventType::AddBucket.into(),
                bucket: b,
                blob: "".to_owned(),
            });

            let blob_added = inner.blobs.iter().cloned().map(|b| cachepb::CacheEvent {
                typ: EventType::AddBlob.into(),
                bucket: b.bucket,
                blob: b.blob,
            });

            let events = bucket_added.chain(blob_added).collect();

            cachepb::CacheEvents {
                mode: cache_events::EventMode::Full.into(),
                events,
            }
        } else {
            let mut events = Vec::new();
            for de in &inner.delta {
                match de {
                    Event::AddBucket { bucket } => events.push(cachepb::CacheEvent {
                        typ: cachepb::cache_event::EventType::AddBucket.into(),
                        bucket: bucket.to_owned(),
                        blob: "".to_owned(),
                    }),
                    Event::DeleteBucket { bucket } => events.push(cachepb::CacheEvent {
                        typ: cachepb::cache_event::EventType::DeleteBucket.into(),
                        bucket: bucket.to_owned(),
                        blob: "".to_owned(),
                    }),
                    Event::AddBlob { bucket, blob } => events.push(cachepb::CacheEvent {
                        typ: cachepb::cache_event::EventType::AddBlob.into(),
                        bucket: bucket.to_owned(),
                        blob: blob.to_owned(),
                    }),
                    Event::DeleteBlob { bucket, blob } => events.push(cachepb::CacheEvent {
                        typ: cachepb::cache_event::EventType::DeleteBlob.into(),
                        bucket: bucket.to_owned(),
                        blob: blob.to_owned(),
                    }),
                }
            }
            cachepb::CacheEvents {
                mode: cache_events::EventMode::Increment.into(),
                events,
            }
        };
        inner.last_heartbeat = current_seq;
        inner.delta = Vec::new();
        ev
    }
}
