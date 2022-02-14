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

use super::{cachepb, storage};
use crate::error::Result;

#[derive(Hash, PartialEq, Eq, Clone)]
struct BucketObject {
    bucket: String,
    object: String,
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
    objects: HashSet<BucketObject>,
    last_heartbeat: u64,
    delta: Vec<Event>,
}

enum Event {
    AddBucket { bucket: String },
    DeleteBucket { bucket: String },
    AddBlob { bucket: String, object: String },
    DeleteObject { bucket: String, object: String },
}

impl<S> CacheStatus<S>
where
    S: storage::CacheStorage,
{
    pub async fn new(store: Arc<S>) -> Result<Self> {
        let s = Self {
            inner: Arc::new(Mutex::new(Inner {
                buckets: HashSet::new(),
                objects: HashSet::new(),
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
        let mut objects = HashSet::new();
        for bucket in &inner.buckets {
            let objs = self.store.list_objects(&bucket).await?;
            for obj in objs {
                objects.insert(BucketObject {
                    bucket: bucket.to_owned(),
                    object: obj.to_owned(),
                });
            }
        }
        inner.objects = objects;
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

    pub async fn add_blob(&self, bucket: &str, object: &str) {
        let mut inner = self.inner.lock().await;
        assert!(inner.buckets.contains(bucket));
        inner.objects.insert(BucketObject {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
        });
        inner.delta.push(Event::AddBlob {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
        });
    }

    pub async fn delete_blob(&self, bucket: &str, object: &str) {
        let mut inner = self.inner.lock().await;
        assert!(inner.buckets.contains(bucket));
        let bo = BucketObject {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
        };
        inner.objects.remove(&bo);
        inner.delta.push(Event::DeleteObject {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
        });
    }

    pub async fn fetch_change_event(&self, last_ts: u64, current_ts: u64) -> cachepb::ObjectEvent {
        let mut inner = self.inner.lock().await;
        let ev = if last_ts != inner.last_heartbeat {
            // new started or fetch but fail to apply manifest-service.
            let bucket_added = inner.buckets.iter().cloned().collect::<Vec<String>>();

            let object_added = inner
                .objects
                .iter()
                .cloned()
                .map(|b| cachepb::BucketObject {
                    bucket: b.bucket.to_owned(),
                    object: b.object.to_owned(),
                })
                .collect::<Vec<cachepb::BucketObject>>();

            cachepb::ObjectEvent {
                typ: cachepb::object_event::EventType::Full.into(),
                bucket_added,
                bucket_delete: vec![],
                object_added,
                object_delete: vec![],
            }
        } else {
            let mut delat_ev = cachepb::ObjectEvent {
                typ: cachepb::object_event::EventType::Increment.into(),
                bucket_added: vec![],
                bucket_delete: vec![],
                object_added: vec![],
                object_delete: vec![],
            };
            for de in &inner.delta {
                match de {
                    Event::AddBucket { bucket } => delat_ev.bucket_added.push(bucket.to_owned()),
                    Event::DeleteBucket { bucket } => {
                        delat_ev.bucket_delete.push(bucket.to_owned())
                    }
                    Event::AddBlob { bucket, object } => {
                        delat_ev.object_added.push(cachepb::BucketObject {
                            bucket: bucket.to_owned(),
                            object: object.to_owned(),
                        })
                    }
                    Event::DeleteObject { bucket, object } => {
                        delat_ev.object_delete.push(cachepb::BucketObject {
                            bucket: bucket.to_owned(),
                            object: object.to_owned(),
                        })
                    }
                }
            }
            delat_ev
        };
        inner.last_heartbeat = current_ts;
        inner.delta = Vec::new();
        ev
    }
}
