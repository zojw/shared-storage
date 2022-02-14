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
    collections::{hash_map, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use futures::future::poll_fn;
use tokio::sync::Mutex;
use tokio_util::time::{delay_queue::Expired, DelayQueue};
use tonic::{transport::Channel, Request};

use crate::{
    cache::cachepb::{
        cache_event::{self, EventType},
        cache_node_service_client::CacheNodeServiceClient,
        CacheEvent, Status,
    },
    error::Result,
};

pub struct ManifestStatus {
    cache_nodes: Vec<CacheNodeServiceClient<Channel>>,

    heartbeat_interval: Duration,
    delay_tasks: DelayQueue<HeartbeatTask>,

    inner: Arc<Mutex<Inner>>,
}

#[derive(Hash, PartialEq, Eq, Clone)]
struct BucketBlob {
    bucket: String,
    blob: String,
}

struct Inner {
    blob_loc: HashMap<BucketBlob, HashSet<u32>>, // {bucket + blob} -> server-id lists

    srv_blob: HashMap<u32, HashSet<BucketBlob>>, // {server-id} -> [{bucket, blob}..]
}

struct HeartbeatTask {
    target: CacheNodeServiceClient<Channel>,
    interval: Duration,
}

impl ManifestStatus {
    pub fn new(
        cache_nodes: Vec<CacheNodeServiceClient<Channel>>,
        heartbeat_interval: Duration,
    ) -> Self {
        let mut s = Self {
            cache_nodes: cache_nodes.to_owned(),
            heartbeat_interval,
            delay_tasks: DelayQueue::new(),
            inner: Arc::new(Mutex::new(Inner {
                blob_loc: HashMap::new(),
                srv_blob: HashMap::new(),
            })),
        };
        s.init(Duration::ZERO);
        s
    }

    fn init(&mut self, interval: Duration) {
        for n in &self.cache_nodes {
            self.delay_tasks.insert(
                HeartbeatTask {
                    target: n.to_owned(),
                    interval,
                },
                interval,
            );
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            match self.next().await {
                None => return Ok(()),
                Some(mut expired) => {
                    let heartbeat = expired.get_mut();
                    let status = Self::send_heartbeat(heartbeat).await?;
                    let ev = status.cache_event.unwrap();
                    match ev.typ {
                        _ if ev.typ == EventType::Full as i32 => {
                            self.apply_info(status.server_id, &ev, true).await?;
                        }
                        _ if ev.typ == EventType::Increment as i32 => {
                            self.apply_info(status.server_id, &ev, false).await?;
                        }
                        _ => unreachable!("unreachabler"),
                    }

                    self.delay_tasks.insert(
                        HeartbeatTask {
                            target: heartbeat.target.to_owned(),
                            interval: self.heartbeat_interval,
                        },
                        self.heartbeat_interval,
                    );
                }
            }
        }
    }

    async fn apply_info(&self, srv_id: u32, ev: &CacheEvent, full_mod: bool) -> Result<()> {
        let mut inner = self.inner.lock().await;

        if full_mod {
            let mut bs = Vec::new();
            if let Some(blobs_in_srv) = inner.srv_blob.get(&srv_id) {
                for b in blobs_in_srv {
                    bs.push(BucketBlob {
                        bucket: b.bucket.to_owned(),
                        blob: b.blob.to_owned(),
                    })
                }
            }
            for b in bs {
                inner.blob_loc.remove(&b);
            }
        }

        {
            match inner.srv_blob.entry(srv_id) {
                hash_map::Entry::Vacant(ent) => {
                    ent.insert(HashSet::new());
                }
                hash_map::Entry::Occupied(_) => {}
            }
            let blobs = inner.srv_blob.get_mut(&srv_id).unwrap();
            for b in &ev.blob_added {
                blobs.insert(BucketBlob {
                    bucket: b.bucket.to_owned(),
                    blob: b.blob.to_owned(),
                });
            }
            for b in &ev.blob_delete {
                blobs.remove(&BucketBlob {
                    bucket: b.bucket.to_owned(),
                    blob: b.blob.to_owned(),
                });
            }
        }

        for b in &ev.blob_added {
            let key = BucketBlob {
                bucket: b.bucket.to_owned(),
                blob: b.blob.to_owned(),
            };
            match inner.blob_loc.entry(key.to_owned()) {
                hash_map::Entry::Vacant(ent) => {
                    ent.insert(HashSet::new());
                }
                hash_map::Entry::Occupied(_) => {}
            };
            inner.blob_loc.get_mut(&key).unwrap().insert(srv_id);
        }

        for b in &ev.blob_delete {
            let key = BucketBlob {
                bucket: b.bucket.to_owned(),
                blob: b.blob.to_owned(),
            };
            if let Some(locs) = inner.blob_loc.get_mut(&key) {
                locs.remove(&srv_id);
            }
        }

        Ok(())
    }

    async fn next(&mut self) -> Option<Expired<HeartbeatTask>> {
        let n = poll_fn(|c| self.delay_tasks.poll_expired(c)).await?;
        Some(n)
    }

    async fn send_heartbeat(heartbeat: &mut HeartbeatTask) -> Result<Status> {
        let hb_req = Request::new(crate::cache::cachepb::HeartbeatRequest {
            server_id: 0, // TODO:.. assign at orch
            current_seq: 1,
            last_seq: 999999,
        });
        let res = heartbeat.target.heartbeat(hb_req).await?;
        let status = res.get_ref().to_owned().status.unwrap();
        Ok(status)
    }
}
