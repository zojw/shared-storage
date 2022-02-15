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
        cache_event::EventType, cache_events::EventMode,
        cache_node_service_client::CacheNodeServiceClient, CacheEvent, HeartbeatRequest,
        HeartbeatResponse,
    },
    error::Result,
};

pub struct ManifestStatus {
    heartbeat_interval: Duration,
    delay_tasks: DelayQueue<HeartbeatTask>,

    cache_nodes: Vec<HeartbeatTarget>,

    inner: Arc<Mutex<Inner>>,
}

#[derive(Clone)]
pub struct HeartbeatTarget {
    server_id: u32,
    invoker: CacheNodeServiceClient<Channel>,
}

#[derive(Hash, PartialEq, Eq, Clone)]
struct BucketBlob {
    bucket: String,
    blob: String,
}

struct Inner {
    seq: u64,
    blob_loc: HashMap<String, HashMap<String, HashSet<u32>>>, // {bucket + blob} -> server-id lists
    srv_blob: HashMap<u32, HashSet<BucketBlob>>,              // {server-id} -> [{bucket, blob}..]
    srv_bucket: HashMap<u32, HashSet<String>>,                // {server-id} -> [server-id, ...]
}

struct HeartbeatTask {
    target: HeartbeatTarget,
    interval: Duration,
}

impl ManifestStatus {
    pub fn new(cache_nodes: Vec<HeartbeatTarget>, heartbeat_interval: Duration) -> Self {
        let mut s = Self {
            cache_nodes: cache_nodes.to_owned(),
            heartbeat_interval,
            delay_tasks: DelayQueue::new(),
            inner: Arc::new(Mutex::new(Inner {
                seq: 0,
                blob_loc: HashMap::new(),
                srv_blob: HashMap::new(),
                srv_bucket: HashMap::new(),
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
            match self.next_task().await {
                None => return Ok(()),
                Some(mut expired) => {
                    let heartbeat = expired.get_mut();
                    let last_seq = self.current_seq().await;
                    let current_seq = last_seq + 1;
                    let resp = Self::send_heartbeat(heartbeat, last_seq, current_seq).await?;
                    let status = resp.status.unwrap();
                    let ev = status.events.unwrap();
                    match ev.mode {
                        _ if ev.mode == EventMode::Full as i32 => {
                            self.apply(status.server_id, ev.events, true, last_seq)
                                .await?;
                        }
                        _ if ev.mode == EventMode::Increment as i32 => {
                            self.apply(status.server_id, ev.events, false, last_seq)
                                .await?;
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

    async fn current_seq(&self) -> u64 {
        let inner = self.inner.lock().await;
        inner.seq
    }

    async fn apply(
        &self,
        srv_id: u32,
        events: Vec<CacheEvent>,
        full_mod: bool,
        base_seq: u64,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;

        if inner.seq != base_seq {
            // local cache has be changed by others ignore current change.
            return Ok(());
        }

        if full_mod {
            // cleanup exist info related to the current server before relay.
            let mut wait_clean_buckets = Vec::new();
            if let Some(buckets_in_srv) = inner.srv_bucket.get(&srv_id) {
                for b in buckets_in_srv {
                    wait_clean_buckets.push(b.to_owned())
                }
            }
            for b in wait_clean_buckets {
                inner.blob_loc.remove(&b);
            }

            let mut wait_clean_blobs = Vec::new();
            if let Some(blobs_in_srv) = inner.srv_blob.get(&srv_id) {
                for b in blobs_in_srv {
                    wait_clean_blobs.push(BucketBlob {
                        bucket: b.bucket.to_owned(),
                        blob: b.blob.to_owned(),
                    })
                }
            }
            for b in wait_clean_blobs {
                if let Some(blobs) = inner.blob_loc.get_mut(&b.bucket) {
                    blobs.remove(&b.blob);
                }
            }
        }

        match inner.srv_bucket.entry(srv_id) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(HashSet::new());
            }
            hash_map::Entry::Occupied(_) => {}
        }
        match inner.srv_blob.entry(srv_id) {
            hash_map::Entry::Vacant(ent) => {
                ent.insert(HashSet::new());
            }
            hash_map::Entry::Occupied(_) => {}
        }
        for ev in events {
            match ev.typ {
                _ if ev.typ == EventType::AddBucket as i32 => {
                    let buckets = inner.srv_bucket.get_mut(&srv_id).unwrap();
                    buckets.insert(ev.bucket.to_owned());

                    match inner.blob_loc.entry(ev.bucket.to_owned()) {
                        hash_map::Entry::Vacant(ent) => {
                            ent.insert(HashMap::new());
                        }
                        hash_map::Entry::Occupied(_) => {}
                    }
                }
                _ if ev.typ == EventType::DeleteBucket as i32 => {
                    let buckets = inner.srv_bucket.get_mut(&srv_id).unwrap();
                    buckets.remove(&ev.bucket);

                    inner.blob_loc.remove(&ev.bucket);
                }
                _ if ev.typ == EventType::AddBlob as i32 => {
                    let blobs = inner.srv_blob.get_mut(&srv_id).unwrap();
                    blobs.insert(BucketBlob {
                        bucket: ev.bucket.to_owned(),
                        blob: ev.blob.to_owned(),
                    });

                    match inner.blob_loc.entry(ev.bucket.to_owned()) {
                        hash_map::Entry::Vacant(ent) => {
                            ent.insert(HashMap::new());
                        }
                        hash_map::Entry::Occupied(_) => {}
                    };
                    let blobs = inner.blob_loc.get_mut(&ev.bucket).unwrap();
                    match blobs.entry(ev.blob.to_owned()) {
                        hash_map::Entry::Occupied(mut ent) => {
                            ent.insert(HashSet::new());
                        }
                        hash_map::Entry::Vacant(_) => {}
                    };
                    blobs.get_mut(&ev.blob).unwrap().insert(srv_id);
                }
                _ if ev.typ == EventType::DeleteBlob as i32 => {
                    let blobs = inner.srv_blob.get_mut(&srv_id).unwrap();
                    blobs.remove(&BucketBlob {
                        bucket: ev.bucket.to_owned(),
                        blob: ev.blob.to_owned(),
                    });

                    if let Some(blobs) = inner.blob_loc.get_mut(&ev.bucket) {
                        if let Some(srvs) = blobs.get_mut(&ev.blob) {
                            srvs.remove(&srv_id);
                        }
                    }
                }
                _ => unreachable!(""),
            }
        }

        Ok(())
    }

    async fn next_task(&mut self) -> Option<Expired<HeartbeatTask>> {
        Some(poll_fn(|c| self.delay_tasks.poll_expired(c)).await?)
    }

    async fn send_heartbeat(
        heartbeat: &mut HeartbeatTask,
        current_seq: u64,
        last_seq: u64,
    ) -> Result<HeartbeatResponse> {
        let hb_req = Request::new(HeartbeatRequest {
            server_id: heartbeat.target.server_id,
            current_seq,
            last_seq,
        });
        let res = heartbeat.target.invoker.heartbeat(hb_req).await?;
        Ok(res.get_ref().to_owned())
    }
}
