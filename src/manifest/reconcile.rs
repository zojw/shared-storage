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
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc,
    },
    time::Duration,
};

use tokio::time::sleep;
use tonic::Request;

use super::{storage::MetaStorage, ManifestStatus, VersionSet};
use crate::{
    cache::cachepb::{
        node_cache_manage_service_client::NodeCacheManageServiceClient, RefillCacheRequest,
        RemoveCacheRequest,
    },
    discover::{Discover, ServiceType},
    error::Result,
    manifest::status::BucketBlob,
};

pub struct Reconciler<D, S>
where
    D: Discover,
    S: MetaStorage,
{
    discover: Arc<D>,
    version_set: Arc<VersionSet<S>>,
    status: Arc<ManifestStatus<D>>,

    stop: AtomicBool,
}

impl<D, S> Reconciler<D, S>
where
    D: Discover + Send + Sync + 'static,
    S: MetaStorage + Sync + Send + 'static,
{
    pub async fn reconcile_blob(&self, bucket: &str, blob: &str) -> Result<()> {
        let ver = self.version_set.current_version().await;

        let desired_replica_count = ver.get_blob(bucket, blob).unwrap_or_default().replica_count;
        let current_replicas = self
            .status
            .get_bucket_replica(bucket, blob)
            .await
            .unwrap_or_default();

        if desired_replica_count == current_replicas.len() as u32 {
            return Ok(());
        }

        let all_cache_srvs = self
            .discover
            .list(ServiceType::NodeCacheManageSvc)
            .await?
            .iter()
            .map(|s| s.server_id)
            .collect::<Vec<u32>>();
        let mut other_srvs = all_cache_srvs.clone();
        other_srvs.retain(|s| !current_replicas.contains(s));

        if desired_replica_count > current_replicas.len() as u32 {
            let add_srvs = Self::sort_server_by_add_score(other_srvs);
            let add_srvs = (&add_srvs
                [..(desired_replica_count as usize - current_replicas.len() as usize)])
                .to_owned();
            self.refill_cache_on_srvs(bucket, blob, &add_srvs).await?;
            return Ok(());
        }

        let remove_srvs = Self::sort_server_by_remove_score(other_srvs);
        let remove_srvs = (&remove_srvs
            [..(current_replicas.len() as usize - desired_replica_count as usize)])
            .to_owned();
        self.remove_cache_on_srvs(bucket, blob, &remove_srvs)
            .await?;
        Ok(())
    }
}

impl<D, S> Reconciler<D, S>
where
    D: Discover + Send + Sync + 'static,
    S: MetaStorage + Sync + Send + 'static,
{
    pub fn new(
        discover: Arc<D>,
        version_set: Arc<VersionSet<S>>,
        status: Arc<ManifestStatus<D>>,
    ) -> Self {
        Self {
            discover,
            version_set,
            status,
            stop: AtomicBool::new(false),
        }
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            if self.stop.load(Relaxed) {
                break;
            }
            sleep(Duration::from_millis(800)).await;
            if self.stop.load(Relaxed) {
                break;
            }
            self.try_reconcile().await?
        }
        Ok(())
    }

    async fn try_reconcile(&self) -> Result<()> {
        let ver = self.version_set.current_version().await;
        let desired_blobs = ver.list_all_blobs();
        let mut current_blobs = self.status.get_bucket_replca_view().await;

        let mut ops = Vec::new();
        for desired_blob in desired_blobs {
            let desired_replica = desired_blob.replica_count;
            let current_key = BucketBlob {
                bucket: desired_blob.bucket.to_owned(),
                blob: desired_blob.blob.to_owned(),
            };
            let empty = HashSet::new();
            let current_replica = current_blobs.get(&current_key).unwrap_or(&empty);
            if desired_replica == current_replica.len() as u32 {
                current_blobs.remove(&current_key);
                continue;
            }
            if desired_replica > current_replica.len() as u32 {
                ops.push(ReconcileTask {
                    bucket: desired_blob.bucket.to_owned(),
                    blob: desired_blob.blob.to_owned(),
                    count: desired_replica as i32 - current_replica.len() as i32,
                    srv_id: current_replica.iter().cloned().collect::<Vec<u32>>(),
                })
            } else {
                ops.push(ReconcileTask {
                    bucket: desired_blob.bucket.to_owned(),
                    blob: desired_blob.blob.to_owned(),
                    count: current_replica.len() as i32 - desired_replica as i32,
                    srv_id: current_replica.iter().cloned().collect::<Vec<u32>>(),
                })
            }
            current_blobs.remove(&current_key);
        }
        for (obsoleted, replica) in current_blobs {
            ops.push(ReconcileTask {
                bucket: obsoleted.bucket.to_owned(),
                blob: obsoleted.blob.to_owned(),
                count: -(replica.len() as i32),
                srv_id: replica.iter().cloned().collect::<Vec<u32>>(),
            })
        }

        let all_cache_srvs = self
            .discover
            .list(ServiceType::NodeCacheManageSvc)
            .await?
            .iter()
            .map(|s| s.server_id)
            .collect::<Vec<u32>>();

        let mut op_srvs = Vec::new();
        for op in ops {
            let srvs = if op.count > 0 {
                let mut other_srvs = all_cache_srvs.clone();
                other_srvs.retain(|s| !op.srv_id.contains(s));
                let srvs = Self::sort_server_by_add_score(op.srv_id.to_owned());
                (&srvs[..(op.count as usize)]).to_owned()
            } else {
                let srvs = Self::sort_server_by_remove_score(op.srv_id.to_owned());
                (&srvs[..((-op.count) as usize)]).to_owned()
            };
            op_srvs.push((op, srvs))
        }

        for (task, change_srvs) in op_srvs {
            if task.count < 0 {
                self.remove_cache_on_srvs(&task.bucket, &task.blob, &change_srvs)
                    .await?;
            } else {
                self.refill_cache_on_srvs(&task.bucket, &task.blob, &change_srvs)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn refill_cache_on_srvs(&self, bucket: &str, blob: &str, srvs: &[u32]) -> Result<()> {
        let ss = self
            .discover
            .find(ServiceType::NodeCacheManageSvc, srvs.to_owned())
            .await?;
        for svc in ss {
            let mut client = NodeCacheManageServiceClient::new(svc.channel.clone());
            let req = Request::new(RefillCacheRequest {
                bucket: bucket.to_owned(),
                blob: blob.to_owned(),
            });
            client.refill_cache(req).await?;
        }
        Ok(())
    }

    async fn remove_cache_on_srvs(&self, bucket: &str, blob: &str, srvs: &[u32]) -> Result<()> {
        let ss = self
            .discover
            .find(ServiceType::NodeCacheManageSvc, srvs.to_owned())
            .await?;
        for svc in ss {
            let mut client = NodeCacheManageServiceClient::new(svc.channel.clone());
            let req = Request::new(RemoveCacheRequest {
                bucket: bucket.to_owned(),
                blob: blob.to_owned(),
            });
            client.remove_cache(req).await?;
        }
        Ok(())
    }

    fn sort_server_by_remove_score(current_srvs: Vec<u32>) -> Vec<u32> {
        // TODO: do real score & sort
        current_srvs
    }

    fn sort_server_by_add_score(other_srvs: Vec<u32>) -> Vec<u32> {
        // TODO: do real score & sort
        other_srvs
    }

    pub fn stop(&self) {
        self.stop.store(true, Relaxed)
    }
}

struct ReconcileTask {
    bucket: String,
    blob: String,
    count: i32,
    srv_id: Vec<u32>,
}
