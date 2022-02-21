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

mod cli;

pub mod apipb {
    tonic::include_proto!("engula.storage.v1.client.api");
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::time::sleep;

    use super::apipb::{
        blob_upload_control_server::BlobUploadControlServer,
        blob_uploader_server::BlobUploaderServer, locator_server::LocatorServer,
    };
    use crate::{
        blobstore::MemBlobStore,
        cache::{
            cachepb::{
                bucket_service_server::BucketServiceServer as NodeBucketServiceServer,
                node_cache_manage_service_server::NodeCacheManageServiceServer,
            },
            CacheStatus, MemCacheStore, NodeBucketService, NodeCacheManager, Uploader,
        },
        client::{apipb, cli::Client},
        discover::{Discover, LocalSvcDiscover},
        error::Result,
        manifest::{
            manifestpb::bucket_service_server::BucketServiceServer as ManifestBucketServiceServer,
            storage::MemBlobMetaStore, BlobControl, BucketService, CacheServerLocator,
            ManifestStatus, Reconciler, SpanBasedBlobPlacement, VersionSet,
        },
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn it_works() -> Result<()> {
        // 0. blob_store access helper (shared lib)
        let blob_store = Arc::new(MemBlobStore::default());
        let discover = Arc::new(LocalSvcDiscover::default());

        // 1. cache node above blob_store(grpc service)
        // 1.1. setup cache-node-1 svc
        let _cache_node1_id = {
            let srv_id = 1;

            let local_store = Arc::new(MemCacheStore::default());
            let cache_status = Arc::new(CacheStatus::new(local_store.clone()).await?);
            // let cache_replica = Arc::new(CacheReplica::new(srv_id, discover.clone()));
            let uploader = Uploader::new(
                local_store.clone(),
                blob_store.clone(),
                // Some(cache_replica),
                None,
                cache_status.clone(),
            );
            let upload_svc = BlobUploaderServer::new(uploader);
            let read_svc = apipb::reader_server::ReaderServer::new(crate::cache::CacheReader {});
            let node_bucket_svc = NodeBucketServiceServer::new(NodeBucketService::new(
                local_store.clone(),
                cache_status.clone(),
            ));
            let node_cache_mng_svc = NodeCacheManageServiceServer::new(NodeCacheManager::new(
                cache_status,
                blob_store.clone(),
                local_store.clone(),
            ));

            discover
                .register_cache_node_svc(
                    srv_id,
                    node_cache_mng_svc,
                    node_bucket_svc,
                    upload_svc,
                    read_svc,
                )
                .await;

            srv_id
        };

        // 1.2. setup cache node-2 svc
        let _cache_node2_id = {
            let srv_id = 2;

            let local_store = Arc::new(MemCacheStore::default());
            let cache_status = Arc::new(CacheStatus::new(local_store.clone()).await?);
            // let cache_replica = Arc::new(CacheReplica::new(srv_id, discover.clone()));
            let uploader = Uploader::new(
                local_store.clone(),
                blob_store.clone(),
                // Some(cache_replica),
                None,
                cache_status.clone(),
            );
            let upload_svc = BlobUploaderServer::new(uploader);
            let read_svc = apipb::reader_server::ReaderServer::new(crate::cache::CacheReader {});
            let node_bucket_svc = NodeBucketServiceServer::new(NodeBucketService::new(
                local_store.clone(),
                cache_status.clone(),
            ));
            let node_cache_mng_svc = NodeCacheManageServiceServer::new(NodeCacheManager::new(
                cache_status,
                blob_store.clone(),
                local_store.clone(),
            ));

            discover
                .register_cache_node_svc(
                    srv_id,
                    node_cache_mng_svc,
                    node_bucket_svc,
                    upload_svc,
                    read_svc,
                )
                .await;

            srv_id
        };

        // 2. manifest server manage cache node and blob_store(grpc service)
        let (_, manifest_status, reconcile) = {
            let manifest_status = build_and_run_manifest_status(discover.clone()).await?;

            let meta_store = MemBlobMetaStore::new(blob_store.clone()).await?;
            let version_set = Arc::new(VersionSet::new(meta_store).await?);
            let reconciler = Arc::new(Reconciler::new(
                discover.clone(),
                version_set.clone(),
                manifest_status.clone(),
            ));
            let placement = Arc::new(
                SpanBasedBlobPlacement::new(version_set.clone(), reconciler.clone()).await,
            );
            let blob_ctrl_svc = BlobUploadControlServer::new(BlobControl::new(
                version_set.clone(),
                placement.clone(),
            ));

            let reconcile = build_and_run_reconciler(
                discover.clone(),
                version_set.clone(),
                manifest_status.clone(),
            )
            .await?;

            let locator_svc = LocatorServer::new(CacheServerLocator::new(
                version_set.clone(),
                manifest_status.clone(),
            ));

            let cluster_bucket_svc = ManifestBucketServiceServer::new(BucketService::new(
                blob_store,
                version_set,
                discover.clone(),
            ));

            let srv_id = 3;
            discover
                .register_manifest_svc(srv_id, blob_ctrl_svc, cluster_bucket_svc, locator_svc)
                .await;

            (srv_id, manifest_status, reconcile)
        };

        // 3. client use mainifest & cache node(lib).
        {
            let mut client = Client::new(discover.clone());

            // 4. simple test.
            client.create_bucket("b1").await?;
            client.flush("b1", "o2", b"abc".to_vec(), 2).await?;

            // simple sleep wait manifest-cache node heartbeat before read
            // TODO: this can avoid in future.
            sleep(Duration::from_millis(2000)).await;

            let res = client.query(apipb::QueryExp {}).await?;
            assert_eq!(res.len(), 1);
        }

        manifest_status.clone().stop();
        reconcile.clone().stop();
        Ok(())
    }

    async fn build_and_run_manifest_status<D>(discover: Arc<D>) -> Result<Arc<ManifestStatus<D>>>
    where
        D: Discover + Sync + Send + 'static,
    {
        let manifest_status =
            Arc::new(ManifestStatus::new(discover, Duration::from_millis(500)).await?);
        {
            let status = manifest_status.clone();
            tokio::spawn(async move {
                let result = status.run().await;
                if result.is_err() {
                    //
                }
            });
        }
        Ok(manifest_status)
    }

    async fn build_and_run_reconciler(
        discover: Arc<LocalSvcDiscover>,
        version_set: Arc<VersionSet<MemBlobMetaStore<MemBlobStore>>>,
        manifest_status: Arc<ManifestStatus<LocalSvcDiscover>>,
    ) -> Result<Arc<Reconciler<LocalSvcDiscover, MemBlobMetaStore<MemBlobStore>>>> {
        let reconcile = Arc::new(Reconciler::new(discover, version_set, manifest_status));
        {
            let r = reconcile.clone();
            tokio::spawn(async move {
                let result = r.run().await;
                if result.is_err() {
                    //
                }
            });
        }
        Ok(reconcile)
    }
}
