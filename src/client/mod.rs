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
    use std::{
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::Duration,
    };

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tonic::transport::{server::Connected, Channel, Endpoint, Server, Uri};

    use super::apipb::{
        blob_upload_control_client::BlobUploadControlClient,
        blob_upload_control_server::BlobUploadControlServer,
        blob_uploader_client::BlobUploaderClient, blob_uploader_server::BlobUploaderServer,
        locator_client::LocatorClient, locator_server::LocatorServer, reader_client::ReaderClient,
    };
    use crate::{
        blobstore::MemBlobStore,
        cache::{
            cachepb::{
                bucket_service_client::BucketServiceClient as NodeBucketServiceClient,
                bucket_service_server::BucketServiceServer as NodeBucketServiceServer,
                node_cache_manage_service_client::NodeCacheManageServiceClient,
                node_cache_manage_service_server::NodeCacheManageServiceServer,
            },
            CacheStatus, MemCacheStore, NodeBucketService, NodeCacheManager, Uploader,
        },
        client::{apipb, cli::Client},
        discovery::{local::LocalSvcDiscovery, Discovery, ServiceType},
        error::Result,
        manifest::{
            manifestpb::{
                bucket_service_client::BucketServiceClient as ManifestBucketServiceClient,
                bucket_service_server::BucketServiceServer as ManifestBucketServiceServer,
            },
            storage::MemBlobMetaStore,
            BlobControl, BucketService, CacheServerLocator, HeartbeatTarget, ManifestStatus,
            VersionSet,
        },
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn it_works() -> Result<()> {
        // 0. blob_store access helper (shared lib)
        let blob_store = Arc::new(MemBlobStore::default());
        let discovery = LocalSvcDiscovery::default();

        // 1. cache node above blob_store(grpc service)
        // 1.1. setup cache-node-1 svc
        let cache_node1_id = {
            let local_store = Arc::new(MemCacheStore::default());
            let cache_status = Arc::new(CacheStatus::new(local_store.clone()).await?);
            let uploader: Uploader<MemCacheStore, MemBlobStore, MemCacheStore> = Uploader::new(
                local_store.clone(),
                blob_store.clone(),
                None,
                cache_status.clone(),
            );
            let upload_svc = BlobUploaderServer::new(uploader);
            let read_svc = apipb::reader_server::ReaderServer::new(crate::cache::CacheReader {});
            let node_bucket_svc = NodeBucketServiceServer::new(NodeBucketService::new(
                local_store.clone(),
                cache_status.clone(),
            ));
            let node_cache_mng_svc =
                NodeCacheManageServiceServer::new(NodeCacheManager::new(cache_status));

            let srv_id = 1;
            discovery
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
        let cache_node2_id = {
            let local_store = Arc::new(MemCacheStore::default());
            let cache_status = Arc::new(CacheStatus::new(local_store.clone()).await?);
            let uploader: Uploader<MemCacheStore, MemBlobStore, MemCacheStore> = Uploader::new(
                local_store.clone(),
                blob_store.clone(),
                None,
                cache_status.clone(),
            );
            let upload_svc = BlobUploaderServer::new(uploader);
            let read_svc = apipb::reader_server::ReaderServer::new(crate::cache::CacheReader {});
            let node_bucket_svc = NodeBucketServiceServer::new(NodeBucketService::new(
                local_store.clone(),
                cache_status.clone(),
            ));
            let node_cache_mng_svc =
                NodeCacheManageServiceServer::new(NodeCacheManager::new(cache_status));

            let srv_id = 2;
            discovery
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
        let (_, manifest_status) = {
            let meta_store = MemBlobMetaStore::new(blob_store.clone()).await?;
            let version_set = Arc::new(VersionSet::new(meta_store).await?);
            let blob_ctrl_svc = BlobUploadControlServer::new(BlobControl::new(version_set.clone()));

            let node_cache_svc = discovery
                .list(ServiceType::NodeCacheManageSvc)
                .await?
                .iter()
                .map(|s| HeartbeatTarget {
                    server_id: s.server_id,
                    invoker: NodeCacheManageServiceClient::new(s.channel.clone()),
                })
                .collect::<Vec<HeartbeatTarget>>();
            let manifest_status = build_and_run_manifest_status(node_cache_svc).await;

            let locator_svc = LocatorServer::new(CacheServerLocator::new(
                version_set.clone(),
                manifest_status.clone(),
            ));

            let bucket_in_caches = discovery
                .list(ServiceType::NodeBucketSvc)
                .await?
                .iter()
                .map(|s| NodeBucketServiceClient::new(s.channel.clone()))
                .collect::<Vec<NodeBucketServiceClient<Channel>>>();
            let cluster_bucket_svc = ManifestBucketServiceServer::new(BucketService::new(
                blob_store,
                version_set,
                bucket_in_caches,
            ));

            let srv_id = 3;
            discovery
                .register_manifest_svc(srv_id, blob_ctrl_svc, cluster_bucket_svc, locator_svc)
                .await;

            (srv_id, manifest_status)
        };

        // 3. client use mainifest & cache node(lib).
        {
            let (blob_ctrl_svc, manifest_locator_svc, manifest_bucket_svc) = (
                &discovery.list(ServiceType::ManifestBlobCtrl).await?[0],
                &discovery.list(ServiceType::ManifestLocatorSvc).await?[0],
                &discovery.list(ServiceType::ManifestBucketSvc).await?[0],
            );
            let blob_ctrl = BlobUploadControlClient::new(blob_ctrl_svc.channel.clone());
            let locator = LocatorClient::new(manifest_locator_svc.channel.clone());
            let manifest_bucket =
                ManifestBucketServiceClient::new(manifest_bucket_svc.channel.clone());

            let (upload_svc, read_svc) = (
                discovery
                    .list(ServiceType::NodeUploadSvc)
                    .await?
                    .iter()
                    .map(|s| BlobUploaderClient::new(s.channel.clone()))
                    .collect::<Vec<_>>(),
                discovery
                    .list(ServiceType::NodeReadSvc)
                    .await?
                    .iter()
                    .map(|s| ReaderClient::new(s.channel.clone()))
                    .collect::<Vec<_>>(),
            );
            let mut client = Client::new(blob_ctrl, upload_svc, locator, read_svc, manifest_bucket);

            // 4. simple test.
            client.create_bucket("b1").await?;
            client.flush("b1", "o2", b"abc".to_vec()).await?;
            let res = client.query(apipb::QueryExp {}).await?;
            assert_eq!(res.len(), 1);
        }

        manifest_status.clone().stop();
        Ok(())
    }

    async fn build_and_run_manifest_status(
        cache_nodes: Vec<HeartbeatTarget>,
    ) -> Arc<ManifestStatus> {
        let manifest_status =
            Arc::new(ManifestStatus::new(cache_nodes, Duration::from_millis(500)).await);
        {
            let status = manifest_status.clone();
            tokio::spawn(async move {
                let result = status.run().await;
                if result.is_err() {
                    //
                }
            });
        }
        manifest_status
    }

    use tonic::{
        body::BoxBody,
        codegen::http::{Request, Response},
        transport::{Body, NamedService},
    };
    use tower::Service;

    async fn local_bridge<S>(svc: S) -> Result<Channel>
    where
        S: Service<Request<Body>, Response = Response<BoxBody>>
            + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        let (client, server) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                    MockStream(server),
                )]))
                .await
        });
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://[::]:50051")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take().unwrap();
                async move { Ok::<_, std::io::Error>(MockStream(client)) }
            }))
            .await?;
        Ok(channel)
    }

    #[derive(Debug)]
    pub struct MockStream(pub tokio::io::DuplexStream);

    impl Connected for MockStream {
        type ConnectInfo = ();

        fn connect_info(&self) -> Self::ConnectInfo {}
    }

    impl AsyncRead for MockStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for MockStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
}
