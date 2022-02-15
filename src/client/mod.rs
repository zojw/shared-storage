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

mod client;

pub mod apipb {
    tonic::include_proto!("engula.storage.v1.client.api");
}

#[cfg(test)]
mod tests {
    use std::{
        borrow::BorrowMut,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::Duration,
    };

    use tokio::{
        io::{AsyncRead, AsyncWrite, ReadBuf},
        sync::Mutex,
    };
    use tonic::transport::{server::Connected, Channel, Endpoint, Server, Uri};

    use super::apipb::{
        blob_upload_control_client::BlobUploadControlClient,
        blob_uploader_client::BlobUploaderClient, locator_client::LocatorClient,
        reader_client::ReaderClient,
    };
    use crate::{
        blobstore::MemBlobStore,
        cache::{
            cachepb::{
                bucket_service_client::BucketServiceClient as CacheBucketServiceClient,
                cache_node_service_client::CacheNodeServiceClient,
            },
            CacheStatus, MemCacheStore, Uploader,
        },
        client::{apipb, client::Client},
        error::Result,
        manifest::{
            self,
            manifestpb::{
                bucket_service_client::BucketServiceClient,
                bucket_service_server::BucketServiceServer,
            },
            storage::MemBlobMetaStore,
            HeartbeatTarget, ManifestStatus, VersionSet,
        },
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn it_works() -> Result<()> {
        // 0. blob_store access helper (shared lib)
        let blob_store = Arc::new(MemBlobStore::default());

        // 1. cache node above blob_store(grpc service)
        let local_store = Arc::new(MemCacheStore::default());
        let cache_status = Arc::new(CacheStatus::new(local_store.clone()).await?);
        let cache_uploader = build_cache_uploader(
            local_store.clone(),
            blob_store.clone(),
            cache_status.clone(),
        )
        .await?;
        let cache_reader = build_cache_reader().await?;
        let cache_bucket =
            build_cache_bucket_mng(local_store.clone(), cache_status.clone()).await?;

        // 2. manifest server manage cache node and blob_store(grpc service)
        let meta_store = MemBlobMetaStore::new(blob_store.clone()).await?;
        let version_set = Arc::new(VersionSet::new(meta_store).await?);
        let blob_control = build_blob_control(version_set.clone()).await?;
        let cache_node = build_cache_node_mng(cache_status).await?;

        let manifest_status = build_and_run_manifest_status(vec![HeartbeatTarget {
            server_id: 1,
            invoker: cache_node,
        }])
        .await;
        let manifest_locator =
            build_manifest_locator(version_set.clone(), manifest_status.clone()).await?;
        let manifest_bucket =
            build_manifest_bucket_mng(blob_store.clone(), version_set.clone(), cache_bucket)
                .await?;

        // 3. client use mainifest & cache node(lib).
        let mut client = Client::new(
            blob_control,
            vec![cache_uploader],
            manifest_locator,
            vec![cache_reader],
            manifest_bucket,
        );

        // 4. simple test.
        client.create_bucket("b1").await?;
        client.flush("b1", "o2", b"abc".to_vec()).await?;
        let res = client.query(apipb::QueryExp {}).await?;
        assert_eq!(res.len(), 1);
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

    async fn build_cache_uploader(
        local_store: Arc<MemCacheStore>,
        blob_store: Arc<MemBlobStore>,
        cache_status: Arc<CacheStatus<MemCacheStore>>,
    ) -> Result<BlobUploaderClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let uploader: Uploader<MemCacheStore, MemBlobStore, MemCacheStore> =
            Uploader::new(local_store, blob_store, None, cache_status);
        tokio::spawn(async move {
            Server::builder()
                .add_service(apipb::blob_uploader_server::BlobUploaderServer::new(
                    uploader,
                ))
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
        let client = BlobUploaderClient::new(channel);
        Ok(client)
    }

    async fn build_cache_reader() -> Result<ReaderClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let reader = crate::cache::CacheReader {};
        tokio::spawn(async move {
            Server::builder()
                .add_service(apipb::reader_server::ReaderServer::new(reader))
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                    MockStream(server),
                )]))
                .await
        });
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://[::]:50053")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take().unwrap();
                async move { Ok::<_, std::io::Error>(MockStream(client)) }
            }))
            .await?;
        let client = apipb::reader_client::ReaderClient::new(channel);
        Ok(client)
    }

    async fn build_manifest_bucket_mng(
        blob_store: Arc<MemBlobStore>,
        version_set: Arc<VersionSet<MemBlobMetaStore<MemBlobStore>>>,
        cache_bucket_mng: CacheBucketServiceClient<Channel>,
    ) -> Result<BucketServiceClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let svc =
            crate::manifest::BucketService::new(blob_store, version_set, vec![cache_bucket_mng]);
        tokio::spawn(async move {
            Server::builder()
                .add_service(BucketServiceServer::new(svc))
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                    MockStream(server),
                )]))
                .await
        });
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://[::]:50055")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take().unwrap();
                async move { Ok::<_, std::io::Error>(MockStream(client)) }
            }))
            .await?;
        let client = BucketServiceClient::new(channel);
        Ok(client)
    }

    async fn build_cache_bucket_mng(
        local: Arc<MemCacheStore>,
        cache_status: Arc<CacheStatus<MemCacheStore>>,
    ) -> Result<CacheBucketServiceClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let svc = crate::cache::CacheNodeBucketService::new(local, cache_status);
        tokio::spawn(async move {
            Server::builder()
                .add_service(
                    crate::cache::cachepb::bucket_service_server::BucketServiceServer::new(svc),
                )
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                    MockStream(server),
                )]))
                .await
        });
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://[::]:50055")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take().unwrap();
                async move { Ok::<_, std::io::Error>(MockStream(client)) }
            }))
            .await?;
        let client = CacheBucketServiceClient::new(channel);
        Ok(client)
    }

    async fn build_cache_node_mng(
        status: Arc<CacheStatus<MemCacheStore>>,
    ) -> Result<CacheNodeServiceClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let svc = crate::cache::CacheNode::new(status);
        tokio::spawn(async move {
            Server::builder()
                .add_service(
                    crate::cache::cachepb::cache_node_service_server::CacheNodeServiceServer::new(
                        svc,
                    ),
                )
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                    MockStream(server),
                )]))
                .await
        });
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://[::]:50056")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take().unwrap();
                async move { Ok::<_, std::io::Error>(MockStream(client)) }
            }))
            .await?;
        let client = CacheNodeServiceClient::new(channel);
        Ok(client)
    }

    async fn build_manifest_locator(
        vs: Arc<VersionSet<MemBlobMetaStore<MemBlobStore>>>,
        manifest_status: Arc<ManifestStatus>,
    ) -> Result<LocatorClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let locator = crate::manifest::CacheServerLocator::new(vs, manifest_status);
        tokio::spawn(async move {
            Server::builder()
                .add_service(apipb::locator_server::LocatorServer::new(locator))
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                    MockStream(server),
                )]))
                .await
        });
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://[::]:50052")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take().unwrap();
                async move { Ok::<_, std::io::Error>(MockStream(client)) }
            }))
            .await?;
        let client = apipb::locator_client::LocatorClient::new(channel);
        Ok(client)
    }

    async fn build_blob_control(
        vs: Arc<VersionSet<MemBlobMetaStore<MemBlobStore>>>,
    ) -> Result<BlobUploadControlClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let cache_server = crate::manifest::BlobControl::new(vs);
        tokio::spawn(async move {
            Server::builder()
                .add_service(
                    apipb::blob_upload_control_server::BlobUploadControlServer::new(cache_server),
                )
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                    MockStream(server),
                )]))
                .await
        });
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://[::]:50052")?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take().unwrap();
                async move { Ok::<_, std::io::Error>(MockStream(client)) }
            }))
            .await?;
        let client = apipb::blob_upload_control_client::BlobUploadControlClient::new(channel);
        Ok(client)
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
