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
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tonic::transport::{channel, server::Connected, Channel, Endpoint, Server, Uri};

    use super::apipb::{
        blob_upload_control_client::BlobUploadControlClient,
        blob_uploader_client::BlobUploaderClient, locator_client::LocatorClient,
        reader_client::ReaderClient,
    };
    use crate::{
        blobstore::MemBlobStore,
        cache::{
            cachepb::bucket_service_client::BucketServiceClient as CacheBucketServiceClient,
            MemCacheStore, Uploader,
        },
        client::{apipb, client::Client},
        error::Result,
        manifest::{
            manifestpb::{
                bucket_service_client::BucketServiceClient,
                bucket_service_server::BucketServiceServer,
            },
            storage::MemBlobMetaStore,
            VersionSet,
        },
    };

    #[tokio::test]
    async fn it_works() -> Result<()> {
        let blob_store = Arc::new(MemBlobStore::default());
        let meta_store = MemBlobMetaStore::new(blob_store.clone()).await?;
        let local_store = Arc::new(MemCacheStore::default());
        let version_set = Arc::new(VersionSet::new(meta_store).await?);
        let blob_control = build_blob_control(version_set.clone()).await?;
        let manifest_locator = build_manifest_locator(version_set.clone()).await?;
        let cache_uploader = build_cache_uploader(local_store.clone(), blob_store.clone()).await?;
        let cache_reader = build_cache_reader().await?;
        let cache_bucket = build_cache_bucket_mng(local_store.clone()).await?;
        let manifest_bucket_mng =
            build_manifest_bucket_mng(blob_store.clone(), version_set.clone(), cache_bucket)
                .await?;

        let mut client = Client::new(
            blob_control,
            vec![cache_uploader],
            manifest_locator,
            vec![cache_reader],
            manifest_bucket_mng,
        );

        client.create_bucket("b1").await?;
        client.flush("b1", "o2", b"abc".to_vec()).await?;
        let res = client.query(apipb::QueryExp {}).await?;
        assert_eq!(res.len(), 1);
        Ok(())
    }

    async fn build_cache_uploader(
        local_store: Arc<MemCacheStore>,
        blob_store: Arc<MemBlobStore>,
    ) -> Result<BlobUploaderClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let uploader: Uploader<MemCacheStore, MemBlobStore, MemCacheStore> =
            Uploader::new(local_store, blob_store, None);
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
    ) -> Result<CacheBucketServiceClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let svc = crate::cache::CacheNodeBucketService::new(local);
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

    async fn build_manifest_locator(
        vs: Arc<VersionSet<MemBlobMetaStore<MemBlobStore>>>,
    ) -> Result<LocatorClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let locator = crate::manifest::CacheServerLocator::new(vs);
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
