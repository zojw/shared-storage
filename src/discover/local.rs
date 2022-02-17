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
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::Mutex,
};
use tonic::{
    body::BoxBody,
    codegen::http::{Request, Response},
    transport::{server::Connected, Body, Channel, Endpoint, NamedService, Server, Uri},
};
use tower::Service;

use super::{Discover, ServiceType, Svc};
use crate::{
    blobstore::MemBlobStore,
    cache::{
        cachepb::{
            bucket_service_server::BucketServiceServer as NodeBucketServiceServer,
            node_cache_manage_service_server::NodeCacheManageServiceServer,
        },
        CacheReader, CacheReplica, MemCacheStore, NodeBucketService, NodeCacheManager, Uploader,
    },
    client::apipb::{
        blob_upload_control_server::BlobUploadControlServer,
        blob_uploader_server::BlobUploaderServer, locator_server::LocatorServer,
        reader_server::ReaderServer,
    },
    error::Result,
    manifest::{
        manifestpb::bucket_service_server::BucketServiceServer as ManifestBucketServiceServer,
        storage::MemBlobMetaStore, BlobControl, BucketService as ManifestBucketService,
        CacheServerLocator,
    },
};

#[derive(Default)]
pub struct LocalSvcDiscover {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    node_cache_mng_svc: Vec<(
        u32,
        NodeCacheManageServiceServer<NodeCacheManager<MemCacheStore, MemBlobStore>>,
    )>,
    node_bucket_svc: Vec<(
        u32,
        NodeBucketServiceServer<NodeBucketService<MemCacheStore>>,
    )>,
    node_upload_svc: Vec<(
        u32,
        BlobUploaderServer<Uploader<MemCacheStore, MemBlobStore, CacheReplica<LocalSvcDiscover>>>,
    )>,
    node_read_svc: Vec<(u32, ReaderServer<CacheReader>)>,

    manifest_blob_ctrl: Vec<(
        u32,
        BlobUploadControlServer<BlobControl<MemBlobMetaStore<MemBlobStore>>>,
    )>,
    manifest_locator: Vec<(
        u32,
        LocatorServer<CacheServerLocator<MemBlobMetaStore<MemBlobStore>, LocalSvcDiscover>>,
    )>,
    manifest_bucket_svc: Vec<(
        u32,
        ManifestBucketServiceServer<
            ManifestBucketService<MemBlobStore, MemBlobMetaStore<MemBlobStore>, LocalSvcDiscover>,
        >,
    )>,
}

impl LocalSvcDiscover {
    pub async fn register_cache_node_svc(
        &self,
        srv_id: u32,
        cache_mng: NodeCacheManageServiceServer<NodeCacheManager<MemCacheStore, MemBlobStore>>,
        node_bucket: NodeBucketServiceServer<NodeBucketService<MemCacheStore>>,
        node_upload: BlobUploaderServer<
            Uploader<MemCacheStore, MemBlobStore, CacheReplica<LocalSvcDiscover>>,
        >,
        node_read: ReaderServer<CacheReader>,
    ) {
        let mut inner = self.inner.lock().await;
        inner.node_cache_mng_svc.push((srv_id, cache_mng));
        inner.node_bucket_svc.push((srv_id, node_bucket));
        inner.node_upload_svc.push((srv_id, node_upload));
        inner.node_read_svc.push((srv_id, node_read));
    }

    pub async fn register_manifest_svc(
        &self,
        srv_id: u32,
        blob_ctrl: BlobUploadControlServer<BlobControl<MemBlobMetaStore<MemBlobStore>>>,
        bucket_svc: ManifestBucketServiceServer<
            ManifestBucketService<MemBlobStore, MemBlobMetaStore<MemBlobStore>, LocalSvcDiscover>,
        >,
        locator: LocatorServer<
            CacheServerLocator<MemBlobMetaStore<MemBlobStore>, LocalSvcDiscover>,
        >,
    ) {
        let mut inner = self.inner.lock().await;
        inner.manifest_blob_ctrl.push((srv_id, blob_ctrl));
        inner.manifest_bucket_svc.push((srv_id, bucket_svc));
        inner.manifest_locator.push((srv_id, locator))
    }
}

#[async_trait]
impl Discover for LocalSvcDiscover {
    async fn list(&self, svc_type: ServiceType) -> Result<Vec<Svc>> {
        let inner = self.inner.lock().await;
        match svc_type {
            ServiceType::NodeCacheManageSvc => local_svc(&inner.node_cache_mng_svc).await,
            ServiceType::NodeBucketSvc => local_svc(&inner.node_bucket_svc).await,
            ServiceType::NodeUploadSvc => local_svc(&inner.node_upload_svc).await,
            ServiceType::NodeReadSvc => local_svc(&inner.node_read_svc).await,
            ServiceType::ManifestBlobCtrl => local_svc(&inner.manifest_blob_ctrl).await,
            ServiceType::ManifestBucketSvc => local_svc(&inner.manifest_bucket_svc).await,
            ServiceType::ManifestLocatorSvc => local_svc(&inner.manifest_locator).await,
        }
    }

    async fn find(&self, svc_type: ServiceType, srv_ids: Vec<u32>) -> Result<Vec<Svc>> {
        self.list(svc_type).await.map(|svc_in_type| {
            svc_in_type
                .iter()
                .cloned()
                .filter(|s| srv_ids.contains(&s.server_id))
                .take(srv_ids.len())
                .collect::<Vec<Svc>>()
        })
    }
}

async fn local_svc<S>(ss: &[(u32, S)]) -> Result<Vec<Svc>>
where
    S: Service<Request<Body>, Response = Response<BoxBody>> + NamedService + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    let mut results = Vec::new();
    for s in ss {
        let c = local_bridge(s.1.to_owned()).await?;
        results.push(Svc {
            server_id: s.0,
            channel: c.clone(),
        })
    }
    Ok(results)
}

async fn local_bridge<S>(svc: S) -> Result<Channel>
where
    S: Service<Request<Body>, Response = Response<BoxBody>> + NamedService + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    let (client, server) = tokio::io::duplex(1024);
    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                TeeStream(server),
            )]))
            .await
    });
    let mut client = Some(client);
    let channel = Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let client = client.take().unwrap();
            async move { Ok::<_, std::io::Error>(TeeStream(client)) }
        }))
        .await?;
    Ok(channel)
}

#[derive(Debug)]
struct TeeStream(pub tokio::io::DuplexStream);

impl Connected for TeeStream {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl AsyncRead for TeeStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TeeStream {
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

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
