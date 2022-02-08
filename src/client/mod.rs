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

mod blob_writer;
mod client;

pub mod apipb {
    tonic::include_proto!("engula.storage.v1.client.api");
}

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::server::Connected;

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tonic::transport::{Channel, Endpoint, Server, Uri};

    use super::{
        apipb::{locator_client::LocatorClient, writer_client::WriterClient},
        blob_writer::BlobStoreWriter,
        MockStream,
    };
    use crate::client::{apipb, client::Client};

    #[tokio::test]
    async fn it_works() -> Result<()> {
        let blob_writer = build_local_blob_writer().await?;
        let manifest_locator = build_manifest_locator().await?;
        let mut client = Client::new(blob_writer, manifest_locator);
        client.flush("b1", "o1", b"abc".to_vec()).await?;
        let res = client.query(apipb::QueryExp {}).await?;
        assert_eq!(res.len(), 1);
        Ok(())
    }

    async fn build_local_blob_writer() -> Result<WriterClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let blob_writer = BlobStoreWriter {};
        tokio::spawn(async move {
            Server::builder()
                .add_service(apipb::writer_server::WriterServer::new(blob_writer))
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
        let client = apipb::writer_client::WriterClient::new(channel);
        Ok(client)
    }

    async fn build_manifest_locator() -> Result<LocatorClient<Channel>> {
        let (client, server) = tokio::io::duplex(1024);
        let locator = crate::manifest::CacheServerLocator {};
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

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
