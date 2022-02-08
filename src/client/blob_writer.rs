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

use async_trait::async_trait;
use tonic::{Request, Response, Status};

use crate::{
    blobstore::{BlobStore, MockBlobStore},
    client::apipb,
};

pub struct BlobStoreWriter {
    pub blob_store: MockBlobStore,
}

#[async_trait]
impl apipb::writer_server::Writer for BlobStoreWriter {
    async fn write(
        &self,
        request: Request<apipb::WriteRequest>,
    ) -> Result<Response<apipb::WriteResponse>, Status> {
        let inner = request.get_ref();
        self.blob_store
            .put_object(&inner.bucket, &inner.object, inner.content.to_owned())
            .await
            .unwrap(); // FIXME...
        Ok(Response::new(apipb::WriteResponse {}))
    }
}
