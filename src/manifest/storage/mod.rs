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

use crate::error::Result;

mod memblob;

tonic::include_proto!("engula.storage.v1.manifest.storage");

#[async_trait]
trait MetaStorage {
    async fn append(&self, ve: VersionEdit) -> Result<()>;

    async fn read_all(&self) -> Result<Vec<VersionEdit>>;
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::{
        memblob::MemBlobMetaStore, BlobStats, DeleteBlob, MetaStorage, NewBlob, VersionEdit,
    };
    use crate::{blobstore::MockBlobStore, error::Result};

    #[tokio::test]
    async fn it_works() -> Result<()> {
        let bs = MockBlobStore::default();
        let s = MemBlobMetaStore::new(bs).await?;
        s.append(VersionEdit {
            add_buckets: vec!["b1".to_owned(), "b2".to_owned()],
            remove_buckets: vec!["b0".to_owned()],
            add_blobs: vec![NewBlob {
                bucket: "b1".to_owned(),
                blob: "o11".to_owned(),
                level: 0,
                stats: Some(BlobStats {
                    smallest: b"1".to_vec(),
                    largest: b"2".to_vec(),
                    smallest_sequence: 1,
                    largest_sequence: 2,
                    object_num: 2,
                    deletion_num: 0,
                }),
            }],
            delete_blobs: vec![DeleteBlob {
                bucket: "b1".to_owned(),
                blob: "o2".to_owned(),
                level: 0,
            }],
        })
        .await?;

        let vs = s.read_all().await?;
        assert_eq!(vs.len(), 1);

        Ok(())
    }
}
