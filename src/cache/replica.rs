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

use crate::{cache::storage::PutOptions, error::Result};

struct CacheReplica {
    current_srv: u32,
}

impl CacheReplica {
    pub fn new(current_srv: u32) -> Self {
        Self { current_srv }
    }
}

#[async_trait]
impl super::ObjectPutter for CacheReplica {
    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        content: Vec<u8>,
        opt: Option<PutOptions>,
    ) -> Result<()> {
        let mut replica = opt.unwrap().replica_srv.to_owned();
        replica.retain(|e| *e != self.current_srv);

        Ok(())
    }
}
