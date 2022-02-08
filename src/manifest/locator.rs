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

use crate::client::apipb::{self, KeyLocation, LocateRequest, LocateResponse};

pub struct CacheServerLocator {}

#[async_trait]
impl apipb::locator_server::Locator for CacheServerLocator {
    async fn locate_keys(
        &self,
        _request: Request<LocateRequest>,
    ) -> Result<Response<LocateResponse>, Status> {
        let locations = vec![KeyLocation {
            start: b"1".to_vec(),
            end: b"3".to_vec(),
            store: 1,
        }];
        Ok(Response::new(LocateResponse { locations }))
    }
}
