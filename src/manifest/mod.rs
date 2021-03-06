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

mod blobctrl;
mod bucket;
mod locator;
mod placement;
mod reconcile;
mod status;
pub mod storage;
mod versions;

pub mod manifestpb {
    tonic::include_proto!("engula.storage.v1.manifest");
}

pub use blobctrl::BlobControl;
pub use bucket::BucketService;
pub use locator::CacheServerLocator;
pub use placement::{Placement, SpanBasedBlobPlacement};
pub use reconcile::Reconciler;
pub use status::{HeartbeatTask, ManifestStatus};
pub use versions::VersionSet;
