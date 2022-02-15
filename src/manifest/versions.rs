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
    collections::{btree_map, hash_map, BTreeMap, HashMap},
    sync::Arc,
};

use tokio::{sync::Mutex, time::Instant};

use super::{storage::MetaStorage, BlobControl};
use crate::{
    client::apipb::{KeyRange, Location},
    error::Result,
    manifest::storage::{StagingOperation, VersionEdit},
};

#[derive(Clone)]
pub struct BlobDesc {
    pub bucket: String,
    pub blob: String,
    pub level: u32,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

#[derive(Clone)]
pub struct StageDesc {
    deadline: Instant,
    locations: Vec<String>,
}

#[derive(Clone)]
pub struct Version {
    // bucket -> { blob -> blob-desc }
    buckets: HashMap<String, HashMap<String, BlobDesc>>,
    // bucket -> { level -> { smallest-key -> blob-desc } }
    levels: HashMap<String, BTreeMap<u32, BTreeMap<Vec<u8>, BlobDesc>>>,
    // token -> stage-operation
    staging_op: BTreeMap<String, StagingOperation>,
}

impl Default for Version {
    fn default() -> Self {
        Self {
            buckets: HashMap::new(),
            levels: HashMap::new(),
            staging_op: BTreeMap::new(),
        }
    }
}

impl Version {
    pub fn list_buckets(&self) -> Vec<String> {
        self.buckets
            .iter()
            .map(|(bucket, _)| bucket.to_owned())
            .collect::<Vec<String>>()
    }

    pub fn list_blobs(&self, bucket: &str) -> Vec<String> {
        match self.buckets.get(bucket) {
            Some(blobs) => blobs
                .iter()
                .map(|(blob, _)| blob.to_owned())
                .collect::<Vec<String>>(),
            None => vec![],
        }
    }

    pub fn get_location(&self, ranges: Vec<KeyRange>) -> Vec<Location> {
        use std::ops::Bound::{Excluded, Included};
        // TODO: find location between levels.
        let mut locs = Vec::new();
        for ran in ranges {
            let levels = self.levels.get(&ran.bucket).unwrap();
            for (level, blobs) in levels {
                for (_, blob) in
                    blobs.range((Included(ran.start.to_owned()), Excluded(ran.end.to_owned())))
                {
                    locs.push(Location {
                        bucket: ran.bucket.to_owned(),
                        blob: blob.blob.to_owned(),
                        range: Some(KeyRange {
                            bucket: ran.bucket.to_owned(),
                            start: ran.start.to_owned(),
                            end: ran.end.to_owned(),
                        }),
                        stores: vec![1], // TODO: collect store info by hearbeat.
                        level: blob.level,
                    });
                    if *level != 0 && ran.end.to_owned() <= blob.largest {
                        break;
                    }
                }
            }
        }
        locs
    }

    pub fn get_blob(&self, bucket: &str, blob: &str) -> Option<BlobDesc> {
        if let Some(blobs) = self.buckets.get(bucket) {
            if let Some(blob) = blobs.get(blob) {
                return Some(blob.to_owned());
            }
        }
        None
    }

    pub fn get_stage(&self, token: &str) -> Option<StagingOperation> {
        self.staging_op.get(token).map(|t| t.to_owned())
    }

    fn apply(&self, ve: VersionEdit) -> Arc<Version> {
        let mut n = self.clone();

        for add_bucket in ve.add_buckets {
            match n.buckets.entry(add_bucket.to_owned()) {
                hash_map::Entry::Vacant(ent) => {
                    ent.insert(HashMap::new());
                }
                hash_map::Entry::Occupied(_) => {}
            }
            match n.levels.entry(add_bucket.to_owned()) {
                hash_map::Entry::Vacant(ent) => {
                    ent.insert(BTreeMap::new());
                }
                hash_map::Entry::Occupied(_) => {}
            }
        }
        for del_bucket in ve.remove_buckets {
            n.buckets.remove(&del_bucket);
            n.levels.remove(&del_bucket);
        }

        for new_blob in ve.add_blobs {
            if let Some(buckets) = n.buckets.get_mut(&new_blob.bucket) {
                let (smallest, largest) = {
                    match &new_blob.stats {
                        Some(stats) => (stats.smallest.to_owned(), stats.largest.to_owned()),
                        None => (vec![], vec![]),
                    }
                };
                buckets.insert(
                    new_blob.blob.to_owned(),
                    BlobDesc {
                        bucket: new_blob.bucket.to_owned(),
                        blob: new_blob.blob.to_owned(),
                        level: new_blob.level,
                        smallest,
                        largest,
                    },
                );
            }
            if let Some(levels) = n.levels.get_mut(&new_blob.bucket) {
                match levels.entry(new_blob.level) {
                    btree_map::Entry::Vacant(ent) => {
                        ent.insert(BTreeMap::new());
                    }
                    btree_map::Entry::Occupied(_) => {}
                }
                let blobs_in_level = levels.get_mut(&new_blob.level).unwrap();
                if let Some(stats) = new_blob.stats {
                    blobs_in_level.insert(
                        stats.smallest.to_owned(),
                        BlobDesc {
                            bucket: new_blob.bucket.to_owned(),
                            blob: new_blob.blob.to_owned(),
                            level: new_blob.level,
                            smallest: stats.smallest,
                            largest: stats.largest,
                        },
                    );
                }
            }
        }
        for del_blob in ve.remove_blobs {
            if let Some(buckets) = n.buckets.get_mut(&del_blob.bucket) {
                buckets.remove(&del_blob.blob);
            }
            if let Some(levels) = n.levels.get_mut(&del_blob.bucket) {
                if let Some(blobs_in_level) = levels.get_mut(&del_blob.level) {
                    blobs_in_level.remove(&del_blob.smallest);
                }
            }
        }

        for stg in ve.add_staging {
            n.staging_op.insert(stg.token.to_owned(), stg);
        }
        for token in ve.remove_staging {
            n.staging_op.remove(&token);
        }

        Arc::new(n)
    }
}

pub struct VersionSet<S>
where
    S: MetaStorage,
{
    versions: Arc<Mutex<Vec<Arc<Version>>>>,
    meta_store: S,
}

impl<S> VersionSet<S>
where
    S: MetaStorage,
{
    pub async fn new(meta_store: S) -> Result<Self> {
        let s = Self {
            versions: Arc::new(Mutex::new(Vec::new())),
            meta_store,
        };
        s.recovery().await?;
        Ok(s)
    }

    pub async fn recovery(&self) -> Result<()> {
        let versions = self.versions.clone();
        let mut vs = versions.lock().await;

        let ves = self.meta_store.read_all().await?;
        let mut new_version = Arc::new(Version::default());
        if !ves.is_empty() {
            for ve in &ves {
                new_version = new_version.apply(ve.clone());
                vs.push(Arc::new(new_version.as_ref().clone()))
            }
        } else {
            vs.push(Arc::new(new_version.as_ref().clone()))
        }

        Ok(())
    }

    pub async fn log_and_apply(&self, ves: Vec<VersionEdit>) -> Result<Arc<Version>> {
        let versions = self.versions.clone();
        let mut vs = versions.lock().await;

        // TODO: leader + mutex to ensure only one appender?
        for ve in &ves {
            self.meta_store.append(ve.clone()).await?;
        }

        let current = &vs[vs.len() - 1];
        let mut new_version = current.clone();
        for ve in &ves {
            new_version = new_version.apply(ve.clone());
            vs.push(Arc::new(new_version.as_ref().clone()))
        }

        return Ok(new_version);
    }

    pub async fn current_version(&self) -> Arc<Version> {
        let versions = self.versions.clone();
        let vs = versions.lock().await;
        let v = &vs[vs.len() - 1];
        v.clone()
    }

    async fn append(&self, v: Version) -> Arc<Version> {
        let v = Arc::new(v);
        let versions = self.versions.clone();
        let mut vs = versions.lock().await;
        vs.push(v.clone());
        v
    }
}
