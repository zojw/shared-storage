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
    borrow::BorrowMut,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use async_trait::async_trait;
use tokio::sync::{Mutex, MutexGuard};

use super::{storage::MetaStorage, versions::Version, Reconciler, VersionSet};
use crate::{discover::Discover, error::Result, manifest::storage};

// TODO: it's fake value
const MAX_SPAN_SIZE: u64 = 10;

struct Span {
    id: u64,
    start: Vec<u8>,
    end: Vec<u8>,
    blobs: BTreeMap<Vec<u8>, SpanBlob>,
    size: u64,
    objects: u64,
}

struct SpanBlob {
    bucket: String,
    blob: String,
    span_id: u64,
    start: Vec<u8>,
    end: Vec<u8>,
    size: u64,
    object: u64,
}

#[async_trait]
pub trait Placement {
    async fn add_new_blob(&self, blob: storage::NewBlob) -> Result<storage::NewBlob>;
}

pub struct SpanBasedBlobPlacement<D, S>
where
    D: Discover,
    S: MetaStorage,
{
    inner: Arc<Mutex<Inner<S>>>,
    reconciler: Arc<Reconciler<D, S>>,
}

struct Inner<S>
where
    S: MetaStorage,
{
    version_set: Arc<VersionSet<S>>,
    key_spans: BTreeMap<Vec<u8>, Arc<Mutex<Span>>>,
    id_spans: HashMap<u64, Arc<Mutex<Span>>>,
    max_span_id: u64,
    smallest_key: Vec<u8>,
    biggest_key: Vec<u8>,
}

impl<D, S> SpanBasedBlobPlacement<D, S>
where
    D: Discover + Send + Sync + 'static,
    S: MetaStorage + Send + Sync + 'static,
{
    pub async fn new(version_set: Arc<VersionSet<S>>, reconciler: Arc<Reconciler<D, S>>) -> Self {
        let smallest_key = b"0".to_vec();
        let biggest_key: Vec<u8> = b"9".to_vec();
        let current_version = version_set.current_version().await;
        let (key_spans, id_spans) = Self::recover_span_blobs(current_version.clone()).await;
        let inner = Arc::new(Mutex::new(Inner {
            version_set,
            key_spans,
            id_spans,
            max_span_id: 0,
            smallest_key,
            biggest_key,
        }));
        Self { inner, reconciler }
    }
}

#[async_trait]
impl<D, S> Placement for SpanBasedBlobPlacement<D, S>
where
    D: Discover + Sync + Send + 'static,
    S: MetaStorage + Sync + Send + 'static,
{
    async fn add_new_blob(&self, blob: storage::NewBlob) -> Result<storage::NewBlob> {
        let mut inner = self.inner.lock().await;

        // init genesis span if first use.
        if inner.id_spans.is_empty() {
            Self::init_genesis_span(&mut inner).await?;
        }

        // add to first suitable span in current span.
        let mut added_spans = Self::add_blob_to_span(&mut inner, &blob).await?;

        // check split and split.
        for added_span in added_spans.iter_mut() {
            let split_at = Self::split_if_need(&mut inner, &added_span).await?;
            if let Some(split_key) = split_at {
                Self::do_split(&mut inner, split_key, &added_span).await?;
            }
        }

        // reconcile after this change.
        self.reconciler
            .reconcile_blob(&blob.bucket, &blob.blob)
            .await?;

        let mut blob = blob.clone();
        blob.span_ids = added_spans;
        Ok(blob)
    }
}

impl<D, S> SpanBasedBlobPlacement<D, S>
where
    D: Discover,
    S: MetaStorage,
{
    async fn recover_span_blobs(
        version: Arc<Version>,
    ) -> (
        BTreeMap<Vec<u8>, Arc<Mutex<Span>>>,
        HashMap<u64, Arc<Mutex<Span>>>,
    ) {
        let key_spans = BTreeMap::new();
        let id_spans = HashMap::new();
        for _desc in version.list_all_blobs() {
            // TODO: use span-id in desc to rebuild span index.
        }
        (key_spans, id_spans)
    }

    async fn init_genesis_span(inner: &mut MutexGuard<'_, Inner<S>>) -> Result<()> {
        inner.max_span_id += 1;
        let id = inner.max_span_id;
        let span = Arc::new(Mutex::new(Span {
            id,
            start: inner.smallest_key.to_owned(),
            end: inner.biggest_key.to_owned(),
            blobs: BTreeMap::new(),
            size: 0,
            objects: 0,
        }));
        inner.id_spans.insert(id, span.clone());
        inner
            .key_spans
            .insert(span.lock().await.start.to_owned(), span.clone());
        Ok(())
    }

    async fn add_blob_to_span(
        inner: &mut MutexGuard<'_, Inner<S>>,
        desc: &storage::NewBlob,
    ) -> Result<Vec<u64>> {
        let stats = desc.stats.as_ref().unwrap().to_owned();

        // find start span
        let mut span_before_sst = inner.key_spans.range(..=stats.smallest.to_owned());
        let left_span = loop {
            let (_, span) = span_before_sst.next_back().unwrap();
            let span = span.lock().await;
            if span.end.to_owned() > stats.smallest.to_owned() {
                break span.start.to_owned();
            }
        };

        // continue to find crossed spans
        let mut span_ids = Vec::new();
        let mut span_finder = inner.key_spans.range(left_span..);
        loop {
            if let Some((_, span)) = span_finder.next() {
                let span = span.lock().await;
                if span.end.to_owned() <= stats.largest.to_owned() {
                    span_ids.push(span.id.to_owned())
                }
            } else {
                break;
            }
        }

        // add blob to each spans.
        for span_id in span_ids.to_owned() {
            let span = inner.id_spans.get_mut(&span_id).unwrap();
            let mut span = span.lock().await;
            span.blobs.insert(
                stats.smallest.to_owned(),
                SpanBlob {
                    bucket: desc.bucket.to_owned(),
                    blob: desc.blob.to_owned(),
                    span_id: span_id.to_owned(),
                    start: stats.smallest.to_owned(),
                    end: stats.largest.to_owned(),
                    size: desc.size.to_owned(),
                    object: desc.objects.to_owned(),
                },
            );
            span.size += desc.size;
            span.objects += desc.objects;
        }

        Ok(span_ids)
    }

    async fn split_if_need(
        inner: &mut MutexGuard<'_, Inner<S>>,
        added_span: &u64,
    ) -> Result<Option<Vec<u8>>> {
        let span = inner
            .borrow_mut()
            .id_spans
            .get(added_span)
            .unwrap()
            .as_ref();

        // need split when it over the size threshold.
        if span.lock().await.size < MAX_SPAN_SIZE {
            return Ok(None);
        }

        // find split point(blob level).
        let split_at = {
            let mut lhs = 0;
            let mut split_at = None;
            let span = span.lock().await;
            let mut iter = span.blobs.iter();
            loop {
                if let Some((_, blob)) = iter.next() {
                    lhs += blob.size;
                    if lhs >= MAX_SPAN_SIZE {
                        if let Some((key, _)) = iter.next_back() {
                            split_at = Some(key.to_owned());
                        }
                        break;
                    }
                } else {
                    break;
                }
            }
            split_at
        };

        Ok(split_at)
    }

    async fn do_split(
        inner: &mut MutexGuard<'_, Inner<S>>,
        split_at: Vec<u8>,
        added_span: &u64,
    ) -> Result<()> {
        inner.max_span_id += 1;
        let new_span_id = inner.max_span_id;
        let new_span = {
            let span = inner.id_spans.get_mut(added_span).unwrap();
            let mut span = span.lock().await;
            let mut new_span = Span {
                id: new_span_id,
                start: split_at.to_owned(),
                end: span.end.to_owned(),
                blobs: BTreeMap::new(),
                size: 0,
                objects: 0,
            };
            let split_key = split_at.to_owned();
            let mut new_span_blobs = span.blobs.split_off(&split_key);
            for (_, blob) in new_span_blobs.iter_mut() {
                blob.span_id = new_span_id;
                new_span.size += blob.size;
                new_span.objects += blob.object;
            }
            span.size -= new_span.size;
            span.objects -= new_span.objects;
            new_span
        };

        let new_span = Arc::new(Mutex::new(new_span));
        inner.id_spans.insert(new_span_id, new_span.to_owned());
        inner
            .key_spans
            .insert(split_at.to_owned(), new_span.to_owned());

        Ok(())
    }
}
