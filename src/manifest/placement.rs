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
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    rc::Rc,
    sync::Arc,
};

use tokio::sync::{Mutex, MutexGuard};

use super::versions::{BlobDesc, Version};
use crate::error::Result;

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

struct SpanBasedBlobPlacement {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    version: Version,
    key_spans: BTreeMap<Vec<u8>, Rc<RefCell<Span>>>,
    id_spans: HashMap<u64, Rc<RefCell<Span>>>,
    max_span_id: u64,
    smallest_key: Vec<u8>,
    biggest_key: Vec<u8>,
}

impl SpanBasedBlobPlacement {
    pub async fn from(version: Version) -> Self {
        let smallest_key = b"a".to_vec();
        let biggest_key: Vec<u8> = b"z".to_vec();
        let (key_spans, id_spans) = Self::recover_span_blobs(version.to_owned()).await;
        let inner = Arc::new(Mutex::new(Inner {
            version,
            key_spans,
            id_spans,
            max_span_id: 0,
            smallest_key,
            biggest_key,
        }));
        Self { inner }
    }
}

impl SpanBasedBlobPlacement {
    async fn recover_span_blobs(
        version: Version,
    ) -> (
        BTreeMap<Vec<u8>, Rc<RefCell<Span>>>,
        HashMap<u64, Rc<RefCell<Span>>>,
    ) {
        let key_spans = BTreeMap::new();
        let id_spans = HashMap::new();
        for desc in version.list_all_blobs() {
            // TODO: use span-id in desc to rebuild span index.
        }
        (key_spans, id_spans)
    }

    async fn add_new_blob(&self, blob: BlobDesc) -> Result<BlobDesc> {
        let mut inner = self.inner.lock().await;

        // init genesis span if first use.
        if inner.id_spans.is_empty() {
            Self::init_genesis_span(&mut inner).await?;
        }

        // add to first suitable span in current span.
        let added_span = Self::add_blob_to_span(&mut inner, &blob).await?;

        // check split and split.
        let split_at = Self::split_if_need(&mut inner, &added_span).await?;
        if let Some(split_key) = split_at {
            Self::do_split(&mut inner, split_key, &added_span).await?;
        }

        // reconcile after this change.

        let mut blob = blob.clone();
        blob.span_id = added_span;
        Ok(blob)
    }

    async fn init_genesis_span(inner: &mut MutexGuard<'_, Inner>) -> Result<()> {
        inner.max_span_id += 1;
        let id = inner.max_span_id;
        let span = Rc::new(RefCell::new(Span {
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
            .insert(span.borrow().start.to_owned(), span.clone());
        Ok(())
    }

    async fn add_blob_to_span(inner: &mut MutexGuard<'_, Inner>, desc: &BlobDesc) -> Result<u64> {
        let mut rs = inner.key_spans.range(..desc.smallest.to_owned());
        let span_id = loop {
            let (_, r) = rs.next_back().unwrap();
            if r.borrow().end.to_owned() > desc.smallest.to_owned() {
                break r.borrow().id.to_owned();
            }
        };
        let span = inner.id_spans.get_mut(&span_id).unwrap();
        let mut span = span.as_ref().borrow_mut();
        span.blobs.insert(
            desc.smallest.to_owned(),
            SpanBlob {
                bucket: desc.bucket.to_owned(),
                blob: desc.blob.to_owned(),
                span_id: span_id.to_owned(),
                start: desc.smallest.to_owned(),
                end: desc.largest.to_owned(),
                size: desc.size.to_owned(),
                object: desc.objects.to_owned(),
            },
        );
        span.size += desc.size;
        span.objects += desc.objects;
        Ok(span_id)
    }

    async fn split_if_need(
        inner: &mut MutexGuard<'_, Inner>,
        added_span: &u64,
    ) -> Result<Option<Vec<u8>>> {
        let span = inner
            .borrow_mut()
            .id_spans
            .get(added_span)
            .unwrap()
            .as_ref();

        // need split when it over the size threshold.
        if span.borrow().size < MAX_SPAN_SIZE {
            return Ok(None);
        }

        // find split point(blob level).
        let split_at = {
            let mut lhs = 0;
            let mut split_at = None;
            let span = span.borrow();
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
        inner: &mut MutexGuard<'_, Inner>,
        split_at: Vec<u8>,
        added_span: &u64,
    ) -> Result<()> {
        inner.max_span_id += 1;
        let new_span_id = inner.max_span_id;
        let span = inner.id_spans.get(added_span).unwrap();
        let mut new_span = Span {
            id: new_span_id,
            start: split_at.to_owned(),
            end: span.borrow().end.to_owned(),
            blobs: BTreeMap::new(),
            size: 0,
            objects: 0,
        };
        let split_key = split_at.to_owned();
        let mut new_span_blobs = span.as_ref().borrow_mut().blobs.split_off(&split_key);
        for (_, blob) in new_span_blobs.iter_mut() {
            blob.span_id = new_span_id;
            new_span.size += blob.size;
            new_span.objects += blob.object;
        }
        span.as_ref().borrow_mut().size -= new_span.size;
        span.as_ref().borrow_mut().objects -= new_span.objects;

        let new_span = Rc::new(RefCell::new(new_span));
        inner.id_spans.insert(new_span_id, new_span.to_owned());
        inner
            .key_spans
            .insert(split_at.to_owned(), new_span.to_owned());

        Ok(())
    }
}
