//! 点呼の届出の申請データの置き場 ([`tenko::filing_archives::FileStore`]) の Workers での実装: R2 の binding `TENKO_FILINGS_R2`
//! (ippoan/alc-measurements-worker の `src/r2.rs` の型)。
//!
//! `FileStore` は `Send + Sync` で `Send` な future を返す (axum の state と handler が要求する)。
//! R2 の値と future は JS の値で `Send` でないので、`worker::send` の `SendWrapper`・`SendFuture` で包む
//! (Workers は単一 thread)。
//!
//! エラーは段の名前だけを持つ `StoreError` に落とす。**key とランタイムの生のエラー文はログに出さない。**
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging の実物)。

use std::collections::HashMap;

use futures_util::future::BoxFuture;
use tenko::filing_archives::{FileStore, StoreError, StoredFile, StoredObject};
use worker::send::{SendFuture, SendWrapper};
use worker::{Bucket, HttpMetadata, Include, Object};

/// R2 の list の 1 回の上限
const LIST_PAGE: usize = 1000;

/// R2 の bucket (binding `TENKO_FILINGS_R2`。本番と staging で別の bucket)
pub struct R2Store(SendWrapper<Bucket>);

impl R2Store {
    pub fn new(bucket: Bucket) -> Self {
        Self(SendWrapper::new(bucket))
    }
}

/// customMetadata が読めなければ空にする (一覧・取得そのものは止めない)
fn stored(o: &Object) -> StoredObject {
    StoredObject {
        key: o.key(),
        size: o.size(),
        metadata: o.custom_metadata().unwrap_or_default(),
    }
}

impl FileStore for R2Store {
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
        metadata: HashMap<String, String>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(SendFuture::new(async move {
            let http = HttpMetadata {
                content_type: Some(content_type.to_owned()),
                ..Default::default()
            };
            let put = self
                .0
                .put(key, bytes)
                .http_metadata(http)
                .custom_metadata(metadata)
                .execute()
                .await;
            put.map(|_| ()).map_err(|_| StoreError("put"))
        }))
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        max: usize,
    ) -> BoxFuture<'a, Result<Vec<StoredObject>, StoreError>> {
        Box::pin(SendFuture::new(async move {
            let mut out = Vec::new();
            let mut cursor: Option<String> = None;
            // include を付けると 1 回で返る件数が limit より少ないことがある。cursor で続きを読む
            while out.len() < max {
                let mut req = self
                    .0
                    .list()
                    .prefix(prefix)
                    .limit((max - out.len()).min(LIST_PAGE) as u32)
                    .include(vec![Include::CustomMetadata]);
                if let Some(c) = cursor.take() {
                    req = req.cursor(c);
                }
                let page = req.execute().await.map_err(|_| StoreError("list"))?;
                out.extend(page.objects().iter().map(stored));
                match page.cursor() {
                    Some(c) if page.truncated() => cursor = Some(c),
                    _ => break,
                }
            }
            out.truncate(max);
            Ok(out)
        }))
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<StoredFile>, StoreError>> {
        Box::pin(SendFuture::new(async move {
            let object = self.0.get(key).execute().await;
            let Some(object) = object.map_err(|_| StoreError("get"))? else {
                return Ok(None);
            };
            let body = object.body().ok_or(StoreError("get body"))?;
            let bytes = body.bytes().await.map_err(|_| StoreError("read body"))?;
            Ok(Some((stored(&object), bytes)))
        }))
    }
}
