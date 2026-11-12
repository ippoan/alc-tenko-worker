//! e-Gov で出した点呼の届出の申請データ (zip) の保管 (Refs ippoan/alc-app#676・#684)。管理画面 (alc-app の「点呼の届出」) が
//! 申請データをブラウザで読んで食い違いを確かめた後、「保存」で到達後の zip をここへ送る。rust-alc-api には無い口。
//!
//! - `POST /tenko/filing-archives?name=…&meta=…` — 本文は zip のバイト列そのまま。クエリの `name` (元のファイル名を
//!   encodeURIComponent したもの。必須、decode 後 1〜200 文字) と `meta` (画面が読んだ要約の JSON object を encodeURIComponent
//!   したもの。任意、decode 後 4000 文字まで。中身は解釈せず、そのまま保存して返す)。応答は 201 で保管 1 件。
//!   ヘッダーで受けないのは、auth-worker の代理が転送するヘッダーを `X-Tenant-ID`・`X-User-*`・`Content-Type`・`Range` に
//!   作り直すため (クエリはそのまま届く)
//! - `GET /tenko/filing-archives` — テナントの保管の一覧 (`saved_at` の新しい順、[`MAX_LIST`] 件まで)
//! - `GET /tenko/filing-archives/{id}` — zip を返す (`Content-Disposition: attachment` で元のファイル名)
//!
//! **3 口とも admin だけ** (`X-User-Role` が admin でなければ 403 `forbidden`。[`crate::filing`] と同じ)。置き場が無ければ
//! (R2 の binding の無いローカルなど) この 3 口だけ 503。DB に行を持たない (保管の要約は R2 の customMetadata に置く)。
//! R2 のキーは `{tenant_id}/{保存した時刻の unix ms}-{uuid}.zip`、id はキーから `{tenant_id}/` と `.zip` を除いたもの。
//! uuid は `gen_random_uuid()` で取る (wasm で乱数の crate を持たないため。ippoan/alc-measurements-worker の upload と同じ)。
//! ログは固定の語だけ (ファイル名・キー・テナントを出さない)。

use std::collections::HashMap;
use std::sync::Arc;

use alc_core_wasm::{AuthUser, TenantId};
use alc_worker_db::PgClient;
use axum::body::Body;
use axum::extract::{Path, RawQuery, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, SecondsFormat};
use futures_util::future::BoxFuture;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde::Serialize;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::filing::{error, is_admin};
use crate::AppState;

/// 本文の上限 (20 MiB)
pub const MAX_BYTES: usize = 20 * 1024 * 1024;
/// ファイル名の文字数の上限 (decode 後)
pub const MAX_NAME_CHARS: usize = 200;
/// 要約の JSON の文字数の上限 (decode 後)
pub const MAX_META_CHARS: usize = 4000;
/// 一覧の件数の上限
pub const MAX_LIST: usize = 1000;
pub const CONTENT_TYPE: &str = "application/zip";

/// 置き場の段の失敗。持つのは段の名前だけ (キーやランタイムのエラー文を持たない)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreError(pub &'static str);

/// 置き場の 1 件 (キー・大きさ・customMetadata)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub key: String,
    pub size: u64,
    pub metadata: HashMap<String, String>,
}

/// 置き場の 1 件と中身
pub type StoredFile = (StoredObject, Vec<u8>);

/// 申請データの置き場 (Worker では R2 の binding `TENKO_FILINGS_R2`、テストではモック)。
/// axum の state と handler が要るので `Send + Sync` で `Send` な future を返す
/// (ippoan/alc-measurements-worker の `files::FileStore` と同じ形)
pub trait FileStore: Send + Sync {
    /// 置く (`content_type` は httpMetadata、`metadata` は customMetadata)
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
        metadata: HashMap<String, String>,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// `prefix` で始まるものを `max` 件まで (customMetadata つき)
    fn list<'a>(
        &'a self,
        prefix: &'a str,
        max: usize,
    ) -> BoxFuture<'a, Result<Vec<StoredObject>, StoreError>>;

    /// 1 件と中身。無ければ `None`
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<StoredFile>, StoreError>>;
}

pub type SharedStore = Arc<dyn FileStore>;

/// 応答の保管 1 件
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Archive {
    pub id: String,
    pub name: String,
    pub size: u64,
    pub saved_at: String,
    pub saved_by: Option<String>,
    pub meta: serde_json::Value,
}

impl Archive {
    /// 置き場の 1 件から。`{tenant_id}/{id}.zip` の形でなければ `None` (一覧に出さない)
    fn from_object(tenant_id: Uuid, o: &StoredObject) -> Option<Self> {
        let id = o
            .key
            .strip_prefix(&format!("{tenant_id}/"))?
            .strip_suffix(".zip")
            .filter(|id| is_id(id))?;
        let m = &o.metadata;
        Some(Self {
            id: id.to_owned(),
            name: name_or_default(m, id),
            size: o.size,
            saved_at: m.get("saved_at").cloned().unwrap_or_default(),
            saved_by: m.get("saved_by").cloned(),
            meta: m
                .get("meta")
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default(),
        })
    }
}

/// customMetadata の名前。無ければ `{id}.zip`
fn name_or_default(m: &HashMap<String, String>, id: &str) -> String {
    m.get("name").cloned().unwrap_or(format!("{id}.zip"))
}

/// id の形 `^\d{13}-[0-9a-f-]{36}$`
pub fn is_id(id: &str) -> bool {
    let b = id.as_bytes();
    b.len() == 13 + 1 + 36
        && b[..13].iter().all(u8::is_ascii_digit)
        && b[13] == b'-'
        && b[14..]
            .iter()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(c) || *c == b'-')
}

fn key(tenant_id: Uuid, id: &str) -> String {
    format!("{tenant_id}/{id}.zip")
}

/// クエリの `key` の値 (最初の 1 つ) を percent-decode する。無ければ `Some(None)`、UTF-8 でなければ `None`。
/// encodeURIComponent の値を受けるので `+` は空白にしない (`+` は `%2B` で届く)
fn decoded(query: Option<&str>, key: &str) -> Option<Option<String>> {
    let Some(v) = query
        .unwrap_or_default()
        .split('&')
        .find_map(|pair| pair.strip_prefix(key)?.strip_prefix('='))
    else {
        return Some(None);
    };
    let s = percent_decode_str(v).decode_utf8().ok()?;
    Some(Some(s.into_owned()))
}

/// クエリの `name` を検める
fn parse_name(query: Option<&str>) -> Option<String> {
    let name = decoded(query, "name")??;
    (1..=MAX_NAME_CHARS)
        .contains(&name.chars().count())
        .then_some(name)
}

/// 保存する要約の JSON の文字列と、応答に返す値
type Meta = Option<(String, serde_json::Value)>;

/// クエリの `meta` を検める。無ければ `Some(None)`、長すぎる・JSON object でなければ `None`
fn parse_meta(query: Option<&str>) -> Option<Meta> {
    let Some(raw) = decoded(query, "meta")? else {
        return Some(None);
    };
    let value = serde_json::from_str::<serde_json::Value>(&raw)
        .ok()
        .filter(|v| v.is_object() && raw.chars().count() <= MAX_META_CHARS)?;
    Some(Some((raw, value)))
}

/// `filename*` の attr-char 以外を % にする (RFC 8187)
const ATTR: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'!')
    .remove(b'#')
    .remove(b'$')
    .remove(b'&')
    .remove(b'+')
    .remove(b'-')
    .remove(b'.')
    .remove(b'^')
    .remove(b'_')
    .remove(b'`')
    .remove(b'|')
    .remove(b'~');

/// 新しい保管の uuid (`gen_random_uuid()`)
pub async fn new_uuid(pg: &mut PgClient, tenant_id: Uuid) -> Result<Uuid, Error> {
    pg.tenant_tx(tenant_id, |tx| {
        Box::pin(async move {
            let row = tx.query_typed_one("SELECT gen_random_uuid()", &[]).await?;
            Ok(row.get::<_, Uuid>(0))
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/tenko/filing-archives",
            get(list_handler).post(create_handler),
        )
        .route("/tenko/filing-archives/{id}", get(get_handler))
}

impl AppState {
    /// 置き場の失敗を固定の語でログに出し、500 にする
    fn store_fail(&self, stage: &str, e: StoreError) -> Response {
        (self.log)(&format!("tenko: {stage} failed: storage ({})", e.0));
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }

    /// admin でなければ 403、置き場が無ければ 503
    fn filing_store(
        &self,
        stage: &str,
        auth_user: &Option<Extension<AuthUser>>,
    ) -> Result<SharedStore, Box<Response>> {
        if !is_admin(auth_user) {
            return Err(Box::new(error(StatusCode::FORBIDDEN, "forbidden")));
        }
        self.files.clone().ok_or_else(|| {
            (self.log)(&format!("tenko: {stage} failed: storage (unconfigured)"));
            Box::new(StatusCode::SERVICE_UNAVAILABLE.into_response())
        })
    }
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    RawQuery(query): RawQuery,
    body: Body,
) -> Response {
    let stage = "filing archive put";
    let files = match state.filing_store(stage, &auth_user) {
        Ok(f) => f,
        Err(r) => return *r,
    };
    let Some(name) = parse_name(query.as_deref()) else {
        return error(StatusCode::BAD_REQUEST, "invalid_name");
    };
    let Some(meta) = parse_meta(query.as_deref()) else {
        return error(StatusCode::BAD_REQUEST, "invalid_meta");
    };
    // 上限を超えたら読むのをやめる (axum の既定の上限 2 MB は `Body` には掛からない)
    let Ok(bytes) = axum::body::to_bytes(body, MAX_BYTES).await else {
        return error(StatusCode::PAYLOAD_TOO_LARGE, "too_large");
    };
    if bytes.is_empty() {
        return error(StatusCode::BAD_REQUEST, "empty");
    }
    let uuid = {
        let mut pg = state.pg.lock().await;
        match new_uuid(&mut pg, tenant_id).await {
            Ok(u) => u,
            Err(e) => return state.db_fail(stage, &e).into_response(),
        }
    };
    let ms = (state.clock)();
    let saved_at = DateTime::from_timestamp_millis(ms)
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let id = format!("{ms:013}-{uuid}");
    // admin の要求には AuthUser が在る (auth-worker の X-User-ID)
    let saved_by = auth_user.map(|Extension(u)| u.user_id.to_string());
    let mut metadata = HashMap::from([
        ("name".to_owned(), name.clone()),
        ("saved_at".to_owned(), saved_at.clone()),
    ]);
    metadata.extend(saved_by.clone().map(|by| ("saved_by".to_owned(), by)));
    metadata.extend(
        meta.as_ref()
            .map(|(raw, _)| ("meta".to_owned(), raw.clone())),
    );
    let size = bytes.len() as u64;
    if let Err(e) = files
        .put(&key(tenant_id, &id), bytes.to_vec(), CONTENT_TYPE, metadata)
        .await
    {
        return state.store_fail(stage, e);
    }
    let archive = Archive {
        id,
        name,
        size,
        saved_at,
        saved_by,
        meta: meta.map(|(_, v)| v).unwrap_or_default(),
    };
    (StatusCode::CREATED, Json(archive)).into_response()
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
) -> Response {
    let stage = "filing archive list";
    let files = match state.filing_store(stage, &auth_user) {
        Ok(f) => f,
        Err(r) => return *r,
    };
    let objects = match files.list(&format!("{tenant_id}/"), MAX_LIST).await {
        Ok(o) => o,
        Err(e) => return state.store_fail(stage, e),
    };
    let mut archives: Vec<Archive> = objects
        .iter()
        .filter_map(|o| Archive::from_object(tenant_id, o))
        .collect();
    archives.sort_by(|a, b| (&b.saved_at, &b.id).cmp(&(&a.saved_at, &a.id)));
    archives.truncate(MAX_LIST);
    Json(archives).into_response()
}

async fn get_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<String>,
) -> Response {
    let stage = "filing archive get";
    let files = match state.filing_store(stage, &auth_user) {
        Ok(f) => f,
        Err(r) => return *r,
    };
    if !is_id(&id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let (object, bytes) = match files.get(&key(tenant_id, &id)).await {
        Ok(Some(found)) => found,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return state.store_fail(stage, e),
    };
    let disposition = format!(
        "attachment; filename*=UTF-8''{}",
        utf8_percent_encode(&name_or_default(&object.metadata, &id), ATTR)
    );
    (
        [
            (header::CONTENT_TYPE, CONTENT_TYPE.to_owned()),
            (header::CONTENT_DISPOSITION, disposition),
        ],
        bytes,
    )
        .into_response()
}
