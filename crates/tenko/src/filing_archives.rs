//! e-Gov で出した点呼の届出の**申請済みの控え** (zip) の保管 (Refs ippoan/alc-app#676・#684)。e-Gov の API v2 は
//! 申請書や添付を返さず (申請案件取得は状況・通知・公文書・納付だけ)、控えは提出の直後に一度しか出ないため、
//! 管理画面 (alc-app の「点呼の届出」) がその zip を**到達番号で**ここへ置く。rust-alc-api には無い口。
//!
//! - `PUT /tenko/filing-archives/{到達番号}?name=…&meta=…` — 本文は zip のバイト列そのまま (先頭が `PK\x03\x04`)。
//!   同じ到達番号は上書き。クエリの `name` (元のファイル名を encodeURIComponent したもの。任意、decode 後 1〜200 文字。
//!   無ければ `{到達番号}.zip`) と `meta` (画面が読んだ要約の JSON object を encodeURIComponent したもの。任意、decode 後
//!   4000 文字まで。中身は解釈せず、そのまま保存して返す)。応答は 200 で保管 1 件。
//!   ヘッダーで受けないのは、auth-worker の代理が転送するヘッダーを `X-Tenant-ID`・`X-User-*`・`Content-Type`・`Range` に
//!   作り直すため (クエリはそのまま届く)
//! - `GET /tenko/filing-archives` — テナントの保管の一覧 (`saved_at` の新しい順、[`MAX_LIST`] 件まで)
//! - `GET /tenko/filing-archives/{到達番号}` — zip を返す (`Content-Disposition: attachment` で元のファイル名)
//!
//! - `PUT /tenko/filing-archives/{到達番号}/companies` — 本文 `{"company_ids": [uuid…]}`。控えをどの事業者に見せるかの置き換え
//!   (alc-migrations 214 の `tenko_filing_archive_companies`)。応答は 200 `{"id", "company_ids"}`。R2 に控えが無くてもよい (行だけ。
//!   置き場が無くても動く)。到達番号の形が違えば 400 `invalid_arrive_id`、本文が読めなければ 400 `invalid_body`、テナントに無い事業者は 400 `invalid_company`。
//!   一覧の 1 件には `"company_ids"` (振り分けが無ければ `[]`) が付く。R2 の list の後に別の短い transaction で読む (R2 の await を transaction の中に入れない)
//!
//! **4 口とも admin だけ** (`X-User-Role` が admin でなければ 403 `forbidden`。[`crate::filing`] と同じ)。置き場が無ければ
//! (R2 の binding の無いローカルなど) 保存・一覧・取得の 3 口だけ 503。控えの本体は DB に行を持たない (保管の要約は R2 の customMetadata に置く。
//! DB に持つのは事業者への振り分けだけ)。
//! R2 のキーは `{tenant_id}/{到達番号}.zip`、id は到達番号そのもの ([`is_arrive_id`] の形)。
//! ログは固定の語だけ (ファイル名・キー・テナント・到達番号を出さない)。

use std::collections::HashMap;
use std::sync::Arc;

use alc_core_wasm::{AuthUser, TenantId};
use alc_worker_db::PgClient;
use axum::body::Body;
use axum::extract::{Path, RawQuery, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Extension, Json, Router};
use chrono::{DateTime, SecondsFormat};
use futures_util::future::BoxFuture;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_postgres::types::Type;
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

/// zip の先頭 (ローカルファイルヘッダー)
pub const ZIP_MAGIC: &[u8] = b"PK\x03\x04";

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

/// 一覧の 1 件 (保管 + 見せる事業者)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Listed {
    #[serde(flatten)]
    pub archive: Archive,
    pub company_ids: Vec<Uuid>,
}

/// `PUT .../companies` の本文
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CompaniesInput {
    pub company_ids: Vec<Uuid>,
}

pub mod sql {
    /// 控えの振り分け (arrive_id, company_id)。$1 tenant_id / $2 到達番号の配列
    pub const LIST: &str =
        "SELECT arrive_id, company_id FROM alc_api.tenko_filing_archive_companies \
         WHERE tenant_id = $1 AND arrive_id = ANY($2::text[]) ORDER BY arrive_id, company_id";

    /// テナントの事業者の数。$1 tenant_id / $2 事業者の id の配列
    pub const COMPANY_COUNT: &str =
        "SELECT count(*) FROM alc_api.tenko_companies WHERE tenant_id = $1 AND id = ANY($2)";

    /// 控えの振り分けを全部外す。$1 tenant_id / $2 到達番号
    pub const CLEAR: &str = "DELETE FROM alc_api.tenko_filing_archive_companies \
         WHERE tenant_id = $1 AND arrive_id = $2";

    /// $1 tenant_id / $2 到達番号 / $3 事業者の id の配列
    pub const ADD: &str =
        "INSERT INTO alc_api.tenko_filing_archive_companies (tenant_id, arrive_id, company_id) \
         SELECT $1, $2, unnest($3::uuid[])";
}

/// 到達番号ごとの事業者 (R2 の await の後に、別の短い transaction で読む)
pub async fn company_ids_of(
    pg: &mut PgClient,
    tenant_id: Uuid,
    ids: Vec<String>,
) -> Result<Vec<(String, Uuid)>, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::LIST,
                    &[(&tenant_id, Type::UUID), (&ids, Type::TEXT_ARRAY)],
                )
                .await?;
            Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
        })
    })
    .await
}

/// 控えの振り分けを置き換える。テナントに無い事業者が混じっていれば false (何も書かない)。`company_ids` は整列・重複なし
pub async fn set_companies(
    pg: &mut PgClient,
    tenant_id: Uuid,
    arrive_id: String,
    company_ids: Vec<Uuid>,
) -> Result<bool, tokio_postgres::Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let found = tx
                .query_typed_one(
                    sql::COMPANY_COUNT,
                    &[(&tenant_id, Type::UUID), (&company_ids, Type::UUID_ARRAY)],
                )
                .await?;
            if found.get::<_, i64>(0) != company_ids.len() as i64 {
                return Ok(false);
            }
            tx.execute_typed(
                sql::CLEAR,
                &[(&tenant_id, Type::UUID), (&arrive_id, Type::TEXT)],
            )
            .await?;
            tx.execute_typed(
                sql::ADD,
                &[
                    (&tenant_id, Type::UUID),
                    (&arrive_id, Type::TEXT),
                    (&company_ids, Type::UUID_ARRAY),
                ],
            )
            .await?;
            Ok(true)
        })
    })
    .await
}

impl Archive {
    /// 置き場の 1 件から。`{tenant_id}/{到達番号}.zip` の形でなければ `None` (一覧に出さない)
    fn from_object(tenant_id: Uuid, o: &StoredObject) -> Option<Self> {
        let id = o
            .key
            .strip_prefix(&format!("{tenant_id}/"))?
            .strip_suffix(".zip")
            .filter(|id| is_arrive_id(id))?;
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

/// 到達番号の形。e-Gov の到達番号は 16〜18 桁 (例 `20261112132630208e`: 数字の後に英小文字 1 字が付くことがある)。
/// 数字だけ、または数字の後に末尾 1 字だけ英小文字
pub fn is_arrive_id(id: &str) -> bool {
    let b = id.as_bytes();
    let Some((last, head)) = b.split_last() else {
        return false;
    };
    (16..=18).contains(&b.len())
        && head.iter().all(u8::is_ascii_digit)
        && (last.is_ascii_digit() || last.is_ascii_lowercase())
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

/// クエリの `name` を検める。無ければ `Some(None)`、空・長すぎる・UTF-8 でなければ `None`
fn parse_name(query: Option<&str>) -> Option<Option<String>> {
    let Some(name) = decoded(query, "name")? else {
        return Some(None);
    };
    (1..=MAX_NAME_CHARS)
        .contains(&name.chars().count())
        .then_some(Some(name))
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

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tenko/filing-archives", get(list_handler))
        .route(
            "/tenko/filing-archives/{id}",
            get(get_handler).put(put_handler),
        )
        .route(
            "/tenko/filing-archives/{id}/companies",
            put(companies_handler),
        )
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

async fn put_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<String>,
    RawQuery(query): RawQuery,
    body: Body,
) -> Response {
    let stage = "filing archive put";
    let files = match state.filing_store(stage, &auth_user) {
        Ok(f) => f,
        Err(r) => return *r,
    };
    if !is_arrive_id(&id) {
        return error(StatusCode::BAD_REQUEST, "invalid_arrive_id");
    }
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
    if !bytes.starts_with(ZIP_MAGIC) {
        return error(StatusCode::BAD_REQUEST, "not_zip");
    }
    let name = name.unwrap_or(format!("{id}.zip"));
    let saved_at = DateTime::from_timestamp_millis((state.clock)())
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Millis, true);
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
    Json(archive).into_response()
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
    // 事業者の振り分けは R2 の list の後に、別の短い transaction で読む
    let ids = archives.iter().map(|a| a.id.clone()).collect();
    let rows = {
        let mut pg = state.pg.lock().await;
        match company_ids_of(&mut pg, tenant_id, ids).await {
            Ok(r) => r,
            Err(e) => return state.db_fail(stage, &e).into_response(),
        }
    };
    let listed: Vec<Listed> = archives
        .into_iter()
        .map(|archive| Listed {
            company_ids: rows
                .iter()
                .filter(|(id, _)| *id == archive.id)
                .map(|(_, c)| *c)
                .collect(),
            archive,
        })
        .collect();
    Json(listed).into_response()
}

async fn companies_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    if !is_admin(&auth_user) {
        return error(StatusCode::FORBIDDEN, "forbidden");
    }
    if !is_arrive_id(&id) {
        return error(StatusCode::BAD_REQUEST, "invalid_arrive_id");
    }
    let Ok(mut input) = serde_json::from_slice::<CompaniesInput>(&body) else {
        return error(StatusCode::BAD_REQUEST, "invalid_body");
    };
    input.company_ids.sort_unstable();
    input.company_ids.dedup();
    let mut pg = state.pg.lock().await;
    match set_companies(&mut pg, tenant_id, id.clone(), input.company_ids.clone()).await {
        Ok(true) => Json(json!({ "id": id, "company_ids": input.company_ids })).into_response(),
        Ok(false) => error(StatusCode::BAD_REQUEST, "invalid_company"),
        Err(e) => state
            .db_fail("filing archive companies", &e)
            .into_response(),
    }
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
    if !is_arrive_id(&id) {
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
