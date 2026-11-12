//! 点呼の事業者 (alc-migrations 214 の `tenko_companies`)。1 つのテナントに複数の事業者 (グループの会社) が入り、
//! 営業所 (`tenko_offices.company_id`) を事業者に振り分ける。点呼の届出の書き込む値 ([`crate::filing`]) と申請済みの控え
//! ([`crate::filing_archives`]) は事業者ごとに持ち、受委託 ([`crate::consignments`]) は営業所の事業者の組で決まる。
//! 点呼の判定 (alc-measurements-worker) はまだこの表を読まない。rust-alc-api には無い口。
//!
//! - `GET /tenko/companies` — `[{"id", "name", "is_self", "office_ids": [営業所の id]}]` (名称順)。`office_ids` は `company_id` がその事業者の営業所
//! - `POST /tenko/companies` — 本文 `{"name", "is_self"?: false, "office_ids"?: []}` → 201 で事業者 1 件 (GET と同じ形)
//! - `PUT /tenko/companies/{id}` — 同じ本文で置き換える (省略した `is_self` は false、`office_ids` は空)。無ければ 404
//! - `DELETE /tenko/companies/{id}` — 204。無ければ 404 (営業所の `company_id` は表の外部キーで NULL に戻り、書き込む値・控えの振り分けは CASCADE で消える)
//!
//! `office_ids` は振り分けの置き換え: 一覧の営業所の `company_id` をこの事業者にし (別の事業者から移る。営業所は 1 つの事業者だけ)、
//! 一覧に無い元の営業所は NULL に戻す。`is_self: true` なら、同じテナントのほかの事業者の `is_self` を先に false にする
//! (同じ transaction の中。`is_self` は 1 テナントに 1 行まで)。
//! 検める欄: 本文が読めない `invalid_body` / 名称が空 `invalid_name` / 500 文字超 `too_long` / 同じ名称 `duplicate_name` (23505) /
//! テナントの営業所でない id `invalid_office` / id の件数 `too_many_ids` / 事業者の件数 `too_many_companies`。外れたら 400 `{"error": "<語>"}`。
//!
//! **4 口とも admin だけ** (filing ([`crate::filing`]) と同じ判定。`X-User-Role` が admin でなければ 403 `{"error": "forbidden"}`)。
//! この表は dev の軸を持たない。

use alc_core_wasm::{AuthUser, TenantId};
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::filing::{error as status_error, is_admin};
use crate::AppState;

/// テナントの事業者の件数の上限
pub const MAX_COMPANIES: i64 = 100;
/// 1 事業者の営業所の件数の上限
pub const MAX_IDS: usize = 1000;
/// 名称の文字数の上限
pub const MAX_CHARS: usize = 500;

/// POST・PUT の本文
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CompanyInput {
    pub name: String,
    #[serde(default)]
    pub is_self: bool,
    #[serde(default)]
    pub office_ids: Vec<Uuid>,
}

/// 事業者 1 件
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Company {
    pub id: Uuid,
    pub name: String,
    pub is_self: bool,
    pub office_ids: Vec<Uuid>,
}

impl TxOutput for Company {}

/// 保存の結果 (検査と 404 は transaction の中で決まる。書く前に返すので何も残らない)
#[derive(Debug, Clone, PartialEq)]
pub enum Saved {
    Done(Company),
    NotFound,
    Invalid(&'static str),
}

impl TxOutput for Saved {}

impl CompanyInput {
    /// 空白を落として検める。外れた欄の語を返す。id は重複をまとめる
    pub fn normalize(mut self) -> Result<Self, &'static str> {
        self.name = self.name.trim().to_owned();
        if self.name.is_empty() {
            return Err("invalid_name");
        }
        if self.name.chars().count() > MAX_CHARS {
            return Err("too_long");
        }
        self.office_ids.sort_unstable();
        self.office_ids.dedup();
        if self.office_ids.len() > MAX_IDS {
            return Err("too_many_ids");
        }
        Ok(self)
    }
}

pub mod sql {
    /// $1 tenant_id / $2 事業者の id (null なら全部)
    pub const COMPANIES: &str = "SELECT id, name, is_self FROM alc_api.tenko_companies \
         WHERE tenant_id = $1 AND ($2::uuid IS NULL OR id = $2) ORDER BY name, id";

    /// 事業者に振り分けた営業所 (company_id, id)。$1 tenant_id / $2 事業者の id (null なら全部)
    pub const OFFICES: &str = "SELECT company_id, id FROM alc_api.tenko_offices \
         WHERE tenant_id = $1 AND company_id IS NOT NULL AND ($2::uuid IS NULL OR company_id = $2) \
         ORDER BY name, id";

    /// 事業者がテナントに在るか。$1 tenant_id / $2 事業者の id
    pub const EXISTS: &str =
        "SELECT id FROM alc_api.tenko_companies WHERE tenant_id = $1 AND id = $2";

    /// 事業者の件数。$1 tenant_id
    pub const COUNT: &str = "SELECT count(*) FROM alc_api.tenko_companies WHERE tenant_id = $1";

    /// テナントの営業所の数。$1 tenant_id / $2 営業所の id の配列
    pub const OFFICE_COUNT: &str =
        "SELECT count(*) FROM alc_api.tenko_offices WHERE tenant_id = $1 AND id = ANY($2)";

    /// ほかの事業者の `is_self` を外す (`is_self` は 1 テナントに 1 行まで)。$1 tenant_id / $2 この事業者の id (作るときは null)
    pub const CLEAR_SELF: &str =
        "UPDATE alc_api.tenko_companies SET is_self = false, updated_at = NOW() \
         WHERE tenant_id = $1 AND is_self AND id IS DISTINCT FROM $2";

    /// $1 tenant_id / $2 name / $3 is_self
    pub const INSERT: &str = "INSERT INTO alc_api.tenko_companies (tenant_id, name, is_self) \
         VALUES ($1, $2, $3) RETURNING id";

    /// $1 tenant_id / $2 id / $3 name / $4 is_self
    pub const UPDATE: &str =
        "UPDATE alc_api.tenko_companies SET name = $3, is_self = $4, updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2";

    /// 一覧に無い営業所を事業者から外す。$1 tenant_id / $2 事業者の id / $3 営業所の id の配列
    pub const RELEASE_OFFICES: &str =
        "UPDATE alc_api.tenko_offices SET company_id = NULL, updated_at = NOW() \
         WHERE tenant_id = $1 AND company_id = $2 AND NOT (id = ANY($3::uuid[]))";

    /// 一覧の営業所をこの事業者へ振り分ける (別の事業者から移る)。$1 tenant_id / $2 事業者の id / $3 営業所の id の配列
    pub const ASSIGN_OFFICES: &str =
        "UPDATE alc_api.tenko_offices SET company_id = $2, updated_at = NOW() \
         WHERE tenant_id = $1 AND id = ANY($3::uuid[]) AND company_id IS DISTINCT FROM $2";

    /// $1 id / $2 tenant_id
    pub const DELETE: &str = "DELETE FROM alc_api.tenko_companies WHERE id = $1 AND tenant_id = $2";
}

/// 事業者(`only` が在ればその 1 件)と、振り分けた営業所を読む
async fn load(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    only: Option<Uuid>,
) -> Result<Vec<Company>, Error> {
    let args = [
        (
            &tenant_id as &(dyn tokio_postgres::types::ToSql + Sync),
            Type::UUID,
        ),
        (&only, Type::UUID),
    ];
    let companies = tx.query_typed(sql::COMPANIES, &args).await?;
    let offices = tx.query_typed(sql::OFFICES, &args).await?;
    Ok(companies
        .iter()
        .map(|r| {
            let id: Uuid = r.get(0);
            Company {
                id,
                name: r.get(1),
                is_self: r.get(2),
                office_ids: offices
                    .iter()
                    .filter(|o| o.get::<_, Uuid>(0) == id)
                    .map(|o| o.get(1))
                    .collect(),
            }
        })
        .collect())
}

pub async fn list(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<Company>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move { load(tx, tenant_id, None).await })
    })
    .await
}

/// 事業者を作る (`id` が None) か、全部置き換える (`id` が在る)。`input` は [`CompanyInput::normalize`] を通したもの。
/// 検査 (営業所・件数) → 事業者の行 (更新で無ければ 404) → `is_self` と営業所の振り分け、の順で、検査が外れたら何も書かない
pub async fn save(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Option<Uuid>,
    input: CompanyInput,
) -> Result<Saved, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let offices = tx
                .query_typed_one(
                    sql::OFFICE_COUNT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.office_ids, Type::UUID_ARRAY),
                    ],
                )
                .await?;
            if offices.get::<_, i64>(0) != input.office_ids.len() as i64 {
                return Ok(Saved::Invalid("invalid_office"));
            }
            match id {
                Some(id) => {
                    let found = tx
                        .query_typed_opt(
                            sql::EXISTS,
                            &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
                        )
                        .await?;
                    if found.is_none() {
                        return Ok(Saved::NotFound);
                    }
                }
                None => {
                    let count = tx
                        .query_typed_one(sql::COUNT, &[(&tenant_id, Type::UUID)])
                        .await?;
                    if count.get::<_, i64>(0) >= MAX_COMPANIES {
                        return Ok(Saved::Invalid("too_many_companies"));
                    }
                }
            }
            // 自社の印は 1 行まで: 先にほかの事業者から外す
            if input.is_self {
                tx.execute_typed(
                    sql::CLEAR_SELF,
                    &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
                )
                .await?;
            }
            let company_id: Uuid = match id {
                Some(id) => {
                    tx.execute_typed(
                        sql::UPDATE,
                        &[
                            (&tenant_id, Type::UUID),
                            (&id, Type::UUID),
                            (&input.name, Type::TEXT),
                            (&input.is_self, Type::BOOL),
                        ],
                    )
                    .await?;
                    id
                }
                None => tx
                    .query_typed_one(
                        sql::INSERT,
                        &[
                            (&tenant_id, Type::UUID),
                            (&input.name, Type::TEXT),
                            (&input.is_self, Type::BOOL),
                        ],
                    )
                    .await?
                    .get(0),
            };
            let offices = [
                (
                    &tenant_id as &(dyn tokio_postgres::types::ToSql + Sync),
                    Type::UUID,
                ),
                (&company_id, Type::UUID),
                (&input.office_ids, Type::UUID_ARRAY),
            ];
            tx.execute_typed(sql::RELEASE_OFFICES, &offices).await?;
            tx.execute_typed(sql::ASSIGN_OFFICES, &offices).await?;
            let mut out = load(tx, tenant_id, Some(company_id)).await?;
            // 今書いた事業者は必ず在る (同じ transaction の中)
            Ok(Saved::Done(out.remove(0)))
        })
    })
    .await
}

/// 消したら true、無ければ false
pub async fn delete(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(sql::DELETE, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tenko/companies", get(list_handler).post(create_handler))
        .route(
            "/tenko/companies/{id}",
            put(update_handler).delete(delete_handler),
        )
}

fn forbidden() -> Response {
    status_error(StatusCode::FORBIDDEN, "forbidden")
}

fn bad_request(word: &'static str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": word }))).into_response()
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    let mut pg = state.pg.lock().await;
    match list(&mut pg, tenant_id).await {
        Ok(l) => Json(l).into_response(),
        Err(e) => state.db_fail("companies list", &e).into_response(),
    }
}

/// 本文を検めて保存し、結果を応答にする (POST・PUT 共通。作ったときは 201)
async fn save_handler(state: AppState, tenant_id: Uuid, id: Option<Uuid>, body: &[u8]) -> Response {
    let Ok(input) = serde_json::from_slice::<CompanyInput>(body) else {
        return bad_request("invalid_body");
    };
    let input = match input.normalize() {
        Ok(i) => i,
        Err(word) => return bad_request(word),
    };
    let mut pg = state.pg.lock().await;
    match save(&mut pg, tenant_id, id, input).await {
        Ok(Saved::Done(c)) if id.is_none() => (StatusCode::CREATED, Json(c)).into_response(),
        Ok(Saved::Done(c)) => Json(c).into_response(),
        Ok(Saved::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Ok(Saved::Invalid(word)) => bad_request(word),
        Err(e) if e.code() == Some(&SqlState::UNIQUE_VIOLATION) => bad_request("duplicate_name"),
        Err(e) => state.db_fail("companies save", &e).into_response(),
    }
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    body: axum::body::Bytes,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    save_handler(state, tenant_id, None, &body).await
}

async fn update_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    body: axum::body::Bytes,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    save_handler(state, tenant_id, Some(id), &body).await
}

async fn delete_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, Response> {
    if !is_admin(&auth_user) {
        return Err(forbidden());
    }
    let mut pg = state.pg.lock().await;
    let deleted = delete(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("companies delete", &e).into_response())?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND.into_response())
    }
}
