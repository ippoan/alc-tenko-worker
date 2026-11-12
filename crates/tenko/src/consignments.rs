//! 点呼の受委託 (alc-migrations 214 の `tenko_consignments`)。事業者間遠隔点呼の受委託 1 件 =
//! 委託 (車両の営業所) → 受託 (運行管理者の営業所) の向き。双方向は 2 行。点呼の判定 (alc-measurements-worker) はまだ読まない。
//! rust-alc-api には無い口。
//!
//! - `GET /tenko/consignments` — `[{"id", "consignor_office_id", "consignee_office_id", "arrive_id", "permit_no", "permit_date", "valid_from", "valid_to"}]`
//!   (日付は `YYYY-MM-DD` か null。`created_at` の順)
//! - `POST /tenko/consignments` — 本文は id を除く上の欄 (`arrive_id`・`permit_*`・`valid_*` は省略で null) → 201 で 1 件
//! - `PUT /tenko/consignments/{id}` — 全部置き換え → 200 で 1 件。無ければ 404
//! - `DELETE /tenko/consignments/{id}` — 204。無ければ 404
//!
//! 検める欄: 本文が読めない `invalid_body` / 営業所がテナントに無い `invalid_office` /
//! 委託と受託の営業所に事業者が付いていない `office_without_company` / 2 つの事業者が同じ `same_company` /
//! 到達番号の形 `invalid_arrive_id` ([`crate::filing_archives::is_arrive_id`]) / 期間が片方だけ・逆順・5 年以上 `invalid_period` /
//! 許可番号が 100 文字超 `too_long` (前後の空白を落とし、空は null)。外れたら 400 `{"error": "<語>"}`。
//!
//! **4 口とも admin だけ** (filing ([`crate::filing`]) と同じ判定)。この表は dev の軸を持たない。

use alc_core_wasm::{AuthUser, TenantId};
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Extension, Json, Router};
use chrono::{Months, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::filing::{error as status_error, is_admin};
use crate::filing_archives::is_arrive_id;
use crate::models::Arg;
use crate::AppState;

/// 許可番号の文字数の上限
pub const MAX_PERMIT_CHARS: usize = 100;

/// POST・PUT の本文
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ConsignmentInput {
    pub consignor_office_id: Uuid,
    pub consignee_office_id: Uuid,
    #[serde(default)]
    pub arrive_id: Option<String>,
    #[serde(default)]
    pub permit_no: Option<String>,
    #[serde(default)]
    pub permit_date: Option<NaiveDate>,
    #[serde(default)]
    pub valid_from: Option<NaiveDate>,
    #[serde(default)]
    pub valid_to: Option<NaiveDate>,
}

/// 受委託 1 件
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Consignment {
    pub id: Uuid,
    pub consignor_office_id: Uuid,
    pub consignee_office_id: Uuid,
    pub arrive_id: Option<String>,
    pub permit_no: Option<String>,
    pub permit_date: Option<NaiveDate>,
    pub valid_from: Option<NaiveDate>,
    pub valid_to: Option<NaiveDate>,
}

impl TxOutput for Consignment {}

/// 保存の結果 (検査と 404 は書く前に決まる)
#[derive(Debug, Clone, PartialEq)]
pub enum Saved {
    Done(Consignment),
    NotFound,
    Invalid(&'static str),
}

impl TxOutput for Saved {}

impl ConsignmentInput {
    /// 空白を落として検める。外れた欄の語を返す
    pub fn normalize(mut self) -> Result<Self, &'static str> {
        if self
            .arrive_id
            .as_deref()
            .is_some_and(|id| !is_arrive_id(id))
        {
            return Err("invalid_arrive_id");
        }
        match (self.valid_from, self.valid_to) {
            (None, None) => {}
            (Some(from), Some(to)) => {
                // 表の CHECK と同じ: to >= from かつ to < from + 5 年
                if to < from
                    || from
                        .checked_add_months(Months::new(60))
                        .is_none_or(|end| to >= end)
                {
                    return Err("invalid_period");
                }
            }
            _ => return Err("invalid_period"),
        }
        self.permit_no = self
            .permit_no
            .take()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty());
        if self
            .permit_no
            .as_ref()
            .is_some_and(|s| s.chars().count() > MAX_PERMIT_CHARS)
        {
            return Err("too_long");
        }
        Ok(self)
    }
}

pub mod sql {
    /// 列の並び ([`super::consignment`] が読む順)
    pub const COLUMNS: &str =
        "id, consignor_office_id, consignee_office_id, arrive_id, permit_no, \
         permit_date, valid_from, valid_to";

    /// $1 tenant_id
    pub const LIST: &str = const_format::concatcp!(
        "SELECT ",
        COLUMNS,
        " FROM alc_api.tenko_consignments WHERE tenant_id = $1 ORDER BY created_at, id"
    );

    /// 営業所の事業者 (営業所が無ければ行が無い)。$1 tenant_id / $2 営業所の id
    pub const OFFICE_COMPANY: &str =
        "SELECT company_id FROM alc_api.tenko_offices WHERE tenant_id = $1 AND id = $2";

    /// $1 tenant_id / $2 委託 / $3 受託 / $4 arrive_id / $5 permit_no / $6 permit_date / $7 valid_from / $8 valid_to
    pub const INSERT: &str = const_format::concatcp!(
        "INSERT INTO alc_api.tenko_consignments (tenant_id, consignor_office_id, consignee_office_id, arrive_id, \
         permit_no, permit_date, valid_from, valid_to) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING ",
        COLUMNS
    );

    /// $1 tenant_id / $2 id / $3 委託 / $4 受託 / $5 arrive_id / $6 permit_no / $7 permit_date / $8 valid_from / $9 valid_to
    pub const UPDATE: &str = const_format::concatcp!(
        "UPDATE alc_api.tenko_consignments SET consignor_office_id = $3, consignee_office_id = $4, arrive_id = $5, \
         permit_no = $6, permit_date = $7, valid_from = $8, valid_to = $9, updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2 RETURNING ",
        COLUMNS
    );

    /// $1 id / $2 tenant_id
    pub const DELETE: &str =
        "DELETE FROM alc_api.tenko_consignments WHERE id = $1 AND tenant_id = $2";
}

fn consignment(r: &Row) -> Consignment {
    Consignment {
        id: r.get(0),
        consignor_office_id: r.get(1),
        consignee_office_id: r.get(2),
        arrive_id: r.get(3),
        permit_no: r.get(4),
        permit_date: r.get(5),
        valid_from: r.get(6),
        valid_to: r.get(7),
    }
}

pub async fn list(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<Consignment>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(consignment).collect())
        })
    })
    .await
}

/// 営業所の事業者。営業所がテナントに無ければ `None`、事業者が付いていなければ `Some(None)`
async fn office_company(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    office_id: Uuid,
) -> Result<Option<Option<Uuid>>, Error> {
    let row = tx
        .query_typed_opt(
            sql::OFFICE_COMPANY,
            &[(&tenant_id, Type::UUID), (&office_id, Type::UUID)],
        )
        .await?;
    Ok(row.map(|r| r.get(0)))
}

/// 受委託を作る (`id` が None) か、全部置き換える (`id` が在る)。`input` は [`ConsignmentInput::normalize`] を通したもの。
/// 営業所の検査が外れたら何も書かない
pub async fn save(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Option<Uuid>,
    input: ConsignmentInput,
) -> Result<Saved, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let Some(consignor) = office_company(tx, tenant_id, input.consignor_office_id).await?
            else {
                return Ok(Saved::Invalid("invalid_office"));
            };
            let Some(consignee) = office_company(tx, tenant_id, input.consignee_office_id).await?
            else {
                return Ok(Saved::Invalid("invalid_office"));
            };
            let (Some(consignor), Some(consignee)) = (consignor, consignee) else {
                return Ok(Saved::Invalid("office_without_company"));
            };
            if consignor == consignee {
                return Ok(Saved::Invalid("same_company"));
            }
            let row = match id {
                Some(id) => {
                    let args: [Arg; 9] = [
                        (&tenant_id, Type::UUID),
                        (&id, Type::UUID),
                        (&input.consignor_office_id, Type::UUID),
                        (&input.consignee_office_id, Type::UUID),
                        (&input.arrive_id, Type::TEXT),
                        (&input.permit_no, Type::TEXT),
                        (&input.permit_date, Type::DATE),
                        (&input.valid_from, Type::DATE),
                        (&input.valid_to, Type::DATE),
                    ];
                    match tx.query_typed_opt(sql::UPDATE, &args).await? {
                        Some(r) => r,
                        None => return Ok(Saved::NotFound),
                    }
                }
                None => {
                    let args: [Arg; 8] = [
                        (&tenant_id, Type::UUID),
                        (&input.consignor_office_id, Type::UUID),
                        (&input.consignee_office_id, Type::UUID),
                        (&input.arrive_id, Type::TEXT),
                        (&input.permit_no, Type::TEXT),
                        (&input.permit_date, Type::DATE),
                        (&input.valid_from, Type::DATE),
                        (&input.valid_to, Type::DATE),
                    ];
                    tx.query_typed_one(sql::INSERT, &args).await?
                }
            };
            Ok(Saved::Done(consignment(&row)))
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
        .route(
            "/tenko/consignments",
            get(list_handler).post(create_handler),
        )
        .route(
            "/tenko/consignments/{id}",
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
        Err(e) => state.db_fail("consignments list", &e).into_response(),
    }
}

/// 本文を検めて保存し、結果を応答にする (POST・PUT 共通。作ったときは 201)
async fn save_handler(state: AppState, tenant_id: Uuid, id: Option<Uuid>, body: &[u8]) -> Response {
    let Ok(input) = serde_json::from_slice::<ConsignmentInput>(body) else {
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
        Err(e) => state.db_fail("consignments save", &e).into_response(),
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
        .map_err(|e| state.db_fail("consignments delete", &e).into_response())?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND.into_response())
    }
}
