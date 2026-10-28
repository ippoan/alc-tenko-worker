//! 点呼の電話 (rust の `alc_tenko::tenko_call`・repo/tenko_call.rs の写し)。
//!
//! アプリの口 (テナントのヘッダーを要らない。rust でも公開の口):
//! - `POST /tenko-call/register` — 乗務員の登録 (電話番号ごとに上書き)。テナントは点呼用の番号から引く。番号が無ければ 400
//! - `POST /tenko-call/tenko` — 点呼の位置を記録する (`tenko_call_logs`)。テナントは登録済みの電話番号から引く。無ければ 404
//!
//! 管理画面の口 (テナント付き):
//! - `GET`・`POST /tenko-call/numbers`、`DELETE /tenko-call/numbers/{id}` (204。他のテナントの番号は 0 行のまま 204)
//! - `GET /tenko-call/drivers`
//!
//! 番号・乗務員の表は SELECT が全部の行に開いている (`USING (true)`。認証の前に番号・電話番号で引くため)。
//! テナントの決まる前の文はそれを読む文だけで、nil のテナントの transaction で流す。書き込みは引いたテナントの transaction で流す
//! (`tenant_id` は text の列で、テナントの UUID の文字列が入る)。
//!
//! rust との違い:
//! - 番号・乗務員の一覧は呼び手のテナントで絞る。rust は絞らず、全部のテナントの行を返していた
//! - 表の `tenant_id` が UUID として読めないときは 500 (rust は文字列のまま RLS に渡していた)

use alc_core_wasm::TenantId;
use alc_worker_db::PgClient;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub phone_number: String,
    pub driver_name: String,
    pub call_number: String,
    pub employee_code: Option<String>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct RegisterResponse {
    pub success: bool,
    pub driver_id: i32,
    pub call_number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TenkoRequest {
    pub phone_number: String,
    pub driver_name: String,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Serialize)]
pub struct TenkoResponse {
    pub success: bool,
    pub call_number: Option<String>,
}

/// rust の `TenkoCallNumber` (作成の時刻は DB の text の形のまま)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CallNumber {
    pub id: i32,
    pub call_number: String,
    pub tenant_id: String,
    pub label: Option<String>,
    pub created_at: String,
}

/// rust の `TenkoCallDriver`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CallDriver {
    pub id: i32,
    pub phone_number: String,
    pub driver_name: String,
    pub call_number: Option<String>,
    pub tenant_id: String,
    pub employee_code: Option<String>,
    pub created_at: String,
}

/// 旧い client は `tenant_id` を送ってくるが無視する (テナントは認証済みのものだけを使う。rust と同じ)
#[derive(Debug, Deserialize)]
pub struct CreateNumberRequest {
    pub call_number: String,
    pub label: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateNumberResponse {
    pub success: bool,
    pub id: i32,
}

impl alc_worker_db::TxOutput for CallNumber {}
impl alc_worker_db::TxOutput for CallDriver {}

pub mod sql {
    /// 点呼用の番号のテナント ($1 call_number)
    pub const NUMBER_TENANT: &str =
        "SELECT tenant_id FROM alc_api.tenko_call_numbers WHERE call_number = $1";

    /// 乗務員の登録 (電話番号ごとに上書き)。$1 phone_number / $2 driver_name / $3 call_number / $4 tenant_id (text) / $5 employee_code
    pub const UPSERT_DRIVER: &str = "INSERT INTO alc_api.tenko_call_drivers (phone_number, driver_name, call_number, tenant_id, employee_code, updated_at) \
         VALUES ($1, $2, $3, $4, $5, now()) \
         ON CONFLICT (phone_number) DO UPDATE SET \
             driver_name = $2, call_number = $3, tenant_id = $4, employee_code = $5, updated_at = now() \
         RETURNING id, call_number";

    /// 登録済みの乗務員 ($1 phone_number) → id, call_number, tenant_id
    pub const DRIVER_BY_PHONE: &str =
        "SELECT id, call_number, tenant_id FROM alc_api.tenko_call_drivers WHERE phone_number = $1";

    /// 点呼の位置。$1 driver_id / $2 phone_number / $3 driver_name / $4 latitude / $5 longitude
    pub const INSERT_LOG: &str = "INSERT INTO alc_api.tenko_call_logs (driver_id, phone_number, driver_name, latitude, longitude) \
         VALUES ($1, $2, $3, $4, $5)";

    /// $1 tenant_id (text)
    pub const LIST_NUMBERS: &str = "SELECT id, call_number, tenant_id, label, created_at::text \
         FROM alc_api.tenko_call_numbers WHERE tenant_id = $1 ORDER BY id";

    /// $1 call_number / $2 tenant_id (text) / $3 label
    pub const INSERT_NUMBER: &str =
        "INSERT INTO alc_api.tenko_call_numbers (call_number, tenant_id, label) VALUES ($1, $2, $3) RETURNING id";

    /// $1 id / $2 tenant_id (text)
    pub const DELETE_NUMBER: &str =
        "DELETE FROM alc_api.tenko_call_numbers WHERE id = $1 AND tenant_id = $2";

    /// $1 tenant_id (text)
    pub const LIST_DRIVERS: &str = "SELECT id, phone_number, driver_name, call_number, tenant_id, employee_code, created_at::text \
         FROM alc_api.tenko_call_drivers WHERE tenant_id = $1 ORDER BY id";
}

/// 引いた行のテナントの問題 (DB の失敗とは分けて数える)
#[derive(Debug)]
pub enum CallError {
    Db(Error),
    /// 表の `tenant_id` が UUID として読めない
    BadTenant,
}

impl From<Error> for CallError {
    fn from(e: Error) -> Self {
        Self::Db(e)
    }
}

/// 点呼用の番号のテナント (text)。番号の表は SELECT が全部の行に開いているので、nil のテナントの transaction で読む
async fn number_tenant(pg: &mut PgClient, call_number: String) -> Result<Option<String>, Error> {
    pg.tenant_tx(Uuid::nil(), move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::NUMBER_TENANT, &[(&call_number, Type::TEXT)])
                .await?;
            Ok(row.map(|r| r.get(0)))
        })
    })
    .await
}

/// 登録済みの乗務員 (id, 点呼用の番号, テナント (text))。乗務員の表も SELECT が全部の行に開いている
async fn driver_by_phone(
    pg: &mut PgClient,
    phone_number: String,
) -> Result<Option<(i32, Option<String>, String)>, Error> {
    pg.tenant_tx(Uuid::nil(), move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::DRIVER_BY_PHONE, &[(&phone_number, Type::TEXT)])
                .await?;
            Ok(row.map(|r| (r.get(0), r.get(1), r.get(2))))
        })
    })
    .await
}

fn parse_tenant(text: &str) -> Result<Uuid, CallError> {
    Uuid::parse_str(text).map_err(|_| CallError::BadTenant)
}

/// 乗務員の登録。番号が無ければ None
pub async fn register(
    pg: &mut PgClient,
    req: RegisterRequest,
) -> Result<Option<(i32, Option<String>)>, CallError> {
    let Some(tenant_text) = number_tenant(pg, req.call_number.clone()).await? else {
        return Ok(None);
    };
    let tenant_id = parse_tenant(&tenant_text)?;
    let row = pg
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                let row = tx
                    .query_typed_one(
                        sql::UPSERT_DRIVER,
                        &[
                            (&req.phone_number, Type::TEXT),
                            (&req.driver_name, Type::TEXT),
                            (&req.call_number, Type::TEXT),
                            (&tenant_text, Type::TEXT),
                            (&req.employee_code, Type::TEXT),
                        ],
                    )
                    .await?;
                Ok((row.get::<_, i32>(0), row.get::<_, Option<String>>(1)))
            })
        })
        .await?;
    Ok(Some(row))
}

/// 点呼の位置の記録。登録が無ければ None。返すのは乗務員の点呼用の番号
pub async fn record_tenko(
    pg: &mut PgClient,
    req: TenkoRequest,
) -> Result<Option<Option<String>>, CallError> {
    let Some((driver_id, call_number, tenant_text)) =
        driver_by_phone(pg, req.phone_number.clone()).await?
    else {
        return Ok(None);
    };
    let tenant_id = parse_tenant(&tenant_text)?;
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::INSERT_LOG,
                &[
                    (&driver_id, Type::INT4),
                    (&req.phone_number, Type::TEXT),
                    (&req.driver_name, Type::TEXT),
                    (&req.latitude, Type::FLOAT8),
                    (&req.longitude, Type::FLOAT8),
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    Ok(Some(call_number))
}

pub async fn list_numbers(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<CallNumber>, Error> {
    let tenant_text = tenant_id.to_string();
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_NUMBERS, &[(&tenant_text, Type::TEXT)])
                .await?;
            Ok(rows
                .iter()
                .map(|r| CallNumber {
                    id: r.get(0),
                    call_number: r.get(1),
                    tenant_id: r.get(2),
                    label: r.get(3),
                    created_at: r.get(4),
                })
                .collect())
        })
    })
    .await
}

pub async fn create_number(
    pg: &mut PgClient,
    tenant_id: Uuid,
    req: CreateNumberRequest,
) -> Result<i32, Error> {
    let tenant_text = tenant_id.to_string();
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::INSERT_NUMBER,
                    &[
                        (&req.call_number, Type::TEXT),
                        (&tenant_text, Type::TEXT),
                        (&req.label, Type::TEXT),
                    ],
                )
                .await?;
            Ok(row.get::<_, i32>(0))
        })
    })
    .await
}

pub async fn delete_number(pg: &mut PgClient, tenant_id: Uuid, id: i32) -> Result<(), Error> {
    let tenant_text = tenant_id.to_string();
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::DELETE_NUMBER,
                &[(&id, Type::INT4), (&tenant_text, Type::TEXT)],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

pub async fn list_drivers(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<CallDriver>, Error> {
    let tenant_text = tenant_id.to_string();
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_DRIVERS, &[(&tenant_text, Type::TEXT)])
                .await?;
            Ok(rows
                .iter()
                .map(|r| CallDriver {
                    id: r.get(0),
                    phone_number: r.get(1),
                    driver_name: r.get(2),
                    call_number: r.get(3),
                    tenant_id: r.get(4),
                    employee_code: r.get(5),
                    created_at: r.get(6),
                })
                .collect())
        })
    })
    .await
}

/// アプリの口 (テナントのヘッダーを要らない)
pub fn public_router() -> Router<AppState> {
    Router::new()
        .route("/tenko-call/register", post(register_handler))
        .route("/tenko-call/tenko", post(tenko_handler))
}

/// 管理画面の口 (lib.rs がテナントの layer を掛ける)
pub fn tenant_router() -> Router<AppState> {
    Router::new()
        .route(
            "/tenko-call/numbers",
            get(list_numbers_handler).post(create_number_handler),
        )
        .route("/tenko-call/numbers/{id}", delete(delete_number_handler))
        .route("/tenko-call/drivers", get(list_drivers_handler))
}

impl AppState {
    /// 点呼の電話の失敗を固定の語でログに出し、500 にする
    fn call_fail(&self, stage: &str, e: &CallError) -> StatusCode {
        match e {
            CallError::Db(e) => self.db_fail(stage, e),
            CallError::BadTenant => {
                (self.log)(&format!("tenko: {stage} failed: tenant is not a uuid"));
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

fn register_failure(error: &str) -> Json<RegisterResponse> {
    Json(RegisterResponse {
        success: false,
        driver_id: 0,
        call_number: None,
        error: Some(error.into()),
    })
}

async fn register_handler(
    State(state): State<AppState>,
    Json(body): Json<RegisterRequest>,
) -> Result<Json<RegisterResponse>, (StatusCode, Json<RegisterResponse>)> {
    let mut pg = state.pg.lock().await;
    match register(&mut pg, body).await {
        Ok(Some((driver_id, call_number))) => Ok(Json(RegisterResponse {
            success: true,
            driver_id,
            call_number,
            error: None,
        })),
        Ok(None) => Err((
            StatusCode::BAD_REQUEST,
            register_failure("未登録の点呼用番号です"),
        )),
        Err(e) => Err((
            state.call_fail("call register", &e),
            register_failure("internal error"),
        )),
    }
}

async fn tenko_handler(
    State(state): State<AppState>,
    Json(body): Json<TenkoRequest>,
) -> Result<Json<TenkoResponse>, StatusCode> {
    let mut pg = state.pg.lock().await;
    match record_tenko(&mut pg, body).await {
        Ok(Some(call_number)) => Ok(Json(TenkoResponse {
            success: true,
            call_number,
        })),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => Err(state.call_fail("call tenko", &e)),
    }
}

async fn list_numbers_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<CallNumber>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list_numbers(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("call numbers", &e))
}

async fn create_number_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateNumberRequest>,
) -> Result<Json<CreateNumberResponse>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let id = create_number(&mut pg, tenant_id, body)
        .await
        .map_err(|e| state.db_fail("call number create", &e))?;
    Ok(Json(CreateNumberResponse { success: true, id }))
}

async fn delete_number_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<i32>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    delete_number(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("call number delete", &e))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_drivers_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<CallDriver>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list_drivers(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("call drivers", &e))
}
