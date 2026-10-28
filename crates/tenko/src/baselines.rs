//! 健康の基準値 (rust の `alc_tenko::health_baselines`・repo/health_baselines.rs の写し)。
//!
//! - `POST /tenko/health-baselines` (201) — 作成か上書き (乗務員ごとに 1 件。省略した値は rust と同じ既定値)
//! - `GET /tenko/health-baselines` — 一覧 (作成の新しい順)
//! - `GET`・`PUT`・`DELETE /tenko/health-baselines/{employee_id}` — 1 件・更新 (null の欄は変えない)・削除 (204)。無ければ 404
//!
//! この表は dev の軸を持たない (rust と同じ)。

use alc_core_wasm::TenantId;
use alc_worker_db::PgClient;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use const_format::concatcp;
use serde::Deserialize;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::models::{baseline, Arg, EmployeeHealthBaseline, BASELINE_COLUMNS};
use crate::AppState;

/// rust の `CreateHealthBaseline`
#[derive(Debug, Clone, Deserialize)]
pub struct CreateBaseline {
    pub employee_id: Uuid,
    pub baseline_systolic: Option<i32>,
    pub baseline_diastolic: Option<i32>,
    pub baseline_temperature: Option<f64>,
    pub systolic_tolerance: Option<i32>,
    pub diastolic_tolerance: Option<i32>,
    pub temperature_tolerance: Option<f64>,
    pub measurement_validity_minutes: Option<i32>,
}

/// rust の `UpdateHealthBaseline`
#[derive(Debug, Clone, Deserialize)]
pub struct UpdateBaseline {
    pub baseline_systolic: Option<i32>,
    pub baseline_diastolic: Option<i32>,
    pub baseline_temperature: Option<f64>,
    pub systolic_tolerance: Option<i32>,
    pub diastolic_tolerance: Option<i32>,
    pub temperature_tolerance: Option<f64>,
    pub measurement_validity_minutes: Option<i32>,
}

pub mod sql {
    use super::*;

    /// 作成か上書き。$1 tenant_id / $2 employee_id / $3〜$9 の値 (既定値は呼び手が埋める)
    pub const UPSERT: &str = concatcp!(
        "INSERT INTO alc_api.employee_health_baselines (tenant_id, employee_id, baseline_systolic, baseline_diastolic, \
             baseline_temperature, systolic_tolerance, diastolic_tolerance, temperature_tolerance, measurement_validity_minutes) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         ON CONFLICT (tenant_id, employee_id) DO UPDATE SET \
             baseline_systolic = EXCLUDED.baseline_systolic, \
             baseline_diastolic = EXCLUDED.baseline_diastolic, \
             baseline_temperature = EXCLUDED.baseline_temperature, \
             systolic_tolerance = EXCLUDED.systolic_tolerance, \
             diastolic_tolerance = EXCLUDED.diastolic_tolerance, \
             temperature_tolerance = EXCLUDED.temperature_tolerance, \
             measurement_validity_minutes = EXCLUDED.measurement_validity_minutes, \
             updated_at = NOW() \
         RETURNING ",
        BASELINE_COLUMNS
    );

    pub const LIST: &str = concatcp!(
        "SELECT ",
        BASELINE_COLUMNS,
        " FROM alc_api.employee_health_baselines WHERE tenant_id = $1 ORDER BY created_at DESC"
    );

    /// $1 tenant_id / $2 employee_id
    pub const GET: &str = concatcp!(
        "SELECT ",
        BASELINE_COLUMNS,
        " FROM alc_api.employee_health_baselines WHERE tenant_id = $1 AND employee_id = $2"
    );

    /// $1 tenant_id / $2 employee_id / $3〜$9 の値 (null は変えない)
    pub const UPDATE: &str = concatcp!(
        "UPDATE alc_api.employee_health_baselines SET \
             baseline_systolic = COALESCE($3, baseline_systolic), \
             baseline_diastolic = COALESCE($4, baseline_diastolic), \
             baseline_temperature = COALESCE($5, baseline_temperature), \
             systolic_tolerance = COALESCE($6, systolic_tolerance), \
             diastolic_tolerance = COALESCE($7, diastolic_tolerance), \
             temperature_tolerance = COALESCE($8, temperature_tolerance), \
             measurement_validity_minutes = COALESCE($9, measurement_validity_minutes), \
             updated_at = NOW() \
         WHERE tenant_id = $1 AND employee_id = $2 RETURNING ",
        BASELINE_COLUMNS
    );

    pub const DELETE: &str =
        "DELETE FROM alc_api.employee_health_baselines WHERE tenant_id = $1 AND employee_id = $2";
}

/// 値の 7 欄 (作成のときは rust と同じ既定値で埋めたもの)
type Values = (
    Option<i32>,
    Option<i32>,
    Option<f64>,
    Option<i32>,
    Option<i32>,
    Option<f64>,
    Option<i32>,
);

/// 作成の既定値 (収縮期 120・拡張期 80・体温 36.5・許容 10/10/0.5・有効 30 分)
pub fn with_defaults(b: &CreateBaseline) -> Values {
    (
        Some(b.baseline_systolic.unwrap_or(120)),
        Some(b.baseline_diastolic.unwrap_or(80)),
        Some(b.baseline_temperature.unwrap_or(36.5)),
        Some(b.systolic_tolerance.unwrap_or(10)),
        Some(b.diastolic_tolerance.unwrap_or(10)),
        Some(b.temperature_tolerance.unwrap_or(0.5)),
        Some(b.measurement_validity_minutes.unwrap_or(30)),
    )
}

/// `UPSERT` か `UPDATE` を流す (引数の形は同じ)
async fn write(
    pg: &mut PgClient,
    statement: &'static str,
    tenant_id: Uuid,
    employee_id: Uuid,
    v: Values,
) -> Result<Option<EmployeeHealthBaseline>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let args: [Arg; 9] = [
                (&tenant_id, Type::UUID),
                (&employee_id, Type::UUID),
                (&v.0, Type::INT4),
                (&v.1, Type::INT4),
                (&v.2, Type::FLOAT8),
                (&v.3, Type::INT4),
                (&v.4, Type::INT4),
                (&v.5, Type::FLOAT8),
                (&v.6, Type::INT4),
            ];
            let row = tx.query_typed_opt(statement, &args).await?;
            Ok(row.as_ref().map(baseline))
        })
    })
    .await
}

pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<EmployeeHealthBaseline>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(baseline).collect())
        })
    })
    .await
}

pub async fn get_one(
    pg: &mut PgClient,
    tenant_id: Uuid,
    employee_id: Uuid,
) -> Result<Option<EmployeeHealthBaseline>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::GET,
                    &[(&tenant_id, Type::UUID), (&employee_id, Type::UUID)],
                )
                .await?;
            Ok(row.as_ref().map(baseline))
        })
    })
    .await
}

pub async fn delete(pg: &mut PgClient, tenant_id: Uuid, employee_id: Uuid) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(
                    sql::DELETE,
                    &[(&tenant_id, Type::UUID), (&employee_id, Type::UUID)],
                )
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/tenko/health-baselines",
            post(upsert_handler).get(list_handler),
        )
        .route(
            "/tenko/health-baselines/{employee_id}",
            get(get_handler).put(update_handler).delete(delete_handler),
        )
}

async fn upsert_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateBaseline>,
) -> Result<(StatusCode, Json<EmployeeHealthBaseline>), StatusCode> {
    let mut pg = state.pg.lock().await;
    let row = write(
        &mut pg,
        sql::UPSERT,
        tenant_id,
        body.employee_id,
        with_defaults(&body),
    )
    .await
    .map_err(|e| state.db_fail("baseline upsert", &e))?;
    // INSERT ... ON CONFLICT DO UPDATE は必ず 1 行を返す
    Ok((
        StatusCode::CREATED,
        Json(row.expect("upsert returns a row")),
    ))
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<EmployeeHealthBaseline>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("baseline list", &e))
}

async fn get_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(employee_id): Path<Uuid>,
) -> Result<Json<EmployeeHealthBaseline>, StatusCode> {
    let mut pg = state.pg.lock().await;
    get_one(&mut pg, tenant_id, employee_id)
        .await
        .map_err(|e| state.db_fail("baseline get", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn update_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(employee_id): Path<Uuid>,
    Json(b): Json<UpdateBaseline>,
) -> Result<Json<EmployeeHealthBaseline>, StatusCode> {
    let values = (
        b.baseline_systolic,
        b.baseline_diastolic,
        b.baseline_temperature,
        b.systolic_tolerance,
        b.diastolic_tolerance,
        b.temperature_tolerance,
        b.measurement_validity_minutes,
    );
    let mut pg = state.pg.lock().await;
    write(&mut pg, sql::UPDATE, tenant_id, employee_id, values)
        .await
        .map_err(|e| state.db_fail("baseline update", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn delete_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(employee_id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let deleted = delete(&mut pg, tenant_id, employee_id)
        .await
        .map_err(|e| state.db_fail("baseline delete", &e))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
