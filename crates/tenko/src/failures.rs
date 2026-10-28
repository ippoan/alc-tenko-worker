//! 機器故障 (rust の `alc_tenko::equipment_failures`・repo/equipment_failures.rs の写し)。
//!
//! - `POST /tenko/equipment-failures` (201) — 記録 (種類は決まった 8 つだけ。外れたら 400。検出の時刻の既定は今)
//! - `GET /tenko/equipment-failures` (`?failure_type=&resolved=&session_id=&date_from=&date_to=&page=&per_page=`) — 一覧 (検出の新しい順)
//! - `GET /tenko/equipment-failures/csv` — 期間 (`date_from`・`date_to`) だけで絞った全件の CSV (BOM 付き)
//! - `GET /tenko/equipment-failures/{id}` — 1 件。`PUT` — 解決の記録 (解決の時刻は今)。無ければ 404
//!
//! rust との違い: 絞り込みは文を組み立てず `($n IS NULL OR ...)` の 1 文にした。

use alc_core_wasm::{DeviceDevSlot, TenantId};
use alc_worker_db::PgClient;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use const_format::concatcp;
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::models::{failure, paging, with_bom, Arg, EquipmentFailure, FAILURE_COLUMNS};
use crate::{set_device_dev, AppState};

/// 記録できる故障の種類 (rust と同じ)
pub const FAILURE_TYPES: [&str; 8] = [
    "face_recognition_error",
    "measurement_recording_failed",
    "kiosk_offline",
    "database_sync_error",
    "webhook_delivery_failed",
    "session_state_error",
    "photo_storage_error",
    "manual_report",
];

/// rust の `CreateEquipmentFailure`
#[derive(Debug, Clone, Deserialize)]
pub struct CreateFailure {
    pub failure_type: String,
    pub description: String,
    pub affected_device: Option<String>,
    pub detected_at: Option<DateTime<Utc>>,
    pub detected_by: Option<String>,
    pub session_id: Option<Uuid>,
}

/// rust の `UpdateEquipmentFailure`
#[derive(Debug, Clone, Deserialize)]
pub struct ResolveFailure {
    pub resolution_notes: Option<String>,
}

/// rust の `EquipmentFailureFilter`
#[derive(Debug, Clone, Default, Deserialize)]
pub struct FailureFilter {
    pub failure_type: Option<String>,
    pub resolved: Option<bool>,
    pub session_id: Option<Uuid>,
    pub date_from: Option<DateTime<Utc>>,
    pub date_to: Option<DateTime<Utc>>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// rust の `EquipmentFailuresResponse`
#[derive(Debug, Serialize)]
pub struct FailuresResponse {
    pub failures: Vec<EquipmentFailure>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

pub mod sql {
    use super::*;

    /// $1 tenant_id / $2 failure_type / $3 description / $4 affected_device / $5 detected_at (NULL は今) / $6 detected_by / $7 session_id
    pub const INSERT: &str = concatcp!(
        "INSERT INTO alc_api.equipment_failures (tenant_id, failure_type, description, affected_device, detected_at, detected_by, session_id) \
         VALUES ($1, $2, $3, $4, COALESCE($5, NOW()), $6, $7) RETURNING ",
        FAILURE_COLUMNS
    );

    /// 絞り込み ($1 tenant_id / $2 failure_type / $3 resolved / $4 session_id / $5 date_from / $6 date_to。NULL は絞らない)
    pub const WHERE: &str = "tenant_id = $1 \
         AND ($2::text IS NULL OR failure_type = $2) \
         AND ($3::bool IS NULL OR (resolved_at IS NOT NULL) = $3) \
         AND ($4::uuid IS NULL OR session_id = $4) \
         AND ($5::timestamptz IS NULL OR detected_at >= $5) \
         AND ($6::timestamptz IS NULL OR detected_at <= $6)";

    pub const COUNT: &str = concatcp!(
        "SELECT COUNT(*) FROM alc_api.equipment_failures WHERE ",
        WHERE
    );

    /// 一覧 ($7 件数 / $8 飛ばす件数)
    pub const LIST: &str = concatcp!(
        "SELECT ",
        FAILURE_COLUMNS,
        " FROM alc_api.equipment_failures WHERE ",
        WHERE,
        " ORDER BY detected_at DESC LIMIT $7 OFFSET $8"
    );

    /// CSV (期間だけで絞る全件。rust と同じ)。$1 tenant_id / $2 date_from / $3 date_to
    pub const CSV: &str = concatcp!(
        "SELECT ",
        FAILURE_COLUMNS,
        " FROM alc_api.equipment_failures WHERE tenant_id = $1 \
         AND ($2::timestamptz IS NULL OR detected_at >= $2) \
         AND ($3::timestamptz IS NULL OR detected_at <= $3) \
         ORDER BY detected_at DESC"
    );

    /// $1 id / $2 tenant_id
    pub const GET: &str = concatcp!(
        "SELECT ",
        FAILURE_COLUMNS,
        " FROM alc_api.equipment_failures WHERE id = $1 AND tenant_id = $2"
    );

    /// 解決の記録。$1 id / $2 tenant_id / $3 resolution_notes
    pub const RESOLVE: &str = concatcp!(
        "UPDATE alc_api.equipment_failures SET resolved_at = NOW(), resolution_notes = $3, updated_at = NOW() \
         WHERE id = $1 AND tenant_id = $2 RETURNING ",
        FAILURE_COLUMNS
    );
}

pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    input: CreateFailure,
) -> Result<EquipmentFailure, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let args: [Arg; 7] = [
                (&tenant_id, Type::UUID),
                (&input.failure_type, Type::TEXT),
                (&input.description, Type::TEXT),
                (&input.affected_device, Type::TEXT),
                (&input.detected_at, Type::TIMESTAMPTZ),
                (&input.detected_by, Type::TEXT),
                (&input.session_id, Type::UUID),
            ];
            Ok(failure(&tx.query_typed_one(sql::INSERT, &args).await?))
        })
    })
    .await
}

/// 件数と 1 ページぶん
pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    f: FailureFilter,
    limit: i64,
    offset: i64,
) -> Result<(i64, Vec<EquipmentFailure>), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let args: [Arg; 6] = [
                (&tenant_id, Type::UUID),
                (&f.failure_type, Type::TEXT),
                (&f.resolved, Type::BOOL),
                (&f.session_id, Type::UUID),
                (&f.date_from, Type::TIMESTAMPTZ),
                (&f.date_to, Type::TIMESTAMPTZ),
            ];
            let total: i64 = tx.query_typed_one(sql::COUNT, &args).await?.get(0);
            let mut page_args = args.to_vec();
            page_args.push((&limit, Type::INT8));
            page_args.push((&offset, Type::INT8));
            let rows = tx.query_typed(sql::LIST, &page_args).await?;
            Ok((total, rows.iter().map(failure).collect()))
        })
    })
    .await
}

pub async fn list_for_csv(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    f: FailureFilter,
) -> Result<Vec<EquipmentFailure>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let rows = tx
                .query_typed(
                    sql::CSV,
                    &[
                        (&tenant_id, Type::UUID),
                        (&f.date_from, Type::TIMESTAMPTZ),
                        (&f.date_to, Type::TIMESTAMPTZ),
                    ],
                )
                .await?;
            Ok(rows.iter().map(failure).collect())
        })
    })
    .await
}

pub async fn get_one(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    id: Uuid,
) -> Result<Option<EquipmentFailure>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let row = tx
                .query_typed_opt(sql::GET, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(row.as_ref().map(failure))
        })
    })
    .await
}

pub async fn resolve(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    id: Uuid,
    input: ResolveFailure,
) -> Result<Option<EquipmentFailure>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let row = tx
                .query_typed_opt(
                    sql::RESOLVE,
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&input.resolution_notes, Type::TEXT),
                    ],
                )
                .await?;
            Ok(row.as_ref().map(failure))
        })
    })
    .await
}

/// CSV の見出し (rust と同じ 9 列)
pub const CSV_HEADER: [&str; 9] = [
    "id",
    "failure_type",
    "description",
    "affected_device",
    "detected_at",
    "detected_by",
    "resolved_at",
    "resolution_notes",
    "session_id",
];

/// 機器故障の CSV (見出し + 行、BOM 付き)。csv の writer はメモリの Vec に書くので失敗しない
pub fn render_csv(failures: &[EquipmentFailure]) -> Vec<u8> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    wtr.write_record(CSV_HEADER).expect("csv to memory");
    for f in failures {
        wtr.write_record([
            f.id.to_string(),
            f.failure_type.clone(),
            f.description.clone(),
            f.affected_device.clone().unwrap_or_default(),
            f.detected_at.to_rfc3339(),
            f.detected_by.clone().unwrap_or_default(),
            f.resolved_at.map_or(String::new(), |t| t.to_rfc3339()),
            f.resolution_notes.clone().unwrap_or_default(),
            f.session_id.map_or(String::new(), |s| s.to_string()),
        ])
        .expect("csv to memory");
    }
    with_bom(wtr.into_inner().expect("csv to memory"))
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/tenko/equipment-failures",
            post(create_handler).get(list_handler),
        )
        .route("/tenko/equipment-failures/csv", get(csv_handler))
        .route(
            "/tenko/equipment-failures/{id}",
            get(get_handler).put(resolve_handler),
        )
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Json(body): Json<CreateFailure>,
) -> Result<(StatusCode, Json<EquipmentFailure>), StatusCode> {
    if !FAILURE_TYPES.contains(&body.failure_type.as_str()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let row = create(&mut pg, tenant_id, dev.get(), body)
        .await
        .map_err(|e| state.db_fail("failure create", &e))?;
    Ok((StatusCode::CREATED, Json(row)))
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Query(filter): Query<FailureFilter>,
) -> Result<Json<FailuresResponse>, StatusCode> {
    let (page, per_page, offset) = paging(filter.page, filter.per_page);
    let mut pg = state.pg.lock().await;
    let (total, failures) = list(&mut pg, tenant_id, dev.get(), filter, per_page, offset)
        .await
        .map_err(|e| state.db_fail("failure list", &e))?;
    Ok(Json(FailuresResponse {
        failures,
        total,
        page,
        per_page,
    }))
}

async fn csv_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Query(filter): Query<FailureFilter>,
) -> Result<impl IntoResponse, StatusCode> {
    let mut pg = state.pg.lock().await;
    let rows = list_for_csv(&mut pg, tenant_id, dev.get(), filter)
        .await
        .map_err(|e| state.db_fail("failure csv", &e))?;
    Ok((
        [
            (header::CONTENT_TYPE, "text/csv; charset=utf-8"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"equipment_failures.csv\"",
            ),
        ],
        render_csv(&rows),
    ))
}

async fn get_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
) -> Result<Json<EquipmentFailure>, StatusCode> {
    let mut pg = state.pg.lock().await;
    get_one(&mut pg, tenant_id, dev.get(), id)
        .await
        .map_err(|e| state.db_fail("failure get", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn resolve_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<ResolveFailure>,
) -> Result<Json<EquipmentFailure>, StatusCode> {
    let mut pg = state.pg.lock().await;
    resolve(&mut pg, tenant_id, dev.get(), id, body)
        .await
        .map_err(|e| state.db_fail("failure resolve", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}
