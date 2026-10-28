//! 点呼の予定 (rust の `alc_tenko::tenko_schedules`・repo/tenko_schedules.rs の写し)。
//!
//! - `POST /tenko/schedules` (201) — 1 件作る。`GET /tenko/schedules` (`?employee_id=&tenko_type=&consumed=&date_from=&date_to=&page=&per_page=`) — 一覧 (予定の新しい順)
//! - `POST /tenko/schedules/batch` (201) — まとめて作る (空は 400)
//! - `GET`・`PUT`・`DELETE /tenko/schedules/{id}` — 1 件・更新 (null の欄は変えない)・削除 (204)。消費済みは更新も削除もしない (404)
//! - `GET /tenko/schedules/pending/{employee_id}` — 乗務員の未消費の予定 (予定の古い順。キオスク)
//!
//! 種類は `pre_operation`・`post_operation` だけ。業務前は指示事項が要る (どちらも外れたら 400)。
//! rust との違い: 一括の作成は 1 つの transaction (途中で失敗したら何も作らない。rust は 1 件ずつ確定していた)。

use alc_core_wasm::{DeviceDevSlot, TenantId};
use alc_worker_db::PgClient;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use const_format::concatcp;
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::models::{paging, schedule, Arg, CreateTenkoSchedule, TenkoSchedule, SCHEDULE_COLUMNS};
use crate::{set_device_dev, AppState};

/// rust の `BatchCreateTenkoSchedules`
#[derive(Debug, Deserialize)]
pub struct BatchCreate {
    pub schedules: Vec<CreateTenkoSchedule>,
}

/// rust の `UpdateTenkoSchedule`
#[derive(Debug, Clone, Deserialize)]
pub struct UpdateSchedule {
    pub responsible_manager_name: Option<String>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub instruction: Option<String>,
}

/// rust の `TenkoScheduleFilter`
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ScheduleFilter {
    pub employee_id: Option<Uuid>,
    pub tenko_type: Option<String>,
    pub consumed: Option<bool>,
    pub date_from: Option<DateTime<Utc>>,
    pub date_to: Option<DateTime<Utc>>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// rust の `TenkoSchedulesResponse`
#[derive(Debug, Serialize)]
pub struct SchedulesResponse {
    pub schedules: Vec<TenkoSchedule>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

pub mod sql {
    use super::*;

    /// 1 件作る。$1 tenant_id / $2 employee_id / $3 tenko_type / $4 responsible_manager_name / $5 scheduled_at / $6 instruction
    pub const INSERT: &str = concatcp!(
        "INSERT INTO alc_api.tenko_schedules (tenant_id, employee_id, tenko_type, responsible_manager_name, scheduled_at, instruction) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING ",
        SCHEDULE_COLUMNS
    );

    /// 絞り込み ($1 tenant_id / $2 employee_id / $3 tenko_type / $4 consumed / $5 date_from / $6 date_to。NULL は絞らない)
    pub const WHERE: &str = "tenant_id = $1 \
         AND ($2::uuid IS NULL OR employee_id = $2) \
         AND ($3::text IS NULL OR tenko_type = $3) \
         AND ($4::bool IS NULL OR consumed = $4) \
         AND ($5::timestamptz IS NULL OR scheduled_at >= $5) \
         AND ($6::timestamptz IS NULL OR scheduled_at <= $6)";

    pub const COUNT: &str = concatcp!("SELECT COUNT(*) FROM alc_api.tenko_schedules WHERE ", WHERE);

    /// 一覧 ($7 件数 / $8 飛ばす件数)
    pub const LIST: &str = concatcp!(
        "SELECT ",
        SCHEDULE_COLUMNS,
        " FROM alc_api.tenko_schedules WHERE ",
        WHERE,
        " ORDER BY scheduled_at DESC LIMIT $7 OFFSET $8"
    );

    /// 1 件。$1 id / $2 tenant_id
    pub const GET: &str = concatcp!(
        "SELECT ",
        SCHEDULE_COLUMNS,
        " FROM alc_api.tenko_schedules WHERE id = $1 AND tenant_id = $2"
    );

    /// 更新 (未消費だけ。null の欄は変えない)。$1 responsible_manager_name / $2 scheduled_at / $3 instruction / $4 id / $5 tenant_id
    pub const UPDATE: &str = concatcp!(
        "UPDATE alc_api.tenko_schedules SET \
             responsible_manager_name = COALESCE($1, responsible_manager_name), \
             scheduled_at = COALESCE($2, scheduled_at), \
             instruction = COALESCE($3, instruction), \
             updated_at = NOW() \
         WHERE id = $4 AND tenant_id = $5 AND consumed = FALSE RETURNING ",
        SCHEDULE_COLUMNS
    );

    /// 削除 (未消費だけ)。$1 id / $2 tenant_id
    pub const DELETE: &str =
        "DELETE FROM alc_api.tenko_schedules WHERE id = $1 AND tenant_id = $2 AND consumed = FALSE";

    /// 乗務員の未消費の予定 (古い順)。$1 tenant_id / $2 employee_id
    pub const PENDING: &str = concatcp!(
        "SELECT ",
        SCHEDULE_COLUMNS,
        " FROM alc_api.tenko_schedules WHERE tenant_id = $1 AND employee_id = $2 AND consumed = FALSE \
         ORDER BY scheduled_at ASC"
    );
}

/// 種類と指示事項の検査 (rust の `validate_schedule`)
pub fn valid_schedule(s: &CreateTenkoSchedule) -> bool {
    match s.tenko_type.as_str() {
        "pre_operation" => s.instruction.as_ref().is_some_and(|i| !i.is_empty()),
        "post_operation" => true,
        _ => false,
    }
}

/// まとめて作る (1 件でも同じ)
pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    inputs: Vec<CreateTenkoSchedule>,
) -> Result<Vec<TenkoSchedule>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let mut out = Vec::with_capacity(inputs.len());
            for s in &inputs {
                let args: [Arg; 6] = [
                    (&tenant_id, Type::UUID),
                    (&s.employee_id, Type::UUID),
                    (&s.tenko_type, Type::TEXT),
                    (&s.responsible_manager_name, Type::TEXT),
                    (&s.scheduled_at, Type::TIMESTAMPTZ),
                    (&s.instruction, Type::TEXT),
                ];
                out.push(schedule(&tx.query_typed_one(sql::INSERT, &args).await?));
            }
            Ok(out)
        })
    })
    .await
}

/// 件数と 1 ページぶん
pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    f: ScheduleFilter,
    limit: i64,
    offset: i64,
) -> Result<(i64, Vec<TenkoSchedule>), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let args: [Arg; 6] = [
                (&tenant_id, Type::UUID),
                (&f.employee_id, Type::UUID),
                (&f.tenko_type, Type::TEXT),
                (&f.consumed, Type::BOOL),
                (&f.date_from, Type::TIMESTAMPTZ),
                (&f.date_to, Type::TIMESTAMPTZ),
            ];
            let total: i64 = tx.query_typed_one(sql::COUNT, &args).await?.get(0);
            let mut page_args = args.to_vec();
            page_args.push((&limit, Type::INT8));
            page_args.push((&offset, Type::INT8));
            let rows = tx.query_typed(sql::LIST, &page_args).await?;
            Ok((total, rows.iter().map(schedule).collect()))
        })
    })
    .await
}

pub async fn get_one(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    id: Uuid,
) -> Result<Option<TenkoSchedule>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let row = tx
                .query_typed_opt(sql::GET, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(row.as_ref().map(schedule))
        })
    })
    .await
}

pub async fn update(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    id: Uuid,
    input: UpdateSchedule,
) -> Result<Option<TenkoSchedule>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let args: [Arg; 5] = [
                (&input.responsible_manager_name, Type::TEXT),
                (&input.scheduled_at, Type::TIMESTAMPTZ),
                (&input.instruction, Type::TEXT),
                (&id, Type::UUID),
                (&tenant_id, Type::UUID),
            ];
            let row = tx.query_typed_opt(sql::UPDATE, &args).await?;
            Ok(row.as_ref().map(schedule))
        })
    })
    .await
}

pub async fn delete(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    id: Uuid,
) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let n = tx
                .execute_typed(sql::DELETE, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub async fn pending(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    employee_id: Uuid,
) -> Result<Vec<TenkoSchedule>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let rows = tx
                .query_typed(
                    sql::PENDING,
                    &[(&tenant_id, Type::UUID), (&employee_id, Type::UUID)],
                )
                .await?;
            Ok(rows.iter().map(schedule).collect())
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tenko/schedules", post(create_handler).get(list_handler))
        .route("/tenko/schedules/batch", post(batch_handler))
        .route(
            "/tenko/schedules/{id}",
            get(get_handler).put(update_handler).delete(delete_handler),
        )
        .route(
            "/tenko/schedules/pending/{employee_id}",
            get(pending_handler),
        )
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Json(body): Json<CreateTenkoSchedule>,
) -> Result<(StatusCode, Json<TenkoSchedule>), StatusCode> {
    if !valid_schedule(&body) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let mut created = create(&mut pg, tenant_id, dev.get(), vec![body])
        .await
        .map_err(|e| state.db_fail("schedule create", &e))?;
    Ok((StatusCode::CREATED, Json(created.remove(0))))
}

async fn batch_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Json(body): Json<BatchCreate>,
) -> Result<(StatusCode, Json<Vec<TenkoSchedule>>), StatusCode> {
    if body.schedules.is_empty() || !body.schedules.iter().all(valid_schedule) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let created = create(&mut pg, tenant_id, dev.get(), body.schedules)
        .await
        .map_err(|e| state.db_fail("schedule batch", &e))?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Query(filter): Query<ScheduleFilter>,
) -> Result<Json<SchedulesResponse>, StatusCode> {
    let (page, per_page, offset) = paging(filter.page, filter.per_page);
    let mut pg = state.pg.lock().await;
    let (total, schedules) = list(&mut pg, tenant_id, dev.get(), filter, per_page, offset)
        .await
        .map_err(|e| state.db_fail("schedule list", &e))?;
    Ok(Json(SchedulesResponse {
        schedules,
        total,
        page,
        per_page,
    }))
}

async fn get_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
) -> Result<Json<TenkoSchedule>, StatusCode> {
    let mut pg = state.pg.lock().await;
    get_one(&mut pg, tenant_id, dev.get(), id)
        .await
        .map_err(|e| state.db_fail("schedule get", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn update_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateSchedule>,
) -> Result<Json<TenkoSchedule>, StatusCode> {
    let mut pg = state.pg.lock().await;
    update(&mut pg, tenant_id, dev.get(), id, body)
        .await
        .map_err(|e| state.db_fail("schedule update", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn delete_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let deleted = delete(&mut pg, tenant_id, dev.get(), id)
        .await
        .map_err(|e| state.db_fail("schedule delete", &e))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn pending_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(employee_id): Path<Uuid>,
) -> Result<Json<Vec<TenkoSchedule>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    pending(&mut pg, tenant_id, dev.get(), employee_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("schedule pending", &e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(tenko_type: &str, instruction: Option<&str>) -> CreateTenkoSchedule {
        CreateTenkoSchedule {
            employee_id: Uuid::nil(),
            tenko_type: tenko_type.into(),
            responsible_manager_name: "m".into(),
            scheduled_at: DateTime::<Utc>::UNIX_EPOCH,
            instruction: instruction.map(str::to_owned),
        }
    }

    #[test]
    fn schedule_validation_matches_rust() {
        assert!(valid_schedule(&input("pre_operation", Some("x"))));
        assert!(!valid_schedule(&input("pre_operation", Some(""))));
        assert!(!valid_schedule(&input("pre_operation", None)));
        assert!(valid_schedule(&input("post_operation", None)));
        assert!(!valid_schedule(&input("other", Some("x"))));
    }
}
