//! 点呼のダッシュボード (rust の `alc_tenko::tenko_sessions` の `dashboard` と repo の写し)。
//!
//! - `GET /tenko/dashboard` — 未消費の予定の数・進行中と中断中のセッションの数・今日の完了と取消の数と、
//!   超過した予定 (古い順)
//!
//! 「今日」は DB の `CURRENT_DATE` (rust と同じ)。超過の規則は予定超過の通知と同じ ([`crate::overdue::sql::IS_OVERDUE`]):
//! 予定の時刻 + テナントの分数 (`tenko_settings.overdue_minutes`、行が無ければ 60) を過ぎても点呼が完了していない予定
//! (未消費、または消費したセッションが completed・cancelled 以外。rust は未消費だけを定数 60 分で見ていた)。

use alc_core_wasm::{DeviceDevSlot, TenantId};
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use const_format::concatcp;
use serde::Serialize;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::models::{schedule, TenkoSchedule, SCHEDULE_COLUMNS};
use crate::overdue::sql::IS_OVERDUE;
use crate::{set_device_dev, AppState};

/// rust の `TenkoDashboard`
#[derive(Debug, PartialEq, Serialize)]
pub struct Dashboard {
    pub pending_schedules: i64,
    pub active_sessions: i64,
    pub interrupted_sessions: i64,
    pub completed_today: i64,
    pub cancelled_today: i64,
    pub overdue_schedules: Vec<TenkoSchedule>,
}

impl TxOutput for Dashboard {}

pub mod sql {
    use super::*;

    /// 5 つの件数を 1 文で。$1 tenant_id
    pub const COUNTS: &str = "SELECT \
         (SELECT COUNT(*) FROM alc_api.tenko_schedules WHERE tenant_id = $1 AND consumed = FALSE), \
         (SELECT COUNT(*) FROM alc_api.tenko_sessions WHERE tenant_id = $1 AND status NOT IN ('completed', 'cancelled', 'interrupted')), \
         (SELECT COUNT(*) FROM alc_api.tenko_sessions WHERE tenant_id = $1 AND status = 'interrupted'), \
         (SELECT COUNT(*) FROM alc_api.tenko_sessions WHERE tenant_id = $1 AND status = 'completed' AND completed_at >= CURRENT_DATE), \
         (SELECT COUNT(*) FROM alc_api.tenko_sessions WHERE tenant_id = $1 AND status = 'cancelled' AND completed_at >= CURRENT_DATE)";

    /// 超過した予定 (古い順。規則は [`IS_OVERDUE`] = 通知と同じ)。$1 tenant_id。列は `SCHEDULE_COLUMNS` を `s.` つきで
    pub const OVERDUE: &str = concatcp!(
        "SELECT ",
        SCHEDULE_COLUMNS,
        " FROM alc_api.tenko_schedules s WHERE s.tenant_id = $1 AND ",
        IS_OVERDUE,
        " ORDER BY s.scheduled_at ASC"
    );
}

pub async fn dashboard(pg: &mut PgClient, tenant_id: Uuid, dev: bool) -> Result<Dashboard, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let c = tx
                .query_typed_one(sql::COUNTS, &[(&tenant_id, Type::UUID)])
                .await?;
            let rows = tx
                .query_typed(sql::OVERDUE, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(Dashboard {
                pending_schedules: c.get(0),
                active_sessions: c.get(1),
                interrupted_sessions: c.get(2),
                completed_today: c.get(3),
                cancelled_today: c.get(4),
                overdue_schedules: rows.iter().map(schedule).collect(),
            })
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new().route("/tenko/dashboard", get(handler))
}

async fn handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
) -> Result<Json<Dashboard>, StatusCode> {
    let mut pg = state.pg.lock().await;
    dashboard(&mut pg, tenant_id, dev.get())
        .await
        .map(Json)
        .map_err(|e| state.db_fail("dashboard", &e))
}
