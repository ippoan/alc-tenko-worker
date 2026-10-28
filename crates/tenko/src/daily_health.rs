//! 日々の健康状態 (rust の `alc_tenko::daily_health`・repo/daily_health.rs の写し)。
//!
//! - `GET /tenko/daily-health-status` (`?date=YYYY-MM-DD`、省略は JST の今日) — 在籍の社員ごとに、その日 (JST) の最後の完了した
//!   点呼の測定値・自己申告・安全の判定と健康の基準値。並びは判定が fail → 未点呼 → ほか、同じなら名前の順。件数のまとめつき

use alc_core_wasm::{DeviceDevSlot, TenantId};
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::{set_device_dev, AppState};

#[derive(Debug, Deserialize)]
pub struct DailyHealthFilter {
    pub date: Option<NaiveDate>,
}

/// rust の `DailyHealthRow`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DailyHealthRow {
    pub employee_id: Uuid,
    pub employee_name: String,
    pub employee_code: Option<String>,
    pub session_id: Option<Uuid>,
    pub tenko_type: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub temperature: Option<f64>,
    pub systolic: Option<i32>,
    pub diastolic: Option<i32>,
    pub pulse: Option<i32>,
    pub medical_measured_at: Option<DateTime<Utc>>,
    pub medical_manual_input: Option<bool>,
    pub alcohol_result: Option<String>,
    pub alcohol_value: Option<f64>,
    pub self_declaration: Option<Value>,
    pub safety_judgment: Option<Value>,
    pub has_baseline: Option<bool>,
    pub baseline_systolic: Option<i32>,
    pub baseline_diastolic: Option<i32>,
    pub baseline_temperature: Option<f64>,
    pub systolic_tolerance: Option<i32>,
    pub diastolic_tolerance: Option<i32>,
    pub temperature_tolerance: Option<f64>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct DailyHealthSummary {
    pub total_employees: i64,
    pub checked_count: i64,
    pub unchecked_count: i64,
    pub pass_count: i64,
    pub fail_count: i64,
}

#[derive(Debug, Serialize)]
pub struct DailyHealthResponse {
    pub date: NaiveDate,
    pub employees: Vec<DailyHealthRow>,
    pub summary: DailyHealthSummary,
}

impl TxOutput for DailyHealthRow {}

/// $1 tenant_id / $2 日付 (JST の 1 日 = UTC の前日 15:00 から当日 15:00)
pub const SQL: &str = "SELECT e.id, e.name, e.code, s.id, s.tenko_type, s.completed_at, s.temperature, s.systolic, \
         s.diastolic, s.pulse, s.medical_measured_at, s.medical_manual_input, s.alcohol_result, s.alcohol_value, \
         s.self_declaration, s.safety_judgment, (b.id IS NOT NULL), b.baseline_systolic, b.baseline_diastolic, \
         b.baseline_temperature, b.systolic_tolerance, b.diastolic_tolerance, b.temperature_tolerance \
     FROM alc_api.employees e \
     LEFT JOIN LATERAL ( \
         SELECT * FROM alc_api.tenko_sessions ts \
         WHERE ts.employee_id = e.id AND ts.tenant_id = $1 AND ts.status = 'completed' \
           AND ts.completed_at >= ($2::date - INTERVAL '9 hours') \
           AND ts.completed_at < ($2::date + INTERVAL '15 hours') \
         ORDER BY ts.completed_at DESC LIMIT 1 \
     ) s ON true \
     LEFT JOIN alc_api.employee_health_baselines b ON b.employee_id = e.id AND b.tenant_id = $1 \
     WHERE e.tenant_id = $1 AND e.deleted_at IS NULL \
     ORDER BY CASE \
         WHEN s.id IS NOT NULL AND (s.safety_judgment->>'status') = 'fail' THEN 0 \
         WHEN s.id IS NULL THEN 1 \
         ELSE 2 END, e.name";

fn row(r: &Row) -> DailyHealthRow {
    DailyHealthRow {
        employee_id: r.get(0),
        employee_name: r.get(1),
        employee_code: r.get(2),
        session_id: r.get(3),
        tenko_type: r.get(4),
        completed_at: r.get(5),
        temperature: r.get(6),
        systolic: r.get(7),
        diastolic: r.get(8),
        pulse: r.get(9),
        medical_measured_at: r.get(10),
        medical_manual_input: r.get(11),
        alcohol_result: r.get(12),
        alcohol_value: r.get(13),
        self_declaration: r.get(14),
        safety_judgment: r.get(15),
        has_baseline: r.get(16),
        baseline_systolic: r.get(17),
        baseline_diastolic: r.get(18),
        baseline_temperature: r.get(19),
        systolic_tolerance: r.get(20),
        diastolic_tolerance: r.get(21),
        temperature_tolerance: r.get(22),
    }
}

pub async fn fetch(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    date: NaiveDate,
) -> Result<Vec<DailyHealthRow>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let rows = tx
                .query_typed(SQL, &[(&tenant_id, Type::UUID), (&date, Type::DATE)])
                .await?;
            Ok(rows.iter().map(row).collect())
        })
    })
    .await
}

/// JST の今日 (unix ms から)
pub fn jst_today(now_ms: i64) -> NaiveDate {
    let now = DateTime::<Utc>::from_timestamp_millis(now_ms).unwrap_or_default();
    (now + chrono::Duration::hours(9)).date_naive()
}

/// 件数のまとめ (点呼済み = セッションあり、pass / fail = 安全の判定の状態)
pub fn summarize(rows: &[DailyHealthRow]) -> DailyHealthSummary {
    let status = |r: &DailyHealthRow, s: &str| {
        r.safety_judgment
            .as_ref()
            .and_then(|j| j.get("status"))
            .and_then(|v| v.as_str())
            == Some(s)
    };
    let total = rows.len() as i64;
    let checked = rows.iter().filter(|r| r.session_id.is_some()).count() as i64;
    DailyHealthSummary {
        total_employees: total,
        checked_count: checked,
        unchecked_count: total - checked,
        pass_count: rows.iter().filter(|r| status(r, "pass")).count() as i64,
        fail_count: rows.iter().filter(|r| status(r, "fail")).count() as i64,
    }
}

pub fn router() -> Router<AppState> {
    Router::new().route("/tenko/daily-health-status", get(handler))
}

async fn handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Query(filter): Query<DailyHealthFilter>,
) -> Result<Json<DailyHealthResponse>, StatusCode> {
    let date = filter.date.unwrap_or_else(|| jst_today((state.clock)()));
    let mut pg = state.pg.lock().await;
    let employees = fetch(&mut pg, tenant_id, dev.get(), date)
        .await
        .map_err(|e| state.db_fail("daily health", &e))?;
    let summary = summarize(&employees);
    Ok(Json(DailyHealthResponse {
        date,
        employees,
        summary,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jst_today_rolls_over_at_15_utc() {
        // 2026-10-27 14:59:59Z = JST 23:59:59、15:00Z = JST の翌日 0:00
        assert_eq!(
            jst_today(1_793_113_199_000),
            NaiveDate::from_ymd_opt(2026, 10, 27).unwrap()
        );
        assert_eq!(
            jst_today(1_793_113_200_000),
            NaiveDate::from_ymd_opt(2026, 10, 28).unwrap()
        );
        // 範囲外の値は 1970-01-01 (UTC) の JST
        assert_eq!(
            jst_today(i64::MAX),
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
        );
    }
}
