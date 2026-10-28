//! 乗務員情報 (rust の `alc_tenko::driver_info`・repo/driver_info.rs の写し)。点呼で運行管理者に見せる項目 (イ〜ト)。
//!
//! - `GET /tenko/driver-info/{employee_id}` — 社員 (在籍でなければ 404)・健康の基準値・直近の測定 5 件・労働時間 7 日・
//!   指導の記録 10 件・携行品・点呼の記録 10 件・日常点検 5 件・未解決の機器故障・直近の整備の記録 5 件
//!
//! 社員・基準値・測定は失敗すれば 500。ほかの項目は失敗しても空の配列にする (rust と同じ。項目ごとに別の transaction で読む —
//! 1 つの transaction だと、失敗した文の後の文が全部失敗する)。
//!
//! rust との違い:
//! - SQL に tenant_id を明示した (rust は RLS 任せの文が多かった)
//! - 未解決の機器故障は検出の新しい順。rust は存在しない列 (`reported_at`) で並べていて文が必ず失敗し、常に空だった

use alc_core_wasm::{DeviceDevSlot, TenantId};
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use const_format::concatcp;
use serde::Serialize;
use serde_json::Value;
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::models::{
    baseline, failure, record, CarryingItem, DtakoDailyWorkHours, Employee, EmployeeHealthBaseline,
    EquipmentFailure, TenkoRecord, BASELINE_COLUMNS, FAILURE_COLUMNS, RECORD_COLUMNS,
};
use crate::{set_device_dev, AppState};

/// rust の `MeasurementSummary`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MeasurementSummary {
    pub id: Uuid,
    pub temperature: Option<f64>,
    pub systolic: Option<i32>,
    pub diastolic: Option<i32>,
    pub pulse: Option<i32>,
    pub measured_at: Option<DateTime<Utc>>,
}

/// rust の `InstructionSummary`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InstructionSummary {
    pub session_id: Uuid,
    pub instruction: String,
    pub instruction_confirmed_at: Option<DateTime<Utc>>,
    pub recorded_at: DateTime<Utc>,
}

/// rust の `DailyInspectionSummary`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DailyInspectionSummary {
    pub session_id: Uuid,
    pub daily_inspection: Value,
    pub recorded_at: DateTime<Utc>,
}

/// rust の `MaintenanceRecordSummary` (費用は numeric を文字列で)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MaintenanceRecordSummary {
    pub id: Uuid,
    pub vehicle_id: Uuid,
    pub category_name: String,
    pub performed_on: NaiveDate,
    pub odometer_km: Option<i32>,
    pub vendor: Option<String>,
    pub description: Option<String>,
    pub cost: Option<String>,
    pub next_due_on: Option<NaiveDate>,
}

/// rust の `DriverInfo`
#[derive(Debug, Serialize)]
pub struct DriverInfo {
    pub health_baseline: Option<EmployeeHealthBaseline>,
    pub recent_measurements: Vec<MeasurementSummary>,
    pub working_hours: Vec<DtakoDailyWorkHours>,
    pub past_instructions: Vec<InstructionSummary>,
    pub carrying_items: Vec<CarryingItem>,
    pub employee: Employee,
    pub past_tenko_records: Vec<TenkoRecord>,
    pub recent_daily_inspections: Vec<DailyInspectionSummary>,
    pub equipment_failures: Vec<EquipmentFailure>,
    pub recent_maintenance_records: Vec<MaintenanceRecordSummary>,
}

impl TxOutput for MeasurementSummary {}
impl TxOutput for InstructionSummary {}
impl TxOutput for DailyInspectionSummary {}
impl TxOutput for MaintenanceRecordSummary {}

pub mod sql {
    use super::*;

    /// 在籍の社員。$1 tenant_id / $2 employee_id
    pub const EMPLOYEE: &str = "SELECT id, tenant_id, code, nfc_id, name, face_photo_url, face_embedding_at, \
             face_model_version, face_approval_status, face_approved_by, face_approved_at, license_issue_date, \
             license_expiry_date, role, created_at, updated_at, deleted_at \
         FROM alc_api.employees WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL";

    pub const BASELINE: &str = concatcp!(
        "SELECT ",
        BASELINE_COLUMNS,
        " FROM alc_api.employee_health_baselines WHERE tenant_id = $1 AND employee_id = $2"
    );

    /// 直近の測定 5 件 (点呼のセッションから)
    pub const MEASUREMENTS: &str = "SELECT id, temperature, systolic, diastolic, pulse, medical_measured_at \
         FROM alc_api.tenko_sessions WHERE tenant_id = $1 AND employee_id = $2 AND medical_measured_at IS NOT NULL \
         ORDER BY medical_measured_at DESC LIMIT 5";

    /// 労働時間 7 日
    pub const WORKING_HOURS: &str = "SELECT id, tenant_id, driver_id, work_date, start_time, total_work_minutes, \
             total_drive_minutes, total_rest_minutes, late_night_minutes, drive_minutes, cargo_minutes, \
             overlap_drive_minutes, overlap_cargo_minutes, overlap_break_minutes, overlap_restraint_minutes, \
             ot_late_night_minutes, total_distance, operation_count, unko_nos, created_at, updated_at \
         FROM alc_api.dtako_daily_work_hours WHERE tenant_id = $1 AND driver_id = $2 \
         ORDER BY work_date DESC LIMIT 7";

    /// 指導の記録 10 件 (点呼の記録の指示事項)
    pub const INSTRUCTIONS: &str =
        "SELECT session_id, instruction, instruction_confirmed_at, recorded_at \
         FROM alc_api.tenko_records WHERE tenant_id = $1 AND employee_id = $2 \
           AND instruction IS NOT NULL AND instruction != '' \
         ORDER BY recorded_at DESC LIMIT 10";

    /// 携行品 (テナントの全部)。$1 tenant_id
    pub const CARRYING_ITEMS: &str =
        "SELECT id, tenant_id, item_name, is_required, sort_order, created_at \
         FROM alc_api.carrying_items WHERE tenant_id = $1 ORDER BY sort_order, created_at";

    /// 点呼の記録 10 件
    pub const RECORDS: &str = concatcp!(
        "SELECT ",
        RECORD_COLUMNS,
        " FROM alc_api.tenko_records r WHERE r.tenant_id = $1 AND r.employee_id = $2 \
         ORDER BY r.recorded_at DESC LIMIT 10"
    );

    /// 日常点検 5 件
    pub const INSPECTIONS: &str = "SELECT session_id, daily_inspection, recorded_at \
         FROM alc_api.tenko_records WHERE tenant_id = $1 AND employee_id = $2 AND daily_inspection IS NOT NULL \
         ORDER BY recorded_at DESC LIMIT 5";

    /// 未解決の機器故障 (テナントの全部)。$1 tenant_id
    pub const FAILURES: &str = concatcp!(
        "SELECT ",
        FAILURE_COLUMNS,
        " FROM alc_api.equipment_failures WHERE tenant_id = $1 AND resolved_at IS NULL ORDER BY detected_at DESC"
    );

    /// 直近の整備の記録 5 件: 乗務員の直近の点呼のセッションの車両 ID → 整備の車両 → 記録 (rust と同じ 1 文)
    pub const MAINTENANCE: &str = "WITH latest_session AS ( \
             SELECT NULLIF(carins_vehicle_id, '') AS car_id FROM alc_api.tenko_sessions \
             WHERE tenant_id = $1 AND employee_id = $2 ORDER BY created_at DESC LIMIT 1 \
         ), vehicle AS ( \
             SELECT mv.id FROM alc_api.maintenance_vehicles mv, latest_session ls \
             WHERE mv.tenant_id = $1 AND mv.car_id = ls.car_id AND mv.deleted_at IS NULL \
         ) \
         SELECT mr.id, mr.vehicle_id, mc.name, mr.performed_on, mr.odometer_km, mr.vendor, mr.description, \
             mr.cost::text, mr.next_due_on \
         FROM alc_api.maintenance_records mr \
         JOIN alc_api.maintenance_categories mc ON mc.id = mr.category_id AND mc.tenant_id = $1 \
         JOIN vehicle v ON v.id = mr.vehicle_id \
         WHERE mr.tenant_id = $1 AND mr.deleted_at IS NULL \
         ORDER BY mr.performed_on DESC LIMIT 5";
}

fn employee(r: &Row) -> Employee {
    Employee {
        id: r.get(0),
        tenant_id: r.get(1),
        code: r.get(2),
        nfc_id: r.get(3),
        name: r.get(4),
        face_photo_url: r.get(5),
        face_embedding_at: r.get(6),
        face_model_version: r.get(7),
        face_approval_status: r.get(8),
        face_approved_by: r.get(9),
        face_approved_at: r.get(10),
        license_issue_date: r.get(11),
        license_expiry_date: r.get(12),
        role: r.get(13),
        created_at: r.get(14),
        updated_at: r.get(15),
        deleted_at: r.get(16),
    }
}

fn measurement(r: &Row) -> MeasurementSummary {
    MeasurementSummary {
        id: r.get(0),
        temperature: r.get(1),
        systolic: r.get(2),
        diastolic: r.get(3),
        pulse: r.get(4),
        measured_at: r.get(5),
    }
}

fn work_hours(r: &Row) -> DtakoDailyWorkHours {
    DtakoDailyWorkHours {
        id: r.get(0),
        tenant_id: r.get(1),
        driver_id: r.get(2),
        work_date: r.get(3),
        start_time: r.get(4),
        total_work_minutes: r.get(5),
        total_drive_minutes: r.get(6),
        total_rest_minutes: r.get(7),
        late_night_minutes: r.get(8),
        drive_minutes: r.get(9),
        cargo_minutes: r.get(10),
        overlap_drive_minutes: r.get(11),
        overlap_cargo_minutes: r.get(12),
        overlap_break_minutes: r.get(13),
        overlap_restraint_minutes: r.get(14),
        ot_late_night_minutes: r.get(15),
        total_distance: r.get(16),
        operation_count: r.get(17),
        unko_nos: r.get(18),
        created_at: r.get(19),
        updated_at: r.get(20),
    }
}

fn instruction(r: &Row) -> InstructionSummary {
    InstructionSummary {
        session_id: r.get(0),
        instruction: r.get(1),
        instruction_confirmed_at: r.get(2),
        recorded_at: r.get(3),
    }
}

fn carrying_item(r: &Row) -> CarryingItem {
    CarryingItem {
        id: r.get(0),
        tenant_id: r.get(1),
        item_name: r.get(2),
        is_required: r.get(3),
        sort_order: r.get(4),
        created_at: r.get(5),
    }
}

fn inspection(r: &Row) -> DailyInspectionSummary {
    DailyInspectionSummary {
        session_id: r.get(0),
        daily_inspection: r.get(1),
        recorded_at: r.get(2),
    }
}

fn maintenance(r: &Row) -> MaintenanceRecordSummary {
    MaintenanceRecordSummary {
        id: r.get(0),
        vehicle_id: r.get(1),
        category_name: r.get(2),
        performed_on: r.get(3),
        odometer_km: r.get(4),
        vendor: r.get(5),
        description: r.get(6),
        cost: r.get(7),
        next_due_on: r.get(8),
    }
}

/// 社員・基準値・直近の測定 (どれかが失敗すれば全体が 500)。社員が居なければ None
#[allow(clippy::type_complexity)]
pub async fn required(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    employee_id: Uuid,
) -> Result<
    Option<(
        Employee,
        Option<EmployeeHealthBaseline>,
        Vec<MeasurementSummary>,
    )>,
    Error,
> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let args = [
                (&tenant_id as _, Type::UUID),
                (&employee_id as _, Type::UUID),
            ];
            let Some(emp) = tx.query_typed_opt(sql::EMPLOYEE, &args).await? else {
                return Ok(None);
            };
            let base = tx.query_typed_opt(sql::BASELINE, &args).await?;
            let measurements = tx.query_typed(sql::MEASUREMENTS, &args).await?;
            Ok(Some((
                employee(&emp),
                base.as_ref().map(baseline),
                measurements.iter().map(measurement).collect(),
            )))
        })
    })
    .await
}

/// 項目 1 つを自分の transaction で読む。`with_employee` なら $2 に employee_id を渡す
pub async fn rows<T: TxOutput + Send + 'static>(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    employee_id: Option<Uuid>,
    statement: &'static str,
    map: fn(&Row) -> T,
) -> Result<Vec<T>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let rows = match employee_id {
                Some(e) => {
                    tx.query_typed(statement, &[(&tenant_id, Type::UUID), (&e, Type::UUID)])
                        .await?
                }
                None => {
                    tx.query_typed(statement, &[(&tenant_id, Type::UUID)])
                        .await?
                }
            };
            Ok(rows.iter().map(map).collect())
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new().route("/tenko/driver-info/{employee_id}", get(handler))
}

fn record_row(r: &Row) -> TenkoRecord {
    record(r, false)
}

async fn handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(employee_id): Path<Uuid>,
) -> Result<Json<DriverInfo>, StatusCode> {
    let dev = dev.get();
    let mut pg = state.pg.lock().await;
    let (employee, health_baseline, recent_measurements) =
        required(&mut pg, tenant_id, dev, employee_id)
            .await
            .map_err(|e| state.db_fail("driver info", &e))?
            .ok_or(StatusCode::NOT_FOUND)?;
    let me = Some(employee_id);
    // 失敗しても空 (rust の unwrap_or_default)。段の名前と kind だけをログに出す
    let optional = |stage: &'static str| {
        let state = state.clone();
        move |e: Error| {
            let _ = state.db_fail(stage, &e);
        }
    };
    macro_rules! items {
        ($stage:literal, $who:expr, $sql:expr, $map:expr) => {
            rows(&mut pg, tenant_id, dev, $who, $sql, $map)
                .await
                .map_err(optional($stage))
                .unwrap_or_default()
        };
    }
    let working_hours = items!(
        "driver info working hours",
        me,
        sql::WORKING_HOURS,
        work_hours
    );
    let past_instructions = items!(
        "driver info instructions",
        me,
        sql::INSTRUCTIONS,
        instruction
    );
    let carrying_items = items!(
        "driver info carrying items",
        None,
        sql::CARRYING_ITEMS,
        carrying_item
    );
    let past_tenko_records = items!("driver info records", me, sql::RECORDS, record_row);
    let recent_daily_inspections =
        items!("driver info inspections", me, sql::INSPECTIONS, inspection);
    let equipment_failures = items!("driver info failures", None, sql::FAILURES, failure);
    let recent_maintenance_records =
        items!("driver info maintenance", me, sql::MAINTENANCE, maintenance);
    Ok(Json(DriverInfo {
        health_baseline,
        recent_measurements,
        working_hours,
        past_instructions,
        carrying_items,
        employee,
        past_tenko_records,
        recent_daily_inspections,
        equipment_failures,
        recent_maintenance_records,
    }))
}
