//! 行と本文の型 (rust の `alc_tenko::models`・`alc_core::models` の写し。JSON の形は同じ) と、それを読む列の並び。
//! rust は `SELECT *` + `FromRow` で読んでいたが、ここでは列を明示して位置で読む (列が足されても形が変わらない)。

use alc_worker_db::TxOutput;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::Row;
use uuid::Uuid;

/// 型付きの文の引数 (値と型)
pub type Arg<'a> = (&'a (dyn ToSql + Sync), Type);

/// ページの番号と件数 (rust と同じ: 件数の既定 50・上限 100、番号は 1 から)。返すのは (page, per_page, 飛ばす件数)
pub fn paging(page: Option<i64>, per_page: Option<i64>) -> (i64, i64, i64) {
    let per_page = per_page.unwrap_or(50).min(100);
    let page = page.unwrap_or(1).max(1);
    (page, per_page, (page - 1) * per_page)
}

/// BOM 付き UTF-8 (Excel 対応。rust の CSV と同じ)
pub fn with_bom(data: Vec<u8>) -> Vec<u8> {
    let mut out = vec![0xEF, 0xBB, 0xBF];
    out.extend_from_slice(&data);
    out
}

// --- 点呼の予定 ---

/// rust の `TenkoSchedule`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TenkoSchedule {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub employee_id: Uuid,
    pub tenko_type: String,
    pub responsible_manager_name: String,
    pub scheduled_at: DateTime<Utc>,
    pub instruction: Option<String>,
    pub consumed: bool,
    pub consumed_by_session_id: Option<Uuid>,
    pub overdue_notified_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub const SCHEDULE_COLUMNS: &str =
    "id, tenant_id, employee_id, tenko_type, responsible_manager_name, scheduled_at, \
     instruction, consumed, consumed_by_session_id, overdue_notified_at, created_at, updated_at";

pub fn schedule(r: &Row) -> TenkoSchedule {
    TenkoSchedule {
        id: r.get(0),
        tenant_id: r.get(1),
        employee_id: r.get(2),
        tenko_type: r.get(3),
        responsible_manager_name: r.get(4),
        scheduled_at: r.get(5),
        instruction: r.get(6),
        consumed: r.get(7),
        consumed_by_session_id: r.get(8),
        overdue_notified_at: r.get(9),
        created_at: r.get(10),
        updated_at: r.get(11),
    }
}

// --- 点呼の記録 (不変) ---

/// rust の `TenkoRecord`。判定の 3 欄は CSV の JOIN だけが引く (一覧・乗務員情報では常に null)。
/// 確認の方法 (`manager_judgment_method`) は CSV の読み替えのためだけに引き、JSON には出さない
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TenkoRecord {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub session_id: Uuid,
    pub employee_id: Uuid,
    pub tenko_type: String,
    pub status: String,
    pub record_data: Value,
    pub employee_name: String,
    pub responsible_manager_name: Option<String>,
    pub tenko_method: String,
    pub location: Option<String>,
    pub alcohol_result: Option<String>,
    pub alcohol_value: Option<f64>,
    pub alcohol_has_face_photo: bool,
    pub temperature: Option<f64>,
    pub systolic: Option<i32>,
    pub diastolic: Option<i32>,
    pub pulse: Option<i32>,
    pub instruction: Option<String>,
    pub instruction_confirmed_at: Option<DateTime<Utc>>,
    pub report_vehicle_road_status: Option<String>,
    pub report_driver_alternation: Option<String>,
    pub report_no_report: Option<bool>,
    pub report_vehicle_road_audio_url: Option<String>,
    pub report_driver_alternation_audio_url: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub recorded_at: DateTime<Utc>,
    pub record_hash: String,
    pub self_declaration: Option<Value>,
    pub safety_judgment: Option<Value>,
    pub daily_inspection: Option<Value>,
    pub interrupted_at: Option<DateTime<Utc>>,
    pub resumed_at: Option<DateTime<Utc>>,
    pub resume_reason: Option<String>,
    pub manager_judgment: Option<String>,
    pub manager_judgment_reason: Option<String>,
    pub manager_judgment_by_name: Option<String>,
    #[serde(skip)]
    pub manager_judgment_method: Option<String>,
}

/// 記録の列 (別名 `r` の表から)。[`record`] がこの順に読む
pub const RECORD_COLUMNS: &str = "r.id, r.tenant_id, r.session_id, r.employee_id, r.tenko_type, r.status, r.record_data, \
     r.employee_name, r.responsible_manager_name, r.tenko_method, r.location, r.alcohol_result, r.alcohol_value, \
     r.alcohol_has_face_photo, r.temperature, r.systolic, r.diastolic, r.pulse, r.instruction, \
     r.instruction_confirmed_at, r.report_vehicle_road_status, r.report_driver_alternation, r.report_no_report, \
     r.report_vehicle_road_audio_url, r.report_driver_alternation_audio_url, r.started_at, r.completed_at, \
     r.recorded_at, r.record_hash, r.self_declaration, r.safety_judgment, r.daily_inspection, r.interrupted_at, \
     r.resumed_at, r.resume_reason";

/// [`RECORD_COLUMNS`] の後ろに判定の 4 欄 (judgment, reason, method, by_name) を足した行なら `with_judgment`
pub fn record(r: &Row, with_judgment: bool) -> TenkoRecord {
    let judgment = |i: usize| -> Option<String> {
        if with_judgment {
            r.get(i)
        } else {
            None
        }
    };
    TenkoRecord {
        id: r.get(0),
        tenant_id: r.get(1),
        session_id: r.get(2),
        employee_id: r.get(3),
        tenko_type: r.get(4),
        status: r.get(5),
        record_data: r.get(6),
        employee_name: r.get(7),
        responsible_manager_name: r.get(8),
        tenko_method: r.get(9),
        location: r.get(10),
        alcohol_result: r.get(11),
        alcohol_value: r.get(12),
        alcohol_has_face_photo: r.get(13),
        temperature: r.get(14),
        systolic: r.get(15),
        diastolic: r.get(16),
        pulse: r.get(17),
        instruction: r.get(18),
        instruction_confirmed_at: r.get(19),
        report_vehicle_road_status: r.get(20),
        report_driver_alternation: r.get(21),
        report_no_report: r.get(22),
        report_vehicle_road_audio_url: r.get(23),
        report_driver_alternation_audio_url: r.get(24),
        started_at: r.get(25),
        completed_at: r.get(26),
        recorded_at: r.get(27),
        record_hash: r.get(28),
        self_declaration: r.get(29),
        safety_judgment: r.get(30),
        daily_inspection: r.get(31),
        interrupted_at: r.get(32),
        resumed_at: r.get(33),
        resume_reason: r.get(34),
        manager_judgment: judgment(35),
        manager_judgment_reason: judgment(36),
        manager_judgment_method: judgment(37),
        manager_judgment_by_name: judgment(38),
    }
}

// --- 健康の基準値 ---

/// rust の `EmployeeHealthBaseline`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EmployeeHealthBaseline {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub employee_id: Uuid,
    pub baseline_systolic: i32,
    pub baseline_diastolic: i32,
    pub baseline_temperature: f64,
    pub systolic_tolerance: i32,
    pub diastolic_tolerance: i32,
    pub temperature_tolerance: f64,
    pub measurement_validity_minutes: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub const BASELINE_COLUMNS: &str =
    "id, tenant_id, employee_id, baseline_systolic, baseline_diastolic, \
     baseline_temperature, systolic_tolerance, diastolic_tolerance, temperature_tolerance, \
     measurement_validity_minutes, created_at, updated_at";

pub fn baseline(r: &Row) -> EmployeeHealthBaseline {
    EmployeeHealthBaseline {
        id: r.get(0),
        tenant_id: r.get(1),
        employee_id: r.get(2),
        baseline_systolic: r.get(3),
        baseline_diastolic: r.get(4),
        baseline_temperature: r.get(5),
        systolic_tolerance: r.get(6),
        diastolic_tolerance: r.get(7),
        temperature_tolerance: r.get(8),
        measurement_validity_minutes: r.get(9),
        created_at: r.get(10),
        updated_at: r.get(11),
    }
}

// --- 機器故障 ---

/// rust の `EquipmentFailure`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EquipmentFailure {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub failure_type: String,
    pub description: String,
    pub affected_device: Option<String>,
    pub detected_at: DateTime<Utc>,
    pub detected_by: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub resolution_notes: Option<String>,
    pub session_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub const FAILURE_COLUMNS: &str =
    "id, tenant_id, failure_type, description, affected_device, detected_at, \
     detected_by, resolved_at, resolution_notes, session_id, created_at, updated_at";

pub fn failure(r: &Row) -> EquipmentFailure {
    EquipmentFailure {
        id: r.get(0),
        tenant_id: r.get(1),
        failure_type: r.get(2),
        description: r.get(3),
        affected_device: r.get(4),
        detected_at: r.get(5),
        detected_by: r.get(6),
        resolved_at: r.get(7),
        resolution_notes: r.get(8),
        session_id: r.get(9),
        created_at: r.get(10),
        updated_at: r.get(11),
    }
}

// --- 乗務員情報で使う行 (rust の alc_core::models の写し) ---

/// rust の `Employee` (顔の特徴量は JSON に出さないので読まない)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Employee {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub code: Option<String>,
    pub nfc_id: Option<String>,
    pub name: String,
    pub face_photo_url: Option<String>,
    pub face_embedding_at: Option<DateTime<Utc>>,
    pub face_model_version: Option<String>,
    pub face_approval_status: String,
    pub face_approved_by: Option<Uuid>,
    pub face_approved_at: Option<DateTime<Utc>>,
    pub license_issue_date: Option<NaiveDate>,
    pub license_expiry_date: Option<NaiveDate>,
    pub role: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

/// rust の `DtakoDailyWorkHours`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DtakoDailyWorkHours {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub driver_id: Uuid,
    pub work_date: NaiveDate,
    pub start_time: NaiveTime,
    pub total_work_minutes: Option<i32>,
    pub total_drive_minutes: Option<i32>,
    pub total_rest_minutes: Option<i32>,
    pub late_night_minutes: i32,
    pub drive_minutes: i32,
    pub cargo_minutes: i32,
    pub overlap_drive_minutes: i32,
    pub overlap_cargo_minutes: i32,
    pub overlap_break_minutes: i32,
    pub overlap_restraint_minutes: i32,
    pub ot_late_night_minutes: i32,
    pub total_distance: Option<f64>,
    pub operation_count: i32,
    pub unko_nos: Option<Vec<String>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// rust の `CarryingItem`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CarryingItem {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub item_name: String,
    pub is_required: bool,
    pub sort_order: i32,
    pub created_at: DateTime<Utc>,
}

// transaction の外へ持ち出す行 (owned な値だけを持つ)
impl TxOutput for TenkoSchedule {}
impl TxOutput for TenkoRecord {}
impl TxOutput for EmployeeHealthBaseline {}
impl TxOutput for EquipmentFailure {}
impl TxOutput for Employee {}
impl TxOutput for DtakoDailyWorkHours {}
impl TxOutput for CarryingItem {}

// --- 入力 (rust の型の写し) ---

/// rust の `CreateTenkoSchedule`
#[derive(Debug, Clone, Deserialize)]
pub struct CreateTenkoSchedule {
    pub employee_id: Uuid,
    pub tenko_type: String,
    pub responsible_manager_name: String,
    pub scheduled_at: DateTime<Utc>,
    pub instruction: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paging_matches_rust() {
        assert_eq!(paging(None, None), (1, 50, 0));
        assert_eq!(paging(Some(3), Some(20)), (3, 20, 40));
        assert_eq!(paging(Some(0), Some(500)), (1, 100, 0));
        assert_eq!(with_bom(b"a".to_vec()), vec![0xEF, 0xBB, 0xBF, b'a']);
    }
}
