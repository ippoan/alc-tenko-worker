//! 点呼の記録 (rust の `alc_tenko::tenko_records`・repo/tenko_records.rs の写し)。
//!
//! - `GET /tenko/records` (`?employee_id=&tenko_type=&status=&date_from=&date_to=&page=&per_page=`) — 一覧 (記録の新しい順)
//! - `GET /tenko/records/csv` — 同じ絞り込みの全件の CSV (BOM 付き)。判定 (運行管理者の OK/NG・理由・判定した人の名前) は
//!   記録が不変なので点呼のセッションから都度引き、対面で確定した IT点呼 は点呼方法を「対面点呼」に読み替える
//!
//! rust との違い: 絞り込みは文を組み立てず `($n IS NULL OR ...)` の 1 文にした。1 件の取得 (`/tenko/records/{id}`) は
//! 呼び手が無く rust-alc-api#781 で消したので持たない。

use alc_core_wasm::{DeviceDevSlot, TenantId};
use alc_worker_db::PgClient;
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use const_format::concatcp;
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::models::{paging, record, with_bom, Arg, TenkoRecord, RECORD_COLUMNS};
use crate::{set_device_dev, AppState};

/// 点呼方法 `IT点呼`
pub const TENKO_METHOD_IT: &str = "IT点呼";
/// 記録簿の CSV に出す点呼方法。対面で確定した IT点呼 の記録だけをこの値に読み替える (DB には書かない)
pub const TENKO_METHOD_IN_PERSON: &str = "対面点呼";
/// 判定の確認の方法: 本人が来て対面で確認した
pub const JUDGMENT_METHOD_IN_PERSON: &str = "in_person";

/// rust の `TenkoRecordFilter`
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RecordFilter {
    pub employee_id: Option<Uuid>,
    pub tenko_type: Option<String>,
    pub status: Option<String>,
    pub date_from: Option<DateTime<Utc>>,
    pub date_to: Option<DateTime<Utc>>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// rust の `TenkoRecordsResponse`
#[derive(Debug, Serialize)]
pub struct RecordsResponse {
    pub records: Vec<TenkoRecord>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

pub mod sql {
    use super::*;

    /// 絞り込み ($1 tenant_id / $2 employee_id / $3 tenko_type / $4 status / $5 date_from / $6 date_to。NULL は絞らない)
    pub const WHERE: &str = "r.tenant_id = $1 \
         AND ($2::uuid IS NULL OR r.employee_id = $2) \
         AND ($3::text IS NULL OR r.tenko_type = $3) \
         AND ($4::text IS NULL OR r.status = $4) \
         AND ($5::timestamptz IS NULL OR r.recorded_at >= $5) \
         AND ($6::timestamptz IS NULL OR r.recorded_at <= $6)";

    pub const COUNT: &str = concatcp!("SELECT COUNT(*) FROM alc_api.tenko_records r WHERE ", WHERE);

    /// 一覧 ($7 件数 / $8 飛ばす件数)
    pub const LIST: &str = concatcp!(
        "SELECT ",
        RECORD_COLUMNS,
        " FROM alc_api.tenko_records r WHERE ",
        WHERE,
        " ORDER BY r.recorded_at DESC LIMIT $7 OFFSET $8"
    );

    /// CSV (全件)。判定はセッションから、判定した人の名前は社員から引く
    pub const CSV: &str = concatcp!(
        "SELECT ",
        RECORD_COLUMNS,
        ", s.manager_judgment, s.manager_judgment_reason, s.manager_judgment_method, e.name \
         FROM alc_api.tenko_records r \
         LEFT JOIN alc_api.tenko_sessions s ON s.id = r.session_id AND s.tenant_id = r.tenant_id \
         LEFT JOIN alc_api.employees e ON e.id = s.manager_judgment_by AND e.tenant_id = r.tenant_id \
         WHERE ",
        WHERE,
        " ORDER BY r.recorded_at DESC"
    );
}

fn filter_args<'a>(tenant_id: &'a Uuid, f: &'a RecordFilter) -> [Arg<'a>; 6] {
    [
        (tenant_id, Type::UUID),
        (&f.employee_id, Type::UUID),
        (&f.tenko_type, Type::TEXT),
        (&f.status, Type::TEXT),
        (&f.date_from, Type::TIMESTAMPTZ),
        (&f.date_to, Type::TIMESTAMPTZ),
    ]
}

/// 件数と 1 ページぶん
pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    filter: RecordFilter,
    limit: i64,
    offset: i64,
) -> Result<(i64, Vec<TenkoRecord>), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let args = filter_args(&tenant_id, &filter);
            let total: i64 = tx.query_typed_one(sql::COUNT, &args).await?.get(0);
            let mut page_args = args.to_vec();
            page_args.push((&limit, Type::INT8));
            page_args.push((&offset, Type::INT8));
            let rows = tx.query_typed(sql::LIST, &page_args).await?;
            Ok((total, rows.iter().map(|r| record(r, false)).collect()))
        })
    })
    .await
}

/// CSV の行 (判定つき)
pub async fn list_for_csv(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    filter: RecordFilter,
) -> Result<Vec<TenkoRecord>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let rows = tx
                .query_typed(sql::CSV, &filter_args(&tenant_id, &filter))
                .await?;
            Ok(rows.iter().map(|r| record(r, true)).collect())
        })
    })
    .await
}

/// CSV に出す点呼方法: IT点呼 の記録を運行管理者が対面で確定したときだけ「対面点呼」。ほかは記録の値のまま
pub fn csv_tenko_method<'a>(tenko_method: &'a str, judgment_method: Option<&str>) -> &'a str {
    if tenko_method == TENKO_METHOD_IT && judgment_method == Some(JUDGMENT_METHOD_IN_PERSON) {
        TENKO_METHOD_IN_PERSON
    } else {
        tenko_method
    }
}

/// record_data (記録時のセッションの JSON) の文字列の値。無い / NULL は空欄
fn record_data_str(record_data: &serde_json::Value, key: &str) -> String {
    record_data
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

/// 自己申告の 3 欄 (bool の文字列。無ければ空欄)
fn declaration(v: Option<&serde_json::Value>) -> [String; 3] {
    let flag = |key: &str| {
        v.and_then(|v| v.get(key))
            .and_then(|b| b.as_bool())
            .map_or(String::new(), |b| b.to_string())
    };
    [flag("illness"), flag("fatigue"), flag("sleep_deprivation")]
}

/// 安全の判定の状態と、通らなかった項目 (`;` 区切り)
fn judgment(v: Option<&serde_json::Value>) -> (String, String) {
    let Some(v) = v else {
        return (String::new(), String::new());
    };
    let status = v
        .get("status")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    let items = v
        .get("failed_items")
        .and_then(|a| a.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|i| i.as_str())
                .collect::<Vec<_>>()
                .join(";")
        })
        .unwrap_or_default();
    (status, items)
}

/// 日常点検の結果 (1 つでも ng なら ng。無ければ空欄)
fn inspection(v: Option<&serde_json::Value>) -> String {
    const ITEMS: [&str; 8] = [
        "brakes",
        "tires",
        "lights",
        "steering",
        "wipers",
        "mirrors",
        "horn",
        "seatbelts",
    ];
    v.map(|v| {
        let ng = ITEMS
            .iter()
            .any(|k| v.get(k).and_then(|s| s.as_str()) == Some("ng"));
        if ng { "ng" } else { "ok" }.to_string()
    })
    .unwrap_or_default()
}

fn opt<T: ToString>(v: Option<T>) -> String {
    v.map_or(String::new(), |v| v.to_string())
}

fn time(v: Option<DateTime<Utc>>) -> String {
    v.map_or(String::new(), |t| t.to_rfc3339())
}

/// CSV の見出し (rust と同じ 39 列)
pub const CSV_HEADER: [&str; 39] = [
    "record_id",
    "employee_name",
    "tenko_type",
    "tenko_method",
    "status",
    "responsible_manager_name",
    "started_at",
    "completed_at",
    "location",
    "alcohol_result",
    "alcohol_value",
    "alcohol_has_face_photo",
    "temperature",
    "systolic",
    "diastolic",
    "pulse",
    "instruction",
    "instruction_confirmed_at",
    "report_vehicle_road_status",
    "report_driver_alternation",
    "report_no_report",
    "self_declaration_illness",
    "self_declaration_fatigue",
    "self_declaration_sleep",
    "safety_judgment_status",
    "safety_judgment_failed_items",
    "manager_judgment",
    "manager_judgment_reason",
    "manager_judgment_by_name",
    "daily_inspection_status",
    "interrupted_at",
    "resumed_at",
    "resume_reason",
    "recorded_at",
    "record_hash",
    "carins_cert_no",
    "carins_vehicle_id",
    "carins_expires_on",
    "carins_matched_by",
];

/// 記録 1 件の CSV の行
pub fn csv_row(r: &TenkoRecord) -> Vec<String> {
    let [illness, fatigue, sleep] = declaration(r.self_declaration.as_ref());
    let (judgment_status, judgment_items) = judgment(r.safety_judgment.as_ref());
    vec![
        r.id.to_string(),
        r.employee_name.clone(),
        r.tenko_type.clone(),
        csv_tenko_method(&r.tenko_method, r.manager_judgment_method.as_deref()).to_string(),
        r.status.clone(),
        r.responsible_manager_name.clone().unwrap_or_default(),
        time(r.started_at),
        time(r.completed_at),
        r.location.clone().unwrap_or_default(),
        r.alcohol_result.clone().unwrap_or_default(),
        opt(r.alcohol_value),
        r.alcohol_has_face_photo.to_string(),
        opt(r.temperature),
        opt(r.systolic),
        opt(r.diastolic),
        opt(r.pulse),
        r.instruction.clone().unwrap_or_default(),
        time(r.instruction_confirmed_at),
        r.report_vehicle_road_status.clone().unwrap_or_default(),
        r.report_driver_alternation.clone().unwrap_or_default(),
        opt(r.report_no_report),
        illness,
        fatigue,
        sleep,
        judgment_status,
        judgment_items,
        r.manager_judgment.clone().unwrap_or_default(),
        r.manager_judgment_reason.clone().unwrap_or_default(),
        r.manager_judgment_by_name.clone().unwrap_or_default(),
        inspection(r.daily_inspection.as_ref()),
        time(r.interrupted_at),
        time(r.resumed_at),
        r.resume_reason.clone().unwrap_or_default(),
        r.recorded_at.to_rfc3339(),
        r.record_hash.clone(),
        record_data_str(&r.record_data, "carins_cert_no"),
        record_data_str(&r.record_data, "carins_vehicle_id"),
        record_data_str(&r.record_data, "carins_expires_on"),
        record_data_str(&r.record_data, "carins_matched_by"),
    ]
}

/// 記録の CSV (見出し + 行、BOM 付き)。csv の writer はメモリの Vec に書くので失敗しない
pub fn render_csv(records: &[TenkoRecord]) -> Vec<u8> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    wtr.write_record(CSV_HEADER).expect("csv to memory");
    for r in records {
        wtr.write_record(csv_row(r)).expect("csv to memory");
    }
    with_bom(wtr.into_inner().expect("csv to memory"))
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tenko/records", get(list_handler))
        .route("/tenko/records/csv", get(csv_handler))
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Query(filter): Query<RecordFilter>,
) -> Result<Json<RecordsResponse>, StatusCode> {
    let (page, per_page, offset) = paging(filter.page, filter.per_page);
    let mut pg = state.pg.lock().await;
    let (total, records) = list(&mut pg, tenant_id, dev.get(), filter, per_page, offset)
        .await
        .map_err(|e| state.db_fail("record list", &e))?;
    Ok(Json(RecordsResponse {
        records,
        total,
        page,
        per_page,
    }))
}

async fn csv_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Query(filter): Query<RecordFilter>,
) -> Result<impl IntoResponse, StatusCode> {
    let mut pg = state.pg.lock().await;
    let records = list_for_csv(&mut pg, tenant_id, dev.get(), filter)
        .await
        .map_err(|e| state.db_fail("record csv", &e))?;
    Ok((
        [
            (header::CONTENT_TYPE, "text/csv; charset=utf-8"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"tenko_records.csv\"",
            ),
        ],
        render_csv(&records),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn csv_method_reads_in_person_only_for_it() {
        assert_eq!(csv_tenko_method("IT点呼", Some("in_person")), "対面点呼");
        assert_eq!(csv_tenko_method("IT点呼", Some("it")), "IT点呼");
        assert_eq!(csv_tenko_method("IT点呼", None), "IT点呼");
        assert_eq!(csv_tenko_method("自動点呼", Some("in_person")), "自動点呼");
    }

    #[test]
    fn csv_helpers_match_rust() {
        assert_eq!(
            declaration(Some(&json!({ "illness": true, "fatigue": false }))),
            ["true".to_string(), "false".into(), String::new()]
        );
        assert_eq!(
            declaration(None),
            [String::new(), String::new(), String::new()]
        );
        assert_eq!(
            judgment(Some(
                &json!({ "status": "fail", "failed_items": ["a", 1, "b"] })
            )),
            ("fail".to_string(), "a;b".to_string())
        );
        assert_eq!(judgment(Some(&json!({}))), (String::new(), String::new()));
        assert_eq!(judgment(None), (String::new(), String::new()));
        assert_eq!(inspection(Some(&json!({ "tires": "ng" }))), "ng");
        assert_eq!(inspection(Some(&json!({ "tires": "ok" }))), "ok");
        assert_eq!(inspection(None), "");
        assert_eq!(record_data_str(&json!({ "k": "v", "n": 1 }), "k"), "v");
        assert_eq!(record_data_str(&json!({ "n": 1 }), "n"), "");
    }
}
