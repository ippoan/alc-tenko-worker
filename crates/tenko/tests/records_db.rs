//! 点呼の記録の一覧・CSV を実 DB で通す (rust-alc-api の tenko_records の要点)。接続は本番の worker と同じ実行用ロール `alc_api_rt`。

mod support;

use serde_json::json;
use support::{app, call, call_dev, call_with, employee, record, session, tenant};

#[tokio::test]
async fn tenant_header_is_required() {
    let app = app().await;
    for uri in [
        "/api/tenko/records",
        "/api/tenko/records/csv",
        "/api/tenko/schedules",
        "/api/tenko/health-baselines",
        "/api/tenko/equipment-failures",
        "/api/tenko/webhooks",
        "/api/tenko/dashboard",
        "/api/tenko/daily-health-status",
        "/api/tenko-call/numbers",
        "/api/tenko-call/drivers",
    ] {
        assert_eq!(
            call(&app, "GET", uri, None, None).await.status,
            401,
            "{uri}"
        );
    }
}

#[tokio::test]
async fn list_filters_and_axis() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let e1 = employee(t, "点呼 一郎").await;
    let e2 = employee(t, "点呼 二郎").await;
    let eo = employee(other, "他社 太郎").await;
    let s1 = session(t, e1, "自動点呼", "completed", false, None).await;
    let s2 = session(t, e2, "自動点呼", "completed", false, None).await;
    let r1 = record(t, s1, e1, "2026-10-01T09:00:00Z", false, &[]).await;
    let r2 = record(
        t,
        s2,
        e2,
        "2026-10-02T09:00:00Z",
        false,
        &[
            ("tenko_type", "'post_operation'"),
            ("status", "'cancelled'"),
        ],
    )
    .await;
    let so = session(other, eo, "自動点呼", "completed", false, None).await;
    record(other, so, eo, "2026-10-03T09:00:00Z", false, &[]).await;
    let sd = session(t, e1, "自動点呼", "completed", true, None).await;
    let rd = record(t, sd, e1, "2026-10-04T09:00:00Z", true, &[]).await;

    let got = call(&app, "GET", "/api/tenko/records", Some(t), None).await;
    assert_eq!(got.status, 200);
    let v = got.json();
    assert_eq!(
        (v["total"].clone(), v["page"].clone(), v["per_page"].clone()),
        (json!(2), json!(1), json!(50))
    );
    let ids: Vec<_> = v["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].clone())
        .collect();
    assert_eq!(ids, vec![json!(r2), json!(r1)]);
    let first = &v["records"][0];
    assert_eq!(first["employee_name"], "点呼 太郎");
    assert_eq!(first["manager_judgment"], json!(null));
    assert_eq!(first["manager_judgment_by_name"], json!(null));
    assert!(first.get("manager_judgment_method").is_none());
    assert_eq!(first["record_data"], json!({}));

    let ids = |q: &str| {
        let app = &app;
        let q = q.to_owned();
        async move {
            let v = call(app, "GET", &format!("/api/tenko/records{q}"), Some(t), None)
                .await
                .json();
            v["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["id"].clone())
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(ids(&format!("?employee_id={e1}")).await, vec![json!(r1)]);
    assert_eq!(ids("?tenko_type=post_operation").await, vec![json!(r2)]);
    assert_eq!(ids("?status=completed").await, vec![json!(r1)]);
    assert_eq!(
        ids("?date_from=2026-10-02T00:00:00Z").await,
        vec![json!(r2)]
    );
    assert_eq!(ids("?date_to=2026-10-01T23:00:00Z").await, vec![json!(r1)]);
    let page = call(
        &app,
        "GET",
        "/api/tenko/records?page=2&per_page=1",
        Some(t),
        None,
    )
    .await
    .json();
    assert_eq!(
        (page["records"][0]["id"].clone(), page["total"].clone()),
        (json!(r1), json!(2))
    );

    // dev の軸は dev の行だけ
    let dev = call_dev(&app, "GET", "/api/tenko/records", t).await.json();
    assert_eq!(dev["records"].as_array().unwrap().len(), 1);
    assert_eq!(dev["records"][0]["id"], json!(rd));

    // 不正な問い合わせは 400 (axum の Query)
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/tenko/records?employee_id=x",
            Some(t),
            None
        )
        .await
        .status,
        400
    );
}

#[tokio::test]
async fn csv_reads_judgment_and_rewrites_method() {
    let app = app().await;
    let t = tenant().await;
    let e = employee(t, "点呼 三郎").await;
    let manager = employee(t, "管理 花子").await;
    let s = session(
        t,
        e,
        "IT点呼",
        "completed",
        false,
        Some(("ng", "顔色", "in_person", manager)),
    )
    .await;
    record(
        t,
        s,
        e,
        "2026-10-05T09:00:00Z",
        false,
        &[
            ("tenko_method", "'IT点呼'"),
            ("alcohol_value", "0.05"),
            ("temperature", "36.6"),
            ("systolic", "120"),
            ("report_no_report", "true"),
            (
                "self_declaration",
                r#"'{"illness": true, "fatigue": false}'"#,
            ),
            (
                "safety_judgment",
                r#"'{"status": "fail", "failed_items": ["bp", "temp"]}'"#,
            ),
            ("daily_inspection", r#"'{"tires": "ng"}'"#),
            (
                "record_data",
                r#"'{"carins_cert_no": "C1", "carins_matched_by": "cert_no"}'"#,
            ),
            ("completed_at", "'2026-10-05T09:01:00Z'"),
        ],
    )
    .await;
    let s2 = session(t, e, "自動点呼", "completed", false, None).await;
    record(t, s2, e, "2026-10-06T09:00:00Z", false, &[]).await;

    let got = call(&app, "GET", "/api/tenko/records/csv", Some(t), None).await;
    assert_eq!(got.status, 200);
    assert_eq!(got.headers["content-type"], "text/csv; charset=utf-8");
    assert_eq!(
        got.headers["content-disposition"],
        "attachment; filename=\"tenko_records.csv\""
    );
    assert_eq!(&got.body[..3], &[0xEF, 0xBB, 0xBF]);
    let text = String::from_utf8(got.body[3..].to_vec()).unwrap();
    let mut rdr = csv::Reader::from_reader(text.as_bytes());
    assert_eq!(rdr.headers().unwrap().len(), 39);
    let rows: Vec<csv::StringRecord> = rdr.records().map(|r| r.unwrap()).collect();
    assert_eq!(rows.len(), 2);
    // 新しい順。自動点呼は判定なし
    assert_eq!(&rows[0][3], "自動点呼");
    assert_eq!(&rows[0][26], "");
    let r = &rows[1];
    assert_eq!(&r[3], "対面点呼");
    assert_eq!(&r[7], "2026-10-05T09:01:00+00:00");
    assert_eq!(&r[10], "0.05");
    assert_eq!(&r[12], "36.6");
    assert_eq!(&r[13], "120");
    assert_eq!(&r[20], "true");
    assert_eq!((&r[21], &r[22], &r[23]), ("true", "false", ""));
    assert_eq!((&r[24], &r[25]), ("fail", "bp;temp"));
    assert_eq!((&r[26], &r[27], &r[28]), ("ng", "顔色", "管理 花子"));
    assert_eq!(&r[29], "ng");
    assert_eq!((&r[35], &r[36], &r[38]), ("C1", "", "cert_no"));

    // 絞り込みは一覧と同じ
    let one = call_with(
        &app,
        "GET",
        "/api/tenko/records/csv?date_from=2026-10-06T00:00:00Z",
        Some(t),
        &[],
        None,
    )
    .await;
    assert_eq!(String::from_utf8(one.body).unwrap().lines().count(), 2);
}
