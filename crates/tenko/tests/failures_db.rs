//! DB の失敗は 500 で、ログは固定の語 (段の名前と kind) だけ。接続を落とした `alc_api_rt` で全部の口を叩く。

mod support;

use serde_json::{json, Value};
use support::{assert_logs_clean, broken_client, build, call};
use uuid::Uuid;

#[tokio::test]
async fn db_failures_are_500_with_fixed_logs() {
    let app = build(broken_client().await);
    let t = Uuid::new_v4();
    let id = Uuid::new_v4();
    let schedule = json!({ "employee_id": id, "tenko_type": "post_operation", "responsible_manager_name": "m", "scheduled_at": "2026-10-28T00:00:00Z" });
    let tenant_cases: Vec<(&str, String, Option<Value>, &str)> = vec![
        ("GET", "/api/tenko/records".into(), None, "record list"),
        ("GET", "/api/tenko/records/csv".into(), None, "record csv"),
        (
            "POST",
            "/api/tenko/schedules".into(),
            Some(schedule.clone()),
            "schedule create",
        ),
        (
            "POST",
            "/api/tenko/schedules/batch".into(),
            Some(json!({ "schedules": [schedule] })),
            "schedule batch",
        ),
        ("GET", "/api/tenko/schedules".into(), None, "schedule list"),
        (
            "GET",
            format!("/api/tenko/schedules/{id}"),
            None,
            "schedule get",
        ),
        (
            "PUT",
            format!("/api/tenko/schedules/{id}"),
            Some(json!({})),
            "schedule update",
        ),
        (
            "DELETE",
            format!("/api/tenko/schedules/{id}"),
            None,
            "schedule delete",
        ),
        (
            "GET",
            format!("/api/tenko/schedules/pending/{id}"),
            None,
            "schedule pending",
        ),
        (
            "POST",
            "/api/tenko/health-baselines".into(),
            Some(json!({ "employee_id": id })),
            "baseline upsert",
        ),
        (
            "GET",
            "/api/tenko/health-baselines".into(),
            None,
            "baseline list",
        ),
        (
            "GET",
            format!("/api/tenko/health-baselines/{id}"),
            None,
            "baseline get",
        ),
        (
            "PUT",
            format!("/api/tenko/health-baselines/{id}"),
            Some(json!({})),
            "baseline update",
        ),
        (
            "DELETE",
            format!("/api/tenko/health-baselines/{id}"),
            None,
            "baseline delete",
        ),
        (
            "POST",
            "/api/tenko/equipment-failures".into(),
            Some(json!({ "failure_type": "manual_report", "description": "d" })),
            "failure create",
        ),
        (
            "GET",
            "/api/tenko/equipment-failures".into(),
            None,
            "failure list",
        ),
        (
            "GET",
            "/api/tenko/equipment-failures/csv".into(),
            None,
            "failure csv",
        ),
        (
            "GET",
            format!("/api/tenko/equipment-failures/{id}"),
            None,
            "failure get",
        ),
        (
            "PUT",
            format!("/api/tenko/equipment-failures/{id}"),
            Some(json!({})),
            "failure resolve",
        ),
        (
            "POST",
            "/api/tenko/webhooks".into(),
            Some(json!({ "event_type": "tenko_completed", "url": "https://example.com/h" })),
            "webhook upsert",
        ),
        ("GET", "/api/tenko/webhooks".into(), None, "webhook list"),
        (
            "GET",
            format!("/api/tenko/webhooks/{id}"),
            None,
            "webhook get",
        ),
        (
            "DELETE",
            format!("/api/tenko/webhooks/{id}"),
            None,
            "webhook delete",
        ),
        (
            "GET",
            format!("/api/tenko/webhooks/{id}/deliveries"),
            None,
            "webhook deliveries",
        ),
        ("GET", "/api/tenko/dashboard".into(), None, "dashboard"),
        (
            "GET",
            "/api/tenko/daily-health-status?date=2026-10-28".into(),
            None,
            "daily health",
        ),
        (
            "GET",
            format!("/api/tenko/driver-info/{id}"),
            None,
            "driver info",
        ),
        (
            "GET",
            "/api/tenko-call/numbers".into(),
            None,
            "call numbers",
        ),
        (
            "POST",
            "/api/tenko-call/numbers".into(),
            Some(json!({ "call_number": "1" })),
            "call number create",
        ),
        (
            "DELETE",
            "/api/tenko-call/numbers/1".into(),
            None,
            "call number delete",
        ),
        (
            "GET",
            "/api/tenko-call/drivers".into(),
            None,
            "call drivers",
        ),
    ];
    for (method, uri, body, stage) in tenant_cases {
        let got = call(&app, method, &uri, Some(t), body).await;
        assert_eq!(got.status, 500, "{stage}");
        assert!(
            app.logs()
                .iter()
                .any(|l| l.starts_with(&format!("tenko: {stage} failed: db ("))),
            "{stage} のログ"
        );
    }
    let reg = call(
        &app,
        "POST",
        "/api/tenko-call/register",
        None,
        Some(json!({ "phone_number": "p", "driver_name": "d", "call_number": "c" })),
    )
    .await;
    assert_eq!(
        (reg.status, reg.json()["error"].clone()),
        (500, json!("internal error"))
    );
    let tenko = call(
        &app,
        "POST",
        "/api/tenko-call/tenko",
        None,
        Some(json!({ "phone_number": "p", "driver_name": "d", "latitude": 0.0, "longitude": 0.0 })),
    )
    .await;
    assert_eq!(tenko.status, 500);
    for stage in ["call register", "call tenko"] {
        assert!(
            app.logs()
                .iter()
                .any(|l| l.starts_with(&format!("tenko: {stage} failed: db ("))),
            "{stage}"
        );
    }
    assert_logs_clean(&app, &[id.to_string(), t.to_string()]);
}
