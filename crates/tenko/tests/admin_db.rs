//! 健康の基準値・機器故障・webhook の設定を実 DB で通す (rust-alc-api の health_baselines・equipment_failures・tenko_webhooks の要点)。

mod support;

use serde_json::json;
use support::{admin, app, call, call_dev, call_with, employee, tenant};
use uuid::Uuid;

#[tokio::test]
async fn baselines_crud() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let e = employee(t, "基準 太郎").await;

    // 省略は rust と同じ既定値
    let got = call(
        &app,
        "POST",
        "/api/tenko/health-baselines",
        Some(t),
        Some(json!({ "employee_id": e })),
    )
    .await;
    assert_eq!(got.status, 201);
    let v = got.json();
    assert_eq!(
        (
            v["baseline_systolic"].clone(),
            v["baseline_diastolic"].clone(),
            v["baseline_temperature"].clone(),
            v["systolic_tolerance"].clone(),
            v["diastolic_tolerance"].clone(),
            v["temperature_tolerance"].clone(),
            v["measurement_validity_minutes"].clone()
        ),
        (
            json!(120),
            json!(80),
            json!(36.5),
            json!(10),
            json!(10),
            json!(0.5),
            json!(30)
        )
    );
    // 同じ乗務員は上書き
    let got = call(
        &app,
        "POST",
        "/api/tenko/health-baselines",
        Some(t),
        Some(json!({ "employee_id": e, "baseline_systolic": 130 })),
    )
    .await;
    assert_eq!(
        (
            got.status,
            got.json()["baseline_systolic"].clone(),
            got.json()["id"].clone()
        ),
        (201, json!(130), v["id"].clone())
    );

    assert_eq!(
        call(&app, "GET", "/api/tenko/health-baselines", Some(t), None)
            .await
            .json()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/tenko/health-baselines",
            Some(other),
            None
        )
        .await
        .json(),
        json!([])
    );
    let path = format!("/api/tenko/health-baselines/{e}");
    assert_eq!(
        call(&app, "GET", &path, Some(t), None).await.json()["baseline_systolic"],
        130
    );
    assert_eq!(
        call(&app, "GET", &path, Some(other), None).await.status,
        404
    );
    let upd = call(
        &app,
        "PUT",
        &path,
        Some(t),
        Some(json!({ "temperature_tolerance": 0.8 })),
    )
    .await;
    assert_eq!(
        (
            upd.status,
            upd.json()["temperature_tolerance"].clone(),
            upd.json()["baseline_systolic"].clone()
        ),
        (200, json!(0.8), json!(130))
    );
    assert_eq!(
        call(&app, "PUT", &path, Some(other), Some(json!({})))
            .await
            .status,
        404
    );
    assert_eq!(call(&app, "DELETE", &path, Some(t), None).await.status, 204);
    assert_eq!(call(&app, "DELETE", &path, Some(t), None).await.status, 404);
}

#[tokio::test]
async fn failures_crud_and_csv() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;

    assert_eq!(
        call(
            &app,
            "POST",
            "/api/tenko/equipment-failures",
            Some(t),
            Some(json!({ "failure_type": "x", "description": "d" }))
        )
        .await
        .status,
        400
    );
    let a = call(
        &app,
        "POST",
        "/api/tenko/equipment-failures",
        Some(t),
        Some(json!({
            "failure_type": "kiosk_offline", "description": "落ちた", "affected_device": "kiosk-1",
            "detected_at": "2026-10-01T00:00:00Z", "detected_by": "管理", "session_id": null
        })),
    )
    .await;
    assert_eq!(a.status, 201);
    let a = a.json();
    let b = call(
        &app,
        "POST",
        "/api/tenko/equipment-failures",
        Some(t),
        Some(json!({ "failure_type": "manual_report", "description": "手書き" })),
    )
    .await
    .json();
    assert_eq!(b["resolved_at"], json!(null));

    let list = call(&app, "GET", "/api/tenko/equipment-failures", Some(t), None)
        .await
        .json();
    assert_eq!(
        (list["total"].clone(), list["failures"][0]["id"].clone()),
        (json!(2), b["id"].clone())
    );
    let only = call(&app, "GET", "/api/tenko/equipment-failures?failure_type=kiosk_offline&date_from=2026-09-01T00:00:00Z&date_to=2026-10-02T00:00:00Z&page=1&per_page=5", Some(t), None).await.json();
    assert_eq!(
        (only["total"].clone(), only["per_page"].clone()),
        (json!(1), json!(5))
    );
    let session = Uuid::new_v4();
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/tenko/equipment-failures?session_id={session}"),
            Some(t),
            None
        )
        .await
        .json()["total"],
        0
    );
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/tenko/equipment-failures",
            Some(other),
            None
        )
        .await
        .json()["total"],
        0
    );

    let a_path = format!(
        "/api/tenko/equipment-failures/{}",
        a["id"].as_str().unwrap()
    );
    assert_eq!(
        call(&app, "GET", &a_path, Some(t), None).await.json()["description"],
        "落ちた"
    );
    assert_eq!(
        call(&app, "GET", &a_path, Some(other), None).await.status,
        404
    );
    let res = call(
        &app,
        "PUT",
        &a_path,
        Some(t),
        Some(json!({ "resolution_notes": "再起動" })),
    )
    .await;
    assert_eq!(res.status, 200);
    assert!(res.json()["resolved_at"].is_string());
    assert_eq!(
        call(&app, "PUT", &a_path, Some(other), Some(json!({})))
            .await
            .status,
        404
    );
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/tenko/equipment-failures?resolved=true",
            Some(t),
            None
        )
        .await
        .json()["total"],
        1
    );
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/tenko/equipment-failures?resolved=false",
            Some(t),
            None
        )
        .await
        .json()["total"],
        1
    );

    let csv = call(
        &app,
        "GET",
        "/api/tenko/equipment-failures/csv?date_to=2026-10-02T00:00:00Z",
        Some(t),
        None,
    )
    .await;
    assert_eq!(csv.status, 200);
    assert_eq!(
        csv.headers["content-disposition"],
        "attachment; filename=\"equipment_failures.csv\""
    );
    let text = String::from_utf8(csv.body[3..].to_vec()).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], "id,failure_type,description,affected_device,detected_at,detected_by,resolved_at,resolution_notes,session_id");
    assert_eq!(lines.len(), 2);
    assert!(lines[1].contains(",kiosk_offline,落ちた,kiosk-1,2026-10-01T00:00:00+00:00,管理,"));
    assert!(lines[1].ends_with(",再起動,"));

    // dev の軸
    assert_eq!(
        call_with(
            &app,
            "POST",
            "/api/tenko/equipment-failures",
            Some(t),
            &[("X-Device-Dev", "1")],
            Some(json!({ "failure_type": "manual_report", "description": "dev" }))
        )
        .await
        .status,
        201
    );
    assert_eq!(
        call_dev(&app, "GET", "/api/tenko/equipment-failures", t)
            .await
            .json()["total"],
        1
    );
}

#[tokio::test]
async fn webhooks_crud() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;

    assert_eq!(
        call(
            &app,
            "POST",
            "/api/tenko/webhooks",
            Some(t),
            Some(json!({ "event_type": "x", "url": "https://example.com/h" }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/tenko/webhooks",
            Some(t),
            Some(json!({ "event_type": "tenko_completed", "url": "https://127.0.0.1/h" }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        app.logs(),
        vec!["tenko: rejected webhook config with disallowed url".to_owned()]
    );

    let got = call(&app, "POST", "/api/tenko/webhooks", Some(t), Some(json!({ "event_type": "tenko_completed", "url": "https://example.com/h", "secret": "s3" }))).await;
    assert_eq!(got.status, 201);
    let v = got.json();
    assert_eq!(v["enabled"], true);
    assert!(v.get("secret").is_none());
    // 同じ種類は上書き
    let again = call(&app, "POST", "/api/tenko/webhooks", Some(t), Some(json!({ "event_type": "tenko_completed", "url": "https://example.com/h2", "enabled": false }))).await.json();
    assert_eq!(
        (
            again["id"].clone(),
            again["url"].clone(),
            again["enabled"].clone()
        ),
        (
            v["id"].clone(),
            json!("https://example.com/h2"),
            json!(false)
        )
    );
    call(
        &app,
        "POST",
        "/api/tenko/webhooks",
        Some(t),
        Some(json!({ "event_type": "alcohol_detected", "url": "https://example.com/a" })),
    )
    .await;

    let list = call(&app, "GET", "/api/tenko/webhooks", Some(t), None)
        .await
        .json();
    let kinds: Vec<_> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["event_type"].clone())
        .collect();
    assert_eq!(
        kinds,
        vec![json!("alcohol_detected"), json!("tenko_completed")]
    );
    assert_eq!(
        call(&app, "GET", "/api/tenko/webhooks", Some(other), None)
            .await
            .json(),
        json!([])
    );

    let id = v["id"].as_str().unwrap();
    let path = format!("/api/tenko/webhooks/{id}");
    assert_eq!(
        call(&app, "GET", &path, Some(t), None).await.json()["url"],
        "https://example.com/h2"
    );
    assert_eq!(
        call(&app, "GET", &path, Some(other), None).await.status,
        404
    );

    // 配信の記録 (新しい順。dev の軸は分かれる)
    let su = admin().await;
    let cid = Uuid::parse_str(id).unwrap();
    for (attempt, dev) in [(1, false), (2, false), (3, true)] {
        su.execute(
            "INSERT INTO alc_api.webhook_deliveries (tenant_id, config_id, event_type, payload, attempt, success, is_dev, created_at) \
             VALUES ($1, $2, 'tenko_completed', '{\"k\": 1}', $3, true, $4, now() + make_interval(mins => $3))",
            &[&t, &cid, &attempt, &dev],
        )
        .await
        .unwrap();
    }
    let d = call(&app, "GET", &format!("{path}/deliveries"), Some(t), None)
        .await
        .json();
    assert_eq!(d.as_array().unwrap().len(), 2);
    assert_eq!(
        (d[0]["attempt"].clone(), d[0]["payload"].clone()),
        (json!(2), json!({ "k": 1 }))
    );
    assert_eq!(
        call_dev(&app, "GET", &format!("{path}/deliveries"), t)
            .await
            .json()
            .as_array()
            .unwrap()
            .len(),
        1
    );

    assert_eq!(
        call(&app, "DELETE", &path, Some(other), None).await.status,
        404
    );
    // 配信の記録がある設定は外部キーで消せない (500。rust と同じ)
    assert_eq!(call(&app, "DELETE", &path, Some(t), None).await.status, 500);
    let a_id = list[0]["id"].as_str().unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/tenko/webhooks/{a_id}"),
            Some(t),
            None
        )
        .await
        .status,
        204
    );
}
