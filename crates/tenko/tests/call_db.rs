//! 点呼の電話を実 DB で通す (rust-alc-api の tenko_call の要点)。アプリの口はテナントのヘッダー無しで呼ぶ。

mod support;

use serde_json::json;
use support::{admin, app, call, tenant};
use uuid::Uuid;

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

#[tokio::test]
async fn numbers_and_drivers_are_tenant_scoped() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let n = unique("0120");

    // 旧い client が送る tenant_id は無視して、認証済みのテナントで作る
    let got = call(
        &app,
        "POST",
        "/api/tenko-call/numbers",
        Some(t),
        Some(json!({ "call_number": n, "label": "本社", "tenant_id": other })),
    )
    .await;
    assert_eq!(got.status, 200);
    let id = got.json()["id"].as_i64().unwrap();
    assert_eq!(got.json()["success"], true);
    call(
        &app,
        "POST",
        "/api/tenko-call/numbers",
        Some(other),
        Some(json!({ "call_number": unique("0120") })),
    )
    .await;

    let list = call(&app, "GET", "/api/tenko-call/numbers", Some(t), None)
        .await
        .json();
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(
        (
            list[0]["call_number"].clone(),
            list[0]["tenant_id"].clone(),
            list[0]["label"].clone()
        ),
        (json!(n), json!(t.to_string()), json!("本社"))
    );
    assert!(list[0]["created_at"].is_string());

    // アプリの登録 (テナントは番号から)・上書き
    let phone = unique("090");
    let reg = call(
        &app,
        "POST",
        "/api/tenko-call/register",
        None,
        Some(json!({ "phone_number": phone, "driver_name": "電話 太郎", "call_number": n })),
    )
    .await;
    assert_eq!(reg.status, 200);
    let driver_id = reg.json()["driver_id"].clone();
    assert_eq!(
        (
            reg.json()["success"].clone(),
            reg.json()["call_number"].clone()
        ),
        (json!(true), json!(n))
    );
    assert!(reg.json().get("error").is_none());
    let again = call(&app, "POST", "/api/tenko-call/register", None, Some(json!({ "phone_number": phone, "driver_name": "電話 次郎", "call_number": n, "employee_code": "E9" }))).await.json();
    assert_eq!(again["driver_id"], driver_id);

    let drivers = call(&app, "GET", "/api/tenko-call/drivers", Some(t), None)
        .await
        .json();
    assert_eq!(drivers.as_array().unwrap().len(), 1);
    assert_eq!(
        (
            drivers[0]["driver_name"].clone(),
            drivers[0]["employee_code"].clone()
        ),
        (json!("電話 次郎"), json!("E9"))
    );
    assert_eq!(
        call(&app, "GET", "/api/tenko-call/drivers", Some(other), None)
            .await
            .json(),
        json!([])
    );

    // 未登録の番号は 400 と文言
    let bad = call(
        &app,
        "POST",
        "/api/tenko-call/register",
        None,
        Some(json!({ "phone_number": phone, "driver_name": "x", "call_number": "none" })),
    )
    .await;
    assert_eq!(bad.status, 400);
    assert_eq!(
        bad.json(),
        json!({ "success": false, "driver_id": 0, "call_number": null, "error": "未登録の点呼用番号です" })
    );

    // 点呼の位置 (テナントは電話番号から)。未登録は 404
    let tenko = call(&app, "POST", "/api/tenko-call/tenko", None, Some(json!({ "phone_number": phone, "driver_name": "電話 次郎", "latitude": 35.6, "longitude": 139.7 }))).await;
    assert_eq!(
        (tenko.status, tenko.json()),
        (200, json!({ "success": true, "call_number": n }))
    );
    let logs: i64 = admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.tenko_call_logs WHERE phone_number = $1",
            &[&phone],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(logs, 1);
    assert_eq!(call(&app, "POST", "/api/tenko-call/tenko", None, Some(json!({ "phone_number": "none", "driver_name": "x", "latitude": 0.0, "longitude": 0.0 }))).await.status, 404);

    // 削除は他のテナントの番号なら 0 行のまま 204
    let path = format!("/api/tenko-call/numbers/{id}");
    assert_eq!(
        call(&app, "DELETE", &path, Some(other), None).await.status,
        204
    );
    assert_eq!(
        call(&app, "GET", "/api/tenko-call/numbers", Some(t), None)
            .await
            .json()
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn non_uuid_tenant_rows_are_500() {
    // 表の tenant_id (text) が UUID として読めない行は 500 (固定の語のログ)
    let app = app().await;
    let n = unique("0570");
    let phone = unique("080");
    let su = admin().await;
    su.execute(
        "INSERT INTO alc_api.tenko_call_numbers (call_number, tenant_id) VALUES ($1, 'not-a-uuid')",
        &[&n],
    )
    .await
    .unwrap();
    su.execute("INSERT INTO alc_api.tenko_call_drivers (phone_number, driver_name, tenant_id) VALUES ($1, 'x', 'not-a-uuid')", &[&phone]).await.unwrap();
    let reg = call(
        &app,
        "POST",
        "/api/tenko-call/register",
        None,
        Some(json!({ "phone_number": unique("070"), "driver_name": "x", "call_number": n })),
    )
    .await;
    assert_eq!(reg.status, 500);
    assert_eq!(reg.json()["error"], "internal error");
    let tenko = call(
        &app,
        "POST",
        "/api/tenko-call/tenko",
        None,
        Some(
            json!({ "phone_number": phone, "driver_name": "x", "latitude": 0.0, "longitude": 0.0 }),
        ),
    )
    .await;
    assert_eq!(tenko.status, 500);
    assert_eq!(
        app.logs(),
        vec![
            "tenko: call register failed: tenant is not a uuid".to_owned(),
            "tenko: call tenko failed: tenant is not a uuid".to_owned(),
        ]
    );
}
