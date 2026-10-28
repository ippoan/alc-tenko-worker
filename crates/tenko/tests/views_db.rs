//! 日々の健康状態と乗務員情報を実 DB で通す (rust-alc-api の daily_health・driver_info の要点)。

mod support;

use serde_json::json;
use support::{
    admin, app, build, call, call_dev, client_as, employee, record, session, tenant, READ_ONLY_ROLE,
};
use uuid::Uuid;

#[tokio::test]
async fn daily_health_orders_and_summarizes() {
    let app = app().await;
    let t = tenant().await;
    let fail = employee(t, "い 失敗").await;
    let pass = employee(t, "あ 合格").await;
    let none = employee(t, "う 未点呼").await;
    let gone = employee(t, "え 退職").await;
    let su = admin().await;
    su.execute(
        "UPDATE alc_api.employees SET deleted_at = now(), code = NULL WHERE id = $1",
        &[&gone],
    )
    .await
    .unwrap();
    su.execute(
        "UPDATE alc_api.employees SET code = 'E1' WHERE id = $1",
        &[&pass],
    )
    .await
    .unwrap();
    su.execute(
        "INSERT INTO alc_api.employee_health_baselines (tenant_id, employee_id) VALUES ($1, $2)",
        &[&t, &pass],
    )
    .await
    .unwrap();
    // JST 2026-10-28 の完了した点呼 (UTC 10-27 15:00 〜 10-28 15:00)
    for (emp, status, at) in [
        (pass, "pass", "2026-10-28T01:00:00Z"),
        (fail, "fail", "2026-10-27T16:00:00Z"),
    ] {
        let s = session(t, emp, "自動点呼", "completed", false, None).await;
        su.execute(
            &format!("UPDATE alc_api.tenko_sessions SET completed_at = '{at}', temperature = 36.4, systolic = 118, safety_judgment = '{{\"status\": \"{status}\"}}' WHERE id = $1"),
            &[&s],
        )
        .await
        .unwrap();
    }
    // 範囲外 (前日の JST) は数えない
    let old = session(t, none, "自動点呼", "completed", false, None).await;
    su.execute(
        "UPDATE alc_api.tenko_sessions SET completed_at = '2026-10-27T14:00:00Z' WHERE id = $1",
        &[&old],
    )
    .await
    .unwrap();

    // 日付の省略はテストの今 (JST 2026-10-28)
    let v = call(&app, "GET", "/api/tenko/daily-health-status", Some(t), None)
        .await
        .json();
    assert_eq!(v["date"], "2026-10-28");
    let order: Vec<_> = v["employees"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["employee_id"].clone())
        .collect();
    assert_eq!(order, vec![json!(fail), json!(none), json!(pass)]);
    let p = &v["employees"][2];
    assert_eq!(
        (
            p["employee_code"].clone(),
            p["temperature"].clone(),
            p["has_baseline"].clone(),
            p["baseline_systolic"].clone()
        ),
        (json!("E1"), json!(36.4), json!(true), json!(120))
    );
    assert_eq!(v["employees"][1]["has_baseline"], false);
    assert_eq!(
        v["summary"],
        json!({ "total_employees": 3, "checked_count": 2, "unchecked_count": 1, "pass_count": 1, "fail_count": 1 })
    );
    let prev = call(
        &app,
        "GET",
        "/api/tenko/daily-health-status?date=2026-10-27",
        Some(t),
        None,
    )
    .await
    .json();
    assert_eq!(prev["summary"]["checked_count"], 1);
    // dev の軸ではセッションが見えない
    assert_eq!(
        call_dev(&app, "GET", "/api/tenko/daily-health-status", t)
            .await
            .json()["summary"]["checked_count"],
        0
    );
}

/// 乗務員情報の各項目を作る。返すのは (テナント, 乗務員)
async fn driver_fixture() -> (Uuid, Uuid) {
    let t = tenant().await;
    let e = employee(t, "乗務 太郎").await;
    let su = admin().await;
    su.execute(
        "UPDATE alc_api.employees SET role = ARRAY['driver'] WHERE id = $1",
        &[&e],
    )
    .await
    .unwrap();
    su.execute(
        "INSERT INTO alc_api.employee_health_baselines (tenant_id, employee_id) VALUES ($1, $2)",
        &[&t, &e],
    )
    .await
    .unwrap();
    let s = session(t, e, "自動点呼", "completed", false, None).await;
    su.execute("UPDATE alc_api.tenko_sessions SET medical_measured_at = now(), temperature = 36.2, carins_vehicle_id = 'CAR1' WHERE id = $1", &[&s]).await.unwrap();
    record(
        t,
        s,
        e,
        "2026-10-01T00:00:00Z",
        false,
        &[
            ("instruction", "'速度に注意'"),
            ("daily_inspection", r#"'{"tires": "ok"}'"#),
        ],
    )
    .await;
    su.execute(
        "INSERT INTO alc_api.dtako_daily_work_hours (tenant_id, driver_id, work_date, start_time, unko_nos) VALUES ($1, $2, '2026-10-01', '08:00', ARRAY['U1'])",
        &[&t, &e],
    )
    .await
    .unwrap();
    su.execute("INSERT INTO alc_api.carrying_items (tenant_id, item_name, sort_order) VALUES ($1, '免許証', 1)", &[&t]).await.unwrap();
    su.execute("INSERT INTO alc_api.equipment_failures (tenant_id, failure_type, description) VALUES ($1, 'manual_report', '未解決')", &[&t]).await.unwrap();
    let v: Uuid = su
        .query_one("INSERT INTO alc_api.maintenance_vehicles (tenant_id, registration_number, car_id) VALUES ($1, '品川 1', 'CAR1') RETURNING id", &[&t])
        .await
        .unwrap()
        .get(0);
    let c: Uuid = su
        .query_one("INSERT INTO alc_api.maintenance_categories (tenant_id, name) VALUES ($1, '点検') RETURNING id", &[&t])
        .await
        .unwrap()
        .get(0);
    su.execute(
        "INSERT INTO alc_api.maintenance_records (tenant_id, vehicle_id, category_id, performed_on, cost) VALUES ($1, $2, $3, '2026-09-01', 1234.50)",
        &[&t, &v, &c],
    )
    .await
    .unwrap();
    (t, e)
}

#[tokio::test]
async fn driver_info_reads_every_item() {
    let app = app().await;
    let (t, e) = driver_fixture().await;
    let got = call(
        &app,
        "GET",
        &format!("/api/tenko/driver-info/{e}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    let v = got.json();
    assert_eq!(
        (v["employee"]["name"].clone(), v["employee"]["role"].clone()),
        (json!("乗務 太郎"), json!(["driver"]))
    );
    assert!(v["employee"].get("face_embedding").is_none());
    assert_eq!(v["health_baseline"]["baseline_systolic"], 120);
    assert_eq!(v["recent_measurements"][0]["temperature"], 36.2);
    assert_eq!(
        (
            v["working_hours"][0]["start_time"].clone(),
            v["working_hours"][0]["unko_nos"].clone()
        ),
        (json!("08:00:00"), json!(["U1"]))
    );
    assert_eq!(v["past_instructions"][0]["instruction"], "速度に注意");
    assert_eq!(v["carrying_items"][0]["item_name"], "免許証");
    assert_eq!(v["past_tenko_records"].as_array().unwrap().len(), 1);
    assert_eq!(
        v["recent_daily_inspections"][0]["daily_inspection"],
        json!({ "tires": "ok" })
    );
    assert_eq!(v["equipment_failures"][0]["description"], "未解決");
    assert_eq!(
        (
            v["recent_maintenance_records"][0]["category_name"].clone(),
            v["recent_maintenance_records"][0]["cost"].clone()
        ),
        (json!("点検"), json!("1234.50"))
    );

    // 他のテナント・居ない社員は 404。dev の軸ではセッション・記録が見えない
    let other = tenant().await;
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/tenko/driver-info/{e}"),
            Some(other),
            None
        )
        .await
        .status,
        404
    );
    let dev = call_dev(&app, "GET", &format!("/api/tenko/driver-info/{e}"), t)
        .await
        .json();
    assert_eq!(
        (
            dev["recent_measurements"].clone(),
            dev["past_tenko_records"].clone()
        ),
        (json!([]), json!([]))
    );
}

#[tokio::test]
async fn driver_info_optional_items_fall_back_to_empty() {
    // 読み取り専用のロールは労働時間・携行品・整備の表を読めない → その項目だけ空 (rust の unwrap_or_default)
    let (t, e) = driver_fixture().await;
    let app = build(client_as(READ_ONLY_ROLE).await);
    let v = call(
        &app,
        "GET",
        &format!("/api/tenko/driver-info/{e}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(v.status, 200);
    let v = v.json();
    assert_eq!(
        (
            v["working_hours"].clone(),
            v["carrying_items"].clone(),
            v["recent_maintenance_records"].clone()
        ),
        (json!([]), json!([]), json!([]))
    );
    assert_eq!(v["past_tenko_records"].as_array().unwrap().len(), 1);
    let logs = app.logs();
    for stage in [
        "driver info working hours",
        "driver info carrying items",
        "driver info maintenance",
    ] {
        assert!(
            logs.iter()
                .any(|l| l.starts_with(&format!("tenko: {stage} failed: db ("))),
            "{stage}"
        );
    }
    support::assert_logs_clean(&app, &[e.to_string(), t.to_string()]);
}
