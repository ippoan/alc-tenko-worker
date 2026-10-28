//! 点呼のセッション (W7b) を実 DB で通す (rust-alc-api の tests/tenko_* と mock_tenko の要点・device_dev_axis_test の点呼の検査を写したもの)。
//! 接続は本番の worker と同じ実行用ロール `alc_api_rt` (RLS が効く)。webhook の送信は偽物の HTTP で確かめる。

mod support;

use std::sync::{Arc, Mutex as StdMutex};

use futures_util::future::BoxFuture;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use support::{
    admin, broken_client, build, call_with, client_as, employee, tenant, App, Got, READ_ONLY_ROLE,
};
use tenko::deliver::{deliver_all, Event, HttpResult, Sender, Sleeper, WebhookHttp};
use uuid::Uuid;

const ADMIN_USER: &str = "0b9f5a52-6a1e-4c1b-9d2a-3f4e5d6c7b8a";

fn admin_headers() -> Vec<(&'static str, &'static str)> {
    vec![
        ("X-User-ID", ADMIN_USER),
        ("X-User-Email", "admin@example.invalid"),
        ("X-User-Role", "admin"),
    ]
}

async fn send(app: &App, method: &str, uri: &str, t: Uuid, body: Option<Value>) -> Got {
    call_with(app, method, uri, Some(t), &[], body).await
}

async fn send_h(
    app: &App,
    method: &str,
    uri: &str,
    t: Uuid,
    h: &[(&str, &str)],
    body: Option<Value>,
) -> Got {
    call_with(app, method, uri, Some(t), h, body).await
}

/// セッションを 1 件作る (superuser で)。`extra` は列名 → SQL の値 (テストの固定値を直に書く)
async fn sess(
    t: Uuid,
    emp: Uuid,
    tenko_type: &str,
    method: &str,
    status: &str,
    extra: &[(&str, &str)],
) -> Uuid {
    let id = Uuid::new_v4();
    let mut extra = extra.to_vec();
    if !extra.iter().any(|(c, _)| *c == "started_at") {
        extra.push(("started_at", "NOW()"));
    }
    let cols: String = extra.iter().map(|(c, _)| format!(", {c}")).collect();
    let vals: String = extra.iter().map(|(_, v)| format!(", {v}")).collect();
    admin()
        .await
        .execute(
            &format!(
                "INSERT INTO alc_api.tenko_sessions (id, tenant_id, employee_id, tenko_type, tenko_method, status{cols}) \
                 VALUES ($1, $2, $3, $4, $5, $6{vals})"
            ),
            &[&id, &t, &emp, &tenko_type, &method, &status],
        )
        .await
        .unwrap();
    id
}

async fn schedule(t: Uuid, emp: Uuid, tenko_type: &str, instruction: Option<&str>) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.tenko_schedules (tenant_id, employee_id, tenko_type, responsible_manager_name, scheduled_at, instruction) \
             VALUES ($1, $2, $3, '運行 管理者', NOW(), $4) RETURNING id",
            &[&t, &emp, &tenko_type, &instruction],
        )
        .await
        .unwrap()
        .get(0)
}

async fn baseline(t: Uuid, emp: Uuid) {
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.employee_health_baselines (tenant_id, employee_id, baseline_systolic, baseline_diastolic, \
                 baseline_temperature, systolic_tolerance, diastolic_tolerance, temperature_tolerance) \
             VALUES ($1, $2, 120, 80, 36.5, 10, 10, 0.5)",
            &[&t, &emp],
        )
        .await
        .unwrap();
}

async fn manager(t: Uuid, role: &str) -> Uuid {
    let id = employee(t, "判定 管理者").await;
    admin()
        .await
        .execute(
            &format!("UPDATE alc_api.employees SET role = ARRAY['{role}'] WHERE id = $1"),
            &[&id],
        )
        .await
        .unwrap();
    id
}

async fn records(session: Uuid) -> Vec<(String, String, Value, String)> {
    admin()
        .await
        .query(
            "SELECT status, tenko_method, record_data, record_hash FROM alc_api.tenko_records WHERE session_id = $1",
            &[&session],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect()
}

fn events(app: &App) -> Vec<(&'static str, Value)> {
    app.outbox
        .take()
        .into_iter()
        .map(|(_, e)| (e.event_type, e.payload))
        .collect()
}

/// 業務前の自動点呼を最後まで通す: 予定の消費 → 医療 (血圧必須) → 自己申告と安全判定 → 日常点検 → 携行品 → アルコール → 指示の確認。
/// 完了で記録 (record_data はセッションの JSON、hash はその SHA-256) を書き、webhook は積まない
#[tokio::test]
async fn pre_operation_auto_flow_writes_record() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 一郎").await;
    baseline(t, emp).await;
    let sch = schedule(t, emp, "pre_operation", Some("安全運転")).await;
    let item: Uuid = admin()
        .await
        .query_one(
            "INSERT INTO alc_api.carrying_items (tenant_id, item_name) VALUES ($1, '免許証') RETURNING id",
            &[&t],
        )
        .await
        .unwrap()
        .get(0);

    let got = send(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        Some(json!({ "schedule_id": sch, "employee_id": emp })),
    )
    .await;
    assert_eq!(got.status, 201);
    let s = got.json();
    let id = s["id"].as_str().unwrap().to_owned();
    assert_eq!(
        (
            s["status"].clone(),
            s["tenko_method"].clone(),
            s["responsible_manager_name"].clone()
        ),
        (
            json!("medical_pending"),
            json!("自動点呼"),
            json!("運行 管理者")
        )
    );
    let consumed = admin()
        .await
        .query_one(
            "SELECT consumed, consumed_by_session_id::text FROM alc_api.tenko_schedules WHERE id = $1",
            &[&sch],
        )
        .await
        .unwrap();
    assert!(consumed.get::<_, bool>(0));
    assert_eq!(consumed.get::<_, Option<String>>(1), Some(id.clone()));
    // 消費済みの予定ではもう始められない
    let again = send(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        Some(json!({ "schedule_id": sch, "employee_id": emp })),
    )
    .await;
    assert_eq!(again.status, 404);

    let uri = |step: &str| format!("/api/tenko/sessions/{id}/{step}");
    let bp = send(
        &app,
        "PUT",
        &uri("medical"),
        t,
        Some(json!({ "temperature": 36.6 })),
    )
    .await;
    assert_eq!(bp.status, 400);
    assert_eq!(bp.json()["error"], "bp_required");
    let med = send(
        &app,
        "PUT",
        &uri("medical"),
        t,
        Some(json!({ "temperature": 36.6, "systolic": 125, "diastolic": 82, "pulse": 70 })),
    )
    .await;
    assert_eq!(med.json()["status"], "self_declaration_pending");

    let decl = send(
        &app,
        "PUT",
        &uri("self-declaration"),
        t,
        Some(json!({ "illness": false, "fatigue": false, "sleep_deprivation": false })),
    )
    .await;
    let decl = decl.json();
    assert_eq!(decl["status"], "daily_inspection_pending");
    assert_eq!(decl["safety_judgment"]["status"], "pass");
    assert_eq!(decl["safety_judgment"]["medical_diffs"]["systolic_diff"], 5);
    assert_eq!(
        decl["self_declaration"]["declared_at"],
        "2026-10-28T01:30:00Z"
    );

    let ok = json!({ "brakes": "ok", "tires": "ok", "lights": "ok", "steering": "ok", "wipers": "ok", "mirrors": "ok", "horn": "ok", "seatbelts": "ok" });
    let insp = send(&app, "PUT", &uri("daily-inspection"), t, Some(ok)).await;
    assert_eq!(insp.json()["status"], "carrying_items_pending");

    let missing = Uuid::new_v4();
    let carry = send(
        &app,
        "PUT",
        &uri("carrying-items"),
        t,
        Some(json!({ "checks": [ { "item_id": item, "checked": true }, { "item_id": missing, "checked": false } ] })),
    )
    .await;
    // マスタに無い品目は外部キーで入らない (rust と同じく 500)
    assert_eq!(carry.status, 500);
    let carry = send(
        &app,
        "PUT",
        &uri("carrying-items"),
        t,
        Some(json!({ "checks": [ { "item_id": item, "checked": true } ] })),
    )
    .await;
    let c = carry.json();
    assert_eq!(c["status"], "identity_verified");
    assert_eq!(
        c["carrying_items_checked"]["items"],
        json!([{ "item_id": item, "item_name": "免許証", "checked": true }])
    );

    let alc = send(
        &app,
        "PUT",
        &uri("alcohol"),
        t,
        Some(json!({ "alcohol_result": "pass", "alcohol_value": 0.0 })),
    )
    .await;
    assert_eq!(alc.json()["status"], "instruction_pending");
    assert!(records(id.parse().unwrap()).await.is_empty());

    let done = send(&app, "PUT", &uri("instruction-confirm"), t, None).await;
    let done_json = done.json();
    assert_eq!(done_json["status"], "completed");
    let recs = records(id.parse().unwrap()).await;
    assert_eq!(recs.len(), 1);
    let (status, method, data, hash) = &recs[0];
    assert_eq!(
        (status.as_str(), method.as_str()),
        ("completed", "自動点呼")
    );
    // record_data は応答のセッションと同じ値、hash はその SHA-256
    assert_eq!(data, &done_json);
    assert_eq!(
        hash,
        &format!("{:x}", Sha256::digest(data.to_string().as_bytes()))
    );
    assert!(events(&app).is_empty());
}

/// 業務後の遠隔点呼 (予定なし): アルコール → 運行報告 (指示が無ければ完了して記録)。報告は webhook を積む。
/// 予定の指示があれば指示の確認へ進み、記録はまだ書かない
#[tokio::test]
async fn post_operation_report_and_webhook() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 二郎").await;
    let got = send(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        Some(json!({ "employee_id": emp, "tenko_type": "post_operation", "location": "本社" })),
    )
    .await;
    assert_eq!(got.status, 201);
    let s = got.json();
    assert_eq!(
        (
            s["status"].clone(),
            s["tenko_method"].clone(),
            s["schedule_id"].clone()
        ),
        (json!("identity_verified"), json!("遠隔点呼"), Value::Null)
    );
    let id = s["id"].as_str().unwrap().to_owned();
    let uri = |step: &str| format!("/api/tenko/sessions/{id}/{step}");
    // 業務後は医療・自己申告・日常点検を受けない
    assert_eq!(
        send(&app, "PUT", &uri("medical"), t, Some(json!({})))
            .await
            .json()["error"],
        "invalid_tenko_type"
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri("self-declaration"),
            t,
            Some(json!({ "illness": false, "fatigue": false, "sleep_deprivation": false }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri("alcohol"),
            t,
            Some(json!({ "alcohol_result": "normal", "alcohol_value": 0.0 }))
        )
        .await
        .json()["status"],
        "report_pending"
    );

    assert_eq!(
        send(
            &app,
            "PUT",
            &uri("report"),
            t,
            Some(json!({ "vehicle_road_status": " ", "driver_alternation": "なし" }))
        )
        .await
        .status,
        400
    );
    let report = json!({ "vehicle_road_status": "異常なし", "driver_alternation": "報告なし", "vehicle_road_audio_url": "https://audio.example.invalid/a.webm" });
    let r = send(&app, "PUT", &uri("report"), t, Some(report))
        .await
        .json();
    assert_eq!(r["status"], "completed");
    assert_eq!(
        r["report_vehicle_road_audio_url"],
        "https://audio.example.invalid/a.webm"
    );
    assert_eq!(records(id.parse().unwrap()).await.len(), 1);
    let ev = events(&app);
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].0, "report_submitted");
    assert_eq!(ev[0].1["data"]["vehicle_road_status"], "異常なし");
    assert_eq!(ev[0].1["timestamp"], "2026-10-28T01:30:00Z");
    // 二度目は状態が違う
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri("report"),
            t,
            Some(json!({ "vehicle_road_status": "a", "driver_alternation": "b" }))
        )
        .await
        .status,
        400
    );

    let gone = format!("/api/tenko/sessions/{}/report", Uuid::new_v4());
    assert_eq!(
        send(
            &app,
            "PUT",
            &gone,
            t,
            Some(json!({ "vehicle_road_status": "a", "driver_alternation": "b" }))
        )
        .await
        .status,
        404
    );
    // 予定に指示があれば指示の確認へ (記録はまだ)
    let sch = schedule(t, emp, "post_operation", Some("給油")).await;
    let s2 = sess(
        t,
        emp,
        "post_operation",
        "自動点呼",
        "report_pending",
        &[("schedule_id", &format!("'{sch}'"))],
    )
    .await;
    let r2 = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{s2}/report"),
        t,
        Some(json!({ "vehicle_road_status": "a", "driver_alternation": "b" })),
    )
    .await
    .json();
    assert_eq!(
        (r2["status"].clone(), r2["completed_at"].clone()),
        (json!("instruction_pending"), Value::Null)
    );
    assert!(records(s2).await.is_empty());
    let done = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{s2}/instruction-confirm"),
        t,
        None,
    )
    .await
    .json();
    assert_eq!(done["status"], "completed");
    let recs = records(s2).await;
    assert_eq!(
        recs[0].2["instruction_confirmed_at"],
        done["instruction_confirmed_at"]
    );
}

/// アルコール検知は中止して記録を書き、webhook を積む。dev端末の要求では積まない (dev の行は本番から見えない)
#[tokio::test]
async fn alcohol_detected_cancels_and_dev_does_not_queue() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 三郎").await;
    let id = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "identity_verified",
        &[("responsible_manager_name", "'管理 太郎'")],
    )
    .await;
    let uri = format!("/api/tenko/sessions/{id}/alcohol");
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri,
            t,
            Some(json!({ "alcohol_result": "bogus", "alcohol_value": 0.1 }))
        )
        .await
        .status,
        400
    );
    let s = send(&app, "PUT", &uri, t, Some(json!({ "alcohol_result": "fail", "alcohol_value": 0.25, "alcohol_face_photo_url": "https://p.example.invalid/f.jpg" }))).await.json();
    assert_eq!(
        (
            s["status"].clone(),
            s["cancel_reason"].clone(),
            s["completed_at"].clone()
        ),
        (
            json!("cancelled"),
            json!("アルコール検知"),
            json!("2026-10-28T01:30:00Z")
        )
    );
    assert_eq!(records(id).await.len(), 1);
    let ev = events(&app);
    assert_eq!(ev[0].0, "alcohol_detected");
    assert_eq!(ev[0].1["data"]["employee_name"], "点呼 三郎");
    assert_eq!(ev[0].1["data"]["alcohol_value"], 0.25);
    assert_eq!(ev[0].1["data"]["responsible_manager_name"], "管理 太郎");
    // 状態が違う・無い
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri,
            t,
            Some(json!({ "alcohol_result": "pass", "alcohol_value": 0.0 }))
        )
        .await
        .status,
        400
    );
    let none = format!("/api/tenko/sessions/{}/alcohol", Uuid::new_v4());
    assert_eq!(
        send(
            &app,
            "PUT",
            &none,
            t,
            Some(json!({ "alcohol_result": "pass", "alcohol_value": 0.0 }))
        )
        .await
        .status,
        404
    );
    // 通常点呼 (種類が業務前・業務後でない) の結果は rust と同じく 500
    let normal = sess(t, emp, "normal", "通常点呼", "identity_verified", &[]).await;
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{normal}/alcohol"),
            t,
            Some(json!({ "alcohol_result": "pass", "alcohol_value": 0.0 }))
        )
        .await
        .status,
        500
    );

    // dev端末: 行は dev の軸に入り、webhook は積まない。本番の要求からは見えない
    let dev = [("X-Device-Dev", "1")];
    let started = send_h(
        &app,
        "POST",
        "/api/tenko/sessions/start",
        t,
        &dev,
        Some(json!({ "employee_id": emp, "tenko_type": "post_operation" })),
    )
    .await
    .json();
    let did = started["id"].as_str().unwrap().to_owned();
    let alc = send_h(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{did}/alcohol"),
        t,
        &dev,
        Some(json!({ "alcohol_result": "over", "alcohol_value": 0.3 })),
    )
    .await
    .json();
    assert_eq!(alc["status"], "cancelled");
    assert!(events(&app).is_empty());
    let is_dev: bool = admin()
        .await
        .query_one(
            "SELECT is_dev FROM alc_api.tenko_sessions WHERE id = $1",
            &[&did.parse::<Uuid>().unwrap()],
        )
        .await
        .unwrap()
        .get(0);
    assert!(is_dev);
    assert_eq!(
        send(&app, "GET", &format!("/api/tenko/sessions/{did}"), t, None)
            .await
            .status,
        404
    );
    assert_eq!(
        send_h(
            &app,
            "GET",
            &format!("/api/tenko/sessions/{did}"),
            t,
            &dev,
            None
        )
        .await
        .status,
        200
    );
}

/// 安全判定の失敗 (基準値の外・自己申告) は中断して記録と webhook。管理者の再開・手動の中断・中止の検査
#[tokio::test]
async fn safety_judgment_interrupt_resume_and_cancel() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 四郎").await;
    baseline(t, emp).await;
    let id = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "self_declaration_pending",
        &[("systolic", "150"), ("temperature", "36.4")],
    )
    .await;
    let uri = |step: &str| format!("/api/tenko/sessions/{id}/{step}");
    let s = send(
        &app,
        "PUT",
        &uri("self-declaration"),
        t,
        Some(json!({ "illness": true, "fatigue": false, "sleep_deprivation": false })),
    )
    .await
    .json();
    assert_eq!(s["status"], "interrupted");
    assert_eq!(
        s["safety_judgment"]["failed_items"],
        json!(["systolic", "illness"])
    );
    assert_eq!(s["interrupted_at"], "2026-10-28T01:30:00Z");
    assert_eq!(records(id).await.len(), 1);
    let ev = events(&app);
    assert_eq!(
        (ev[0].0, ev[0].1["data"]["failed_items"].clone()),
        ("tenko_interrupted", json!(["systolic", "illness"]))
    );

    // 管理者の再開: 理由が必須・AuthUser が要る (無いと rust と同じく 500)・中断中だけ
    assert_eq!(
        send_h(
            &app,
            "POST",
            &uri("resume"),
            t,
            &admin_headers(),
            Some(json!({ "reason": " " }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "POST",
            &uri("resume"),
            t,
            Some(json!({ "reason": "確認済み" }))
        )
        .await
        .status,
        500
    );
    let r = send_h(
        &app,
        "POST",
        &uri("resume"),
        t,
        &admin_headers(),
        Some(json!({ "reason": "確認済み" })),
    )
    .await
    .json();
    assert_eq!(
        (r["status"].clone(), r["resumed_by_user_id"].clone()),
        (json!("daily_inspection_pending"), json!(ADMIN_USER))
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &uri("resume"),
            t,
            &admin_headers(),
            Some(json!({ "reason": "x" }))
        )
        .await
        .status,
        400
    );
    let gone = format!("/api/tenko/sessions/{}/resume", Uuid::new_v4());
    assert_eq!(
        send_h(
            &app,
            "POST",
            &gone,
            t,
            &admin_headers(),
            Some(json!({ "reason": "x" }))
        )
        .await
        .status,
        404
    );
    // 日常点検は済み・自己申告が無い中断は自己申告へ戻す
    let s2 = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "interrupted",
        &[("daily_inspection", "'{}'")],
    )
    .await;
    let r2 = send_h(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{s2}/resume"),
        t,
        &admin_headers(),
        Some(json!({ "reason": "x" })),
    )
    .await
    .json();
    assert_eq!(r2["status"], "self_declaration_pending");
    let s3 = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "interrupted",
        &[("daily_inspection", "'{}'"), ("self_declaration", "'{}'")],
    )
    .await;
    let r3 = send_h(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{s3}/resume"),
        t,
        &admin_headers(),
        Some(json!({ "reason": "x" })),
    )
    .await
    .json();
    assert_eq!(r3["status"], "daily_inspection_pending");

    // 手動の中断 (webhook を積む)。終端は弾く
    let i = send(
        &app,
        "POST",
        &uri("interrupt"),
        t,
        Some(json!({ "reason": "体調" })),
    )
    .await
    .json();
    assert_eq!(
        (i["status"].clone(), i["cancel_reason"].clone()),
        (json!("interrupted"), json!("体調"))
    );
    assert_eq!(events(&app)[0].1["data"]["reason"], "体調");
    assert_eq!(
        send(&app, "POST", &uri("interrupt"), t, Some(json!({})))
            .await
            .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "POST",
            &format!("/api/tenko/sessions/{}/interrupt", Uuid::new_v4()),
            t,
            Some(json!({}))
        )
        .await
        .status,
        404
    );

    // 中止 (記録を書く)。完了・中止済みは弾く
    let c = send(
        &app,
        "POST",
        &uri("cancel"),
        t,
        Some(json!({ "reason": "取りやめ" })),
    )
    .await
    .json();
    assert_eq!(
        (c["status"].clone(), c["cancel_reason"].clone()),
        (json!("cancelled"), json!("取りやめ"))
    );
    assert_eq!(records(id).await.len(), 2);
    assert_eq!(
        send(&app, "POST", &uri("cancel"), t, Some(json!({})))
            .await
            .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "POST",
            &format!("/api/tenko/sessions/{}/cancel", Uuid::new_v4()),
            t,
            Some(json!({}))
        )
        .await
        .status,
        404
    );

    // 基準値が無ければ体調は見ずに通す (ログに残す)
    let emp2 = employee(t, "点呼 五郎").await;
    let s4 = sess(
        t,
        emp2,
        "pre_operation",
        "自動点呼",
        "self_declaration_pending",
        &[("systolic", "200")],
    )
    .await;
    let p = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{s4}/self-declaration"),
        t,
        Some(json!({ "illness": false, "fatigue": false, "sleep_deprivation": false })),
    )
    .await
    .json();
    assert_eq!(
        (p["status"].clone(), p["safety_judgment"]["status"].clone()),
        (json!("daily_inspection_pending"), json!("pass"))
    );
    assert!(app
        .logs()
        .contains(&"tenko: no health baseline, safety judgment defaults to pass".to_owned()));
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/self-declaration", Uuid::new_v4()),
            t,
            Some(json!({ "illness": false, "fatigue": false, "sleep_deprivation": false }))
        )
        .await
        .status,
        404
    );
}

/// 日常点検: 値は ok / ng だけ。NG は中止・記録・webhook。携行品のマスタが無ければ本人確認済みへ
#[tokio::test]
async fn daily_inspection_and_carrying_items() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 六郎").await;
    let all = |v: &str| json!({ "brakes": v, "tires": "ok", "lights": "ok", "steering": "ok", "wipers": "ok", "mirrors": "ok", "horn": "ok", "seatbelts": "ok" });
    let id = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "daily_inspection_pending",
        &[],
    )
    .await;
    let uri = format!("/api/tenko/sessions/{id}/daily-inspection");
    assert_eq!(
        send(&app, "PUT", &uri, t, Some(all("bad"))).await.status,
        400
    );
    let ok = send(&app, "PUT", &uri, t, Some(all("ok"))).await.json();
    assert_eq!(ok["status"], "identity_verified");
    assert_eq!(
        send(&app, "PUT", &uri, t, Some(all("ok"))).await.status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/daily-inspection", Uuid::new_v4()),
            t,
            Some(all("ok"))
        )
        .await
        .status,
        404
    );

    let ng = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "daily_inspection_pending",
        &[],
    )
    .await;
    let s = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{ng}/daily-inspection"),
        t,
        Some(all("ng")),
    )
    .await
    .json();
    assert_eq!(
        (s["status"].clone(), s["cancel_reason"].clone()),
        (json!("cancelled"), json!("日常点検異常"))
    );
    assert_eq!(s["daily_inspection"]["brakes"], "ng");
    assert_eq!(records(ng).await.len(), 1);
    let ev = events(&app);
    assert_eq!(
        (ev[0].0, ev[0].1["data"]["inspection"]["brakes"].clone()),
        ("inspection_ng", json!("ng"))
    );

    // 携行品: 状態の検査と、マスタに無い (別のテナントの) 品目の名前は空
    let c = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "carrying_items_pending",
        &[],
    )
    .await;
    let other = tenant().await;
    let foreign: Uuid = admin()
        .await
        .query_one("INSERT INTO alc_api.carrying_items (tenant_id, item_name) VALUES ($1, '他社') RETURNING id", &[&other])
        .await
        .unwrap()
        .get(0);
    let got = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{c}/carrying-items"),
        t,
        Some(json!({ "checks": [ { "item_id": foreign, "checked": false } ] })),
    )
    .await
    .json();
    assert_eq!(got["carrying_items_checked"]["items"][0]["item_name"], "");
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{c}/carrying-items"),
            t,
            Some(json!({ "checks": [] }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/carrying-items", Uuid::new_v4()),
            t,
            Some(json!({ "checks": [] }))
        )
        .await
        .status,
        404
    );
}

/// 遠隔点呼への切り替え・医療と指示の確認の検査
#[tokio::test]
async fn escalate_medical_and_instruction_guards() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 七郎").await;
    let id = sess(t, emp, "pre_operation", "自動点呼", "medical_pending", &[]).await;
    let uri = format!("/api/tenko/sessions/{id}/escalate-remote");
    assert_eq!(
        send(&app, "PUT", &uri, t, Some(json!({ "reason": "  " })))
            .await
            .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &uri,
            t,
            Some(json!({ "reason": "x".repeat(201) }))
        )
        .await
        .status,
        400
    );
    let e = send(
        &app,
        "PUT",
        &uri,
        t,
        Some(json!({ "reason": " 血圧計の故障 " })),
    )
    .await
    .json();
    assert_eq!(
        (
            e["tenko_method"].clone(),
            e["remote_escalation_reason"].clone(),
            e["status"].clone()
        ),
        (
            json!("遠隔点呼"),
            json!("血圧計の故障"),
            json!("medical_pending")
        )
    );
    // 遠隔点呼は血圧なしで医療を受ける
    let m = send(
        &app,
        "PUT",
        &format!("/api/tenko/sessions/{id}/medical"),
        t,
        Some(json!({ "temperature": 36.5, "medical_manual_input": true })),
    )
    .await
    .json();
    assert_eq!(
        (m["status"].clone(), m["medical_manual_input"].clone()),
        (json!("self_declaration_pending"), json!(true))
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{id}/medical"),
            t,
            Some(json!({}))
        )
        .await
        .json()["error"],
        "invalid_session_status"
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/medical", Uuid::new_v4()),
            t,
            Some(json!({}))
        )
        .await
        .json(),
        json!({ "error": "session_not_found" })
    );
    // 二重の切り替え・終了済み・無い
    assert_eq!(
        send(&app, "PUT", &uri, t, Some(json!({ "reason": "x" })))
            .await
            .status,
        400
    );
    let done = sess(t, emp, "pre_operation", "自動点呼", "completed", &[]).await;
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{done}/escalate-remote"),
            t,
            Some(json!({ "reason": "x" }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/escalate-remote", Uuid::new_v4()),
            t,
            Some(json!({ "reason": "x" }))
        )
        .await
        .status,
        404
    );
    // 指示の確認は指示待ちだけ
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{done}/instruction-confirm"),
            t,
            None
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            &format!("/api/tenko/sessions/{}/instruction-confirm", Uuid::new_v4()),
            t,
            None
        )
        .await
        .status,
        404
    );
    // 予定の乗務員が違うと 400、無い予定は 404
    let other = employee(t, "点呼 別人").await;
    let sch = schedule(t, other, "pre_operation", Some("安全運転")).await;
    assert_eq!(
        send(
            &app,
            "POST",
            "/api/tenko/sessions/start",
            t,
            Some(json!({ "schedule_id": sch, "employee_id": emp }))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send(
            &app,
            "POST",
            "/api/tenko/sessions/start",
            t,
            Some(json!({ "schedule_id": Uuid::new_v4(), "employee_id": emp }))
        )
        .await
        .status,
        404
    );
}

/// キオスクの自己再開: 理由が必須・終端は弾く・1 回まで・AuthUser が無ければ再開した人は空
#[tokio::test]
async fn self_resume_rules() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 八郎").await;
    let id = sess(t, emp, "pre_operation", "自動点呼", "medical_pending", &[]).await;
    let uri = format!("/api/tenko/sessions/{id}/self-resume");
    assert_eq!(
        send(&app, "POST", &uri, t, Some(json!({ "reason": " " })))
            .await
            .json()["error"],
        "reason_required"
    );
    let r = send(
        &app,
        "POST",
        &uri,
        t,
        Some(json!({ "reason": " 電池切れ " })),
    )
    .await
    .json();
    assert_eq!(
        (
            r["resume_reason"].clone(),
            r["resumed_by_user_id"].clone(),
            r["status"].clone()
        ),
        (json!("電池切れ"), Value::Null, json!("medical_pending"))
    );
    assert_eq!(
        send(&app, "POST", &uri, t, Some(json!({ "reason": "x" })))
            .await
            .json()["error"],
        "already_resumed"
    );
    let s2 = sess(t, emp, "pre_operation", "自動点呼", "medical_pending", &[]).await;
    let by = send_h(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{s2}/self-resume"),
        t,
        &admin_headers(),
        Some(json!({ "reason": "x" })),
    )
    .await
    .json();
    assert_eq!(by["resumed_by_user_id"], ADMIN_USER);
    let done = sess(t, emp, "pre_operation", "自動点呼", "interrupted", &[]).await;
    assert_eq!(
        send(
            &app,
            "POST",
            &format!("/api/tenko/sessions/{done}/self-resume"),
            t,
            Some(json!({ "reason": "x" }))
        )
        .await
        .json()["error"],
        "session_not_resumable"
    );
    let gone = send(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{}/self-resume", Uuid::new_v4()),
        t,
        Some(json!({ "reason": "x" })),
    )
    .await;
    assert_eq!(
        (gone.status, gone.json()),
        (404, json!({ "error": "session_not_found" }))
    );
}

/// 運行管理者の判定 (rust の device_dev_axis_test の点呼の検査を含む)
#[tokio::test]
async fn manager_judgment_rules() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 九郎").await;
    let judge = manager(t, "manager").await;
    let driver_only = manager(t, "driver").await;
    let auto = sess(t, emp, "pre_operation", "自動点呼", "completed", &[]).await;
    let it = sess(t, emp, "pre_operation", "IT点呼", "completed", &[]).await;
    let key = [("X-Device-Role", "device-tenko-manager")];
    let dev_key = [
        ("X-Device-Role", "device-tenko-manager"),
        ("X-Device-Dev", "1"),
    ];
    let body = |j: &str, by: Uuid, m: Option<&str>| json!({ "judgment": j, "judged_by_employee_id": by, "reason": "確認", "method": m });
    let u = |id: Uuid| format!("/api/tenko/sessions/{id}/judgment");

    // 管理者ログインでも運行管理者の鍵でもない → 401。判定の値・判定した人の検査
    assert_eq!(
        send(&app, "POST", &u(auto), t, Some(body("ok", judge, None)))
            .await
            .status,
        401
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &admin_headers(),
            Some(body("maybe", judge, None))
        )
        .await
        .status,
        400
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &admin_headers(),
            Some(body("ok", Uuid::new_v4(), None))
        )
        .await
        .status,
        403
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &admin_headers(),
            Some(body("ok", driver_only, None))
        )
        .await
        .status,
        403
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(Uuid::new_v4()),
            t,
            &admin_headers(),
            Some(body("ok", judge, None))
        )
        .await
        .status,
        404
    );
    // 管理者は点呼の方法で絞らない (自動点呼に確認の方法は付けない)
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &admin_headers(),
            Some(body("ok", judge, Some("it")))
        )
        .await
        .status,
        400
    );
    let a = send_h(
        &app,
        "POST",
        &u(auto),
        t,
        &admin_headers(),
        Some(body("ng", judge, None)),
    )
    .await
    .json();
    assert_eq!(
        (
            a["manager_judgment"].clone(),
            a["manager_judgment_by"].clone()
        ),
        (json!("ng"), json!(judge))
    );
    // dev でない運行管理者の鍵は IT点呼 だけ。IT点呼 は確認の方法が要る (入っていれば押し直しで省ける)
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &key,
            Some(body("ok", judge, None))
        )
        .await
        .status,
        403
    );
    assert_eq!(
        send_h(&app, "POST", &u(it), t, &key, Some(body("ok", judge, None)))
            .await
            .status,
        400
    );
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(it),
            t,
            &key,
            Some(body("ok", judge, Some("phone")))
        )
        .await
        .status,
        400
    );
    let k = send_h(
        &app,
        "POST",
        &u(it),
        t,
        &key,
        Some(body("ok", judge, Some("in_person"))),
    )
    .await
    .json();
    assert_eq!(k["manager_judgment_method"], "in_person");
    let again = send_h(&app, "POST", &u(it), t, &key, Some(body("ng", judge, None)))
        .await
        .json();
    assert_eq!(
        (
            again["manager_judgment"].clone(),
            again["manager_judgment_method"].clone()
        ),
        (json!("ng"), json!("in_person"))
    );
    // dev の運行管理者の鍵は dev のセッションだけ見え、点呼の方法で絞らない
    let dev_auto = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "completed",
        &[("is_dev", "true")],
    )
    .await;
    assert_eq!(
        send_h(
            &app,
            "POST",
            &u(auto),
            t,
            &dev_key,
            Some(body("ok", judge, None))
        )
        .await
        .status,
        404
    );
    let d = send_h(
        &app,
        "POST",
        &u(dev_auto),
        t,
        &dev_key,
        Some(body("ok", judge, None)),
    )
    .await
    .json();
    assert_eq!(d["manager_judgment"], "ok");
}

/// 一覧の絞り込みと頁、dev の軸
#[tokio::test]
async fn list_filters_and_paging() {
    let app = support::app().await;
    let t = tenant().await;
    let other = tenant().await;
    let emp = employee(t, "一覧 一郎").await;
    let emp2 = employee(t, "一覧 二郎").await;
    let a = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "completed",
        &[
            ("started_at", "'2026-10-01T00:00:00Z'"),
            ("created_at", "'2026-10-01T00:00:00Z'"),
        ],
    )
    .await;
    let b = sess(
        t,
        emp2,
        "post_operation",
        "IT点呼",
        "completed",
        &[
            ("started_at", "'2026-10-02T00:00:00Z'"),
            ("created_at", "'2026-10-02T00:00:00Z'"),
            ("manager_judgment", "'ok'"),
        ],
    )
    .await;
    let c = sess(
        t,
        emp,
        "pre_operation",
        "遠隔点呼",
        "interrupted",
        &[
            ("started_at", "'2026-10-03T00:00:00Z'"),
            ("created_at", "'2026-10-03T00:00:00Z'"),
        ],
    )
    .await;
    sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "completed",
        &[("is_dev", "true")],
    )
    .await;
    let oe = employee(other, "他社").await;
    sess(other, oe, "pre_operation", "自動点呼", "completed", &[]).await;

    let ids = |v: &Value| -> Vec<String> {
        v["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["id"].as_str().unwrap().to_owned())
            .collect()
    };
    let all = send(&app, "GET", "/api/tenko/sessions", t, None)
        .await
        .json();
    assert_eq!(
        (
            ids(&all),
            all["total"].clone(),
            all["page"].clone(),
            all["per_page"].clone()
        ),
        (
            vec![c.to_string(), b.to_string(), a.to_string()],
            json!(3),
            json!(1),
            json!(50)
        )
    );
    for (q, want) in [
        (format!("employee_id={emp}"), vec![c, a]),
        ("status=completed".to_owned(), vec![b, a]),
        ("tenko_type=post_operation".to_owned(), vec![b]),
        (
            "tenko_method=%E9%81%A0%E9%9A%94%E7%82%B9%E5%91%BC".to_owned(),
            vec![c],
        ),
        ("judgment_pending=true".to_owned(), vec![c, a]),
        ("judgment_pending=false".to_owned(), vec![c, b, a]),
        (
            "date_from=2026-10-02T00:00:00Z&date_to=2026-10-02T12:00:00Z".to_owned(),
            vec![b],
        ),
    ] {
        let got = send(&app, "GET", &format!("/api/tenko/sessions?{q}"), t, None)
            .await
            .json();
        assert_eq!(
            ids(&got),
            want.iter().map(Uuid::to_string).collect::<Vec<_>>(),
            "{q}"
        );
    }
    let page = send(
        &app,
        "GET",
        "/api/tenko/sessions?page=2&per_page=2",
        t,
        None,
    )
    .await
    .json();
    assert_eq!(
        (ids(&page), page["total"].clone()),
        (vec![a.to_string()], json!(3))
    );
    let dev = send_h(
        &app,
        "GET",
        "/api/tenko/sessions",
        t,
        &[("X-Device-Dev", "1")],
        None,
    )
    .await
    .json();
    assert_eq!(dev["total"], 1);
    assert_eq!(
        send(
            &app,
            "GET",
            &format!("/api/tenko/sessions/{a}"),
            other,
            None
        )
        .await
        .status,
        404
    );
    assert_eq!(
        support::call(&app, "GET", "/api/tenko/sessions", None, None)
            .await
            .status,
        401
    );
}

/// 記録の書き込みが失敗しても区切りだけ戻し、セッションの更新は残す (rust の `let _ = create_tenko_record(..)`)
#[tokio::test]
async fn record_failure_keeps_session_update() {
    let su = admin().await;
    su.batch_execute(
        "CREATE OR REPLACE FUNCTION alc_api.tenko_test_reject_record() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN IF NEW.location = 'TEST-REJECT-RECORD' THEN RAISE EXCEPTION 'test reject'; END IF; RETURN NEW; END $$; \
         DROP TRIGGER IF EXISTS tenko_test_reject_record ON alc_api.tenko_records; \
         CREATE TRIGGER tenko_test_reject_record BEFORE INSERT ON alc_api.tenko_records \
         FOR EACH ROW EXECUTE FUNCTION alc_api.tenko_test_reject_record();",
    )
    .await
    .unwrap();
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee(t, "記録 失敗").await;
    let id = sess(
        t,
        emp,
        "pre_operation",
        "自動点呼",
        "medical_pending",
        &[("location", "'TEST-REJECT-RECORD'")],
    )
    .await;
    let c = send(
        &app,
        "POST",
        &format!("/api/tenko/sessions/{id}/cancel"),
        t,
        Some(json!({})),
    )
    .await;
    assert_eq!(
        (c.status, c.json()["status"].clone()),
        (200, json!("cancelled"))
    );
    assert!(records(id).await.is_empty());
    assert!(app
        .logs()
        .iter()
        .any(|l| l.starts_with("tenko: session record failed: db (")));
    support::assert_logs_clean(&app, &[id.to_string(), t.to_string()]);
}

/// DB の失敗は 500。理由付きの口 (医療・自己再開) は `{"error":"internal_error"}`、ほかは空
#[tokio::test]
async fn db_failures_are_500() {
    let app = build(broken_client().await);
    let t = Uuid::new_v4();
    let id = Uuid::new_v4();
    let s = |step: &str| format!("/api/tenko/sessions/{id}/{step}");
    let cases: Vec<(&str, String, Option<Value>, &str)> = vec![
        ("GET", "/api/tenko/sessions".into(), None, "session list"),
        (
            "GET",
            format!("/api/tenko/sessions/{id}"),
            None,
            "session get",
        ),
        (
            "POST",
            "/api/tenko/sessions/start".into(),
            Some(json!({ "employee_id": id })),
            "session start",
        ),
        (
            "PUT",
            s("alcohol"),
            Some(json!({ "alcohol_result": "pass", "alcohol_value": 0.0 })),
            "session alcohol",
        ),
        (
            "PUT",
            s("escalate-remote"),
            Some(json!({ "reason": "x" })),
            "session escalate",
        ),
        ("PUT", s("instruction-confirm"), None, "session instruction"),
        (
            "PUT",
            s("report"),
            Some(json!({ "vehicle_road_status": "a", "driver_alternation": "b" })),
            "session report",
        ),
        ("POST", s("cancel"), Some(json!({})), "session cancel"),
        (
            "PUT",
            s("self-declaration"),
            Some(json!({ "illness": false, "fatigue": false, "sleep_deprivation": false })),
            "session self-declaration",
        ),
        (
            "PUT",
            s("daily-inspection"),
            Some(
                json!({ "brakes": "ok", "tires": "ok", "lights": "ok", "steering": "ok", "wipers": "ok", "mirrors": "ok", "horn": "ok", "seatbelts": "ok" }),
            ),
            "session daily-inspection",
        ),
        (
            "PUT",
            s("carrying-items"),
            Some(json!({ "checks": [] })),
            "session carrying-items",
        ),
        ("POST", s("interrupt"), Some(json!({})), "session interrupt"),
    ];
    for (method, uri, body, stage) in cases {
        let got = call_with(&app, method, &uri, Some(t), &[], body).await;
        assert_eq!((got.status, got.body.is_empty()), (500, true), "{stage}");
        assert!(
            app.logs()
                .iter()
                .any(|l| l.starts_with(&format!("tenko: {stage} failed: db ("))),
            "{stage}"
        );
    }
    let r = call_with(
        &app,
        "POST",
        &s("resume"),
        Some(t),
        &admin_headers(),
        Some(json!({ "reason": "x" })),
    )
    .await;
    assert_eq!(r.status, 500);
    let j = call_with(
        &app,
        "POST",
        &s("judgment"),
        Some(t),
        &admin_headers(),
        Some(json!({ "judgment": "ok", "judged_by_employee_id": id })),
    )
    .await;
    assert_eq!(j.status, 500);
    for (uri, body) in [
        (s("medical"), json!({})),
        (s("self-resume"), json!({ "reason": "x" })),
    ] {
        let got = call_with(
            &app,
            if uri.ends_with("medical") {
                "PUT"
            } else {
                "POST"
            },
            &uri,
            Some(t),
            &[],
            Some(body),
        )
        .await;
        assert_eq!(
            (got.status, got.json()),
            (500, json!({ "error": "internal_error" }))
        );
    }
    support::assert_logs_clean(&app, &[id.to_string(), t.to_string()]);
}

// --- webhook の送信 ---

/// 送ったもの (URL・ヘッダー・本文)
type Sent = (String, Vec<(&'static str, String)>, String);

#[derive(Default)]
struct FakeHttp {
    replies: StdMutex<Vec<HttpResult>>,
    sent: StdMutex<Vec<Sent>>,
}

impl WebhookHttp for FakeHttp {
    fn post<'a>(
        &'a self,
        url: &'a str,
        headers: Vec<(&'static str, String)>,
        body: String,
    ) -> BoxFuture<'a, HttpResult> {
        self.sent
            .lock()
            .unwrap()
            .push((url.to_owned(), headers, body));
        let reply = self.replies.lock().unwrap().remove(0);
        Box::pin(async move { reply })
    }
}

#[derive(Default)]
struct FakeSleeper(StdMutex<Vec<u64>>);

impl Sleeper for FakeSleeper {
    fn sleep(&self, secs: u64) -> BoxFuture<'_, ()> {
        self.0.lock().unwrap().push(secs);
        Box::pin(async {})
    }
}

async fn config(t: Uuid, event_type: &str, secret: Option<&str>, enabled: bool) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.webhook_configs (tenant_id, event_type, url, secret, enabled) \
             VALUES ($1, $2, 'https://hooks.example.invalid/x', $3, $4) RETURNING id",
            &[&t, &event_type, &secret, &enabled],
        )
        .await
        .unwrap()
        .get(0)
}

async fn deliveries(config: Uuid) -> Vec<(i32, Option<i32>, Option<String>, bool, bool)> {
    admin()
        .await
        .query(
            "SELECT attempt, status_code, response_body, success, delivered_at IS NOT NULL \
             FROM alc_api.webhook_deliveries WHERE config_id = $1 ORDER BY attempt",
            &[&config],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect()
}

fn sender(http: Arc<FakeHttp>, sleeper: Arc<FakeSleeper>) -> Sender {
    Sender { http, sleeper }
}

/// 送り方は rust と同じ: 設定が無い・無効なら送らない、署名を付ける、失敗は 1 秒・5 秒待って 3 回まで、毎回記録する
#[tokio::test]
async fn webhook_delivery_retries_and_records() {
    let app = support::app().await;
    let t = tenant().await;
    let signed = config(t, "alcohol_detected", Some("s3cret"), true).await;
    let plain = config(t, "tenko_interrupted", None, true).await;
    config(t, "inspection_ng", None, false).await;
    let http = Arc::new(FakeHttp::default());
    *http.replies.lock().unwrap() = vec![
        Ok((500, "busy".into())),
        Err("timeout".into()),
        Ok((204, String::new())),
        Ok((503, "down".into())),
        Ok((503, "down".into())),
        Ok((503, "down".into())),
    ];
    let sleeper = Arc::new(FakeSleeper::default());
    let payload = json!({ "event": "alcohol_detected", "n": 1 });
    let events = vec![
        (t, Event::new("alcohol_detected", payload.clone())),
        (t, Event::new("inspection_ng", json!({}))),
        (t, Event::new("tenko_overdue", json!({}))),
        (t, Event::new("tenko_interrupted", json!({ "n": 2 }))),
    ];
    let logs = Arc::new(StdMutex::new(Vec::<String>::new()));
    let sink = logs.clone();
    deliver_all(
        app.pg.clone(),
        sender(http.clone(), sleeper.clone()),
        Arc::new(move |m: &str| sink.lock().unwrap().push(m.to_owned())),
        events,
    )
    .await;

    let sent = http.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 6);
    let (url, headers, body) = &sent[0];
    assert_eq!(url, "https://hooks.example.invalid/x");
    assert_eq!(body, &payload.to_string());
    assert!(headers.contains(&("X-Webhook-Event", "alcohol_detected".to_owned())));
    assert!(headers.contains(&(
        "X-Webhook-Signature",
        tenko::deliver::signature("s3cret", body)
    )));
    assert!(!sent[3].1.iter().any(|(k, _)| *k == "X-Webhook-Signature"));
    assert_eq!(
        deliveries(signed).await,
        vec![
            (1, Some(500), Some("busy".into()), false, false),
            (2, None, Some("timeout".into()), false, false),
            (3, Some(204), Some(String::new()), true, true),
        ]
    );
    assert_eq!(deliveries(plain).await.len(), 3);
    assert_eq!(*sleeper.0.lock().unwrap(), vec![1, 5, 1, 5]);
    assert_eq!(
        *logs.lock().unwrap(),
        vec!["tenko: webhook delivery failed: timeout".to_owned()]
    );
}

/// 設定を読めない・記録を書けないときは固定の語でログに出し、止めない
#[tokio::test]
async fn webhook_delivery_db_failures_are_logged() {
    let t = tenant().await;
    config(t, "alcohol_detected", None, true).await;
    let logs = Arc::new(StdMutex::new(Vec::<String>::new()));
    let sink = logs.clone();
    let log: tenko::LogSink = Arc::new(move |m: &str| sink.lock().unwrap().push(m.to_owned()));
    let ev = || vec![(t, Event::new("alcohol_detected", json!({})))];

    let broken = Arc::new(futures_util::lock::Mutex::new(broken_client().await));
    let http = Arc::new(FakeHttp::default());
    deliver_all(
        broken,
        sender(http.clone(), Arc::new(FakeSleeper::default())),
        log.clone(),
        ev(),
    )
    .await;
    assert!(http.sent.lock().unwrap().is_empty());

    // 読み取り専用のロール: 設定は読めて送るが、記録は書けない
    let ro = Arc::new(futures_util::lock::Mutex::new(
        client_as(READ_ONLY_ROLE).await,
    ));
    *http.replies.lock().unwrap() = vec![Ok((200, "ok".into()))];
    deliver_all(
        ro,
        sender(http.clone(), Arc::new(FakeSleeper::default())),
        log,
        ev(),
    )
    .await;
    assert_eq!(http.sent.lock().unwrap().len(), 1);
    let logs = logs.lock().unwrap().clone();
    assert!(logs[0].starts_with("tenko: webhook config lookup failed: db ("));
    assert!(logs[1].starts_with("tenko: webhook record failed: db ("));
    assert_eq!(logs.len(), 2);
}
