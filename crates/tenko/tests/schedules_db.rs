//! 点呼の予定とダッシュボードを実 DB で通す (rust-alc-api の tenko_schedules・dashboard の要点)。

mod support;

use serde_json::{json, Value};
use support::{admin, app, call, call_dev, call_with, employee, session, tenant};
use uuid::Uuid;

fn body(employee: Uuid, tenko_type: &str, at: &str, instruction: Option<&str>) -> Value {
    json!({
        "employee_id": employee,
        "tenko_type": tenko_type,
        "responsible_manager_name": "管理 一郎",
        "scheduled_at": at,
        "instruction": instruction,
    })
}

#[tokio::test]
async fn schedule_crud() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let e = employee(t, "予定 太郎").await;

    // 検査: 種類・業務前の指示事項
    for bad in [
        body(e, "other", "2026-10-28T00:00:00Z", Some("x")),
        body(e, "pre_operation", "2026-10-28T00:00:00Z", None),
    ] {
        assert_eq!(
            call(&app, "POST", "/api/tenko/schedules", Some(t), Some(bad))
                .await
                .status,
            400
        );
    }

    let got = call(
        &app,
        "POST",
        "/api/tenko/schedules",
        Some(t),
        Some(body(
            e,
            "pre_operation",
            "2026-10-28T00:00:00Z",
            Some("飲酒に注意"),
        )),
    )
    .await;
    assert_eq!(got.status, 201);
    let a = got.json();
    assert_eq!(a["consumed"], false);
    assert_eq!(a["instruction"], "飲酒に注意");
    assert_eq!(a["overdue_notified_at"], json!(null));
    let a_id = a["id"].as_str().unwrap().to_owned();

    // 一括 (空は 400、1 件でも検査に外れたら 400)
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/tenko/schedules/batch",
            Some(t),
            Some(json!({ "schedules": [] }))
        )
        .await
        .status,
        400
    );
    let mixed = json!({ "schedules": [body(e, "post_operation", "2026-10-29T00:00:00Z", None), body(e, "x", "2026-10-29T00:00:00Z", None)] });
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/tenko/schedules/batch",
            Some(t),
            Some(mixed)
        )
        .await
        .status,
        400
    );
    let batch = json!({ "schedules": [body(e, "post_operation", "2026-10-29T00:00:00Z", None), body(e, "post_operation", "2026-10-30T00:00:00Z", None)] });
    let got = call(
        &app,
        "POST",
        "/api/tenko/schedules/batch",
        Some(t),
        Some(batch),
    )
    .await;
    assert_eq!(got.status, 201);
    assert_eq!(got.json().as_array().unwrap().len(), 2);

    // 一覧 (予定の新しい順)・絞り込み・頁
    let list = call(&app, "GET", "/api/tenko/schedules", Some(t), None)
        .await
        .json();
    assert_eq!(list["total"], 3);
    assert_eq!(list["schedules"][2]["id"], json!(a_id));
    let pre = call(
        &app,
        "GET",
        "/api/tenko/schedules?tenko_type=pre_operation&consumed=false",
        Some(t),
        None,
    )
    .await
    .json();
    assert_eq!(pre["total"], 1);
    let ranged = call(&app, "GET", &format!("/api/tenko/schedules?employee_id={e}&date_from=2026-10-29T00:00:00Z&date_to=2026-10-29T23:00:00Z&page=1&per_page=10"), Some(t), None).await.json();
    assert_eq!(
        (ranged["total"].clone(), ranged["per_page"].clone()),
        (json!(1), json!(10))
    );
    assert_eq!(
        call(&app, "GET", "/api/tenko/schedules", Some(other), None)
            .await
            .json()["total"],
        0
    );

    // 1 件・更新・未消費の予定
    let one = call(
        &app,
        "GET",
        &format!("/api/tenko/schedules/{a_id}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!((one.status, one.json()["id"].clone()), (200, json!(a_id)));
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/tenko/schedules/{a_id}"),
            Some(other),
            None
        )
        .await
        .status,
        404
    );
    let upd = call(
        &app,
        "PUT",
        &format!("/api/tenko/schedules/{a_id}"),
        Some(t),
        Some(json!({ "instruction": "睡眠に注意" })),
    )
    .await;
    assert_eq!(upd.status, 200);
    assert_eq!(
        (
            upd.json()["instruction"].clone(),
            upd.json()["responsible_manager_name"].clone()
        ),
        (json!("睡眠に注意"), json!("管理 一郎"))
    );
    let pending = call(
        &app,
        "GET",
        &format!("/api/tenko/schedules/pending/{e}"),
        Some(t),
        None,
    )
    .await
    .json();
    assert_eq!(pending.as_array().unwrap().len(), 3);
    assert_eq!(pending[0]["id"], json!(a_id));

    // 消費済みは更新も削除もしない
    admin()
        .await
        .execute(
            "UPDATE alc_api.tenko_schedules SET consumed = TRUE WHERE id = $1",
            &[&Uuid::parse_str(&a_id).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("/api/tenko/schedules/{a_id}"),
            Some(t),
            Some(json!({}))
        )
        .await
        .status,
        404
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/tenko/schedules/{a_id}"),
            Some(t),
            None
        )
        .await
        .status,
        404
    );
    let b_id = list["schedules"][0]["id"].as_str().unwrap().to_owned();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/tenko/schedules/{b_id}"),
            Some(t),
            None
        )
        .await
        .status,
        204
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/tenko/schedules/{b_id}"),
            Some(t),
            None
        )
        .await
        .status,
        404
    );

    // dev の軸: dev で作った予定は dev でしか見えない
    let dev = call_with(
        &app,
        "POST",
        "/api/tenko/schedules",
        Some(t),
        &[("X-Device-Dev", "1")],
        Some(body(e, "post_operation", "2026-10-31T00:00:00Z", None)),
    )
    .await;
    assert_eq!(dev.status, 201);
    assert_eq!(
        call_dev(&app, "GET", "/api/tenko/schedules", t)
            .await
            .json()["total"],
        1
    );
    assert_eq!(
        call(&app, "GET", "/api/tenko/schedules", Some(t), None)
            .await
            .json()["total"],
        2
    );
}

#[tokio::test]
async fn dashboard_counts() {
    let app = app().await;
    let t = tenant().await;
    let e = employee(t, "画面 太郎").await;
    // 未消費の予定 2 件 (1 件は 2 時間前 = 超過)、消費済み 1 件
    let su = admin().await;
    for (at, consumed) in [
        ("now() - interval '2 hours'", false),
        ("now() + interval '1 hour'", false),
        ("now() - interval '3 hours'", true),
    ] {
        su.execute(
            &format!("INSERT INTO alc_api.tenko_schedules (tenant_id, employee_id, tenko_type, responsible_manager_name, scheduled_at, consumed) VALUES ($1, $2, 'post_operation', 'm', {at}, {consumed})"),
            &[&t, &e],
        )
        .await
        .unwrap();
    }
    for status in ["identity_verified", "interrupted", "completed", "cancelled"] {
        let s = session(t, e, "自動点呼", status, false, None).await;
        su.execute("UPDATE alc_api.tenko_sessions SET completed_at = now() WHERE id = $1 AND status IN ('completed', 'cancelled')", &[&s]).await.unwrap();
    }
    let got = call(&app, "GET", "/api/tenko/dashboard", Some(t), None).await;
    assert_eq!(got.status, 200);
    let v = got.json();
    assert_eq!(
        (
            v["pending_schedules"].clone(),
            v["active_sessions"].clone(),
            v["interrupted_sessions"].clone(),
            v["completed_today"].clone(),
            v["cancelled_today"].clone()
        ),
        (json!(2), json!(1), json!(1), json!(1), json!(1))
    );
    assert_eq!(v["overdue_schedules"].as_array().unwrap().len(), 1);
    // dev の軸は空
    let dev = call_dev(&app, "GET", "/api/tenko/dashboard", t)
        .await
        .json();
    assert_eq!(
        (
            dev["pending_schedules"].clone(),
            dev["active_sessions"].clone()
        ),
        (json!(0), json!(0))
    );
}
