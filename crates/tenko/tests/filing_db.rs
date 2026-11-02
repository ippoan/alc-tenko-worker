//! 点呼の届出・認定申請に差し込む値 (`/api/tenko/filing-settings`) を実 DB で通す。値は架空。

mod support;

use serde_json::json;
use support::{
    admin, app, assert_logs_clean, broken_client, build, call, call_with, client_as, tenant,
    READ_ONLY_ROLE,
};
use uuid::Uuid;

const PATH: &str = "/api/tenko/filing-settings";

fn as_role(role: &str) -> [(&'static str, String); 3] {
    [
        ("X-User-ID", Uuid::new_v4().to_string()),
        ("X-User-Email", "filing-test@example.com".to_owned()),
        ("X-User-Role", role.to_owned()),
    ]
}

async fn call_as(
    app: &support::App,
    role: &str,
    method: &str,
    tenant: Uuid,
    body: Option<serde_json::Value>,
) -> support::Got {
    let h = as_role(role);
    let headers: Vec<(&str, &str)> = h.iter().map(|(k, v)| (*k, v.as_str())).collect();
    call_with(app, method, PATH, Some(tenant), &headers, body).await
}

fn sample() -> serde_json::Value {
    json!({
        "business_number": " 000000000001 ",
        "trade_name": "",
        "business_type": 1,
        "offices": [
            { "name": "架空運輸 本社営業所", "address": "架空県架空市1-1", "role": 1 },
            { "name": "架空運輸 第二車庫", "address": null, "role": 2 }
        ],
        "device_name": "",
        "auto_cert_pre": "前-0001",
        "operator_name": "架空運輸株式会社",
        "applicant_contact_email": "contact@example.com",
        "product_url": "https://example.com/tenko",
        "updated_at": "2000-01-01T00:00:00Z"
    })
}

#[tokio::test]
async fn filing_settings_get_put() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;

    // admin 以外 (役の無い要求・admin でない役) は 403
    for got in [
        call(&app, "GET", PATH, Some(t), None).await,
        call_as(&app, "viewer", "GET", t, None).await,
        call_as(&app, "viewer", "PUT", t, Some(sample())).await,
    ] {
        assert_eq!(
            (got.status, got.json()),
            (403, json!({ "error": "forbidden" }))
        );
    }

    // 行が無ければ既定値
    let got = call_as(&app, "admin", "GET", t, None).await;
    assert_eq!(got.status, 200);
    let v = got.json();
    assert_eq!(
        (
            v["device_name"].clone(),
            v["offices"].clone(),
            v["business_number"].clone(),
            v["updated_at"].clone()
        ),
        (json!("一歩庵点呼 v1"), json!([]), json!(null), json!(null))
    );

    // 本文が読めない・欄が外れる → 400 (保存しない)
    let bad = call_with(
        &app,
        "PUT",
        PATH,
        Some(t),
        &as_role("admin")
            .iter()
            .map(|(k, v)| (*k, v.as_str()))
            .collect::<Vec<_>>(),
        None,
    )
    .await;
    assert_eq!(
        (bad.status, bad.json()),
        (400, json!({ "error": "invalid_body" }))
    );
    for (body, word) in [
        (
            json!({ "business_number": "123" }),
            "invalid_business_number",
        ),
        (json!({ "business_type": 9 }), "invalid_business_type"),
        (
            json!({ "offices": [{ "name": "a", "role": 5 }] }),
            "invalid_office",
        ),
    ] {
        let got = call_as(&app, "admin", "PUT", t, Some(body)).await;
        assert_eq!((got.status, got.json()), (400, json!({ "error": word })));
    }

    // 保存 (空白を落とす・空は null・機器の名称は既定に戻す・updated_at は本文から読まない)
    let saved = call_as(&app, "admin", "PUT", t, Some(sample())).await;
    assert_eq!(saved.status, 200);
    let v = saved.json();
    assert_eq!(v["business_number"], "000000000001");
    assert_eq!(v["trade_name"], json!(null));
    assert_eq!(v["device_name"], "一歩庵点呼 v1");
    assert_eq!(
        v["offices"][1],
        json!({ "name": "架空運輸 第二車庫", "address": null, "role": 2 })
    );
    assert_ne!(v["updated_at"], json!("2000-01-01T00:00:00Z"));
    assert_eq!(call_as(&app, "admin", "GET", t, None).await.json(), v);

    // 置き換え: 省略した欄は null / 空に戻る
    let again = call_as(
        &app,
        "admin",
        "PUT",
        t,
        Some(json!({ "device_name": "架空の機器" })),
    )
    .await
    .json();
    assert_eq!(
        (
            again["device_name"].clone(),
            again["offices"].clone(),
            again["operator_name"].clone()
        ),
        (json!("架空の機器"), json!([]), json!(null))
    );

    // 別のテナントからは見えない
    assert_eq!(
        call_as(&app, "admin", "GET", other, None).await.json()["device_name"],
        "一歩庵点呼 v1"
    );
    let rows: i64 = admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.tenko_filing_settings WHERE tenant_id = $1",
            &[&other],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(rows, 0);

    // 表の営業所が読めない形でも、画面は開ける (営業所を空にする)
    admin()
        .await
        .execute(
            "UPDATE alc_api.tenko_filing_settings SET offices = '[1]'::jsonb WHERE tenant_id = $1",
            &[&t],
        )
        .await
        .unwrap();
    assert_eq!(
        call_as(&app, "admin", "GET", t, None).await.json()["offices"],
        json!([])
    );
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn filing_settings_db_failures() {
    let t = tenant().await;

    // 書けないロール → 保存は 500
    let app = build(client_as(READ_ONLY_ROLE).await);
    assert_eq!(call_as(&app, "admin", "GET", t, None).await.status, 200);
    assert_eq!(
        call_as(&app, "admin", "PUT", t, Some(sample()))
            .await
            .status,
        500
    );
    assert_eq!(app.logs().len(), 1);
    assert!(app.logs()[0].starts_with("tenko: filing settings put failed: db ("));
    assert_logs_clean(&app, &[t.to_string()]);

    // 切れた接続 → 読みも 500
    let app = build(broken_client().await);
    assert_eq!(call_as(&app, "admin", "GET", t, None).await.status, 500);
    assert!(app.logs()[0].starts_with("tenko: filing settings get failed: db ("));
}
