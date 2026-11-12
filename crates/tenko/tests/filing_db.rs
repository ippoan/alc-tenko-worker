//! 点呼の届出・認定申請に差し込む値 (`/api/tenko/filing-settings`) を実 DB で通す。値は架空。

mod support;

use serde_json::json;
use support::{
    admin, app, assert_logs_clean, broken_client, build, call, call_with, client_as, company,
    tenant, READ_ONLY_ROLE,
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

/// 事業者 `company` の設定を打つ (口は `?company_id=` が要る)
async fn call_as(
    app: &support::App,
    role: &str,
    method: &str,
    tenant: Uuid,
    company: Uuid,
    body: Option<serde_json::Value>,
) -> support::Got {
    call_query(
        app,
        role,
        method,
        tenant,
        &format!("company_id={company}"),
        body,
    )
    .await
}

/// クエリを指定して打つ
async fn call_query(
    app: &support::App,
    role: &str,
    method: &str,
    tenant: Uuid,
    query: &str,
    body: Option<serde_json::Value>,
) -> support::Got {
    let h = as_role(role);
    let headers: Vec<(&str, &str)> = h.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let uri = if query.is_empty() {
        PATH.to_owned()
    } else {
        format!("{PATH}?{query}")
    };
    call_with(app, method, &uri, Some(tenant), &headers, body).await
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
        "operator_kana": "カクウウンユ",
        "operator_postal_code": "000-0001",
        "operator_corporate_number": "0000000000001",
        "operator_representative_title": "代表取締役",
        "operator_contact_dept": " 架空課 ",
        "partners": [
            { "name": " 架空物流株式会社 ", "address": "架空県架空市2-2", "representative": "", "offices": [
                { "name": "架空物流 本社営業所", "address": " " }
            ] }
        ],
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
    let c = company(t, "架空運輸", true).await;
    let other_c = company(other, "架空運輸", true).await;

    // admin 以外 (役の無い要求・admin でない役) は 403
    for got in [
        call(
            &app,
            "GET",
            &format!("{PATH}?company_id={c}"),
            Some(t),
            None,
        )
        .await,
        call_as(&app, "viewer", "GET", t, c, None).await,
        call_as(&app, "viewer", "PUT", t, c, Some(sample())).await,
    ] {
        assert_eq!(
            (got.status, got.json()),
            (403, json!({ "error": "forbidden" }))
        );
    }

    // 行が無ければ既定値
    let got = call_as(&app, "admin", "GET", t, c, None).await;
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
        &format!("{PATH}?company_id={c}"),
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
        (
            json!({ "operator_postal_code": "12345" }),
            "invalid_postal_code",
        ),
        (
            json!({ "operator_corporate_number": "123" }),
            "invalid_corporate_number",
        ),
        (json!({ "partners": [{ "name": " " }] }), "invalid_partner"),
    ] {
        let got = call_as(&app, "admin", "PUT", t, c, Some(body)).await;
        assert_eq!((got.status, got.json()), (400, json!({ "error": word })));
    }

    // 保存 (空白を落とす・空は null・機器の名称は既定に戻す・updated_at は本文から読まない)
    let saved = call_as(&app, "admin", "PUT", t, c, Some(sample())).await;
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
    // 213 の欄: 郵便番号はハイフンを落とす・相手の会社は空白を落として空は null
    assert_eq!(
        (
            v["operator_kana"].clone(),
            v["operator_postal_code"].clone(),
            v["operator_corporate_number"].clone(),
            v["operator_representative_title"].clone(),
            v["operator_contact_dept"].clone(),
            v["operator_phone"].clone(),
        ),
        (
            json!("カクウウンユ"),
            json!("0000001"),
            json!("0000000000001"),
            json!("代表取締役"),
            json!("架空課"),
            json!(null),
        )
    );
    assert_eq!(
        v["partners"],
        json!([{ "name": "架空物流株式会社", "address": "架空県架空市2-2", "representative": null, "offices": [
            { "name": "架空物流 本社営業所", "address": null }
        ] }])
    );
    assert_eq!(call_as(&app, "admin", "GET", t, c, None).await.json(), v);

    // 置き換え: 省略した欄は null / 空に戻る
    let again = call_as(
        &app,
        "admin",
        "PUT",
        t,
        c,
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
    assert_eq!(again["partners"], json!([]));

    // 別のテナントからは見えない
    assert_eq!(
        call_as(&app, "admin", "GET", other, other_c, None)
            .await
            .json()["device_name"],
        "一歩庵点呼 v1"
    );
    let rows: i64 = admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.tenko_company_filing_settings WHERE tenant_id = $1",
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
            "UPDATE alc_api.tenko_company_filing_settings SET offices = '[1]'::jsonb WHERE tenant_id = $1",
            &[&t],
        )
        .await
        .unwrap();
    assert_eq!(
        call_as(&app, "admin", "GET", t, c, None).await.json()["offices"],
        json!([])
    );
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn filing_settings_db_failures() {
    let t = tenant().await;
    let c = company(t, "架空運輸", false).await;

    // 書けないロール → 保存は 500
    let app = build(client_as(READ_ONLY_ROLE).await);
    assert_eq!(call_as(&app, "admin", "GET", t, c, None).await.status, 200);
    assert_eq!(
        call_as(&app, "admin", "PUT", t, c, Some(sample()))
            .await
            .status,
        500
    );
    assert_eq!(app.logs().len(), 1);
    assert!(app.logs()[0].starts_with("tenko: filing settings put failed: db ("));
    assert_logs_clean(&app, &[t.to_string()]);

    // 切れた接続 → 読みも 500
    let app = build(broken_client().await);
    assert_eq!(call_as(&app, "admin", "GET", t, c, None).await.status, 500);
    assert!(app.logs()[0].starts_with("tenko: filing settings get failed: db ("));
}

#[tokio::test]
async fn filing_settings_are_per_company() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let ca = company(t, "架空A運輸", true).await;
    let cb = company(t, "架空B物流", false).await;
    let foreign = company(other, "架空C", false).await;

    // company_id が無い・空・UUID でない → 400 (GET も PUT も)
    for query in ["", "company_id=", "company_id=x", "x=1"] {
        for (method, body) in [("GET", None), ("PUT", Some(sample()))] {
            let got = call_query(&app, "admin", method, t, query, body).await;
            assert_eq!(
                (got.status, got.json()),
                (400, json!({ "error": "company_required" }))
            );
        }
    }
    // 権限は事業者より先に検める
    let got = call_query(&app, "viewer", "GET", t, "", None).await;
    assert_eq!(got.status, 403);

    // テナントに無い事業者 (別のテナントの事業者も) は 404。PUT は何も書かない
    for id in [Uuid::new_v4(), foreign] {
        let got = call_as(&app, "admin", "GET", t, id, None).await;
        assert_eq!((got.status, got.body.len()), (404, 0));
        let got = call_as(&app, "admin", "PUT", t, id, Some(sample())).await;
        assert_eq!((got.status, got.body.len()), (404, 0));
    }
    let rows = |tenant: Uuid| async move {
        admin()
            .await
            .query_one(
                "SELECT count(*) FROM alc_api.tenko_company_filing_settings WHERE tenant_id = $1",
                &[&tenant],
            )
            .await
            .unwrap()
            .get::<_, i64>(0)
    };
    assert_eq!((rows(t).await, rows(other).await), (0, 0));

    // 旧い表 (テナントに 1 行) はもう読まない
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.tenko_filing_settings (tenant_id, device_name) VALUES ($1, '旧い機器')",
            &[&t],
        )
        .await
        .unwrap();
    let got = call_as(&app, "admin", "GET", t, ca, None).await;
    assert_eq!(got.json()["device_name"], "一歩庵点呼 v1");

    // 事業者ごとに別の値: 片方の保存は、もう片方に響かない
    let put = |name: &'static str| json!({ "device_name": name, "operator_name": name });
    assert_eq!(
        call_as(&app, "admin", "PUT", t, ca, Some(put("架空機器A")))
            .await
            .status,
        200
    );
    let b = call_as(&app, "admin", "GET", t, cb, None).await.json();
    assert_eq!(
        (b["device_name"].clone(), b["operator_name"].clone()),
        (json!("一歩庵点呼 v1"), json!(null))
    );
    assert_eq!(
        call_as(&app, "admin", "PUT", t, cb, Some(put("架空機器B")))
            .await
            .status,
        200
    );
    // 同じ事業者への 2 回目の保存は上書き (行は増えない)
    assert_eq!(
        call_as(&app, "admin", "PUT", t, ca, Some(put("架空機器A2")))
            .await
            .status,
        200
    );
    let a = call_as(&app, "admin", "GET", t, ca, None).await.json();
    let b = call_as(&app, "admin", "GET", t, cb, None).await.json();
    assert_eq!(
        (a["device_name"].clone(), b["device_name"].clone()),
        (json!("架空機器A2"), json!("架空機器B"))
    );
    assert_eq!(rows(t).await, 2);
    assert!(app.logs().is_empty());
}
