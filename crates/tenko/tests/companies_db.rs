//! 点呼の事業者 (`/api/tenko/companies`) を実 DB で通す。値は架空。

mod support;

use serde_json::{json, Value};
use support::{
    admin, app, assert_logs_clean, broken_client, build, call_role, client_as, company, office,
    tenant, Got, READ_ONLY_ROLE,
};
use uuid::Uuid;

const PATH: &str = "/api/tenko/companies";
/// 架空の到達番号
const ARRIVE: &str = "20991231235959001e";

async fn admin_call(
    app: &support::App,
    method: &str,
    uri: &str,
    tenant: Uuid,
    body: Option<Value>,
) -> Got {
    call_role(app, "admin", method, uri, tenant, body).await
}

fn ids(v: &Value) -> Vec<String> {
    v["office_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i.as_str().unwrap().to_owned())
        .collect()
}

async fn office_company(office: Uuid) -> Option<Uuid> {
    admin()
        .await
        .query_one(
            "SELECT company_id FROM alc_api.tenko_offices WHERE id = $1",
            &[&office],
        )
        .await
        .unwrap()
        .get(0)
}

async fn count(sql: &str, tenant: Uuid) -> i64 {
    admin()
        .await
        .query_one(sql, &[&tenant])
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn companies_crud() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let o1 = office(t, "架空営業所A", None).await;
    let o2 = office(t, "架空営業所B", None).await;
    let o3 = office(t, "架空営業所C", None).await;
    let foreign = office(other, "架空営業所X", None).await;
    let missing = Uuid::new_v4();
    let one = format!("{PATH}/{missing}");

    // admin 以外 (役の無い要求・admin でない役) は 4 口とも 403
    let got = support::call(&app, "GET", PATH, Some(t), None).await;
    assert_eq!(
        (got.status, got.json()),
        (403, json!({ "error": "forbidden" }))
    );
    for (method, uri) in [
        ("GET", PATH),
        ("POST", PATH),
        ("PUT", one.as_str()),
        ("DELETE", one.as_str()),
    ] {
        let got = call_role(&app, "viewer", method, uri, t, Some(json!({ "name": "a" }))).await;
        assert_eq!(
            (got.status, got.json()),
            (403, json!({ "error": "forbidden" }))
        );
    }

    // 空
    let got = admin_call(&app, "GET", PATH, t, None).await;
    assert_eq!((got.status, got.json()), (200, json!([])));

    // 本文・欄が外れる → 400 (保存しない)
    let too_many: Vec<Uuid> = (0..1001).map(|_| Uuid::new_v4()).collect();
    for (body, word) in [
        (None, "invalid_body"),
        (
            Some(json!({ "name": "a", "office_ids": ["x"] })),
            "invalid_body",
        ),
        (Some(json!({ "name": " 　" })), "invalid_name"),
        (Some(json!({ "name": "あ".repeat(501) })), "too_long"),
        (
            Some(json!({ "name": "a", "office_ids": too_many })),
            "too_many_ids",
        ),
        (
            Some(json!({ "name": "a", "office_ids": [foreign] })),
            "invalid_office",
        ),
        (
            Some(json!({ "name": "a", "office_ids": [o1, Uuid::new_v4()] })),
            "invalid_office",
        ),
    ] {
        let got = admin_call(&app, "POST", PATH, t, body).await;
        assert_eq!((got.status, got.json()), (400, json!({ "error": word })));
    }
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.tenko_companies WHERE tenant_id = $1",
            t
        )
        .await,
        0
    );
    // 名称は 500 文字ちょうどまで
    let got = admin_call(
        &app,
        "POST",
        PATH,
        t,
        Some(json!({ "name": "あ".repeat(500) })),
    )
    .await;
    assert_eq!(got.status, 201);
    let long = got.json()["id"].as_str().unwrap().to_owned();
    assert_eq!(
        admin_call(&app, "DELETE", &format!("{PATH}/{long}"), t, None)
            .await
            .status,
        204
    );

    // 作る (空白を落とす・営業所の重複はまとめる・営業所は振り分けられる)
    let got = admin_call(
        &app,
        "POST",
        PATH,
        t,
        Some(json!({ "name": " 架空A運輸 ", "is_self": true, "office_ids": [o1, o2, o1] })),
    )
    .await;
    assert_eq!(got.status, 201);
    let c1 = got.json();
    assert_eq!(
        (c1["name"].clone(), c1["is_self"].clone()),
        (json!("架空A運輸"), json!(true))
    );
    assert_eq!(ids(&c1), [o1.to_string(), o2.to_string()]);
    let c1_id: Uuid = c1["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(office_company(o1).await, Some(c1_id));

    // 同じ名称は 400
    for name in ["架空A運輸", "  架空A運輸"] {
        let got = admin_call(&app, "POST", PATH, t, Some(json!({ "name": name }))).await;
        assert_eq!(
            (got.status, got.json()),
            (400, json!({ "error": "duplicate_name" }))
        );
    }

    // 2 つ目: 営業所 o2 は別の事業者から移る。is_self は省略で false
    let got = admin_call(
        &app,
        "POST",
        PATH,
        t,
        Some(json!({ "name": "架空B物流", "office_ids": [o2] })),
    )
    .await;
    assert_eq!(got.status, 201);
    let c2 = got.json();
    let c2_id: Uuid = c2["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(c2["is_self"], json!(false));
    let list = admin_call(&app, "GET", PATH, t, None).await.json();
    assert_eq!(list.as_array().unwrap().len(), 2);
    assert_eq!(
        (list[0]["id"].clone(), ids(&list[0])),
        (json!(c1_id), vec![o1.to_string()])
    );
    assert_eq!(
        (list[1]["id"].clone(), ids(&list[1])),
        (json!(c2_id), vec![o2.to_string()])
    );

    // is_self を移す: 先に元の事業者の印が外れる
    let got = admin_call(
        &app,
        "PUT",
        &format!("{PATH}/{c2_id}"),
        t,
        Some(json!({ "name": "架空B物流", "is_self": true, "office_ids": [o2, o3] })),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(
        (got.json()["is_self"].clone(), ids(&got.json())),
        (json!(true), vec![o2.to_string(), o3.to_string()])
    );
    let list = admin_call(&app, "GET", PATH, t, None).await.json();
    assert_eq!(
        (list[0]["is_self"].clone(), list[1]["is_self"].clone()),
        (json!(false), json!(true))
    );

    // 置き換え: 省略した is_self は false、営業所 o3 は移り、一覧に無い o1 は外れる
    let got = admin_call(
        &app,
        "PUT",
        &format!("{PATH}/{c1_id}"),
        t,
        Some(json!({ "name": "架空A運輸", "office_ids": [o3] })),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(ids(&got.json()), vec![o3.to_string()]);
    assert_eq!(
        (
            office_company(o1).await,
            office_company(o2).await,
            office_company(o3).await
        ),
        (None, Some(c2_id), Some(c1_id))
    );

    // 更新の外れ: 同じ名称・無い id (本文が正しくても 404)・営業所がテナントに無い・別のテナントの事業者
    let got = admin_call(
        &app,
        "PUT",
        &format!("{PATH}/{c2_id}"),
        t,
        Some(json!({ "name": "架空A運輸" })),
    )
    .await;
    assert_eq!(
        (got.status, got.json()),
        (400, json!({ "error": "duplicate_name" }))
    );
    let got = admin_call(&app, "PUT", &one, t, Some(json!({ "name": "架空C" }))).await;
    assert_eq!((got.status, got.body.len()), (404, 0));
    let got = admin_call(
        &app,
        "PUT",
        &format!("{PATH}/{c1_id}"),
        t,
        Some(json!({ "name": "架空A運輸", "office_ids": [foreign] })),
    )
    .await;
    assert_eq!(
        (got.status, got.json()),
        (400, json!({ "error": "invalid_office" }))
    );
    let got = admin_call(
        &app,
        "PUT",
        &format!("{PATH}/{c1_id}"),
        other,
        Some(json!({ "name": "架空A運輸" })),
    )
    .await;
    assert_eq!(got.status, 404);
    // 失敗した更新は何も変えない
    assert_eq!(office_company(o3).await, Some(c1_id));

    // 別のテナントからは見えない・消せない
    let got = admin_call(&app, "GET", PATH, other, None).await;
    assert_eq!(got.json(), json!([]));
    let got = admin_call(&app, "DELETE", &format!("{PATH}/{c2_id}"), other, None).await;
    assert_eq!(got.status, 404);

    // 消す: 営業所は外れ、書き込む値・控えの振り分けは消える
    let admin_db = admin().await;
    admin_db
        .execute(
            "INSERT INTO alc_api.tenko_company_filing_settings (tenant_id, company_id) VALUES ($1, $2)",
            &[&t, &c2_id],
        )
        .await
        .unwrap();
    admin_db
        .execute(
            "INSERT INTO alc_api.tenko_filing_archive_companies (tenant_id, arrive_id, company_id) VALUES ($1, $2, $3)",
            &[&t, &ARRIVE, &c2_id],
        )
        .await
        .unwrap();
    let got = admin_call(&app, "DELETE", &format!("{PATH}/{c2_id}"), t, None).await;
    assert_eq!((got.status, got.body.len()), (204, 0));
    let got = admin_call(&app, "DELETE", &format!("{PATH}/{c2_id}"), t, None).await;
    assert_eq!(got.status, 404);
    assert_eq!(office_company(o2).await, None);
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.tenko_company_filing_settings WHERE tenant_id = $1",
            t
        )
        .await,
        0
    );
    assert_eq!(
        count(
            "SELECT count(*) FROM alc_api.tenko_filing_archive_companies WHERE tenant_id = $1",
            t
        )
        .await,
        0
    );
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn companies_limit_is_100() {
    let app = app().await;
    let t = tenant().await;
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.tenko_companies (tenant_id, name) SELECT $1, 'c' || g FROM generate_series(1, 100) g",
            &[&t],
        )
        .await
        .unwrap();
    let got = admin_call(&app, "POST", PATH, t, Some(json!({ "name": "架空101" }))).await;
    assert_eq!(
        (got.status, got.json()),
        (400, json!({ "error": "too_many_companies" }))
    );
    // 別のテナントは数えない
    let other = tenant().await;
    let got = admin_call(
        &app,
        "POST",
        PATH,
        other,
        Some(json!({ "name": "架空101" })),
    )
    .await;
    assert_eq!(got.status, 201);
}

#[tokio::test]
async fn companies_db_failures() {
    let t = tenant().await;
    let c = company(t, "架空運輸", false).await;
    let one = format!("{PATH}/{c}");

    // 書けないロール → 一覧は読めて、保存・更新・削除は 500
    let app = build(client_as(READ_ONLY_ROLE).await);
    assert_eq!(admin_call(&app, "GET", PATH, t, None).await.status, 200);
    let body = Some(json!({ "name": "架空物流" }));
    assert_eq!(
        admin_call(&app, "POST", PATH, t, body.clone()).await.status,
        500
    );
    assert_eq!(admin_call(&app, "PUT", &one, t, body).await.status, 500);
    assert_eq!(admin_call(&app, "DELETE", &one, t, None).await.status, 500);
    let logs = app.logs();
    assert_eq!(logs.len(), 3);
    assert!(logs[0].starts_with("tenko: companies save failed: db ("));
    assert!(logs[1].starts_with("tenko: companies save failed: db ("));
    assert!(logs[2].starts_with("tenko: companies delete failed: db ("));
    assert_logs_clean(&app, &[t.to_string(), c.to_string(), "架空物流".to_owned()]);

    // 切れた接続 → 一覧も 500
    let app = build(broken_client().await);
    assert_eq!(admin_call(&app, "GET", PATH, t, None).await.status, 500);
    assert!(app.logs()[0].starts_with("tenko: companies list failed: db ("));
}
