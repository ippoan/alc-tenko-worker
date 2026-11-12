//! 点呼の受委託 (`/api/tenko/consignments`) を実 DB で通す。値は架空。

mod support;

use serde_json::{json, Value};
use support::{
    admin, app, assert_logs_clean, broken_client, build, call_role, client_as, company, office,
    tenant, Got, READ_ONLY_ROLE,
};
use uuid::Uuid;

const PATH: &str = "/api/tenko/consignments";
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

/// 委託・受託の営業所だけの本文
fn pair(consignor: Uuid, consignee: Uuid) -> Value {
    json!({ "consignor_office_id": consignor, "consignee_office_id": consignee })
}

/// 本文に欄を足す
fn with(mut body: Value, extra: Value) -> Value {
    for (k, v) in extra.as_object().unwrap() {
        body[k] = v.clone();
    }
    body
}

#[tokio::test]
async fn consignments_crud() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let ca = company(t, "架空A運輸", true).await;
    let cb = company(t, "架空B物流", false).await;
    let oa = office(t, "架空A本社", Some(ca)).await;
    let oa2 = office(t, "架空A支店", Some(ca)).await;
    let ob = office(t, "架空B本社", Some(cb)).await;
    let bare = office(t, "架空無所属", None).await;
    let foreign = office(other, "架空X本社", None).await;
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
        let got = call_role(&app, "viewer", method, uri, t, Some(pair(oa, ob))).await;
        assert_eq!(
            (got.status, got.json()),
            (403, json!({ "error": "forbidden" }))
        );
    }

    // 空
    let got = admin_call(&app, "GET", PATH, t, None).await;
    assert_eq!((got.status, got.json()), (200, json!([])));

    // 外れる → 400 (保存しない)
    let long = "あ".repeat(101);
    for (body, word) in [
        (None, "invalid_body"),
        (Some(json!({ "consignor_office_id": "x" })), "invalid_body"),
        (
            Some(with(pair(oa, ob), json!({ "valid_from": "2099-13-01" }))),
            "invalid_body",
        ),
        (Some(pair(foreign, ob)), "invalid_office"),
        (Some(pair(oa, foreign)), "invalid_office"),
        (Some(pair(missing, ob)), "invalid_office"),
        (Some(pair(bare, ob)), "office_without_company"),
        (Some(pair(oa, bare)), "office_without_company"),
        (Some(pair(oa, oa2)), "same_company"),
        (Some(pair(oa, oa)), "same_company"),
        (
            Some(with(pair(oa, ob), json!({ "arrive_id": "x" }))),
            "invalid_arrive_id",
        ),
        (
            Some(with(pair(oa, ob), json!({ "arrive_id": "" }))),
            "invalid_arrive_id",
        ),
        (
            Some(with(pair(oa, ob), json!({ "valid_from": "2099-01-01" }))),
            "invalid_period",
        ),
        (
            Some(with(pair(oa, ob), json!({ "valid_to": "2099-01-01" }))),
            "invalid_period",
        ),
        (
            Some(with(
                pair(oa, ob),
                json!({ "valid_from": "2099-01-02", "valid_to": "2099-01-01" }),
            )),
            "invalid_period",
        ),
        (
            Some(with(
                pair(oa, ob),
                json!({ "valid_from": "2099-01-01", "valid_to": "2104-01-01" }),
            )),
            "invalid_period",
        ),
        (
            Some(with(
                pair(oa, ob),
                json!({ "valid_from": "2096-02-29", "valid_to": "2101-02-28" }),
            )),
            "invalid_period",
        ),
        (
            Some(with(pair(oa, ob), json!({ "permit_no": long }))),
            "too_long",
        ),
    ] {
        let got = admin_call(&app, "POST", PATH, t, body).await;
        assert_eq!((got.status, got.json()), (400, json!({ "error": word })));
    }
    assert_eq!(
        admin_call(&app, "GET", PATH, t, None).await.json(),
        json!([])
    );

    // 作る: 欄を省略すれば null
    let got = admin_call(&app, "POST", PATH, t, Some(pair(oa, ob))).await;
    assert_eq!(got.status, 201);
    let first = got.json();
    let first_id = first["id"].as_str().unwrap().to_owned();
    assert_eq!(
        first,
        json!({
            "id": first_id, "consignor_office_id": oa, "consignee_office_id": ob,
            "arrive_id": null, "permit_no": null, "permit_date": null, "valid_from": null, "valid_to": null
        })
    );
    // 作る: 全部の欄 (許可番号は空白を落とす。期間は 5 年に 1 日足りないところまで、閏日の起点も通る)
    let got = admin_call(
        &app,
        "POST",
        PATH,
        t,
        Some(with(
            pair(ob, oa2),
            json!({
                "arrive_id": ARRIVE, "permit_no": "  許可-0001 ", "permit_date": "2099-01-05",
                "valid_from": "2099-01-01", "valid_to": "2103-12-31"
            }),
        )),
    )
    .await;
    assert_eq!(got.status, 201);
    let second = got.json();
    let second_id = second["id"].as_str().unwrap().to_owned();
    assert_eq!(
        second,
        json!({
            "id": second_id, "consignor_office_id": ob, "consignee_office_id": oa2,
            "arrive_id": ARRIVE, "permit_no": "許可-0001", "permit_date": "2099-01-05",
            "valid_from": "2099-01-01", "valid_to": "2103-12-31"
        })
    );
    let got = admin_call(
        &app,
        "POST",
        PATH,
        t,
        Some(with(
            pair(oa2, ob),
            json!({ "valid_from": "2096-02-29", "valid_to": "2101-02-27" }),
        )),
    )
    .await;
    assert_eq!(got.status, 201);
    let third_id = got.json()["id"].as_str().unwrap().to_owned();

    // 一覧は作った順
    let list = admin_call(&app, "GET", PATH, t, None).await.json();
    let listed: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        listed,
        [first_id.as_str(), second_id.as_str(), third_id.as_str()]
    );

    // 置き換え: 省略した欄は null に戻る。空白だけの許可番号も null
    let got = admin_call(
        &app,
        "PUT",
        &format!("{PATH}/{first_id}"),
        t,
        Some(with(
            pair(ob, oa),
            json!({ "arrive_id": ARRIVE, "permit_no": "  ", "permit_date": "2099-02-01" }),
        )),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(
        got.json(),
        json!({
            "id": first_id, "consignor_office_id": ob, "consignee_office_id": oa,
            "arrive_id": ARRIVE, "permit_no": null, "permit_date": "2099-02-01", "valid_from": null, "valid_to": null
        })
    );
    let got = admin_call(
        &app,
        "PUT",
        &format!("{PATH}/{first_id}"),
        t,
        Some(pair(oa, ob)),
    )
    .await;
    assert_eq!(got.json()["arrive_id"], json!(null));

    // 更新の外れ: 営業所がテナントに無い (何も変えない)・無い id (本文が正しければ 404)
    let got = admin_call(
        &app,
        "PUT",
        &format!("{PATH}/{first_id}"),
        t,
        Some(pair(foreign, ob)),
    )
    .await;
    assert_eq!(
        (got.status, got.json()),
        (400, json!({ "error": "invalid_office" }))
    );
    let got = admin_call(&app, "PUT", &one, t, Some(pair(oa, ob))).await;
    assert_eq!((got.status, got.body.len()), (404, 0));
    let list = admin_call(&app, "GET", PATH, t, None).await.json();
    assert_eq!(
        (
            list[0]["consignor_office_id"].clone(),
            list[0]["consignee_office_id"].clone()
        ),
        (json!(oa), json!(ob))
    );

    // 別のテナントからは見えない・消せない
    assert_eq!(
        admin_call(&app, "GET", PATH, other, None).await.json(),
        json!([])
    );
    let got = admin_call(&app, "DELETE", &format!("{PATH}/{first_id}"), other, None).await;
    assert_eq!(got.status, 404);

    // 消す
    let got = admin_call(&app, "DELETE", &format!("{PATH}/{first_id}"), t, None).await;
    assert_eq!((got.status, got.body.len()), (204, 0));
    let got = admin_call(&app, "DELETE", &format!("{PATH}/{first_id}"), t, None).await;
    assert_eq!(got.status, 404);

    // 営業所を消すと、その受委託も消える (表の外部キー)
    admin()
        .await
        .execute("DELETE FROM alc_api.tenko_offices WHERE id = $1", &[&ob])
        .await
        .unwrap();
    assert_eq!(
        admin_call(&app, "GET", PATH, t, None).await.json(),
        json!([])
    );
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn consignments_db_failures() {
    let t = tenant().await;
    let ca = company(t, "架空A運輸", true).await;
    let cb = company(t, "架空B物流", false).await;
    let oa = office(t, "架空A本社", Some(ca)).await;
    let ob = office(t, "架空B本社", Some(cb)).await;
    let c: Uuid = admin()
        .await
        .query_one(
            "INSERT INTO alc_api.tenko_consignments (tenant_id, consignor_office_id, consignee_office_id) \
             VALUES ($1, $2, $3) RETURNING id",
            &[&t, &oa, &ob],
        )
        .await
        .unwrap()
        .get(0);
    let one = format!("{PATH}/{c}");

    // 書けないロール → 一覧は読めて、保存・更新・削除は 500
    let app = build(client_as(READ_ONLY_ROLE).await);
    assert_eq!(admin_call(&app, "GET", PATH, t, None).await.status, 200);
    let body = Some(pair(oa, ob));
    assert_eq!(
        admin_call(&app, "POST", PATH, t, body.clone()).await.status,
        500
    );
    assert_eq!(admin_call(&app, "PUT", &one, t, body).await.status, 500);
    assert_eq!(admin_call(&app, "DELETE", &one, t, None).await.status, 500);
    let logs = app.logs();
    assert_eq!(logs.len(), 3);
    assert!(logs[0].starts_with("tenko: consignments save failed: db ("));
    assert!(logs[1].starts_with("tenko: consignments save failed: db ("));
    assert!(logs[2].starts_with("tenko: consignments delete failed: db ("));
    assert_logs_clean(&app, &[t.to_string(), c.to_string(), oa.to_string()]);

    // 切れた接続 → 一覧も 500
    let app = build(broken_client().await);
    assert_eq!(admin_call(&app, "GET", PATH, t, None).await.status, 500);
    assert!(app.logs()[0].starts_with("tenko: consignments list failed: db ("));
}
