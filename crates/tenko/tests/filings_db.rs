//! 点呼の届出の申請の下書き (`/api/tenko/filings`) を実 DB で通す。値は架空。

mod support;

use serde_json::{json, Value};
use support::{
    admin, app, assert_logs_clean, broken_client, build, call, call_role, client_as, company,
    office, tenant, Got, READ_ONLY_ROLE,
};
use uuid::Uuid;

const PATH: &str = "/api/tenko/filings";
/// 架空の到達番号
const ARRIVE: &str = "20991231235959001e";
const ARRIVE2: &str = "2099123123595900";

async fn admin_call(
    app: &support::App,
    method: &str,
    uri: &str,
    tenant: Uuid,
    body: Option<Value>,
) -> Got {
    call_role(app, "admin", method, uri, tenant, body).await
}

/// 営業所に住所を入れる
async fn set_address(office: Uuid, address: &str) {
    admin()
        .await
        .execute(
            "UPDATE alc_api.tenko_offices SET address = $2 WHERE id = $1",
            &[&office, &address],
        )
        .await
        .unwrap();
}

/// 控えの振り分け (到達番号, 事業者) を到達番号・事業者の順で
async fn archive_companies(tenant: Uuid) -> Vec<(String, Uuid)> {
    admin()
        .await
        .query(
            "SELECT arrive_id, company_id FROM alc_api.tenko_filing_archive_companies \
             WHERE tenant_id = $1 ORDER BY arrive_id, company_id",
            &[&tenant],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

fn body(company: Uuid, offices: Value) -> Value {
    json!({
        "company_id": company,
        "kind": "remote",
        "apply_date": "2026-11-13",
        "offices": offices,
    })
}

#[tokio::test]
async fn filings_crud() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let ca = company(t, "架空A運輸", true).await;
    let cb = company(t, "架空B物流", false).await;
    let foreign_c = company(other, "架空X", false).await;
    let oa = office(t, "架空A 本社営業所", Some(ca)).await;
    set_address(oa, "架空県架空市1-1").await;
    let ob = office(t, "架空B 営業所", Some(cb)).await;
    let foreign_o = office(other, "架空X 営業所", Some(foreign_c)).await;
    let missing = format!("{PATH}/{}", Uuid::new_v4());

    // admin 以外 (役の無い要求・admin でない役) は 4 口とも 403
    let got = call(
        &app,
        "GET",
        &format!("{PATH}?company_id={ca}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(
        (got.status, got.json()),
        (403, json!({ "error": "forbidden" }))
    );
    for (method, uri) in [
        ("GET", format!("{PATH}?company_id={ca}")),
        ("POST", PATH.to_owned()),
        ("PATCH", missing.clone()),
        ("DELETE", missing.clone()),
    ] {
        let got = call_role(&app, "viewer", method, &uri, t, Some(json!({}))).await;
        assert_eq!(
            (got.status, got.json()),
            (403, json!({ "error": "forbidden" }))
        );
    }

    // 一覧は company_id が要る
    for q in ["", "?company_id=", "?company_id=x"] {
        let got = admin_call(&app, "GET", &format!("{PATH}{q}"), t, None).await;
        assert_eq!(
            (got.status, got.json()),
            (400, json!({ "error": "company_required" }))
        );
    }
    let got = admin_call(&app, "GET", &format!("{PATH}?company_id={ca}"), t, None).await;
    assert_eq!((got.status, got.json()), (200, json!([])));

    // 本文・欄が外れる → 400 (保存しない)
    let one = json!([{ "office_id": oa, "role": 1 }]);
    let too_many: Vec<Value> = (0..21)
        .map(|_| json!({ "office_id": Uuid::new_v4(), "role": 1 }))
        .collect();
    for (b, word) in [
        (None, "invalid_body"),
        (Some(json!({ "company_id": "x" })), "invalid_body"),
        (
            Some(json!({ "kind": "remote", "apply_date": "2026-11-13", "offices": one })),
            "company_required",
        ),
        (
            Some(
                json!({ "company_id": ca, "kind": "x", "apply_date": "2026-11-13", "offices": one }),
            ),
            "invalid_kind",
        ),
        (
            Some(
                json!({ "company_id": ca, "kind": "auto", "apply_date": "2026/11/13", "offices": one }),
            ),
            "invalid_date",
        ),
        (
            Some(
                json!({ "company_id": ca, "kind": "auto", "apply_date": "2026-11-13", "start_date": "x", "offices": one }),
            ),
            "invalid_date",
        ),
        (Some(body(ca, json!([]))), "invalid_office"),
        (
            Some(body(ca, json!([{ "office_id": oa, "role": 4 }]))),
            "invalid_office",
        ),
        (Some(body(ca, json!(too_many))), "too_many_offices"),
        (
            Some(
                json!({ "company_id": ca, "kind": "auto", "apply_date": "2026-11-13", "offices": one, "created_by_name": "あ".repeat(501) }),
            ),
            "too_long",
        ),
        // テナントに無い事業者 (別のテナントの事業者も)
        (Some(body(Uuid::new_v4(), one.clone())), "invalid_company"),
        (Some(body(foreign_c, one.clone())), "invalid_company"),
        // 別のテナントの営業所・無い営業所
        (
            Some(body(
                ca,
                json!([{ "office_id": oa, "role": 1 }, { "office_id": foreign_o, "role": 2 }]),
            )),
            "invalid_office",
        ),
        (
            Some(body(
                ca,
                json!([{ "office_id": Uuid::new_v4(), "role": 1 }]),
            )),
            "invalid_office",
        ),
    ] {
        let got = admin_call(&app, "POST", PATH, t, b).await;
        assert_eq!((got.status, got.json()), (400, json!({ "error": word })));
    }
    let rows: i64 = admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.tenko_filings WHERE tenant_id = $1",
            &[&t],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(rows, 0);

    // 作る: 営業所 (他社の営業所も) の名称・住所を写す。作った人の名前は空白を落とす
    let got = admin_call(
        &app,
        "POST",
        PATH,
        t,
        Some(json!({
            "company_id": ca,
            "kind": "auto",
            "apply_date": "2026-11-13",
            "start_date": "2026-12-01",
            "offices": [{ "office_id": oa, "role": 1 }, { "office_id": ob, "role": 2 }],
            "created_by_name": " 架空 太郎 ",
        })),
    )
    .await;
    assert_eq!(got.status, 201);
    let f1 = got.json();
    assert_eq!(
        (
            f1["company_id"].clone(),
            f1["kind"].clone(),
            f1["apply_date"].clone(),
            f1["start_date"].clone(),
            f1["arrive_id"].clone(),
            f1["created_by_name"].clone(),
        ),
        (
            json!(ca),
            json!("auto"),
            json!("2026-11-13"),
            json!("2026-12-01"),
            json!(null),
            json!("架空 太郎"),
        )
    );
    assert!(f1["created_by"].as_str().unwrap().parse::<Uuid>().is_ok());
    assert_eq!(
        f1["offices"],
        json!([
            { "office_id": oa, "role": 1, "name": "架空A 本社営業所", "address": "架空県架空市1-1" },
            { "office_id": ob, "role": 2, "name": "架空B 営業所", "address": null },
        ])
    );
    let f1_id = f1["id"].as_str().unwrap().to_owned();

    // 写しなので、営業所が後で変わっても申請は変わらない
    set_address(oa, "架空県架空市9-9").await;

    // 2 件目 (start_date は省略・null、作った人の名前は無し)
    let got = admin_call(
        &app,
        "POST",
        PATH,
        t,
        Some(json!({
            "company_id": ca, "kind": "remote", "apply_date": "2026-11-14", "start_date": null,
            "offices": [{ "office_id": oa, "role": 3 }],
        })),
    )
    .await;
    assert_eq!(got.status, 201);
    let f2 = got.json();
    assert_eq!(
        (f2["start_date"].clone(), f2["created_by_name"].clone()),
        (json!(null), json!(null))
    );
    let f2_id = f2["id"].as_str().unwrap().to_owned();

    // 一覧: 新しい順。ほかの事業者・ほかのテナントからは見えない
    let list = admin_call(&app, "GET", &format!("{PATH}?company_id={ca}"), t, None)
        .await
        .json();
    assert_eq!(list, json!([f2, f1]));
    assert_eq!(list[1]["offices"][0]["address"], json!("架空県架空市1-1"));
    let got = admin_call(&app, "GET", &format!("{PATH}?company_id={cb}"), t, None).await;
    assert_eq!(got.json(), json!([]));
    let got = admin_call(&app, "GET", &format!("{PATH}?company_id={ca}"), other, None).await;
    assert_eq!(got.json(), json!([]));

    // 到達番号を付ける: 本文・形が外れる → 400、無い行・ほかのテナント → 404
    let f1_path = format!("{PATH}/{f1_id}");
    let f2_path = format!("{PATH}/{f2_id}");
    let got = admin_call(&app, "PATCH", &f1_path, t, None).await;
    assert_eq!(
        (got.status, got.json()),
        (400, json!({ "error": "invalid_body" }))
    );
    for b in [
        json!({}),
        json!({ "arrive_id": null }),
        json!({ "arrive_id": "123" }),
        json!({ "arrive_id": "2099123123595900AB" }),
    ] {
        let got = admin_call(&app, "PATCH", &f1_path, t, Some(b)).await;
        assert_eq!(
            (got.status, got.json()),
            (400, json!({ "error": "invalid_arrive_id" }))
        );
    }
    for (uri, tenant) in [(missing.as_str(), t), (f1_path.as_str(), other)] {
        let got = admin_call(
            &app,
            "PATCH",
            uri,
            tenant,
            Some(json!({ "arrive_id": ARRIVE })),
        )
        .await;
        assert_eq!(
            (got.status, got.json()),
            (404, json!({ "error": "not_found" }))
        );
    }

    // 控えに既に別の事業者が振り分けてあっても消さない (足すだけ)
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.tenko_filing_archive_companies (tenant_id, arrive_id, company_id) VALUES ($1, $2, $3)",
            &[&t, &ARRIVE, &cb],
        )
        .await
        .unwrap();
    let got = admin_call(
        &app,
        "PATCH",
        &f1_path,
        t,
        Some(json!({ "arrive_id": ARRIVE })),
    )
    .await;
    assert_eq!(got.status, 200);
    let v = got.json();
    assert_eq!(v["arrive_id"], json!(ARRIVE));
    assert_ne!(v["updated_at"], f1["updated_at"]);
    assert_eq!(v["offices"], f1["offices"]);
    let mut want = vec![(ARRIVE.to_owned(), ca), (ARRIVE.to_owned(), cb)];
    want.sort();
    assert_eq!(archive_companies(t).await, want);
    // 同じ到達番号をもう一度付けても振り分けは増えない
    let got = admin_call(
        &app,
        "PATCH",
        &f1_path,
        t,
        Some(json!({ "arrive_id": ARRIVE })),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(archive_companies(t).await, want);

    // ほかの申請に付いた到達番号は 409 (何も書かない)
    let got = admin_call(
        &app,
        "PATCH",
        &f2_path,
        t,
        Some(json!({ "arrive_id": ARRIVE })),
    )
    .await;
    assert_eq!(
        (got.status, got.json()),
        (409, json!({ "error": "duplicate_arrive_id" }))
    );
    assert_eq!(archive_companies(t).await, want);
    // 別のテナントなら同じ到達番号でも付けられる
    let fo = admin_call(
        &app,
        "POST",
        PATH,
        other,
        Some(body(
            foreign_c,
            json!([{ "office_id": foreign_o, "role": 1 }]),
        )),
    )
    .await
    .json();
    let got = admin_call(
        &app,
        "PATCH",
        &format!("{PATH}/{}", fo["id"].as_str().unwrap()),
        other,
        Some(json!({ "arrive_id": ARRIVE })),
    )
    .await;
    assert_eq!(got.status, 200);

    // 消す: 到達番号付きは 409、無い行・ほかのテナントは 404、下書きは 204
    let got = admin_call(&app, "DELETE", &f1_path, t, None).await;
    assert_eq!(
        (got.status, got.json()),
        (409, json!({ "error": "already_submitted" }))
    );
    for (uri, tenant) in [(missing.as_str(), t), (f2_path.as_str(), other)] {
        let got = admin_call(&app, "DELETE", uri, tenant, None).await;
        assert_eq!(
            (got.status, got.json()),
            (404, json!({ "error": "not_found" }))
        );
    }
    let got = admin_call(&app, "DELETE", &f2_path, t, None).await;
    assert_eq!((got.status, got.body.len()), (204, 0));
    let got = admin_call(&app, "DELETE", &f2_path, t, None).await;
    assert_eq!(got.status, 404);
    let list = admin_call(&app, "GET", &format!("{PATH}?company_id={ca}"), t, None)
        .await
        .json();
    assert_eq!(list.as_array().unwrap().len(), 1);

    // 到達番号を付け替えても前の振り分けは残る (正本は控えの振り分け)
    let got = admin_call(
        &app,
        "PATCH",
        &f1_path,
        t,
        Some(json!({ "arrive_id": ARRIVE2 })),
    )
    .await;
    assert_eq!(got.status, 200);
    let mut want2 = want.clone();
    want2.insert(0, (ARRIVE2.to_owned(), ca));
    assert_eq!(archive_companies(t).await, want2);
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn filings_db_failures() {
    let t = tenant().await;
    let c = company(t, "架空運輸", false).await;
    let o = office(t, "架空営業所", Some(c)).await;
    let id: Uuid = admin()
        .await
        .query_one(
            "INSERT INTO alc_api.tenko_filings (tenant_id, company_id, kind, apply_date) \
             VALUES ($1, $2, 'remote', '2026-11-13') RETURNING id",
            &[&t, &c],
        )
        .await
        .unwrap()
        .get(0);
    let one = format!("{PATH}/{id}");

    // 書けないロール → 一覧は読めて、作成・到達番号・削除は 500
    let app = build(client_as(READ_ONLY_ROLE).await);
    let got = admin_call(&app, "GET", &format!("{PATH}?company_id={c}"), t, None).await;
    assert_eq!(got.status, 200);
    assert_eq!(
        admin_call(
            &app,
            "POST",
            PATH,
            t,
            Some(body(c, json!([{ "office_id": o, "role": 1 }])))
        )
        .await
        .status,
        500
    );
    assert_eq!(
        admin_call(&app, "PATCH", &one, t, Some(json!({ "arrive_id": ARRIVE })))
            .await
            .status,
        500
    );
    assert_eq!(admin_call(&app, "DELETE", &one, t, None).await.status, 500);
    let logs = app.logs();
    assert_eq!(logs.len(), 3);
    assert!(logs[0].starts_with("tenko: filings create failed: db ("));
    assert!(logs[1].starts_with("tenko: filings arrive failed: db ("));
    assert!(logs[2].starts_with("tenko: filings delete failed: db ("));
    assert_logs_clean(
        &app,
        &[
            t.to_string(),
            c.to_string(),
            id.to_string(),
            ARRIVE.to_owned(),
        ],
    );

    // 切れた接続 → 一覧も 500
    let app = build(broken_client().await);
    let got = admin_call(&app, "GET", &format!("{PATH}?company_id={c}"), t, None).await;
    assert_eq!(got.status, 500);
    assert!(app.logs()[0].starts_with("tenko: filings list failed: db ("));
}
