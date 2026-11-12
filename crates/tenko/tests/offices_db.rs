//! 点呼の営業所 (`/api/tenko/offices`) を実 DB で通す。値は架空。

mod support;

use serde_json::{json, Value};
use support::{
    admin, app, assert_logs_clean, broken_client, build, call_with, client_as, employee, tenant,
    READ_ONLY_ROLE,
};
use uuid::Uuid;

const PATH: &str = "/api/tenko/offices";

fn as_role(role: &str) -> [(&'static str, String); 3] {
    [
        ("X-User-ID", Uuid::new_v4().to_string()),
        ("X-User-Email", "offices-test@example.com".to_owned()),
        ("X-User-Role", role.to_owned()),
    ]
}

/// 役割つきで打つ
async fn call_role(
    app: &support::App,
    role: &str,
    method: &str,
    uri: &str,
    tenant: Uuid,
    body: Option<Value>,
) -> support::Got {
    let h = as_role(role);
    let headers: Vec<(&str, &str)> = h.iter().map(|(k, v)| (*k, v.as_str())).collect();
    call_with(app, method, uri, Some(tenant), &headers, body).await
}

/// admin として打つ (この口は admin だけ)
async fn call(
    app: &support::App,
    method: &str,
    uri: &str,
    tenant: Option<Uuid>,
    body: Option<Value>,
) -> support::Got {
    let h = as_role("admin");
    let headers: Vec<(&str, &str)> = h.iter().map(|(k, v)| (*k, v.as_str())).collect();
    call_with(app, method, uri, tenant, &headers, body).await
}

/// 役割 (employees.role は TEXT[]) を付けた社員
async fn staff(tenant: Uuid, name: &str, roles: &[&str], deleted: bool) -> Uuid {
    let id = employee(tenant, name).await;
    let roles: Vec<String> = roles.iter().map(|r| (*r).to_owned()).collect();
    admin()
        .await
        .execute(
            "UPDATE alc_api.employees SET role = $2, deleted_at = CASE WHEN $3 THEN now() END, \
             code = CASE WHEN $3 THEN NULL ELSE code END WHERE id = $1",
            &[&id, &roles, &deleted],
        )
        .await
        .unwrap();
    id
}

async fn vehicle(tenant: Uuid, number: &str, name: Option<&str>, deleted: bool) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.maintenance_vehicles (tenant_id, registration_number, display_name, deleted_at) \
             VALUES ($1, $2, $3, CASE WHEN $4 THEN now() END) RETURNING id",
            &[&tenant, &number, &name, &deleted],
        )
        .await
        .unwrap()
        .get(0)
}

/// 車両の所属営業所を直接読む
async fn vehicle_office(vehicle: Uuid) -> Option<Uuid> {
    admin()
        .await
        .query_one(
            "SELECT office_id FROM alc_api.maintenance_vehicles WHERE id = $1",
            &[&vehicle],
        )
        .await
        .unwrap()
        .get(0)
}

async fn count(table: &str, tenant: Uuid) -> i64 {
    admin()
        .await
        .query_one(
            &format!("SELECT count(*) FROM alc_api.{table} WHERE tenant_id = $1"),
            &[&tenant],
        )
        .await
        .unwrap()
        .get(0)
}

/// car_inspection に合成の行を 1 件入れる (superuser で。NOT NULL の残りの列は空文字で埋める)
async fn car_inspection(
    tenant: Uuid,
    cert_no: &str,
    car_id: &str,
    base: &str,
    high: &str,
    low: &str,
) {
    let su = admin().await;
    let given: [(&str, String); 10] = [
        ("ElectCertMgNo", cert_no.to_owned()),
        ("CarId", car_id.to_owned()),
        ("EntryNoCarNo", format!("TEST-CAR-NO-{cert_no}")),
        ("GrantdateE", "R".to_owned()),
        ("GrantdateY", "07".to_owned()),
        ("GrantdateM", "01".to_owned()),
        ("GrantdateD", "01".to_owned()),
        ("UseheadqrterChar", base.to_owned()),
        ("UsernameHighLevelChar", high.to_owned()),
        ("UsernameLowLevelChar", low.to_owned()),
    ];
    let rest: Vec<String> = su
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = 'alc_api' AND table_name = 'car_inspection' \
               AND is_nullable = 'NO' AND column_default IS NULL AND data_type = 'text'",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .filter(|c| given.iter().all(|(g, _)| g != c))
        .collect();
    let quote = |s: &str| format!("'{}'", s.replace('\'', "''"));
    let mut cols = vec!["tenant_id".to_owned()];
    let mut vals = vec![quote(&tenant.to_string())];
    for (c, v) in &given {
        cols.push(format!("\"{c}\""));
        vals.push(quote(v));
    }
    for c in &rest {
        cols.push(format!("\"{c}\""));
        vals.push("''".to_owned());
    }
    su.batch_execute(&format!(
        "INSERT INTO alc_api.car_inspection ({}) VALUES ({})",
        cols.join(", "),
        vals.join(", ")
    ))
    .await
    .expect("car_inspection の合成行を入れられない");
}

fn ids(v: &Value) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_owned())
        .collect()
}

fn sorted(mut v: Vec<Uuid>) -> Vec<String> {
    v.sort();
    v.iter().map(Uuid::to_string).collect()
}

#[tokio::test]
async fn offices_crud() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;

    let m1 = staff(t, "架空 運行管理者A", &["driver", "manager"], false).await;
    let m2 = staff(t, "架空 管理者B", &["admin"], false).await;
    let driver = staff(t, "架空 乗務員C", &["driver"], false).await;
    let left = staff(t, "架空 退職した運行管理者", &["manager"], true).await;
    let foreign = staff(other, "架空 他社の運行管理者", &["manager"], false).await;
    let v1 = vehicle(t, "架空 100 あ 1", Some("1号車"), false).await;
    let v2 = vehicle(t, "架空 100 あ 2", None, false).await;
    let v3 = vehicle(t, "架空 100 あ 3", None, false).await;
    let gone = vehicle(t, "架空 100 あ 9", None, true).await;
    let foreign_v = vehicle(other, "架空 200 い 1", None, false).await;

    // admin 以外 (admin でない役割・役割の無い要求) は 4 口とも 403 で、何も書かない
    let one = format!("{PATH}/{}", Uuid::new_v4());
    for (method, path, body) in [
        ("GET", PATH, None),
        ("POST", PATH, Some(json!({ "name": "書かれない" }))),
        ("PUT", one.as_str(), Some(json!({ "name": "書かれない" }))),
        ("DELETE", one.as_str(), None),
    ] {
        for got in [
            call_role(&app, "viewer", method, path, t, body.clone()).await,
            call_with(&app, method, path, Some(t), &[], body.clone()).await,
        ] {
            assert_eq!(
                (got.status, got.json()),
                (403, json!({ "error": "forbidden" }))
            );
        }
    }
    assert_eq!(count("tenko_offices", t).await, 0);

    // 何も無ければ空の営業所と、未削除の車両 (車両番号順)
    let got = call(&app, "GET", PATH, Some(t), None).await;
    assert_eq!(got.status, 200);
    assert_eq!(
        got.json(),
        json!({
            "offices": [],
            "carins_bases": [],
            "vehicles": [
                { "id": v1, "registration_number": "架空 100 あ 1", "display_name": "1号車", "office_id": null },
                { "id": v2, "registration_number": "架空 100 あ 2", "display_name": null, "office_id": null },
                { "id": v3, "registration_number": "架空 100 あ 3", "display_name": null, "office_id": null },
            ]
        })
    );

    // 本文が読めない・欄が外れる → 400 (何も書かない)
    let bad = call(&app, "POST", PATH, Some(t), None).await;
    assert_eq!(
        (bad.status, bad.json()),
        (400, json!({ "error": "invalid_body" }))
    );
    let too_many: Vec<Uuid> = (0..1001).map(|_| Uuid::new_v4()).collect();
    for (body, word) in [
        (json!({ "company_name": "x" }), "invalid_body"),
        (json!({ "name": " \u{3000} " }), "invalid_name"),
        (json!({ "name": "あ".repeat(501) }), "too_long"),
        (
            json!({ "name": "a", "company_name": "あ".repeat(501) }),
            "too_long",
        ),
        (
            json!({ "name": "a", "address": "あ".repeat(501) }),
            "too_long",
        ),
        (
            json!({ "name": "a", "manager_ids": too_many }),
            "too_many_ids",
        ),
        (
            json!({ "name": "a", "vehicle_ids": too_many }),
            "too_many_ids",
        ),
        // 運行管理者でない社員・退職した社員・他のテナントの社員・無い id
        (
            json!({ "name": "a", "manager_ids": [m1, driver] }),
            "invalid_manager",
        ),
        (
            json!({ "name": "a", "manager_ids": [left] }),
            "invalid_manager",
        ),
        (
            json!({ "name": "a", "manager_ids": [foreign] }),
            "invalid_manager",
        ),
        (
            json!({ "name": "a", "manager_ids": [Uuid::new_v4()] }),
            "invalid_manager",
        ),
        // 削除済みの車両・他のテナントの車両・無い id
        (
            json!({ "name": "a", "vehicle_ids": [v1, gone] }),
            "invalid_vehicle",
        ),
        (
            json!({ "name": "a", "vehicle_ids": [foreign_v] }),
            "invalid_vehicle",
        ),
        (
            json!({ "name": "a", "vehicle_ids": [Uuid::new_v4()] }),
            "invalid_vehicle",
        ),
    ] {
        let got = call(&app, "POST", PATH, Some(t), Some(body)).await;
        assert_eq!((got.status, got.json()), (400, json!({ "error": word })));
    }
    assert_eq!(count("tenko_offices", t).await, 0);
    assert_eq!(count("tenko_office_managers", t).await, 0);
    assert_eq!(vehicle_office(v1).await, None);

    // 作る (空白を落とす・空は null・id の重複はまとめる)
    let made = call(
        &app,
        "POST",
        PATH,
        Some(t),
        Some(json!({
            "name": " 架空運輸 本社営業所 ",
            "company_name": "  ",
            "address": " 架空県架空市1-1 ",
            "manager_ids": [m1, m2, m1],
            "vehicle_ids": [v1, v1, v2]
        })),
    )
    .await;
    assert_eq!(made.status, 201);
    let a = made.json();
    assert_eq!(a["name"], "架空運輸 本社営業所");
    assert_eq!(a["company_name"], json!(null));
    assert_eq!(a["address"], "架空県架空市1-1");
    assert_eq!(ids(&a["manager_ids"]), sorted(vec![m1, m2]));
    assert_eq!(ids(&a["vehicle_ids"]), vec![v1.to_string(), v2.to_string()]);
    let a_id: Uuid = a["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(vehicle_office(v1).await, Some(a_id));

    // 省略した欄は空。車両は別の営業所から移る (v1 は b へ移り、a は v2 だけになる)
    let made = call(
        &app,
        "POST",
        PATH,
        Some(t),
        Some(json!({ "name": "架空グループ 第一営業所", "company_name": "架空グループ株式会社", "vehicle_ids": [v1] })),
    )
    .await;
    assert_eq!(made.status, 201);
    let b = made.json();
    assert_eq!(b["company_name"], "架空グループ株式会社");
    assert_eq!(b["address"], json!(null));
    assert_eq!(b["manager_ids"], json!([]));
    assert_eq!(ids(&b["vehicle_ids"]), vec![v1.to_string()]);
    let b_id: Uuid = b["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(vehicle_office(v1).await, Some(b_id));

    // 一覧 (名称順。車両の所属つき)
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    let names: Vec<_> = list["offices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["name"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["架空グループ 第一営業所", "架空運輸 本社営業所"]);
    assert_eq!(list["offices"][1]["vehicle_ids"], json!([v2]));
    assert_eq!(list["vehicles"][0]["office_id"], json!(b_id));
    assert_eq!(list["vehicles"][1]["office_id"], json!(a_id));
    assert_eq!(list["vehicles"][2]["office_id"], json!(null));

    // PUT は全部の置き換え: 名称を変え、運行管理者は m2 だけ、車両は v3 だけにする (v2 は所属が外れる)
    let put = call(
        &app,
        "PUT",
        &format!("{PATH}/{a_id}"),
        Some(t),
        Some(json!({ "name": "架空運輸 本社", "manager_ids": [m2], "vehicle_ids": [v3] })),
    )
    .await;
    assert_eq!(put.status, 200);
    let a = put.json();
    assert_eq!(
        a,
        json!({
            "id": a_id, "name": "架空運輸 本社", "company_name": null, "company_id": null, "address": null,
            "manager_ids": [m2], "vehicle_ids": [v3], "carins_bases": [], "device_ids": []
        })
    );
    assert_eq!(vehicle_office(v2).await, None);
    assert_eq!(vehicle_office(v3).await, Some(a_id));
    assert_eq!(vehicle_office(v1).await, Some(b_id));

    // 省略は空 (運行管理者も車両も外れる)
    let put = call(
        &app,
        "PUT",
        &format!("{PATH}/{a_id}"),
        Some(t),
        Some(json!({ "name": "架空運輸 本社" })),
    )
    .await
    .json();
    assert_eq!(
        (&put["manager_ids"], &put["vehicle_ids"]),
        (&json!([]), &json!([]))
    );
    assert_eq!(vehicle_office(v3).await, None);

    // PUT の検査 (外れたら何も書かない) と 404
    let got = call(
        &app,
        "PUT",
        &format!("{PATH}/{b_id}"),
        Some(t),
        Some(json!({ "name": "書かれない", "manager_ids": [driver] })),
    )
    .await;
    assert_eq!(
        (got.status, got.json()),
        (400, json!({ "error": "invalid_manager" }))
    );
    let got = call(
        &app,
        "PUT",
        &format!("{PATH}/{b_id}"),
        Some(t),
        Some(json!({ "name": "書かれない", "vehicle_ids": [gone] })),
    )
    .await;
    assert_eq!(
        (got.status, got.json()),
        (400, json!({ "error": "invalid_vehicle" }))
    );
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert_eq!(list["offices"][0]["name"], "架空グループ 第一営業所");
    let missing = format!("{PATH}/{}", Uuid::new_v4());
    for (method, body) in [("PUT", Some(json!({ "name": "x" }))), ("DELETE", None)] {
        assert_eq!(
            call(&app, method, &missing, Some(t), body).await.status,
            404
        );
    }
    let bad = call(&app, "PUT", &format!("{PATH}/{b_id}"), Some(t), None).await;
    assert_eq!(bad.status, 400);

    // 別のテナントからは見えず、触れない
    let theirs = call(&app, "GET", PATH, Some(other), None).await.json();
    assert_eq!(theirs["offices"], json!([]));
    assert_eq!(theirs["vehicles"].as_array().unwrap().len(), 1);
    let own = format!("{PATH}/{b_id}");
    assert_eq!(
        call(&app, "PUT", &own, Some(other), Some(json!({ "name": "x" })))
            .await
            .status,
        404
    );
    assert_eq!(
        call(&app, "DELETE", &own, Some(other), None).await.status,
        404
    );
    // 他のテナントの営業所へは自分の運行管理者・車両を付けられない
    let their_office = call(
        &app,
        "POST",
        PATH,
        Some(other),
        Some(json!({ "name": "架空の他社営業所", "manager_ids": [foreign], "vehicle_ids": [foreign_v] })),
    )
    .await;
    assert_eq!(their_office.status, 201);
    let their_id = their_office.json()["id"].as_str().unwrap().to_owned();
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("{PATH}/{their_id}"),
            Some(t),
            Some(json!({ "name": "x", "vehicle_ids": [foreign_v] })),
        )
        .await
        .status,
        400
    );

    // 消す: 所属は外れる (運行管理者の行は消え、車両は営業所なしに戻る)。社員も車両も消えない
    assert_eq!(call(&app, "DELETE", &own, Some(t), None).await.status, 204);
    assert_eq!(call(&app, "DELETE", &own, Some(t), None).await.status, 404);
    assert_eq!(vehicle_office(v1).await, None);
    assert_eq!(count("tenko_offices", t).await, 1);
    assert_eq!(count("tenko_office_managers", t).await, 0);
    assert_eq!(count("tenko_offices", other).await, 1);
    assert_eq!(count("tenko_office_managers", other).await, 1);

    // 社員が消えると運行管理者の所属も消える (表の外部キー)
    let put = call(
        &app,
        "PUT",
        &format!("{PATH}/{a_id}"),
        Some(t),
        Some(json!({ "name": "架空運輸 本社", "manager_ids": [m1] })),
    )
    .await;
    assert_eq!(put.status, 200);
    admin()
        .await
        .execute("DELETE FROM alc_api.employees WHERE id = $1", &[&m1])
        .await
        .unwrap();
    assert_eq!(count("tenko_office_managers", t).await, 0);
    assert!(app.logs().is_empty());
}

/// その事業者を 1 件作る
async fn company(tenant: Uuid, name: &str) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.tenko_companies (tenant_id, name) VALUES ($1, $2) RETURNING id",
            &[&tenant, &name],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn offices_company() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let c1 = company(t, "架空運輸株式会社").await;
    let c2 = company(t, "架空グループ株式会社").await;
    let foreign = company(other, "他社の事業者").await;

    // company_id を付けて作る (応答にも一覧にも出る。company_name は別の欄のまま)
    let post = call(
        &app,
        "POST",
        PATH,
        Some(t),
        Some(json!({ "name": "架空 第一営業所", "company_name": "旧い会社名", "company_id": c1 })),
    )
    .await;
    assert_eq!(post.status, 201);
    let a = post.json();
    assert_eq!(a["company_id"], json!(c1));
    assert_eq!(a["company_name"], "旧い会社名");
    let a_id = a["id"].as_str().unwrap().to_owned();

    // 省略は null
    let post = call(
        &app,
        "POST",
        PATH,
        Some(t),
        Some(json!({ "name": "架空 第二営業所" })),
    )
    .await;
    assert_eq!(post.status, 201);
    assert_eq!(post.json()["company_id"], json!(null));
    let b_id = post.json()["id"].as_str().unwrap().to_owned();

    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert_eq!(list["offices"][0]["company_id"], json!(c1));
    assert_eq!(list["offices"][1]["company_id"], json!(null));

    // PUT で付け替える
    let put = call(
        &app,
        "PUT",
        &format!("{PATH}/{a_id}"),
        Some(t),
        Some(json!({ "name": "架空 第一営業所", "company_id": c2 })),
    )
    .await;
    assert_eq!(put.status, 200);
    assert_eq!(put.json()["company_id"], json!(c2));

    // PUT で省略すると null に戻る
    let put = call(
        &app,
        "PUT",
        &format!("{PATH}/{a_id}"),
        Some(t),
        Some(json!({ "name": "架空 第一営業所" })),
    )
    .await;
    assert_eq!(put.status, 200);
    assert_eq!(put.json()["company_id"], json!(null));

    // 別テナントの事業者・無い事業者は 400 invalid_company で、何も書かない
    let before = count("tenko_offices", t).await;
    for bad in [foreign, Uuid::new_v4()] {
        let body = json!({ "name": "書かれない", "company_id": bad });
        let got = call(&app, "POST", PATH, Some(t), Some(body.clone())).await;
        assert_eq!(
            (got.status, got.json()),
            (400, json!({ "error": "invalid_company" }))
        );
        let got = call(&app, "PUT", &format!("{PATH}/{b_id}"), Some(t), Some(body)).await;
        assert_eq!(
            (got.status, got.json()),
            (400, json!({ "error": "invalid_company" }))
        );
    }
    assert_eq!(count("tenko_offices", t).await, before);

    // 事業者を消すと営業所の company_id は null になる
    let put = call(
        &app,
        "PUT",
        &format!("{PATH}/{b_id}"),
        Some(t),
        Some(json!({ "name": "架空 第二営業所", "company_id": c1 })),
    )
    .await;
    assert_eq!(put.json()["company_id"], json!(c1));
    admin()
        .await
        .execute("DELETE FROM alc_api.tenko_companies WHERE id = $1", &[&c1])
        .await
        .unwrap();
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert!(list["offices"]
        .as_array()
        .unwrap()
        .iter()
        .all(|o| o["company_id"] == json!(null)));
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn offices_carins_bases() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;

    // 車検証が空のテナントは空の配列 (エラーにしない)
    let got = call(&app, "GET", PATH, Some(t), None).await;
    assert_eq!(
        (got.status, got.json()["carins_bases"].clone()),
        (200, json!([]))
    );

    // 組は (使用の本拠, 使用者) で、使用者は高位の欄が空なら低位の欄 (どちらも前後の空白を落とす)。
    // 使用の本拠が空の行は除き、車両数は distinct CarId
    car_inspection(t, "C1", "CAR1", " KANTO1-1 ", " ACME ", "ignored").await;
    car_inspection(t, "C2", "CAR2", "KANTO1-1", "ACME", "").await;
    car_inspection(t, "C3", "CAR2", "KANTO1-1", "ACME", "").await;
    car_inspection(t, "C4", "CAR3", "KANSAI2-2", "  ", " LOWNAME ").await;
    car_inspection(t, "C5", "CAR4", "  ", "ACME", "").await;
    car_inspection(other, "C6", "CAR5", "OTHER9-9", "OTHERCO", "").await;
    let bases = |v: &Value| v["carins_bases"].clone();
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert_eq!(
        bases(&list),
        json!([
            { "use_base": "KANSAI2-2", "user_name": "LOWNAME", "office_id": null, "vehicle_count": 1 },
            { "use_base": "KANTO1-1", "user_name": "ACME", "office_id": null, "vehicle_count": 2 },
        ])
    );
    let theirs = call(&app, "GET", PATH, Some(other), None).await.json();
    assert_eq!(
        bases(&theirs),
        json!([{ "use_base": "OTHER9-9", "user_name": "OTHERCO", "office_id": null, "vehicle_count": 1 }])
    );

    // 営業所に組を対応づける (空白を落とす・重複はまとめる)。車検証に無い組も置ける
    let a = call(
        &app,
        "POST",
        PATH,
        Some(t),
        Some(json!({ "name": "架空 A営業所", "carins_bases": [
            { "use_base": " KANTO1-1 ", "user_name": " ACME " },
            { "use_base": "KANTO1-1", "user_name": "ACME" },
            { "use_base": "NOTYET", "user_name": "ACME" },
        ] })),
    )
    .await;
    assert_eq!(a.status, 201);
    let a = a.json();
    assert_eq!(
        a["carins_bases"],
        json!([
            { "use_base": "KANTO1-1", "user_name": "ACME" },
            { "use_base": "NOTYET", "user_name": "ACME" },
        ])
    );
    let a_id = a["id"].as_str().unwrap().to_owned();
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert_eq!(list["carins_bases"][1]["office_id"], json!(a_id));
    assert_eq!(list["carins_bases"][0]["office_id"], json!(null));

    // 別の営業所へ移る (A からは外れ、B の応答には移した組が出る)
    let b = call(
        &app,
        "POST",
        PATH,
        Some(t),
        Some(json!({ "name": "架空 B営業所", "carins_bases": [{ "use_base": "KANTO1-1", "user_name": "ACME" }] })),
    )
    .await
    .json();
    let b_id = b["id"].as_str().unwrap().to_owned();
    assert_eq!(
        b["carins_bases"],
        json!([{ "use_base": "KANTO1-1", "user_name": "ACME" }])
    );
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert_eq!(list["carins_bases"][1]["office_id"], json!(b_id));
    assert_eq!(
        list["offices"][0]["carins_bases"],
        json!([{ "use_base": "NOTYET", "user_name": "ACME" }])
    );
    assert_eq!(count("tenko_office_carins_bases", t).await, 2);

    // 検査 (外れたら何も書かない)
    let many: Vec<Value> = (0..1001)
        .map(|i| json!({ "use_base": format!("b{i}"), "user_name": "u" }))
        .collect();
    for (bases, word) in [
        (
            json!([{ "use_base": " ", "user_name": "u" }]),
            "invalid_carins_base",
        ),
        (
            json!([{ "use_base": "b", "user_name": "" }]),
            "invalid_carins_base",
        ),
        (
            json!([{ "use_base": "あ".repeat(501), "user_name": "u" }]),
            "too_long",
        ),
        (
            json!([{ "use_base": "b", "user_name": "あ".repeat(501) }]),
            "too_long",
        ),
        (json!(many), "too_many_ids"),
    ] {
        let got = call(
            &app,
            "PUT",
            &format!("{PATH}/{b_id}"),
            Some(t),
            Some(json!({ "name": "架空 B営業所", "carins_bases": bases })),
        )
        .await;
        assert_eq!((got.status, got.json()), (400, json!({ "error": word })));
    }
    assert_eq!(count("tenko_office_carins_bases", t).await, 2);

    // 置き換え (省略は空)。営業所を消すと組の対応も消える
    let put = call(
        &app,
        "PUT",
        &format!("{PATH}/{b_id}"),
        Some(t),
        Some(json!({ "name": "架空 B営業所" })),
    )
    .await;
    assert_eq!(put.json()["carins_bases"], json!([]));
    assert_eq!(count("tenko_office_carins_bases", t).await, 1);
    assert_eq!(
        call(&app, "DELETE", &format!("{PATH}/{a_id}"), Some(t), None)
            .await
            .status,
        204
    );
    assert_eq!(count("tenko_office_carins_bases", t).await, 0);
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert_eq!(list["carins_bases"][1]["office_id"], json!(null));
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn offices_devices() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let ids = |v: &Value| v["device_ids"].clone();

    // 空白を落とす・重複はまとめる・device_id 順
    let a = call(
        &app,
        "POST",
        PATH,
        Some(t),
        Some(json!({ "name": "架空 A営業所", "device_ids": [" dev-b ", "dev-a", "dev-b", "AA:11_x"] })),
    )
    .await;
    assert_eq!(a.status, 201);
    let a = a.json();
    assert_eq!(ids(&a), json!(["AA:11_x", "dev-a", "dev-b"]));
    let a_id = a["id"].as_str().unwrap().to_owned();

    // 端末は複数の営業所に所属できる (A からは外れない。同じ建物の別の会社の営業所が 1 台を共用する)。省略した PUT は空に置き換える
    let b = call(
        &app,
        "POST",
        PATH,
        Some(t),
        Some(json!({ "name": "架空 B営業所", "device_ids": ["dev-a"] })),
    )
    .await
    .json();
    let b_id = b["id"].as_str().unwrap().to_owned();
    assert_eq!(ids(&b), json!(["dev-a"]));
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert_eq!(
        ids(&list["offices"][0]),
        json!(["AA:11_x", "dev-a", "dev-b"])
    );
    assert_eq!(ids(&list["offices"][1]), json!(["dev-a"]));
    assert_eq!(count("tenko_office_devices", t).await, 4);

    // 検査 (外れたら何も書かない)。形・長さ・件数
    let many: Vec<String> = (0..1001).map(|i| format!("d{i}")).collect();
    for (devices, word) in [
        (json!(["ok", ""]), "invalid_device"),
        (json!(["  "]), "invalid_device"),
        (json!(["a b"]), "invalid_device"),
        (json!(["日本語"]), "invalid_device"),
        (json!(["a".repeat(65)]), "invalid_device"),
        (json!(many), "too_many_ids"),
    ] {
        let got = call(
            &app,
            "PUT",
            &format!("{PATH}/{b_id}"),
            Some(t),
            Some(json!({ "name": "架空 B営業所", "device_ids": devices })),
        )
        .await;
        assert_eq!((got.status, got.json()), (400, json!({ "error": word })));
    }
    assert_eq!(count("tenko_office_devices", t).await, 4);
    let ok64 = "a".repeat(64);
    let put = call(
        &app,
        "PUT",
        &format!("{PATH}/{b_id}"),
        Some(t),
        Some(json!({ "name": "架空 B営業所", "device_ids": [ok64] })),
    )
    .await;
    assert_eq!(put.status, 200);
    let put = call(
        &app,
        "PUT",
        &format!("{PATH}/{b_id}"),
        Some(t),
        Some(json!({ "name": "架空 B営業所" })),
    )
    .await;
    assert_eq!(ids(&put.json()), json!([]));
    assert_eq!(count("tenko_office_devices", t).await, 3);

    // 他のテナントの行は見えず、同じ端末の id も別に持てる。営業所を消すと所属も消える
    let theirs = call(
        &app,
        "POST",
        PATH,
        Some(other),
        Some(json!({ "name": "架空の他社営業所", "device_ids": ["dev-b"] })),
    )
    .await;
    assert_eq!(ids(&theirs.json()), json!(["dev-b"]));
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert_eq!(
        ids(&list["offices"][0]),
        json!(["AA:11_x", "dev-a", "dev-b"])
    );
    assert_eq!(
        call(&app, "DELETE", &format!("{PATH}/{a_id}"), Some(t), None)
            .await
            .status,
        204
    );
    assert_eq!(count("tenko_office_devices", t).await, 0);
    assert_eq!(count("tenko_office_devices", other).await, 1);
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn offices_carins_bases_ignore_spaces() {
    let app = app().await;
    let t = tenant().await;
    let name = "有限会社大石運輸本社営業所";

    // 空白 (半角・全角) の有無だけ違う 2 枚の車検証は 1 組 (台数 2)
    car_inspection(
        t,
        "S1",
        "CAR1",
        "KANTO 1-1",
        "有限会社　大石運輸　本社営業所",
        "",
    )
    .await;
    car_inspection(
        t,
        "S2",
        "CAR2",
        "KANTO1-1",
        "有限会社大石運輸　本社営業所",
        "",
    )
    .await;
    let one = |office: Value| json!([{ "use_base": "KANTO1-1", "user_name": name, "office_id": office, "vehicle_count": 2 }]);
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert_eq!(list["carins_bases"], one(json!(null)));

    // 空白入りで保存した古い行 (直接 INSERT) が一覧の office_id に効く。複数の営業所に分かれていたら office_id が最小の 1 つ
    let mk = |n: &str| {
        let app = &app;
        let n = n.to_owned();
        async move {
            call(app, "POST", PATH, Some(t), Some(json!({ "name": n })))
                .await
                .json()["id"]
                .as_str()
                .unwrap()
                .parse::<Uuid>()
                .unwrap()
        }
    };
    let x = mk("架空 X営業所").await;
    let y = mk("架空 Y営業所").await;
    for (office, base, user) in [
        (x, "KANTO 1-1", "有限会社 大石運輸 本社営業所"),
        (y, "KANTO1-1 ", "有限会社大石運輸　本社営業所"),
    ] {
        admin()
            .await
            .execute(
                "INSERT INTO alc_api.tenko_office_carins_bases (tenant_id, office_id, use_base, user_name) VALUES ($1, $2, $3, $4)",
                &[&t, &office, &base, &user],
            )
            .await
            .unwrap();
    }
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert_eq!(list["carins_bases"], one(json!(x.min(y))));
    // 営業所ごとの組は正規化して返す
    let own: Vec<_> = list["offices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["carins_bases"].clone())
        .collect();
    assert_eq!(
        own,
        vec![json!([{ "use_base": "KANTO1-1", "user_name": name }]); 2]
    );

    // 空白入りの入力は正規化して保存し、別の営業所の空白違いの古い行は消える
    let z = call(
        &app,
        "POST",
        PATH,
        Some(t),
        Some(json!({ "name": "架空 Z営業所", "carins_bases": [
            { "use_base": " KANTO　1-1 ", "user_name": "有限会社　大石運輸　本社営業所" },
            { "use_base": "KANTO1-1", "user_name": name },
        ] })),
    )
    .await;
    assert_eq!(z.status, 201);
    let z = z.json();
    assert_eq!(
        z["carins_bases"],
        json!([{ "use_base": "KANTO1-1", "user_name": name }])
    );
    let z_id: Uuid = z["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(count("tenko_office_carins_bases", t).await, 1);
    let list = call(&app, "GET", PATH, Some(t), None).await.json();
    assert_eq!(list["carins_bases"], one(json!(z_id)));

    // 空白だけの値は空と同じ
    let got = call(
        &app,
        "POST",
        PATH,
        Some(t),
        Some(json!({ "name": "架空", "carins_bases": [{ "use_base": " 　 ", "user_name": "u" }] })),
    )
    .await;
    assert_eq!(
        (got.status, got.json()),
        (400, json!({ "error": "invalid_carins_base" }))
    );
    assert_eq!(count("tenko_office_carins_bases", t).await, 1);
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn offices_limit() {
    let app = app().await;
    let t = tenant().await;
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.tenko_offices (tenant_id, name) SELECT $1, 'n' || g FROM generate_series(1, 100) g",
            &[&t],
        )
        .await
        .unwrap();
    // 上限に届いたら増やせない (別のテナントは作れる)
    let got = call(
        &app,
        "POST",
        PATH,
        Some(t),
        Some(json!({ "name": "もう 1 件" })),
    )
    .await;
    assert_eq!(
        (got.status, got.json()),
        (400, json!({ "error": "too_many_offices" }))
    );
    assert_eq!(count("tenko_offices", t).await, 100);
    let other = tenant().await;
    let got = call(
        &app,
        "POST",
        PATH,
        Some(other),
        Some(json!({ "name": "1 件目" })),
    )
    .await;
    assert_eq!(got.status, 201);
}

#[tokio::test]
async fn offices_db_failures() {
    let t = tenant().await;
    let id = format!("{PATH}/{}", Uuid::new_v4());

    // 書けないロール (車両の表も読めない) → 4 つの口とも 500 でログは固定の語
    let app = build(client_as(READ_ONLY_ROLE).await);
    let body = || Some(json!({ "name": "架空の営業所" }));
    for (method, path, body, stage) in [
        ("GET", PATH.to_owned(), None, "list"),
        ("POST", PATH.to_owned(), body(), "save"),
        ("PUT", id.clone(), body(), "save"),
        ("DELETE", id.clone(), None, "delete"),
    ] {
        assert_eq!(call(&app, method, &path, Some(t), body).await.status, 500);
        let last = app.logs().pop().unwrap();
        assert!(last.starts_with(&format!("tenko: offices {stage} failed: db (")));
    }
    assert_logs_clean(&app, &[t.to_string()]);

    // 切れた接続 → 読みも 500
    let app = build(broken_client().await);
    assert_eq!(call(&app, "GET", PATH, Some(t), None).await.status, 500);
    assert!(app.logs()[0].starts_with("tenko: offices list failed: db ("));
}
