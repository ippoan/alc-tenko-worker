//! 点呼の営業所 (`/api/tenko/offices`) を実 DB で通す。値は架空。

mod support;

use serde_json::{json, Value};
use support::{
    admin, app, assert_logs_clean, broken_client, build, call, client_as, employee, tenant,
    READ_ONLY_ROLE,
};
use uuid::Uuid;

const PATH: &str = "/api/tenko/offices";

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

    // 何も無ければ空の営業所と、未削除の車両 (車両番号順)
    let got = call(&app, "GET", PATH, Some(t), None).await;
    assert_eq!(got.status, 200);
    assert_eq!(
        got.json(),
        json!({
            "offices": [],
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
            "id": a_id, "name": "架空運輸 本社", "company_name": null, "address": null,
            "manager_ids": [m2], "vehicle_ids": [v3]
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
