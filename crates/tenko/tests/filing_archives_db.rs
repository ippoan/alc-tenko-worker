//! 点呼の届出の申請データの保管 (`/api/tenko/filing-archives`) を、モックの置き場と実 DB (uuid を引く) で通す。値は架空。

mod support;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex as StdMutex};

use axum::body::Body;
use axum::http::{HeaderValue, Request};
use futures_util::future::BoxFuture;
use serde_json::json;
use support::{assert_logs_clean, broken_client, build_with_files, rt_client, tenant, App, Got};
use tenko::filing_archives::{
    FileStore, SharedStore, StoreError, StoredFile, StoredObject, MAX_BYTES,
};
use tower::ServiceExt;
use uuid::Uuid;

const PATH: &str = "/api/tenko/filing-archives";
/// support::NOW_MS の RFC3339
const NOW: &str = "2026-10-28T01:30:00.000Z";

/// キー → 中身と customMetadata
type Objects = BTreeMap<String, (Vec<u8>, HashMap<String, String>)>;

/// モックの置き場 (キー → 中身と customMetadata)。`fail` なら全部の段で失敗する
#[derive(Default)]
struct Mock {
    objects: StdMutex<Objects>,
    fail: bool,
}

impl Mock {
    fn insert(&self, key: String, bytes: &[u8], metadata: &[(&str, &str)]) {
        let m = metadata
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        self.objects
            .lock()
            .unwrap()
            .insert(key, (bytes.to_vec(), m));
    }
}

fn object(key: &str, bytes: &[u8], metadata: &HashMap<String, String>) -> StoredObject {
    StoredObject {
        key: key.to_owned(),
        size: bytes.len() as u64,
        metadata: metadata.clone(),
    }
}

impl FileStore for Mock {
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
        metadata: HashMap<String, String>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            if self.fail {
                return Err(StoreError("put"));
            }
            assert_eq!(content_type, "application/zip");
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_owned(), (bytes, metadata));
            Ok(())
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        max: usize,
    ) -> BoxFuture<'a, Result<Vec<StoredObject>, StoreError>> {
        Box::pin(async move {
            if self.fail {
                return Err(StoreError("list"));
            }
            Ok(self
                .objects
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .take(max)
                .map(|(k, (b, m))| object(k, b, m))
                .collect())
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<StoredFile>, StoreError>> {
        Box::pin(async move {
            if self.fail {
                return Err(StoreError("get"));
            }
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .map(|(b, m)| (object(key, b, m), b.clone())))
        })
    }
}

/// 1 リクエストを打つ。`role` が在れば auth-worker と同じく利用者の 3 つのヘッダーを付ける
async fn send(
    app: &App,
    method: &str,
    uri: &str,
    tenant: Uuid,
    role: Option<&str>,
    headers: &[(&str, HeaderValue)],
    body: Vec<u8>,
) -> Got {
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Tenant-ID", tenant.to_string());
    if let Some(role) = role {
        b = b
            .header("X-User-ID", Uuid::new_v4().to_string())
            .header("X-User-Email", "archive-test@example.com")
            .header("X-User-Role", role);
    }
    for (k, v) in headers {
        b = b.header(*k, v.clone());
    }
    let resp = app
        .router
        .clone()
        .oneshot(b.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    Got {
        status,
        headers,
        body,
    }
}

fn h(v: &str) -> HeaderValue {
    HeaderValue::from_str(v).unwrap()
}

/// encodeURIComponent と同じく、英数字と `-_.!~*'()` 以外を % にする
fn enc(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// RFC 8187 の attr-char (英数字と `!#$&+-.^_`|~`) 以外を % にする
fn attr(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// `query` (encode 済み) を付けて保存の口を admin で打つ
async fn post(app: &App, tenant: Uuid, query: &str, body: &[u8]) -> Got {
    let uri = format!("{PATH}?{query}");
    let ct = [("Content-Type", h("application/zip"))];
    send(app, "POST", &uri, tenant, Some("admin"), &ct, body.to_vec()).await
}

fn with_store(pg: alc_worker_db::PgClient, store: &Arc<Mock>) -> App {
    build_with_files(pg, Some(store.clone() as SharedStore))
}

#[tokio::test]
async fn archives_save_list_get() {
    let store = Arc::new(Mock::default());
    let app = with_store(rt_client().await, &store);
    let t = tenant().await;
    let other = tenant().await;
    let name = "点呼の届出 申請データ (架空).zip";
    let meta =
        json!({ "reportKind": "届出", "startDate": "2026-11-01", "errors": [], "warns": ["架空"] });

    // 保存 (201)。meta は JSON の値で返り、saved_by は利用者の ID
    let got = post(
        &app,
        t,
        &format!("meta={}&name={}", enc(&meta.to_string()), enc(name)),
        b"PK\x03\x04 fake zip",
    )
    .await;
    assert_eq!(got.status, 201);
    let saved = got.json();
    let id = saved["id"].as_str().unwrap().to_owned();
    assert!(id.starts_with("1793151000000-") && id.len() == 50);
    assert_eq!(
        (
            saved["name"].clone(),
            saved["size"].clone(),
            saved["saved_at"].clone(),
            saved["meta"].clone(),
            saved["saved_by"].is_string(),
        ),
        (json!(name), json!(18), json!(NOW), meta.clone(), true)
    );
    // 置き場のキーと customMetadata
    let (bytes, m) = store.objects.lock().unwrap()[&format!("{t}/{id}.zip")].clone();
    assert_eq!(bytes, b"PK\x03\x04 fake zip");
    assert_eq!(
        (
            m["name"].as_str(),
            m["saved_at"].as_str(),
            m.get("saved_by").cloned()
        ),
        (name, NOW, saved["saved_by"].as_str().map(str::to_owned))
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&m["meta"]).unwrap(),
        meta
    );

    // meta 無しの保存 (meta は null)
    let got = post(&app, t, "name=b.zip", b"zip").await;
    assert_eq!((got.status, got.json()["meta"].clone()), (201, json!(null)));
    let second = got.json()["id"].as_str().unwrap().to_owned();

    // 古い保管・customMetadata の無い保管・形の違うキー (一覧に出さない) を置き場に直に置く
    let old = format!("1577836800000-{}", Uuid::new_v4());
    store.insert(
        format!("{t}/{old}.zip"),
        b"old",
        &[
            ("name", "old.zip"),
            ("saved_at", "2020-01-01T00:00:00.000Z"),
            ("meta", "not json"),
        ],
    );
    let bare = format!("0000000000001-{}", Uuid::new_v4());
    store.insert(format!("{t}/{bare}.zip"), b"bare", &[]);
    store.insert(format!("{t}/{bare}.txt"), b"x", &[]);
    store.insert(format!("{t}/not-an-id.zip"), b"x", &[]);
    // 別のテナントの保管
    store.insert(
        format!("{other}/{old}.zip"),
        b"other",
        &[("name", "other.zip")],
    );

    // 一覧は saved_at の新しい順 (同じ時刻は id の大きい順)。形の違うキーと別のテナントは出ない
    let got = send(&app, "GET", PATH, t, Some("admin"), &[], vec![]).await;
    assert_eq!(got.status, 200);
    let list = got.json();
    let mut newest = [id.clone(), second.clone()];
    newest.sort();
    newest.reverse();
    let ids: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        [
            newest[0].as_str(),
            newest[1].as_str(),
            old.as_str(),
            bare.as_str()
        ]
    );
    assert_eq!(
        list[3],
        json!({ "id": bare, "name": format!("{bare}.zip"), "size": 4, "saved_at": "", "saved_by": null, "meta": null })
    );
    assert_eq!(
        (list[2]["meta"].clone(), list[2]["saved_by"].clone()),
        (json!(null), json!(null))
    );

    // 取得は zip と元のファイル名
    let got = send(
        &app,
        "GET",
        &format!("{PATH}/{id}"),
        t,
        Some("admin"),
        &[],
        vec![],
    )
    .await;
    assert_eq!(
        (got.status, got.body.as_slice()),
        (200, &b"PK\x03\x04 fake zip"[..])
    );
    assert_eq!(got.headers["content-type"], "application/zip");
    assert_eq!(
        got.headers["content-disposition"],
        format!("attachment; filename*=UTF-8''{}", attr(name))
    );
    // 名前の無い保管は `{id}.zip`
    let got = send(
        &app,
        "GET",
        &format!("{PATH}/{bare}"),
        t,
        Some("admin"),
        &[],
        vec![],
    )
    .await;
    assert_eq!(
        got.headers["content-disposition"],
        format!("attachment; filename*=UTF-8''{bare}.zip")
    );

    // 別のテナントからは見えない
    let got = send(&app, "GET", PATH, other, Some("admin"), &[], vec![]).await;
    assert_eq!(got.json().as_array().unwrap().len(), 1);
    let got = send(
        &app,
        "GET",
        &format!("{PATH}/{id}"),
        other,
        Some("admin"),
        &[],
        vec![],
    )
    .await;
    assert_eq!(got.status, 404);
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn archives_reject_bad_requests() {
    let store = Arc::new(Mock::default());
    let app = with_store(rt_client().await, &store);
    let t = tenant().await;
    let name = "name=a.zip";

    // admin 以外 (役の無い要求・admin でない役) は 3 口とも 403
    let one = format!("{PATH}/1793151000000-{}", Uuid::new_v4());
    let post_uri = format!("{PATH}?{name}");
    for (method, uri, role) in [
        ("POST", post_uri.as_str(), None),
        ("POST", post_uri.as_str(), Some("viewer")),
        ("GET", PATH, Some("viewer")),
        ("GET", one.as_str(), Some("viewer")),
    ] {
        let got = send(&app, method, uri, t, role, &[], b"zip".to_vec()).await;
        assert_eq!(
            (got.status, got.json()),
            (403, json!({ "error": "forbidden" }))
        );
    }

    // 名前: 無い・`=` が無い・似た名前のキーだけ・空・201 文字・UTF-8 でない。ヘッダーでは受けない
    for query in [
        String::new(),
        "name".to_owned(),
        "names=a.zip&filename=a.zip".to_owned(),
        "name=".to_owned(),
        format!("name={}", enc(&"あ".repeat(201))),
        "name=%FF".to_owned(),
    ] {
        let got = post(&app, t, &query, b"zip").await;
        assert_eq!(
            (got.status, got.json()),
            (400, json!({ "error": "invalid_name" }))
        );
    }
    let got = send(
        &app,
        "POST",
        PATH,
        t,
        Some("admin"),
        &[("X-Filing-Name", h("a.zip"))],
        b"zip".to_vec(),
    )
    .await;
    assert_eq!(
        (got.status, got.json()),
        (400, json!({ "error": "invalid_name" }))
    );
    // 200 文字は通る
    let got = post(
        &app,
        t,
        &format!("x=1&name={}", enc(&"あ".repeat(200))),
        b"zip",
    )
    .await;
    assert_eq!(got.status, 201);
    // `+` は空白にしない (encodeURIComponent は空白を %20 にする)
    let got = post(&app, t, "name=a+b%20c.zip", b"zip").await;
    assert_eq!(got.json()["name"], "a+b c.zip");

    // 要約: JSON でない・object でない・4000 文字を超える・UTF-8 でない
    let long = json!({ "x": "a".repeat(4000) }).to_string();
    for meta in [enc("{"), enc("[1]"), enc(&long), "%FF".to_owned()] {
        let got = post(&app, t, &format!("{name}&meta={meta}"), b"zip").await;
        assert_eq!(
            (got.status, got.json()),
            (400, json!({ "error": "invalid_meta" }))
        );
    }

    // 本文: 空は 400、20 MiB を超えると 413 (ちょうど 20 MiB は通る)
    let got = post(&app, t, name, b"").await;
    assert_eq!((got.status, got.json()), (400, json!({ "error": "empty" })));
    let got = post(&app, t, name, &vec![0u8; MAX_BYTES + 1]).await;
    assert_eq!(
        (got.status, got.json()),
        (413, json!({ "error": "too_large" }))
    );
    let got = post(&app, t, name, &vec![0u8; MAX_BYTES]).await;
    assert_eq!(
        (got.status, got.json()["size"].clone()),
        (201, json!(MAX_BYTES))
    );

    // 取得: id の形でない・無い は 404
    for id in [
        "x".to_owned(),
        format!("179315100000-{}", Uuid::new_v4()),
        format!("1793151000000_{}", Uuid::new_v4()),
        format!(
            "1793151000000-{}",
            Uuid::new_v4().to_string().to_uppercase()
        ),
        format!("1793151000000-{}", Uuid::new_v4()),
    ] {
        let got = send(
            &app,
            "GET",
            &format!("{PATH}/{id}"),
            t,
            Some("admin"),
            &[],
            vec![],
        )
        .await;
        assert_eq!(got.status, 404);
    }
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn archives_unconfigured_store_is_503() {
    let app = build_with_files(rt_client().await, None);
    let t = tenant().await;
    let one = format!("{PATH}/1793151000000-{}", Uuid::new_v4());
    let post_uri = format!("{PATH}?name=a.zip");
    for (method, uri) in [
        ("POST", post_uri.as_str()),
        ("GET", PATH),
        ("GET", one.as_str()),
    ] {
        let got = send(&app, method, uri, t, Some("admin"), &[], b"zip".to_vec()).await;
        assert_eq!((got.status, got.body.len()), (503, 0));
    }
    assert_eq!(
        app.logs(),
        [
            "tenko: filing archive put failed: storage (unconfigured)",
            "tenko: filing archive list failed: storage (unconfigured)",
            "tenko: filing archive get failed: storage (unconfigured)",
        ]
    );
    // ほかの口は動く
    let got = send(
        &app,
        "GET",
        "/api/tenko/filing-settings",
        t,
        Some("admin"),
        &[],
        vec![],
    )
    .await;
    assert_eq!(got.status, 200);
}

#[tokio::test]
async fn archives_store_and_db_failures_are_500() {
    let t = tenant().await;
    let name = "架空の申請.zip";
    let failing = Arc::new(Mock {
        fail: true,
        ..Mock::default()
    });
    let app = with_store(rt_client().await, &failing);
    let one = format!("{PATH}/1793151000000-{}", Uuid::new_v4());
    let post_uri = format!("{PATH}?name={}", enc(name));
    for (method, uri) in [
        ("POST", post_uri.as_str()),
        ("GET", PATH),
        ("GET", one.as_str()),
    ] {
        let got = send(&app, method, uri, t, Some("admin"), &[], b"zip".to_vec()).await;
        assert_eq!((got.status, got.body.len()), (500, 0));
    }
    assert_eq!(
        app.logs(),
        [
            "tenko: filing archive put failed: storage (put)",
            "tenko: filing archive list failed: storage (list)",
            "tenko: filing archive get failed: storage (get)",
        ]
    );
    assert_logs_clean(&app, &[t.to_string(), name.to_owned()]);

    // uuid を引く DB が落ちていれば 500 (置き場には置かない)
    let store = Arc::new(Mock::default());
    let app = with_store(broken_client().await, &store);
    let got = post(&app, t, &format!("name={}", enc(name)), b"zip").await;
    assert_eq!(got.status, 500);
    assert!(store.objects.lock().unwrap().is_empty());
    let logs = app.logs();
    assert_eq!(logs.len(), 1);
    assert!(logs[0].starts_with("tenko: filing archive put failed: db ("));
    assert_logs_clean(&app, &[t.to_string(), name.to_owned()]);
}
