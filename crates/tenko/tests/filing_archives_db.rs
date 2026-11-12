//! 点呼の届出の申請済みの控えの保管 (`/api/tenko/filing-archives`、到達番号がキー) を、モックの置き場で通す。値は架空。

mod support;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex as StdMutex};

use axum::body::Body;
use axum::http::{HeaderValue, Request};
use futures_util::future::BoxFuture;
use serde_json::json;
use support::{assert_logs_clean, build_with_files, rt_client, tenant, App, Got};
use tenko::filing_archives::{
    is_arrive_id, FileStore, SharedStore, StoreError, StoredFile, StoredObject, MAX_BYTES,
};
use tower::ServiceExt;
use uuid::Uuid;

const PATH: &str = "/api/tenko/filing-archives";
/// 架空の到達番号 (数字 17 桁 + 英小文字)
const ARRIVE: &str = "20991231235959001e";
/// 架空の到達番号 (数字 16 桁)
const ARRIVE2: &str = "2099123123595902";
/// zip の先頭を持つ架空の本文
const ZIP: &[u8] = b"PK\x03\x04 fake zip";
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

/// `id` と `query` (encode 済み、空なら付けない) で保存の口を admin で打つ
async fn put(app: &App, tenant: Uuid, id: &str, query: &str, body: &[u8]) -> Got {
    let uri = if query.is_empty() {
        format!("{PATH}/{id}")
    } else {
        format!("{PATH}/{id}?{query}")
    };
    let ct = [("Content-Type", h("application/zip"))];
    send(app, "PUT", &uri, tenant, Some("admin"), &ct, body.to_vec()).await
}

async fn get(app: &App, tenant: Uuid, uri: &str) -> Got {
    send(app, "GET", uri, tenant, Some("admin"), &[], vec![]).await
}

fn with_store(pg: alc_worker_db::PgClient, store: &Arc<Mock>) -> App {
    build_with_files(pg, Some(store.clone() as SharedStore))
}

/// 本文の先頭を zip にし、全体を `len` バイトにする
fn zip_of(len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    v[..4].copy_from_slice(b"PK\x03\x04");
    v
}

#[tokio::test]
async fn archives_save_list_get() {
    let store = Arc::new(Mock::default());
    let app = with_store(rt_client().await, &store);
    let t = tenant().await;
    let other = tenant().await;
    let name = format!("{ARRIVE}.zip");
    let meta =
        json!({ "reportKind": "実施", "startDate": "2099-12-01", "offices": ["架空営業所"] });

    // 保存 (200)。id は到達番号、meta は JSON の値で返り、saved_by は利用者の ID
    let got = put(
        &app,
        t,
        ARRIVE,
        &format!("meta={}&name={}", enc(&meta.to_string()), enc(&name)),
        ZIP,
    )
    .await;
    assert_eq!(got.status, 200);
    let saved = got.json();
    assert_eq!(
        (
            saved["id"].clone(),
            saved["name"].clone(),
            saved["size"].clone(),
            saved["saved_at"].clone(),
            saved["meta"].clone(),
            saved["saved_by"].is_string(),
        ),
        (
            json!(ARRIVE),
            json!(name),
            json!(18),
            json!(NOW),
            meta.clone(),
            true
        )
    );
    // 置き場のキーと customMetadata
    let (bytes, m) = store.objects.lock().unwrap()[&format!("{t}/{ARRIVE}.zip")].clone();
    assert_eq!(bytes, ZIP);
    assert_eq!(
        (
            m["name"].as_str(),
            m["saved_at"].as_str(),
            m.get("saved_by").cloned()
        ),
        (
            name.as_str(),
            NOW,
            saved["saved_by"].as_str().map(str::to_owned)
        )
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&m["meta"]).unwrap(),
        meta
    );

    // 同じ到達番号は上書き。name も meta も無ければ name は `{到達番号}.zip`、meta は null
    let got = put(&app, t, ARRIVE, "", b"PK\x03\x04 newer").await;
    assert_eq!(
        (
            got.status,
            got.json()["name"].clone(),
            got.json()["meta"].clone()
        ),
        (200, json!(name), json!(null))
    );
    assert_eq!(
        store.objects.lock().unwrap()[&format!("{t}/{ARRIVE}.zip")].0,
        b"PK\x03\x04 newer"
    );
    let got = put(&app, t, ARRIVE2, "name=b.zip", ZIP).await;
    assert_eq!(got.status, 200);

    // 古い保管・customMetadata の無い保管・形の違うキー (一覧に出さない) を置き場に直に置く
    let old = "2000010100000001";
    store.insert(
        format!("{t}/{old}.zip"),
        b"old",
        &[
            ("name", "old.zip"),
            ("saved_at", "2020-01-01T00:00:00.000Z"),
            ("meta", "not json"),
        ],
    );
    let bare = "20000101000000002";
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
    let got = get(&app, t, PATH).await;
    assert_eq!(got.status, 200);
    let list = got.json();
    let ids: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, [ARRIVE, ARRIVE2, old, bare]);
    assert_eq!(
        list[3],
        json!({ "id": bare, "name": format!("{bare}.zip"), "size": 4, "saved_at": "", "saved_by": null, "meta": null })
    );
    assert_eq!(
        (list[2]["meta"].clone(), list[2]["saved_by"].clone()),
        (json!(null), json!(null))
    );

    // 取得は zip と元のファイル名
    let got = get(&app, t, &format!("{PATH}/{ARRIVE2}")).await;
    assert_eq!((got.status, got.body.as_slice()), (200, ZIP));
    assert_eq!(got.headers["content-type"], "application/zip");
    assert_eq!(
        got.headers["content-disposition"],
        format!("attachment; filename*=UTF-8''{}", attr("b.zip"))
    );
    // 名前の無い保管は `{到達番号}.zip`
    let got = get(&app, t, &format!("{PATH}/{bare}")).await;
    assert_eq!(
        got.headers["content-disposition"],
        format!("attachment; filename*=UTF-8''{bare}.zip")
    );

    // 別のテナントからは見えない
    let got = get(&app, other, PATH).await;
    assert_eq!(got.json().as_array().unwrap().len(), 1);
    let got = get(&app, other, &format!("{PATH}/{ARRIVE}")).await;
    assert_eq!(got.status, 404);
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn archives_reject_bad_requests() {
    let store = Arc::new(Mock::default());
    let app = with_store(rt_client().await, &store);
    let t = tenant().await;

    // 到達番号の形
    for (id, ok) in [
        (ARRIVE, true),
        (ARRIVE2, true),
        ("209912312359590", false),
        ("2099123123595900123", false),
        ("2099123123595900E", false),
        ("209912312359590ee", false),
        ("x", false),
        ("", false),
    ] {
        assert_eq!(is_arrive_id(id), ok, "{id}");
    }

    // admin 以外 (役の無い要求・admin でない役) は 3 口とも 403
    let one = format!("{PATH}/{ARRIVE}");
    for (method, uri, role) in [
        ("PUT", one.as_str(), None),
        ("PUT", one.as_str(), Some("viewer")),
        ("GET", PATH, Some("viewer")),
        ("GET", one.as_str(), Some("viewer")),
    ] {
        let got = send(&app, method, uri, t, role, &[], ZIP.to_vec()).await;
        assert_eq!(
            (got.status, got.json()),
            (403, json!({ "error": "forbidden" }))
        );
    }

    // 到達番号の形でなければ保存は 400、取得は 404
    for id in ["x", "209912312359590", "2099123123595900E"] {
        let got = put(&app, t, id, "", ZIP).await;
        assert_eq!(
            (got.status, got.json()),
            (400, json!({ "error": "invalid_arrive_id" }))
        );
        let got = get(&app, t, &format!("{PATH}/{id}")).await;
        assert_eq!(got.status, 404);
    }
    // 形は合うが無い到達番号は 404
    let got = get(&app, t, &format!("{PATH}/{ARRIVE2}")).await;
    assert_eq!(got.status, 404);

    // 名前: 空・201 文字・UTF-8 でない。ヘッダーでは受けない (付けても既定の名前)
    for query in [
        "name=".to_owned(),
        format!("name={}", enc(&"あ".repeat(201))),
        "name=%FF".to_owned(),
    ] {
        let got = put(&app, t, ARRIVE, &query, ZIP).await;
        assert_eq!(
            (got.status, got.json()),
            (400, json!({ "error": "invalid_name" }))
        );
    }
    let got = send(
        &app,
        "PUT",
        &format!("{PATH}/{ARRIVE}"),
        t,
        Some("admin"),
        &[("X-Filing-Name", h("a.zip"))],
        ZIP.to_vec(),
    )
    .await;
    assert_eq!(got.json()["name"], format!("{ARRIVE}.zip"));
    // `=` の無い name・似た名前のキーは無いのと同じ。200 文字は通る
    let got = put(&app, t, ARRIVE, "name&names=a.zip&filename=a.zip", ZIP).await;
    assert_eq!(got.json()["name"], format!("{ARRIVE}.zip"));
    let got = put(
        &app,
        t,
        ARRIVE,
        &format!("x=1&name={}", enc(&"あ".repeat(200))),
        ZIP,
    )
    .await;
    assert_eq!(got.status, 200);
    // `+` は空白にしない (encodeURIComponent は空白を %20 にする)
    let got = put(&app, t, ARRIVE, "name=a+b%20c.zip", ZIP).await;
    assert_eq!(got.json()["name"], "a+b c.zip");

    // 要約: JSON でない・object でない・4000 文字を超える・UTF-8 でない
    let long = json!({ "x": "a".repeat(4000) }).to_string();
    for meta in [enc("{"), enc("[1]"), enc(&long), "%FF".to_owned()] {
        let got = put(&app, t, ARRIVE, &format!("meta={meta}"), ZIP).await;
        assert_eq!(
            (got.status, got.json()),
            (400, json!({ "error": "invalid_meta" }))
        );
    }

    // 本文: 空・zip でないは 400、20 MiB を超えると 413 (ちょうど 20 MiB は通る)
    let got = put(&app, t, ARRIVE, "", b"").await;
    assert_eq!((got.status, got.json()), (400, json!({ "error": "empty" })));
    for body in [&b"PK"[..], b"not a zip at all", b"%PDF-1.7"] {
        let got = put(&app, t, ARRIVE, "", body).await;
        assert_eq!(
            (got.status, got.json()),
            (400, json!({ "error": "not_zip" }))
        );
    }
    let got = put(&app, t, ARRIVE, "", &zip_of(MAX_BYTES + 1)).await;
    assert_eq!(
        (got.status, got.json()),
        (413, json!({ "error": "too_large" }))
    );
    let got = put(&app, t, ARRIVE, "", &zip_of(MAX_BYTES)).await;
    assert_eq!(
        (got.status, got.json()["size"].clone()),
        (200, json!(MAX_BYTES))
    );
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn archives_unconfigured_store_is_503() {
    let app = build_with_files(rt_client().await, None);
    let t = tenant().await;
    let one = format!("{PATH}/{ARRIVE}");
    for (method, uri) in [("PUT", one.as_str()), ("GET", PATH), ("GET", one.as_str())] {
        let got = send(&app, method, uri, t, Some("admin"), &[], ZIP.to_vec()).await;
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
    let got = get(&app, t, "/api/tenko/filing-settings").await;
    assert_eq!(got.status, 200);
}

#[tokio::test]
async fn archives_store_failures_are_500() {
    let t = tenant().await;
    let name = "架空の控え.zip";
    let failing = Arc::new(Mock {
        fail: true,
        ..Mock::default()
    });
    let app = with_store(rt_client().await, &failing);
    let one = format!("{PATH}/{ARRIVE}");
    let put_uri = format!("{one}?name={}", enc(name));
    for (method, uri) in [
        ("PUT", put_uri.as_str()),
        ("GET", PATH),
        ("GET", one.as_str()),
    ] {
        let got = send(&app, method, uri, t, Some("admin"), &[], ZIP.to_vec()).await;
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
    assert_logs_clean(&app, &[t.to_string(), name.to_owned(), ARRIVE.to_owned()]);
}
