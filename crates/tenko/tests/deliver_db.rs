//! webhook の送信 ([`tenko::deliver`]) を実 DB と偽物の HTTP で通す (署名・再送・配信の記録・DB の失敗)。
//! 送る中身は、この worker の予定超過の確認と、測定の口 (ippoan/alc-measurements-worker) が内部の口
//! `/internal/webhooks/fire` に渡す点呼の節目 (Refs ippoan/rust-alc-api#680)。
//! tests/sessions_db.rs (点呼のセッションの口。alc-measurements-worker へ移した) から分けた。

mod support;

use std::sync::{Arc, Mutex as StdMutex};

use futures_util::future::BoxFuture;
use serde_json::json;
use support::{admin, broken_client, client_as, tenant, READ_ONLY_ROLE};
use tenko::deliver::{deliver_all, Event, HttpResult, Sender, Sleeper, WebhookHttp};
use uuid::Uuid;

/// 送ったもの (URL・ヘッダー・本文)
type Sent = (String, Vec<(&'static str, String)>, String);

#[derive(Default)]
struct FakeHttp {
    replies: StdMutex<Vec<HttpResult>>,
    sent: StdMutex<Vec<Sent>>,
}

impl WebhookHttp for FakeHttp {
    fn post<'a>(
        &'a self,
        url: &'a str,
        headers: Vec<(&'static str, String)>,
        body: String,
    ) -> BoxFuture<'a, HttpResult> {
        self.sent
            .lock()
            .unwrap()
            .push((url.to_owned(), headers, body));
        let reply = self.replies.lock().unwrap().remove(0);
        Box::pin(async move { reply })
    }
}

#[derive(Default)]
struct FakeSleeper(StdMutex<Vec<u64>>);

impl Sleeper for FakeSleeper {
    fn sleep(&self, secs: u64) -> BoxFuture<'_, ()> {
        self.0.lock().unwrap().push(secs);
        Box::pin(async {})
    }
}

async fn config(t: Uuid, event_type: &str, secret: Option<&str>, enabled: bool) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.webhook_configs (tenant_id, event_type, url, secret, enabled) \
             VALUES ($1, $2, 'https://hooks.example.invalid/x', $3, $4) RETURNING id",
            &[&t, &event_type, &secret, &enabled],
        )
        .await
        .unwrap()
        .get(0)
}

async fn deliveries(config: Uuid) -> Vec<(i32, Option<i32>, Option<String>, bool, bool)> {
    admin()
        .await
        .query(
            "SELECT attempt, status_code, response_body, success, delivered_at IS NOT NULL \
             FROM alc_api.webhook_deliveries WHERE config_id = $1 ORDER BY attempt",
            &[&config],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect()
}

fn sender(http: Arc<FakeHttp>, sleeper: Arc<FakeSleeper>) -> Sender {
    Sender { http, sleeper }
}

/// 送り方は rust と同じ: 設定が無い・無効なら送らない、署名を付ける、失敗は 1 秒・5 秒待って 3 回まで、毎回記録する
#[tokio::test]
async fn webhook_delivery_retries_and_records() {
    let app = support::app().await;
    let t = tenant().await;
    let signed = config(t, "alcohol_detected", Some("s3cret"), true).await;
    let plain = config(t, "tenko_interrupted", None, true).await;
    config(t, "inspection_ng", None, false).await;
    let http = Arc::new(FakeHttp::default());
    *http.replies.lock().unwrap() = vec![
        Ok((500, "busy".into())),
        Err("timeout".into()),
        Ok((204, String::new())),
        Ok((503, "down".into())),
        Ok((503, "down".into())),
        Ok((503, "down".into())),
    ];
    let sleeper = Arc::new(FakeSleeper::default());
    let payload = json!({ "event": "alcohol_detected", "n": 1 });
    let events = vec![
        (t, Event::new("alcohol_detected", payload.clone())),
        (t, Event::new("inspection_ng", json!({}))),
        (t, Event::new("tenko_overdue", json!({}))),
        (t, Event::new("tenko_interrupted", json!({ "n": 2 }))),
    ];
    let logs = Arc::new(StdMutex::new(Vec::<String>::new()));
    let sink = logs.clone();
    deliver_all(
        app.pg.clone(),
        sender(http.clone(), sleeper.clone()),
        Arc::new(move |m: &str| sink.lock().unwrap().push(m.to_owned())),
        events,
    )
    .await;

    let sent = http.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 6);
    let (url, headers, body) = &sent[0];
    assert_eq!(url, "https://hooks.example.invalid/x");
    assert_eq!(body, &payload.to_string());
    assert!(headers.contains(&("X-Webhook-Event", "alcohol_detected".to_owned())));
    assert!(headers.contains(&(
        "X-Webhook-Signature",
        tenko::deliver::signature("s3cret", body)
    )));
    assert!(!sent[3].1.iter().any(|(k, _)| *k == "X-Webhook-Signature"));
    assert_eq!(
        deliveries(signed).await,
        vec![
            (1, Some(500), Some("busy".into()), false, false),
            (2, None, Some("timeout".into()), false, false),
            (3, Some(204), Some(String::new()), true, true),
        ]
    );
    assert_eq!(deliveries(plain).await.len(), 3);
    assert_eq!(*sleeper.0.lock().unwrap(), vec![1, 5, 1, 5]);
    assert_eq!(
        *logs.lock().unwrap(),
        vec!["tenko: webhook delivery failed: timeout".to_owned()]
    );
}

/// 設定を読めない・記録を書けないときは固定の語でログに出し、止めない
#[tokio::test]
async fn webhook_delivery_db_failures_are_logged() {
    let t = tenant().await;
    config(t, "alcohol_detected", None, true).await;
    let logs = Arc::new(StdMutex::new(Vec::<String>::new()));
    let sink = logs.clone();
    let log: tenko::LogSink = Arc::new(move |m: &str| sink.lock().unwrap().push(m.to_owned()));
    let ev = || vec![(t, Event::new("alcohol_detected", json!({})))];

    let broken = Arc::new(futures_util::lock::Mutex::new(broken_client().await));
    let http = Arc::new(FakeHttp::default());
    deliver_all(
        broken,
        sender(http.clone(), Arc::new(FakeSleeper::default())),
        log.clone(),
        ev(),
    )
    .await;
    assert!(http.sent.lock().unwrap().is_empty());

    // 読み取り専用のロール: 設定は読めて送るが、記録は書けない
    let ro = Arc::new(futures_util::lock::Mutex::new(
        client_as(READ_ONLY_ROLE).await,
    ));
    *http.replies.lock().unwrap() = vec![Ok((200, "ok".into()))];
    deliver_all(
        ro,
        sender(http.clone(), Arc::new(FakeSleeper::default())),
        log,
        ev(),
    )
    .await;
    assert_eq!(http.sent.lock().unwrap().len(), 1);
    let logs = logs.lock().unwrap().clone();
    assert!(logs[0].starts_with("tenko: webhook config lookup failed: db ("));
    assert!(logs[1].starts_with("tenko: webhook record failed: db ("));
    assert_eq!(logs.len(), 2);
}
