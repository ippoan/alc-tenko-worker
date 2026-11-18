//! IT点呼 の判定待ちのメール (Refs ippoan/alc-app-s3#352) を実 DB で通す: 毎朝のまとめ (`it_pending::run`) と、
//! 測定の口が渡す着信 (`/internal/webhooks/fire` の `it_tenko_pending`)。接続は本番の worker と同じ実行用ロール
//! `alc_api_rt` (RLS が効く)。送り手は偽物。

mod support;

use std::sync::{Arc, Mutex as StdMutex};

use futures_util::future::BoxFuture;
use futures_util::lock::Mutex;
use serde_json::json;
use support::{
    admin, broken_client, call, call_with, employee, function_only_client, notify_group,
    notify_member, rt_client, session, tenant, FakeMailer,
};
use tenko::deliver::{deliver_all, HttpResult, Sender, Sleeper, WebhookHttp};
use tenko::it_pending::{run, tenants};
use tenko::mail::{notify_all, SharedMailer, GROUP};
use uuid::Uuid;

type Logs = Arc<StdMutex<Vec<String>>>;

fn sink(logs: &Logs) -> tenko::LogSink {
    let l = logs.clone();
    Arc::new(move |m: &str| l.lock().unwrap().push(m.to_owned()))
}

fn shared(m: &Arc<FakeMailer>) -> SharedMailer {
    m.clone()
}

fn addr(t: Uuid, who: &str) -> String {
    format!("{who}-{t}@example.invalid")
}

/// 開始の時刻と拠点を入れる (superuser で)
async fn set_start(id: Uuid, started_at: &str, location: Option<&str>) {
    admin()
        .await
        .execute(
            "UPDATE alc_api.tenko_sessions SET started_at = $2::text::timestamptz, location = $3 WHERE id = $1",
            &[&id, &started_at, &location],
        )
        .await
        .unwrap();
}

/// 判定待ち (IT点呼 で判定が空。status は問わない) を、テナントごとに 1 通にまとめて宛先ごとに送る。
/// 判定済み・ほかの点呼方法・dev端末の行は載せない。判定待ちの無いテナントには送らない
#[tokio::test]
async fn pending_it_tenko_is_mailed_once_per_tenant() {
    let t = tenant().await;
    let g = notify_group(t, GROUP).await;
    let (a, b) = (addr(t, "a"), addr(t, "b"));
    notify_member(t, g, Some(&a), true).await;
    notify_member(t, g, Some(&b), true).await;
    let emp = employee(t, "判定 待子").await;
    let newer = session(t, emp, "IT点呼", "judgment_pending", false, None).await;
    set_start(newer, "2026-11-17T00:30:00Z", Some("第一営業所")).await;
    let older = session(t, emp, "IT点呼", "cancelled", false, None).await;
    set_start(older, "2026-11-13T00:12:00Z", None).await;
    // 載せないもの: 判定済み・通常点呼・dev端末の行
    let judged = session(
        t,
        emp,
        "IT点呼",
        "completed",
        false,
        Some(("ok", "", "it", emp)),
    )
    .await;
    set_start(judged, "2026-11-12T00:00:00Z", None).await;
    session(t, emp, "通常点呼", "judgment_pending", false, None).await;
    session(t, emp, "IT点呼", "judgment_pending", true, None).await;

    // 判定済みしか無いテナント (グループ・宛先は在る) と、判定待ちが dev だけのテナント
    let done = tenant().await;
    let dg = notify_group(done, GROUP).await;
    notify_member(done, dg, Some(&addr(done, "d")), true).await;
    let demp = employee(done, "済み 一郎").await;
    session(
        done,
        demp,
        "IT点呼",
        "completed",
        false,
        Some(("ng", "体調", "it", demp)),
    )
    .await;
    session(done, demp, "IT点呼", "judgment_pending", true, None).await;

    let mut pg = rt_client().await;
    let listed = tenants(&mut pg).await.unwrap();
    assert!(listed.contains(&t));
    assert!(!listed.contains(&done));

    let mailer = Arc::new(FakeMailer::default());
    let logs: Logs = Arc::default();
    let (sent, failed) = run(Arc::new(Mutex::new(pg)), Some(shared(&mailer)), sink(&logs)).await;
    // ほかのテストのテナントの分も数えうるので下限だけ
    assert!(sent >= 2);
    assert_eq!(failed, 0);
    for to in [&a, &b] {
        let mails = mailer.sent_to(to);
        assert_eq!(mails.len(), 1);
        assert_eq!(
            mails[0].subject,
            "【点呼の異常】IT点呼 の判定待ちが 2 件あります"
        );
        assert_eq!(
            mails[0].body,
            "IT点呼 の判定待ちが 2 件あります。運行管理者の判定を付けてください。\n\n\
             1. 開始: 2026-11-13 09:12 (日本時間) / 拠点: 不明 / 種類: 乗務前 / 乗務員: 判定 待子 / 状態: 中止\n\
             2. 開始: 2026-11-17 09:30 (日本時間) / 拠点: 第一営業所 / 種類: 乗務前 / 乗務員: 判定 待子 / 状態: 判定待ち\n\n\
             このメールは自動点呼の記録から送っています。\n"
        );
    }
    assert!(mailer.sent_to(&addr(done, "d")).is_empty());
    // ログは固定の語だけ (宛先・名前・tenant_id を出さない)
    for l in logs.lock().unwrap().iter() {
        assert!(l.starts_with("tenko: "), "{l}");
        for secret in [
            t.to_string(),
            done.to_string(),
            a.clone(),
            "判定 待子".into(),
        ] {
            assert!(!l.contains(&secret), "{l}");
        }
    }
}

/// 失敗はログ 1 行 (固定の語) で、次へ進む: 横断の引きが失敗 / テナントの読みが失敗 / 送り手が無い
#[tokio::test]
async fn failures_are_logged() {
    let t = tenant().await;
    let emp = employee(t, "失敗 二郎").await;
    session(t, emp, "IT点呼", "judgment_pending", false, None).await;

    let logs: Logs = Arc::default();
    let mailer = Arc::new(FakeMailer::default());
    let broken = Arc::new(Mutex::new(broken_client().await));
    assert_eq!(
        run(broken, Some(shared(&mailer)), sink(&logs)).await,
        (0, 0)
    );
    assert_eq!(logs.lock().unwrap().len(), 1);
    assert!(logs.lock().unwrap()[0].starts_with("tenko: it pending tenants failed: db ("));

    // 関数は呼べるが表を読めないロール: テナントごとの読みが失敗する
    let logs: Logs = Arc::default();
    let fn_only = function_only_client(
        "tenko_test_it_pending_fn_only",
        "list_it_tenko_pending_tenants()",
    )
    .await;
    assert_eq!(
        run(
            Arc::new(Mutex::new(fn_only)),
            Some(shared(&mailer)),
            sink(&logs)
        )
        .await,
        (0, 0)
    );
    let got = logs.lock().unwrap().clone();
    assert!(!got.is_empty());
    assert!(got
        .iter()
        .all(|l| l.starts_with("tenko: it pending read failed: db (")));
    assert!(mailer.sent.lock().unwrap().is_empty());

    // 送り手が無い
    let logs: Logs = Arc::default();
    run(Arc::new(Mutex::new(rt_client().await)), None, sink(&logs)).await;
    assert!(logs
        .lock()
        .unwrap()
        .iter()
        .any(|l| l == "tenko: anomaly mail skipped: no mailer"));
}

/// webhook の外向きの HTTP の偽物 (呼ばれたら控える)
#[derive(Default)]
struct FakeHttp {
    sent: StdMutex<Vec<String>>,
}

impl WebhookHttp for FakeHttp {
    fn post<'a>(
        &'a self,
        _url: &'a str,
        _headers: Vec<(&'static str, String)>,
        body: String,
    ) -> BoxFuture<'a, HttpResult> {
        self.sent.lock().unwrap().push(body);
        Box::pin(async { Ok((200, "ok".into())) })
    }
}

struct NoSleep;

impl Sleeper for NoSleep {
    fn sleep(&self, _secs: u64) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// 測定の口が渡す IT点呼 の着信は、グループの宛先へ 1 人 1 通のメールだけ (webhook は送らない・設定を引きに行かない)。
/// dev端末の要求は積まない
#[tokio::test]
async fn fired_it_tenko_pending_is_mailed_not_webhooked() {
    let app = support::app().await;
    let t = tenant().await;
    let g = notify_group(t, GROUP).await;
    let a = addr(t, "a");
    notify_member(t, g, Some(&a), true).await;

    let body = Some(json!({
        "event_type": "it_tenko_pending",
        "payload": {
            "event": "it_tenko_pending",
            "timestamp": "2026-11-17T23:05:00Z",
            "tenant_id": t,
            "data": {
                "employee_name": "着信 四郎",
                "tenko_type": "normal",
                "started_at": "2026-11-17T23:00:00Z",
                "location": "第一営業所",
            }
        }
    }));
    let uri = "/internal/webhooks/fire";
    assert_eq!(
        call(&app, "POST", uri, Some(t), body.clone()).await.status,
        202
    );
    let dev = call_with(&app, "POST", uri, Some(t), &[("X-Device-Dev", "1")], body).await;
    assert_eq!(dev.status, 202);

    let events = app.outbox.take();
    assert_eq!(events.len(), 1);
    let mailer = Arc::new(FakeMailer::default());
    let got = notify_all(&app.pg, Some(&shared(&mailer)), &sink(&app.logs), &events).await;
    assert_eq!(got, (1, 0));
    let mails = mailer.sent_to(&a);
    assert_eq!(
        mails[0].subject,
        "【点呼の異常】IT点呼 の着信がありました (運行管理者の判定待ち)"
    );
    assert!(mails[0].body.contains("乗務員: 着信 四郎\n"));
    assert!(mails[0]
        .body
        .contains("開始: 2026-11-18 08:00 (日本時間)\n"));
    assert!(mails[0].body.contains("拠点: 第一営業所\n"));

    // webhook は送らない: 切れた接続を渡しても設定を引きに行かない (ログが出ない)・HTTP も呼ばない
    let http = Arc::new(FakeHttp::default());
    let logs: Logs = Arc::default();
    deliver_all(
        Arc::new(Mutex::new(broken_client().await)),
        Sender {
            http: http.clone(),
            sleeper: Arc::new(NoSleep),
        },
        sink(&logs),
        events,
    )
    .await;
    assert!(http.sent.lock().unwrap().is_empty());
    assert!(logs.lock().unwrap().is_empty());
    assert!(app.logs().is_empty());
}
