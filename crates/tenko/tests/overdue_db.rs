//! 予定超過の確認 (W7c) を実 DB で通す (rust-alc-api の alc-tenko の overdue.rs の要点と、印を先に付ける形)。
//! 接続は本番の worker と同じ実行用ロール `alc_api_rt` (RLS が効く)。

mod support;

use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use futures_util::future::BoxFuture;
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use support::{
    admin, broken_client, client_as, employee, notify_group, notify_member, rt_client, tenant,
    FakeMailer, READ_ONLY_ROLE,
};
use tenko::deliver::{HttpResult, Sender, Sleeper, WebhookHttp};
use tenko::mail::{SharedMailer, GROUP};
use tenko::overdue::run;
use uuid::Uuid;

#[derive(Default)]
struct FakeHttp {
    /// 返す応答 (このテナントに送ったときだけ順に使う。ほかのテナントと空になった後は 200)
    replies: StdMutex<Vec<HttpResult>>,
    replies_for: StdMutex<Option<Value>>,
    sent: StdMutex<Vec<Value>>,
}

impl WebhookHttp for FakeHttp {
    fn post<'a>(
        &'a self,
        _url: &'a str,
        _headers: Vec<(&'static str, String)>,
        body: String,
    ) -> BoxFuture<'a, HttpResult> {
        let payload: Value = serde_json::from_str(&body).unwrap();
        let mine = self.replies_for.lock().unwrap().as_ref() == Some(&payload["tenant_id"]);
        self.sent.lock().unwrap().push(payload);
        let mut replies = self.replies.lock().unwrap();
        let reply = if mine && !replies.is_empty() {
            replies.remove(0)
        } else {
            Ok((200, "ok".into()))
        };
        Box::pin(async move { reply })
    }
}

struct NoSleep;

impl Sleeper for NoSleep {
    fn sleep(&self, _secs: u64) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// `run` は全テナントを回るので、このファイルのテストは 1 本ずつ流す (並行に流すと、ほかのテストの `run` が
/// 自分のテナントの予定を先に取ってしまう)。送ったものはテナントで絞って数える (前の回の残りの行も送られるため)
fn serial() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

impl FakeHttp {
    fn sent_for(&self, t: Uuid) -> Vec<Value> {
        let t = json!(t);
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p["tenant_id"] == t)
            .cloned()
            .collect()
    }
}

fn sender(http: Arc<FakeHttp>) -> Sender {
    Sender {
        http,
        sleeper: Arc::new(NoSleep),
    }
}

type Logs = Arc<StdMutex<Vec<String>>>;

fn mailer(m: &Arc<FakeMailer>) -> Option<SharedMailer> {
    Some(m.clone())
}

/// `run` は全テナント (ほかのテストの binary が作った超過の予定も) を回るので、ほかのテナントのメールの見送りの
/// ログが混ざりうる。それ以外のログが無いこと
fn only_mail_skips(logs: &Logs) -> bool {
    logs.lock()
        .unwrap()
        .iter()
        .all(|l| l.starts_with("tenko: anomaly mail skipped: no "))
}

/// グループ「点呼の異常」に宛先を 1 人入れ、その email を返す
async fn mail_to(t: Uuid) -> String {
    let g = notify_group(t, GROUP).await;
    let to = format!("kanri-{t}@example.invalid");
    notify_member(t, g, Some(&to), true).await;
    to
}

fn sink(logs: &Logs) -> tenko::LogSink {
    let l = logs.clone();
    Arc::new(move |m: &str| l.lock().unwrap().push(m.to_owned()))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn config(t: Uuid, enabled: bool) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.webhook_configs (tenant_id, event_type, url, enabled) \
             VALUES ($1, 'tenko_overdue', 'https://hooks.example.invalid/x', $2) RETURNING id",
            &[&t, &enabled],
        )
        .await
        .unwrap()
        .get(0)
}

/// 予定 1 件。`ago` は予定の時刻が今からどれだけ前か (SQL の interval)
async fn schedule(
    t: Uuid,
    emp: Uuid,
    ago: &str,
    consumed: bool,
    notified: bool,
    is_dev: bool,
) -> Uuid {
    admin()
        .await
        .query_one(
            &format!(
                "INSERT INTO alc_api.tenko_schedules \
                 (tenant_id, employee_id, tenko_type, responsible_manager_name, scheduled_at, consumed, overdue_notified_at, is_dev) \
                 VALUES ($1, $2, 'post_operation', '運行 管理者', now() - interval '{ago}', $3, \
                         CASE WHEN $4 THEN now() - interval '1 minute' END, $5) RETURNING id"
            ),
            &[&t, &emp, &consumed, &notified, &is_dev],
        )
        .await
        .unwrap()
        .get(0)
}

async fn notified(id: Uuid) -> bool {
    admin()
        .await
        .query_one(
            "SELECT overdue_notified_at IS NOT NULL FROM alc_api.tenko_schedules WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0)
}

async fn deliveries(config: Uuid) -> Vec<(i32, bool)> {
    admin()
        .await
        .query(
            "SELECT attempt, success FROM alc_api.webhook_deliveries WHERE config_id = $1 ORDER BY attempt",
            &[&config],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// 超過した予定だけに印を付けて送る。未超過・消費済み・通知済み・dev の行は送らない。webhook の設定の無い / 無効な
/// テナントにも印は付け、webhook は送らずメールだけ送る (Refs ippoan/alc-app#676)。
/// 2 回目は何も送らない (印を先に付けるので、webhook もメールも 1 回だけ)
#[tokio::test]
async fn overdue_schedules_are_claimed_and_sent_once() {
    let _serial = serial().lock().await;
    let t = tenant().await;
    let no_config = tenant().await;
    let disabled = tenant().await;
    let cfg = config(t, true).await;
    config(disabled, false).await;
    let emp = employee(t, "超過 一郎").await;
    let late = schedule(t, emp, "2 hours", false, false, false).await;
    let soon = schedule(t, emp, "30 minutes", false, false, false).await;
    let done = schedule(t, emp, "3 hours", true, false, false).await;
    let already = schedule(t, emp, "4 hours", false, true, false).await;
    let dev = schedule(t, emp, "2 hours", false, false, true).await;
    let e2 = employee(no_config, "設定 無し").await;
    let other = schedule(no_config, e2, "2 hours", false, false, false).await;
    let e3 = employee(disabled, "無効 設定").await;
    let off = schedule(disabled, e3, "2 hours", false, false, false).await;
    let to_t = mail_to(t).await;
    let to_no_config = mail_to(no_config).await;

    let pg = Arc::new(Mutex::new(rt_client().await));
    let http = Arc::new(FakeHttp::default());
    let mails = Arc::new(FakeMailer::default());
    let logs: Logs = Arc::default();
    let now = now_ms();
    run(
        pg.clone(),
        sender(http.clone()),
        mailer(&mails),
        sink(&logs),
        now,
    )
    .await;

    let sent = http.sent_for(t);
    assert_eq!(sent.len(), 1);
    let p = &sent[0];
    assert_eq!(p["event"], "tenko_overdue");
    assert_eq!(p["tenant_id"], json!(t));
    assert_eq!(p["data"]["schedule_id"], json!(late));
    assert_eq!(p["data"]["employee_id"], json!(emp));
    assert_eq!(p["data"]["employee_name"], "超過 一郎");
    assert_eq!(p["data"]["responsible_manager_name"], "運行 管理者");
    assert_eq!(p["data"]["tenko_type"], "post_operation");
    // 未消費の予定は session_id・session_status が null
    assert_eq!(p["data"]["session_id"], json!(null));
    assert_eq!(p["data"]["session_status"], json!(null));
    let minutes = p["data"]["minutes_overdue"].as_i64().unwrap();
    assert!((119..=121).contains(&minutes), "{minutes}");
    assert!(notified(late).await);
    for id in [soon, done, dev] {
        assert!(!notified(id).await);
    }
    for id in [already, other, off] {
        assert!(notified(id).await);
    }
    assert_eq!(deliveries(cfg).await, vec![(1, true)]);
    // webhook の設定が無い・無効なテナントには webhook を送らない
    assert!(http.sent_for(no_config).is_empty());
    assert!(http.sent_for(disabled).is_empty());
    // メールは超過した予定ごとに 1 通 (設定の無いテナントにも)。グループの無いテナント (disabled) には送らない
    for to in [&to_t, &to_no_config] {
        let sent = mails.sent_to(to);
        assert_eq!(sent.len(), 1, "{to}");
        assert_eq!(
            sent[0].subject,
            "【点呼の異常】点呼が予定の時刻を過ぎても完了していません"
        );
    }
    assert!(mails.sent_to(&to_t)[0].body.contains("乗務員: 超過 一郎\n"));
    assert!(mails.sent_to(&to_t)[0]
        .body
        .contains("点呼の種類: 乗務後\n"));
    assert!(mails.sent_to(&to_t)[0]
        .body
        .contains("運行管理者: 運行 管理者\n"));

    run(pg, sender(http.clone()), mailer(&mails), sink(&logs), now).await;
    assert_eq!(http.sent_for(t).len(), 1);
    assert_eq!(mails.sent_to(&to_t).len(), 1);
    assert_eq!(mails.sent_to(&to_no_config).len(), 1);
    assert!(only_mail_skips(&logs));
    assert!(logs
        .lock()
        .unwrap()
        .iter()
        .any(|l| l == "tenko: anomaly mail skipped: no group"));
}

/// 送るのに失敗しても印は残る (rust と同じく次の回に送り直さない)。毎回の失敗は配信の記録に残る
#[tokio::test]
async fn failed_delivery_is_recorded_and_not_resent() {
    let _serial = serial().lock().await;
    let t = tenant().await;
    let cfg = config(t, true).await;
    let emp = employee(t, "失敗 二郎").await;
    let late = schedule(t, emp, "90 minutes", false, false, false).await;
    let pg = Arc::new(Mutex::new(rt_client().await));
    let http = Arc::new(FakeHttp::default());
    *http.replies_for.lock().unwrap() = Some(json!(t));
    *http.replies.lock().unwrap() = vec![
        Ok((500, "busy".into())),
        Err("timeout".into()),
        Ok((503, "down".into())),
    ];
    let logs: Logs = Arc::default();
    let mails = Arc::new(FakeMailer::default());
    run(
        pg.clone(),
        sender(http.clone()),
        mailer(&mails),
        sink(&logs),
        now_ms(),
    )
    .await;
    assert_eq!(http.sent_for(t).len(), 3);
    assert_eq!(
        deliveries(cfg).await,
        vec![(1, false), (2, false), (3, false)]
    );
    assert!(notified(late).await);
    run(
        pg,
        sender(http.clone()),
        mailer(&mails),
        sink(&logs),
        now_ms(),
    )
    .await;
    assert_eq!(http.sent_for(t).len(), 3);
}

/// テナントを引けない → 固定の語でログに出して終わる。印を付けられない (更新の権限が無い) → ログに出して次のテナントへ
#[tokio::test]
async fn db_failures_are_logged() {
    let _serial = serial().lock().await;
    let t = tenant().await;
    config(t, true).await;
    let emp = employee(t, "権限 三郎").await;
    let late = schedule(t, emp, "2 hours", false, false, false).await;

    let logs: Logs = Arc::default();
    let http = Arc::new(FakeHttp::default());
    let mails = Arc::new(FakeMailer::default());
    let broken = Arc::new(Mutex::new(broken_client().await));
    run(
        broken,
        sender(http.clone()),
        mailer(&mails),
        sink(&logs),
        now_ms(),
    )
    .await;
    assert!(logs.lock().unwrap()[0].starts_with("tenko: overdue tenants failed: db ("));

    // 読み取り専用のロール: 関数は呼べる (このテストで付ける) が、予定を更新できない
    admin()
        .await
        .batch_execute(&format!(
            "GRANT EXECUTE ON FUNCTION alc_api.list_tenko_overdue_tenants() TO {READ_ONLY_ROLE}"
        ))
        .await
        .unwrap();
    let ro = Arc::new(Mutex::new(client_as(READ_ONLY_ROLE).await));
    run(
        ro,
        sender(http.clone()),
        mailer(&mails),
        sink(&logs),
        now_ms(),
    )
    .await;
    let logs = logs.lock().unwrap().clone();
    assert!(logs
        .iter()
        .any(|l| l.starts_with("tenko: overdue claim failed: db (")));
    assert!(http.sent.lock().unwrap().is_empty());
    assert!(mails.sent.lock().unwrap().is_empty());
    assert!(!notified(late).await);
}

/// 消費したセッションを持つ予定 (`session_status` が None なら未消費)。予定の時刻は `ago` 前
async fn schedule_with_session(t: Uuid, emp: Uuid, ago: &str, status: &str) -> (Uuid, Uuid) {
    let session = support::session(t, emp, "自動点呼", status, false, None).await;
    let id = schedule(t, emp, ago, true, false, false).await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.tenko_schedules SET consumed_by_session_id = $2 WHERE id = $1",
            &[&id, &session],
        )
        .await
        .unwrap();
    (id, session)
}

/// 分数はテナントの設定 (行が無ければ 60 分)。超過とするのは「時刻 + 分数 を過ぎても点呼が完了していない」予定:
/// 未消費、または消費したセッションが completed・cancelled 以外 (中断・判定待ち・途中)。payload にそのセッションを載せる
#[tokio::test]
async fn minutes_are_per_tenant_and_unfinished_sessions_count() {
    let _serial = serial().lock().await;
    let short = tenant().await;
    let long = tenant().await;
    let default = tenant().await;
    for t in [short, long, default] {
        config(t, true).await;
    }
    let su = admin().await;
    su.execute(
        "INSERT INTO alc_api.tenko_settings (tenant_id, overdue_minutes) VALUES ($1, 10), ($2, 180)",
        &[&short, &long],
    )
    .await
    .unwrap();

    // 10 分の設定: 20 分前は超過、5 分前は超過でない
    let e1 = employee(short, "短い 一郎").await;
    let short_late = schedule(short, e1, "20 minutes", false, false, false).await;
    let short_soon = schedule(short, e1, "5 minutes", false, false, false).await;
    // 180 分の設定: 2 時間前 (60 分の既定なら超過) は超過でない
    let e2 = employee(long, "長い 二郎").await;
    let long_wait = schedule(long, e2, "2 hours", false, false, false).await;
    let long_late = schedule(long, e2, "4 hours", false, false, false).await;
    // 設定の行が無い: 既定の 60 分
    let e3 = employee(default, "既定 三郎").await;
    let default_late = schedule(default, e3, "90 minutes", false, false, false).await;
    let default_soon = schedule(default, e3, "50 minutes", false, false, false).await;
    // 消費済み: 完了・中止は超過でない。中断・判定待ち・途中は超過
    let (done, _) = schedule_with_session(default, e3, "3 hours", "completed").await;
    let (cancelled, _) = schedule_with_session(default, e3, "3 hours", "cancelled").await;
    let (interrupted, s_int) = schedule_with_session(default, e3, "3 hours", "interrupted").await;
    let (pending, _) = schedule_with_session(default, e3, "3 hours", "judgment_pending").await;
    let (running, _) = schedule_with_session(default, e3, "3 hours", "identity_verified").await;
    // 消費済みで 60 分を過ぎていても、まだ分数の内なら送らない
    let (young, _) = schedule_with_session(default, e3, "20 minutes", "interrupted").await;
    // 消費済みだがセッションの印が無い予定は数えない
    let orphan = schedule(default, e3, "3 hours", true, false, false).await;

    let pg = Arc::new(Mutex::new(rt_client().await));
    let http = Arc::new(FakeHttp::default());
    let logs: Logs = Arc::default();
    let mails = Arc::new(FakeMailer::default());
    run(
        pg,
        sender(http.clone()),
        mailer(&mails),
        sink(&logs),
        now_ms(),
    )
    .await;

    for id in [
        short_late,
        long_late,
        default_late,
        interrupted,
        pending,
        running,
    ] {
        assert!(notified(id).await, "{id}");
    }
    for id in [
        short_soon,
        long_wait,
        default_soon,
        done,
        cancelled,
        young,
        orphan,
    ] {
        assert!(!notified(id).await, "{id}");
    }
    assert_eq!(http.sent_for(short).len(), 1);
    assert_eq!(http.sent_for(long).len(), 1);
    assert_eq!(http.sent_for(default).len(), 4);

    // 消費済みの予定の payload には、そのセッションの id と status が載る
    let sent = http.sent_for(default);
    let p = sent
        .iter()
        .find(|p| p["data"]["schedule_id"] == json!(interrupted))
        .unwrap();
    assert_eq!(p["data"]["session_id"], json!(s_int));
    assert_eq!(p["data"]["session_status"], "interrupted");
    let statuses: Vec<_> = sent
        .iter()
        .filter(|p| p["data"]["schedule_id"] != json!(default_late))
        .map(|p| p["data"]["session_status"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(statuses.len(), 3);
    for s in ["interrupted", "judgment_pending", "identity_verified"] {
        assert!(statuses.iter().any(|x| x == s), "{s}");
    }
    assert!(only_mail_skips(&logs));
}
