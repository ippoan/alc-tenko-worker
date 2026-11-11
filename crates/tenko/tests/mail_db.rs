//! 点呼の異常のメール (Refs ippoan/alc-app#676) を実 DB で通す: 宛先の SQL (notify のグループ「点呼の異常」) と、
//! 口 (`/internal/webhooks/fire`) が積んだ控えから中断・日常点検の NG だけを 1 人 1 通ずつ送ること。
//! 接続は本番の worker と同じ実行用ロール `alc_api_rt` (RLS が効く)。

mod support;

use std::sync::{Arc, Mutex as StdMutex};

use serde_json::json;
use support::{
    broken_client, call, call_with, client_as, notify_group, notify_member, rt_client, tenant,
    FakeMailer, READ_ONLY_ROLE,
};
use tenko::deliver::Event;
use tenko::mail::{notify, notify_all, recipients, Recipients, SharedMailer, GROUP};
use uuid::Uuid;

type Logs = Arc<StdMutex<Vec<String>>>;

fn sink(logs: &Logs) -> tenko::LogSink {
    let l = logs.clone();
    Arc::new(move |m: &str| l.lock().unwrap().push(m.to_owned()))
}

fn shared(m: &Arc<FakeMailer>) -> SharedMailer {
    m.clone()
}

/// テナントごとに違う宛先 (ほかのテストの行と混ざらない)
fn addr(t: Uuid, who: &str) -> String {
    format!("{who}-{t}@example.invalid")
}

/// 宛先はグループ「点呼の異常」の使える宛先だけ (無効・email 無し・空白・別のグループ・別のテナントは入らない)。
/// 同じ email は 1 つ。グループが無いのと、メンバーが 0 なのを見分ける
#[tokio::test]
async fn recipients_follow_the_group() {
    let t = tenant().await;
    let mut pg = rt_client().await;
    assert_eq!(
        recipients(&mut pg, t).await.unwrap(),
        Recipients {
            group_exists: false,
            emails: vec![]
        }
    );
    let g = notify_group(t, GROUP).await;
    assert_eq!(
        recipients(&mut pg, t).await.unwrap(),
        Recipients {
            group_exists: true,
            emails: vec![]
        }
    );
    let (a, b) = (addr(t, "a"), addr(t, "b"));
    notify_member(t, g, Some(&b), true).await;
    notify_member(t, g, Some(&a), true).await;
    notify_member(t, g, Some(&a), true).await;
    notify_member(t, g, Some(&addr(t, "off")), false).await;
    notify_member(t, g, None, true).await;
    notify_member(t, g, Some("  "), true).await;
    let alcohol = notify_group(t, "アルコール検知").await;
    notify_member(t, alcohol, Some(&addr(t, "alcohol")), true).await;
    let other = tenant().await;
    let og = notify_group(other, GROUP).await;
    notify_member(other, og, Some(&addr(other, "x")), true).await;

    assert_eq!(
        recipients(&mut pg, t).await.unwrap(),
        Recipients {
            group_exists: true,
            emails: vec![a, b]
        }
    );
}

/// 口に渡された中断・日常点検の NG だけを、宛先ごとに 1 通ずつ送る (アルコール検知・運行報告は送らない)。
/// dev端末の要求は積まれないので送らない。ログに宛先・名前・tenant_id を出さない
#[tokio::test]
async fn fired_anomalies_are_mailed_once_per_recipient() {
    let app = support::app().await;
    let t = tenant().await;
    let g = notify_group(t, GROUP).await;
    let (a, b) = (addr(t, "a"), addr(t, "b"));
    notify_member(t, g, Some(&a), true).await;
    notify_member(t, g, Some(&b), true).await;

    let fire = |event_type: &str, data: serde_json::Value| {
        Some(json!({
            "event_type": event_type,
            "payload": {
                "event": event_type,
                "timestamp": "2026-10-28T01:30:00Z",
                "tenant_id": t,
                "data": data,
            }
        }))
    };
    let uri = "/internal/webhooks/fire";
    for (event_type, data) in [
        (
            "tenko_interrupted",
            json!({ "employee_name": "中断 一郎", "tenko_type": "pre_operation", "reason": "体調不良" }),
        ),
        (
            "inspection_ng",
            json!({ "employee_name": "点検 二郎", "inspection": { "brakes": "ng" } }),
        ),
        ("alcohol_detected", json!({ "employee_name": "検知 三郎" })),
        ("report_submitted", json!({})),
    ] {
        let got = call(&app, "POST", uri, Some(t), fire(event_type, data)).await;
        assert_eq!(got.status, 202);
    }
    // dev端末の要求は積まない
    let dev = call_with(
        &app,
        "POST",
        uri,
        Some(t),
        &[("X-Device-Dev", "1")],
        fire("tenko_interrupted", json!({})),
    )
    .await;
    assert_eq!(dev.status, 202);

    let events = app.outbox.take();
    assert_eq!(events.len(), 4);
    let mailer = Arc::new(FakeMailer::default());
    let got = notify_all(&app.pg, Some(&shared(&mailer)), &sink(&app.logs), &events).await;
    assert_eq!(got, (4, 0));
    for to in [&a, &b] {
        let mails = mailer.sent_to(to);
        let subjects: Vec<_> = mails.iter().map(|m| m.subject.as_str()).collect();
        assert_eq!(
            subjects,
            [
                "【点呼の異常】点呼が中断されました",
                "【点呼の異常】日常点検で NG がありました"
            ]
        );
        assert!(mails[0].body.contains("乗務員: 中断 一郎\n"));
        assert!(mails[0]
            .body
            .contains("日時: 2026-10-28 10:30 (日本時間)\n"));
        assert!(mails[0].body.contains("理由: 体調不良\n"));
        assert!(mails[1].body.contains("NG の項目: ブレーキ\n"));
    }
    assert!(app.logs().is_empty());

    // 控えが空・送らない種類だけ → DB に触らない (切れた接続でもログが出ない)
    let broken = Arc::new(futures_util::lock::Mutex::new(broken_client().await));
    let logs: Logs = Arc::default();
    let only_alcohol = [(t, Event::new("alcohol_detected", json!({})))];
    for evs in [&[][..], &only_alcohol[..]] {
        assert_eq!(
            notify_all(&broken, Some(&shared(&mailer)), &sink(&logs), evs).await,
            (0, 0)
        );
    }
    assert!(logs.lock().unwrap().is_empty());
}

/// 送らない理由と失敗はログ 1 行 (固定の語)。1 人に送れなくてもほかの宛先へは送る。テナントごとに宛先を引く
#[tokio::test]
async fn skips_and_failures_are_logged() {
    let interrupted = || Event::new("tenko_interrupted", json!({}));
    let mailer = Arc::new(FakeMailer::default());
    let pg = Arc::new(futures_util::lock::Mutex::new(rt_client().await));

    // 送り手が無い
    let t = tenant().await;
    let logs: Logs = Arc::default();
    assert_eq!(
        notify(&pg, None, &sink(&logs), t, &[interrupted()]).await,
        (0, 0)
    );
    // グループが無い・メンバーが 0
    let empty = tenant().await;
    notify_group(empty, GROUP).await;
    let events = [(t, interrupted()), (empty, interrupted())];
    assert_eq!(
        notify_all(&pg, Some(&shared(&mailer)), &sink(&logs), &events).await,
        (0, 0)
    );
    assert_eq!(
        *logs.lock().unwrap(),
        [
            "tenko: anomaly mail skipped: no mailer",
            "tenko: anomaly mail skipped: no group",
            "tenko: anomaly mail skipped: no recipients",
        ]
    );

    // 1 人に送れない → ログ 1 行、ほかの宛先へは送る
    let partly = tenant().await;
    let g = notify_group(partly, GROUP).await;
    let (ok, ng) = (addr(partly, "ok"), addr(partly, "ng"));
    notify_member(partly, g, Some(&ok), true).await;
    notify_member(partly, g, Some(&ng), true).await;
    mailer.fail.lock().unwrap().push(ng.clone());
    let logs: Logs = Arc::default();
    assert_eq!(
        notify(
            &pg,
            Some(&shared(&mailer)),
            &sink(&logs),
            partly,
            &[interrupted()]
        )
        .await,
        (1, 1)
    );
    assert_eq!(mailer.sent_to(&ok).len(), 1);
    assert_eq!(
        *logs.lock().unwrap(),
        ["tenko: anomaly mail send failed: send"]
    );

    // 宛先を引けない (切れた接続・notify の表を読めないロール)
    for client in [broken_client().await, client_as(READ_ONLY_ROLE).await] {
        let logs: Logs = Arc::default();
        let c = Arc::new(futures_util::lock::Mutex::new(client));
        assert_eq!(
            notify(
                &c,
                Some(&shared(&mailer)),
                &sink(&logs),
                partly,
                &[interrupted()]
            )
            .await,
            (0, 0)
        );
        let logs = logs.lock().unwrap().clone();
        assert_eq!(logs.len(), 1);
        assert!(logs[0].starts_with("tenko: anomaly mail failed: db ("));
    }
}
