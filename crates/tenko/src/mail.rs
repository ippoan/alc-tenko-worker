//! 点呼の異常のメール (Refs ippoan/alc-app#676)。自動点呼機器の認定要件 (日常点検の NG・安全の判定による中断・
//! 点呼の遅延を運行管理者へ知らせる) に合わせ、webhook を設定していない事業者にも、テナントの notify のグループ
//! **「点呼の異常」** のメンバーへ 1 人 1 通のメールを送る。手本は ippoan/alc-measurements-worker の
//! アルコール検知メール (`crates/measurements/src/alert.rs`)。宛先は notify の既存の表 (`notify_groups`・
//! `notify_recipient_groups`・`notify_recipients`) から引き、専用の表は作らない。
//!
//! 送る種類は 3 つ ([`compose`] が件名・本文を組めるもの。ほかの種類は送らない):
//! - `tenko_interrupted` (中断)・`inspection_ng` (日常点検の NG) — 測定の口が [`crate::fire`] に渡し、口が
//!   tenant の一致と dev端末の除外を通して [`crate::deliver::Outbox`] に積んだ控え。worker が応答の後に
//!   [`notify_all`] で送る (webhook の送信と同じ控え。dev端末の要求・テナントの違う payload は積まれないので送らない)
//! - `tenko_overdue` (遅延) — [`crate::overdue::run`] が通知済みの印を先に付けた予定だけ (同じ予定に 2 回送らない)
//!
//! 送るのは [`Mailer`] (worker では Cloudflare の `send_email` の binding、テストでは偽物)。無ければ (`None`) 送らない。
//! 失敗はログ 1 行だけで、リトライしない。**ログは固定の語だけ** (メールアドレス・tenant_id・氏名を出さない)。

use std::sync::Arc;

use alc_worker_db::{kind, PgClient, TxOutput};
use chrono::{DateTime, FixedOffset};
use futures_util::future::BoxFuture;
use futures_util::lock::Mutex;
use serde_json::Value;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::deliver::Event;
use crate::LogSink;

/// 宛先のグループの名前 (テナントごとに notify の画面で作る。`notify_groups` は (tenant_id, name) が UNIQUE)
pub const GROUP: &str = "点呼の異常";

/// 件名の接頭辞
pub const SUBJECT_PREFIX: &str = "【点呼の異常】";

/// 値が引けないときの表示
pub const UNKNOWN: &str = "不明";

/// 送信の段の失敗。持つのは段の名前だけ (宛先・本文・ランタイムのエラー文を持たない)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailError(pub &'static str);

/// 1 人宛の 1 通 (差出人は送り手 [`Mailer`] が持つ)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mail {
    pub to: String,
    pub subject: String,
    pub body: String,
}

/// メールの送り手 (worker では `send_email` の binding、テストでは偽物)
pub trait Mailer: Send + Sync {
    fn send<'a>(&'a self, mail: &'a Mail) -> BoxFuture<'a, Result<(), MailError>>;
}

pub type SharedMailer = Arc<dyn Mailer>;

pub mod sql {
    /// 宛先 (alc-measurements-worker の alert.rs の `RECIPIENTS` の写し)。グループ → (LEFT JOIN) メンバー → 宛先。
    /// グループが在ってメンバーが 0 でもグループの行は 1 行返る (email は NULL)。グループが無ければ 0 行 —
    /// 設定漏れと「メンバー 0」を見分けるため。使えない宛先 (`enabled` でない・email が NULL) は JOIN の条件で落とす。
    /// $1 グループの名前 / $2 tenant_id → email
    pub const RECIPIENTS: &str = "SELECT r.email \
         FROM alc_api.notify_groups g \
         LEFT JOIN alc_api.notify_recipient_groups m ON m.group_id = g.id \
         LEFT JOIN alc_api.notify_recipients r ON r.id = m.recipient_id \
             AND r.tenant_id = $2 AND r.enabled AND r.email IS NOT NULL \
         WHERE g.name = $1 AND g.tenant_id = $2";
}

/// グループの宛先 (グループが在るか, 使える email (昇順・重複なし))
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recipients {
    pub group_exists: bool,
    pub emails: Vec<String>,
}

impl TxOutput for Recipients {}

/// グループ「点呼の異常」の使える宛先を、短い tenant_tx 1 回で引く (dev の軸は持たない表)
pub async fn recipients(pg: &mut PgClient, tenant_id: Uuid) -> Result<Recipients, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::RECIPIENTS,
                    &[(&GROUP, Type::TEXT), (&tenant_id, Type::UUID)],
                )
                .await?;
            let mut emails: Vec<String> = rows
                .iter()
                .filter_map(|r| r.get::<_, Option<String>>(0))
                .filter(|e| !e.trim().is_empty())
                .collect();
            // 同じ email の宛先が 2 つ在っても 1 通
            emails.sort();
            emails.dedup();
            Ok(Recipients {
                group_exists: !rows.is_empty(),
                emails,
            })
        })
    })
    .await
}

/// 改行を空白にした 1 行の文字列 (件名・本文の行を崩させない)
fn one_line(s: &str) -> String {
    s.replace(['\r', '\n'], " ")
}

/// `data` の文字列の項目 (無い・空なら None)
fn text<'a>(data: &'a Value, key: &str) -> Option<&'a str> {
    data.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
}

/// RFC 3339 の時刻を日本時間の `YYYY-MM-DD HH:MM` に (読めなければ [`UNKNOWN`])
fn jst(at: Option<&str>) -> String {
    let jst = FixedOffset::east_opt(9 * 3600).expect("+09:00");
    at.and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&jst).format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| UNKNOWN.to_owned())
}

/// 点呼の種類の表示
fn tenko_type(data: &Value) -> String {
    match text(data, "tenko_type") {
        Some("pre_operation") => "乗務前".to_owned(),
        Some("post_operation") => "乗務後".to_owned(),
        Some(other) => one_line(other),
        None => UNKNOWN.to_owned(),
    }
}

/// 安全の判定で外れた項目の表示 (測定の口の `failed_items` の語)
fn failed_item(item: &str) -> String {
    match item {
        "illness" => "疾病".to_owned(),
        "fatigue" => "疲労".to_owned(),
        "sleep_deprivation" => "睡眠不足".to_owned(),
        "temperature" => "体温".to_owned(),
        "systolic" => "最高血圧".to_owned(),
        "diastolic" => "最低血圧".to_owned(),
        other => one_line(other),
    }
}

/// 日常点検の項目 (測定の口の `inspection` の鍵) と表示。この順で並べる
pub const INSPECTION_ITEMS: [(&str, &str); 8] = [
    ("brakes", "ブレーキ"),
    ("tires", "タイヤ"),
    ("lights", "灯火類"),
    ("steering", "ハンドル"),
    ("wipers", "ワイパー"),
    ("mirrors", "ミラー"),
    ("horn", "警音器"),
    ("seatbelts", "シートベルト"),
];

/// 本文の末尾
const FOOTER: &str = "\nこのメールは自動点呼の記録から送っています。\n";

/// 種類ごとの件名と本文 (日本語。時刻は日本時間)。メールを送らない種類は None。
/// payload は webhook と同じもの (`timestamp` と `data`)
pub fn compose(event: &Event) -> Option<(String, String)> {
    let p = &event.payload;
    let data = p.get("data").unwrap_or(&Value::Null);
    let name = text(data, "employee_name").map_or_else(|| UNKNOWN.to_owned(), one_line);
    let mut lines = vec![];
    let title = match event.event_type {
        "tenko_interrupted" => {
            lines.push(format!("乗務員: {name}"));
            lines.push(format!("点呼の種類: {}", tenko_type(data)));
            lines.push(format!(
                "日時: {} (日本時間)",
                jst(p.get("timestamp").and_then(Value::as_str))
            ));
            if let Some(reason) = text(data, "reason") {
                lines.push(format!("理由: {}", one_line(reason)));
            }
            let failed: Vec<String> = data
                .get("failed_items")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(failed_item)
                        .collect()
                })
                .unwrap_or_default();
            if !failed.is_empty() {
                lines.push(format!("安全の判定で外れた項目: {}", failed.join("、")));
            }
            "点呼が中断されました"
        }
        "inspection_ng" => {
            let inspection = data.get("inspection").unwrap_or(&Value::Null);
            let at = inspection
                .get("inspected_at")
                .and_then(Value::as_str)
                .or_else(|| p.get("timestamp").and_then(Value::as_str));
            let ng: Vec<&str> = INSPECTION_ITEMS
                .iter()
                .filter(|(key, _)| inspection.get(key).and_then(Value::as_str) == Some("ng"))
                .map(|(_, label)| *label)
                .collect();
            lines.push(format!("乗務員: {name}"));
            lines.push(format!("日時: {} (日本時間)", jst(at)));
            if !ng.is_empty() {
                lines.push(format!("NG の項目: {}", ng.join("、")));
            }
            lines.push("点呼は中止しました。".to_owned());
            "日常点検で NG がありました"
        }
        "tenko_overdue" => {
            lines.push(format!("乗務員: {name}"));
            lines.push(format!("点呼の種類: {}", tenko_type(data)));
            lines.push(format!(
                "予定の時刻: {} (日本時間)",
                jst(data.get("scheduled_at").and_then(Value::as_str))
            ));
            if let Some(m) = data.get("minutes_overdue").and_then(Value::as_i64) {
                lines.push(format!("超過: {m} 分"));
            }
            let state = if data.get("session_id").is_some_and(|v| !v.is_null()) {
                "開始したが完了していない"
            } else {
                "開始していない"
            };
            lines.push(format!("点呼の状態: {state}"));
            "点呼が予定の時刻を過ぎても完了していません"
        }
        _ => return None,
    };
    if let Some(manager) = text(data, "responsible_manager_name") {
        lines.push(format!("運行管理者: {}", one_line(manager)));
    }
    let body = format!("{title}。\n\n{}\n{FOOTER}", lines.join("\n"));
    Some((format!("{SUBJECT_PREFIX}{title}"), body))
}

/// テナントの控えのうち [`compose`] が組めるものを、グループ「点呼の異常」の宛先へ 1 人 1 通ずつ送る。
/// 組めるものが無ければ DB に触らない。宛先は 1 回だけ引く (pg の lock は引く間だけ持つ)。
/// 送らない理由・失敗はログ 1 行 (固定の語。グループが無いときだけ別の語 = 設定漏れの見分け)。返すのは (送れた数, 失敗した数)
pub async fn notify(
    pg: &Arc<Mutex<PgClient>>,
    mailer: Option<&SharedMailer>,
    log: &LogSink,
    tenant_id: Uuid,
    events: &[Event],
) -> (usize, usize) {
    let mails: Vec<(String, String)> = events.iter().filter_map(compose).collect();
    if mails.is_empty() {
        return (0, 0);
    }
    let Some(mailer) = mailer else {
        log("tenko: anomaly mail skipped: no mailer");
        return (0, 0);
    };
    let found = {
        let mut c = pg.lock().await;
        recipients(&mut c, tenant_id).await
    };
    let to = match found {
        Ok(r) if !r.group_exists => {
            log("tenko: anomaly mail skipped: no group");
            return (0, 0);
        }
        Ok(r) if r.emails.is_empty() => {
            log("tenko: anomaly mail skipped: no recipients");
            return (0, 0);
        }
        Ok(r) => r.emails,
        Err(e) => {
            log(&format!("tenko: anomaly mail failed: db ({})", kind(&e)));
            return (0, 0);
        }
    };
    let (mut sent, mut failed) = (0, 0);
    for (subject, body) in &mails {
        for addr in &to {
            let mail = Mail {
                to: addr.clone(),
                subject: subject.clone(),
                body: body.clone(),
            };
            match mailer.send(&mail).await {
                Ok(()) => sent += 1,
                Err(e) => {
                    failed += 1;
                    log(&format!("tenko: anomaly mail send failed: {}", e.0));
                }
            }
        }
    }
    (sent, failed)
}

/// 口が積んだ控え (応答の後) を、テナントごとにまとめて [`notify`] へ渡す (webhook の [`crate::deliver::deliver_all`] と
/// 同じ控え)。返すのは (送れた数, 失敗した数) の合計
pub async fn notify_all(
    pg: &Arc<Mutex<PgClient>>,
    mailer: Option<&SharedMailer>,
    log: &LogSink,
    events: &[(Uuid, Event)],
) -> (usize, usize) {
    let mut tenants: Vec<Uuid> = vec![];
    for (t, _) in events {
        if !tenants.contains(t) {
            tenants.push(*t);
        }
    }
    let (mut sent, mut failed) = (0, 0);
    for t in tenants {
        let mine: Vec<Event> = events
            .iter()
            .filter(|(x, _)| *x == t)
            .map(|(_, e)| e.clone())
            .collect();
        let (s, f) = notify(pg, mailer, log, t, &mine).await;
        sent += s;
        failed += f;
    }
    (sent, failed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn interrupted_has_driver_time_and_reasons() {
        let e = Event::new(
            "tenko_interrupted",
            json!({
                "timestamp": "2026-09-12T15:30:00Z",
                "data": {
                    "employee_name": "山田\r\n太郎",
                    "tenko_type": "pre_operation",
                    "reason": "体調\n不良",
                    "failed_items": ["temperature", "systolic", "diastolic", "illness", "fatigue", "sleep_deprivation", "other"],
                    "responsible_manager_name": "運行 管理者",
                }
            }),
        );
        let (subject, body) = compose(&e).unwrap();
        assert_eq!(subject, "【点呼の異常】点呼が中断されました");
        assert_eq!(
            body,
            "点呼が中断されました。\n\n\
             乗務員: 山田  太郎\n\
             点呼の種類: 乗務前\n\
             日時: 2026-09-13 00:30 (日本時間)\n\
             理由: 体調 不良\n\
             安全の判定で外れた項目: 体温、最高血圧、最低血圧、疾病、疲労、睡眠不足、other\n\
             運行管理者: 運行 管理者\n\n\
             このメールは自動点呼の記録から送っています。\n"
        );
    }

    #[test]
    fn interrupted_without_fields_says_unknown() {
        let e = Event::new("tenko_interrupted", json!({}));
        let (_, body) = compose(&e).unwrap();
        assert!(body.contains("乗務員: 不明\n"));
        assert!(body.contains("点呼の種類: 不明\n"));
        assert!(body.contains("日時: 不明 (日本時間)\n"));
        assert!(!body.contains("理由"));
        assert!(!body.contains("外れた項目"));
        assert!(!body.contains("運行管理者"));
        // 空の名前・読めない時刻・知らない点呼の種類
        let e = Event::new(
            "tenko_interrupted",
            json!({ "timestamp": "x", "data": { "employee_name": " ", "tenko_type": "remote", "failed_items": [] } }),
        );
        let (_, body) = compose(&e).unwrap();
        assert!(body.contains("乗務員: 不明\n"));
        assert!(body.contains("点呼の種類: remote\n"));
        assert!(body.contains("日時: 不明 (日本時間)\n"));
    }

    #[test]
    fn inspection_ng_lists_ng_items_in_order() {
        let e = Event::new(
            "inspection_ng",
            json!({
                "timestamp": "2026-09-12T15:31:00Z",
                "data": {
                    "employee_name": "点検 次郎",
                    "inspection": {
                        "brakes": "ok", "tires": "ng", "lights": "ok", "steering": "ok",
                        "wipers": "ok", "mirrors": "ok", "horn": "ng", "seatbelts": "ok",
                        "inspected_at": "2026-09-12T15:30:00Z"
                    },
                }
            }),
        );
        let (subject, body) = compose(&e).unwrap();
        assert_eq!(subject, "【点呼の異常】日常点検で NG がありました");
        assert_eq!(
            body,
            "日常点検で NG がありました。\n\n\
             乗務員: 点検 次郎\n\
             日時: 2026-09-13 00:30 (日本時間)\n\
             NG の項目: タイヤ、警音器\n\
             点呼は中止しました。\n\n\
             このメールは自動点呼の記録から送っています。\n"
        );
        // 点検の時刻が無ければ payload の時刻。NG の項目が読めなければ行を出さない
        let e = Event::new(
            "inspection_ng",
            json!({ "timestamp": "2026-09-12T15:31:00Z", "data": {} }),
        );
        let (_, body) = compose(&e).unwrap();
        assert!(body.contains("日時: 2026-09-13 00:31 (日本時間)\n"));
        assert!(!body.contains("NG の項目"));
    }

    #[test]
    fn overdue_has_schedule_minutes_and_state() {
        let e = Event::new(
            "tenko_overdue",
            json!({
                "data": {
                    "employee_name": "遅延 三郎",
                    "tenko_type": "post_operation",
                    "scheduled_at": "2026-09-12T15:00:00Z",
                    "minutes_overdue": 61,
                    "session_id": null,
                    "responsible_manager_name": null,
                }
            }),
        );
        let (subject, body) = compose(&e).unwrap();
        assert_eq!(
            subject,
            "【点呼の異常】点呼が予定の時刻を過ぎても完了していません"
        );
        assert_eq!(
            body,
            "点呼が予定の時刻を過ぎても完了していません。\n\n\
             乗務員: 遅延 三郎\n\
             点呼の種類: 乗務後\n\
             予定の時刻: 2026-09-13 00:00 (日本時間)\n\
             超過: 61 分\n\
             点呼の状態: 開始していない\n\n\
             このメールは自動点呼の記録から送っています。\n"
        );
        let e = Event::new(
            "tenko_overdue",
            json!({ "data": { "session_id": "00000000-0000-0000-0000-000000000000" } }),
        );
        let (_, body) = compose(&e).unwrap();
        assert!(body.contains("点呼の状態: 開始したが完了していない\n"));
        assert!(!body.contains("超過"));
    }

    #[test]
    fn other_events_are_not_mailed() {
        for t in ["alcohol_detected", "report_submitted"] {
            assert_eq!(compose(&Event::new(t, json!({}))), None);
        }
    }
}
