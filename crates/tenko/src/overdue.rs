//! 予定超過の確認 (W7c、Refs ippoan/rust-alc-api#680)。rust の `alc-tenko` の overdue.rs・repo/overdue.rs と、
//! `src/main.rs` の 60 秒のループの写し。worker は Cron Trigger (毎分、本番だけ) の `scheduled` から [`run`] を呼ぶ。
//!
//! - 送り先の設定 (`tenko_overdue` の有効な webhook) が在るテナントを、SECURITY DEFINER の関数
//!   `list_tenko_overdue_webhook_configs()` で横断して引く (nil のテナントの transaction。rust と同じ関数)
//! - テナントごとに、未通知で、予定の時刻から **そのテナントの分数** (`tenko_settings.overdue_minutes`。行が無ければ
//!   [`DEFAULT_OVERDUE_MINUTES`]) を過ぎても点呼が完了していない予定に **通知済みの印を先に付け** ([`sql::CLAIM`])、
//!   印を付けられた行だけを送る。「完了していない」= 未消費、または消費したセッションの status が `completed`・`cancelled` 以外
//!   (中断・判定待ち・途中も含む。自動点呼機器の認定要件の予定超過の確認)。本文の `data` にそのセッションの
//!   `session_id`・`session_status` (未消費は null) を載せる
//! - 送るのは W7b の [`crate::deliver::deliver`] (署名・再送・配信の記録)。送り終えるまで待つ
//!
//! rust との違い:
//! - rust は「読む → 送る中身を作る → 印を付ける → 送る」の順で、2 つの実行が重なると同じ予定を 2 回送りうる。
//!   ここは印を付ける UPDATE が条件 (`overdue_notified_at IS NULL`) ごと行を取るので、rust のループと重なって動いても
//!   1 つの予定は 1 回しか送らない
//! - 社員の名前は印を付ける文の中で引く (rust は 1 件ずつ別の文)
//! - 1 つのテナントで DB が失敗しても、ログを出して次のテナントへ進む (rust は全体を止めていた)
//! - 超過の分数はテナントの設定 (rust は env `TENKO_OVERDUE_MINUTES` で全テナント共通、本番の Cloud Run には設定が無く 60 分だった)。
//!   読み書きは `GET`・`PUT /tenko/settings` ([`crate::settings`])
//! - 消費済みでも点呼が終わっていない予定を超過に数える (rust は未消費だけ)
//!
//! dev の軸は立てない (定期の確認は本番の行だけ。rust の `TenantConn` も dev を立てていなかった)。

use std::sync::Arc;

use alc_worker_db::{kind, PgClient, TxOutput};
use chrono::{DateTime, Utc};
use futures_util::lock::Mutex;
use serde_json::{json, Value};
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::deliver::{deliver, Event, Sender};
use crate::LogSink;

/// 設定の行が無いテナントの、予定の時刻から何分過ぎたら超過とするか (rust の `TENKO_OVERDUE_MINUTES` の既定値)
pub const DEFAULT_OVERDUE_MINUTES: i32 = 60;

/// 送る webhook の種類
pub const EVENT: &str = "tenko_overdue";

pub mod sql {
    use super::DEFAULT_OVERDUE_MINUTES;
    use const_format::concatcp;

    /// 送り先の設定が在るテナント (全テナントの横断。SECURITY DEFINER の関数、migration 158)
    pub const LIST_TENANTS: &str =
        "SELECT DISTINCT tenant_id FROM alc_api.list_tenko_overdue_webhook_configs()";

    /// 予定を消費したセッションの status (消費していなければ NULL)。予定の別名は `s`
    pub const SESSION_STATUS: &str = "(SELECT ss.status FROM alc_api.tenko_sessions ss \
         WHERE ss.id = s.consumed_by_session_id AND ss.tenant_id = s.tenant_id)";

    /// 「超過」の条件 (予定の別名は `s`)。予定の時刻 + テナントの分数 (`tenko_settings`、行が無ければ既定) を過ぎ、
    /// 未消費か、消費したセッションが `completed`・`cancelled` 以外。分数は文の中で引く (追加の往復は無い)。
    /// 通知 ([`CLAIM`]) とダッシュボードの超過の一覧 ([`crate::dashboard`]) が同じ規則を使うための共有の断片
    pub const IS_OVERDUE: &str = concatcp!(
        "s.scheduled_at + make_interval(mins => COALESCE( \
           (SELECT t.overdue_minutes FROM alc_api.tenko_settings t WHERE t.tenant_id = s.tenant_id), ",
        DEFAULT_OVERDUE_MINUTES,
        ")) < NOW() AND (s.consumed = FALSE OR COALESCE(",
        SESSION_STATUS,
        " NOT IN ('completed', 'cancelled'), FALSE))"
    );

    /// 超過 ([`IS_OVERDUE`]) の予定に通知済みの印を付け、付けた行を返す (印を先に付ける。重なって動いても 1 回だけ取れる)。
    /// 分数はこの 1 文の中で引く (テナントごとに 1 文のまま)。
    /// $1 tenant_id → id, employee_id, scheduled_at, responsible_manager_name, tenko_type, 社員の名前, session_id, session_status
    pub const CLAIM: &str = concatcp!(
        "UPDATE alc_api.tenko_schedules s SET overdue_notified_at = NOW() \
         WHERE s.tenant_id = $1 AND s.overdue_notified_at IS NULL AND ",
        IS_OVERDUE,
        " RETURNING s.id, s.employee_id, s.scheduled_at, s.responsible_manager_name, s.tenko_type, \
           (SELECT e.name FROM alc_api.employees e WHERE e.id = s.employee_id AND e.tenant_id = s.tenant_id), \
           s.consumed_by_session_id, ",
        SESSION_STATUS
    );
}

/// 印を付けた予定 1 件
#[derive(Debug, Clone, PartialEq)]
pub struct Overdue {
    pub id: Uuid,
    pub employee_id: Uuid,
    pub scheduled_at: DateTime<Utc>,
    pub responsible_manager_name: Option<String>,
    pub tenko_type: String,
    pub employee_name: Option<String>,
    /// 消費したセッション (未消費は None)
    pub session_id: Option<Uuid>,
    /// そのセッションの status (未消費は None)
    pub session_status: Option<String>,
}

impl TxOutput for Overdue {}

/// 送り先の設定が在るテナント
pub async fn tenants(pg: &mut PgClient) -> Result<Vec<Uuid>, Error> {
    pg.tenant_tx(Uuid::nil(), |tx| {
        Box::pin(async move {
            let rows = tx.query_typed(sql::LIST_TENANTS, &[]).await?;
            Ok(rows.iter().map(|r| r.get::<_, Uuid>(0)).collect())
        })
    })
    .await
}

/// テナントの超過した予定に印を付けて返す
pub async fn claim(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<Overdue>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::CLAIM, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows
                .iter()
                .map(|r| Overdue {
                    id: r.get(0),
                    employee_id: r.get(1),
                    scheduled_at: r.get(2),
                    responsible_manager_name: r.get(3),
                    tenko_type: r.get(4),
                    employee_name: r.get(5),
                    session_id: r.get(6),
                    session_status: r.get(7),
                })
                .collect())
        })
    })
    .await
}

/// 送る本文 (rust と同じ形)。`now_ms` は今 (unix ms)
pub fn payload(tenant_id: Uuid, s: &Overdue, now_ms: i64) -> Value {
    let now = DateTime::from_timestamp_millis(now_ms).unwrap_or_default();
    json!({
        "event": EVENT,
        "timestamp": now,
        "tenant_id": tenant_id,
        "data": {
            "schedule_id": s.id,
            "employee_id": s.employee_id,
            "employee_name": s.employee_name.clone().unwrap_or_default(),
            "scheduled_at": s.scheduled_at,
            "minutes_overdue": (now - s.scheduled_at).num_minutes(),
            "responsible_manager_name": s.responsible_manager_name,
            "tenko_type": s.tenko_type,
            "session_id": s.session_id,
            "session_status": s.session_status,
        }
    })
}

/// 1 回の確認 (Cron から毎分)。テナントごとに印を付けて送る。DB の失敗は固定の語でログに出し、次へ進む
pub async fn run(pg: Arc<Mutex<PgClient>>, sender: Sender, log: LogSink, now_ms: i64) {
    let list = {
        let mut c = pg.lock().await;
        tenants(&mut c).await
    };
    let list = match list {
        Ok(l) => l,
        Err(e) => {
            log(&format!("tenko: overdue configs failed: db ({})", kind(&e)));
            return;
        }
    };
    for tenant_id in list {
        let claimed = {
            let mut c = pg.lock().await;
            claim(&mut c, tenant_id).await
        };
        let claimed = match claimed {
            Ok(c) => c,
            Err(e) => {
                log(&format!("tenko: overdue claim failed: db ({})", kind(&e)));
                continue;
            }
        };
        for s in &claimed {
            let event = Event::new(EVENT, payload(tenant_id, s, now_ms));
            deliver(&pg, &sender, &log, tenant_id, event).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_matches_rust() {
        let t = Uuid::nil();
        let s = Overdue {
            id: Uuid::max(),
            employee_id: Uuid::nil(),
            scheduled_at: DateTime::from_timestamp_millis(1_000_000).unwrap(),
            responsible_manager_name: None,
            tenko_type: "pre_operation".into(),
            employee_name: None,
            session_id: None,
            session_status: None,
        };
        let p = payload(t, &s, 1_000_000 + 61 * 60_000 + 30_000);
        assert_eq!(p["event"], "tenko_overdue");
        assert_eq!(p["data"]["minutes_overdue"], 61);
        assert_eq!(p["data"]["employee_name"], "");
        assert_eq!(p["data"]["responsible_manager_name"], Value::Null);
        assert_eq!(p["data"]["session_id"], Value::Null);
        assert_eq!(p["data"]["session_status"], Value::Null);
        assert_eq!(p["tenant_id"], json!(t));
        // 範囲外の時刻は 1970-01-01 に落とす (panic しない)
        assert_eq!(
            payload(t, &s, i64::MAX)["timestamp"],
            json!(DateTime::<Utc>::default())
        );
    }
}
