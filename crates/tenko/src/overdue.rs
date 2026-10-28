//! 予定超過の確認 (W7c、Refs ippoan/rust-alc-api#680)。rust の `alc-tenko` の overdue.rs・repo/overdue.rs と、
//! `src/main.rs` の 60 秒のループの写し。worker は Cron Trigger (毎分、本番だけ) の `scheduled` から [`run`] を呼ぶ。
//!
//! - 送り先の設定 (`tenko_overdue` の有効な webhook) が在るテナントを、SECURITY DEFINER の関数
//!   `list_tenko_overdue_webhook_configs()` で横断して引く (nil のテナントの transaction。rust と同じ関数)
//! - テナントごとに、未消費・未通知で、予定の時刻から [`DEFAULT_OVERDUE_MINUTES`] 分を過ぎた予定に
//!   **通知済みの印を先に付け** ([`sql::CLAIM`])、印を付けられた行だけを送る
//! - 送るのは W7b の [`crate::deliver::deliver`] (署名・再送・配信の記録)。送り終えるまで待つ
//!
//! rust との違い:
//! - rust は「読む → 送る中身を作る → 印を付ける → 送る」の順で、2 つの実行が重なると同じ予定を 2 回送りうる。
//!   ここは印を付ける UPDATE が条件 (`overdue_notified_at IS NULL`) ごと行を取るので、rust のループと重なって動いても
//!   1 つの予定は 1 回しか送らない
//! - 社員の名前は印を付ける文の中で引く (rust は 1 件ずつ別の文)
//! - 1 つのテナントで DB が失敗しても、ログを出して次のテナントへ進む (rust は全体を止めていた)
//! - 超過の分数は定数 (rust は env `TENKO_OVERDUE_MINUTES`。本番の Cloud Run には設定が無く既定の 60 分で動いていた)
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

/// 予定の時刻から何分過ぎたら超過とするか (rust の `TENKO_OVERDUE_MINUTES` の既定値)
pub const DEFAULT_OVERDUE_MINUTES: i32 = 60;

/// 送る webhook の種類
pub const EVENT: &str = "tenko_overdue";

pub mod sql {
    /// 送り先の設定が在るテナント (全テナントの横断。SECURITY DEFINER の関数、migration 158)
    pub const LIST_TENANTS: &str =
        "SELECT DISTINCT tenant_id FROM alc_api.list_tenko_overdue_webhook_configs()";

    /// 超過した予定に通知済みの印を付け、付けた行を返す (印を先に付ける。重なって動いても 1 回だけ取れる)。
    /// $1 tenant_id / $2 超過とする分数 → id, employee_id, scheduled_at, responsible_manager_name, tenko_type, 社員の名前
    pub const CLAIM: &str = "UPDATE alc_api.tenko_schedules s SET overdue_notified_at = NOW() \
         WHERE s.tenant_id = $1 AND s.consumed = FALSE AND s.overdue_notified_at IS NULL \
           AND s.scheduled_at + make_interval(mins => $2) < NOW() \
         RETURNING s.id, s.employee_id, s.scheduled_at, s.responsible_manager_name, s.tenko_type, \
           (SELECT e.name FROM alc_api.employees e WHERE e.id = s.employee_id AND e.tenant_id = s.tenant_id)";
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
pub async fn claim(
    pg: &mut PgClient,
    tenant_id: Uuid,
    minutes: i32,
) -> Result<Vec<Overdue>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::CLAIM,
                    &[(&tenant_id, Type::UUID), (&minutes, Type::INT4)],
                )
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
        }
    })
}

/// 1 回の確認 (Cron から毎分)。テナントごとに印を付けて送る。DB の失敗は固定の語でログに出し、次へ進む
pub async fn run(
    pg: Arc<Mutex<PgClient>>,
    sender: Sender,
    log: LogSink,
    now_ms: i64,
    minutes: i32,
) {
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
            claim(&mut c, tenant_id, minutes).await
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
        };
        let p = payload(t, &s, 1_000_000 + 61 * 60_000 + 30_000);
        assert_eq!(p["event"], "tenko_overdue");
        assert_eq!(p["data"]["minutes_overdue"], 61);
        assert_eq!(p["data"]["employee_name"], "");
        assert_eq!(p["data"]["responsible_manager_name"], Value::Null);
        assert_eq!(p["tenant_id"], json!(t));
        // 範囲外の時刻は 1970-01-01 に落とす (panic しない)
        assert_eq!(
            payload(t, &s, i64::MAX)["timestamp"],
            json!(DateTime::<Utc>::default())
        );
    }
}
