//! IT点呼 の判定待ちの毎朝のまとめ (Refs ippoan/alc-app-s3#352)。
//!
//! IT点呼 は運行管理者が判定 (`manager_judgment`) を付けて完了する。判定の付かないまま何日も残ることがあったため、
//! 毎朝 06:00 (日本時間。worker の Cron Trigger [`CRON`] = 21:00 UTC、本番だけ) に、判定待ちの IT点呼 を持つテナントの
//! notify のグループ「点呼の異常」のメンバーへ 1 人 1 通、判定待ちの一覧を送る ([`crate::mail::send_to_group`])。
//!
//! - 判定待ち = 点呼方法が `IT点呼` で判定が空 (status は問わない)。管理画面の判定待ちの一覧
//!   (`GET /tenko/sessions?tenko_method=IT点呼&judgment_pending=true`) と同じ定義で、件数が一致する
//! - テナントは SECURITY DEFINER の関数 `list_it_tenko_pending_tenants()` (alc-migrations 217。同じ定義) で横断して引く
//!   (nil のテナントの transaction)。中身はテナントの transaction で RLS 越しに読む
//! - dev端末の行は載せない (dev の軸を立てない transaction は本番の行しか見えない。文にも `is_dev = FALSE` を書く)
//! - 0 件のテナントには送らない (関数で引いた後に判定されたなど)
//! - 送った印は付けない (1 日 1 回の Cron に任せる)。1 つのテナントで DB が失敗しても、ログを出して次のテナントへ進む
//!
//! IT点呼 の着信 (判定待ちができた瞬間) の 1 通は別の経路 ([`crate::mail::IT_TENKO_PENDING`]。測定の口が渡す)。

use std::sync::Arc;

use alc_worker_db::{kind, PgClient, TxOutput};
use chrono::{DateTime, Utc};
use futures_util::lock::Mutex;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::mail::{
    jst_time, or_unknown, send_to_group, tenko_type_label, SharedMailer, FOOTER, SUBJECT_PREFIX,
    UNKNOWN,
};
use crate::LogSink;

/// 毎朝のまとめの Cron (wrangler.toml の `[triggers]`。21:00 UTC = 06:00 日本時間)。worker の `scheduled` はこの式で分ける
pub const CRON: &str = "0 21 * * *";

pub mod sql {
    /// 判定待ちの IT点呼 を持つテナント (全テナントの横断。SECURITY DEFINER の関数、alc-migrations 217。条件は [`PENDING`] と同じ)
    pub const LIST_TENANTS: &str = "SELECT t FROM alc_api.list_it_tenko_pending_tenants() AS t";

    /// テナントの判定待ちの IT点呼 (古い順)。$1 tenant_id → started_at, location, tenko_type, status, 社員の名前
    pub const PENDING: &str = "SELECT s.started_at, s.location, s.tenko_type, s.status, \
           (SELECT e.name FROM alc_api.employees e WHERE e.id = s.employee_id AND e.tenant_id = s.tenant_id) \
         FROM alc_api.tenko_sessions s \
         WHERE s.tenant_id = $1 AND s.tenko_method = 'IT点呼' AND s.manager_judgment IS NULL \
           AND s.is_dev = FALSE \
         ORDER BY s.started_at NULLS LAST, s.created_at, s.id";
}

/// 判定待ちの IT点呼 1 件
#[derive(Debug, Clone, PartialEq)]
pub struct Pending {
    pub started_at: Option<DateTime<Utc>>,
    pub location: Option<String>,
    pub tenko_type: String,
    pub status: String,
    pub employee_name: Option<String>,
}

impl TxOutput for Pending {}

/// 判定待ちの IT点呼 を持つテナント
pub async fn tenants(pg: &mut PgClient) -> Result<Vec<Uuid>, Error> {
    pg.tenant_tx(Uuid::nil(), |tx| {
        Box::pin(async move {
            let rows = tx.query_typed(sql::LIST_TENANTS, &[]).await?;
            Ok(rows.iter().map(|r| r.get::<_, Uuid>(0)).collect())
        })
    })
    .await
}

/// テナントの判定待ちの IT点呼 (古い順)
pub async fn pending(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<Pending>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::PENDING, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows
                .iter()
                .map(|r| Pending {
                    started_at: r.get(0),
                    location: r.get(1),
                    tenko_type: r.get(2),
                    status: r.get(3),
                    employee_name: r.get(4),
                })
                .collect())
        })
    })
    .await
}

/// status の表示 (管理画面の表示語。IT点呼 の判定待ちに来うるものだけ訳し、ほかは値そのもの)
fn status_label(status: &str) -> String {
    match status {
        "judgment_pending" => "判定待ち".to_owned(),
        "cancelled" => "中止".to_owned(),
        "interrupted" => "中断".to_owned(),
        "completed" => "完了".to_owned(),
        other => or_unknown(Some(other)),
    }
}

/// まとめの件名と本文 (日本語。時刻は日本時間)。0 件なら None (送らない)
pub fn compose(list: &[Pending]) -> Option<(String, String)> {
    if list.is_empty() {
        return None;
    }
    let title = format!("IT点呼 の判定待ちが {} 件あります", list.len());
    let lines: Vec<String> = list
        .iter()
        .enumerate()
        .map(|(i, p)| {
            format!(
                "{}. 開始: {} (日本時間) / 拠点: {} / 種類: {} / 乗務員: {} / 状態: {}",
                i + 1,
                p.started_at
                    .as_ref()
                    .map_or_else(|| UNKNOWN.to_owned(), jst_time),
                or_unknown(p.location.as_deref()),
                tenko_type_label(Some(&p.tenko_type)),
                or_unknown(p.employee_name.as_deref()),
                status_label(&p.status),
            )
        })
        .collect();
    let body = format!(
        "{title}。運行管理者の判定を付けてください。\n\n{}\n{FOOTER}",
        lines.join("\n")
    );
    Some((format!("{SUBJECT_PREFIX}{title}"), body))
}

/// 1 回のまとめ (Cron から毎朝)。テナントごとに判定待ちを読み、1 件以上あればグループ「点呼の異常」へ送る。
/// DB の失敗は固定の語でログに出し、次へ進む。返すのは (送れた数, 失敗した数) の合計
pub async fn run(
    pg: Arc<Mutex<PgClient>>,
    mailer: Option<SharedMailer>,
    log: LogSink,
) -> (usize, usize) {
    let list = {
        let mut c = pg.lock().await;
        tenants(&mut c).await
    };
    let list = match list {
        Ok(l) => l,
        Err(e) => {
            log(&format!(
                "tenko: it pending tenants failed: db ({})",
                kind(&e)
            ));
            return (0, 0);
        }
    };
    let (mut sent, mut failed) = (0, 0);
    for tenant_id in list {
        let found = {
            let mut c = pg.lock().await;
            pending(&mut c, tenant_id).await
        };
        let found = match found {
            Ok(f) => f,
            Err(e) => {
                log(&format!("tenko: it pending read failed: db ({})", kind(&e)));
                continue;
            }
        };
        let mails: Vec<(String, String)> = compose(&found).into_iter().collect();
        let (s, f) = send_to_group(&pg, mailer.as_ref(), &log, tenant_id, &mails).await;
        sent += s;
        failed += f;
    }
    (sent, failed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(ms: i64) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_millis(ms)
    }

    #[test]
    fn empty_is_not_mailed() {
        assert_eq!(compose(&[]), None);
    }

    #[test]
    fn lists_each_pending_in_order() {
        let list = [
            Pending {
                // 2026-11-13T00:12:00Z = 09:12 日本時間
                started_at: at(1_794_528_720_000),
                location: Some("第一\n営業所".into()),
                tenko_type: "pre_operation".into(),
                status: "judgment_pending".into(),
                employee_name: Some("判定 待子".into()),
            },
            Pending {
                started_at: None,
                location: None,
                tenko_type: "normal".into(),
                status: "cancelled".into(),
                employee_name: None,
            },
        ];
        let (subject, body) = compose(&list).unwrap();
        assert_eq!(subject, "【点呼の異常】IT点呼 の判定待ちが 2 件あります");
        assert_eq!(
            body,
            "IT点呼 の判定待ちが 2 件あります。運行管理者の判定を付けてください。\n\n\
             1. 開始: 2026-11-13 09:12 (日本時間) / 拠点: 第一 営業所 / 種類: 乗務前 / 乗務員: 判定 待子 / 状態: 判定待ち\n\
             2. 開始: 不明 (日本時間) / 拠点: 不明 / 種類: 通常 / 乗務員: 不明 / 状態: 中止\n\n\
             このメールは自動点呼の記録から送っています。\n"
        );
    }

    #[test]
    fn status_labels() {
        assert_eq!(status_label("interrupted"), "中断");
        assert_eq!(status_label("completed"), "完了");
        assert_eq!(status_label("report_pending"), "report_pending");
        assert_eq!(status_label(""), "不明");
    }
}
