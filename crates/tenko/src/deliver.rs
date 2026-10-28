//! webhook の送信 (rust の `alc_core::webhook` の `fire_event_impl`・`deliver_webhook`・`ReqwestWebhookClient` と、
//! alc-misc の repo/webhook.rs の `find_config`・`record_delivery` の写し)。
//!
//! 口 ([`crate::sessions`]) は送らずに [`Outbox`] に積む。worker は応答を返した後に [`deliver_all`] を
//! `ctx.wait_until` で流す (fire-and-forget の spawn はしない。rust は `tokio::spawn` だった)。
//!
//! 送り方は rust と同じ:
//! - テナントと種類の、有効な設定が無ければ何もしない
//! - `POST` で JSON を送り、`X-Webhook-Event` と、secret があれば `X-Webhook-Signature: sha256=<HMAC-SHA256 の hex>` を付ける
//! - 2xx なら成功。失敗なら 1 秒・5 秒待って最大 3 回。毎回 `webhook_deliveries` に記録する (記録の失敗は無視)
//! - リダイレクトは追わない・10 秒で打ち切る (worker の HTTP の実装 = 直下の src/webhook.rs)
//!
//! 配信の記録は本番の軸だけ (dev端末の要求では口が積まないので、ここに dev の行は来ない)。

use std::sync::{Arc, Mutex as StdMutex};

use alc_worker_db::{kind, PgClient, TxOutput};
use futures_util::future::BoxFuture;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::LogSink;

/// 送る 1 件 (種類と本文)
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub event_type: &'static str,
    pub payload: Value,
}

impl Event {
    pub fn new(event_type: &'static str, payload: Value) -> Self {
        Self {
            event_type,
            payload,
        }
    }
}

/// 要求の中で積み、応答の後で送る控え (要求ごとに 1 つ)
#[derive(Clone, Default)]
pub struct Outbox(Arc<StdMutex<Vec<(Uuid, Event)>>>);

impl Outbox {
    pub fn push(&self, tenant_id: Uuid, event: Event) {
        self.0.lock().expect("控えの lock").push((tenant_id, event));
    }

    /// 積んだものを全部取り出す (控えは空になる)
    pub fn take(&self) -> Vec<(Uuid, Event)> {
        std::mem::take(&mut *self.0.lock().expect("控えの lock"))
    }
}

/// 外向きの HTTP の応答 (status と本文)。送れなかったら `Err(理由の固定の語)`
pub type HttpResult = Result<(u16, String), String>;

/// webhook の外向きの HTTP (テストは偽物を差す)
pub trait WebhookHttp: Send + Sync {
    /// `url` に `body` を POST する。`headers` は (名前, 値)
    fn post<'a>(
        &'a self,
        url: &'a str,
        headers: Vec<(&'static str, String)>,
        body: String,
    ) -> BoxFuture<'a, HttpResult>;
}

/// やり直しの前の待ち (テストは待たない)
pub trait Sleeper: Send + Sync {
    fn sleep(&self, secs: u64) -> BoxFuture<'_, ()>;
}

/// 送信に要る部品
#[derive(Clone)]
pub struct Sender {
    pub http: Arc<dyn WebhookHttp>,
    pub sleeper: Arc<dyn Sleeper>,
}

/// 有効な設定 (送り先と secret)
struct Target {
    id: Uuid,
    url: String,
    secret: Option<String>,
}

impl TxOutput for Target {}

pub mod sql {
    /// $1 tenant_id / $2 event_type → id, url, secret
    pub const FIND_CONFIG: &str = "SELECT id, url, secret FROM alc_api.webhook_configs \
         WHERE tenant_id = $1 AND event_type = $2 AND enabled = TRUE";

    /// 配信の記録。$1 tenant_id / $2 config_id / $3 event_type / $4 payload / $5 status_code / $6 response_body /
    /// $7 attempt / $8 success (成功のときだけ delivered_at に今を入れる)
    pub const RECORD: &str = "INSERT INTO alc_api.webhook_deliveries \
             (tenant_id, config_id, event_type, payload, status_code, response_body, attempt, delivered_at, success) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, CASE WHEN $8 THEN NOW() END, $8)";
}

/// 再送の前の待ち (秒)。rust の `[1, 5, 25]` のうち、3 回目の後は送らないので使うのは前の 2 つ
pub const DELAYS: [u64; 2] = [1, 5];

/// `X-Webhook-Signature` の値 (`sha256=` + 本文の HMAC-SHA256 の hex)
pub fn signature(secret: &str, body: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC は任意の長さの鍵を受ける");
    mac.update(body.as_bytes());
    format!("sha256={:x}", mac.finalize().into_bytes())
}

/// 送る 1 回ぶんのヘッダー
pub fn headers(event_type: &str, secret: Option<&str>, body: &str) -> Vec<(&'static str, String)> {
    let mut h = vec![
        ("Content-Type", "application/json".to_owned()),
        ("X-Webhook-Event", event_type.to_owned()),
    ];
    if let Some(secret) = secret {
        h.push(("X-Webhook-Signature", signature(secret, body)));
    }
    h
}

async fn find(
    pg: &mut PgClient,
    tenant_id: Uuid,
    event_type: &'static str,
) -> Result<Option<Target>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::FIND_CONFIG,
                    &[(&tenant_id, Type::UUID), (&event_type, Type::TEXT)],
                )
                .await?;
            Ok(row.map(|r| Target {
                id: r.get(0),
                url: r.get(1),
                secret: r.get(2),
            }))
        })
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn record(
    pg: &mut PgClient,
    tenant_id: Uuid,
    config_id: Uuid,
    event_type: &'static str,
    payload: Value,
    status_code: Option<i32>,
    response_body: Option<String>,
    attempt: i32,
    success: bool,
) -> Result<u64, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::RECORD,
                &[
                    (&tenant_id, Type::UUID),
                    (&config_id, Type::UUID),
                    (&event_type, Type::TEXT),
                    (&payload, Type::JSONB),
                    (&status_code, Type::INT4),
                    (&response_body, Type::TEXT),
                    (&attempt, Type::INT4),
                    (&success, Type::BOOL),
                ],
            )
            .await
        })
    })
    .await
}

/// 1 件を送る (設定が無ければ何もしない。失敗は固定の語でログに出し、止めない)
pub async fn deliver(
    pg: &futures_util::lock::Mutex<PgClient>,
    sender: &Sender,
    log: &LogSink,
    tenant_id: Uuid,
    event: Event,
) {
    let target = {
        let mut c = pg.lock().await;
        find(&mut c, tenant_id, event.event_type).await
    };
    let target = match target {
        Ok(Some(t)) => t,
        Ok(None) => return,
        Err(e) => {
            log(&format!(
                "tenko: webhook config lookup failed: db ({})",
                kind(&e)
            ));
            return;
        }
    };
    let body = event.payload.to_string();
    for attempt in 1..=3 {
        let h = headers(event.event_type, target.secret.as_deref(), &body);
        let (code, text, ok) = match sender.http.post(&target.url, h, body.clone()).await {
            Ok((code, text)) => (
                Some(i32::from(code)),
                Some(text),
                (200..300).contains(&code),
            ),
            Err(why) => {
                log(&format!("tenko: webhook delivery failed: {why}"));
                (None, Some(why), false)
            }
        };
        let recorded = {
            let mut c = pg.lock().await;
            record(
                &mut c,
                tenant_id,
                target.id,
                event.event_type,
                event.payload.clone(),
                code,
                text,
                attempt,
                ok,
            )
            .await
        };
        if let Err(e) = recorded {
            log(&format!("tenko: webhook record failed: db ({})", kind(&e)));
        }
        if ok {
            return;
        }
        if let Some(secs) = DELAYS.get(attempt as usize - 1) {
            sender.sleeper.sleep(*secs).await;
        }
    }
}

/// 積んだものを順に送る (worker は `ctx.wait_until` の中で呼ぶ)
pub async fn deliver_all(
    pg: Arc<futures_util::lock::Mutex<PgClient>>,
    sender: Sender,
    log: LogSink,
    events: Vec<(Uuid, Event)>,
) {
    for (tenant_id, event) in events {
        deliver(&pg, &sender, &log, tenant_id, event).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn signature_is_hmac_sha256_hex() {
        // RFC 4231 の test case 2 (key "Jefe")
        assert_eq!(
            signature("Jefe", "what do ya want for nothing?"),
            "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        let h = headers("alcohol_detected", None, "{}");
        assert_eq!(
            h,
            vec![
                ("Content-Type", "application/json".to_owned()),
                ("X-Webhook-Event", "alcohol_detected".to_owned()),
            ]
        );
        assert_eq!(headers("x", Some("k"), "{}").len(), 3);
    }

    #[test]
    fn outbox_takes_everything() {
        let o = Outbox::default();
        let t = Uuid::nil();
        o.push(t, Event::new("a", json!(1)));
        o.clone().push(t, Event::new("b", json!(2)));
        let got = o.take();
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].1.event_type, "b");
        assert!(o.take().is_empty());
    }
}
