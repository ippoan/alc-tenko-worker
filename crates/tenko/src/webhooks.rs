//! webhook の設定と配信の記録 (rust の `alc_tenko::tenko_webhooks`・repo/tenko_webhooks.rs と、
//! `alc_core::webhook::validate_webhook_url` の写し)。**送信はしない** (点呼のセッションの節目の送信は W7b、予定超過は W7c)。
//!
//! - `POST /tenko/webhooks` (201) — 作成か上書き (テナントと種類ごとに 1 件)。種類は決まった 9 つ、URL は https の外向きだけ (外れたら 400)
//! - `GET /tenko/webhooks` — 一覧 (種類の順)。`GET`・`DELETE /tenko/webhooks/{id}` — 1 件・削除 (204)。無ければ 404
//! - `GET /tenko/webhooks/{id}/deliveries` — 配信の記録 (新しい順に 100 件)
//!
//! 応答に secret は載せない (rust と同じ `skip_serializing`)。

use alc_core_wasm::{DeviceDevSlot, TenantId};
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::models::Arg;
use crate::{set_device_dev, AppState};

/// 設定できる種類 (rust と同じ)
pub const EVENT_TYPES: [&str; 9] = [
    "alcohol_detected",
    "tenko_overdue",
    "tenko_completed",
    "tenko_cancelled",
    "tenko_interrupted",
    "inspection_ng",
    "safety_judgment_fail",
    "equipment_failure",
    "report_submitted",
];

/// rust の `WebhookConfig`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WebhookConfig {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub event_type: String,
    pub url: String,
    #[serde(skip_serializing)]
    pub secret: Option<String>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// rust の `CreateWebhookConfig` (`enabled` の既定は true)
#[derive(Debug, Clone, Deserialize)]
pub struct CreateWebhook {
    pub event_type: String,
    pub url: String,
    pub secret: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// rust の `WebhookDelivery`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WebhookDelivery {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub config_id: Uuid,
    pub event_type: String,
    pub payload: Value,
    pub status_code: Option<i32>,
    pub response_body: Option<String>,
    pub attempt: i32,
    pub delivered_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub success: bool,
}

impl TxOutput for WebhookConfig {}
impl TxOutput for WebhookDelivery {}

pub mod sql {
    /// 作成か上書き。$1 tenant_id / $2 event_type / $3 url / $4 secret / $5 enabled
    pub const UPSERT: &str = "INSERT INTO alc_api.webhook_configs (tenant_id, event_type, url, secret, enabled) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (tenant_id, event_type) DO UPDATE SET url = $3, secret = $4, enabled = $5, updated_at = NOW() \
         RETURNING id, tenant_id, event_type, url, secret, enabled, created_at, updated_at";

    pub const LIST: &str =
        "SELECT id, tenant_id, event_type, url, secret, enabled, created_at, updated_at \
         FROM alc_api.webhook_configs WHERE tenant_id = $1 ORDER BY event_type";

    /// $1 id / $2 tenant_id
    pub const GET: &str =
        "SELECT id, tenant_id, event_type, url, secret, enabled, created_at, updated_at \
         FROM alc_api.webhook_configs WHERE id = $1 AND tenant_id = $2";

    pub const DELETE: &str = "DELETE FROM alc_api.webhook_configs WHERE id = $1 AND tenant_id = $2";

    /// 配信の記録 (新しい順に 100 件)。$1 config_id / $2 tenant_id
    pub const DELIVERIES: &str =
        "SELECT id, tenant_id, config_id, event_type, payload, status_code, response_body, \
             attempt, delivered_at, created_at, success \
         FROM alc_api.webhook_deliveries WHERE config_id = $1 AND tenant_id = $2 \
         ORDER BY created_at DESC LIMIT 100";
}

fn config(r: &Row) -> WebhookConfig {
    WebhookConfig {
        id: r.get(0),
        tenant_id: r.get(1),
        event_type: r.get(2),
        url: r.get(3),
        secret: r.get(4),
        enabled: r.get(5),
        created_at: r.get(6),
        updated_at: r.get(7),
    }
}

fn delivery(r: &Row) -> WebhookDelivery {
    WebhookDelivery {
        id: r.get(0),
        tenant_id: r.get(1),
        config_id: r.get(2),
        event_type: r.get(3),
        payload: r.get(4),
        status_code: r.get(5),
        response_body: r.get(6),
        attempt: r.get(7),
        delivered_at: r.get(8),
        created_at: r.get(9),
        success: r.get(10),
    }
}

/// 配信先の URL の検査 (SSRF 対策。rust の `validate_webhook_url` と同じ規則): https だけ・利用者情報なし・
/// 内部の IP (private・loopback・link-local・未指定・broadcast・文書用・CGNAT・ULA) と内部の名前を拒む
pub fn valid_webhook_url(raw: &str) -> bool {
    let Ok(url) = url::Url::parse(raw) else {
        return false;
    };
    // host を持たない scheme (data: / javascript: 等) は None で拒む
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    // IPv6 は host_str がブラケット付きで返るので外してから IP 判定
    let candidate = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = candidate.parse::<std::net::IpAddr>() {
        return is_global_ip(ip);
    }
    !(host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".internal")
        || host.ends_with(".local")
        || host == "metadata.google.internal")
}

fn is_global_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            !(v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                // CGNAT 100.64.0.0/10
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 0x40))
        }
        std::net::IpAddr::V6(v6) => {
            !(v6.is_loopback()
                || v6.is_unspecified()
                // unique local fc00::/7
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                // link-local fe80::/10
                || (v6.segments()[0] & 0xffc0) == 0xfe80)
        }
    }
}

pub async fn upsert(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: CreateWebhook,
) -> Result<WebhookConfig, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let args: [Arg; 5] = [
                (&tenant_id, Type::UUID),
                (&input.event_type, Type::TEXT),
                (&input.url, Type::TEXT),
                (&input.secret, Type::TEXT),
                (&input.enabled, Type::BOOL),
            ];
            Ok(config(&tx.query_typed_one(sql::UPSERT, &args).await?))
        })
    })
    .await
}

pub async fn list(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<WebhookConfig>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(config).collect())
        })
    })
    .await
}

pub async fn get_one(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<WebhookConfig>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::GET, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(row.as_ref().map(config))
        })
    })
    .await
}

pub async fn delete(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(sql::DELETE, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

/// 配信の記録 (`webhook_deliveries` は dev の軸を持つ)
pub async fn deliveries(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    config_id: Uuid,
) -> Result<Vec<WebhookDelivery>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let rows = tx
                .query_typed(
                    sql::DELIVERIES,
                    &[(&config_id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(rows.iter().map(delivery).collect())
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tenko/webhooks", post(upsert_handler).get(list_handler))
        .route(
            "/tenko/webhooks/{id}",
            get(get_handler).delete(delete_handler),
        )
        .route("/tenko/webhooks/{id}/deliveries", get(deliveries_handler))
}

async fn upsert_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateWebhook>,
) -> Result<(StatusCode, Json<WebhookConfig>), StatusCode> {
    if !EVENT_TYPES.contains(&body.event_type.as_str()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    if !valid_webhook_url(&body.url) {
        (state.log)("tenko: rejected webhook config with disallowed url");
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let row = upsert(&mut pg, tenant_id, body)
        .await
        .map_err(|e| state.db_fail("webhook upsert", &e))?;
    Ok((StatusCode::CREATED, Json(row)))
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<WebhookConfig>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("webhook list", &e))
}

async fn get_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<WebhookConfig>, StatusCode> {
    let mut pg = state.pg.lock().await;
    get_one(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("webhook get", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn delete_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let deleted = delete(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("webhook delete", &e))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn deliveries_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<WebhookDelivery>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    deliveries(&mut pg, tenant_id, dev.get(), id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("webhook deliveries", &e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webhook_url_rules_match_rust() {
        for ok in [
            "https://example.com/hook",
            "https://hooks.example.com.:8443/x",
            "https://8.8.8.8/x",
            "https://[2001:4860::8888]/x",
        ] {
            assert!(valid_webhook_url(ok), "{ok}");
        }
        for ng in [
            "not a url",
            "data:text/plain,hi",
            "http://example.com/hook",
            "https://user:pw@example.com/",
            "https://user@example.com/",
            "https://10.0.0.1/",
            "https://127.0.0.1/",
            "https://169.254.169.254/",
            "https://0.0.0.0/",
            "https://255.255.255.255/",
            "https://192.0.2.1/",
            "https://100.64.0.1/",
            "https://[::1]/",
            "https://[::]/",
            "https://[fd00::1]/",
            "https://[fe80::1]/",
            "https://localhost/",
            "https://a.localhost/",
            "https://metadata.google.internal/",
            "https://x.internal/",
            "https://printer.local/",
        ] {
            assert!(!valid_webhook_url(ng), "{ng}");
        }
    }
}
