//! 点呼の設定 (alc-migrations 204 の `tenko_settings`。テナントに 1 行)。予定超過とする分数 (自動点呼機器の認定要件の
//! 業務前 十九・業務後 十)。超過の確認 ([`crate::overdue`]) がこの値を使う。rust-alc-api には無い口。
//!
//! - `GET /tenko/settings` — `{"overdue_minutes": 60}`。行が無ければ既定の [`DEFAULT_OVERDUE_MINUTES`]
//! - `PUT /tenko/settings` — 本文 `{"overdue_minutes": N}` (1〜1440 の整数) で置き換える (upsert)。応答は保存後の設定。
//!   外れた値・読めない本文は 400 `{"error": "<語>"}`
//!
//! 権限は予定 ([`crate::schedules`]) の口と同じ (テナントのヘッダーだけ。auth-worker が役割を見て通す)。この表は dev の軸を持たない。

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::overdue::DEFAULT_OVERDUE_MINUTES;
use crate::AppState;

/// 分数の範囲 (表の CHECK と同じ)
pub const MIN_OVERDUE_MINUTES: i32 = 1;
pub const MAX_OVERDUE_MINUTES: i32 = 1440;

/// GET の応答と PUT の本文・応答
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenkoSettings {
    pub overdue_minutes: i32,
}

impl TxOutput for TenkoSettings {}

pub mod sql {
    /// $1 tenant_id
    pub const GET: &str = "SELECT overdue_minutes FROM alc_api.tenko_settings WHERE tenant_id = $1";

    /// $1 tenant_id / $2 overdue_minutes
    pub const UPSERT: &str = "INSERT INTO alc_api.tenko_settings (tenant_id, overdue_minutes) VALUES ($1, $2) \
         ON CONFLICT (tenant_id) DO UPDATE SET overdue_minutes = EXCLUDED.overdue_minutes, updated_at = NOW() \
         RETURNING overdue_minutes";
}

pub async fn get_settings(pg: &mut PgClient, tenant_id: Uuid) -> Result<TenkoSettings, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::GET, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(TenkoSettings {
                overdue_minutes: row.map_or(DEFAULT_OVERDUE_MINUTES, |r| r.get(0)),
            })
        })
    })
    .await
}

pub async fn put_settings(
    pg: &mut PgClient,
    tenant_id: Uuid,
    overdue_minutes: i32,
) -> Result<TenkoSettings, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::UPSERT,
                    &[(&tenant_id, Type::UUID), (&overdue_minutes, Type::INT4)],
                )
                .await?;
            Ok(TenkoSettings {
                overdue_minutes: row.get(0),
            })
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new().route("/tenko/settings", get(get_handler).put(put_handler))
}

fn error(word: &'static str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": word }))).into_response()
}

async fn get_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<TenkoSettings>, StatusCode> {
    let mut pg = state.pg.lock().await;
    get_settings(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("settings get", &e))
}

async fn put_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    body: axum::body::Bytes,
) -> Response {
    let Ok(body) = serde_json::from_slice::<TenkoSettings>(&body) else {
        return error("invalid_body");
    };
    if !(MIN_OVERDUE_MINUTES..=MAX_OVERDUE_MINUTES).contains(&body.overdue_minutes) {
        return error("invalid_overdue_minutes");
    }
    let mut pg = state.pg.lock().await;
    match put_settings(&mut pg, tenant_id, body.overdue_minutes).await {
        Ok(s) => Json(s).into_response(),
        Err(e) => state.db_fail("settings put", &e).into_response(),
    }
}
