//! 点呼の節目の webhook を、ほかの worker から預かる口 (Refs ippoan/rust-alc-api#680)。
//!
//! 点呼の記録の入口を測定の口 (ippoan/alc-measurements-worker) へ一本化すると、段 (アルコール検知・運行報告・中断・
//! 日常点検 NG) を進める worker が測定の口になる。送信 (署名・再送・配信の記録) は [`crate::deliver`] の 1 か所に置いたまま
//! にするため、測定の口は commit の後に Service Binding でここへ渡す。
//!
//! - `POST /internal/webhooks/fire` (`X-Tenant-ID` が要る) — 本文 `{"event_type", "payload"}`。積んで 202 を返す。
//!   送るのは worker が応答の後に `ctx.wait_until` で ([`crate::sessions`] の口と同じ控え)
//!
//! **`/api` の下に置かない。** auth-worker は `/api/...` の path だけを Service Binding に渡すので、この口には外から届かない
//! (この worker の入口は Service Binding だけ)。auth-worker の振り分けの表にも載せない。
//!
//! 受ける種類は [`EVENT_TYPES`] の 4 つだけ。payload の `tenant_id` はヘッダーのテナントと同じでなければ 400
//! (別のテナントの送り先へ本文を渡さない)。**dev端末の要求 (`X-Device-Dev: 1`) は 202 で受けて積まない** (段の口と同じ)。

use alc_core_wasm::{DeviceDevSlot, TenantId};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::Value;

use crate::deliver::Event;
use crate::AppState;

/// 預かる webhook の種類 (段の口が送るもの。予定超過 `tenko_overdue` はこの worker の定時実行が送るので入れない)
pub const EVENT_TYPES: [&str; 4] = [
    "alcohol_detected",
    "report_submitted",
    "tenko_interrupted",
    "inspection_ng",
];

/// `POST /internal/webhooks/fire` の本文
#[derive(Debug, Deserialize)]
pub struct Fire {
    pub event_type: String,
    pub payload: Value,
}

pub fn router() -> Router<AppState> {
    Router::new().route("/webhooks/fire", post(fire_handler))
}

async fn fire_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Json(body): Json<Fire>,
) -> StatusCode {
    let Some(event_type) = EVENT_TYPES.into_iter().find(|t| *t == body.event_type) else {
        return StatusCode::BAD_REQUEST;
    };
    if body.payload.get("tenant_id").and_then(Value::as_str) != Some(&tenant_id.to_string()) {
        return StatusCode::BAD_REQUEST;
    }
    if !dev.get() {
        state
            .outbox
            .push(tenant_id, Event::new(event_type, body.payload));
    }
    StatusCode::ACCEPTED
}
