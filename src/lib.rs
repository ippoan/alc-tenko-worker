//! 点呼 (tenko) の Worker `alc-tenko` (Refs ippoan/rust-alc-api#680)。crates/tenko の口
//! (`router`、axum) を workers-rs (`http` / `axum` feature) に載せる。型は ippoan/alc-people-worker と同じ。
//! DB への経路 (staging = Workers VPC の先の PgBouncer、本番 = Hyperdrive) は [`db::connect`] の 1 か所で出し分ける。
//! 定時実行 (`scheduled`、Cron Trigger は本番だけ) は予定超過の確認 (crates/tenko の `overdue`、W7c)。
//!
//! **この Worker は呼び手を認証しない。** 本番・staging とも workers.dev を開けず (`workers_dev` / `preview_urls` = false、
//! `routes` を持たない)、入口は auth-worker からの Service Binding だけ (wrangler.toml と scripts/check-exposure.sh が保証する)。

// worker 本体は wasm32 専用。native の `cargo build` / `clippy` / `test` (workspace の root で打つもの) では空の crate にして、
// crates/tenko だけを検査させる
#![cfg(target_arch = "wasm32")]

mod db;
mod tcp;
mod webhook;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderValue, Response, StatusCode};
use futures_util::lock::Mutex;
use tenko::deliver::{deliver_all, Outbox, Sender};
use tenko::overdue::{self, DEFAULT_OVERDUE_MINUTES};
use tenko::{router, AppState, LogSink};
use tower_service::Service;
use worker::{
    console_error, event, Context, Env, HttpRequest, Result, ScheduleContext, ScheduledEvent,
    WorkerVersionMetadata,
};

fn log_sink() -> LogSink {
    Arc::new(|message| console_error!("{message}"))
}

fn error_response(status: StatusCode, code: &str) -> Response<Body> {
    let mut resp = Response::new(Body::from(format!(r#"{{"error":"{code}"}}"#)));
    *resp.status_mut() = status;
    resp
}

#[event(fetch)]
async fn fetch(req: HttpRequest, env: Env, ctx: Context) -> Result<Response<Body>> {
    // 全リクエストで routing の前に繋ぐ (ippoan/alc-people-worker と同じ)
    let pg = match db::connect(&env).await {
        Ok(c) => c,
        Err(db::ConnectError::NotConfigured) => {
            console_error!("tenko: {}", db::ConnectError::NotConfigured);
            return Ok(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "database_not_configured",
            ));
        }
        Err(e) => {
            console_error!("tenko: {e}");
            return Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ));
        }
    };
    let pg = Arc::new(Mutex::new(pg));
    let outbox = Outbox::default();
    let state = AppState {
        pg: pg.clone(),
        log: log_sink(),
        // wasm32 で chrono の Utc::now() は使えない (時計を持たない) ので、Date.now() を渡す
        clock: Arc::new(|| worker::Date::now().as_millis() as i64),
        outbox: outbox.clone(),
    };
    let mut resp = match router().with_state(state).call(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
    // 点呼の節目の webhook は応答の後で送り切る (fire-and-forget の spawn はしない。rust は tokio::spawn だった)
    let events = outbox.take();
    if !events.is_empty() {
        let sender = Sender {
            http: Arc::new(webhook::WorkerHttp),
            sleeper: Arc::new(webhook::WorkerSleeper),
        };
        ctx.wait_until(deliver_all(pg, sender, log_sink(), events));
    }
    // どの版が応えたかを応答ヘッダーで分かるようにする。binding が取れなければ何も付けない
    if let Ok(meta) = env.get_binding::<WorkerVersionMetadata>("CF_VERSION_METADATA") {
        if let Ok(v) = HeaderValue::from_str(&meta.id()) {
            resp.headers_mut().insert("x-worker-version", v);
        }
        let tag = meta.tag();
        if !tag.is_empty() {
            if let Ok(v) = HeaderValue::from_str(&tag) {
                resp.headers_mut().insert("x-worker-tag", v);
            }
        }
    }
    Ok(resp)
}

/// 予定超過の確認 (wrangler.toml の `[triggers]` = 毎分、本番だけ)。中身は crates/tenko の `overdue::run`。
/// 送り終えるまで待つ (rust は Cloud Run の中の 60 秒のループだった)
#[event(scheduled)]
async fn scheduled(_event: ScheduledEvent, env: Env, _ctx: ScheduleContext) {
    let pg = match db::connect(&env).await {
        Ok(c) => c,
        Err(e) => {
            console_error!("tenko: overdue {e}");
            return;
        }
    };
    let sender = Sender {
        http: Arc::new(webhook::WorkerHttp),
        sleeper: Arc::new(webhook::WorkerSleeper),
    };
    let now = worker::Date::now().as_millis() as i64;
    overdue::run(
        Arc::new(Mutex::new(pg)),
        sender,
        log_sink(),
        now,
        DEFAULT_OVERDUE_MINUTES,
    )
    .await;
}
