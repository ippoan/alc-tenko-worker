//! 点呼 (tenko) の口 (W7a、Refs ippoan/rust-alc-api#680)。rust-alc-api の `crates/alc-tenko` の写しで、
//! path・method・JSON の形・status は rust と同じ (alc-app の管理画面・キオスク・点呼の電話のアプリがそのまま使う)。
//! W7a は読み取りと管理、W7b は点呼のセッション (`/tenko/sessions*`) と webhook の送信、W7c は予定超過の確認 ([`overdue`])。
//!
//! - [`records`] — 点呼の記録の一覧・CSV
//! - [`schedules`] — 点呼の予定 (一覧・作成・一括・1 件・更新・削除・乗務員の未消費の予定)
//! - [`baselines`] — 健康の基準値
//! - [`failures`] — 機器故障 (一覧・作成・1 件・解決・CSV)
//! - [`webhooks`] — webhook の設定と配信の記録 (送信はしない)
//! - [`dashboard`] — ダッシュボードの件数と予定超過
//! - [`daily_health`] — 日々の健康状態
//! - [`driver_info`] — 乗務員情報 (点呼で見せる 8 つの項目)
//! - [`call`] — 点呼の電話 (番号・乗務員の管理と、アプリからの登録・点呼の位置の記録)
//! - [`sessions`] — 点呼のセッション (一覧・1 件・開始と各段・安全判定・運行管理者の判定。W7b)
//! - [`deliver`] — webhook の送信 (口は [`deliver::Outbox`] に積み、worker が応答の後に `ctx.wait_until` で送る。W7b)
//! - [`overdue`] — 予定超過の確認 (worker の Cron Trigger が毎分呼ぶ。通知済みの印を先に付けた予定だけ送る。W7c)
//! - [`fire`] — ほかの worker (測定の口) から点呼の節目の webhook を預かる口 (`/internal/webhooks/fire`。`/api` の外)
//!
//! **点呼の電話のアプリの口 (`/tenko-call/register`・`/tenko-call/tenko`) だけがテナントのヘッダーを要らない** (rust でも公開の口。
//! テナントは点呼用の番号・電話番号から引く)。ほかは `X-Tenant-ID` を要る (auth-worker が付ける。無い・UUID でない → 401。
//! `alc_core_wasm::require_tenant_header`)。dev端末の要求 (`X-Device-Dev: 1`) では transaction の中で `app.device_dev` を
//! 立てる (`tenko_records`・`tenko_schedules`・`equipment_failures`・`webhook_deliveries`・`tenko_sessions` の RLS が
//! dev の行と本番の行を分ける。rust の `TenantConn` と同じ)。
//! DB は `PgClient::tenant_tx` + `query_typed` 系だけ (名前付き prepared statement は使わない)。WHERE に tenant_id も書く
//! (rust は RLS 任せの文もあった)。ログ・エラー応答は固定の語だけ (エラーの文・識別子・名前を載せない)。

pub mod baselines;
pub mod call;
pub mod daily_health;
pub mod dashboard;
pub mod deliver;
pub mod driver_info;
pub mod failures;
pub mod fire;
pub mod models;
pub mod overdue;
pub mod records;
pub mod schedules;
pub mod sessions;
pub mod webhooks;

use std::sync::Arc;

use alc_core_wasm::{require_tenant_header, DeviceDevSlot};
use alc_worker_db::{kind, PgClient, TenantTx};
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::Router;
use futures_util::lock::Mutex;

pub type LogSink = Arc<dyn Fn(&str) + Send + Sync>;

/// 今の時刻 (unix ms)。Workers では `Date.now()`、テストは固定の値 (日々の健康状態の既定の日付)
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

#[derive(Clone)]
pub struct AppState {
    pub pg: Arc<Mutex<PgClient>>,
    pub log: LogSink,
    pub clock: Clock,
    /// 要求の中で積む webhook の控え (worker が応答の後に送る)
    pub outbox: deliver::Outbox,
}

impl AppState {
    /// DB の失敗を固定の語でログに出し、500 にする
    fn db_fail(&self, stage: &str, e: &tokio_postgres::Error) -> StatusCode {
        (self.log)(&format!("tenko: {stage} failed: db ({})", kind(e)));
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// dev端末の要求で、transaction の中だけ `app.device_dev` を `'1'` にする文 (固定。値を埋め込まない)。
/// rust の `alc_core::tenant::set_current_tenant` と同じ働き (rust は接続ごと、ここは transaction-local)
pub const SET_DEVICE_DEV: &str = "SELECT set_config('app.device_dev', '1', true)";

/// `dev` なら [`SET_DEVICE_DEV`] を流す (tenant_tx の最初の文。dev でなければ何もしない = 設定は空のまま)
pub async fn set_device_dev(tx: &TenantTx<'_>, dev: bool) -> Result<(), tokio_postgres::Error> {
    if dev {
        tx.execute_typed(SET_DEVICE_DEV, &[]).await?;
    }
    Ok(())
}

/// 要求ごとに空の `DeviceDevSlot` を置く (kit の `require_tenant_header` は、在る slot にだけ dev の印を書く。
/// 共有の 1 つを使い回さない)
pub async fn with_device_dev_slot(mut req: Request, next: Next) -> Response {
    req.extensions_mut().insert(DeviceDevSlot::default());
    next.run(req).await
}

/// 全部の口。rust と同じく `/api` の下 (auth-worker は `/api/...` のまま Service Binding に渡す)。
/// `/internal` の下はほかの worker から Service Binding で届くだけの口 ([`fire`])
pub fn router() -> Router<AppState> {
    // 後に足した route_layer が外側: slot を置く → tenant と dev の印を読む → 口
    let tenant = records::router()
        .merge(schedules::router())
        .merge(baselines::router())
        .merge(failures::router())
        .merge(webhooks::router())
        .merge(dashboard::router())
        .merge(daily_health::router())
        .merge(driver_info::router())
        .merge(call::tenant_router())
        .merge(sessions::router())
        .route_layer(middleware::from_fn(require_tenant_header))
        .route_layer(middleware::from_fn(with_device_dev_slot));
    let api = tenant.merge(call::public_router());
    // ほかの worker から Service Binding で届くだけの口 (auth-worker は `/api` の下しか渡さない)
    let internal = fire::router()
        .route_layer(middleware::from_fn(require_tenant_header))
        .route_layer(middleware::from_fn(with_device_dev_slot));
    Router::new().nest("/api", api).nest("/internal", internal)
}
