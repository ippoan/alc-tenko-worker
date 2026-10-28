# alc-tenko-worker

点呼 (tenko) の Worker `alc-tenko` (Refs ippoan/rust-alc-api#680)。rust-alc-api の `crates/alc-tenko` を移す先で、
path・method・JSON の形・status は rust と同じ (alc-app の管理画面・キオスク・点呼の電話のアプリがそのまま使う)。
直下 = Worker (wasm 専用)、`crates/tenko/` = 口・SQL。W7 は 4 段 (オーナーの決定): W7-0 使われていない口の整理 /
W7a 読み取りと管理 / W7b 点呼のセッション (`/api/tenko/sessions*`・`judgment`) と webhook の送信 /
**W7c 予定超過の確認を Cron Trigger (毎分) へ (通知済みの印を先に付けた行だけ送る)**。点呼の記録の入口は worker 移行の後に
測定の口 (alc-measurements-worker) へ一本化する (オーナーの決定。段の口はそれまでのつなぎ)。
口の一覧は README。auth-worker の振り分け (`ALC_BINDING_ROUTES`) は別の PR。

## コマンド
```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --target wasm32-unknown-unknown --release -- -D warnings
bash scripts/fetch-migrations.sh   # 版は scripts/ALC_MIGRATIONS_REV
TENKO_TEST_ADMIN_DATABASE_URL=postgresql://postgres:<pw>@127.0.0.1:<port>/postgres cargo test --workspace
worker-build --release && npx wrangler@4.144.0 deploy --dry-run
```
DB テストは使い捨ての postgres:17 へ (ほかの worker のテストと同じ DB を使い回さない。準備は 1 回きりで冪等でない)。
URL が無ければ失敗 (skip しない)。接続ロールは `alc_api_rt`。

## 規範
- 公開範囲を弱めない: 本番・staging とも Service Binding だけ (どの env も `workers_dev`/`preview_urls` = false、route 無し)。
  Hyperdrive (`TENKO_HYPERDRIVE`) は本番だけ。`wrangler.toml` の表の順を変えない (新しい表は placement より後)。
- **ログ・エラー応答は固定の語だけ。** エラーの文 (`{e}`)・tenant_id・社員の名前・電話番号を出さない。
  DB の失敗は `tenko: <段> failed: db (<kind>)`。応答の本文は rust と同じ (多くの失敗は status だけ、点呼の電話の登録は文言つき)。
- **dev の軸:** `tenko_records`・`tenko_schedules`・`equipment_failures`・`webhook_deliveries`・`tenko_sessions` を読み書きする
  transaction は最初に `set_device_dev` (`X-Device-Dev: 1` のとき `app.device_dev` を立てる)。健康の基準値・webhook の設定・
  点呼の電話の表は dev の軸を持たない。
- **点呼の電話のアプリの口** (`/tenko-call/register`・`/tenko-call/tenko`) はテナントのヘッダーを見ない。テナントの決まる前の文は
  SELECT が USING (true) の番号・乗務員の表を読む文だけで、nil のテナントの transaction で流す。書き込みは引いたテナントで。
- SQL に tenant_id を明示する (rust は RLS 任せの文があった)。絞り込みは文を組み立てず `($n IS NULL OR ...)` の 1 文。
- 乗務員情報の任意の項目 (労働時間・指導・携行品・記録・点検・故障・整備) は項目ごとに別の transaction で読み、失敗は空の配列 (rust と同じ)。
- 予定超過の分数は rust の既定値の定数 (`dashboard::OVERDUE_MINUTES` = 60。本番の Cloud Run は `TENKO_OVERDUE_MINUTES` を設定していない)。
- wasm32 で chrono の `Utc::now()` を使わない (時計は `AppState.clock`。Workers は `Date.now()`)。
- DB は `PgClient::tenant_tx` + `query_typed` 系だけ。名前付き prepared statement 禁止。Row を tx の外へ出さない。
- **点呼のセッション** (`sessions.rs`): 口ごとの読み書きを 1 つの transaction にする。点呼の記録 (`tenko_records`、不変) は区切り
  (`SAVEPOINT`) の中で書き、失敗しても区切りだけ戻してセッションの更新は残す (rust の `let _ = create_tenko_record(..)`)。
  記録の `record_data` はセッションの JSON、`record_hash` はその SHA-256 で、ippoan/alc-measurements-worker の通常点呼と同じ作り方・
  同じ列 (`INSERT_RECORD`)。**片方を変えたらもう片方も変える。** `TenkoSession` の欄の名前を変えると hash が変わる。
- **webhook の送信** (`deliver.rs`): 口は送らずに `AppState.outbox` に積み、worker (`src/lib.rs`) が応答の後に `ctx.wait_until` で送る。
  fire-and-forget の spawn はしない。**dev端末の要求では積まない** (rust の `spawn_webhook` と同じ)。署名・再送 (1 秒・5 秒、3 回まで)・
  配信の記録は rust と同じ。外向きの HTTP (`src/webhook.rs`) はリダイレクトを追わず 7 秒で打ち切る (rust は 10 秒。
  3 回 × 7 秒 + 待ち 1 秒 + 5 秒 = 最長 27 秒で、`wait_until` の上限 = 応答の後 30 秒に収める)。
- **予定超過の確認** (`overdue.rs`、W7c): `src/lib.rs` の `scheduled` (wrangler.toml の `[triggers]` = 毎分、**本番だけ**。triggers は env に
  継承されるので env.staging / env.local は `crons = []`、check-exposure.sh が検査) から `overdue::run`。送り先の設定の在るテナントを
  SECURITY DEFINER の関数で引き、**通知済みの印を先に付ける UPDATE** (`sql::CLAIM`) が返した予定だけを `deliver` で送る (2 つの実行が
  重なっても 1 回だけ。rust は読む → 印 → 送るで重なると 2 回送りえた)。超過は 60 分 (rust の既定。本番に env は無かった)。dev の軸は立てない。
- `coverage_100.toml` の登録を外さない。CI の本数 (lib 13・admin_db 3・call_db 2・failures_db 1・overdue_db 3・records_db 3・schedules_db 2・sessions_db 13・views_db 3)
  はテストを足したら上げる。
- public repo: ホスト名・tenant UUID・個人情報を書かない。`v*` タグ = 本番 (Tag Release 手動)。
- kit の rev は直下 `Cargo.toml` の 1 か所 (alc-devices-worker と同じ rev)。
