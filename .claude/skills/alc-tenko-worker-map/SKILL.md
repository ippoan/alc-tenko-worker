---
name: alc-tenko-worker-map
generated-from: alc-tenko-worker:1e4ce883877f7aadacca5ebbf3e1937d735e81f0
paths: [src/, crates/, scripts/, .github/workflows/, wrangler.toml, coverage_100.toml]
description: alc-tenko-worker (点呼 tenko の Cloudflare Worker `alc-tenko` と口・SQL の crate `tenko`。workers-rs + tokio-postgres、ippoan/rust-alc-api の crates/alc-tenko から分離) の構造ナビゲーション。どのモジュールがどの口を持つか / 点呼の届出 (filing-settings・filings・filing-archives) / webhook とメールの送信 / 予定超過の Cron / 実 DB のテストの走らせ方 / CI の本数固定と coverage 100% / デプロイとタグ を 1 枚にまとめる。トリガー:「alc-tenko」「alc-tenko-worker」「tenko worker」「点呼の worker」「/api/tenko」「点呼の届出」「filing-settings」「filings」「filing-archives」「到達番号」「tenko_filings」「tenko_offices」「受委託」「consignments」「companies」「overdue」「予定超過」「webhooks/fire」「点呼の異常のメール」「TENKO_HYPERDRIVE」「TENKO_FILINGS_R2」「TENKO_TEST_ADMIN_DATABASE_URL」「ALC_MIGRATIONS_REV」等。
---

# alc-tenko-worker-map — alc-tenko-worker 構造ナビゲーション

点呼 (tenko) の Cloudflare Worker `alc-tenko` (staging は `alc-tenko-staging`)。rust-alc-api の `crates/alc-tenko` を移す先
(W7、Refs ippoan/rust-alc-api#680)。path・method・JSON の形・status は rust と同じ (alc-app の管理画面・キオスク・点呼の電話の
アプリがそのまま使う)。届出・事業者・受委託・申請の下書きは rust に無い口 (Refs ippoan/alc-app#676)。
直下の `Cargo.toml` が workspace の root (`members = ["crates/tenko"]`、`Cargo.lock` は直下の 1 つ)。規範の正本は `CLAUDE.md`、
口の一覧の正本は `README.md`。

| 場所 | 中身 |
|---|---|
| `src/lib.rs` | worker (wasm 専用)。`tenko::router()` を workers-rs に載せ、応答の後に `ctx.wait_until` で webhook (`deliver_all`) とメール (`notify_all`) を送る。`scheduled` (Cron、本番だけ) から `overdue::run` |
| `src/db.rs` | DB への経路の 1 か所: staging = Workers VPC `TENKO_DB_VPC` の先の PgBouncer / 本番 = Hyperdrive `TENKO_HYPERDRIVE` / local = 文字列 `DATABASE_URL`。`src/tcp.rs` は VPC の `connect()` の extern |
| `src/webhook.rs`・`src/mail.rs`・`src/r2.rs` | 外向きの部品 (wasm 専用、CI のテストと coverage の外): webhook の HTTP (リダイレクトを追わず 7 秒) / `send_email` の binding `ANOMALY_EMAIL` / R2 の binding `TENKO_FILINGS_R2` の `FileStore` 実装 |
| `crates/tenko/src/lib.rs` | `router()`: `/api` の下は tenant の口を `merge` で束ね `require_tenant_header` と `with_device_dev_slot` を掛け、`call::public_router()` だけヘッダー無し。`/internal` の下は `fire` だけ。`AppState`・`set_device_dev`・`db_fail` (固定の語のログ) |
| `crates/tenko/tests/*_db.rs` + `tests/support/mod.rs` | 実 DB のテスト (口から DB まで)。support が使い捨ての postgres に alc-migrations を流し、`alc_api_rt` で繋ぐ。`TABLES` は読み取り専用ロールへ GRANT する表の一覧 |

## 口 → モジュール (`crates/tenko/src/`。router の merge の順)

| モジュール | 口 (`/api` の下) | 一言 |
|---|---|---|
| `records.rs` | `GET /tenko/records`・`/tenko/records/csv` | 点呼の記録の一覧・CSV |
| `schedules.rs` | `/tenko/schedules` (GET・POST)・`/batch`・`/{id}` (GET・PUT・DELETE)・`/pending/{employee_id}` | 点呼の予定 |
| `settings.rs` | `GET`・`PUT /tenko/settings` | 予定超過の分数 (行が無ければ 60) |
| `baselines.rs` | `/tenko/health-baselines`・`/{employee_id}` | 健康の基準値 |
| `failures.rs` | `/tenko/equipment-failures`・`/csv`・`/{id}`・`/{id}/history` | 機器故障 |
| `webhooks.rs` | `/tenko/webhooks`・`/{id}`・`/{id}/deliveries` | webhook の設定と配信の記録 (送信はしない) |
| `dashboard.rs` | `GET /tenko/dashboard` | 件数と予定超過 (`overdue::sql::IS_OVERDUE` を共有) |
| `daily_health.rs` | `GET /tenko/daily-health-status` | 日々の健康状態 |
| `driver_info.rs` | `GET /tenko/driver-info/{employee_id}` | 乗務員情報 8 項目 (項目ごとに別の tx、失敗は空配列) |
| `filing.rs` | `GET`・`PUT /tenko/filing-settings?company_id=` | 届出に差し込む値 (事業者ごと、admin だけ、表 `tenko_company_filing_settings`)。営業所は `office_id` で `tenko_offices` を参照 (名称・住所は保存時に写し、応答で今の値に引き直す)。`partners` は書かず、他の事業者の営業所から導く (`refresh`) |
| `filing_archives.rs` | `GET /tenko/filing-archives`・`/{到達番号}` (GET・PUT)・`/{到達番号}/companies` (PUT) | e-Gov の申請済みの控え (zip) を到達番号で R2 に置く (admin だけ。DB に行を持たない、事業者の振り分けは `tenko_filing_archive_companies`)。置き場が無い env.local は 503 |
| `filings.rs` | `POST`・`GET /tenko/filings?company_id=`・`PATCH`・`DELETE /tenko/filings/{id}` | 届出の申請の下書き (admin だけ、migration 215 の `tenko_filings`)。営業所は `tenko_offices` の写し。PATCH で到達番号を付けると同じ tx で `tenko_filing_archive_companies` に追記 (ON CONFLICT DO NOTHING)。消せるのは下書き (到達番号なし) だけ |
| `offices.rs` | `/tenko/offices`・`/{id}` | 点呼の営業所・運行管理者・車両の所属・`company_id` |
| `companies.rs` | `/tenko/companies`・`/{id}` | 点呼の事業者と `is_self` (admin だけ) |
| `consignments.rs` | `/tenko/consignments`・`/{id}` | 受委託 (委託 → 受託の営業所の組、admin だけ) |
| `call.rs` | `/tenko-call/numbers`・`/{id}`・`/drivers` / 公開: `/tenko-call/register`・`/tenko-call/tenko` | 点呼の電話。公開の 2 口はテナントのヘッダーを見ない |
| `fire.rs` | `POST /internal/webhooks/fire` (`/api` の外) | 測定の口 (alc-measurements-worker) から webhook を預かり `outbox` に積む |
| `deliver.rs`・`overdue.rs`・`mail.rs` | (口なし) | webhook の送信 (署名・再送・記録) / 予定超過の確認 (`CLAIM` で印を先に付けた予定だけ送る) / 点呼の異常のメール |
| `models.rs` | (口なし) | 行の型・列・頁 |

## テストと gate

- 準備: `bash scripts/fetch-migrations.sh` が `scripts/ALC_MIGRATIONS_REV` (rev はこの 1 行だけ) の ippoan/alc-migrations を
  直下の `.alc-migrations/` (`.gitignore` 済み) に取り出す。通していないと DB テストが落ちる。
- 実行: `TENKO_TEST_ADMIN_DATABASE_URL=postgresql://postgres:<pw>@127.0.0.1:<port>/postgres cargo test --workspace`。
  接続先は**使い捨ての postgres:17** (準備は 1 回きりで冪等でない。ほかの worker と DB を共有しない)。URL が無ければ失敗 (skip しない)。
- CI (`.github/workflows/ci.yml`): check-exposure → 陰性対照 → fmt → clippy (native と wasm32) → fetch-migrations →
  `cargo llvm-cov -p tenko --no-report` を 1 回 → **test target ごとの本数を awk/grep で固定** (`Running tests/<名前>.rs` の
  `test result: ok. N passed`) → `scripts/check_coverage_100.sh` が `coverage_100.toml` に登録したファイルの行カバレッジ 100% を検める。
  worker 本体 (`src/`) は wasm 専用で計測の外。

## デプロイ

- `.github/workflows/deploy.yml`: PR = `wrangler deploy --dry-run` (トップレベルと staging) / main へのマージ = staging /
  タグ `v*` = 本番。
- 本番のタグは `tag-release.yml` を**手で** `workflow_dispatch` (入力 `bump`)。マージでの自動タグは無い。手で `v*` を push しない。
- 本番・staging とも Service Binding だけ (`workers_dev`/`preview_urls` = false、route 無し)。Hyperdrive と Cron は本番だけ
  (env.staging / env.local は `crons = []`)。`scripts/check-exposure.sh` が検める。`wrangler.toml` の表の順を変えない。

## 罠 (症状 → 対処)

| 症状 | 対処 |
|---|---|
| 新しい表を使う DB テストが `relation does not exist` や権限の不足で落ちる | `scripts/ALC_MIGRATIONS_REV` をその migration を含む rev に上げ、`fetch-migrations.sh` をやり直す。表を `tests/support/mod.rs` の `TABLES` にも足す |
| テストを足したのに CI の本数の step が落ちる / 新しい `tests/*_db.rs` が数えられていない | `ci.yml` の awk/grep の行の本数を上げる、または target の行を足す (step の name も)。`CLAUDE.md` の CI の本数も合わせる |
| 新しいモジュールを足しても coverage の gate が見ない | `coverage_100.toml` に `[[files]]` を足す (登録は外さない) |
| 作った人の氏名が空で保存される | 分割 worker の `AuthUser.name` は空 (auth-worker が名前を渡さない)。氏名は画面から本文で送らせる (`filings.rs` の `created_by_name`) |
| R2 を読む処理で `Send` でない future のコンパイルエラー | `tenant_tx` の中で R2 を await しない。R2 の後に別の短い `tenant_tx` で読む (`filing_archives.rs`) |
| Hyperdrive 経由だけで接続が切れる | 名前付き prepared statement を使っている。`TenantTx` の `query_typed` 系だけにする |

## 依存の固定 (上げ方は CLAUDE.md)

| 依存 | 出どころ | 版を書く場所 (1 か所) |
|---|---|---|
| `alc-core-wasm`・`alc-worker-db` | ippoan/alc-worker-kit (public) の git 依存・rev 固定 (2 つとも同じ rev。alc-devices-worker と同じ rev) | 直下の `Cargo.toml` の `[workspace.dependencies]`。出どころが 2 つになると型が別になり全リクエストが 500 |
| テストが流す SQL (`init_local_db.sql`・`local_app_grants.sql`・`migrations/`) | ippoan/alc-migrations (public) | `scripts/ALC_MIGRATIONS_REV` |

## 関連 skill

- `alc-workers-ops` — 分割 worker 全体の運用 (migration の置き場・デプロイと版・secret・auth-worker の振り分け)
- `alc-vein-worker-map`・`alc-dtako-worker-map` — 同じ型のほかの worker の地図
