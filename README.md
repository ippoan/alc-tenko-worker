# alc-tenko-worker

alc の点呼 (tenko) の Cloudflare Worker `alc-tenko`。rust-alc-api (Cloud Run) の `crates/alc-tenko` を移したもの
(W7、Refs ippoan/rust-alc-api#680)。型は ippoan/alc-devices-worker と同じ。呼び手は auth-worker の Service Binding だけ。

W7a は読み取りと管理、W7b は点呼のセッション (`/api/tenko/sessions*`) と webhook の送信、W7c は予定超過の確認
(Cron Trigger で毎分・本番だけ。通知済みの印を先に付けた予定だけ `tenko_overdue` の webhook で送る)。

## 口 (path・method・JSON の形・status は rust-alc-api と同じ)

管理画面・キオスク (`X-Tenant-ID` 必須、dev端末の印 `X-Device-Dev: 1` で dev の行を読み書きする):

| path | method | 中身 |
|---|---|---|
| `/api/tenko/records` | GET | 点呼の記録の一覧 (記録の新しい順。乗務員・種類・状態・期間で絞る) |
| `/api/tenko/records/csv` | GET | 同じ絞り込みの全件の CSV (BOM 付き。判定は点呼のセッションから) |
| `/api/tenko/schedules` | GET・POST | 点呼の予定の一覧・作成 |
| `/api/tenko/schedules/batch` | POST | 予定のまとめての作成 |
| `/api/tenko/schedules/{id}` | GET・PUT・DELETE | 予定の 1 件・更新・削除 (消費済みは 404) |
| `/api/tenko/schedules/pending/{employee_id}` | GET | 乗務員の未消費の予定 (キオスク) |
| `/api/tenko/health-baselines` | GET・POST | 健康の基準値の一覧・作成か上書き |
| `/api/tenko/health-baselines/{employee_id}` | GET・PUT・DELETE | 基準値の 1 件・更新・削除 |
| `/api/tenko/equipment-failures` | GET・POST | 機器故障の一覧・記録 |
| `/api/tenko/equipment-failures/csv` | GET | 機器故障の CSV |
| `/api/tenko/equipment-failures/{id}` | GET・PUT | 1 件・解決の記録 |
| `/api/tenko/webhooks` | GET・POST | webhook の設定の一覧・作成か上書き (送信はしない) |
| `/api/tenko/webhooks/{id}` | GET・DELETE | 設定の 1 件・削除 |
| `/api/tenko/webhooks/{id}/deliveries` | GET | 配信の記録 (新しい順に 100 件) |
| `/api/tenko/dashboard` | GET | ダッシュボード (件数と予定超過) |
| `/api/tenko/daily-health-status` | GET | 日々の健康状態 (`?date=`、省略は JST の今日) |
| `/api/tenko/driver-info/{employee_id}` | GET | 乗務員情報 (点呼で見せる項目) |
| `/api/tenko-call/numbers` | GET・POST | 点呼の電話の番号 (テナントで絞る) |
| `/api/tenko-call/numbers/{id}` | DELETE | 番号の削除 (204) |
| `/api/tenko-call/drivers` | GET | 点呼の電話の乗務員 (テナントで絞る) |
| `/api/tenko/sessions` | GET | 点呼のセッションの一覧 (作成の新しい順。乗務員・状態・種類・方法・未判定・期間で絞る) |
| `/api/tenko/sessions/{id}` | GET | セッションの 1 件 |
| `/api/tenko/sessions/start` | POST | 開始 (201。予定ありは自動点呼で予定を消費、予定なしは遠隔点呼) |
| `/api/tenko/sessions/{id}/medical` | PUT | 体温・血圧 (業務前。自動点呼は血圧必須) |
| `/api/tenko/sessions/{id}/self-declaration` | PUT | 自己申告と安全判定 (失敗は中断・記録・webhook) |
| `/api/tenko/sessions/{id}/daily-inspection` | PUT | 日常点検 (NG は中止・記録・webhook) |
| `/api/tenko/sessions/{id}/carrying-items` | PUT | 携行品の確認 |
| `/api/tenko/sessions/{id}/alcohol` | PUT | アルコールの結果 (検知は中止・記録・webhook) |
| `/api/tenko/sessions/{id}/instruction-confirm` | PUT | 指示の確認 (完了・記録) |
| `/api/tenko/sessions/{id}/report` | PUT | 運行報告 (業務後。音声は URL だけ。webhook) |
| `/api/tenko/sessions/{id}/escalate-remote` | PUT | 自動点呼から遠隔点呼へ切り替え |
| `/api/tenko/sessions/{id}/cancel` | POST | 中止 (記録) |
| `/api/tenko/sessions/{id}/interrupt` | POST | 管理者の中断 (webhook) |
| `/api/tenko/sessions/{id}/resume` | POST | 管理者の再開 (`X-User-*` が要る) |
| `/api/tenko/sessions/{id}/self-resume` | POST | キオスクの自己再開 (1 回まで) |
| `/api/tenko/sessions/{id}/judgment` | POST | 運行管理者の判定 (管理者ログインか運行管理者の鍵) |

点呼の電話のアプリ (テナントのヘッダー無し。テナントは点呼用の番号・電話番号から引く):

| path | method | 中身 |
|---|---|---|
| `/api/tenko-call/register` | POST | 乗務員の登録 (未登録の番号は 400 と文言) |
| `/api/tenko-call/tenko` | POST | 点呼の位置の記録 (未登録の電話番号は 404) |

ほかの worker から Service Binding で届くだけの口 (`/api` の外。auth-worker の振り分けには載せない。`X-Tenant-ID` が要る):

| path | method | 中身 |
|---|---|---|
| `/internal/webhooks/fire` | POST | 点呼の節目の webhook を預かる (202。測定の口が段を進めたときに渡す。送るのは応答の後) |

## 開発

`CLAUDE.md` のコマンドを参照。デプロイは `.github/workflows/deploy.yml` (main = staging、タグ `v*` = 本番)。
