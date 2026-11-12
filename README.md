# alc-tenko-worker

alc の点呼 (tenko) の Cloudflare Worker `alc-tenko`。rust-alc-api (Cloud Run) の `crates/alc-tenko` を移したもの
(W7、Refs ippoan/rust-alc-api#680)。型は ippoan/alc-devices-worker と同じ。呼び手は auth-worker の Service Binding だけ。

W7a は読み取りと管理、W7b は webhook の送信、W7c は予定超過の確認 (点呼のセッション `/api/tenko/sessions*` は
ippoan/alc-measurements-worker へ移した。Refs ippoan/rust-alc-api#680)
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
| `/api/tenko/equipment-failures/{id}/history` | GET | 変更前の行の履歴 `[{changed_at, old_row}]` (古い順) |
| `/api/tenko/settings` | GET・PUT | 点呼の設定 `{overdue_minutes}` (予定超過の分数 1〜1440。行が無ければ 60・PUT は upsert) |
| `/api/tenko/webhooks` | GET・POST | webhook の設定の一覧・作成か上書き (送信はしない) |
| `/api/tenko/webhooks/{id}` | GET・DELETE | 設定の 1 件・削除 |
| `/api/tenko/webhooks/{id}/deliveries` | GET | 配信の記録 (新しい順に 100 件) |
| `/api/tenko/filing-settings` | GET・PUT | 点呼の届出・認定申請に差し込む値 (テナント 1 行、admin だけ。行が無ければ既定値・PUT は全欄の置き換え)。213 の欄 (事業者のフリガナ・郵便番号・電話・法人番号・代表者の役職・担当者の部門名と住所、受委託の相手の会社 `partners`) は画面が e-Gov の申請データと突き合わせるのに使う。郵便番号はハイフンを落として 7 桁 (`invalid_postal_code`)、法人番号は 13 桁 (`invalid_corporate_number`)、相手の会社は 10 社まで (`too_many_partners`)・名称必須 (`invalid_partner`) |
| `/api/tenko/offices` | GET・POST | 点呼の営業所 (運行管理者・車両の所属つき、4 口とも admin だけ)。GET は `{offices, vehicles, carins_bases}` (未削除の車両と、車検証の使用の本拠+使用者の組の一覧つき。組ごとに営業所を選ぶ。組の値は半角・全角の空白を取り除いて正規化。営業所は所属の点呼端末 `device_ids` つき)・POST は 201 で営業所 1 件 |
| `/api/tenko/offices/{id}` | PUT・DELETE | 営業所の全部の置き換え (`manager_ids`・`vehicle_ids`・`carins_bases`・`device_ids` も。車両は別の営業所から移る)・削除 (204)。無ければ 404 |
| `/api/tenko/dashboard` | GET | ダッシュボード (件数と予定超過) |
| `/api/tenko/daily-health-status` | GET | 日々の健康状態 (`?date=`、省略は JST の今日) |
| `/api/tenko/driver-info/{employee_id}` | GET | 乗務員情報 (点呼で見せる項目) |
| `/api/tenko-call/numbers` | GET・POST | 点呼の電話の番号 (テナントで絞る) |
| `/api/tenko-call/numbers/{id}` | DELETE | 番号の削除 (204) |
| `/api/tenko-call/drivers` | GET | 点呼の電話の乗務員 (テナントで絞る) |

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
