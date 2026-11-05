//! 点呼の営業所と、運行管理者・車両の所属 (alc-migrations 205 の `tenko_offices`・`tenko_office_managers`・
//! `maintenance_vehicles.office_id`)。血圧の記録が足りず対面点呼に切り替えた業務前の点呼で、運行管理者の判定のとき、
//! 車両の営業所と判定した運行管理者の所属営業所を比べて点呼の方法を決める (判定は alc-measurements-worker が書く)。
//! この口はその所属を管理画面 (alc-app) から設定するためのもの。rust-alc-api には無い口。
//!
//! - `GET /tenko/offices` — `{"offices": [<営業所>], "vehicles": [<車両>]}`。営業所は名称順、車両は未削除を車両番号順
//! - `POST /tenko/offices` — 本文 `{"name", "company_name"?, "address"?, "manager_ids"?, "vehicle_ids"?}` → 201 で営業所 1 件
//! - `PUT /tenko/offices/{id}` — 同じ本文で全部置き換える (省略した `manager_ids`・`vehicle_ids` は空)。無ければ 404
//! - `DELETE /tenko/offices/{id}` — 204。無ければ 404 (所属は表の外部キーで外れる)
//!
//! 営業所 1 件の形: `{"id", "name", "company_name", "address", "manager_ids": [社員の id], "vehicle_ids": [車両の id],
//! "carins_bases": [{"use_base", "user_name"}]}`。`carins_bases` は車検証の「使用の本拠の位置」+「使用者の氏名又は名称」の組で、
//! 組ごとに営業所を 1 つ選ぶ (車両の所属は基本これで決まり、`vehicle_ids` で手で選んだ車両は例外の上書き)。組は 1 営業所なので別の営業所から移る。
//! `device_ids` は営業所に所属する点呼端末 (auth-worker の端末の鍵の登録簿の id。`^[A-Za-z0-9:_-]{1,64}$`、登録簿に在るかは検査しない。
//! 1 台を複数の営業所に所属させられる (同じ建物の別の会社の営業所が共用する。migration 208)。キオスクの車両の候補の並びだけに使い、
//! 判定には使わない)。
//! GET の応答にはさらに `"carins_bases": [{"use_base", "user_name", "office_id", "vehicle_count"}]` (car_inspection に出てくる組の全部。
//! 使用の本拠が空の組は除く。`vehicle_count` は組ごとの distinct CarId、`office_id` は対応が無ければ null。車検証が空のテナントは空配列)。
//! `company_name` が null なら自社、値があればグループ企業の営業所。
//! 検める欄: 本文が読めない `invalid_body` / 名称が空 `invalid_name` / 長さ `too_long` / 営業所の件数 `too_many_offices` /
//! 組の空 `invalid_carins_base` / 端末の id の形 `invalid_device` / id と組の件数 `too_many_ids` / `manager_ids` に同じテナントの在籍で role に manager か admin を含む社員以外 `invalid_manager` /
//! `vehicle_ids` に同じテナントの未削除の車両以外 `invalid_vehicle`。外れたら 400 `{"error": "<語>"}`。
//! 文字列は前後の空白を落とし、会社名・住所は空なら null。重複した id は 1 つにまとめる。
//! `vehicle_ids` は営業所の車両の置き換え: 一覧に無い車両は所属を外し、一覧の車両はこの営業所へ付け替える (車両は 1 営業所だけ)。
//!
//! **4 口とも admin だけ** (filing ([`crate::filing`]) と同じ判定。`X-User-Role` が admin でなければ 403 `{"error": "forbidden"}`)。
//! 判定の点呼の方法がこの所属で決まるので、届出の設定と同じ強さにする。この表は dev の軸を持たない。

use alc_core_wasm::{AuthUser, TenantId};
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::filing::{error as status_error, is_admin};
use crate::models::Arg;
use crate::AppState;

/// テナントの営業所の件数の上限
pub const MAX_OFFICES: i64 = 100;
/// 1 営業所の運行管理者・車両それぞれの件数の上限
pub const MAX_IDS: usize = 1000;
/// 1 欄 (名称・会社名・住所) の文字数の上限
pub const MAX_CHARS: usize = 500;

/// POST・PUT の本文
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct OfficeInput {
    pub name: String,
    #[serde(default)]
    pub company_name: Option<String>,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub manager_ids: Vec<Uuid>,
    #[serde(default)]
    pub vehicle_ids: Vec<Uuid>,
    #[serde(default)]
    pub carins_bases: Vec<CarinsBase>,
    #[serde(default)]
    pub device_ids: Vec<String>,
}

/// 車検証の「使用の本拠の位置」と「使用者の氏名又は名称」の組 (正規化の式は [`sql::CARINS`]。判定側と同じ式)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CarinsBase {
    pub use_base: String,
    pub user_name: String,
}

/// 一覧の組 (車検証に出てくるもの。`office_id` はその組の対応)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CarinsBaseCount {
    pub use_base: String,
    pub user_name: String,
    pub office_id: Option<Uuid>,
    pub vehicle_count: i64,
}

/// 営業所 1 件
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Office {
    pub id: Uuid,
    pub name: String,
    pub company_name: Option<String>,
    pub address: Option<String>,
    pub manager_ids: Vec<Uuid>,
    pub vehicle_ids: Vec<Uuid>,
    pub carins_bases: Vec<CarinsBase>,
    pub device_ids: Vec<String>,
}

/// 一覧の車両 (所属の付け替えに使う)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Vehicle {
    pub id: Uuid,
    pub registration_number: String,
    pub display_name: Option<String>,
    pub office_id: Option<Uuid>,
}

/// GET の応答
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OfficeList {
    pub offices: Vec<Office>,
    pub vehicles: Vec<Vehicle>,
    pub carins_bases: Vec<CarinsBaseCount>,
}

impl TxOutput for OfficeList {}

/// 保存の結果 (検査と 404 は transaction の中で決まる。書く前に返すので何も残らない)
#[derive(Debug, Clone, PartialEq)]
pub enum Saved {
    Done(Office),
    NotFound,
    Invalid(&'static str),
}

impl TxOutput for Saved {}

impl OfficeInput {
    /// 空白を落として検める。外れた欄の語を返す。id は重複をまとめる
    pub fn normalize(mut self) -> Result<Self, &'static str> {
        self.name = self.name.trim().to_owned();
        self.company_name = clean(self.company_name.take());
        self.address = clean(self.address.take());
        if self.name.is_empty() {
            return Err("invalid_name");
        }
        if [
            Some(&self.name),
            self.company_name.as_ref(),
            self.address.as_ref(),
        ]
        .into_iter()
        .flatten()
        .any(|s| s.chars().count() > MAX_CHARS)
        {
            return Err("too_long");
        }
        for b in &mut self.carins_bases {
            b.use_base = b.use_base.trim().to_owned();
            b.user_name = b.user_name.trim().to_owned();
        }
        if self
            .carins_bases
            .iter()
            .any(|b| b.use_base.is_empty() || b.user_name.is_empty())
        {
            return Err("invalid_carins_base");
        }
        if self.carins_bases.iter().any(|b| {
            b.use_base.chars().count() > MAX_CHARS || b.user_name.chars().count() > MAX_CHARS
        }) {
            return Err("too_long");
        }
        for d in &mut self.device_ids {
            *d = d.trim().to_owned();
        }
        // 端末の id の形 (表の CHECK `^[A-Za-z0-9:_-]{1,64}$` と同じ。登録簿に在るかは検査しない)
        if self.device_ids.iter().any(|d| {
            !(1..=64).contains(&d.len())
                || !d
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b':' | b'_' | b'-'))
        }) {
            return Err("invalid_device");
        }
        self.device_ids.sort_unstable();
        self.device_ids.dedup();
        self.carins_bases.sort_unstable();
        self.carins_bases.dedup();
        self.manager_ids.sort_unstable();
        self.manager_ids.dedup();
        self.vehicle_ids.sort_unstable();
        self.vehicle_ids.dedup();
        if self.manager_ids.len() > MAX_IDS
            || self.vehicle_ids.len() > MAX_IDS
            || self.carins_bases.len() > MAX_IDS
            || self.device_ids.len() > MAX_IDS
        {
            return Err("too_many_ids");
        }
        Ok(self)
    }
}

fn clean(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_owned()).filter(|s| !s.is_empty())
}

pub mod sql {
    /// $1 tenant_id / $2 営業所の id (null なら全部)
    pub const OFFICES: &str = "SELECT id, name, company_name, address FROM alc_api.tenko_offices \
         WHERE tenant_id = $1 AND ($2::uuid IS NULL OR id = $2) ORDER BY name, id";

    /// $1 tenant_id / $2 営業所の id (null なら全部)
    pub const MANAGERS: &str = "SELECT office_id, employee_id FROM alc_api.tenko_office_managers \
         WHERE tenant_id = $1 AND ($2::uuid IS NULL OR office_id = $2) ORDER BY office_id, employee_id";

    /// 未削除の車両。$1 tenant_id
    pub const VEHICLES: &str = "SELECT id, registration_number, display_name, office_id FROM alc_api.maintenance_vehicles \
         WHERE tenant_id = $1 AND deleted_at IS NULL ORDER BY registration_number, id";

    /// 営業所に対応づけた組。$1 tenant_id / $2 営業所の id (null なら全部)
    pub const BASES: &str = "SELECT office_id, use_base, user_name FROM alc_api.tenko_office_carins_bases \
         WHERE tenant_id = $1 AND ($2::uuid IS NULL OR office_id = $2) ORDER BY use_base, user_name";

    /// 営業所に所属する端末。$1 tenant_id / $2 営業所の id (null なら全部)
    pub const DEVICES: &str = "SELECT office_id, device_id FROM alc_api.tenko_office_devices \
         WHERE tenant_id = $1 AND ($2::uuid IS NULL OR office_id = $2) ORDER BY device_id";

    /// 営業所の端末を全部外す。$1 tenant_id / $2 office_id
    pub const CLEAR_DEVICES: &str =
        "DELETE FROM alc_api.tenko_office_devices WHERE tenant_id = $1 AND office_id = $2";

    /// 端末をこの営業所に所属させる (別の営業所の所属はそのまま。直前に外しているので重なりは無い)。
    /// $1 tenant_id / $2 office_id / $3 device_id の配列
    pub const ADD_DEVICES: &str =
        "INSERT INTO alc_api.tenko_office_devices (tenant_id, office_id, device_id) \
         SELECT $1, $2, unnest($3::text[]) ON CONFLICT DO NOTHING";

    /// 車検証に出てくる組と、組ごとの車両数 (distinct CarId) と対応。組の式は判定 (alc-measurements-worker) と同じ。
    /// 使用の本拠が空の組は除く。$1 tenant_id
    pub const CARINS: &str = "SELECT g.use_base, g.user_name, b.office_id, g.vehicle_count FROM ( \
             SELECT btrim(ci.\"UseheadqrterChar\") AS use_base, \
                    btrim(COALESCE(NULLIF(btrim(ci.\"UsernameHighLevelChar\"), ''), ci.\"UsernameLowLevelChar\")) AS user_name, \
                    count(DISTINCT ci.\"CarId\") AS vehicle_count \
             FROM alc_api.car_inspection ci \
             WHERE ci.tenant_id = $1 AND btrim(ci.\"UseheadqrterChar\") <> '' \
             GROUP BY 1, 2) g \
         LEFT JOIN alc_api.tenko_office_carins_bases b \
             ON b.tenant_id = $1 AND b.use_base = g.use_base AND b.user_name = g.user_name \
         ORDER BY g.use_base, g.user_name";

    /// 営業所の組を全部消す。$1 tenant_id / $2 office_id
    pub const CLEAR_BASES: &str =
        "DELETE FROM alc_api.tenko_office_carins_bases WHERE tenant_id = $1 AND office_id = $2";

    /// 組をこの営業所に対応づける (別の営業所に在った組は付け替える)。$1 tenant_id / $2 office_id / $3 use_base の配列 / $4 user_name の配列
    pub const ADD_BASES: &str = "INSERT INTO alc_api.tenko_office_carins_bases (tenant_id, office_id, use_base, user_name) \
         SELECT $1, $2, unnest($3::text[]), unnest($4::text[]) \
         ON CONFLICT (tenant_id, use_base, user_name) DO UPDATE SET office_id = EXCLUDED.office_id";

    /// 営業所の件数。$1 tenant_id
    pub const COUNT: &str = "SELECT count(*) FROM alc_api.tenko_offices WHERE tenant_id = $1";

    /// 運行管理者にできる社員の数 (在籍で role に manager か admin。employees.role は TEXT[])。
    /// alc-measurements-worker の判定 (`judgment.rs` の `JUDGE_ROLE`) と同じ扱い。$1 tenant_id / $2 社員の id の配列
    pub const MANAGER_COUNT: &str = "SELECT count(*) FROM alc_api.employees \
         WHERE tenant_id = $1 AND id = ANY($2) AND deleted_at IS NULL AND role && ARRAY['manager', 'admin']::text[]";

    /// 未削除の車両の数。$1 tenant_id / $2 車両の id の配列
    pub const VEHICLE_COUNT: &str = "SELECT count(*) FROM alc_api.maintenance_vehicles \
         WHERE tenant_id = $1 AND id = ANY($2) AND deleted_at IS NULL";

    /// $1 tenant_id / $2 name / $3 company_name / $4 address
    pub const INSERT: &str =
        "INSERT INTO alc_api.tenko_offices (tenant_id, name, company_name, address) \
         VALUES ($1, $2, $3, $4) RETURNING id";

    /// $1 tenant_id / $2 id / $3 name / $4 company_name / $5 address
    pub const UPDATE: &str = "UPDATE alc_api.tenko_offices SET name = $3, company_name = $4, address = $5, updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2 RETURNING id";

    /// $1 tenant_id / $2 office_id
    pub const CLEAR_MANAGERS: &str =
        "DELETE FROM alc_api.tenko_office_managers WHERE tenant_id = $1 AND office_id = $2";

    /// $1 tenant_id / $2 office_id / $3 社員の id の配列
    pub const ADD_MANAGERS: &str =
        "INSERT INTO alc_api.tenko_office_managers (tenant_id, office_id, employee_id) \
         SELECT $1, $2, unnest($3::uuid[])";

    /// 一覧に無い車両の所属を外す。$1 tenant_id / $2 office_id / $3 車両の id の配列
    pub const RELEASE_VEHICLES: &str = "UPDATE alc_api.maintenance_vehicles SET office_id = NULL \
         WHERE tenant_id = $1 AND office_id = $2 AND NOT (id = ANY($3::uuid[]))";

    /// 一覧の車両をこの営業所へ付け替える。$1 tenant_id / $2 office_id / $3 車両の id の配列
    pub const ASSIGN_VEHICLES: &str = "UPDATE alc_api.maintenance_vehicles SET office_id = $2 \
         WHERE tenant_id = $1 AND id = ANY($3::uuid[])";

    /// $1 id / $2 tenant_id
    pub const DELETE: &str = "DELETE FROM alc_api.tenko_offices WHERE id = $1 AND tenant_id = $2";
}

/// 営業所(`only` が在ればその 1 件)と、未削除の車両を読む。営業所の `vehicle_ids` は車両の `office_id` から作る
async fn load(tx: &TenantTx<'_>, tenant_id: Uuid, only: Option<Uuid>) -> Result<OfficeList, Error> {
    let offices = tx
        .query_typed(
            sql::OFFICES,
            &[(&tenant_id, Type::UUID), (&only, Type::UUID)],
        )
        .await?;
    let managers = tx
        .query_typed(
            sql::MANAGERS,
            &[(&tenant_id, Type::UUID), (&only, Type::UUID)],
        )
        .await?;
    let bases = tx
        .query_typed(sql::BASES, &[(&tenant_id, Type::UUID), (&only, Type::UUID)])
        .await?;
    let devices = tx
        .query_typed(
            sql::DEVICES,
            &[(&tenant_id, Type::UUID), (&only, Type::UUID)],
        )
        .await?;
    let vehicles = tx
        .query_typed(sql::VEHICLES, &[(&tenant_id, Type::UUID)])
        .await?;
    let vehicles: Vec<Vehicle> = vehicles
        .iter()
        .map(|r| Vehicle {
            id: r.get(0),
            registration_number: r.get(1),
            display_name: r.get(2),
            office_id: r.get(3),
        })
        .collect();
    let offices = offices
        .iter()
        .map(|r| {
            let id: Uuid = r.get(0);
            Office {
                id,
                name: r.get(1),
                company_name: r.get(2),
                address: r.get(3),
                manager_ids: managers
                    .iter()
                    .filter(|m| m.get::<_, Uuid>(0) == id)
                    .map(|m| m.get(1))
                    .collect(),
                vehicle_ids: vehicles
                    .iter()
                    .filter(|v| v.office_id == Some(id))
                    .map(|v| v.id)
                    .collect(),
                carins_bases: bases
                    .iter()
                    .filter(|b| b.get::<_, Uuid>(0) == id)
                    .map(|b| CarinsBase {
                        use_base: b.get(1),
                        user_name: b.get(2),
                    })
                    .collect(),
                device_ids: devices
                    .iter()
                    .filter(|d| d.get::<_, Uuid>(0) == id)
                    .map(|d| d.get(1))
                    .collect(),
            }
        })
        .collect();
    Ok(OfficeList {
        offices,
        vehicles,
        carins_bases: Vec::new(),
    })
}

pub async fn list(pg: &mut PgClient, tenant_id: Uuid) -> Result<OfficeList, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let mut out = load(tx, tenant_id, None).await?;
            // car_inspection が空のテナント (車検証を取り込んでいない) は空の配列
            out.carins_bases = tx
                .query_typed(sql::CARINS, &[(&tenant_id, Type::UUID)])
                .await?
                .iter()
                .map(|r| CarinsBaseCount {
                    use_base: r.get(0),
                    user_name: r.get(1),
                    office_id: r.get(2),
                    vehicle_count: r.get(3),
                })
                .collect();
            Ok(out)
        })
    })
    .await
}

/// 営業所を作る (`id` が None) か、全部置き換える (`id` が在る)。`input` は [`OfficeInput::normalize`] を通したもの。
/// 検査 (運行管理者・車両・件数) → 営業所の行 (更新で無ければ 404) → 運行管理者・車両の置き換え、の順で、検査が外れたら何も書かない
pub async fn save(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Option<Uuid>,
    input: OfficeInput,
) -> Result<Saved, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let managers = tx
                .query_typed_one(
                    sql::MANAGER_COUNT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.manager_ids, Type::UUID_ARRAY),
                    ],
                )
                .await?;
            if managers.get::<_, i64>(0) != input.manager_ids.len() as i64 {
                return Ok(Saved::Invalid("invalid_manager"));
            }
            let vehicles = tx
                .query_typed_one(
                    sql::VEHICLE_COUNT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.vehicle_ids, Type::UUID_ARRAY),
                    ],
                )
                .await?;
            if vehicles.get::<_, i64>(0) != input.vehicle_ids.len() as i64 {
                return Ok(Saved::Invalid("invalid_vehicle"));
            }
            let office_id: Uuid = match id {
                Some(id) => {
                    let args: [Arg; 5] = [
                        (&tenant_id, Type::UUID),
                        (&id, Type::UUID),
                        (&input.name, Type::TEXT),
                        (&input.company_name, Type::TEXT),
                        (&input.address, Type::TEXT),
                    ];
                    match tx.query_typed_opt(sql::UPDATE, &args).await? {
                        Some(r) => r.get(0),
                        None => return Ok(Saved::NotFound),
                    }
                }
                None => {
                    let count = tx
                        .query_typed_one(sql::COUNT, &[(&tenant_id, Type::UUID)])
                        .await?;
                    if count.get::<_, i64>(0) >= MAX_OFFICES {
                        return Ok(Saved::Invalid("too_many_offices"));
                    }
                    let args: [Arg; 4] = [
                        (&tenant_id, Type::UUID),
                        (&input.name, Type::TEXT),
                        (&input.company_name, Type::TEXT),
                        (&input.address, Type::TEXT),
                    ];
                    tx.query_typed_one(sql::INSERT, &args).await?.get(0)
                }
            };
            let who: [Arg; 2] = [(&tenant_id, Type::UUID), (&office_id, Type::UUID)];
            tx.execute_typed(sql::CLEAR_MANAGERS, &who).await?;
            let managers: [Arg; 3] = [
                (&tenant_id, Type::UUID),
                (&office_id, Type::UUID),
                (&input.manager_ids, Type::UUID_ARRAY),
            ];
            tx.execute_typed(sql::ADD_MANAGERS, &managers).await?;
            let vehicles: [Arg; 3] = [
                (&tenant_id, Type::UUID),
                (&office_id, Type::UUID),
                (&input.vehicle_ids, Type::UUID_ARRAY),
            ];
            tx.execute_typed(sql::RELEASE_VEHICLES, &vehicles).await?;
            tx.execute_typed(sql::ASSIGN_VEHICLES, &vehicles).await?;
            let use_bases: Vec<&str> = input
                .carins_bases
                .iter()
                .map(|b| b.use_base.as_str())
                .collect();
            let user_names: Vec<&str> = input
                .carins_bases
                .iter()
                .map(|b| b.user_name.as_str())
                .collect();
            tx.execute_typed(sql::CLEAR_BASES, &who).await?;
            let bases: [Arg; 4] = [
                (&tenant_id, Type::UUID),
                (&office_id, Type::UUID),
                (&use_bases, Type::TEXT_ARRAY),
                (&user_names, Type::TEXT_ARRAY),
            ];
            tx.execute_typed(sql::ADD_BASES, &bases).await?;
            tx.execute_typed(sql::CLEAR_DEVICES, &who).await?;
            let devices: [Arg; 3] = [
                (&tenant_id, Type::UUID),
                (&office_id, Type::UUID),
                (&input.device_ids, Type::TEXT_ARRAY),
            ];
            tx.execute_typed(sql::ADD_DEVICES, &devices).await?;
            let mut out = load(tx, tenant_id, Some(office_id)).await?;
            // 今書いた営業所は必ず在る (同じ transaction の中)
            Ok(Saved::Done(out.offices.remove(0)))
        })
    })
    .await
}

/// 消したら true、無ければ false
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

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tenko/offices", get(list_handler).post(create_handler))
        .route(
            "/tenko/offices/{id}",
            put(update_handler).delete(delete_handler),
        )
}

fn forbidden() -> Response {
    status_error(StatusCode::FORBIDDEN, "forbidden")
}

fn error(word: &'static str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": word }))).into_response()
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    let mut pg = state.pg.lock().await;
    match list(&mut pg, tenant_id).await {
        Ok(l) => Json(l).into_response(),
        Err(e) => state.db_fail("offices list", &e).into_response(),
    }
}

/// 本文を検めて保存し、結果を応答にする (POST・PUT 共通。`created` なら 201)
async fn save_handler(state: AppState, tenant_id: Uuid, id: Option<Uuid>, body: &[u8]) -> Response {
    let Ok(input) = serde_json::from_slice::<OfficeInput>(body) else {
        return error("invalid_body");
    };
    let input = match input.normalize() {
        Ok(i) => i,
        Err(word) => return error(word),
    };
    let mut pg = state.pg.lock().await;
    match save(&mut pg, tenant_id, id, input).await {
        Ok(Saved::Done(o)) if id.is_none() => (StatusCode::CREATED, Json(o)).into_response(),
        Ok(Saved::Done(o)) => Json(o).into_response(),
        Ok(Saved::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Ok(Saved::Invalid(word)) => error(word),
        Err(e) => state.db_fail("offices save", &e).into_response(),
    }
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    body: axum::body::Bytes,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    save_handler(state, tenant_id, None, &body).await
}

async fn update_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    body: axum::body::Bytes,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    save_handler(state, tenant_id, Some(id), &body).await
}

async fn delete_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, Response> {
    if !is_admin(&auth_user) {
        return Err(forbidden());
    }
    let mut pg = state.pg.lock().await;
    let deleted = delete(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("offices delete", &e).into_response())?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND.into_response())
    }
}
