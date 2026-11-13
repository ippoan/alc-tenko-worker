//! 点呼の届出 (遠隔点呼・自動点呼の実施の届出) の申請の下書き (alc-migrations 215 の `tenko_filings`。Refs ippoan/alc-app#676)。
//! 管理画面が様式を作った時点で 1 行残し、e-Gov へ出した後に到達番号を付けて状況を追う。申請 1 件 = 事業者 1 つの届出 1 通。
//! 到達番号と事業者の対応の正本は `tenko_filing_archive_companies` ([`crate::filing_archives`])。到達番号を付けると、同じ
//! transaction でそちらにも (到達番号, この申請の事業者) を足す (既存の振り分けは消さない)。rust-alc-api には無い口。
//!
//! - `POST /tenko/filings` — 本文 `{"company_id", "kind": "remote"|"auto", "apply_date": "YYYY-MM-DD", "start_date"?: "YYYY-MM-DD"|null,
//!   "offices": [{"office_id", "role"}], "created_by_name"?: string}` → 201 で申請 1 件
//! - `GET /tenko/filings?company_id=<uuid>` — その事業者の申請 (新しい順)
//! - `PATCH /tenko/filings/{id}` — 本文 `{"arrive_id"}` で到達番号を付ける → 200 で申請 1 件。無ければ 404 `not_found`、
//!   同じテナントのほかの申請に付いた到達番号は 409 `duplicate_arrive_id`
//! - `DELETE /tenko/filings/{id}` — 下書き (到達番号の無い行) だけ消せる → 204。到達番号付きは 409 `already_submitted`、無ければ 404 `not_found`
//!
//! 申請 1 件の形: `{"id", "company_id", "kind", "apply_date", "start_date", "offices": [{"office_id", "role", "name", "address"}],
//! "arrive_id", "created_by", "created_by_name", "created_at", "updated_at"}`。`offices` は作った時点の点呼の営業所 (`tenko_offices`)
//! の名称・住所の写し (営業所が後で変わっても出した内容が残る)。営業所は同じテナントのほかの事業者のものも選べる。
//! 検める欄 (外れたら 400 `{"error": "<語>"}`): 本文が読めない `invalid_body` / 事業者が無い `company_required` /
//! テナントに無い事業者 `invalid_company` / 種類 `invalid_kind` / 日付 `invalid_date` / 営業所が 0 件・役が 1〜3 以外・同じ営業所が 2 回・
//! テナントの営業所でない `invalid_office` / 営業所が 20 件超 `too_many_offices` / 作った人の名前が 500 文字超 `too_long` /
//! 到達番号の形 `invalid_arrive_id`。
//!
//! **4 口とも admin だけ** ([`crate::filing`] と同じ判定。admin でなければ 403 `forbidden`)。この表は dev の軸を持たない。
//! 作った人は `created_by` = 利用者の id、`created_by_name` = 本文の値 (分割 worker の利用者には名前が届かないので画面が送る)。

use std::collections::HashMap;

use alc_core_wasm::{AuthUser, TenantId};
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch};
use axum::{Extension, Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::filing::{error as status_error, is_admin, CompanyQuery, MAX_CHARS, MAX_OFFICES};
use crate::filing_archives::is_arrive_id;
use crate::AppState;

/// POST の本文の営業所
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct OfficeRef {
    pub office_id: Uuid,
    pub role: i16,
}

/// POST の本文
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(default)]
pub struct FilingInput {
    pub company_id: Option<Uuid>,
    pub kind: Option<String>,
    pub apply_date: Option<String>,
    pub start_date: Option<String>,
    pub offices: Vec<OfficeRef>,
    pub created_by_name: Option<String>,
}

/// 検めた本文
#[derive(Debug, Clone, PartialEq)]
pub struct NewFiling {
    pub company_id: Uuid,
    pub kind: String,
    pub apply_date: NaiveDate,
    pub start_date: Option<NaiveDate>,
    pub offices: Vec<OfficeRef>,
    pub created_by_name: Option<String>,
}

/// 保存した営業所の写し
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilingOffice {
    pub office_id: Uuid,
    pub role: i16,
    pub name: String,
    pub address: Option<String>,
}

/// 申請 1 件
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Filing {
    pub id: Uuid,
    pub company_id: Uuid,
    pub kind: String,
    pub apply_date: NaiveDate,
    pub start_date: Option<NaiveDate>,
    pub offices: serde_json::Value,
    pub arrive_id: Option<String>,
    pub created_by: Option<Uuid>,
    pub created_by_name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TxOutput for Filing {}

/// 作った結果 (検査は transaction の中でも決まる。外れたら何も書かない)
#[derive(Debug, Clone, PartialEq)]
pub enum Created {
    Done(Filing),
    Invalid(&'static str),
}

impl TxOutput for Created {}

/// 消した結果
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Deleted {
    Done,
    NotFound,
    Submitted,
}

impl TxOutput for Deleted {}

fn date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

impl FilingInput {
    /// 検める。外れた欄の語を返す
    pub fn normalize(self) -> Result<NewFiling, &'static str> {
        let company_id = self.company_id.ok_or("company_required")?;
        let kind = self
            .kind
            .filter(|k| k == "remote" || k == "auto")
            .ok_or("invalid_kind")?;
        let apply_date = self
            .apply_date
            .as_deref()
            .and_then(date)
            .ok_or("invalid_date")?;
        let start_date = match self.start_date.as_deref() {
            Some(s) => Some(date(s).ok_or("invalid_date")?),
            None => None,
        };
        if self.offices.is_empty() {
            return Err("invalid_office");
        }
        if self.offices.len() > MAX_OFFICES {
            return Err("too_many_offices");
        }
        let mut seen: Vec<Uuid> = self.offices.iter().map(|o| o.office_id).collect();
        seen.sort_unstable();
        seen.dedup();
        if seen.len() != self.offices.len()
            || self.offices.iter().any(|o| !(1..=3).contains(&o.role))
        {
            return Err("invalid_office");
        }
        let created_by_name = self
            .created_by_name
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty());
        if created_by_name
            .as_ref()
            .is_some_and(|s| s.chars().count() > MAX_CHARS)
        {
            return Err("too_long");
        }
        Ok(NewFiling {
            company_id,
            kind,
            apply_date,
            start_date,
            offices: self.offices,
            created_by_name,
        })
    }
}

pub mod sql {
    /// 列の並び (`filing` が読む順)
    pub const COLUMNS: &str = "id, company_id, kind, apply_date, start_date, offices, arrive_id, \
         created_by, created_by_name, created_at, updated_at";

    /// $1 tenant_id / $2 company_id / $3 kind / $4 apply_date / $5 start_date / $6 offices / $7 created_by / $8 created_by_name
    pub const INSERT: &str = const_format::concatcp!(
        "INSERT INTO alc_api.tenko_filings \
         (tenant_id, company_id, kind, apply_date, start_date, offices, created_by, created_by_name) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING ",
        COLUMNS
    );

    /// 事業者の申請 (新しい順)。$1 tenant_id / $2 company_id
    pub const LIST: &str = const_format::concatcp!(
        "SELECT ",
        COLUMNS,
        " FROM alc_api.tenko_filings WHERE tenant_id = $1 AND company_id = $2 ORDER BY created_at DESC, id"
    );

    /// 到達番号を付ける。$1 tenant_id / $2 id / $3 到達番号
    pub const ARRIVE: &str = const_format::concatcp!(
        "UPDATE alc_api.tenko_filings SET arrive_id = $3, updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2 RETURNING ",
        COLUMNS
    );

    /// 控えの振り分けに (到達番号, 事業者) を足す (在れば何もしない)。$1 tenant_id / $2 到達番号 / $3 company_id
    pub const ADD_ARCHIVE: &str =
        "INSERT INTO alc_api.tenko_filing_archive_companies (tenant_id, arrive_id, company_id) \
         VALUES ($1, $2, $3) ON CONFLICT DO NOTHING";

    /// 申請が出してあるか (行を押さえる)。$1 tenant_id / $2 id
    pub const SUBMITTED: &str = "SELECT arrive_id IS NOT NULL FROM alc_api.tenko_filings \
         WHERE tenant_id = $1 AND id = $2 FOR UPDATE";

    /// $1 tenant_id / $2 id
    pub const DELETE: &str = "DELETE FROM alc_api.tenko_filings WHERE tenant_id = $1 AND id = $2";
}

fn filing(r: &Row) -> Filing {
    Filing {
        id: r.get(0),
        company_id: r.get(1),
        kind: r.get(2),
        apply_date: r.get(3),
        start_date: r.get(4),
        offices: r.get(5),
        arrive_id: r.get(6),
        created_by: r.get(7),
        created_by_name: r.get(8),
        created_at: r.get(9),
        updated_at: r.get(10),
    }
}

/// 申請を作る。`input` は [`FilingInput::normalize`] を通したもの。事業者・営業所がテナントに無ければ何も書かない
pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    created_by: Uuid,
    input: NewFiling,
) -> Result<Created, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let company = tx
                .query_typed_opt(
                    crate::companies::sql::EXISTS,
                    &[(&tenant_id, Type::UUID), (&input.company_id, Type::UUID)],
                )
                .await?;
            if company.is_none() {
                return Ok(Created::Invalid("invalid_company"));
            }
            let ids: Vec<Uuid> = input.offices.iter().map(|o| o.office_id).collect();
            let found: HashMap<Uuid, (String, Option<String>)> = tx
                .query_typed(
                    crate::filing::sql::OFFICES,
                    &[(&tenant_id, Type::UUID), (&ids, Type::UUID_ARRAY)],
                )
                .await?
                .iter()
                .map(|r| (r.get(0), (r.get(1), r.get(2))))
                .collect();
            let mut offices = Vec::with_capacity(input.offices.len());
            for o in &input.offices {
                let Some((name, address)) = found.get(&o.office_id) else {
                    return Ok(Created::Invalid("invalid_office"));
                };
                offices.push(FilingOffice {
                    office_id: o.office_id,
                    role: o.role,
                    name: name.clone(),
                    address: address.clone(),
                });
            }
            let offices = json!(offices);
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.company_id, Type::UUID),
                        (&input.kind, Type::TEXT),
                        (&input.apply_date, Type::DATE),
                        (&input.start_date, Type::DATE),
                        (&offices, Type::JSONB),
                        (&created_by, Type::UUID),
                        (&input.created_by_name, Type::TEXT),
                    ],
                )
                .await?;
            Ok(Created::Done(filing(&row)))
        })
    })
    .await
}

pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    company_id: Uuid,
) -> Result<Vec<Filing>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::LIST,
                    &[(&tenant_id, Type::UUID), (&company_id, Type::UUID)],
                )
                .await?;
            Ok(rows.iter().map(filing).collect())
        })
    })
    .await
}

/// 到達番号を付け、同じ transaction で控えの振り分けに (到達番号, 申請の事業者) を足す。行が無ければ `None`
pub async fn arrive(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    arrive_id: String,
) -> Result<Option<Filing>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let Some(row) = tx
                .query_typed_opt(
                    sql::ARRIVE,
                    &[
                        (&tenant_id, Type::UUID),
                        (&id, Type::UUID),
                        (&arrive_id, Type::TEXT),
                    ],
                )
                .await?
            else {
                return Ok(None);
            };
            let f = filing(&row);
            tx.execute_typed(
                sql::ADD_ARCHIVE,
                &[
                    (&tenant_id, Type::UUID),
                    (&arrive_id, Type::TEXT),
                    (&f.company_id, Type::UUID),
                ],
            )
            .await?;
            Ok(Some(f))
        })
    })
    .await
}

/// 下書きだけ消す
pub async fn delete(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<Deleted, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let args = [
                (
                    &tenant_id as &(dyn tokio_postgres::types::ToSql + Sync),
                    Type::UUID,
                ),
                (&id, Type::UUID),
            ];
            match tx.query_typed_opt(sql::SUBMITTED, &args).await? {
                None => Ok(Deleted::NotFound),
                Some(r) if r.get::<_, bool>(0) => Ok(Deleted::Submitted),
                Some(_) => {
                    tx.execute_typed(sql::DELETE, &args).await?;
                    Ok(Deleted::Done)
                }
            }
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tenko/filings", get(list_handler).post(create_handler))
        .route(
            "/tenko/filings/{id}",
            patch(arrive_handler).delete(delete_handler),
        )
}

fn forbidden() -> Response {
    status_error(StatusCode::FORBIDDEN, "forbidden")
}

fn bad_request(word: &'static str) -> Response {
    status_error(StatusCode::BAD_REQUEST, word)
}

fn not_found() -> Response {
    status_error(StatusCode::NOT_FOUND, "not_found")
}

fn conflict(word: &'static str) -> Response {
    status_error(StatusCode::CONFLICT, word)
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Query(query): Query<CompanyQuery>,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    let Some(company_id) = query.company_id() else {
        return bad_request("company_required");
    };
    let mut pg = state.pg.lock().await;
    match list(&mut pg, tenant_id, company_id).await {
        Ok(l) => Json(l).into_response(),
        Err(e) => state.db_fail("filings list", &e).into_response(),
    }
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    body: axum::body::Bytes,
) -> Response {
    let Some(Extension(user)) = auth_user.as_ref().filter(|_| is_admin(&auth_user)) else {
        return forbidden();
    };
    let Ok(input) = serde_json::from_slice::<FilingInput>(&body) else {
        return bad_request("invalid_body");
    };
    let input = match input.normalize() {
        Ok(i) => i,
        Err(word) => return bad_request(word),
    };
    let mut pg = state.pg.lock().await;
    match create(&mut pg, tenant_id, user.user_id, input).await {
        Ok(Created::Done(f)) => (StatusCode::CREATED, Json(f)).into_response(),
        Ok(Created::Invalid(word)) => bad_request(word),
        Err(e) => state.db_fail("filings create", &e).into_response(),
    }
}

/// PATCH の本文
#[derive(Deserialize)]
struct ArriveInput {
    #[serde(default)]
    arrive_id: Option<String>,
}

async fn arrive_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
    body: axum::body::Bytes,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    let Ok(input) = serde_json::from_slice::<ArriveInput>(&body) else {
        return bad_request("invalid_body");
    };
    let Some(arrive_id) = input.arrive_id.filter(|a| is_arrive_id(a)) else {
        return bad_request("invalid_arrive_id");
    };
    let mut pg = state.pg.lock().await;
    match arrive(&mut pg, tenant_id, id, arrive_id).await {
        Ok(Some(f)) => Json(f).into_response(),
        Ok(None) => not_found(),
        Err(e) if e.code() == Some(&SqlState::UNIQUE_VIOLATION) => conflict("duplicate_arrive_id"),
        Err(e) => state.db_fail("filings arrive", &e).into_response(),
    }
}

async fn delete_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Path(id): Path<Uuid>,
) -> Response {
    if !is_admin(&auth_user) {
        return forbidden();
    }
    let mut pg = state.pg.lock().await;
    match delete(&mut pg, tenant_id, id).await {
        Ok(Deleted::Done) => StatusCode::NO_CONTENT.into_response(),
        Ok(Deleted::NotFound) => not_found(),
        Ok(Deleted::Submitted) => conflict("already_submitted"),
        Err(e) => state.db_fail("filings delete", &e).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> FilingInput {
        FilingInput {
            company_id: Some(Uuid::nil()),
            kind: Some("remote".to_owned()),
            apply_date: Some("2026-11-13".to_owned()),
            start_date: None,
            offices: vec![OfficeRef {
                office_id: Uuid::nil(),
                role: 1,
            }],
            created_by_name: None,
        }
    }

    #[test]
    fn normalize_checks_each_field() {
        let bad = |i: FilingInput| i.normalize().unwrap_err();
        assert_eq!(
            bad(FilingInput {
                company_id: None,
                ..input()
            }),
            "company_required"
        );
        for k in [None, Some("x".to_owned())] {
            assert_eq!(bad(FilingInput { kind: k, ..input() }), "invalid_kind");
        }
        for d in [
            None,
            Some("2026-13-01".to_owned()),
            Some("20261113".to_owned()),
        ] {
            assert_eq!(
                bad(FilingInput {
                    apply_date: d,
                    ..input()
                }),
                "invalid_date"
            );
        }
        assert_eq!(
            bad(FilingInput {
                start_date: Some("x".to_owned()),
                ..input()
            }),
            "invalid_date"
        );
        let office = |role| OfficeRef {
            office_id: Uuid::new_v4(),
            role,
        };
        for offices in [
            vec![],
            vec![office(0)],
            vec![office(4)],
            vec![input().offices[0].clone(), input().offices[0].clone()],
        ] {
            assert_eq!(bad(FilingInput { offices, ..input() }), "invalid_office");
        }
        assert_eq!(
            bad(FilingInput {
                offices: (0..=MAX_OFFICES).map(|_| office(1)).collect(),
                ..input()
            }),
            "too_many_offices"
        );
        assert_eq!(
            bad(FilingInput {
                created_by_name: Some("あ".repeat(MAX_CHARS + 1)),
                ..input()
            }),
            "too_long"
        );
        let ok = FilingInput {
            kind: Some("auto".to_owned()),
            start_date: Some("2026-12-01".to_owned()),
            offices: (0..MAX_OFFICES).map(|i| office(i as i16 % 3 + 1)).collect(),
            created_by_name: Some(" ".to_owned()),
            ..input()
        }
        .normalize()
        .unwrap();
        assert_eq!(
            (ok.kind.as_str(), ok.start_date, ok.created_by_name),
            ("auto", NaiveDate::from_ymd_opt(2026, 12, 1), None)
        );
        let named = FilingInput {
            created_by_name: Some(format!(" {} ", "あ".repeat(MAX_CHARS))),
            ..input()
        }
        .normalize()
        .unwrap();
        assert_eq!(named.created_by_name, Some("あ".repeat(MAX_CHARS)));
    }
}
