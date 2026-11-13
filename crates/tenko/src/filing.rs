//! 点呼の届出・自動点呼機器の認定申請に差し込む値 (alc-migrations 214 の `tenko_company_filing_settings`。事業者 ([`crate::companies`]) ごとに 1 行、
//! キーは tenant_id と company_id。201・213 の `tenko_filing_settings` (テナントに 1 行) はもう読まない)。
//! 管理画面 (alc-app の「点呼の届出」) が、この値を様式 (e-Gov の参考様式・別紙7・認定申請の様式1・様式2) に差し込んで
//! ダウンロードさせる。差し込みはブラウザでするので、ここは値の読み書きだけ。rust-alc-api には無い口。
//!
//! - `GET /tenko/filing-settings?company_id=<uuid>` — 設定。行が無ければ既定値 (機器の名称 `一歩庵点呼 v1`・営業所なし・ほかは null、`updated_at` は null)
//! - `PUT /tenko/filing-settings?company_id=<uuid>` — 全部の欄を置き換える (省略した欄は null / 空に戻る)。応答は保存後の設定
//!
//! **2 口とも admin だけ** (`X-User-Role` が admin でなければ 403 `forbidden`)。`company_id` が無い・UUID でなければ 400 `company_required`、
//! テナントに無い事業者は 404。文字列は前後の空白を落とし、空は null にする。
//! 検める欄: 事業者番号 (半角数字 12 桁)・事業種別 (1〜8)・営業所 (`office_id` が無ければ名称は必須、実施・被実施の別は 1〜3、
//! 20 件まで。`office_id` がテナントの点呼の営業所でなければ `invalid_office`)・郵便番号 (ハイフンを落として半角数字 7 桁)・
//! 法人番号 (半角数字 13 桁)・長さ (1 欄 500 文字まで)。外れたら 400 `{"error": "<欄>"}`。この表は dev の軸を持たない。
//! 営業所の `office_id` は点呼の営業所 (`tenko_offices`) の id で、名称・住所は保存時に写し、応答では今の値に引き直す (Refs ippoan/alc-app#676)。
//! **受委託の相手の会社 (`partners`) は書かない** (本文の値は読まない): 応答の `partners` は、`office_id` の在る営業所のうち
//! 事業者がこの事業者以外のものから事業者ごとに導く ([`refresh`])。`office_id` の在る営業所が無い (旧形式だけの) 行は保存してある値を返す。
//! フリガナ・郵便番号・電話・法人番号・代表者の役職・担当者の部門名と住所・相手の会社 (partners) は 213 で足した欄で、
//! 画面が e-Gov の申請データ (一時保存・申請済みの控え) と突き合わせるのに使う。

use std::collections::HashMap;

use alc_core_wasm::{AuthUser, TenantId};
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::models::Arg;
use crate::AppState;

/// 使用する機器の名称の既定 (表の既定値と同じ)
pub const DEFAULT_DEVICE_NAME: &str = "一歩庵点呼 v1";
/// 営業所・車庫の件数の上限 (参考様式の申請欄は 5 件。それより多い分は画面が別紙にする)
pub const MAX_OFFICES: usize = 20;
/// 1 欄の文字数の上限
pub const MAX_CHARS: usize = 500;
/// 点呼に係る営業所又は車庫 (参考様式の申請 1 件ぶん)。`role` は #17 の 1 実施 / 2 被実施 / 3 実施・被実施両方。
/// `office_id` は点呼の営業所 (`tenko_offices`) の id で、在れば名称・住所は保存時にマスタから写し、読むときは今の値に引き直す
/// (営業所が消えていれば写しのまま)。無い要素 (旧形式) は名称・住所を手で持つ
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Office {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub office_id: Option<Uuid>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub address: Option<String>,
    pub role: i16,
}

/// 受委託の相手の会社の営業所
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartnerOffice {
    pub name: String,
    #[serde(default)]
    pub address: Option<String>,
}

/// 受委託の相手の会社 (事業者間遠隔点呼の受委託の許可申請の委託者・受託者)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Partner {
    pub name: String,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub representative: Option<String>,
    #[serde(default)]
    pub offices: Vec<PartnerOffice>,
}

/// 設定の全部の欄 (GET の応答と PUT の本文。PUT の `updated_at` は読まない)
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FilingSettings {
    pub business_number: Option<String>,
    pub trade_name: Option<String>,
    pub business_type: Option<i16>,
    pub offices: Vec<Office>,
    pub device_name: Option<String>,
    pub auto_cert_pre: Option<String>,
    pub auto_cert_post: Option<String>,
    pub operator_name: Option<String>,
    pub operator_address: Option<String>,
    pub operator_representative: Option<String>,
    pub operator_contact_name: Option<String>,
    pub operator_contact_phone: Option<String>,
    pub operator_contact_email: Option<String>,
    pub operator_kana: Option<String>,
    pub operator_address_kana: Option<String>,
    pub operator_postal_code: Option<String>,
    pub operator_phone: Option<String>,
    pub operator_corporate_number: Option<String>,
    pub operator_representative_kana: Option<String>,
    pub operator_representative_title: Option<String>,
    pub operator_contact_kana: Option<String>,
    pub operator_contact_dept: Option<String>,
    pub operator_contact_postal_code: Option<String>,
    pub operator_contact_address: Option<String>,
    pub partners: Vec<Partner>,
    pub applicant_name: Option<String>,
    pub applicant_kana: Option<String>,
    pub applicant_address: Option<String>,
    pub applicant_contact_name: Option<String>,
    pub applicant_contact_dept: Option<String>,
    pub applicant_contact_phone: Option<String>,
    pub applicant_contact_email: Option<String>,
    pub product_url: Option<String>,
    #[serde(skip_deserializing)]
    pub updated_at: Option<DateTime<Utc>>,
}

impl TxOutput for FilingSettings {}

impl FilingSettings {
    /// 行が無いときの既定値
    pub fn defaults() -> Self {
        Self {
            device_name: Some(DEFAULT_DEVICE_NAME.to_owned()),
            ..Self::default()
        }
    }

    /// 文字列の欄 (営業所の中を除く)。並びは表の列と同じ
    fn texts_mut(&mut self) -> [&mut Option<String>; 30] {
        [
            &mut self.business_number,
            &mut self.trade_name,
            &mut self.device_name,
            &mut self.auto_cert_pre,
            &mut self.auto_cert_post,
            &mut self.operator_name,
            &mut self.operator_address,
            &mut self.operator_representative,
            &mut self.operator_contact_name,
            &mut self.operator_contact_phone,
            &mut self.operator_contact_email,
            &mut self.operator_kana,
            &mut self.operator_address_kana,
            &mut self.operator_postal_code,
            &mut self.operator_phone,
            &mut self.operator_corporate_number,
            &mut self.operator_representative_kana,
            &mut self.operator_representative_title,
            &mut self.operator_contact_kana,
            &mut self.operator_contact_dept,
            &mut self.operator_contact_postal_code,
            &mut self.operator_contact_address,
            &mut self.applicant_name,
            &mut self.applicant_kana,
            &mut self.applicant_address,
            &mut self.applicant_contact_name,
            &mut self.applicant_contact_dept,
            &mut self.applicant_contact_phone,
            &mut self.applicant_contact_email,
            &mut self.product_url,
        ]
    }

    /// 空白を落として検める。外れた欄の名前を返す。機器の名称が空なら既定に戻す
    pub fn normalize(mut self) -> Result<Self, &'static str> {
        for v in self.texts_mut() {
            *v = clean(v.take());
            if v.as_ref().is_some_and(|s| s.chars().count() > MAX_CHARS) {
                return Err("too_long");
            }
        }
        if self.device_name.is_none() {
            self.device_name = Some(DEFAULT_DEVICE_NAME.to_owned());
        }
        if self
            .business_number
            .as_ref()
            .is_some_and(|n| n.len() != 12 || !n.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err("invalid_business_number");
        }
        for v in [
            &mut self.operator_postal_code,
            &mut self.operator_contact_postal_code,
        ] {
            *v = v.take().map(|p| p.replace(['-', '－', '‐'], ""));
            if v.as_ref().is_some_and(|p| !digits(p, 7)) {
                return Err("invalid_postal_code");
            }
        }
        if self
            .operator_corporate_number
            .as_ref()
            .is_some_and(|n| !digits(n, 13))
        {
            return Err("invalid_corporate_number");
        }
        if self.business_type.is_some_and(|t| !(1..=8).contains(&t)) {
            return Err("invalid_business_type");
        }
        if self.offices.len() > MAX_OFFICES {
            return Err("too_many_offices");
        }
        for o in &mut self.offices {
            o.name = o.name.trim().to_owned();
            o.address = clean(o.address.take());
            if (o.name.is_empty() && o.office_id.is_none()) || !(1..=3).contains(&o.role) {
                return Err("invalid_office");
            }
            if o.name.chars().count() > MAX_CHARS
                || o.address
                    .as_ref()
                    .is_some_and(|a| a.chars().count() > MAX_CHARS)
            {
                return Err("too_long");
            }
        }
        self.updated_at = None;
        Ok(self)
    }
}

/// 半角数字 `n` 桁
fn digits(s: &str, n: usize) -> bool {
    s.len() == n && s.bytes().all(|b| b.is_ascii_digit())
}

fn clean(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_owned()).filter(|s| !s.is_empty())
}

pub mod sql {
    /// 列の並び (`settings` が読む順)
    pub const COLUMNS: &str = "business_number, trade_name, business_type, offices, device_name, auto_cert_pre, \
         auto_cert_post, operator_name, operator_address, operator_representative, operator_contact_name, \
         operator_contact_phone, operator_contact_email, applicant_name, applicant_kana, applicant_address, \
         applicant_contact_name, applicant_contact_dept, applicant_contact_phone, applicant_contact_email, \
         product_url, operator_kana, operator_address_kana, operator_postal_code, operator_phone, \
         operator_corporate_number, operator_representative_kana, operator_representative_title, \
         operator_contact_kana, operator_contact_dept, operator_contact_postal_code, operator_contact_address, \
         partners, updated_at";

    /// $1 tenant_id / $2 company_id
    pub const GET: &str = const_format::concatcp!(
        "SELECT ",
        COLUMNS,
        " FROM alc_api.tenko_company_filing_settings WHERE tenant_id = $1 AND company_id = $2"
    );

    /// 全部の欄を置き換える (partners は書かない。読むときに営業所から導く)。$1 tenant_id / $2〜$33 は [`COLUMNS`] の
    /// partners の前までと同じ順 / $34 company_id
    pub const UPSERT: &str = const_format::concatcp!(
        "INSERT INTO alc_api.tenko_company_filing_settings (tenant_id, ",
        "business_number, trade_name, business_type, offices, device_name, auto_cert_pre, \
         auto_cert_post, operator_name, operator_address, operator_representative, operator_contact_name, \
         operator_contact_phone, operator_contact_email, applicant_name, applicant_kana, applicant_address, \
         applicant_contact_name, applicant_contact_dept, applicant_contact_phone, applicant_contact_email, product_url, \
         operator_kana, operator_address_kana, operator_postal_code, operator_phone, operator_corporate_number, \
         operator_representative_kana, operator_representative_title, operator_contact_kana, operator_contact_dept, \
         operator_contact_postal_code, operator_contact_address, company_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, \
         $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34) \
         ON CONFLICT (tenant_id, company_id) DO UPDATE SET \
             business_number = EXCLUDED.business_number, trade_name = EXCLUDED.trade_name, \
             business_type = EXCLUDED.business_type, offices = EXCLUDED.offices, device_name = EXCLUDED.device_name, \
             auto_cert_pre = EXCLUDED.auto_cert_pre, auto_cert_post = EXCLUDED.auto_cert_post, \
             operator_name = EXCLUDED.operator_name, operator_address = EXCLUDED.operator_address, \
             operator_representative = EXCLUDED.operator_representative, \
             operator_contact_name = EXCLUDED.operator_contact_name, \
             operator_contact_phone = EXCLUDED.operator_contact_phone, \
             operator_contact_email = EXCLUDED.operator_contact_email, \
             applicant_name = EXCLUDED.applicant_name, applicant_kana = EXCLUDED.applicant_kana, \
             applicant_address = EXCLUDED.applicant_address, \
             applicant_contact_name = EXCLUDED.applicant_contact_name, \
             applicant_contact_dept = EXCLUDED.applicant_contact_dept, \
             applicant_contact_phone = EXCLUDED.applicant_contact_phone, \
             applicant_contact_email = EXCLUDED.applicant_contact_email, \
             product_url = EXCLUDED.product_url, \
             operator_kana = EXCLUDED.operator_kana, operator_address_kana = EXCLUDED.operator_address_kana, \
             operator_postal_code = EXCLUDED.operator_postal_code, operator_phone = EXCLUDED.operator_phone, \
             operator_corporate_number = EXCLUDED.operator_corporate_number, \
             operator_representative_kana = EXCLUDED.operator_representative_kana, \
             operator_representative_title = EXCLUDED.operator_representative_title, \
             operator_contact_kana = EXCLUDED.operator_contact_kana, \
             operator_contact_dept = EXCLUDED.operator_contact_dept, \
             operator_contact_postal_code = EXCLUDED.operator_contact_postal_code, \
             operator_contact_address = EXCLUDED.operator_contact_address, updated_at = NOW() \
         RETURNING ",
        COLUMNS
    );

    /// テナントの営業所の名称・住所・事業者。$1 tenant_id / $2 営業所の id の配列
    pub const OFFICES: &str = "SELECT id, name, address, company_id FROM alc_api.tenko_offices \
         WHERE tenant_id = $1 AND id = ANY($2)";

    /// 受委託の相手の事業者の名称・住所・代表者 (書き込む値の事業者の欄、無ければ事業者の名称)。$1 tenant_id / $2 事業者の id の配列
    pub const PARTNERS: &str = "SELECT c.id, COALESCE(f.operator_name, c.name), f.operator_address, \
             f.operator_representative \
         FROM alc_api.tenko_companies c \
         LEFT JOIN alc_api.tenko_company_filing_settings f ON f.tenant_id = c.tenant_id AND f.company_id = c.id \
         WHERE c.tenant_id = $1 AND c.id = ANY($2)";
}

fn settings(r: &Row) -> FilingSettings {
    // offices は表の CHECK で配列。要素の形は書き込みで検めているが、読めなければ空にする (画面を開けなくしない)
    let offices: serde_json::Value = r.get(3);
    let partners: serde_json::Value = r.get(32);
    FilingSettings {
        business_number: r.get(0),
        trade_name: r.get(1),
        business_type: r.get(2),
        offices: serde_json::from_value(offices).unwrap_or_default(),
        device_name: r.get(4),
        auto_cert_pre: r.get(5),
        auto_cert_post: r.get(6),
        operator_name: r.get(7),
        operator_address: r.get(8),
        operator_representative: r.get(9),
        operator_contact_name: r.get(10),
        operator_contact_phone: r.get(11),
        operator_contact_email: r.get(12),
        applicant_name: r.get(13),
        applicant_kana: r.get(14),
        applicant_address: r.get(15),
        applicant_contact_name: r.get(16),
        applicant_contact_dept: r.get(17),
        applicant_contact_phone: r.get(18),
        applicant_contact_email: r.get(19),
        product_url: r.get(20),
        operator_kana: r.get(21),
        operator_address_kana: r.get(22),
        operator_postal_code: r.get(23),
        operator_phone: r.get(24),
        operator_corporate_number: r.get(25),
        operator_representative_kana: r.get(26),
        operator_representative_title: r.get(27),
        operator_contact_kana: r.get(28),
        operator_contact_dept: r.get(29),
        operator_contact_postal_code: r.get(30),
        operator_contact_address: r.get(31),
        partners: serde_json::from_value(partners).unwrap_or_default(),
        updated_at: r.get(33),
    }
}

/// 事業者の設定。事業者がテナントに無ければ `None`、行が無ければ既定値
pub async fn get_settings(
    pg: &mut PgClient,
    tenant_id: Uuid,
    company_id: Uuid,
) -> Result<Option<FilingSettings>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            if !company_exists(tx, tenant_id, company_id).await? {
                return Ok(None);
            }
            let row = tx
                .query_typed_opt(
                    sql::GET,
                    &[(&tenant_id, Type::UUID), (&company_id, Type::UUID)],
                )
                .await?;
            let mut s = row
                .as_ref()
                .map(settings)
                .unwrap_or_else(FilingSettings::defaults);
            refresh(tx, tenant_id, company_id, &mut s).await?;
            Ok(Some(s))
        })
    })
    .await
}

/// 事業者がテナントに在るか
async fn company_exists(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    company_id: Uuid,
) -> Result<bool, Error> {
    Ok(tx
        .query_typed_opt(
            crate::companies::sql::EXISTS,
            &[(&tenant_id, Type::UUID), (&company_id, Type::UUID)],
        )
        .await?
        .is_some())
}

/// `office_id` の在る営業所の名称・住所を今のマスタの値にし、`partners` を導く。テナントに無い (消えた) 営業所は写しのまま
/// にして false を返す。`office_id` の在る営業所が 1 つも無ければ何もしない (`partners` は保存した値のまま)。
///
/// `partners` は、営業所の事業者が `company_id` 以外のものを事業者ごとに (営業所の並びで最初に出た順) まとめ、事業者の名称・住所・
/// 代表者 (その事業者の書き込む値の `operator_*`、名称が無ければ事業者の名称) と営業所の名称・住所を入れる
async fn refresh(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    company_id: Uuid,
    s: &mut FilingSettings,
) -> Result<bool, Error> {
    let ids: Vec<Uuid> = s.offices.iter().filter_map(|o| o.office_id).collect();
    if ids.is_empty() {
        return Ok(true);
    }
    let found: HashMap<Uuid, (String, Option<String>, Option<Uuid>)> = tx
        .query_typed(
            sql::OFFICES,
            &[(&tenant_id, Type::UUID), (&ids, Type::UUID_ARRAY)],
        )
        .await?
        .iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2), r.get(3))))
        .collect();
    let mut all = true;
    let mut groups: Vec<(Uuid, Vec<PartnerOffice>)> = Vec::new();
    for o in &mut s.offices {
        let Some(id) = o.office_id else { continue };
        let Some((name, address, owner)) = found.get(&id) else {
            all = false;
            continue;
        };
        o.name = name.clone();
        o.address = address.clone();
        let Some(owner) = owner.filter(|c| *c != company_id) else {
            continue;
        };
        let po = PartnerOffice {
            name: name.clone(),
            address: address.clone(),
        };
        match groups.iter_mut().find(|(c, _)| *c == owner) {
            Some((_, offices)) => offices.push(po),
            None => groups.push((owner, vec![po])),
        }
    }
    let owners: Vec<Uuid> = groups.iter().map(|(c, _)| *c).collect();
    let heads: HashMap<Uuid, (String, Option<String>, Option<String>)> = tx
        .query_typed(
            sql::PARTNERS,
            &[(&tenant_id, Type::UUID), (&owners, Type::UUID_ARRAY)],
        )
        .await?
        .iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2), r.get(3))))
        .collect();
    s.partners = groups
        .into_iter()
        .filter_map(|(c, offices)| {
            let (name, address, representative) = heads.get(&c)?.clone();
            Some(Partner {
                name,
                address,
                representative,
                offices,
            })
        })
        .collect();
    Ok(all)
}

/// 保存の結果
#[derive(Debug, Clone, PartialEq)]
pub enum Put {
    Done(Box<FilingSettings>),
    /// 事業者がテナントに無い (何も書かない)
    NotFound,
    /// `office_id` がテナントの営業所でない (何も書かない)
    InvalidOffice,
}

impl TxOutput for Put {}

/// `s` は [`FilingSettings::normalize`] を通したもの。本文の `partners` は読まない (営業所から導く)
pub async fn put_settings(
    pg: &mut PgClient,
    tenant_id: Uuid,
    company_id: Uuid,
    mut s: FilingSettings,
) -> Result<Put, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            if !company_exists(tx, tenant_id, company_id).await? {
                return Ok(Put::NotFound);
            }
            // office_id の在る営業所は、名称・住所をマスタから写して保存する
            if !refresh(tx, tenant_id, company_id, &mut s).await? {
                return Ok(Put::InvalidOffice);
            }
            let offices = serde_json::to_value(&s.offices).unwrap_or_else(|_| json!([]));
            let args: [Arg; 34] = [
                (&tenant_id, Type::UUID),
                (&s.business_number, Type::TEXT),
                (&s.trade_name, Type::TEXT),
                (&s.business_type, Type::INT2),
                (&offices, Type::JSONB),
                (&s.device_name, Type::TEXT),
                (&s.auto_cert_pre, Type::TEXT),
                (&s.auto_cert_post, Type::TEXT),
                (&s.operator_name, Type::TEXT),
                (&s.operator_address, Type::TEXT),
                (&s.operator_representative, Type::TEXT),
                (&s.operator_contact_name, Type::TEXT),
                (&s.operator_contact_phone, Type::TEXT),
                (&s.operator_contact_email, Type::TEXT),
                (&s.applicant_name, Type::TEXT),
                (&s.applicant_kana, Type::TEXT),
                (&s.applicant_address, Type::TEXT),
                (&s.applicant_contact_name, Type::TEXT),
                (&s.applicant_contact_dept, Type::TEXT),
                (&s.applicant_contact_phone, Type::TEXT),
                (&s.applicant_contact_email, Type::TEXT),
                (&s.product_url, Type::TEXT),
                (&s.operator_kana, Type::TEXT),
                (&s.operator_address_kana, Type::TEXT),
                (&s.operator_postal_code, Type::TEXT),
                (&s.operator_phone, Type::TEXT),
                (&s.operator_corporate_number, Type::TEXT),
                (&s.operator_representative_kana, Type::TEXT),
                (&s.operator_representative_title, Type::TEXT),
                (&s.operator_contact_kana, Type::TEXT),
                (&s.operator_contact_dept, Type::TEXT),
                (&s.operator_contact_postal_code, Type::TEXT),
                (&s.operator_contact_address, Type::TEXT),
                (&company_id, Type::UUID),
            ];
            let mut out = settings(&tx.query_typed_one(sql::UPSERT, &args).await?);
            refresh(tx, tenant_id, company_id, &mut out).await?;
            Ok(Put::Done(Box::new(out)))
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new().route("/tenko/filing-settings", get(get_handler).put(put_handler))
}

pub(crate) fn is_admin(auth_user: &Option<Extension<AuthUser>>) -> bool {
    auth_user
        .as_ref()
        .is_some_and(|Extension(u)| u.role == "admin")
}

pub(crate) fn error(status: StatusCode, word: &'static str) -> Response {
    (status, Json(json!({ "error": word }))).into_response()
}

/// クエリの `company_id`
#[derive(Deserialize)]
pub(crate) struct CompanyQuery {
    company_id: Option<String>,
}

impl CompanyQuery {
    /// 無い・UUID でなければ `None`
    pub(crate) fn company_id(&self) -> Option<Uuid> {
        self.company_id
            .as_deref()
            .and_then(|s| Uuid::parse_str(s).ok())
    }
}

async fn get_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Query(query): Query<CompanyQuery>,
) -> Response {
    if !is_admin(&auth_user) {
        return error(StatusCode::FORBIDDEN, "forbidden");
    }
    let Some(company_id) = query.company_id() else {
        return error(StatusCode::BAD_REQUEST, "company_required");
    };
    let mut pg = state.pg.lock().await;
    match get_settings(&mut pg, tenant_id, company_id).await {
        Ok(Some(s)) => Json(s).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => state.db_fail("filing settings get", &e).into_response(),
    }
}

async fn put_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    auth_user: Option<Extension<AuthUser>>,
    Query(query): Query<CompanyQuery>,
    body: axum::body::Bytes,
) -> Response {
    if !is_admin(&auth_user) {
        return error(StatusCode::FORBIDDEN, "forbidden");
    }
    let Some(company_id) = query.company_id() else {
        return error(StatusCode::BAD_REQUEST, "company_required");
    };
    let Ok(body) = serde_json::from_slice::<FilingSettings>(&body) else {
        return error(StatusCode::BAD_REQUEST, "invalid_body");
    };
    let body = match body.normalize() {
        Ok(b) => b,
        Err(word) => return error(StatusCode::BAD_REQUEST, word),
    };
    let mut pg = state.pg.lock().await;
    match put_settings(&mut pg, tenant_id, company_id, body).await {
        Ok(Put::Done(s)) => Json(s).into_response(),
        Ok(Put::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Ok(Put::InvalidOffice) => error(StatusCode::BAD_REQUEST, "invalid_office"),
        Err(e) => state.db_fail("filing settings put", &e).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn office(name: &str, role: i16) -> Office {
        Office {
            office_id: None,
            name: name.to_owned(),
            address: None,
            role,
        }
    }

    #[test]
    fn normalize_trims_empties_and_restores_the_device_name() {
        let s = FilingSettings {
            business_number: Some(" 012345678901 ".to_owned()),
            trade_name: Some("   ".to_owned()),
            device_name: Some(" ".to_owned()),
            offices: vec![Office {
                office_id: None,
                name: " 本社営業所 ".to_owned(),
                address: Some(" ".to_owned()),
                role: 1,
            }],
            updated_at: Some(DateTime::<Utc>::MIN_UTC),
            ..FilingSettings::default()
        }
        .normalize()
        .unwrap();
        assert_eq!(s.business_number.as_deref(), Some("012345678901"));
        assert_eq!(s.trade_name, None);
        assert_eq!(s.device_name.as_deref(), Some(DEFAULT_DEVICE_NAME));
        assert_eq!(s.offices, vec![office("本社営業所", 1)]);
        assert_eq!(s.updated_at, None);
    }

    #[test]
    fn normalize_lets_an_office_id_go_without_a_name() {
        let o = Office {
            office_id: Some(Uuid::nil()),
            ..office(" ", 2)
        };
        let s = FilingSettings {
            offices: vec![o.clone()],
            ..FilingSettings::default()
        }
        .normalize()
        .unwrap();
        assert_eq!(
            s.offices,
            vec![Office {
                name: String::new(),
                ..o.clone()
            }]
        );
        // 役は office_id が在っても検める
        assert_eq!(
            FilingSettings {
                offices: vec![Office { role: 0, ..o }],
                ..FilingSettings::default()
            }
            .normalize()
            .unwrap_err(),
            "invalid_office"
        );
    }

    #[test]
    fn normalize_rejects_each_field() {
        let bad = |s: FilingSettings| s.normalize().unwrap_err();
        let base = FilingSettings::default;
        for n in [
            "12345678901",
            "1234567890123",
            "12345678901a",
            "１２３４５６７８９０１２",
        ] {
            assert_eq!(
                bad(FilingSettings {
                    business_number: Some(n.to_owned()),
                    ..base()
                }),
                "invalid_business_number"
            );
        }
        for t in [0, 9] {
            assert_eq!(
                bad(FilingSettings {
                    business_type: Some(t),
                    ..base()
                }),
                "invalid_business_type"
            );
        }
        for o in [office(" ", 1), office("a", 0), office("a", 4)] {
            assert_eq!(
                bad(FilingSettings {
                    offices: vec![o],
                    ..base()
                }),
                "invalid_office"
            );
        }
        assert_eq!(
            bad(FilingSettings {
                offices: vec![office("a", 1); MAX_OFFICES + 1],
                ..base()
            }),
            "too_many_offices"
        );
        let long = "あ".repeat(MAX_CHARS + 1);
        assert_eq!(
            bad(FilingSettings {
                applicant_name: Some(long.clone()),
                ..base()
            }),
            "too_long"
        );
        assert_eq!(
            bad(FilingSettings {
                offices: vec![office(&long, 1)],
                ..base()
            }),
            "too_long"
        );
        assert_eq!(
            bad(FilingSettings {
                offices: vec![Office {
                    address: Some(long.clone()),
                    ..office("a", 1)
                }],
                ..base()
            }),
            "too_long"
        );
        for p in ["12345", "12345678", "123-456", "１２３４５６７"] {
            assert_eq!(
                bad(FilingSettings {
                    operator_contact_postal_code: Some(p.to_owned()),
                    ..base()
                }),
                "invalid_postal_code"
            );
        }
        for n in ["123456789012", "12345678901234", "123456789012a"] {
            assert_eq!(
                bad(FilingSettings {
                    operator_corporate_number: Some(n.to_owned()),
                    ..base()
                }),
                "invalid_corporate_number"
            );
        }
        // 郵便番号のハイフン (半角・全角) は落とす
        let s = FilingSettings {
            operator_postal_code: Some("856－0044".to_owned()),
            operator_contact_postal_code: Some(" 856-0044 ".to_owned()),
            ..base()
        }
        .normalize()
        .unwrap();
        assert_eq!(
            (s.operator_postal_code, s.operator_contact_postal_code),
            (Some("8560044".to_owned()), Some("8560044".to_owned()))
        );
        // 上限ちょうどは通る
        assert!(FilingSettings {
            business_type: Some(8),
            offices: vec![office(&"あ".repeat(MAX_CHARS), 3); MAX_OFFICES],
            ..base()
        }
        .normalize()
        .is_ok());
    }
}
