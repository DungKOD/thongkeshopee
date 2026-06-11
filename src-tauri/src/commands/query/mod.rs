//! Commands đọc data từ DB để UI render.
//!
//! Query trung tâm: `list_days_with_rows` aggregate on-the-fly từ 4 raw tables
//! và `manual_entries`, group theo tuple sub_id.
//!
//! Matching logic **prefix-compatible** với anchor = canonical từ Shopee order
//! (hoa hồng sản phẩm). FB/click/manual có canonical prefix-compatible với
//! anchor sẽ merge vào anchor đó. Không có anchor compatible → giữ canonical
//! gốc (FB campaign standalone dùng tên camp).

use std::collections::{HashMap, HashSet};

#[cfg(not(test))]
use rusqlite::Connection;
#[cfg(test)]
use rusqlite::{params, Connection};
use serde::Deserialize;
use tauri::State;

use crate::db::types::{
    FbAdLeaf, FbAdSetGroup, FbBreakdown, FbCampaignGroup, UiDay, UiRow,
};
use crate::db::DbState;

use super::{CmdError, CmdResult};

/// Smoke test — verify DB connection mở được + bảng `days` hiện diện.
/// Trả về `total_days_count`.
#[tauri::command]
pub fn db_ping(state: State<'_, DbState>) -> CmdResult<i64> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let count: i64 =
        conn.query_row("SELECT COUNT(*) FROM days", [], |r| r.get(0))?;
    Ok(count)
}

#[tauri::command]
pub fn list_days(state: State<'_, DbState>) -> CmdResult<Vec<String>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare_cached("SELECT date FROM days ORDER BY date DESC")?;
    let rows: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// Filter args cho `list_days_with_rows`. Mọi field optional → `None` = unfiltered.
/// `from_date`/`to_date` inclusive YYYY-MM-DD. `limit` = N ngày mới nhất.
/// `sub_id_filter` = chuỗi user chọn từ dropdown, split by `-` rồi subset match
/// trên display_name của từng row (giữ nguyên semantics FE `matchSubId`).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DaysFilter {
    pub from_date: Option<String>,
    pub to_date: Option<String>,
    pub limit: Option<i64>,
    pub sub_id_filter: Option<String>,
    /// Account Shopee filter. None hoặc `All` = không filter (behavior cũ).
    pub account_filter: Option<AccountFilterMode>,
    /// Sub_id tuple exact của 1 product (dialog Chi tiết). Khi provided,
    /// analytics commands post-filter rows theo prefix-compatible match
    /// (cùng rule `get_order_items_for_row` dùng). None = all products.
    pub sub_ids: Option<[String; 5]>,
}

/// Filter mode theo account. Tagged union trùng FE `AccountFilter`.
/// - `All`: không filter — return everything (backward compat).
/// - `Account { id }`: Shopee/manual WHERE shopee_account_id = id; FB derive
///   attribution qua JOIN (day_date, sub_ids) với Shopee data của account đó.
///   **Account id=1 ("Mặc định")** là bucket catch-all: FB không match account
///   nào trên cùng ngày cũng rơi vào Mặc định. Logic: Mặc định filter matches
///   owners.is_empty() OR owners.contains(1).
///
/// Custom deserializer — chấp nhận cả JSON number và JSON string cho id,
///   parse về i64. FE gửi string (vì content_id > 2^53 JS precision loss),
///   nhưng cũng tolerant với number cho backward compat + tests.
fn deser_id_flexible<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Any {
        Num(i64),
        Str(String),
    }
    match Any::deserialize(d)? {
        Any::Num(n) => Ok(n),
        Any::Str(s) => s.parse::<i64>().map_err(serde::de::Error::custom),
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum AccountFilterMode {
    #[default]
    All,
    Account {
        #[serde(deserialize_with = "deser_id_flexible")]
        id: i64,
    },
}

/// Tên reserved cho account "Mặc định" — catch-all bucket cho sub_id chưa
/// gán explicit account nào. Sau v13 migration id = content_id hash (không
/// còn = 1), nên lookup theo name mỗi query.
const DEFAULT_ACCOUNT_NAME: &str = "Mặc định";

/// Lookup id account "Mặc định" từ DB. Cache 1 lần per query. Return None
/// nếu account không tồn tại (lỗi setup DB — không nên xảy ra vì seed migration).
fn default_account_id_lookup(conn: &rusqlite::Connection) -> Option<i64> {
    use rusqlite::OptionalExtension;
    conn.query_row(
        "SELECT id FROM shopee_accounts WHERE name = ?",
        [DEFAULT_ACCOUNT_NAME],
        |r| r.get::<_, i64>(0),
    )
    .optional()
    .ok()
    .flatten()
}

#[tauri::command]
pub fn list_days_with_rows(
    state: State<'_, DbState>,
    filter: Option<DaysFilter>,
) -> CmdResult<Vec<UiDay>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    list_days_with_rows_impl(&conn, filter.unwrap_or_default())
}

/// Tách impl khỏi command để test truy cập trực tiếp với `Connection`.
fn list_days_with_rows_impl(
    conn: &Connection,
    filter: DaysFilter,
) -> CmdResult<Vec<UiDay>> {
    // Build query động theo filter. Tất cả đều parameterized — không string-concat user input.
    //
    // Pre-filter "has data": chỉ trả ngày thực sự có dữ liệu trong ít nhất 1
    // trong 4 nguồn (raw clicks/orders/fb_ads/manual). Đẩy logic này lên SQL
    // để LIMIT pick đúng N ngày có data — nếu không, ngày trong `days` table
    // được auto-create (vd hôm nay user mở app nhưng chưa import) sẽ chiếm
    // slot LIMIT → kết quả empty dù ngày trước đó có data. Ngữ nghĩa khớp với
    // logic drop ở dưới: rows.is_empty() && !has_totals_data.
    let mut sql = String::from(
        "SELECT date, notes FROM days WHERE (\
         EXISTS (SELECT 1 FROM raw_shopee_clicks WHERE day_date = days.date) \
         OR EXISTS (SELECT 1 FROM raw_shopee_order_items WHERE day_date = days.date) \
         OR EXISTS (SELECT 1 FROM raw_fb_ads WHERE day_date = days.date) \
         OR EXISTS (SELECT 1 FROM raw_fb_ads_hierarchy WHERE day_date = days.date) \
         OR EXISTS (SELECT 1 FROM manual_entries WHERE day_date = days.date))",
    );
    if filter.from_date.is_some() {
        sql.push_str(" AND date >= ?");
    }
    if filter.to_date.is_some() {
        sql.push_str(" AND date <= ?");
    }
    sql.push_str(" ORDER BY date DESC");
    if filter.limit.is_some() {
        sql.push_str(" LIMIT ?");
    }

    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(v) = &filter.from_date {
        params_vec.push(Box::new(v.clone()));
    }
    if let Some(v) = &filter.to_date {
        params_vec.push(Box::new(v.clone()));
    }
    if let Some(v) = filter.limit {
        params_vec.push(Box::new(v.max(0)));
    }

    let mut stmt = conn.prepare_cached(&sql)?;
    let params_refs: Vec<&dyn rusqlite::ToSql> =
        params_vec.iter().map(|b| b.as_ref() as &dyn rusqlite::ToSql).collect();
    let days: Vec<(String, Option<String>)> = stmt
        .query_map(params_refs.as_slice(), |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })?
        .collect::<Result<_, _>>()?;

    // Early exit — empty IN clause is invalid SQL, and nothing to process.
    if days.is_empty() {
        return Ok(vec![]);
    }

    // Sub_id filter: split user input → Vec<String>, check subset trên display_name parts.
    let selected_parts: Vec<String> = filter
        .sub_id_filter
        .as_deref()
        .map(|s| {
            s.split('-')
                .filter(|p| !p.is_empty())
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();

    let account_filter = filter.account_filter.clone().unwrap_or_default();
    let match_mode = read_sub_id_match_mode(conn);
    let default_id = default_account_id_lookup(conn);
    let account_names: HashMap<i64, String> = {
        let mut stmt = conn.prepare_cached("SELECT id, name FROM shopee_accounts")?;
        let iter = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
        iter.collect::<rusqlite::Result<HashMap<_, _>>>()?
    };

    // SQLite variable limit = 999 by default. For safety, fall back to the
    // per-day loop when the date list is very large (>900 slots). In practice
    // this app rarely queries more than ~100 days at a time.
    if days.len() > 900 {
        let mut out = Vec::with_capacity(days.len());
        for (date, notes) in days {
            let (mut rows, totals) =
                aggregate_rows_for_day(conn, &date, &account_filter, match_mode, default_id, &account_names)?;
            let sub_id_filter_active = !selected_parts.is_empty();
            if sub_id_filter_active {
                rows.retain(|r| display_name_subset_match(&r.display_name, &selected_parts));
            }
            let has_totals_data = totals.ads_clicks != 0
                || totals.shopee_clicks_total != 0
                || totals.orders_count != 0
                || totals.commission_total != 0.0
                || totals.total_spend != 0.0
                || totals.impressions != 0
                || totals.order_value_total != 0.0
                || totals.mcn_fee_total != 0.0;
            if rows.is_empty() && (sub_id_filter_active || !has_totals_data) {
                continue;
            }
            out.push(UiDay { date, notes, rows, totals });
        }
        return Ok(out);
    }

    // =========================================================================
    // Batch path: 5 SQL queries total regardless of N days.
    // Build IN clause placeholders "?,?,?..." for N dates.
    // =========================================================================
    let dates: Vec<&str> = days.iter().map(|(d, _)| d.as_str()).collect();
    let placeholders = dates.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let date_params: Vec<&dyn rusqlite::ToSql> =
        dates.iter().map(|d| d as &dyn rusqlite::ToSql).collect();

    let account_id_eq: Option<i64> = match &account_filter {
        AccountFilterMode::Account { id } => Some(*id),
        _ => None,
    };

    // Fetch all 5 sources in 5 queries (+ up to 3 extra when account filter active).
    let mut batch_fb_ads =
        batch_fetch_fb_ads(conn, &placeholders, date_params.as_slice())?;
    let mut batch_fb_hier =
        batch_fetch_fb_hier(conn, &placeholders, date_params.as_slice())?;
    let mut batch_clicks =
        batch_fetch_shopee_clicks(conn, &placeholders, date_params.as_slice(), account_id_eq)?;
    let mut batch_orders =
        batch_fetch_shopee_orders(conn, &placeholders, date_params.as_slice(), account_id_eq)?;
    let mut batch_manuals =
        batch_fetch_manuals(conn, &placeholders, date_params.as_slice(), account_id_eq)?;

    // All-account owner pairs: only needed when account filter is active.
    // When All: derived per-day from already-fetched (unfiltered) data.
    let mut batch_owner_pairs: Option<HashMap<String, RawOwnerPairs>> = if account_id_eq.is_some() {
        Some(batch_fetch_all_account_owner_pairs(
            conn,
            &placeholders,
            date_params.as_slice(),
        )?)
    } else {
        None
    };

    let mut out = Vec::with_capacity(days.len());
    for (date, notes) in &days {
        let fb_ads_day = batch_fb_ads.remove(date).unwrap_or_default();
        let fb_hier_day = batch_fb_hier.remove(date).unwrap_or_default();
        let shopee_clicks_day = batch_clicks.remove(date).unwrap_or_default();
        let shopee_orders_day = batch_orders.remove(date).unwrap_or_default();
        let manuals_day = batch_manuals.remove(date).unwrap_or_default();

        // Owner pairs: derive from in-memory data (All) or pre-fetched (Account).
        let (hard_owner_pairs, click_owner_pairs, anchors) =
            if account_id_eq.is_none() {
                // All filter: derive from unfiltered data already in memory.
                let hard: Vec<(Canonical, i64)> = shopee_orders_day
                    .iter()
                    .map(|o| (o.canonical.clone(), o.account_id))
                    .chain(
                        manuals_day
                            .iter()
                            .map(|m| (m.canonical.clone(), m.shopee_account_id.unwrap_or(0))),
                    )
                    .collect();
                let click: Vec<(Canonical, i64)> = shopee_clicks_day
                    .iter()
                    .map(|c| (c.canonical.clone(), c.account_id))
                    .collect();
                let anch: Vec<Canonical> = shopee_orders_day
                    .iter()
                    .map(|o| o.canonical.clone())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                (hard, click, anch)
            } else {
                // Account filter: use pre-fetched all-account owner pairs.
                let op = batch_owner_pairs
                    .as_mut()
                    .and_then(|m| m.remove(date))
                    .unwrap_or(RawOwnerPairs {
                        hard_owner_pairs: vec![],
                        click_owner_pairs: vec![],
                        anchors: vec![],
                    });
                (op.hard_owner_pairs, op.click_owner_pairs, op.anchors)
            };

        let (mut rows, totals) = process_day_data(
            date,
            &account_filter,
            match_mode,
            default_id,
            &account_names,
            fb_ads_day,
            fb_hier_day,
            shopee_clicks_day,
            shopee_orders_day,
            manuals_day,
            hard_owner_pairs,
            click_owner_pairs,
            anchors,
        )?;

        let sub_id_filter_active = !selected_parts.is_empty();
        if sub_id_filter_active {
            rows.retain(|r| display_name_subset_match(&r.display_name, &selected_parts));
        }

        // Khi nào skip ngày:
        //   - Sub_id filter active + rows rỗng → drop (totals pre-sub_id nên
        //     không đại diện kết quả filter — hiện KPI sẽ misleading).
        //   - Không có filter / account filter only, rows rỗng, totals cũng
        //     rỗng → truly empty, drop.
        //   - Account filter, rows rỗng, totals CÓ data (vd account X chỉ có
        //     click không spend/commission, row-0 drop hết) → GIỮ day để
        //     Overview KPI count được clicks. DayBlock sẽ render bảng rỗng.
        let has_totals_data = totals.ads_clicks != 0
            || totals.shopee_clicks_total != 0
            || totals.orders_count != 0
            || totals.commission_total != 0.0
            || totals.total_spend != 0.0
            || totals.impressions != 0
            || totals.order_value_total != 0.0
            || totals.mcn_fee_total != 0.0;
        if rows.is_empty() && (sub_id_filter_active || !has_totals_data) {
            continue;
        }
        out.push(UiDay { date: date.clone(), notes: notes.clone(), rows, totals });
    }
    Ok(out)
}

/// Subset match — replicate FE `matchSubId` (App.tsx) exactly.
/// `selected_parts` rỗng → match all. Ngược lại: mọi part phải tồn tại trong
/// set parts (split by `-`, bỏ rỗng) của `display_name`.
fn display_name_subset_match(display_name: &str, selected_parts: &[String]) -> bool {
    if selected_parts.is_empty() {
        return true;
    }
    let row_parts: HashSet<&str> = display_name
        .split('-')
        .filter(|p| !p.is_empty())
        .collect();
    selected_parts
        .iter()
        .all(|p| row_parts.contains(p.as_str()))
}

mod aggregate;
pub mod files;
pub mod insights;
pub mod items;
pub mod overview;

pub use aggregate::SubIdMatchMode;
pub(crate) use aggregate::{
    is_compatible, read_sub_id_match_mode, to_canonical, Canonical,
};
use aggregate::{canonical_to_array, default_name, representative};

// ============================================================
// Module-level raw data structs — dùng cho cả single-day và batch paths.
// ============================================================

/// FB ads row (sau dedup ad_group-priority trong SQL).
struct RawFbAds {
    canonical: Canonical,
    spend_cents: i64,
    imps: Option<i64>,
    clicks: Option<i64>,
    weighted_cpc_sum: Option<f64>,
}

/// FB hierarchy raw row (format 3 cấp).
struct RawFbHier {
    canonical: Canonical,
    campaign_name: String,
    ad_set_name: String,
    ad_name: String,
    occurrence_idx: i64,
    spend_cents: i64,
    clicks: Option<i64>,
    weighted_cpc_sum: f64,
}

/// Shopee clicks row (grouped by tuple + referrer + account_id).
struct RawShopeeClick {
    canonical: Canonical,
    account_id: i64,
    referrer: String,
    count: i64,
}

/// Shopee orders row (aggregated per tuple + account_id).
struct RawShopeeOrder {
    canonical: Canonical,
    account_id: i64,
    orders: i64,
    commission_cents: i64,
    commission_pending_cents: i64,
    order_value_cents: i64,
    mcn_fee_cents: i64,
}

/// Manual entry row.
struct RawManual {
    canonical: Canonical,
    display_name: Option<String>,
    clicks: Option<i64>,
    spend: Option<f64>,
    cpc: Option<f64>,
    orders: Option<i64>,
    commission: Option<f64>,
    shopee_account_id: Option<i64>,
}

/// All-account owner pairs for FB attribution when account_filter = Account(X).
struct RawOwnerPairs {
    hard_owner_pairs: Vec<(Canonical, i64)>,
    click_owner_pairs: Vec<(Canonical, i64)>,
    anchors: Vec<Canonical>,
}

/// Helper: build cây campaign → adset → ad từ hierarchy rows (module-level để
/// dùng được từ cả single-day và batch path).
fn build_breakdown_for_rows(rs: &[&RawFbHier]) -> FbBreakdown {
    let mut by_camp: Vec<(String, Vec<&RawFbHier>)> = Vec::new();
    for r in rs {
        if let Some((_, bucket)) = by_camp.iter_mut().find(|(n, _)| *n == r.campaign_name) {
            bucket.push(r);
        } else {
            by_camp.push((r.campaign_name.clone(), vec![*r]));
        }
    }

    let cpc_at_level =
        |spend_cents: i64, clicks_total: i64, cpc_sum: f64| -> Option<f64> {
            if clicks_total <= 0 {
                return None;
            }
            if cpc_sum > 0.0 {
                Some(cpc_sum / clicks_total as f64)
            } else {
                Some((spend_cents as f64 / 100.0) / clicks_total as f64)
            }
        };

    let mut total_spend_cents: i64 = 0;
    let mut campaigns: Vec<FbCampaignGroup> = Vec::new();

    for (camp_name, camp_rows) in by_camp {
        let mut by_adset: Vec<(String, Vec<&RawFbHier>)> = Vec::new();
        for r in camp_rows {
            if let Some((_, bucket)) = by_adset.iter_mut().find(|(n, _)| *n == r.ad_set_name) {
                bucket.push(r);
            } else {
                by_adset.push((r.ad_set_name.clone(), vec![r]));
            }
        }

        let mut camp_spend: i64 = 0;
        let mut camp_clicks: i64 = 0;
        let mut camp_cpc_sum: f64 = 0.0;
        let mut camp_has_clicks = false;
        let mut adsets: Vec<FbAdSetGroup> = Vec::new();

        for (adset_name, adset_rows) in by_adset {
            let mut adset_spend: i64 = 0;
            let mut adset_clicks: i64 = 0;
            let mut adset_cpc_sum: f64 = 0.0;
            let mut adset_has_clicks = false;
            let mut ads: Vec<FbAdLeaf> = Vec::new();

            for r in adset_rows {
                adset_spend += r.spend_cents;
                if let Some(c) = r.clicks {
                    adset_clicks += c;
                    adset_has_clicks = true;
                }
                adset_cpc_sum += r.weighted_cpc_sum;

                let leaf_cpc =
                    cpc_at_level(r.spend_cents, r.clicks.unwrap_or(0), r.weighted_cpc_sum);
                ads.push(FbAdLeaf {
                    ad_name: r.ad_name.clone(),
                    occurrence_idx: r.occurrence_idx,
                    spend: r.spend_cents as f64 / 100.0,
                    clicks: r.clicks,
                    cpc: leaf_cpc,
                });
            }

            let adset_cpc = cpc_at_level(adset_spend, adset_clicks, adset_cpc_sum);
            camp_spend += adset_spend;
            camp_clicks += adset_clicks;
            camp_cpc_sum += adset_cpc_sum;
            if adset_has_clicks {
                camp_has_clicks = true;
            }
            adsets.push(FbAdSetGroup {
                ad_set_name: adset_name,
                spend: adset_spend as f64 / 100.0,
                clicks: if adset_has_clicks { Some(adset_clicks) } else { None },
                cpc: adset_cpc,
                ads,
            });
        }

        let camp_cpc = cpc_at_level(camp_spend, camp_clicks, camp_cpc_sum);
        total_spend_cents += camp_spend;
        campaigns.push(FbCampaignGroup {
            campaign_name: camp_name,
            spend: camp_spend as f64 / 100.0,
            clicks: if camp_has_clicks { Some(camp_clicks) } else { None },
            cpc: camp_cpc,
            ad_sets: adsets,
        });
    }

    FbBreakdown {
        campaigns,
        total_spend: total_spend_cents as f64 / 100.0,
    }
}

/// Per-day processing: Phase 2 + Phase 3 logic — NO SQL inside.
/// Nhận pre-fetched Vecs (từ batch hoặc single-day query), trả về
/// `(Vec<UiRow>, UiDayTotals)`. Logic giống hệt phần Phase 2+3 của
/// `aggregate_rows_for_day`, được tách ra để batch path gọi lại.
#[allow(clippy::too_many_arguments)]
fn process_day_data(
    day_date: &str,
    account_filter: &AccountFilterMode,
    match_mode: SubIdMatchMode,
    default_id: Option<i64>,
    account_names: &HashMap<i64, String>,
    mut fb_ads: Vec<RawFbAds>,
    fb_hier_rows: Vec<RawFbHier>,
    shopee_clicks: Vec<RawShopeeClick>,
    shopee_orders: Vec<RawShopeeOrder>,
    manuals: Vec<RawManual>,
    // All-account owner pairs (pre-computed by caller):
    hard_owner_pairs: Vec<(Canonical, i64)>,
    click_owner_pairs: Vec<(Canonical, i64)>,
    anchors: Vec<Canonical>,
) -> CmdResult<(Vec<UiRow>, crate::db::types::UiDayTotals)> {
    // Tuple-level dedup: hierarchy thay thế raw_fb_ads cùng canonical.
    let hier_canonicals: HashSet<Canonical> =
        fb_hier_rows.iter().map(|r| r.canonical.clone()).collect();
    fb_ads.retain(|ad| !hier_canonicals.contains(&ad.canonical));

    // Aggregate hierarchy → FbAds shape, push vào fb_ads.
    {
        let mut by_canonical: HashMap<Canonical, RawFbAds> = HashMap::new();
        for r in &fb_hier_rows {
            let entry = by_canonical.entry(r.canonical.clone()).or_insert_with(|| RawFbAds {
                canonical: r.canonical.clone(),
                spend_cents: 0,
                imps: None,
                clicks: None,
                weighted_cpc_sum: None,
            });
            entry.spend_cents += r.spend_cents;
            if let Some(c) = r.clicks {
                entry.clicks = Some(entry.clicks.unwrap_or(0) + c);
            }
            if r.weighted_cpc_sum > 0.0 {
                entry.weighted_cpc_sum =
                    Some(entry.weighted_cpc_sum.unwrap_or(0.0) + r.weighted_cpc_sum);
            }
        }
        fb_ads.extend(by_canonical.into_values());
    }

    let resolve = |c: &Canonical| representative(c, &anchors, match_mode);

    // Build owners_for_day (2-tier: hard owners + click fallback).
    let owners_for_day: HashMap<Canonical, HashSet<i64>> = {
        let normalize_acc = |acc_id: i64| -> i64 {
            if acc_id == 0 { default_id.unwrap_or(0) } else { acc_id }
        };
        let mut map: HashMap<Canonical, HashSet<i64>> = HashMap::new();
        for (canon, acc_id) in &hard_owner_pairs {
            map.entry(resolve(canon)).or_default().insert(normalize_acc(*acc_id));
        }
        let hard_owned: HashSet<Canonical> = map.keys().cloned().collect();
        for (canon, acc_id) in &click_owner_pairs {
            let rep = resolve(canon);
            if !hard_owned.contains(&rep) {
                map.entry(rep).or_default().insert(normalize_acc(*acc_id));
            }
        }
        map
    };

    // Filter FB ads theo account mode.
    fb_ads.retain(|ad| {
        let rep = resolve(&ad.canonical);
        let owners = owners_for_day.get(&rep);
        match account_filter {
            AccountFilterMode::All => true,
            AccountFilterMode::Account { id } => {
                if Some(*id) == default_id {
                    match owners {
                        None => true,
                        Some(set) => set.is_empty() || set.contains(id),
                    }
                } else {
                    matches!(owners, Some(set) if set.contains(id))
                }
            }
        }
    });

    // Phase 3: aggregate vào Accumulator.
    let bucket_for_shopee = |acc_id: i64| -> Option<i64> {
        if acc_id == 0 { default_id } else { Some(acc_id) }
    };

    let fb_bucket_for = |canonical: &Canonical| -> Option<i64> {
        match account_filter {
            AccountFilterMode::Account { id } => Some(*id),
            AccountFilterMode::All => {
                let rep = resolve(canonical);
                let owners = owners_for_day.get(&rep);
                let owner_ids: Vec<i64> = owners
                    .map(|s| s.iter().copied().filter(|v| *v != 0).collect())
                    .unwrap_or_default();
                match owner_ids.len() {
                    0 => default_id,
                    1 => Some(owner_ids[0]),
                    _ => None,
                }
            }
        }
    };

    type Key = (Canonical, Option<i64>);

    let breakdown_rows_by_key: HashMap<Key, Vec<&RawFbHier>> = {
        let mut map: HashMap<Key, Vec<&RawFbHier>> = HashMap::new();
        for r in &fb_hier_rows {
            let rep = resolve(&r.canonical);
            let bucket = fb_bucket_for(&r.canonical);
            map.entry((rep, bucket)).or_default().push(r);
        }
        map
    };

    struct Accumulator {
        display_name: String,
        ads_clicks: Option<i64>,
        spend_cents: Option<i64>,
        cpc: Option<f64>,
        weighted_cpc_num: f64,
        cpc_clicks_total: i64,
        impressions: Option<i64>,
        shopee_clicks_by_referrer: HashMap<String, i64>,
        shopee_clicks_total: i64,
        orders_count: i64,
        commission_cents: i64,
        commission_pending_cents: i64,
        order_value_cents: i64,
        mcn_fee_cents: i64,
        has_fb: bool,
        has_shopee_clicks: bool,
        has_shopee_orders: bool,
        has_manual: bool,
        shopee_account_id: Option<i64>,
    }

    let mut map: HashMap<Key, Accumulator> = HashMap::new();
    let make_empty = |c: &Canonical| Accumulator {
        display_name: default_name(c),
        ads_clicks: None,
        spend_cents: None,
        cpc: None,
        weighted_cpc_num: 0.0,
        cpc_clicks_total: 0,
        impressions: None,
        shopee_clicks_by_referrer: HashMap::new(),
        shopee_clicks_total: 0,
        orders_count: 0,
        commission_cents: 0,
        commission_pending_cents: 0,
        order_value_cents: 0,
        mcn_fee_cents: 0,
        has_fb: false,
        has_shopee_clicks: false,
        has_shopee_orders: false,
        has_manual: false,
        shopee_account_id: None,
    };

    for r in fb_ads {
        let rep = resolve(&r.canonical);
        let bucket = fb_bucket_for(&r.canonical);
        let key: Key = (rep.clone(), bucket);
        let entry = map.entry(key).or_insert_with(|| make_empty(&rep));
        entry.has_fb = true;
        entry.spend_cents = Some(entry.spend_cents.unwrap_or(0) + r.spend_cents);
        if r.imps.is_some() {
            entry.impressions = Some(entry.impressions.unwrap_or(0) + r.imps.unwrap_or(0));
        }
        if r.clicks.is_some() {
            entry.ads_clicks = Some(entry.ads_clicks.unwrap_or(0) + r.clicks.unwrap_or(0));
        }
        if let (Some(wsum), Some(clicks)) = (r.weighted_cpc_sum, r.clicks) {
            if clicks > 0 && wsum > 0.0 {
                entry.weighted_cpc_num += wsum;
                entry.cpc_clicks_total += clicks;
            }
        }
    }

    for r in shopee_clicks {
        let rep = resolve(&r.canonical);
        let key: Key = {
            let hard_ids: Vec<i64> = owners_for_day
                .get(&rep)
                .map(|s| s.iter().copied().filter(|v| *v != 0).collect())
                .unwrap_or_default();
            if hard_ids.len() == 1 {
                (rep.clone(), Some(hard_ids[0]))
            } else {
                (rep.clone(), bucket_for_shopee(r.account_id))
            }
        };
        let entry = map.entry(key).or_insert_with(|| make_empty(&rep));
        entry.has_shopee_clicks = true;
        entry.shopee_clicks_total += r.count;
        *entry.shopee_clicks_by_referrer.entry(r.referrer).or_insert(0) += r.count;
    }

    for r in shopee_orders {
        let rep = resolve(&r.canonical);
        let key: Key = (rep.clone(), bucket_for_shopee(r.account_id));
        let entry = map.entry(key).or_insert_with(|| make_empty(&rep));
        entry.has_shopee_orders = true;
        entry.orders_count += r.orders;
        entry.commission_cents += r.commission_cents;
        entry.commission_pending_cents += r.commission_pending_cents;
        entry.order_value_cents += r.order_value_cents;
        entry.mcn_fee_cents += r.mcn_fee_cents;
    }

    for r in manuals {
        let rep = resolve(&r.canonical);
        let key: Key = (rep.clone(), bucket_for_shopee(r.shopee_account_id.unwrap_or(0)));
        let entry = map.entry(key).or_insert_with(|| make_empty(&rep));
        entry.has_manual = true;
        entry.shopee_account_id = r.shopee_account_id;
        if rep.is_empty() {
            if let Some(name) = r.display_name.as_ref() {
                if !name.is_empty() {
                    entry.display_name = name.clone();
                }
            }
        }
        if let Some(v) = r.clicks {
            entry.ads_clicks = Some(v);
        }
        if let Some(v) = r.spend {
            entry.spend_cents = Some((v * 100.0).round() as i64);
        }
        if let Some(v) = r.cpc {
            entry.cpc = Some(v);
        }
        if let Some(v) = r.orders {
            entry.orders_count = v;
        }
        if let Some(v) = r.commission {
            entry.commission_cents = (v * 100.0).round() as i64;
            entry.commission_pending_cents = 0;
        }
    }

    let entries: Vec<(Key, Accumulator)> = map.into_iter().collect();
    let mut rows: Vec<UiRow> = Vec::with_capacity(entries.len());
    let mut day_totals = crate::db::types::UiDayTotals::default();
    for ((c, account_id), acc) in entries {
        let total_spend = acc.spend_cents.map(|c| c as f64 / 100.0);
        let commission_total = acc.commission_cents as f64 / 100.0;
        let commission_pending = acc.commission_pending_cents as f64 / 100.0;
        let cpc = acc.cpc.or_else(|| {
            if acc.cpc_clicks_total > 0 && acc.weighted_cpc_num > 0.0 {
                Some(acc.weighted_cpc_num / acc.cpc_clicks_total as f64)
            } else if let (Some(s_cents), Some(clicks)) = (acc.spend_cents, acc.ads_clicks) {
                if clicks > 0 { Some((s_cents as f64 / 100.0) / clicks as f64) } else { None }
            } else {
                None
            }
        });

        day_totals.ads_clicks += acc.ads_clicks.unwrap_or(0);
        day_totals.total_spend += total_spend.unwrap_or(0.0);
        day_totals.impressions += acc.impressions.unwrap_or(0);
        day_totals.shopee_clicks_total += acc.shopee_clicks_total;
        for (referrer, count) in &acc.shopee_clicks_by_referrer {
            *day_totals.shopee_clicks_by_referrer.entry(referrer.clone()).or_insert(0) += count;
        }
        day_totals.orders_count += acc.orders_count;
        day_totals.commission_total += commission_total;
        day_totals.commission_pending += commission_pending;
        day_totals.order_value_total += acc.order_value_cents as f64 / 100.0;
        day_totals.mcn_fee_total += acc.mcn_fee_cents as f64 / 100.0;

        let has_spend = acc.spend_cents.map(|v| v != 0).unwrap_or(false);
        let has_commission = acc.commission_cents != 0;
        if !has_spend && !has_commission {
            continue;
        }

        let account_name =
            account_id.as_ref().and_then(|id| account_names.get(id).cloned());

        let fb_breakdown = breakdown_rows_by_key
            .get(&(c.clone(), account_id))
            .map(|rs| build_breakdown_for_rows(rs));

        rows.push(UiRow {
            day_date: day_date.to_string(),
            sub_ids: canonical_to_array(&c),
            display_name: acc.display_name,
            ads_clicks: acc.ads_clicks,
            total_spend,
            cpc,
            impressions: acc.impressions,
            shopee_clicks_by_referrer: acc.shopee_clicks_by_referrer,
            shopee_clicks_total: acc.shopee_clicks_total,
            orders_count: acc.orders_count,
            commission_total,
            commission_pending,
            order_value_total: acc.order_value_cents as f64 / 100.0,
            has_fb: acc.has_fb,
            has_shopee_clicks: acc.has_shopee_clicks,
            has_shopee_orders: acc.has_shopee_orders,
            has_manual: acc.has_manual,
            shopee_account_id: acc.shopee_account_id,
            account_id,
            account_name,
            fb_breakdown,
        });
    }

    rows.sort_by(|a, b| {
        a.display_name
            .cmp(&b.display_name)
            .then_with(|| match (&a.account_name, &b.account_name) {
                (Some(x), Some(y)) => x.cmp(y),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
    });
    Ok((rows, day_totals))
}

/// Batch-fetch all 5 data sources for a set of dates using `IN (...)`.
/// Returns `HashMap<String, Vec<T>>` keyed by day_date.
/// `account_id_eq`: None = All accounts (no filter on Shopee/manual tables).
///
/// SQLite variable limit: caller must ensure dates.len() <= 900.
fn batch_fetch_fb_ads(
    conn: &Connection,
    placeholders: &str,
    date_params: &[&dyn rusqlite::ToSql],
) -> CmdResult<HashMap<String, Vec<RawFbAds>>> {
    let sql = format!(
        "WITH ranked AS (
            SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                   level, spend, clicks, cpc, impressions, tax_rate,
                   MIN(CASE level WHEN 'ad_group' THEN 0 ELSE 1 END)
                     OVER (PARTITION BY day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5)
                     AS preferred_rank
            FROM raw_fb_ads
            WHERE day_date IN ({placeholders})
         )
         SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                COALESCE(SUM(CAST(ROUND(spend * (1.0 + tax_rate / 100.0) * 100) AS INTEGER)), 0),
                SUM(impressions),
                SUM(clicks),
                SUM(CASE WHEN clicks IS NOT NULL AND cpc IS NOT NULL
                         THEN clicks * cpc * (1.0 + tax_rate / 100.0) ELSE 0 END)
         FROM ranked
         WHERE (CASE level WHEN 'ad_group' THEN 0 ELSE 1 END) = preferred_rank
         GROUP BY day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5",
    );
    let mut stmt = conn.prepare_cached(&sql)?;
    let iter = stmt.query_map(date_params, |r| {
        let day_date: String = r.get(0)?;
        let tuple: [String; 5] = [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?];
        Ok((
            day_date,
            RawFbAds {
                canonical: to_canonical(tuple),
                spend_cents: r.get(6)?,
                imps: r.get(7)?,
                clicks: r.get(8)?,
                weighted_cpc_sum: r.get(9)?,
            },
        ))
    })?;
    let mut map: HashMap<String, Vec<RawFbAds>> = HashMap::new();
    for row in iter {
        let (date, v) = row?;
        map.entry(date).or_default().push(v);
    }
    Ok(map)
}

fn batch_fetch_fb_hier(
    conn: &Connection,
    placeholders: &str,
    date_params: &[&dyn rusqlite::ToSql],
) -> CmdResult<HashMap<String, Vec<RawFbHier>>> {
    let sql = format!(
        "SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                campaign_name, ad_set_name, ad_name, occurrence_idx,
                CAST(ROUND(COALESCE(spend, 0) * (1.0 + tax_rate / 100.0) * 100) AS INTEGER),
                clicks,
                CASE WHEN clicks IS NOT NULL AND cpc IS NOT NULL
                     THEN clicks * cpc * (1.0 + tax_rate / 100.0) ELSE 0 END
         FROM raw_fb_ads_hierarchy
         WHERE day_date IN ({placeholders})",
    );
    let mut stmt = conn.prepare_cached(&sql)?;
    let iter = stmt.query_map(date_params, |r| {
        let day_date: String = r.get(0)?;
        let tuple: [String; 5] = [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?];
        Ok((
            day_date,
            RawFbHier {
                canonical: to_canonical(tuple),
                campaign_name: r.get(6)?,
                ad_set_name: r.get(7)?,
                ad_name: r.get(8)?,
                occurrence_idx: r.get(9)?,
                spend_cents: r.get(10)?,
                clicks: r.get(11)?,
                weighted_cpc_sum: r.get(12)?,
            },
        ))
    })?;
    let mut map: HashMap<String, Vec<RawFbHier>> = HashMap::new();
    for row in iter {
        let (date, v) = row?;
        map.entry(date).or_default().push(v);
    }
    Ok(map)
}

fn batch_fetch_shopee_clicks(
    conn: &Connection,
    placeholders: &str,
    date_params: &[&dyn rusqlite::ToSql],
    account_id_eq: Option<i64>,
) -> CmdResult<HashMap<String, Vec<RawShopeeClick>>> {
    let mut sql = format!(
        "SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                COALESCE(shopee_account_id, 0) AS acc,
                COALESCE(referrer, '(khác)') AS ref, COUNT(*) AS cnt
         FROM raw_shopee_clicks
         WHERE day_date IN ({placeholders})",
    );
    let mut extra_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(id) = account_id_eq {
        sql.push_str(" AND shopee_account_id = ?");
        extra_params.push(Box::new(id));
    }
    sql.push_str(" GROUP BY day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, acc, ref");

    let mut all_params: Vec<&dyn rusqlite::ToSql> = date_params.to_vec();
    let extra_refs: Vec<&dyn rusqlite::ToSql> =
        extra_params.iter().map(|b| b.as_ref() as &dyn rusqlite::ToSql).collect();
    all_params.extend(extra_refs.iter().copied());

    let mut stmt = conn.prepare_cached(&sql)?;
    let iter = stmt.query_map(all_params.as_slice(), |r| {
        let day_date: String = r.get(0)?;
        let tuple: [String; 5] = [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?];
        Ok((
            day_date,
            RawShopeeClick {
                canonical: to_canonical(tuple),
                account_id: r.get(6)?,
                referrer: r.get(7)?,
                count: r.get(8)?,
            },
        ))
    })?;
    let mut map: HashMap<String, Vec<RawShopeeClick>> = HashMap::new();
    for row in iter {
        let (date, v) = row?;
        map.entry(date).or_default().push(v);
    }
    Ok(map)
}

fn batch_fetch_shopee_orders(
    conn: &Connection,
    placeholders: &str,
    date_params: &[&dyn rusqlite::ToSql],
    account_id_eq: Option<i64>,
) -> CmdResult<HashMap<String, Vec<RawShopeeOrder>>> {
    let mut sql = format!(
        "SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                COALESCE(shopee_account_id, 0) AS acc,
                COUNT(DISTINCT order_id),
                COALESCE(SUM(CAST(ROUND(net_commission * 100) AS INTEGER)), 0),
                COALESCE(SUM(CASE WHEN order_status IN ('Đang chờ xử lý', 'Chưa thanh toán')
                                  THEN CAST(ROUND(net_commission * 100) AS INTEGER)
                                  ELSE 0 END), 0),
                COALESCE(SUM(CAST(ROUND(order_value * 100) AS INTEGER)), 0),
                COALESCE(SUM(CAST(ROUND(mcn_fee * 100) AS INTEGER)), 0)
         FROM raw_shopee_order_items
         WHERE day_date IN ({placeholders})",
    );
    let mut extra_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(id) = account_id_eq {
        sql.push_str(" AND shopee_account_id = ?");
        extra_params.push(Box::new(id));
    }
    sql.push_str(
        " GROUP BY day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, acc",
    );

    let mut all_params: Vec<&dyn rusqlite::ToSql> = date_params.to_vec();
    let extra_refs: Vec<&dyn rusqlite::ToSql> =
        extra_params.iter().map(|b| b.as_ref() as &dyn rusqlite::ToSql).collect();
    all_params.extend(extra_refs.iter().copied());

    let mut stmt = conn.prepare_cached(&sql)?;
    let iter = stmt.query_map(all_params.as_slice(), |r| {
        let day_date: String = r.get(0)?;
        let tuple: [String; 5] = [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?];
        Ok((
            day_date,
            RawShopeeOrder {
                canonical: to_canonical(tuple),
                account_id: r.get(6)?,
                orders: r.get(7)?,
                commission_cents: r.get(8)?,
                commission_pending_cents: r.get(9)?,
                order_value_cents: r.get(10)?,
                mcn_fee_cents: r.get(11)?,
            },
        ))
    })?;
    let mut map: HashMap<String, Vec<RawShopeeOrder>> = HashMap::new();
    for row in iter {
        let (date, v) = row?;
        map.entry(date).or_default().push(v);
    }
    Ok(map)
}

fn batch_fetch_manuals(
    conn: &Connection,
    placeholders: &str,
    date_params: &[&dyn rusqlite::ToSql],
    account_id_eq: Option<i64>,
) -> CmdResult<HashMap<String, Vec<RawManual>>> {
    let mut sql = format!(
        "SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                display_name, override_clicks, override_spend, override_cpc,
                override_orders, override_commission, shopee_account_id
         FROM manual_entries
         WHERE day_date IN ({placeholders})",
    );
    let mut extra_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(id) = account_id_eq {
        sql.push_str(" AND shopee_account_id = ?");
        extra_params.push(Box::new(id));
    }

    let mut all_params: Vec<&dyn rusqlite::ToSql> = date_params.to_vec();
    let extra_refs: Vec<&dyn rusqlite::ToSql> =
        extra_params.iter().map(|b| b.as_ref() as &dyn rusqlite::ToSql).collect();
    all_params.extend(extra_refs.iter().copied());

    let mut stmt = conn.prepare_cached(&sql)?;
    let iter = stmt.query_map(all_params.as_slice(), |r| {
        let day_date: String = r.get(0)?;
        let tuple: [String; 5] = [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?];
        Ok((
            day_date,
            RawManual {
                canonical: to_canonical(tuple),
                display_name: r.get(6)?,
                clicks: r.get(7)?,
                spend: r.get(8)?,
                cpc: r.get(9)?,
                orders: r.get(10)?,
                commission: r.get(11)?,
                shopee_account_id: r.get(12)?,
            },
        ))
    })?;
    let mut map: HashMap<String, Vec<RawManual>> = HashMap::new();
    for row in iter {
        let (date, v) = row?;
        map.entry(date).or_default().push(v);
    }
    Ok(map)
}

/// Batch-fetch all-account owner pairs khi account_filter = Account(X).
/// Trả về `HashMap<String, RawOwnerPairs>` keyed by day_date.
fn batch_fetch_all_account_owner_pairs(
    conn: &Connection,
    placeholders: &str,
    date_params: &[&dyn rusqlite::ToSql],
) -> CmdResult<HashMap<String, RawOwnerPairs>> {
    // Hard owner pairs: orders UNION manual (all accounts).
    let hard_sql = format!(
        "SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, COALESCE(shopee_account_id, 0)
         FROM raw_shopee_order_items WHERE day_date IN ({placeholders})
         UNION
         SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, COALESCE(shopee_account_id, 0)
         FROM manual_entries WHERE day_date IN ({placeholders})",
    );
    // date_params must be doubled for both IN clauses.
    let doubled: Vec<&dyn rusqlite::ToSql> =
        date_params.iter().copied().chain(date_params.iter().copied()).collect();
    let mut stmt = conn.prepare_cached(&hard_sql)?;
    let iter = stmt.query_map(doubled.as_slice(), |r| {
        let day_date: String = r.get(0)?;
        let tuple: [String; 5] = [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?];
        let acc: i64 = r.get(6)?;
        Ok((day_date, to_canonical(tuple), acc))
    })?;
    let mut hard_map: HashMap<String, Vec<(Canonical, i64)>> = HashMap::new();
    for row in iter {
        let (date, canon, acc) = row?;
        hard_map.entry(date).or_default().push((canon, acc));
    }

    // Click owner pairs: all accounts.
    let click_sql = format!(
        "SELECT DISTINCT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                COALESCE(shopee_account_id, 0)
         FROM raw_shopee_clicks WHERE day_date IN ({placeholders})",
    );
    let mut stmt2 = conn.prepare_cached(&click_sql)?;
    let iter2 = stmt2.query_map(date_params, |r| {
        let day_date: String = r.get(0)?;
        let tuple: [String; 5] = [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?];
        let acc: i64 = r.get(6)?;
        Ok((day_date, to_canonical(tuple), acc))
    })?;
    let mut click_map: HashMap<String, Vec<(Canonical, i64)>> = HashMap::new();
    for row in iter2 {
        let (date, canon, acc) = row?;
        click_map.entry(date).or_default().push((canon, acc));
    }

    // Anchors: distinct canonicals from orders (all accounts).
    let anchor_sql = format!(
        "SELECT DISTINCT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
         FROM raw_shopee_order_items WHERE day_date IN ({placeholders})",
    );
    let mut stmt3 = conn.prepare_cached(&anchor_sql)?;
    let iter3 = stmt3.query_map(date_params, |r| {
        let day_date: String = r.get(0)?;
        let tuple: [String; 5] = [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?];
        Ok((day_date, to_canonical(tuple)))
    })?;
    let mut anchor_map: HashMap<String, HashSet<Canonical>> = HashMap::new();
    for row in iter3 {
        let (date, canon) = row?;
        anchor_map.entry(date).or_default().insert(canon);
    }

    // Merge into per-date RawOwnerPairs.
    let mut result: HashMap<String, RawOwnerPairs> = HashMap::new();
    // Collect all dates seen across all three maps.
    let all_dates: HashSet<&String> = hard_map
        .keys()
        .chain(click_map.keys())
        .chain(anchor_map.keys())
        .collect();
    for date in all_dates {
        result.insert(
            date.clone(),
            RawOwnerPairs {
                hard_owner_pairs: hard_map.get(date).cloned().unwrap_or_default(),
                click_owner_pairs: click_map.get(date).cloned().unwrap_or_default(),
                anchors: anchor_map
                    .get(date)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            },
        );
    }
    Ok(result)
}

/// Fallback single-day query path (used when dates > 900 or for direct calls).
/// Fetches 5 data sources for a single day_date, builds owner pairs, then
/// delegates to `process_day_data` for Phase 2 + Phase 3 logic.
fn aggregate_rows_for_day(
    conn: &Connection,
    day_date: &str,
    account_filter: &AccountFilterMode,
    match_mode: SubIdMatchMode,
    default_id: Option<i64>,
    account_names: &HashMap<i64, String>,
) -> CmdResult<(Vec<UiRow>, crate::db::types::UiDayTotals)> {
    let account_id_eq: Option<i64> = match account_filter {
        AccountFilterMode::Account { id } => Some(*id),
        _ => None,
    };

    // Build single-date IN clause (1 placeholder) — reuse batch helpers.
    let placeholders = "?";
    let date_ref: &dyn rusqlite::ToSql = &day_date;
    let date_params: &[&dyn rusqlite::ToSql] = &[date_ref];

    let fb_ads = batch_fetch_fb_ads(conn, placeholders, date_params)?
        .remove(day_date)
        .unwrap_or_default();
    let fb_hier_rows = batch_fetch_fb_hier(conn, placeholders, date_params)?
        .remove(day_date)
        .unwrap_or_default();
    let shopee_clicks =
        batch_fetch_shopee_clicks(conn, placeholders, date_params, account_id_eq)?
            .remove(day_date)
            .unwrap_or_default();
    let shopee_orders =
        batch_fetch_shopee_orders(conn, placeholders, date_params, account_id_eq)?
            .remove(day_date)
            .unwrap_or_default();
    let manuals = batch_fetch_manuals(conn, placeholders, date_params, account_id_eq)?
        .remove(day_date)
        .unwrap_or_default();

    // Owner pairs: derive from already-fetched data when All, else query all accounts.
    let (hard_owner_pairs, click_owner_pairs, anchors) = if account_id_eq.is_none() {
        let hard: Vec<(Canonical, i64)> = shopee_orders
            .iter()
            .map(|o| (o.canonical.clone(), o.account_id))
            .chain(manuals.iter().map(|m| (m.canonical.clone(), m.shopee_account_id.unwrap_or(0))))
            .collect();
        let click: Vec<(Canonical, i64)> = shopee_clicks
            .iter()
            .map(|c| (c.canonical.clone(), c.account_id))
            .collect();
        let anch: Vec<Canonical> = shopee_orders
            .iter()
            .map(|o| o.canonical.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        (hard, click, anch)
    } else {
        let owner_pairs = batch_fetch_all_account_owner_pairs(conn, placeholders, date_params)?
            .remove(day_date)
            .unwrap_or(RawOwnerPairs {
                hard_owner_pairs: vec![],
                click_owner_pairs: vec![],
                anchors: vec![],
            });
        (owner_pairs.hard_owner_pairs, owner_pairs.click_owner_pairs, owner_pairs.anchors)
    };

    process_day_data(
        day_date,
        account_filter,
        match_mode,
        default_id,
        account_names,
        fb_ads,
        fb_hier_rows,
        shopee_clicks,
        shopee_orders,
        manuals,
        hard_owner_pairs,
        click_owner_pairs,
        anchors,
    )
}


#[cfg(test)]
mod tests {
    use super::*;
    use super::aggregate::{append_subid_prefilter, is_prefix, sub_ids_match};
    use super::overview::load_overview_impl;

    #[test]
    fn canonical_strips_trailing_empty() {
        let c = to_canonical([
            "a".into(),
            "b".into(),
            "".into(),
            "".into(),
            "".into(),
        ]);
        assert_eq!(c, vec!["a".to_string(), "b".to_string()]);

        let c2 = to_canonical([
            "a".into(),
            "b".into(),
            "c".into(),
            "".into(),
            "".into(),
        ]);
        assert_eq!(
            c2,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn prefix_match() {
        let ab: Canonical = vec!["a".into(), "b".into()];
        let abc: Canonical = vec!["a".into(), "b".into(), "c".into()];
        let abd: Canonical = vec!["a".into(), "b".into(), "d".into()];
        let xy: Canonical = vec!["x".into(), "y".into()];

        assert!(is_prefix(&ab, &abc));
        assert!(!is_prefix(&abc, &ab));
        assert!(!is_prefix(&abc, &abd));
        assert!(!is_prefix(&ab, &xy));
    }

    #[test]
    fn fb_merges_into_shopee_order_anchor() {
        // Shopee order (hoa hồng) = anchor
        let order: Canonical = vec!["Muse".into(), "aoto".into()];
        let fb_with_date: Canonical =
            vec!["Muse".into(), "aoto".into(), "0412".into()];
        let fb_other: Canonical = vec!["dammaxi".into()];

        let anchors = vec![order.clone()];

        // FB [Muse, aoto, 0412] → anchor [Muse, aoto] (order) → rep = order
        assert_eq!(representative(&fb_with_date, &anchors, SubIdMatchMode::Exact), order);
        // Order itself → anchor matches self → rep = self
        assert_eq!(representative(&order, &anchors, SubIdMatchMode::Exact), order);
        // FB [dammaxi] không có anchor compatible → rep = self (fallback tên camp)
        assert_eq!(representative(&fb_other, &anchors, SubIdMatchMode::Exact), fb_other);
    }

    #[test]
    fn fb_standalone_when_no_commission() {
        let fb: Canonical = vec!["abc".into(), "def".into(), "0412".into()];
        // Không có anchor (không có Shopee order nào cho day này)
        let anchors: Vec<Canonical> = vec![];
        assert_eq!(representative(&fb, &anchors, SubIdMatchMode::Exact), fb);
    }

    #[test]
    fn anchor_tiebreak_longest_most_specific() {
        // 2 anchor overlap: [A] và [A, B]
        let short: Canonical = vec!["A".into()];
        let long: Canonical = vec!["A".into(), "B".into()];
        let fb: Canonical = vec!["A".into(), "B".into(), "C".into()];

        let anchors = vec![short.clone(), long.clone()];
        // FB [A, B, C] compatible với cả 2 anchor → pick anchor dài nhất = [A, B]
        assert_eq!(representative(&fb, &anchors, SubIdMatchMode::Exact), long);
    }

    // =====================================================================
    // SubIdMatchMode::Substring — joined canonical case-insensitive contains.
    // =====================================================================

    #[test]
    fn substring_mode_dungcamp1_matches_camp1() {
        let shopee: Canonical = vec!["dungcamp1".into()];
        let fb: Canonical = vec!["camp1".into()];
        // Exact: tuple position 0 khác → không match.
        assert!(!is_compatible(&shopee, &fb, SubIdMatchMode::Exact));
        // Substring: "dungcamp1".contains("camp1") → match.
        assert!(is_compatible(&shopee, &fb, SubIdMatchMode::Substring));
        assert!(is_compatible(&fb, &shopee, SubIdMatchMode::Substring));
    }

    #[test]
    fn substring_mode_case_insensitive() {
        let a: Canonical = vec!["MuseStudio".into()];
        let b: Canonical = vec!["studio".into()];
        // Slot equality fails ("MuseStudio" != "studio").
        assert!(!is_compatible(&a, &b, SubIdMatchMode::Exact));
        // Substring case-insensitive matches.
        assert!(is_compatible(&a, &b, SubIdMatchMode::Substring));
    }

    #[test]
    fn substring_mode_min_3_chars_guard() {
        // "ab" và "abc" — short side dài 2 < 3 → fallback equality → không match.
        let a: Canonical = vec!["ab".into()];
        let b: Canonical = vec!["abc".into()];
        assert!(!is_compatible(&a, &b, SubIdMatchMode::Substring));
        // Cùng length 2 và bằng nhau → match (equality).
        let c: Canonical = vec!["ab".into()];
        assert!(is_compatible(&a, &c, SubIdMatchMode::Substring));
    }

    #[test]
    fn substring_mode_anchor_resolution() {
        // FB "camp1" với anchor Shopee "dungcamp1" — substring mode merge.
        let anchor: Canonical = vec!["dungcamp1".into()];
        let fb: Canonical = vec!["camp1".into()];
        let anchors = vec![anchor.clone()];
        // Exact: không merge — fallback rep = self.
        assert_eq!(representative(&fb, &anchors, SubIdMatchMode::Exact), fb);
        // Substring: merge về anchor dài hơn = "dungcamp1".
        assert_eq!(
            representative(&fb, &anchors, SubIdMatchMode::Substring),
            anchor
        );
    }

    #[test]
    fn substring_mode_preserves_exact_behavior_when_compatible() {
        // Khi tuple đã prefix-compatible (Exact match), Substring KHÔNG thay
        // đổi kết quả — superset semantics.
        let order: Canonical = vec!["Muse".into(), "aoto".into()];
        let fb: Canonical = vec!["Muse".into(), "aoto".into(), "0412".into()];
        let anchors = vec![order.clone()];
        assert_eq!(
            representative(&fb, &anchors, SubIdMatchMode::Exact),
            representative(&fb, &anchors, SubIdMatchMode::Substring)
        );
    }

    #[test]
    fn empty_canonical_does_not_match_non_empty() {
        let empty: Canonical = vec![];
        let sp1: Canonical = vec!["sp1".into()];
        let ab: Canonical = vec!["a".into(), "b".into()];

        // Empty không compatible với non-empty dù ở mode nào.
        assert!(!is_compatible(&empty, &sp1, SubIdMatchMode::Exact));
        assert!(!is_compatible(&sp1, &empty, SubIdMatchMode::Exact));
        assert!(!is_compatible(&empty, &ab, SubIdMatchMode::Substring));
        assert!(!is_compatible(&ab, &empty, SubIdMatchMode::Substring));

        // Empty compatible với chính nó (2 row đều không có sub_id → cùng nhóm).
        assert!(is_compatible(&empty, &empty, SubIdMatchMode::Exact));
        assert!(is_compatible(&empty, &empty, SubIdMatchMode::Substring));
    }

    #[test]
    fn empty_canonical_not_merged_into_anchor() {
        // FB ad không có sub_id (canonical = []) KHÔNG được merge vào anchor
        // "sp1" dù là prefix-compatible theo định nghĩa cũ (bug đã fix).
        let empty_fb: Canonical = vec![];
        let anchor: Canonical = vec!["sp1".into()];
        let anchors = vec![anchor.clone()];

        // representative([]) phải trả về [] (fallback), không phải anchor.
        assert_eq!(
            representative(&empty_fb, &anchors, SubIdMatchMode::Exact),
            empty_fb
        );
        assert_eq!(
            representative(&empty_fb, &anchors, SubIdMatchMode::Substring),
            empty_fb
        );
    }

    // ========================================================================
    // Subset match — thay thế cho FE `matchSubId` trong App.tsx.
    // ========================================================================

    #[test]
    fn subset_match_empty_selected_matches_any() {
        assert!(display_name_subset_match("anything", &[]));
        assert!(display_name_subset_match("", &[]));
    }

    #[test]
    fn subset_match_single_part() {
        let sel = vec!["dammaxi".to_string()];
        assert!(display_name_subset_match("dammaxi", &sel));
        assert!(display_name_subset_match("dammaxi-0410", &sel));
        assert!(display_name_subset_match("MuseStudio-dammaxi-0412", &sel));
        assert!(!display_name_subset_match("MuseStudio", &sel));
        assert!(!display_name_subset_match("", &sel));
    }

    #[test]
    fn subset_match_multi_part_order_agnostic() {
        // Cả 2 part đều phải có mặt, không quan tâm thứ tự.
        let sel = vec!["MuseStudio".to_string(), "dammaxi".to_string()];
        assert!(display_name_subset_match("MuseStudio-dammaxi", &sel));
        assert!(display_name_subset_match("MuseStudio-dammaxi-0412", &sel));
        assert!(display_name_subset_match("dammaxi-MuseStudio", &sel)); // order-agnostic
        assert!(!display_name_subset_match("dammaxi-0412", &sel)); // thiếu MuseStudio
        assert!(!display_name_subset_match("MuseStudio-other", &sel)); // thiếu dammaxi
    }

    // ========================================================================
    // Integration: seed DB → list_days_with_rows / load_overview.
    // Dùng in-memory SQLite với full schema (re-include từ db/schema.sql).
    // ========================================================================

    const SCHEMA_SQL: &str = include_str!("../../db/schema.sql");

    fn seed_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
        // Chạy migrations thật — include shopee_accounts + FK columns.
        crate::db::migrate_for_tests(&conn).unwrap();
        conn
    }

    fn seed_day(conn: &Connection, date: &str) {
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES(?, ?)",
            params![date, format!("{date}T00:00:00Z")],
        )
        .unwrap();
    }

    fn seed_manual(
        conn: &Connection,
        date: &str,
        sub_id1: &str,
        sub_id2: &str,
        spend: f64,
        commission: f64,
    ) {
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, sub_id2, day_date, override_spend, override_commission,
              created_at, updated_at)
             VALUES(?, ?, ?, ?, ?, 'now', 'now')",
            params![sub_id1, sub_id2, date, spend, commission],
        )
        .unwrap();
    }

    #[test]
    fn list_days_limit_takes_most_recent() {
        let conn = seed_conn();
        seed_day(&conn, "2026-04-15");
        seed_day(&conn, "2026-04-16");
        seed_day(&conn, "2026-04-17");
        seed_manual(&conn, "2026-04-15", "s", "a", 10.0, 1.0);
        seed_manual(&conn, "2026-04-16", "s", "a", 10.0, 1.0);
        seed_manual(&conn, "2026-04-17", "s", "a", 10.0, 1.0);

        let days = list_days_with_rows_impl(
            &conn,
            DaysFilter {
                limit: Some(2),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].date, "2026-04-17");
        assert_eq!(days[1].date, "2026-04-16");
    }

    #[test]
    fn list_days_date_range_inclusive() {
        let conn = seed_conn();
        for d in ["2026-04-15", "2026-04-16", "2026-04-17", "2026-04-18"] {
            seed_day(&conn, d);
            seed_manual(&conn, d, "s", "a", 10.0, 1.0);
        }

        let days = list_days_with_rows_impl(
            &conn,
            DaysFilter {
                from_date: Some("2026-04-16".into()),
                to_date: Some("2026-04-17".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].date, "2026-04-17");
        assert_eq!(days[1].date, "2026-04-16");
    }

    #[test]
    fn list_days_sub_id_filter_drops_non_matching_rows_and_empty_days() {
        let conn = seed_conn();
        seed_day(&conn, "2026-04-17");
        seed_day(&conn, "2026-04-18");
        seed_manual(&conn, "2026-04-17", "MuseStudio", "dammaxi", 10.0, 1.0);
        seed_manual(&conn, "2026-04-17", "MuseStudio", "other", 10.0, 1.0);
        seed_manual(&conn, "2026-04-18", "MuseStudio", "other", 10.0, 1.0);

        // Filter "dammaxi" → chỉ day 04-17 với 1 row match.
        let days = list_days_with_rows_impl(
            &conn,
            DaysFilter {
                sub_id_filter: Some("dammaxi".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].date, "2026-04-17");
        assert_eq!(days[0].rows.len(), 1);
        assert_eq!(days[0].rows[0].display_name, "MuseStudio-dammaxi");
    }

    #[test]
    fn list_days_no_filter_equivalent_to_legacy() {
        let conn = seed_conn();
        seed_day(&conn, "2026-04-17");
        seed_manual(&conn, "2026-04-17", "s", "a", 10.0, 1.0);

        let days = list_days_with_rows_impl(&conn, DaysFilter::default()).unwrap();
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].rows.len(), 1);
    }

    #[test]
    fn load_overview_aggregates_across_all_days() {
        let conn = seed_conn();
        seed_day(&conn, "2026-04-15");
        seed_day(&conn, "2026-04-17");
        seed_manual(&conn, "2026-04-15", "MuseStudio", "dammaxi", 10.0, 1.0);
        seed_manual(&conn, "2026-04-17", "shopA", "other", 5.0, 2.0);

        let ov = load_overview_impl(&conn).unwrap();
        assert_eq!(ov.total_days_count, 2);
        assert_eq!(ov.total_rows_count, 2);
        assert_eq!(ov.oldest_date.as_deref(), Some("2026-04-15"));
        assert_eq!(ov.newest_date.as_deref(), Some("2026-04-17"));

        // allSubIds phải chứa full names + prefix levels + từng part riêng.
        assert!(ov.all_sub_ids.contains(&"MuseStudio-dammaxi".into()));
        assert!(ov.all_sub_ids.contains(&"MuseStudio".into())); // prefix + part
        assert!(ov.all_sub_ids.contains(&"dammaxi".into())); // part
        assert!(ov.all_sub_ids.contains(&"shopA-other".into()));
        assert!(ov.all_sub_ids.contains(&"shopA".into()));
        assert!(ov.all_sub_ids.contains(&"other".into()));

        // Sorted.
        let mut sorted = ov.all_sub_ids.clone();
        sorted.sort();
        assert_eq!(ov.all_sub_ids, sorted);
    }

    #[test]
    fn load_overview_empty_db() {
        let conn = seed_conn();
        let ov = load_overview_impl(&conn).unwrap();
        assert_eq!(ov.total_days_count, 0);
        assert_eq!(ov.total_rows_count, 0);
        assert!(ov.oldest_date.is_none());
        assert!(ov.newest_date.is_none());
        assert!(ov.all_sub_ids.is_empty());
    }

    // ========================================================================
    // Multi-account aggregate split — 2 acc Shopee cùng ngày tách thành rows
    // riêng theo (canonical, account_id). Bảo vệ regression v0.4.5+.
    // ========================================================================

    fn seed_account(conn: &Connection, name: &str) -> i64 {
        // shopee_accounts.id là INTEGER PRIMARY KEY → autoincrement nếu không
        // truyền giá trị. Tests gen id liên tục từ DB (1, 2, ...).
        conn.execute(
            "INSERT INTO shopee_accounts(name, created_at) VALUES(?, datetime('now'))",
            params![name],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn seed_imported_file(conn: &Connection, kind: &str, day_date: &str) -> i64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        // Counter unique per process — tránh file_hash collision khi 1 test gọi
        // helper nhiều lần (UNIQUE(file_hash) trên imported_files).
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        conn.execute(
            "INSERT OR IGNORE INTO days(date, created_at) VALUES(?, ?)",
            params![day_date, format!("{day_date}T00:00:00Z")],
        )
        .unwrap();
        let filename = format!("test-{kind}-{day_date}-{seq}.csv");
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES(?, ?, datetime('now'), ?, ?)",
            params![&filename, kind, format!("hash-{filename}-{seq}"), day_date],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Insert 1 raw_shopee_order_items row. Mỗi call = 1 đơn riêng (DISTINCT
    /// order_id) → có thể seed N call để bơm orders_count.
    fn seed_shopee_order(
        conn: &Connection,
        day_date: &str,
        account_id: i64,
        sub_ids: [&str; 5],
        order_seq: i64,
        net_commission: f64,
        order_value: f64,
    ) {
        let file_id = seed_imported_file(conn, "shopee_orders", day_date);
        conn.execute(
            "INSERT INTO raw_shopee_order_items
             (order_id, checkout_id, item_id, model_id, order_status,
              order_value, net_commission,
              sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
              day_date, source_file_id, shopee_account_id)
             VALUES(?, ?, ?, '', 'Đã hoàn thành', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                format!("ORD-{account_id}-{order_seq}"),
                format!("CHK-{account_id}-{order_seq}"),
                format!("ITM-{account_id}-{order_seq}"),
                order_value,
                net_commission,
                sub_ids[0],
                sub_ids[1],
                sub_ids[2],
                sub_ids[3],
                sub_ids[4],
                day_date,
                file_id,
                account_id,
            ],
        )
        .unwrap();
    }

    fn seed_fb_ad(
        conn: &Connection,
        day_date: &str,
        sub_ids: [&str; 5],
        spend: f64,
        clicks: i64,
    ) {
        let file_id = seed_imported_file(conn, "fb_ads", day_date);
        conn.execute(
            "INSERT INTO raw_fb_ads
             (level, name, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
              spend, clicks, day_date, source_file_id)
             VALUES('campaign', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                format!("camp-{}", sub_ids.join("-")),
                sub_ids[0],
                sub_ids[1],
                sub_ids[2],
                sub_ids[3],
                sub_ids[4],
                spend,
                clicks,
                day_date,
                file_id,
            ],
        )
        .unwrap();
    }

    #[test]
    fn multi_account_same_tuple_splits_into_separate_rows() {
        let conn = seed_conn();
        let acc_a = seed_account(&conn, "ShopA");
        let acc_b = seed_account(&conn, "ShopB");
        let date = "2026-04-20";
        seed_day(&conn, date);

        // Cả 2 acc đều có 1 đơn cùng tuple sub_id, commission khác nhau.
        seed_shopee_order(&conn, date, acc_a, ["camp", "x", "", "", ""], 1, 100.0, 1000.0);
        seed_shopee_order(&conn, date, acc_b, ["camp", "x", "", "", ""], 2, 200.0, 2000.0);

        let days =
            list_days_with_rows_impl(&conn, DaysFilter::default()).unwrap();
        assert_eq!(days.len(), 1);
        let day = &days[0];
        // Phải tách thành 2 rows riêng — KHÔNG merge cross-account.
        assert_eq!(day.rows.len(), 2, "expected 1 row per account");

        let row_a = day.rows.iter().find(|r| r.account_id == Some(acc_a)).unwrap();
        let row_b = day.rows.iter().find(|r| r.account_id == Some(acc_b)).unwrap();
        assert_eq!(row_a.commission_total, 100.0);
        assert_eq!(row_a.account_name.as_deref(), Some("ShopA"));
        assert_eq!(row_b.commission_total, 200.0);
        assert_eq!(row_b.account_name.as_deref(), Some("ShopB"));

        // Day totals = sum across accounts (KPI tổng vẫn đúng).
        assert_eq!(day.totals.commission_total, 300.0);
        assert_eq!(day.totals.orders_count, 2);
    }

    #[test]
    fn fb_with_two_owners_creates_fb_chung_row() {
        let conn = seed_conn();
        let acc_a = seed_account(&conn, "ShopA");
        let acc_b = seed_account(&conn, "ShopB");
        let date = "2026-04-21";
        seed_day(&conn, date);

        // Cùng tuple — 2 acc Shopee đều có order, FB ad cũng cùng tuple.
        seed_shopee_order(&conn, date, acc_a, ["camp", "y", "", "", ""], 1, 50.0, 500.0);
        seed_shopee_order(&conn, date, acc_b, ["camp", "y", "", "", ""], 2, 70.0, 700.0);
        seed_fb_ad(&conn, date, ["camp", "y", "", "", ""], 999.0, 100);

        let days =
            list_days_with_rows_impl(&conn, DaysFilter::default()).unwrap();
        let day = &days[0];

        // 3 rows: 1 cho A (Shopee), 1 cho B (Shopee), 1 cho FB chung (None).
        assert_eq!(day.rows.len(), 3, "expected A + B + FB chung");
        let fb_chung = day
            .rows
            .iter()
            .find(|r| r.account_id.is_none())
            .expect("expected FB chung row");
        assert!(fb_chung.account_name.is_none());
        assert_eq!(fb_chung.total_spend, Some(999.0));
        assert!(fb_chung.has_fb);
        assert!(!fb_chung.has_shopee_orders);

        // Spend không được duplicate sang A/B — nằm 1 chỗ duy nhất ở FB chung.
        let row_a = day.rows.iter().find(|r| r.account_id == Some(acc_a)).unwrap();
        let row_b = day.rows.iter().find(|r| r.account_id == Some(acc_b)).unwrap();
        assert_eq!(row_a.total_spend, None);
        assert_eq!(row_b.total_spend, None);
        // Day total spend chỉ count 1 lần (no double count).
        assert_eq!(day.totals.total_spend, 999.0);
    }

    #[test]
    fn fb_with_single_owner_merges_with_that_account() {
        let conn = seed_conn();
        let acc_a = seed_account(&conn, "ShopA");
        let date = "2026-04-22";
        seed_day(&conn, date);

        seed_shopee_order(&conn, date, acc_a, ["camp", "z", "", "", ""], 1, 80.0, 800.0);
        seed_fb_ad(&conn, date, ["camp", "z", "", "", ""], 500.0, 50);

        let days =
            list_days_with_rows_impl(&conn, DaysFilter::default()).unwrap();
        let day = &days[0];
        // 1 owner → FB merge thẳng vào row của owner đó (behavior single-acc cũ).
        assert_eq!(day.rows.len(), 1);
        let row = &day.rows[0];
        assert_eq!(row.account_id, Some(acc_a));
        assert_eq!(row.total_spend, Some(500.0));
        assert_eq!(row.commission_total, 80.0);
        assert!(row.has_fb && row.has_shopee_orders);
    }

    #[test]
    fn account_filter_returns_only_target_account_rows() {
        let conn = seed_conn();
        let acc_a = seed_account(&conn, "ShopA");
        let acc_b = seed_account(&conn, "ShopB");
        let date = "2026-04-23";
        seed_day(&conn, date);

        seed_shopee_order(&conn, date, acc_a, ["camp", "x", "", "", ""], 1, 100.0, 1000.0);
        seed_shopee_order(&conn, date, acc_b, ["camp", "x", "", "", ""], 2, 200.0, 2000.0);
        seed_fb_ad(&conn, date, ["camp", "x", "", "", ""], 999.0, 100);

        let days = list_days_with_rows_impl(
            &conn,
            DaysFilter {
                account_filter: Some(AccountFilterMode::Account { id: acc_a }),
                ..Default::default()
            },
        )
        .unwrap();
        let day = &days[0];
        // Filter=A → chỉ 1 row, FB attribute cho A (đã match owner A).
        assert_eq!(day.rows.len(), 1);
        assert_eq!(day.rows[0].account_id, Some(acc_a));
        assert_eq!(day.rows[0].commission_total, 100.0);
        assert_eq!(day.rows[0].total_spend, Some(999.0));
    }

    fn seed_shopee_click(
        conn: &Connection,
        day_date: &str,
        account_id: i64,
        sub_ids: [&str; 5],
        click_id_suffix: &str,
    ) {
        let file_id = seed_imported_file(conn, "shopee_clicks", day_date);
        conn.execute(
            "INSERT INTO raw_shopee_clicks
             (click_id, click_time, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
              day_date, source_file_id, shopee_account_id)
             VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                format!("CLK-{account_id}-{click_id_suffix}"),
                format!("{day_date} 10:00:00"),
                sub_ids[0],
                sub_ids[1],
                sub_ids[2],
                sub_ids[3],
                sub_ids[4],
                day_date,
                file_id,
                account_id,
            ],
        )
        .unwrap();
    }

    /// Regression test cho bug: import hoa hồng TK_A + click TK_B cùng sub_id
    /// + FB ads → trước fix `owners_for_day` có {A, B} → fb_bucket = None →
    /// FB chung thay vì TK_A → row TK_B mất FB spend → row-0 filter ẩn click.
    /// Sau fix: click TK_B chỉ là soft owner, không thêm vào nếu A đã hard-own.
    #[test]
    fn commission_account_a_click_account_b_same_sub_fb_attributed_to_a() {
        let conn = seed_conn();
        let acc_a = seed_account(&conn, "TK_A");
        let acc_b = seed_account(&conn, "TK_B");
        let date = "2026-04-30";
        seed_day(&conn, date);

        // TK_A có hoa hồng, TK_B có click, cùng sub_id "camp-q".
        seed_shopee_order(&conn, date, acc_a, ["camp", "q", "", "", ""], 1, 120.0, 1200.0);
        seed_shopee_click(&conn, date, acc_b, ["camp", "q", "", "", ""], "1");
        seed_fb_ad(&conn, date, ["camp", "q", "", "", ""], 500.0, 40);

        let days =
            list_days_with_rows_impl(&conn, DaysFilter::default()).unwrap();
        let day = &days[0];

        // Không được có "FB chung" (account_id = None) — FB phải về TK_A.
        let fb_chung = day.rows.iter().find(|r| r.account_id.is_none());
        assert!(
            fb_chung.is_none(),
            "FB không được là 'FB chung' khi TK_A là hard owner duy nhất"
        );

        // Row TK_A: commission + FB spend (single hard owner → merge).
        let row_a = day.rows.iter().find(|r| r.account_id == Some(acc_a)).unwrap();
        assert_eq!(row_a.commission_total, 120.0);
        assert_eq!(row_a.total_spend, Some(500.0));
        assert!(row_a.has_fb);

        // Day totals: spend không bị double count.
        assert_eq!(day.totals.total_spend, 500.0);
    }

    /// Click-only (không hoa hồng) vẫn được dùng làm soft owner cho FB.
    /// Đảm bảo fix không phá fallback case: TK_B có click cho sub_id "lone"
    /// nhưng không có hoa hồng → FB vẫn về TK_B (không phải Mặc định).
    #[test]
    fn click_only_account_acts_as_soft_owner_for_fb() {
        let conn = seed_conn();
        let acc_b = seed_account(&conn, "TK_B");
        let date = "2026-05-01";
        seed_day(&conn, date);

        // Chỉ click + FB, không có hoa hồng → soft owner TK_B.
        seed_shopee_click(&conn, date, acc_b, ["lone", "", "", "", ""], "1");
        seed_fb_ad(&conn, date, ["lone", "", "", "", ""], 300.0, 30);

        let days =
            list_days_with_rows_impl(&conn, DaysFilter::default()).unwrap();
        let day = &days[0];
        assert_eq!(day.rows.len(), 1);
        // FB phải về TK_B (fallback từ click owner).
        assert_eq!(day.rows[0].account_id, Some(acc_b));
        assert_eq!(day.rows[0].total_spend, Some(300.0));
    }

    #[test]
    fn legacy_default_account_seed_assigns_default_to_orphan_fb() {
        let conn = seed_conn();
        let date = "2026-04-24";
        seed_day(&conn, date);

        // Chỉ có FB ad — không có Shopee owner nào → FB phải gắn Mặc định.
        seed_fb_ad(&conn, date, ["camp", "lonely", "", "", ""], 300.0, 30);
        let default_id = default_account_id_lookup(&conn).expect("Mặc định seeded");

        let days =
            list_days_with_rows_impl(&conn, DaysFilter::default()).unwrap();
        let day = &days[0];
        assert_eq!(day.rows.len(), 1);
        assert_eq!(day.rows[0].account_id, Some(default_id));
        assert_eq!(
            day.rows[0].account_name.as_deref(),
            Some(DEFAULT_ACCOUNT_NAME)
        );
    }

    /// Click TK_B cho sub_id của TK_A → click phải hiện trong row TK_A (không bị row-0 filter).
    /// Scenario: import hoa hồng TK_A trước, import click TK_B sau — cùng sub_id.
    #[test]
    fn click_from_other_account_merged_into_hard_owner_row() {
        let conn = seed_conn();
        let acc_a = seed_account(&conn, "TK_A");
        let acc_b = seed_account(&conn, "TK_B");
        let date = "2026-05-10";
        seed_day(&conn, date);

        seed_shopee_order(&conn, date, acc_a, ["prod", "x", "", "", ""], 1, 80.0, 800.0);
        seed_shopee_click(&conn, date, acc_b, ["prod", "x", "", "", ""], "ck1");
        seed_shopee_click(&conn, date, acc_b, ["prod", "x", "", "", ""], "ck2");

        let days = list_days_with_rows_impl(&conn, DaysFilter::default()).unwrap();
        let day = &days[0];

        // Chỉ 1 row (TK_A) — không có row TK_B click-only bị row-0 filter.
        assert_eq!(day.rows.len(), 1, "phải chỉ có 1 row (TK_A)");
        let row = &day.rows[0];
        assert_eq!(row.account_id, Some(acc_a));
        assert_eq!(row.commission_total, 80.0);
        // 2 click của TK_B phải được merge vào row TK_A.
        assert_eq!(row.shopee_clicks_total, 2, "clicks TK_B phải merge vào row TK_A");
        assert!(row.has_shopee_clicks);

        // Totals không đổi.
        assert_eq!(day.totals.shopee_clicks_total, 2);
    }

    /// Khi 2 TK cùng có hoa hồng cho cùng sub_id (2 hard owners), click giữ nguyên
    /// account của click row — không thể xác định merge vào TK nào.
    #[test]
    fn click_not_merged_when_two_hard_owners() {
        let conn = seed_conn();
        let acc_a = seed_account(&conn, "TK_A");
        let acc_b = seed_account(&conn, "TK_B");
        let date = "2026-05-11";
        seed_day(&conn, date);

        // Cả TK_A và TK_B đều có hoa hồng cho cùng sub_id "shared" (đơn khác nhau).
        seed_shopee_order(&conn, date, acc_a, ["shared", "", "", "", ""], 1, 50.0, 500.0);
        seed_shopee_order(&conn, date, acc_b, ["shared", "", "", "", ""], 1, 60.0, 600.0);
        // Click từ TK_B — 2 hard owners → click giữ nguyên TK_B.
        seed_shopee_click(&conn, date, acc_b, ["shared", "", "", "", ""], "ckX");

        let days = list_days_with_rows_impl(&conn, DaysFilter::default()).unwrap();
        let day = &days[0];

        let row_a = day.rows.iter().find(|r| r.account_id == Some(acc_a)).unwrap();
        let row_b = day.rows.iter().find(|r| r.account_id == Some(acc_b)).unwrap();
        // Click về TK_B (không merge vào TK_A vì 2 hard owners).
        assert_eq!(row_b.shopee_clicks_total, 1);
        assert_eq!(row_a.shopee_clicks_total, 0);

        assert_eq!(day.totals.shopee_clicks_total, 1);
    }

    // =====================================================================
    // Regression tests cho perf optimizations (v0.10.3):
    // - append_subid_prefilter: pre-filter SQL pushdown cho sub_id matching
    // - list_imported_files CTE: gộp 4 subquery thành 1 UNION ALL + GROUP BY
    // =====================================================================

    #[test]
    fn subid_prefilter_exact_non_empty_pushes_slot0() {
        let mut sql = String::from("SELECT 1 WHERE 1=1");
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        append_subid_prefilter(
            &mut sql,
            &mut params,
            &["camp".into(), "x".into(), "".into(), "".into(), "".into()],
            SubIdMatchMode::Exact,
        );
        assert!(sql.ends_with(" AND sub_id1 = ?"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn subid_prefilter_exact_empty_locks_all_slots() {
        let mut sql = String::from("SELECT 1 WHERE 1=1");
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        append_subid_prefilter(
            &mut sql,
            &mut params,
            &["".into(), "".into(), "".into(), "".into(), "".into()],
            SubIdMatchMode::Exact,
        );
        // Empty target match chỉ empty row → SQL lock 5 cột = ''.
        assert!(sql.contains("sub_id1 = ''"));
        assert!(sql.contains("sub_id5 = ''"));
        assert!(params.is_empty());
    }

    #[test]
    fn subid_prefilter_substring_skips_pushdown() {
        let mut sql_before = String::from("SELECT 1 WHERE 1=1");
        let sql_clone = sql_before.clone();
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        append_subid_prefilter(
            &mut sql_before,
            &mut params,
            &["dungcamp1".into(), "".into(), "".into(), "".into(), "".into()],
            SubIdMatchMode::Substring,
        );
        // Substring không pushdown → SQL không đổi.
        assert_eq!(sql_before, sql_clone);
        assert!(params.is_empty());
    }

    /// Verify pre-filter là điều kiện CẦN của is_compatible: KHÔNG skip row nào
    /// thực sự compatible. Seed nhiều order với sub_id khác nhau, target=[a,b];
    /// chạy SQL với prefilter rồi compare với full-scan + Rust filter.
    #[test]
    fn subid_prefilter_exact_does_not_skip_compatible_rows() {
        let conn = seed_conn();
        let acc = seed_account(&conn, "TK");
        let date = "2026-04-20";
        seed_day(&conn, date);
        // Rows compatible với target=[a,b] (prefix bi-directional):
        seed_shopee_order(&conn, date, acc, ["a", "b", "", "", ""], 1, 10.0, 100.0); // = target
        seed_shopee_order(&conn, date, acc, ["a", "b", "c", "", ""], 2, 10.0, 100.0); // target prefix of row
        seed_shopee_order(&conn, date, acc, ["a", "", "", "", ""], 3, 10.0, 100.0); // row prefix of target
        // Rows KHÔNG compatible:
        seed_shopee_order(&conn, date, acc, ["a", "x", "", "", ""], 4, 10.0, 100.0); // slot 1 khác
        seed_shopee_order(&conn, date, acc, ["z", "b", "", "", ""], 5, 10.0, 100.0); // slot 0 khác
        seed_shopee_order(&conn, date, acc, ["", "", "", "", ""], 6, 10.0, 100.0); // empty

        // Pre-filter SQL: count rows passing slot0=a.
        let mut sql = String::from(
            "SELECT COUNT(*) FROM raw_shopee_order_items WHERE day_date = ?",
        );
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(date.to_string())];
        let target = ["a".to_string(), "b".to_string(), "".into(), "".into(), "".into()];
        append_subid_prefilter(&mut sql, &mut params, &target, SubIdMatchMode::Exact);
        let refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|b| b.as_ref()).collect();
        let prefiltered: i64 = conn
            .query_row(&sql, refs.as_slice(), |r| r.get(0))
            .unwrap();
        // Pre-filter giữ 4 row có sub_id1='a' (3 compatible + 1 không compatible "a,x").
        assert_eq!(prefiltered, 4);

        // Sanity: post-filter Rust giữ đúng 3 compatible.
        let target_canon = to_canonical(target.clone());
        let mut stmt = conn
            .prepare(
                "SELECT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
                 FROM raw_shopee_order_items WHERE day_date = ?",
            )
            .unwrap();
        let rows: Vec<[String; 5]> = stmt
            .query_map(params![date], |r| {
                Ok([r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?])
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        let kept: Vec<_> = rows
            .into_iter()
            .filter(|r| {
                is_compatible(
                    &to_canonical(r.clone()),
                    &target_canon,
                    SubIdMatchMode::Exact,
                )
            })
            .collect();
        assert_eq!(kept.len(), 3, "expected 3 compatible rows post-filter");
    }

    #[test]
    fn subid_prefilter_substring_full_scan_keeps_all_candidates() {
        let conn = seed_conn();
        let acc = seed_account(&conn, "TK");
        let date = "2026-04-20";
        seed_day(&conn, date);
        // Substring case: row [dungcamp1] match target [camp1] (slot0 khác!).
        seed_shopee_order(&conn, date, acc, ["dungcamp1", "", "", "", ""], 1, 10.0, 100.0);
        seed_shopee_order(&conn, date, acc, ["xyz", "", "", "", ""], 2, 10.0, 100.0);

        // Pre-filter Substring không pushdown → SQL chỉ filter day_date.
        let mut sql = String::from(
            "SELECT COUNT(*) FROM raw_shopee_order_items WHERE day_date = ?",
        );
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(date.to_string())];
        let target = ["camp1".to_string(), "".into(), "".into(), "".into(), "".into()];
        append_subid_prefilter(&mut sql, &mut params, &target, SubIdMatchMode::Substring);
        let refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|b| b.as_ref()).collect();
        let count: i64 = conn
            .query_row(&sql, refs.as_slice(), |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2, "Substring mode phải giữ cả 2 row cho Rust check");
    }

    /// Verify CTE refactor: active_rows = SUM(mapping count). Compare giá trị
    /// trả về với compute manual từng table riêng.
    #[test]
    fn list_imported_files_active_rows_matches_legacy_count() {
        let conn = seed_conn();
        let date = "2026-04-20";
        seed_day(&conn, date);

        // File 1: orders mapping (2 row).
        let file_orders = seed_imported_file(&conn, "shopee_commission", date);
        conn.execute(
            "INSERT INTO orders_to_file(order_item_id, file_id) VALUES(1, ?)",
            params![file_orders],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO orders_to_file(order_item_id, file_id) VALUES(2, ?)",
            params![file_orders],
        )
        .unwrap();

        // File 2: clicks mapping (3 row).
        let file_clicks = seed_imported_file(&conn, "shopee_clicks", date);
        for cid in ["c1", "c2", "c3"] {
            conn.execute(
                "INSERT INTO clicks_to_file(click_id, file_id) VALUES(?, ?)",
                params![cid, file_clicks],
            )
            .unwrap();
        }

        // File 3: fb_ads mapping (1 row) + fb_hier (2 row) → tổng 3.
        let file_fb = seed_imported_file(&conn, "fb_ad_group", date);
        conn.execute(
            "INSERT INTO fb_ads_to_file(fb_ad_id, file_id) VALUES(10, ?)",
            params![file_fb],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO fb_ads_hier_to_file(fb_ad_id, file_id) VALUES(20, ?)",
            params![file_fb],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO fb_ads_hier_to_file(fb_ad_id, file_id) VALUES(21, ?)",
            params![file_fb],
        )
        .unwrap();

        // File 4: không có mapping → active_rows = 0.
        let file_empty = seed_imported_file(&conn, "shopee_clicks", date);

        // Test trực tiếp CTE SQL (Tauri State API không thuận để construct
        // trong unit test → query DB-level y hệt SQL trong list_imported_files).
        let mut stmt = conn
            .prepare(
                "WITH mapping_counts AS (
                     SELECT file_id, COUNT(*) AS cnt FROM (
                         SELECT file_id FROM clicks_to_file
                         UNION ALL SELECT file_id FROM orders_to_file
                         UNION ALL SELECT file_id FROM fb_ads_to_file
                         UNION ALL SELECT file_id FROM fb_ads_hier_to_file
                     ) GROUP BY file_id
                 )
                 SELECT f.id, COALESCE(mc.cnt, 0) AS active_rows
                 FROM imported_files f
                 LEFT JOIN mapping_counts mc ON mc.file_id = f.id
                 ORDER BY f.id",
            )
            .unwrap();
        let actual: Vec<(i64, i64)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|x| x.unwrap())
            .collect();
        let map: std::collections::HashMap<i64, i64> = actual.into_iter().collect();
        assert_eq!(map[&file_orders], 2);
        assert_eq!(map[&file_clicks], 3);
        assert_eq!(map[&file_fb], 3); // 1 fb_ads + 2 fb_hier
        assert_eq!(map[&file_empty], 0);
    }

    // ========================================================================
    // E2E với data thực từ docs/ — chạy manual: `cargo test --lib -- --ignored`.
    //
    // Fixture build bởi `node scripts/make_fixtures.mjs` (chạy trước). Test này:
    //  1. Load `fixtures/payloads.json` → INSERT vào in-memory DB dùng SQL y hệt
    //     production (imports.rs).
    //  2. Query raw aggregates từ DB → so với `fixtures/csv_totals.json` (ground
    //     truth tính trực tiếp trên CSV).
    //  3. Gọi `list_days_with_rows_impl` → in báo cáo per-day cho spot check.
    //
    // Report discrepancies (không assert-abort) để thấy toàn bộ sai lệch 1 lần.
    // ========================================================================

    #[derive(serde::Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    struct FixtureGroup {
        kind: String,
        day_date: String,
        filename: String,
        rows: serde_json::Value,
    }

    /// BigInt từ Node → JSON string → parse i64. Tránh float round-trip.
    fn parse_cents(s: &str) -> i64 {
        s.parse::<i64>().unwrap_or(0)
    }

    #[derive(serde::Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    struct CsvTotal {
        kind: String,
        day_date: String,
        row_count: i64,
        // Shopee orders
        #[serde(default)]
        distinct_order_id_count: i64,
        #[serde(default)]
        distinct_checkout_item_count: i64,
        // Integer cents (VND × 100) làm ground truth bit-exact.
        #[serde(default)]
        sum_net_commission_cents: String,
        #[serde(default)]
        sum_order_value_cents: String,
        // Shopee clicks
        #[serde(default)]
        sum_clicks: i64,
        #[serde(default)]
        by_referrer: std::collections::HashMap<String, i64>,
        // FB ads
        #[serde(default)]
        sum_spend_cents: String,
        #[serde(default)]
        sum_impressions: i64,
        #[serde(default)]
        sum_link_clicks: i64,
        #[serde(default)]
        sum_all_clicks: i64,
    }

    /// Match Node `Math.round(v * 100)` bit-exact. NULL/0 → 0.
    fn f64_to_cents(v: Option<f64>) -> i64 {
        v.map(|x| (x * 100.0).round() as i64).unwrap_or(0)
    }

    fn insert_source_file(
        conn: &Connection,
        filename: &str,
        kind: &str,
        day_date: &str,
    ) -> i64 {
        conn.execute(
            "INSERT OR IGNORE INTO days(date, created_at) VALUES(?, ?)",
            params![day_date, format!("{day_date}T00:00:00Z")],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES(?, ?, datetime('now'), ?, ?)",
            params![filename, kind, format!("hash-{filename}"), day_date],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_shopee_clicks(
        conn: &Connection,
        payload: &FixtureGroup,
        source_file_id: i64,
    ) {
        let rows = payload.rows.as_array().unwrap();
        let mut stmt = conn
            .prepare(
                "INSERT OR IGNORE INTO raw_shopee_clicks
                 (click_id, click_time, region, sub_id_raw,
                  sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                  referrer, day_date, source_file_id)
                 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .unwrap();
        for r in rows {
            let sub_ids = r["subIds"].as_array().unwrap();
            stmt.execute(params![
                r["clickId"].as_str().unwrap_or(""),
                r["clickTime"].as_str().unwrap_or(""),
                r["region"].as_str(),
                r["subIdRaw"].as_str(),
                sub_ids[0].as_str().unwrap_or(""),
                sub_ids[1].as_str().unwrap_or(""),
                sub_ids[2].as_str().unwrap_or(""),
                sub_ids[3].as_str().unwrap_or(""),
                sub_ids[4].as_str().unwrap_or(""),
                r["referrer"].as_str(),
                payload.day_date,
                source_file_id,
            ])
            .unwrap();
        }
    }

    fn insert_shopee_orders(
        conn: &Connection,
        payload: &FixtureGroup,
        source_file_id: i64,
    ) {
        let rows = payload.rows.as_array().unwrap();
        let mut stmt = conn
            .prepare(
                "INSERT INTO raw_shopee_order_items
                 (order_id, checkout_id, item_id, model_id, order_status,
                  order_time, completed_time, click_time,
                  shop_id, shop_name, shop_type, item_name,
                  category_l1, category_l2, category_l3,
                  price, quantity, order_value, refund_amount,
                  net_commission, commission_total,
                  sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                  channel, day_date, source_file_id)
                 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(checkout_id, item_id, model_id) DO UPDATE SET
                    order_status = excluded.order_status,
                    order_time = excluded.order_time,
                    net_commission = excluded.net_commission,
                    commission_total = excluded.commission_total,
                    order_value = excluded.order_value",
            )
            .unwrap();
        for r in rows {
            let sub_ids = r["subIds"].as_array().unwrap();
            stmt.execute(params![
                r["orderId"].as_str().unwrap_or(""),
                r["checkoutId"].as_str().unwrap_or(""),
                r["itemId"].as_str().unwrap_or(""),
                r["modelId"].as_str().unwrap_or(""),
                r["orderStatus"].as_str(),
                r["orderTime"].as_str().unwrap_or(""),
                r["completedTime"].as_str(),
                r["clickTime"].as_str(),
                r["shopId"].as_str(),
                r["shopName"].as_str(),
                r["shopType"].as_str(),
                r["itemName"].as_str(),
                r["categoryL1"].as_str(),
                r["categoryL2"].as_str(),
                r["categoryL3"].as_str(),
                r["price"].as_f64(),
                r["quantity"].as_i64(),
                r["orderValue"].as_f64(),
                r["refundAmount"].as_f64(),
                r["netCommission"].as_f64(),
                r["commissionTotal"].as_f64(),
                sub_ids[0].as_str().unwrap_or(""),
                sub_ids[1].as_str().unwrap_or(""),
                sub_ids[2].as_str().unwrap_or(""),
                sub_ids[3].as_str().unwrap_or(""),
                sub_ids[4].as_str().unwrap_or(""),
                r["channel"].as_str(),
                payload.day_date,
                source_file_id,
            ])
            .unwrap();
        }
    }

    fn normalize_clicks_json(r: &serde_json::Value) -> Option<i64> {
        r["linkClicks"]
            .as_i64()
            .or_else(|| r["allClicks"].as_i64())
            .or_else(|| r["resultCount"].as_i64())
    }

    fn normalize_cpc_json(r: &serde_json::Value) -> Option<f64> {
        r["linkCpc"]
            .as_f64()
            .or_else(|| r["allCpc"].as_f64())
            .or_else(|| r["costPerResult"].as_f64())
    }

    fn insert_fb(
        conn: &Connection,
        payload: &FixtureGroup,
        source_file_id: i64,
        level: &str,
    ) {
        let rows = payload.rows.as_array().unwrap();
        let mut stmt = conn
            .prepare(
                "INSERT INTO raw_fb_ads
                 (level, name, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                  report_start, report_end, status,
                  spend, clicks, cpc, impressions, reach,
                  day_date, source_file_id)
                 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(day_date, level, name) DO UPDATE SET
                    spend = excluded.spend, clicks = excluded.clicks,
                    cpc = excluded.cpc, impressions = excluded.impressions,
                    reach = excluded.reach, source_file_id = excluded.source_file_id",
            )
            .unwrap();
        for r in rows {
            let name = if level == "ad_group" {
                r["adGroupName"].as_str().unwrap_or("")
            } else {
                r["campaignName"].as_str().unwrap_or("")
            };
            let sub_ids = r["subIds"].as_array().unwrap();
            let clicks = normalize_clicks_json(r);
            let cpc = normalize_cpc_json(r);
            stmt.execute(params![
                level,
                name,
                sub_ids[0].as_str().unwrap_or(""),
                sub_ids[1].as_str().unwrap_or(""),
                sub_ids[2].as_str().unwrap_or(""),
                sub_ids[3].as_str().unwrap_or(""),
                sub_ids[4].as_str().unwrap_or(""),
                r["reportStart"].as_str().unwrap_or(""),
                r["reportEnd"].as_str().unwrap_or(""),
                r["status"].as_str(),
                r["spend"].as_f64(),
                clicks,
                cpc,
                r["impressions"].as_i64(),
                r["reach"].as_i64(),
                payload.day_date,
                source_file_id,
            ])
            .unwrap();
        }
    }

    #[test]
    #[ignore]
    fn e2e_real_data_from_docs() {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let fixtures_dir = std::path::Path::new(&manifest_dir)
            .parent()
            .unwrap()
            .join("fixtures");

        let payloads: Vec<FixtureGroup> = serde_json::from_str(
            &std::fs::read_to_string(fixtures_dir.join("payloads.json"))
                .expect("run `node scripts/make_fixtures.mjs` trước"),
        )
        .unwrap();
        let csv_totals: Vec<CsvTotal> = serde_json::from_str(
            &std::fs::read_to_string(fixtures_dir.join("csv_totals.json")).unwrap(),
        )
        .unwrap();

        let conn = seed_conn();
        let mut discrepancies: Vec<String> = Vec::new();

        // ---- INSERT all payloads ----
        for payload in &payloads {
            let source_file_id = insert_source_file(
                &conn,
                &payload.filename,
                &payload.kind,
                &payload.day_date,
            );
            match payload.kind.as_str() {
                "shopee_clicks" => insert_shopee_clicks(&conn, payload, source_file_id),
                "shopee_commission" => insert_shopee_orders(&conn, payload, source_file_id),
                "fb_ad_group" => insert_fb(&conn, payload, source_file_id, "ad_group"),
                "fb_campaign" => insert_fb(&conn, payload, source_file_id, "campaign"),
                other => panic!("unknown kind: {other}"),
            }
        }

        // ---- So sánh raw totals DB vs CSV ground truth (integer cents) ----
        println!("\n========== RAW TABLE TOTALS vs CSV (cents) =========");
        for t in &csv_totals {
            match t.kind.as_str() {
                "shopee_clicks" => {
                    let (db_count, db_distinct): (i64, i64) = conn
                        .query_row(
                            "SELECT COUNT(*), COUNT(DISTINCT click_id)
                             FROM raw_shopee_clicks WHERE day_date = ?",
                            params![t.day_date],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )
                        .unwrap();
                    println!(
                        "[shopee_clicks {}] csv_rows={} db_rows={} db_distinct={} sum_clicks_csv={}",
                        t.day_date, t.row_count, db_count, db_distinct, t.sum_clicks
                    );
                    if db_distinct != t.sum_clicks {
                        discrepancies.push(format!(
                            "shopee_clicks {}: db_distinct_click_id={} != csv_clicks={}",
                            t.day_date, db_distinct, t.sum_clicks
                        ));
                    }
                }
                "shopee_commission" => {
                    let (db_count, db_distinct_orders): (i64, i64) = conn
                        .query_row(
                            "SELECT COUNT(*), COUNT(DISTINCT order_id)
                             FROM raw_shopee_order_items WHERE day_date = ?",
                            params![t.day_date],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )
                        .unwrap();
                    // Sum cents row-by-row với cùng phép làm tròn như Node.
                    // Không dùng SQL SUM (f64 non-associative → drift).
                    let mut db_commission_cents: i64 = 0;
                    let mut db_order_value_cents: i64 = 0;
                    {
                        let mut stmt = conn
                            .prepare(
                                "SELECT net_commission, order_value
                                 FROM raw_shopee_order_items WHERE day_date = ?",
                            )
                            .unwrap();
                        let iter = stmt
                            .query_map(params![t.day_date], |r| {
                                Ok((r.get::<_, Option<f64>>(0)?, r.get::<_, Option<f64>>(1)?))
                            })
                            .unwrap();
                        for row in iter {
                            let (c, ov) = row.unwrap();
                            db_commission_cents += f64_to_cents(c);
                            db_order_value_cents += f64_to_cents(ov);
                        }
                    }
                    let csv_commission_cents = parse_cents(&t.sum_net_commission_cents);
                    let csv_order_value_cents = parse_cents(&t.sum_order_value_cents);
                    println!(
                        "[shopee_commission {}] csv_rows={} db_rows={} csv_orders={} db_orders={} csv_commission_cents={} db_commission_cents={} csv_order_value_cents={} db_order_value_cents={}",
                        t.day_date,
                        t.row_count,
                        db_count,
                        t.distinct_order_id_count,
                        db_distinct_orders,
                        csv_commission_cents,
                        db_commission_cents,
                        csv_order_value_cents,
                        db_order_value_cents,
                    );
                    if db_count != t.distinct_checkout_item_count {
                        discrepancies.push(format!(
                            "shopee_commission {}: db_rows={} != csv_distinct(checkout,item,model)={}",
                            t.day_date, db_count, t.distinct_checkout_item_count
                        ));
                    }
                    if db_distinct_orders != t.distinct_order_id_count {
                        discrepancies.push(format!(
                            "shopee_commission {}: db_distinct_orders={} != csv_distinct_orders={}",
                            t.day_date, db_distinct_orders, t.distinct_order_id_count
                        ));
                    }
                    if db_commission_cents != csv_commission_cents {
                        discrepancies.push(format!(
                            "shopee_commission {}: db_commission_cents={} != csv={} (diff={})",
                            t.day_date,
                            db_commission_cents,
                            csv_commission_cents,
                            db_commission_cents - csv_commission_cents,
                        ));
                    }
                    if db_order_value_cents != csv_order_value_cents {
                        discrepancies.push(format!(
                            "shopee_commission {}: db_order_value_cents={} != csv={} (diff={})",
                            t.day_date,
                            db_order_value_cents,
                            csv_order_value_cents,
                            db_order_value_cents - csv_order_value_cents,
                        ));
                    }
                }
                "fb_ad_group" | "fb_campaign" => {
                    let level = if t.kind == "fb_ad_group" {
                        "ad_group"
                    } else {
                        "campaign"
                    };
                    let (db_count, db_sum_imps): (i64, i64) = conn
                        .query_row(
                            "SELECT COUNT(*), COALESCE(SUM(impressions), 0)
                             FROM raw_fb_ads WHERE day_date = ? AND level = ?",
                            params![t.day_date, level],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )
                        .unwrap();
                    // Sum spend cents row-by-row.
                    let mut db_spend_cents: i64 = 0;
                    {
                        let mut stmt = conn
                            .prepare(
                                "SELECT spend FROM raw_fb_ads WHERE day_date = ? AND level = ?",
                            )
                            .unwrap();
                        let iter = stmt
                            .query_map(params![t.day_date, level], |r| {
                                r.get::<_, Option<f64>>(0)
                            })
                            .unwrap();
                        for row in iter {
                            db_spend_cents += f64_to_cents(row.unwrap());
                        }
                    }
                    let csv_spend_cents = parse_cents(&t.sum_spend_cents);
                    println!(
                        "[{} {}] csv_rows={} db_rows={} csv_spend_cents={} db_spend_cents={} csv_imps={} db_imps={}",
                        t.kind,
                        t.day_date,
                        t.row_count,
                        db_count,
                        csv_spend_cents,
                        db_spend_cents,
                        t.sum_impressions,
                        db_sum_imps,
                    );
                    if db_count != t.row_count {
                        discrepancies.push(format!(
                            "{} {}: db_rows={} != csv_rows={}",
                            t.kind, t.day_date, db_count, t.row_count
                        ));
                    }
                    if db_spend_cents != csv_spend_cents {
                        discrepancies.push(format!(
                            "{} {}: db_spend_cents={} != csv={} (diff={})",
                            t.kind,
                            t.day_date,
                            db_spend_cents,
                            csv_spend_cents,
                            db_spend_cents - csv_spend_cents,
                        ));
                    }
                    if db_sum_imps != t.sum_impressions {
                        discrepancies.push(format!(
                            "{} {}: db_sum_impressions={} != csv={}",
                            t.kind, t.day_date, db_sum_imps, t.sum_impressions
                        ));
                    }
                }
                _ => {}
            }
        }

        // ---- Aggregate via production `list_days_with_rows_impl` ----
        println!("\n========== AGGREGATE OUTPUT (spot-check) =========");
        let days = list_days_with_rows_impl(&conn, DaysFilter::default()).unwrap();
        for day in &days {
            let total_spend: f64 = day
                .rows
                .iter()
                .map(|r| r.total_spend.unwrap_or(0.0))
                .sum();
            let total_commission: f64 =
                day.rows.iter().map(|r| r.commission_total).sum();
            let total_shopee_clicks: i64 =
                day.rows.iter().map(|r| r.shopee_clicks_total).sum();
            let total_orders: i64 = day.rows.iter().map(|r| r.orders_count).sum();
            let total_ads_clicks: i64 = day
                .rows
                .iter()
                .map(|r| r.ads_clicks.unwrap_or(0))
                .sum();
            println!(
                "[{}] rows={}  total_spend={:.2}  ads_clicks={}  shopee_clicks={}  orders={}  commission={:.2}",
                day.date,
                day.rows.len(),
                total_spend,
                total_ads_clicks,
                total_shopee_clicks,
                total_orders,
                total_commission,
            );
        }

        // ---- Aggregate-level: compare với CSV totals (integer cents) ----
        println!("\n========== AGGREGATE vs CSV TOTAL (cents) =========");
        for day in &days {
            // Sum cents row-by-row từ aggregate output.
            let agg_commission_cents: i64 = day
                .rows
                .iter()
                .map(|r| f64_to_cents(Some(r.commission_total)))
                .sum();
            let agg_total_shopee_clicks: i64 =
                day.rows.iter().map(|r| r.shopee_clicks_total).sum();
            let agg_total_orders: i64 = day.rows.iter().map(|r| r.orders_count).sum();

            let csv_commission_cents = csv_totals
                .iter()
                .find(|t| t.kind == "shopee_commission" && t.day_date == day.date)
                .map(|t| parse_cents(&t.sum_net_commission_cents))
                .unwrap_or(0);
            let csv_orders_count = csv_totals
                .iter()
                .find(|t| t.kind == "shopee_commission" && t.day_date == day.date)
                .map(|t| t.distinct_order_id_count)
                .unwrap_or(0);
            let csv_clicks = csv_totals
                .iter()
                .find(|t| t.kind == "shopee_clicks" && t.day_date == day.date)
                .map(|t| t.sum_clicks)
                .unwrap_or(0);

            println!(
                "[{}] commission_cents: agg={} csv={} | shopee_clicks: agg={} csv={} | orders: agg={} csv={}",
                day.date,
                agg_commission_cents,
                csv_commission_cents,
                agg_total_shopee_clicks,
                csv_clicks,
                agg_total_orders,
                csv_orders_count,
            );

            if agg_commission_cents != csv_commission_cents {
                discrepancies.push(format!(
                    "AGG {} commission_cents: agg={} != csv={} (diff={})",
                    day.date,
                    agg_commission_cents,
                    csv_commission_cents,
                    agg_commission_cents - csv_commission_cents,
                ));
            }
            // shopee_clicks: row-0 filter ("chỉ giữ row có spend ≠ 0 HOẶC
            // commission ≠ 0") drop clicks trên tuple không monetize. Chỉ flag
            // info, không phải bug — đối chiếu raw_shopee_clicks SQL cho exact.
            if agg_total_shopee_clicks != csv_clicks {
                println!(
                    "    ℹ AGG {} shopee_clicks: agg={} < csv={} (diff={} clicks trên tuple không có spend/commission — row-0 filter)",
                    day.date,
                    agg_total_shopee_clicks,
                    csv_clicks,
                    csv_clicks - agg_total_shopee_clicks,
                );
            }
            if agg_total_orders != csv_orders_count {
                discrepancies.push(format!(
                    "AGG {} orders: agg={} != csv_distinct_orders={}",
                    day.date, agg_total_orders, csv_orders_count
                ));
            }
        }

        // ---- Overview-equivalent totals (cross-day aggregate) ----
        // Simulate Overview tab formula: Σ across all rows, source='all',
        // default profitFees (tax 10.98% + return 9%, netRatio = 0.8002).
        // Assert against baseline để đảm bảo khi user mở Overview tab + chọn
        // "Từ trước đến nay", UI phải hiện đúng những giá trị này.
        let net_ratio = 1.0 - 0.1098 - 0.09; // = 0.8002
        let ov_spend: f64 = days.iter().flat_map(|d| d.rows.iter())
            .map(|r| r.total_spend.unwrap_or(0.0)).sum();
        let ov_ads_clicks: i64 = days.iter().flat_map(|d| d.rows.iter())
            .map(|r| r.ads_clicks.unwrap_or(0)).sum();
        let ov_shopee_clicks: i64 = days.iter().flat_map(|d| d.rows.iter())
            .map(|r| r.shopee_clicks_total).sum();
        let ov_orders: i64 = days.iter().flat_map(|d| d.rows.iter())
            .map(|r| r.orders_count).sum();
        let ov_commission: f64 = days.iter().flat_map(|d| d.rows.iter())
            .map(|r| r.commission_total).sum();
        let ov_order_value: f64 = days.iter().flat_map(|d| d.rows.iter())
            .map(|r| r.order_value_total).sum();
        let ov_rows: usize = days.iter().map(|d| d.rows.len()).sum();
        let ov_net_commission = ov_commission * net_ratio;
        let ov_profit = ov_net_commission - ov_spend;
        let ov_roi = if ov_spend > 0.0 { ov_profit / ov_spend * 100.0 } else { 0.0 };

        println!("\n========== OVERVIEW TAB EXPECTED (source=all, netRatio=0.8002) =========");
        println!("  rows:            {ov_rows}");
        println!("  ads_clicks:      {ov_ads_clicks}");
        println!("  shopee_clicks:   {ov_shopee_clicks}");
        println!("  spend:           {ov_spend:.2} đ");
        println!("  orders:          {ov_orders}");
        println!("  commission:      {ov_commission:.2} đ");
        println!("  net_commission:  {ov_net_commission:.2} đ");
        println!("  order_value:     {ov_order_value:.2} đ");
        println!("  profit:          {ov_profit:.2} đ");
        println!("  ROI:             {ov_roi:.2}%");

        // Baseline từ 3-day fixture (2026-04-16/17/18). KHỚP CHÍNH XÁC với
        // Rust output trên data thật. Nếu fixtures bị thay → update baseline.
        let assert_eq_cents = |name: &str, actual: f64, expected_cents: i64| {
            let actual_cents = (actual * 100.0).round() as i64;
            assert_eq!(
                actual_cents, expected_cents,
                "Overview {name}: actual={} cents vs expected={} cents",
                actual_cents, expected_cents
            );
        };
        assert_eq!(ov_rows, 80, "Overview rows count");
        assert_eq!(ov_ads_clicks, 9300, "Overview ads_clicks");
        assert_eq!(ov_shopee_clicks, 8279, "Overview shopee_clicks (post row-0 filter)");
        assert_eq!(ov_orders, 640, "Overview orders");
        assert_eq_cents("spend", ov_spend, 581_541_200);
        assert_eq_cents("commission_gross", ov_commission, 1_175_345_557);
        // Profit = net_commission - spend = 11,753,455.57 × 0.8002 - 5,815,412.
        // = 9,405,115.1465... - 5,815,412 = 3,589,703.1465. Tolerance ±1 cent.
        let expected_profit_cents = 358_970_315i64;
        let actual_profit_cents = (ov_profit * 100.0).round() as i64;
        assert!(
            (actual_profit_cents - expected_profit_cents).abs() <= 1,
            "Overview profit: actual={} cents vs expected={} cents (tolerance ±1)",
            actual_profit_cents, expected_profit_cents
        );

        // ---- Final report ----
        println!("\n========== DISCREPANCIES ({}) =========", discrepancies.len());
        for d in &discrepancies {
            println!("  ✗ {d}");
        }
        if discrepancies.is_empty() {
            println!("  ✓ All checked fields match.");
        }
        assert!(
            discrepancies.is_empty(),
            "{} field(s) không khớp — xem log phía trên",
            discrepancies.len()
        );
    }
}
