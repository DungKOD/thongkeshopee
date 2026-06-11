//! Days listing + per-day aggregation core.
//!
//! `list_days_with_rows` là entry-point chính cho UI stats tab. Batch path
//! (≤900 ngày, default) gọi 5-8 SQL queries 1 lần rồi distribute trong Rust;
//! fallback per-day khi range vượt SQLite variable limit.
//!
//! `process_day_data` (~330 dòng) merge Shopee orders + clicks + FB ads
//! (legacy + hierarchy) + manual entries thành Vec<UiRow> theo prefix-matching
//! với anchor logic ưu tiên Shopee commission.

use std::collections::{HashMap, HashSet};

#[cfg(not(test))]
use rusqlite::Connection;
#[cfg(test)]
use rusqlite::{params, Connection};
use tauri::State;

use crate::db::types::{FbAdLeaf, FbAdSetGroup, FbBreakdown, FbCampaignGroup, UiDay, UiRow};
use crate::db::DbState;

use super::super::{CmdError, CmdResult};
use super::aggregate::{
    canonical_to_array, default_name, read_sub_id_match_mode, representative, to_canonical,
    Canonical, SubIdMatchMode,
};
use super::{default_account_id_lookup, AccountFilterMode, DaysFilter};

#[tauri::command]
pub fn list_days_with_rows(
    state: State<'_, DbState>,
    filter: Option<DaysFilter>,
) -> CmdResult<Vec<UiDay>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    list_days_with_rows_impl(&conn, filter.unwrap_or_default())
}

/// Tách impl khỏi command để test truy cập trực tiếp với `Connection`.
pub(super) fn list_days_with_rows_impl(
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
pub(super) fn display_name_subset_match(display_name: &str, selected_parts: &[String]) -> bool {
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


