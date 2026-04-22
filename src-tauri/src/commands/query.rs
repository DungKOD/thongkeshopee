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

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tauri::State;

use crate::db::types::{UiDay, UiRow};
use crate::db::DbState;

use super::{CmdError, CmdResult};

/// Smoke test.
#[tauri::command]
pub fn db_ping(state: State<'_, DbState>) -> CmdResult<i64> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let version: i64 =
        conn.query_row("SELECT MAX(version) FROM _schema_version", [], |r| r.get(0))?;
    Ok(version)
}

#[tauri::command]
pub fn list_days(state: State<'_, DbState>) -> CmdResult<Vec<String>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare("SELECT date FROM days ORDER BY date DESC")?;
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
    let mut sql = String::from("SELECT date, notes FROM days");
    let mut where_clauses: Vec<&str> = Vec::new();
    if filter.from_date.is_some() {
        where_clauses.push("date >= ?");
    }
    if filter.to_date.is_some() {
        where_clauses.push("date <= ?");
    }
    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
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

    let mut stmt = conn.prepare(&sql)?;
    let params_refs: Vec<&dyn rusqlite::ToSql> =
        params_vec.iter().map(|b| b.as_ref() as &dyn rusqlite::ToSql).collect();
    let days: Vec<(String, Option<String>)> = stmt
        .query_map(params_refs.as_slice(), |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })?
        .collect::<Result<_, _>>()?;

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

    let mut out = Vec::with_capacity(days.len());
    for (date, notes) in days {
        let (mut rows, totals) = aggregate_rows_for_day(conn, &date)?;
        if !selected_parts.is_empty() {
            rows.retain(|r| display_name_subset_match(&r.display_name, &selected_parts));
            if rows.is_empty() {
                continue;
            }
        }
        out.push(UiDay { date, notes, rows, totals });
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

/// Canonical tuple = non-empty slots có ý nghĩa. Trailing empty bỏ đi.
pub(crate) type Canonical = Vec<String>;

pub(crate) fn to_canonical(s: [String; 5]) -> Canonical {
    let mut v: Vec<String> = s.into();
    while let Some(last) = v.last() {
        if last.is_empty() {
            v.pop();
        } else {
            break;
        }
    }
    v
}

/// `a` là prefix của `b` (bao gồm trường hợp a == b).
pub(crate) fn is_prefix(a: &Canonical, b: &Canonical) -> bool {
    a.len() <= b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y)
}

/// Chọn "đại diện" cho 1 canonical: ưu tiên **anchor** (canonical từ Shopee
/// order = hoa hồng sản phẩm) mà prefix-compatible. Nếu không có anchor compatible,
/// fallback về chính nó (giữ tuple gốc, VD FB campaign standalone).
/// Tie-break giữa nhiều anchor: chọn dài nhất (match cụ thể nhất), rồi lex order.
fn representative(c: &Canonical, anchors: &[Canonical]) -> Canonical {
    let mut best: Option<&Canonical> = None;
    for a in anchors {
        let compatible = is_prefix(c, a) || is_prefix(a, c);
        if !compatible {
            continue;
        }
        match best {
            None => best = Some(a),
            Some(b) => {
                if a.len() > b.len() || (a.len() == b.len() && a < b) {
                    best = Some(a);
                }
            }
        }
    }
    best.cloned().unwrap_or_else(|| c.clone())
}

fn canonical_to_array(c: &Canonical) -> [String; 5] {
    std::array::from_fn(|i| c.get(i).cloned().unwrap_or_default())
}

fn default_name(c: &Canonical) -> String {
    if c.is_empty() {
        "(chưa đặt tên)".to_string()
    } else {
        c.join("-")
    }
}

fn aggregate_rows_for_day(
    conn: &Connection,
    day_date: &str,
) -> CmdResult<(Vec<UiRow>, crate::db::types::UiDayTotals)> {
    // ============================================================
    // Phase 1: load raw data từ 5 nguồn, giữ canonical tuple.
    // ============================================================

    // FB ads — unified. Dedup per sub_id_tuple: nếu tuple có row level='ad_group'
    // thì CHỈ dùng ad_group (bỏ campaign); nếu không có → dùng campaign.
    // Logic: `preferred_rank = MIN(0 if ad_group else 1) OVER PARTITION BY tuple`,
    // rồi filter row cùng rank.
    struct FbAds {
        canonical: Canonical,
        /// Integer cents (spend × 100). 0 = không có spend (NULL raw → SQL COALESCE 0).
        spend_cents: i64,
        imps: Option<i64>,
        clicks: Option<i64>,
        weighted_cpc_sum: Option<f64>,
    }
    let mut fb_ads: Vec<FbAds> = Vec::new();
    {
        // Spend SUM via integer cents (cùng lý do Shopee commission): tránh
        // float drift nếu data có fractional VND. `weighted_cpc_sum` giữ f64
        // (dùng làm numerator trong division → drift không tích lũy qua nhiều
        // row — chỉ được chia 1 lần cuối).
        let mut stmt = conn.prepare(
            "WITH ranked AS (
                SELECT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                       level, spend, clicks, cpc, impressions,
                       MIN(CASE level WHEN 'ad_group' THEN 0 ELSE 1 END)
                         OVER (PARTITION BY sub_id1, sub_id2, sub_id3, sub_id4, sub_id5)
                         AS preferred_rank
                FROM raw_fb_ads
                WHERE day_date = ?
             )
             SELECT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                    COALESCE(SUM(CAST(ROUND(spend * 100) AS INTEGER)), 0),
                    SUM(impressions),
                    SUM(clicks),
                    SUM(CASE WHEN clicks IS NOT NULL AND cpc IS NOT NULL
                             THEN clicks * cpc ELSE 0 END)
             FROM ranked
             WHERE (CASE level WHEN 'ad_group' THEN 0 ELSE 1 END) = preferred_rank
             GROUP BY sub_id1, sub_id2, sub_id3, sub_id4, sub_id5",
        )?;
        let iter = stmt.query_map(params![day_date], |r| {
            let tuple: [String; 5] =
                [r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?];
            let spend_cents: i64 = r.get(5)?;
            Ok(FbAds {
                canonical: to_canonical(tuple),
                spend_cents,
                imps: r.get(6)?,
                clicks: r.get(7)?,
                weighted_cpc_sum: r.get(8)?,
            })
        })?;
        for row in iter {
            fb_ads.push(row?);
        }
    }

    // Shopee clicks (grouped by tuple + referrer)
    struct ShopeeClick {
        canonical: Canonical,
        referrer: String,
        count: i64,
    }
    let mut shopee_clicks: Vec<ShopeeClick> = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                    COALESCE(referrer, '(khác)') AS ref, COUNT(*) AS cnt
             FROM raw_shopee_clicks
             WHERE day_date = ?
             GROUP BY sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, ref",
        )?;
        let iter = stmt.query_map(params![day_date], |r| {
            let tuple: [String; 5] =
                [r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?];
            Ok(ShopeeClick {
                canonical: to_canonical(tuple),
                referrer: r.get(5)?,
                count: r.get(6)?,
            })
        })?;
        for row in iter {
            shopee_clicks.push(row?);
        }
    }

    // Shopee orders — SUM via integer cents để tránh float drift.
    // IEEE 754 không associative: SUM(f64) trong SQLite có thể lệch vài cents
    // cho cùng dataset tùy thứ tự cộng. Cast × 100 → INTEGER trước khi SUM
    // đảm bảo bit-exact. Giữ cents ở i64 xuyên suốt accumulator, chỉ chia 100
    // thành f64 khi populate UiRow cuối cùng.
    struct ShopeeOrder {
        canonical: Canonical,
        orders: i64,
        commission_cents: i64,
        order_value_cents: i64,
    }
    let mut shopee_orders: Vec<ShopeeOrder> = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                    COUNT(DISTINCT order_id),
                    COALESCE(SUM(CAST(ROUND(net_commission * 100) AS INTEGER)), 0),
                    COALESCE(SUM(CAST(ROUND(order_value * 100) AS INTEGER)), 0)
             FROM raw_shopee_order_items
             WHERE day_date = ?
             GROUP BY sub_id1, sub_id2, sub_id3, sub_id4, sub_id5",
        )?;
        let iter = stmt.query_map(params![day_date], |r| {
            let tuple: [String; 5] =
                [r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?];
            Ok(ShopeeOrder {
                canonical: to_canonical(tuple),
                orders: r.get(5)?,
                commission_cents: r.get(6)?,
                order_value_cents: r.get(7)?,
            })
        })?;
        for row in iter {
            shopee_orders.push(row?);
        }
    }

    // Manual entries
    struct Manual {
        canonical: Canonical,
        display_name: Option<String>,
        clicks: Option<i64>,
        spend: Option<f64>,
        cpc: Option<f64>,
        orders: Option<i64>,
        commission: Option<f64>,
    }
    let mut manuals: Vec<Manual> = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                    display_name, override_clicks, override_spend, override_cpc,
                    override_orders, override_commission
             FROM manual_entries
             WHERE day_date = ?",
        )?;
        let iter = stmt.query_map(params![day_date], |r| {
            let tuple: [String; 5] =
                [r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?];
            Ok(Manual {
                canonical: to_canonical(tuple),
                display_name: r.get(5)?,
                clicks: r.get(6)?,
                spend: r.get(7)?,
                cpc: r.get(8)?,
                orders: r.get(9)?,
                commission: r.get(10)?,
            })
        })?;
        for row in iter {
            manuals.push(row?);
        }
    }

    // ============================================================
    // Phase 2: anchors = canonicals từ Shopee orders (hoa hồng).
    // FB/click/manual sẽ merge vào anchor prefix-compatible.
    // Không có anchor compatible → giữ canonical gốc (FB standalone dùng tên camp).
    // ============================================================
    let anchors: Vec<Canonical> = shopee_orders
        .iter()
        .map(|r| r.canonical.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let resolve = |c: &Canonical| representative(c, &anchors);

    // ============================================================
    // Phase 3: aggregate vào Accumulator theo representative.
    //
    // Monetary fields (spend, commission, order_value) accumulate ở **integer
    // cents (i64)** để tránh float drift khi nhiều tuple merge vào cùng anchor
    // (f64 `+=` non-associative). Convert cents → f64 khi populate UiRow cuối.
    // ============================================================
    struct Accumulator {
        display_name: String,
        ads_clicks: Option<i64>,
        // Cents (×100). `None` = chưa có FB/manual override nào đặt spend.
        spend_cents: Option<i64>,
        cpc: Option<f64>,
        impressions: Option<i64>,
        shopee_clicks_by_referrer: HashMap<String, i64>,
        shopee_clicks_total: i64,
        orders_count: i64,
        commission_cents: i64,
        order_value_cents: i64,
        has_fb: bool,
        has_shopee_clicks: bool,
        has_shopee_orders: bool,
        has_manual: bool,
    }

    let mut map: HashMap<Canonical, Accumulator> = HashMap::new();
    let make_empty = |c: &Canonical| Accumulator {
        display_name: default_name(c),
        ads_clicks: None,
        spend_cents: None,
        cpc: None,
        impressions: None,
        shopee_clicks_by_referrer: HashMap::new(),
        shopee_clicks_total: 0,
        orders_count: 0,
        commission_cents: 0,
        order_value_cents: 0,
        has_fb: false,
        has_shopee_clicks: false,
        has_shopee_orders: false,
        has_manual: false,
    };

    // FB ads (đã dedup ad_group ưu tiên trong SQL).
    for r in fb_ads {
        let rep = resolve(&r.canonical);
        let entry = map.entry(rep.clone()).or_insert_with(|| make_empty(&rep));
        entry.has_fb = true;
        entry.spend_cents = Some(entry.spend_cents.unwrap_or(0) + r.spend_cents);
        if r.imps.is_some() {
            entry.impressions = Some(entry.impressions.unwrap_or(0) + r.imps.unwrap_or(0));
        }
        if r.clicks.is_some() {
            entry.ads_clicks = Some(entry.ads_clicks.unwrap_or(0) + r.clicks.unwrap_or(0));
        }
        // CPC weighted: SUM(clicks * cpc) / SUM(clicks). Fallback spend/clicks
        // ở cuối function nếu weighted sum = 0 (raw không có CPC).
        if let (Some(wsum), Some(clicks)) = (r.weighted_cpc_sum, r.clicks) {
            if clicks > 0 && wsum > 0.0 {
                entry.cpc = Some(wsum / clicks as f64);
            }
        }
    }

    // Shopee clicks
    for r in shopee_clicks {
        let rep = resolve(&r.canonical);
        let entry = map.entry(rep.clone()).or_insert_with(|| make_empty(&rep));
        entry.has_shopee_clicks = true;
        entry.shopee_clicks_total += r.count;
        *entry
            .shopee_clicks_by_referrer
            .entry(r.referrer)
            .or_insert(0) += r.count;
    }

    // Shopee orders
    for r in shopee_orders {
        let rep = resolve(&r.canonical);
        let entry = map.entry(rep.clone()).or_insert_with(|| make_empty(&rep));
        entry.has_shopee_orders = true;
        entry.orders_count += r.orders;
        entry.commission_cents += r.commission_cents;
        entry.order_value_cents += r.order_value_cents;
    }

    // Manual entries — override field, display_name chỉ khi không có sub_id.
    for r in manuals {
        let rep = resolve(&r.canonical);
        let entry = map.entry(rep.clone()).or_insert_with(|| make_empty(&rep));
        entry.has_manual = true;
        // Display name rule: nếu canonical empty (no sub_id) + manual có name → dùng.
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
            // Manual override: replace, không accumulate (behavior cũ).
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
        }
    }

    // Convert Accumulator → UiRow + fallback CPC.
    // Accumulate day-level totals BEFORE row-0 filter — KPI phải đúng 100%
    // với raw data kể cả khi tuple chỉ có click (không spend/commission) bị
    // filter khỏi row display.
    let canonicals: Vec<(Canonical, Accumulator)> = map.into_iter().collect();
    let mut rows: Vec<UiRow> = Vec::with_capacity(canonicals.len());
    let mut day_totals = crate::db::types::UiDayTotals::default();
    for (c, acc) in canonicals {
        let total_spend = acc.spend_cents.map(|c| c as f64 / 100.0);
        let commission_total = acc.commission_cents as f64 / 100.0;
        let cpc = acc.cpc.or_else(|| {
            if let (Some(s_cents), Some(clicks)) = (acc.spend_cents, acc.ads_clicks) {
                if clicks > 0 {
                    Some((s_cents as f64 / 100.0) / clicks as f64)
                } else {
                    None
                }
            } else {
                None
            }
        });

        // Accumulate day totals từ MỌI tuple — pre-filter, không miss data.
        day_totals.ads_clicks += acc.ads_clicks.unwrap_or(0);
        day_totals.total_spend += total_spend.unwrap_or(0.0);
        day_totals.impressions += acc.impressions.unwrap_or(0);
        day_totals.shopee_clicks_total += acc.shopee_clicks_total;
        for (referrer, count) in &acc.shopee_clicks_by_referrer {
            *day_totals
                .shopee_clicks_by_referrer
                .entry(referrer.clone())
                .or_insert(0) += count;
        }
        day_totals.orders_count += acc.orders_count;
        day_totals.commission_total += commission_total;
        day_totals.order_value_total += acc.order_value_cents as f64 / 100.0;

        // Row-0 filter: chỉ giữ row có spend ≠ 0 HOẶC commission ≠ 0 cho UI
        // display (tránh clutter). Day totals đã tính ở trên → KPI vẫn đúng.
        let has_spend = acc.spend_cents.map(|v| v != 0).unwrap_or(false);
        let has_commission = acc.commission_cents != 0;
        if !has_spend && !has_commission {
            continue;
        }

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
            order_value_total: acc.order_value_cents as f64 / 100.0,
            has_fb: acc.has_fb,
            has_shopee_clicks: acc.has_shopee_clicks,
            has_shopee_orders: acc.has_shopee_orders,
            has_manual: acc.has_manual,
        });
    }

    rows.sort_by(|a, b| a.display_name.cmp(&b.display_name));
    Ok((rows, day_totals))
}

#[tauri::command]
pub fn list_imported_files(state: State<'_, DbState>) -> CmdResult<Vec<ImportedFileInfo>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare(
        "SELECT id, filename, kind, imported_at, row_count, day_date
         FROM imported_files ORDER BY imported_at DESC",
    )?;
    let rows: Vec<ImportedFileInfo> = stmt
        .query_map([], |r| {
            Ok(ImportedFileInfo {
                id: r.get(0)?,
                filename: r.get(1)?,
                kind: r.get(2)?,
                imported_at: r.get(3)?,
                row_count: r.get(4)?,
                day_date: r.get(5)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportedFileInfo {
    pub id: i64,
    pub filename: String,
    pub kind: String,
    pub imported_at: String,
    pub row_count: i64,
    pub day_date: String,
}

/// Snapshot toàn DB dùng cho FE autocomplete + summary. Gọi 1 lần khi app start
/// và sau mỗi mutation — KHÔNG gọi mỗi filter-change. FE dùng để:
/// - `allSubIds`: dropdown search sub_id (bao gồm prefix hierarchy + từng part
///   riêng) → user chọn được kể cả sub_id thuộc ngày không render trong slice.
/// - Counters: Settings dialog + pagination UI (`canLoadMore`).
/// - Date bounds: picker "Từ trước đến nay".
///
/// Cost: chạy full aggregation qua mọi day → O(tổng rows). Chấp nhận được vì
/// frequency thấp (mutation-only). `list_days_with_rows` giờ chạy nhanh nhờ
/// LIMIT.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OverviewPayload {
    pub all_sub_ids: Vec<String>,
    pub total_days_count: i64,
    pub total_rows_count: i64,
    pub oldest_date: Option<String>,
    pub newest_date: Option<String>,
}

#[tauri::command]
pub fn load_overview(state: State<'_, DbState>) -> CmdResult<OverviewPayload> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    load_overview_impl(&conn)
}

fn load_overview_impl(conn: &Connection) -> CmdResult<OverviewPayload> {
    // Dates: MIN/MAX + COUNT từ `days` table. Cheap single pass.
    let (total_days_count, oldest_date, newest_date): (i64, Option<String>, Option<String>) =
        conn.query_row(
            "SELECT COUNT(*), MIN(date), MAX(date) FROM days",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;

    // Full aggregation để thu display_names + đếm rows. Không filter.
    let all_days = list_days_with_rows_impl(conn, DaysFilter::default())?;

    let mut set: HashSet<String> = HashSet::new();
    let mut total_rows: i64 = 0;
    for day in &all_days {
        total_rows += day.rows.len() as i64;
        for row in &day.rows {
            let name = &row.display_name;
            if name.is_empty() {
                continue;
            }
            set.insert(name.clone());
            let parts: Vec<&str> = name.split('-').filter(|p| !p.is_empty()).collect();
            // Prefix hierarchy: "a-b-c" → thêm "a", "a-b".
            for i in 1..parts.len() {
                set.insert(parts[..i].join("-"));
            }
            // Từng part riêng (ngang — user có thể search "dammaxi" để match
            // cả "MuseStudio-dammaxi").
            for p in &parts {
                set.insert((*p).to_string());
            }
        }
    }

    let mut all_sub_ids: Vec<String> = set.into_iter().collect();
    all_sub_ids.sort();

    Ok(OverviewPayload {
        all_sub_ids,
        total_days_count,
        total_rows_count: total_rows,
        oldest_date,
        newest_date,
    })
}

/// Unique referrer values (cột "Người giới thiệu" trong WebsiteClickReport).
/// UI Settings dùng để hiển thị list checkbox cho user filter.
#[tauri::command]
pub fn list_click_referrers(state: State<'_, DbState>) -> CmdResult<Vec<String>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare(
        "SELECT DISTINCT COALESCE(referrer, '(khác)') FROM raw_shopee_clicks
         ORDER BY 1",
    )?;
    let rows: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// Chi tiết 1 item trong order (từ raw_shopee_order_items) để hiển thị drill-down.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderItemDetail {
    pub order_id: String,
    pub checkout_id: String,
    pub item_id: String,
    pub model_id: String,
    pub item_name: Option<String>,
    pub shop_name: Option<String>,
    pub order_status: Option<String>,
    pub order_time: Option<String>,
    pub click_time: Option<String>,
    pub completed_time: Option<String>,
    pub price: Option<f64>,
    pub quantity: Option<i64>,
    pub order_value: Option<f64>,
    pub net_commission: Option<f64>,
    pub commission_total: Option<f64>,
    pub channel: Option<String>,
    pub sub_ids: [String; 5],
}

/// Lấy chi tiết các item đã mua qua tuple sub_id + date — dùng cho drill-down.
/// Matching theo prefix-compatible (xem `representative` ở aggregate). Order có
/// canonical là prefix của `sub_ids` truyền vào (hoặc ngược lại) đều match.
#[tauri::command]
pub fn get_order_items_for_row(
    state: State<'_, DbState>,
    day_date: String,
    sub_ids: [String; 5],
) -> CmdResult<Vec<OrderItemDetail>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let target_canonical = to_canonical(sub_ids);

    let mut stmt = conn.prepare(
        "SELECT order_id, checkout_id, item_id, model_id, item_name,
                shop_name, order_status, order_time, click_time, completed_time,
                price, quantity, order_value, net_commission, commission_total,
                channel, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
         FROM raw_shopee_order_items
         WHERE day_date = ?
         ORDER BY order_time DESC",
    )?;

    let iter = stmt.query_map(params![day_date], |r| {
        let subs: [String; 5] = [
            r.get(16)?,
            r.get(17)?,
            r.get(18)?,
            r.get(19)?,
            r.get(20)?,
        ];
        Ok(OrderItemDetail {
            order_id: r.get(0)?,
            checkout_id: r.get(1)?,
            item_id: r.get(2)?,
            model_id: r.get(3)?,
            item_name: r.get(4)?,
            shop_name: r.get(5)?,
            order_status: r.get(6)?,
            order_time: r.get(7)?,
            click_time: r.get(8)?,
            completed_time: r.get(9)?,
            price: r.get(10)?,
            quantity: r.get(11)?,
            order_value: r.get(12)?,
            net_commission: r.get(13)?,
            commission_total: r.get(14)?,
            channel: r.get(15)?,
            sub_ids: subs,
        })
    })?;

    let mut out: Vec<OrderItemDetail> = Vec::new();
    for row in iter {
        let item = row?;
        let item_canonical = to_canonical(item.sub_ids.clone());
        // Prefix-compatible: 1 là prefix của cái còn lại.
        if is_prefix(&item_canonical, &target_canonical)
            || is_prefix(&target_canonical, &item_canonical)
        {
            out.push(item);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(representative(&fb_with_date, &anchors), order);
        // Order itself → anchor matches self → rep = self
        assert_eq!(representative(&order, &anchors), order);
        // FB [dammaxi] không có anchor compatible → rep = self (fallback tên camp)
        assert_eq!(representative(&fb_other, &anchors), fb_other);
    }

    #[test]
    fn fb_standalone_when_no_commission() {
        let fb: Canonical = vec!["abc".into(), "def".into(), "0412".into()];
        // Không có anchor (không có Shopee order nào cho day này)
        let anchors: Vec<Canonical> = vec![];
        assert_eq!(representative(&fb, &anchors), fb);
    }

    #[test]
    fn anchor_tiebreak_longest_most_specific() {
        // 2 anchor overlap: [A] và [A, B]
        let short: Canonical = vec!["A".into()];
        let long: Canonical = vec!["A".into(), "B".into()];
        let fb: Canonical = vec!["A".into(), "B".into(), "C".into()];

        let anchors = vec![short.clone(), long.clone()];
        // FB [A, B, C] compatible với cả 2 anchor → pick anchor dài nhất = [A, B]
        assert_eq!(representative(&fb, &anchors), long);
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

    const SCHEMA_SQL: &str = include_str!("../db/schema.sql");

    fn seed_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
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
                  channel, raw_json, day_date, source_file_id)
                 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                r["rawJson"].as_str(),
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
                  raw_json, day_date, source_file_id)
                 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                r["rawJson"].as_str(),
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
