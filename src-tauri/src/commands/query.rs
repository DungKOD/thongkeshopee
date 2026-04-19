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

#[tauri::command]
pub fn list_days_with_rows(state: State<'_, DbState>) -> CmdResult<Vec<UiDay>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt =
        conn.prepare("SELECT date, notes FROM days ORDER BY date DESC")?;
    let days: Vec<(String, Option<String>)> = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)))?
        .collect::<Result<_, _>>()?;

    let mut out = Vec::with_capacity(days.len());
    for (date, notes) in days {
        let rows = aggregate_rows_for_day(&conn, &date)?;
        out.push(UiDay { date, notes, rows });
    }
    Ok(out)
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

fn aggregate_rows_for_day(conn: &Connection, day_date: &str) -> CmdResult<Vec<UiRow>> {
    // ============================================================
    // Phase 1: load raw data từ 5 nguồn, giữ canonical tuple.
    // ============================================================

    // FB ads — unified. Dedup per sub_id_tuple: nếu tuple có row level='ad_group'
    // thì CHỈ dùng ad_group (bỏ campaign); nếu không có → dùng campaign.
    // Logic: `preferred_rank = MIN(0 if ad_group else 1) OVER PARTITION BY tuple`,
    // rồi filter row cùng rank.
    struct FbAds {
        canonical: Canonical,
        spend: Option<f64>,
        imps: Option<i64>,
        clicks: Option<i64>,
        weighted_cpc_sum: Option<f64>,
    }
    let mut fb_ads: Vec<FbAds> = Vec::new();
    {
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
                    SUM(spend),
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
            Ok(FbAds {
                canonical: to_canonical(tuple),
                spend: r.get(5)?,
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

    // Shopee orders
    struct ShopeeOrder {
        canonical: Canonical,
        orders: i64,
        commission: f64,
        order_value: f64,
    }
    let mut shopee_orders: Vec<ShopeeOrder> = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                    COUNT(DISTINCT order_id),
                    COALESCE(SUM(net_commission), 0),
                    COALESCE(SUM(order_value), 0)
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
                commission: r.get(6)?,
                order_value: r.get(7)?,
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
    // Phase 3: aggregate vào UiRow theo representative.
    // ============================================================
    let mut map: HashMap<Canonical, UiRow> = HashMap::new();
    let make_empty = |c: &Canonical| UiRow {
        day_date: day_date.to_string(),
        sub_ids: canonical_to_array(c),
        display_name: default_name(c),
        ads_clicks: None,
        total_spend: None,
        cpc: None,
        impressions: None,
        shopee_clicks_by_referrer: HashMap::new(),
        shopee_clicks_total: 0,
        orders_count: 0,
        commission_total: 0.0,
        order_value_total: 0.0,
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
        if r.spend.is_some() {
            entry.total_spend = Some(entry.total_spend.unwrap_or(0.0) + r.spend.unwrap_or(0.0));
        }
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
        entry.commission_total += r.commission;
        entry.order_value_total += r.order_value;
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
            entry.total_spend = Some(v);
        }
        if let Some(v) = r.cpc {
            entry.cpc = Some(v);
        }
        if let Some(v) = r.orders {
            entry.orders_count = v;
        }
        if let Some(v) = r.commission {
            entry.commission_total = v;
        }
    }

    // Fallback CPC từ spend/clicks.
    for row in map.values_mut() {
        if row.cpc.is_none() {
            if let (Some(s), Some(c)) = (row.total_spend, row.ads_clicks) {
                if c > 0 {
                    row.cpc = Some(s / c as f64);
                }
            }
        }
    }

    // Filter: chỉ giữ row có spend ≠ 0 HOẶC commission ≠ 0.
    let mut rows: Vec<UiRow> = map
        .into_values()
        .filter(|r| {
            let has_spend = r.total_spend.map(|v| v != 0.0).unwrap_or(false);
            let has_commission = r.commission_total != 0.0;
            has_spend || has_commission
        })
        .collect();

    rows.sort_by(|a, b| a.display_name.cmp(&b.display_name));
    Ok(rows)
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
}
