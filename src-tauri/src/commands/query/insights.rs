//! Analytics endpoints cho Overview tab:
//! - `list_click_referrers`: dropdown nguồn traffic
//! - `load_hourly_orders` / `load_hourly_clicks`: phân bố giờ
//! - `load_referrer_efficiency`: CR + commission per referrer
//! - `load_click_order_delays`: histogram delay click → order
//! - `load_cancellation_by_subid`: % hủy per sub_id × day
//!
//! Tất cả filter theo `DaysFilter` (from/to/account/optional sub_ids).

use tauri::State;

use crate::db::DbState;

use super::super::{CmdError, CmdResult};
use super::aggregate::{
    append_date_account_filters, append_subid_prefilter, is_prefix, params_to_refs,
    read_sub_id_match_mode, sub_ids_match, to_canonical, Canonical,
};
use super::{AccountFilterMode, DaysFilter};

/// Unique referrer values (cột "Người giới thiệu" trong WebsiteClickReport).
/// UI Settings dùng để hiển thị list checkbox cho user filter.
#[tauri::command]
pub fn list_click_referrers(state: State<'_, DbState>) -> CmdResult<Vec<String>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare_cached(
        "SELECT DISTINCT COALESCE(referrer, '(khác)') FROM raw_shopee_clicks
         ORDER BY 1",
    )?;
    let rows: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// Phân bố đơn theo giờ trong ngày (0-23) — aggregate toàn bộ orders trong
/// khoảng filter. Giúp user biết giờ nào buy nhiều → tối ưu đăng bài, run ads.
///
/// Filter: `from_date`, `to_date`, `account_filter`, `sub_ids`. Nếu `sub_ids`
/// provided, chỉ count order có sub_id tuple prefix-compatible với target
/// (dùng cho dialog Chi tiết sản phẩm).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HourlyOrderBucket {
    pub hour: u8,
    pub orders: i64,
    pub order_value: f64,
    pub commission: f64,
}

#[tauri::command]
pub fn load_hourly_orders(
    state: State<'_, DbState>,
    filter: Option<DaysFilter>,
) -> CmdResult<Vec<HourlyOrderBucket>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let f = filter.unwrap_or_default();
    let match_mode = read_sub_id_match_mode(&conn);

    // 2 paths: aggregate SQL-native (fast) khi không filter sub_ids, hoặc
    // scan-and-aggregate per-row (filter prefix-match) khi có sub_ids.
    if f.sub_ids.is_none() {
        let mut sql = String::from(
            "SELECT
                CAST(strftime('%H', order_time) AS INTEGER) as hour,
                COUNT(DISTINCT order_id) as orders,
                COALESCE(SUM(order_value), 0) as gmv,
                COALESCE(SUM(net_commission), 0) as commission
             FROM raw_shopee_order_items
             WHERE order_time IS NOT NULL AND order_time != ''",
        );
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        append_date_account_filters(&mut sql, &mut params_vec, &f);
        sql.push_str(" GROUP BY hour ORDER BY hour ASC");

        let mut stmt = conn.prepare_cached(&sql)?;
        let params_refs = params_to_refs(&params_vec);
        let rows: Vec<HourlyOrderBucket> = stmt
            .query_map(params_refs.as_slice(), |r| {
                let hour_i: i64 = r.get(0)?;
                Ok(HourlyOrderBucket {
                    hour: hour_i.clamp(0, 23) as u8,
                    orders: r.get(1)?,
                    order_value: r.get(2)?,
                    commission: r.get(3)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        return Ok(fill_24_orders(rows));
    }

    // Sub_ids filter path: query per-item với sub_id columns + filter Rust-side.
    let target = f.sub_ids.clone().unwrap();
    let mut sql = String::from(
        "SELECT order_id,
                CAST(strftime('%H', order_time) AS INTEGER) as hour,
                COALESCE(order_value, 0) as order_value,
                COALESCE(net_commission, 0) as net_commission,
                sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
         FROM raw_shopee_order_items
         WHERE order_time IS NOT NULL AND order_time != ''",
    );
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    append_date_account_filters(&mut sql, &mut params_vec, &f);
    append_subid_prefilter(&mut sql, &mut params_vec, &target, match_mode);

    let mut stmt = conn.prepare_cached(&sql)?;
    let params_refs = params_to_refs(&params_vec);
    // Per-hour: (distinct_order_ids, sum_order_value, sum_commission).
    let mut buckets: [(std::collections::HashSet<String>, f64, f64); 24] =
        std::array::from_fn(|_| (std::collections::HashSet::new(), 0.0, 0.0));
    for row in stmt.query_map(params_refs.as_slice(), |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, f64>(2)?,
            r.get::<_, f64>(3)?,
            [
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, String>(8)?,
            ],
        ))
    })? {
        let (order_id, hour, ov, comm, subs) = row?;
        if !sub_ids_match(&subs, &target, match_mode) {
            continue;
        }
        let idx = hour.clamp(0, 23) as usize;
        buckets[idx].0.insert(order_id);
        buckets[idx].1 += ov;
        buckets[idx].2 += comm;
    }
    Ok((0..24)
        .map(|i| HourlyOrderBucket {
            hour: i as u8,
            orders: buckets[i].0.len() as i64,
            order_value: buckets[i].1,
            commission: buckets[i].2,
        })
        .collect())
}

/// Helper: fill 24 hour buckets từ Vec partial (SQL GROUP BY thường skip hour rỗng).
fn fill_24_orders(rows: Vec<HourlyOrderBucket>) -> Vec<HourlyOrderBucket> {
    let mut full = vec![
        HourlyOrderBucket {
            hour: 0,
            orders: 0,
            order_value: 0.0,
            commission: 0.0,
        };
        24
    ];
    for i in 0..24u8 {
        full[i as usize].hour = i;
    }
    for row in rows {
        let idx = row.hour as usize;
        full[idx] = row;
    }
    full
}

/// Phân bố click Shopee theo giờ trong ngày (0-23) — mirror `load_hourly_orders`
/// nhưng aggregate `raw_shopee_clicks.click_time`. Giúp user biết giờ nào
/// traffic peak (khác giờ mua → user click đêm chốt sáng = insight cho schedule ads).
///
/// Filter: `from_date`, `to_date`, `account_filter`. Skip `sub_id_filter` (Overview tổng hợp).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HourlyClickBucket {
    pub hour: u8,
    pub clicks: i64,
}

#[tauri::command]
pub fn load_hourly_clicks(
    state: State<'_, DbState>,
    filter: Option<DaysFilter>,
) -> CmdResult<Vec<HourlyClickBucket>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let f = filter.unwrap_or_default();
    let match_mode = read_sub_id_match_mode(&conn);

    // SQL-native path nếu không filter sub_ids. Có sub_ids → query row level
    // + post-filter bằng prefix match.
    if f.sub_ids.is_none() {
        let mut sql = String::from(
            "SELECT
                CAST(strftime('%H', click_time) AS INTEGER) as hour,
                COUNT(*) as clicks
             FROM raw_shopee_clicks
             WHERE click_time IS NOT NULL AND click_time != ''",
        );
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        append_date_account_filters(&mut sql, &mut params_vec, &f);
        sql.push_str(" GROUP BY hour ORDER BY hour ASC");

        let mut stmt = conn.prepare_cached(&sql)?;
        let params_refs = params_to_refs(&params_vec);
        let rows: Vec<HourlyClickBucket> = stmt
            .query_map(params_refs.as_slice(), |r| {
                let hour_i: i64 = r.get(0)?;
                Ok(HourlyClickBucket {
                    hour: hour_i.clamp(0, 23) as u8,
                    clicks: r.get(1)?,
                })
            })?
            .collect::<Result<_, _>>()?;

        let mut full = vec![HourlyClickBucket { hour: 0, clicks: 0 }; 24];
        for i in 0..24u8 {
            full[i as usize].hour = i;
        }
        for row in rows {
            let idx = row.hour as usize;
            full[idx] = row;
        }
        return Ok(full);
    }

    // Sub_ids filter path.
    let target = f.sub_ids.clone().unwrap();
    let mut sql = String::from(
        "SELECT CAST(strftime('%H', click_time) AS INTEGER) as hour,
                sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
         FROM raw_shopee_clicks
         WHERE click_time IS NOT NULL AND click_time != ''",
    );
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    append_date_account_filters(&mut sql, &mut params_vec, &f);
    let mut stmt = conn.prepare_cached(&sql)?;
    let params_refs = params_to_refs(&params_vec);
    let mut counts = [0_i64; 24];
    for row in stmt.query_map(params_refs.as_slice(), |r| {
        Ok((
            r.get::<_, i64>(0)?,
            [
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
            ],
        ))
    })? {
        let (hour, subs) = row?;
        if !sub_ids_match(&subs, &target, match_mode) {
            continue;
        }
        let idx = hour.clamp(0, 23) as usize;
        counts[idx] += 1;
    }
    Ok((0..24)
        .map(|i| HourlyClickBucket {
            hour: i as u8,
            clicks: counts[i],
        })
        .collect())
}

/// Hiệu suất từng referrer (nguồn traffic Shopee) — aggregate click + đơn.
/// Khác `clicksByReferrer` chỉ count click, cái này bring CR + commission →
/// biết referrer nào quality traffic (click nhiều + CR cao) vs referrer junk.
///
/// Logic match click → order: cùng sub_ids tuple trong **cùng ngày click**.
/// Shopee `referrer` chỉ gắn với click_row; order không có referrer →
/// phải JOIN qua sub_ids + click date. Order side dùng `DATE(click_time)`
/// (không phải `day_date` = order date) vì order delay >24h là chuyện
/// thường — nếu join theo order date sẽ mất attribution xuyên ngày.
///
/// Tuple sub_id được merge theo **longest-prefix canonical** trong cùng
/// day: click `(A,B,'','','')` + order `(A,B,C,'','')` → cùng canonical
/// `(A,B,C)`, được attribute chung. Tránh mất attribution khi click row
/// và order row ghi sub_ids ở depth khác nhau.
///
/// Approximate — nếu 1 sub_id có nhiều referrer cùng ngày, order được
/// chia theo tỉ lệ click:
///   orders_from_R = total_orders_for_subids × (clicks_R / total_clicks_for_subids)
///
/// Order có `click_time` không match click row nào (cùng canonical, cùng
/// ngày) → đẩy về row đặc biệt `(không gắn click)` với `clicks=0, cr=null`,
/// để user thấy có bao nhiêu đơn không attribute được vào nguồn traffic.
///
/// `commission` và `commission_pending` từ DB (raw `net_commission` đã
/// trừ MCN, **chưa** trừ tax + reserve). FE áp `computeNetCommission`
/// để ra số ròng cuối cùng — nhất quán với KPI Overview.
///
/// Filter: `from_date`, `to_date`, `account_filter` (áp trên click date
/// ở cả 2 phía để đối xứng).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReferrerEfficiency {
    pub referrer: String,
    pub clicks: i64,
    pub orders: f64,
    pub commission: f64,
    pub commission_pending: f64,
    pub cr: Option<f64>,
}

#[tauri::command]
pub fn load_referrer_efficiency(
    state: State<'_, DbState>,
    filter: Option<DaysFilter>,
) -> CmdResult<Vec<ReferrerEfficiency>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let f = filter.unwrap_or_default();
    let match_mode = read_sub_id_match_mode(&conn);

    // Click side filter trên `day_date` (= click date theo schema).
    // Order side filter trên `DATE(click_time)` để đối xứng — đơn có click_time
    // nằm trong range được attribute, không quan tâm order_time.
    let mut where_clicks = String::from(" WHERE 1=1");
    let mut where_orders = String::from(" WHERE click_time IS NOT NULL");
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    let mut params_orders: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(v) = &f.from_date {
        where_clicks.push_str(" AND day_date >= ?");
        where_orders.push_str(" AND DATE(click_time) >= ?");
        params_vec.push(Box::new(v.clone()));
        params_orders.push(Box::new(v.clone()));
    }
    if let Some(v) = &f.to_date {
        where_clicks.push_str(" AND day_date <= ?");
        where_orders.push_str(" AND DATE(click_time) <= ?");
        params_vec.push(Box::new(v.clone()));
        params_orders.push(Box::new(v.clone()));
    }
    if let Some(AccountFilterMode::Account { id }) = f.account_filter.as_ref() {
        where_clicks.push_str(" AND shopee_account_id = ?");
        where_orders.push_str(" AND shopee_account_id = ?");
        params_vec.push(Box::new(*id));
        params_orders.push(Box::new(*id));
    }

    // Step 1: click count per (day, sub_ids, referrer)
    let clicks_sql = format!(
        "SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                COALESCE(referrer, '') as referrer, COUNT(*) as clicks
         FROM raw_shopee_clicks
         {where_clicks}
         GROUP BY day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, referrer"
    );
    let mut stmt = conn.prepare_cached(&clicks_sql)?;
    let refs_clicks = params_to_refs(&params_vec);
    type ClickKey = (String, [String; 5]);
    let mut clicks_by_key: std::collections::HashMap<ClickKey, Vec<(String, i64)>> =
        std::collections::HashMap::new();
    for row in stmt.query_map(refs_clicks.as_slice(), |r| {
        Ok((
            r.get::<_, String>(0)?,
            [
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
            ],
            r.get::<_, String>(6)?,
            r.get::<_, i64>(7)?,
        ))
    })? {
        let (day, subs, referrer, clicks) = row?;
        clicks_by_key
            .entry((day, subs))
            .or_default()
            .push((referrer, clicks));
    }

    // Step 2: order count + commission per (click_day, sub_ids) — distinct order_id.
    // Key dùng DATE(click_time) chứ KHÔNG phải day_date (= order date) → để
    // attribute đúng cho click 1 ngày + order 3 ngày sau (delay phổ biến).
    // commission_pending split theo order_status để FE áp reserve rate đúng.
    let orders_sql = format!(
        "SELECT DATE(click_time) AS click_day,
                sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                COUNT(DISTINCT order_id) as orders,
                COALESCE(SUM(net_commission), 0) as commission,
                COALESCE(SUM(CASE WHEN order_status IN ('Đang chờ xử lý', 'Chưa thanh toán')
                                  THEN net_commission ELSE 0 END), 0) as commission_pending
         FROM raw_shopee_order_items
         {where_orders}
         GROUP BY click_day, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5"
    );
    let mut stmt = conn.prepare_cached(&orders_sql)?;
    let refs_orders = params_to_refs(&params_orders);
    type OrderAgg = (i64, f64, f64);
    let mut orders_by_key: std::collections::HashMap<ClickKey, OrderAgg> =
        std::collections::HashMap::new();
    for row in stmt.query_map(refs_orders.as_slice(), |r| {
        Ok((
            r.get::<_, String>(0)?,
            [
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
            ],
            r.get::<_, i64>(6)?,
            r.get::<_, f64>(7)?,
            r.get::<_, f64>(8)?,
        ))
    })? {
        let (day, subs, orders, commission, pending) = row?;
        orders_by_key.insert((day, subs), (orders, commission, pending));
    }

    // Step 3: canonical mapping per-day. Trong cùng day, tuple ngắn được
    // merge vào tuple dài hơn nếu là prefix-compatible (longest wins).
    // Multi-chain conflict (vd cùng day có (A,B,C) và (A,B,D)) → iterate
    // longest first, tuple ngắn (A,B) merge vào tuple longer xuất hiện
    // trước theo lexicographic order — deterministic, hiếm trên thực tế.
    use std::collections::HashSet;
    let mut keys_per_day: std::collections::HashMap<String, Vec<[String; 5]>> =
        std::collections::HashMap::new();
    for (day, subs) in clicks_by_key.keys().chain(orders_by_key.keys()) {
        keys_per_day
            .entry(day.clone())
            .or_default()
            .push(subs.clone());
    }
    let mut canonical_for: std::collections::HashMap<ClickKey, [String; 5]> =
        std::collections::HashMap::new();
    for (day, raw_tuples) in keys_per_day {
        // Dedup tuples in this day.
        let mut tuples: Vec<[String; 5]> = raw_tuples;
        tuples.sort();
        tuples.dedup();
        // Pre-compute Canonical (Vec) for each, then sort longest-first.
        let mut indexed: Vec<([String; 5], Canonical)> = tuples
            .into_iter()
            .map(|s| {
                let c = to_canonical(s.clone());
                (s, c)
            })
            .collect();
        indexed.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then_with(|| a.1.cmp(&b.1)));
        let mut assigned: HashSet<[String; 5]> = HashSet::new();
        for (long_s, long_c) in &indexed {
            if assigned.contains(long_s) {
                continue;
            }
            for (short_s, short_c) in &indexed {
                if assigned.contains(short_s) {
                    continue;
                }
                if !is_prefix(short_c, long_c) {
                    continue;
                }
                // Empty canonical (= không có sub_id) là prefix của mọi tuple,
                // nhưng "không gắn sub_id" KHÔNG được phép merge vào tuple
                // có sub_id — sẽ gán nhầm click/order vô danh cho 1 product.
                if short_c.is_empty() && !long_c.is_empty() {
                    continue;
                }
                canonical_for.insert((day.clone(), short_s.clone()), long_s.clone());
                assigned.insert(short_s.clone());
            }
        }
    }

    // Step 4: re-key cả 2 maps về canonical key.
    let mut canonical_clicks: std::collections::HashMap<ClickKey, std::collections::HashMap<String, i64>> =
        std::collections::HashMap::new();
    for ((day, subs), referrer_clicks) in clicks_by_key {
        let canonical = canonical_for
            .get(&(day.clone(), subs.clone()))
            .cloned()
            .unwrap_or(subs);
        let bucket = canonical_clicks
            .entry((day, canonical))
            .or_default();
        for (referrer, c) in referrer_clicks {
            *bucket.entry(referrer).or_insert(0) += c;
        }
    }
    let mut canonical_orders: std::collections::HashMap<ClickKey, OrderAgg> =
        std::collections::HashMap::new();
    for ((day, subs), (orders, commission, pending)) in orders_by_key {
        let canonical = canonical_for
            .get(&(day.clone(), subs.clone()))
            .cloned()
            .unwrap_or(subs);
        let entry = canonical_orders
            .entry((day, canonical))
            .or_insert((0, 0.0, 0.0));
        entry.0 += orders;
        entry.1 += commission;
        entry.2 += pending;
    }

    // Step 5: aggregate per referrer — distribute orders theo tỉ lệ click.
    // Nếu f.sub_ids provided (Chi tiết product), filter keys prefix-match trước.
    let target_sub_ids = f.sub_ids.clone();
    // Tuple agg: (clicks, orders, commission, commission_pending).
    let mut agg: std::collections::HashMap<String, (i64, f64, f64, f64)> =
        std::collections::HashMap::new();
    for (key, referrer_clicks) in &canonical_clicks {
        if let Some(target) = target_sub_ids.as_ref() {
            if !sub_ids_match(&key.1, target, match_mode) {
                continue;
            }
        }
        let total_clicks: i64 = referrer_clicks.values().sum();
        let (orders_for_key, commission_for_key, pending_for_key) = canonical_orders
            .get(key)
            .copied()
            .unwrap_or((0, 0.0, 0.0));
        if total_clicks == 0 {
            continue;
        }
        for (referrer, clicks) in referrer_clicks {
            let share = *clicks as f64 / total_clicks as f64;
            let orders_share = orders_for_key as f64 * share;
            let commission_share = commission_for_key * share;
            let pending_share = pending_for_key * share;
            let e = agg.entry(referrer.clone()).or_insert((0, 0.0, 0.0, 0.0));
            e.0 += *clicks;
            e.1 += orders_share;
            e.2 += commission_share;
            e.3 += pending_share;
        }
    }

    // Step 6: orphan orders — canonical key có trong orders nhưng không có
    // click match → đẩy vào row đặc biệt "(không gắn click)" với clicks=0.
    // Đây là đơn có click_time NOT NULL nhưng tuple/day không khớp click row
    // nào trong DB (ví dụ click data thiếu, hoặc tracking sai).
    let mut orphan_orders = 0.0_f64;
    let mut orphan_commission = 0.0_f64;
    let mut orphan_pending = 0.0_f64;
    for (key, (orders, commission, pending)) in &canonical_orders {
        if let Some(target) = target_sub_ids.as_ref() {
            if !sub_ids_match(&key.1, target, match_mode) {
                continue;
            }
        }
        let has_clicks = canonical_clicks
            .get(key)
            .is_some_and(|m| m.values().any(|c| *c > 0));
        if !has_clicks {
            orphan_orders += *orders as f64;
            orphan_commission += commission;
            orphan_pending += pending;
        }
    }

    let mut out: Vec<ReferrerEfficiency> = agg
        .into_iter()
        .map(|(referrer, (clicks, orders, commission, commission_pending))| {
            let cr = if clicks > 0 {
                Some(orders / clicks as f64 * 100.0)
            } else {
                None
            };
            ReferrerEfficiency {
                referrer,
                clicks,
                orders,
                commission,
                commission_pending,
                cr,
            }
        })
        .collect();
    if orphan_orders > 0.0 || orphan_commission > 0.0 || orphan_pending > 0.0 {
        out.push(ReferrerEfficiency {
            referrer: "(không gắn click)".to_string(),
            clicks: 0,
            orders: orphan_orders,
            commission: orphan_commission,
            commission_pending: orphan_pending,
            cr: None,
        });
    }
    // Sort desc by CR (null last). Tiebreak by clicks desc.
    out.sort_by(|a, b| {
        b.cr
            .partial_cmp(&a.cr)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.clicks.cmp(&a.clicks))
    });
    Ok(out)
}

/// Phân bố thời gian từ click → đặt hàng. Bucket cố định:
/// <1h, 1-6h, 6-24h, 1-3d, >3d, no_click (click_time null).
/// Giúp hiểu user behavior: impulse buy vs consider → ảnh hưởng retargeting window.
///
/// Filter: `from_date`, `to_date`, `account_filter`. Skip sub_id.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClickOrderDelayBucket {
    pub bucket: String,
    pub orders: i64,
}

#[tauri::command]
pub fn load_click_order_delays(
    state: State<'_, DbState>,
    filter: Option<DaysFilter>,
) -> CmdResult<Vec<ClickOrderDelayBucket>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let f = filter.unwrap_or_default();
    let match_mode = read_sub_id_match_mode(&conn);

    // Compute delay seconds. Null click_time → bucket 'no_click'.
    // Query sub_id columns để filter prefix-match khi f.sub_ids provided.
    let mut sql = String::from(
        "SELECT order_id, click_time, order_time,
                sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
         FROM raw_shopee_order_items
         WHERE order_time IS NOT NULL AND order_time != ''",
    );
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    append_date_account_filters(&mut sql, &mut params_vec, &f);
    let target = f.sub_ids.clone();
    if let Some(t) = target.as_ref() {
        append_subid_prefilter(&mut sql, &mut params_vec, t, match_mode);
    }

    let mut stmt = conn.prepare_cached(&sql)?;
    let refs = params_to_refs(&params_vec);
    // Distinct orders (1 order nhiều items) — dedupe qua HashMap<order_id, delay>.
    let mut by_order: std::collections::HashMap<String, Option<f64>> =
        std::collections::HashMap::new();
    for row in stmt.query_map(refs.as_slice(), |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, String>(2)?,
            [
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, String>(7)?,
            ],
        ))
    })? {
        let (order_id, click_time, order_time, subs) = row?;
        if let Some(t) = target.as_ref() {
            if !sub_ids_match(&subs, t, match_mode) {
                continue;
            }
        }
        if by_order.contains_key(&order_id) {
            continue;
        }
        let delay_s = match click_time.as_deref() {
            None | Some("") => None,
            Some(ct) => parse_timestamp_to_epoch(ct).and_then(|c| {
                parse_timestamp_to_epoch(&order_time).map(|o| (o - c).max(0) as f64)
            }),
        };
        by_order.insert(order_id, delay_s);
    }

    // Bucket counts. Cứng 6 buckets theo thứ tự hiển thị.
    let mut buckets: std::collections::HashMap<&'static str, i64> =
        std::collections::HashMap::from([
            ("<1h", 0),
            ("1-6h", 0),
            ("6-24h", 0),
            ("1-3d", 0),
            (">3d", 0),
            ("no_click", 0),
        ]);
    for delay in by_order.into_values() {
        let key = match delay {
            None => "no_click",
            Some(s) if s < 3600.0 => "<1h",
            Some(s) if s < 6.0 * 3600.0 => "1-6h",
            Some(s) if s < 24.0 * 3600.0 => "6-24h",
            Some(s) if s < 3.0 * 86400.0 => "1-3d",
            Some(_) => ">3d",
        };
        *buckets.entry(key).or_insert(0) += 1;
    }

    // Output theo thứ tự fixed.
    let order = ["<1h", "1-6h", "6-24h", "1-3d", ">3d", "no_click"];
    Ok(order
        .iter()
        .map(|&k| ClickOrderDelayBucket {
            bucket: k.to_string(),
            orders: *buckets.get(k).unwrap_or(&0),
        })
        .collect())
}

/// Bucket per (sub_id tuple, day_date) — dùng cho chart "Tỉ lệ hoàn hủy theo
/// sản phẩm" trên Overview tab. FE aggregate qua sub_id để xếp hạng DESC theo
/// % hủy. `cancelled` = đơn có ≥1 line status chứa "hủy" / "Hủy" / "cancel".
/// `zero_hh` = đơn có SUM(net_commission) = 0 (gồm hủy + đơn chưa attribute).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancellationByDayBucket {
    pub day_date: String,
    pub sub_id1: String,
    pub sub_id2: String,
    pub sub_id3: String,
    pub sub_id4: String,
    pub sub_id5: String,
    pub total_orders: i64,
    pub cancelled_orders: i64,
    pub zero_hh_orders: i64,
}

/// Aggregate cancellation/zero-HH count per (sub_id, day) — group SQL-side
/// để 1 round-trip cover toàn bộ Overview range. FE sort + topN.
/// Filter: from_date/to_date/account. `sub_ids` không dùng (Overview = all SP).
#[tauri::command]
pub fn load_cancellation_by_subid(
    state: State<'_, DbState>,
    filter: Option<DaysFilter>,
) -> CmdResult<Vec<CancellationByDayBucket>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let f = filter.unwrap_or_default();

    // 2-tầng: inner collapse line-items thành (sub_ids, day, order_id) +
    // flag hủy + sum net_commission; outer COUNT distinct order theo
    // (sub_ids, day). Match "hủy" qua INSTR (case sensitivity Vietnamese:
    // 'hủy' cover "Đã hủy", thêm 'Hủy' cho variant viết hoa; 'cancel' cover
    // LOWER cho English). Cùng pattern với regex FE `/hủy|cancel/i`.
    let mut sql = String::from(
        "SELECT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date,
                COUNT(*) AS total_orders,
                COALESCE(SUM(has_cancelled), 0) AS cancelled_orders,
                COALESCE(SUM(CASE WHEN net_sum = 0 THEN 1 ELSE 0 END), 0) AS zero_hh_orders
         FROM (
            SELECT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date, order_id,
                   MAX(CASE
                        WHEN instr(order_status, 'hủy') > 0 THEN 1
                        WHEN instr(order_status, 'Hủy') > 0 THEN 1
                        WHEN instr(lower(order_status), 'cancel') > 0 THEN 1
                        ELSE 0
                       END) AS has_cancelled,
                   COALESCE(SUM(net_commission), 0) AS net_sum
            FROM raw_shopee_order_items
            WHERE 1=1",
    );
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    append_date_account_filters(&mut sql, &mut params_vec, &f);
    sql.push_str(
        "    GROUP BY sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date, order_id
         )
         GROUP BY sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date",
    );

    let mut stmt = conn.prepare_cached(&sql)?;
    let refs = params_to_refs(&params_vec);
    let rows: Vec<CancellationByDayBucket> = stmt
        .query_map(refs.as_slice(), |r| {
            Ok(CancellationByDayBucket {
                sub_id1: r.get(0)?,
                sub_id2: r.get(1)?,
                sub_id3: r.get(2)?,
                sub_id4: r.get(3)?,
                sub_id5: r.get(4)?,
                day_date: r.get(5)?,
                total_orders: r.get(6)?,
                cancelled_orders: r.get(7)?,
                zero_hh_orders: r.get(8)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// Parse "YYYY-MM-DD HH:MM:SS" (Shopee format) or ISO8601 → epoch seconds.
/// Return None nếu fail — caller treat as no_click bucket.
fn parse_timestamp_to_epoch(s: &str) -> Option<i64> {
    use chrono::{DateTime, NaiveDateTime};
    // Try RFC3339/ISO8601 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp());
    }
    // Fallback: "YYYY-MM-DD HH:MM:SS" (Shopee CSV format)
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(ndt.and_utc().timestamp());
    }
    None
}
