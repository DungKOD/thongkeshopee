//! Snapshot DB cho FE autocomplete + summary.
//!
//! `load_overview` chạy 1 lần khi app start và sau mỗi mutation — KHÔNG gọi
//! mỗi filter-change. Cost: O(tổng rows) qua nhiều DISTINCT scan, frequency
//! thấp nên chấp nhận được.

use std::collections::HashSet;

use rusqlite::Connection;
use serde::Serialize;
use tauri::State;

use crate::db::ReadPool;

use super::super::CmdResult;

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
pub async fn load_overview(pool: State<'_, ReadPool>) -> CmdResult<OverviewPayload> {
    let conn = pool.acquire();
    load_overview_impl(&conn)
}

pub(super) fn load_overview_impl(conn: &Connection) -> CmdResult<OverviewPayload> {
    // Dates + day count: cheap single pass trên `days` table nhỏ.
    let (total_days_count, oldest_date, newest_date): (i64, Option<String>, Option<String>) =
        conn.query_row(
            "SELECT COUNT(*), MIN(date), MAX(date) FROM days",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;

    // Display names: query trực tiếp từ bảng nguồn thay vì chạy full aggregation.
    // Cũ: gọi list_days_with_rows_impl (N ngày × ~8 SQL queries) chỉ để lấy
    // display_name → O(N×8) queries. Mới: 3 queries phẳng, O(1) không phụ thuộc số ngày.
    let mut set: HashSet<String> = HashSet::new();

    // 1. manual_entries — nguồn chính của tên sản phẩm do user đặt.
    // a) Explicit display_name (user-set label).
    {
        let mut stmt = conn.prepare_cached(
            "SELECT DISTINCT display_name FROM manual_entries
             WHERE display_name IS NOT NULL AND display_name != ''",
        )?;
        for name in stmt.query_map([], |r| r.get::<_, String>(0))? {
            overview_insert_name(&mut set, &name?);
        }
    }
    // b) Sub_id derived names (khi không có display_name — standard case).
    // Build canonical bằng cách join các sub_id không rỗng, giống default_name().
    {
        let mut stmt = conn.prepare_cached(
            "SELECT DISTINCT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
             FROM manual_entries",
        )?;
        for row in stmt.query_map([], |r| {
            Ok([
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ])
        })? {
            let parts: Vec<String> = row?
                .into_iter()
                .filter(|s| !s.is_empty())
                .collect();
            if parts.is_empty() {
                continue;
            }
            let name = parts.join("-");
            overview_insert_name(&mut set, &name);
        }
    }
    // 2. FB hierarchy — sub_id1..5 (nguồn chính, ưu tiên ad_name) + campaign_name.
    // Sub_id resolution trong FE: ad_name → adSetName → campaignName. Phải query
    // sub_id columns trực tiếp để cover trường hợp sub_id nằm ở ad_name (không
    // phải campaign_name). Campaign_name vẫn thêm vào để user tìm được theo tên chiến dịch.
    {
        let mut stmt = conn.prepare_cached(
            "SELECT DISTINCT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
             FROM raw_fb_ads_hierarchy",
        )?;
        for row in stmt.query_map([], |r| {
            Ok([
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ])
        })? {
            let parts: Vec<String> =
                row?.into_iter().filter(|s| !s.is_empty()).collect();
            if !parts.is_empty() {
                overview_insert_name(&mut set, &parts.join("-"));
            }
        }
        let mut stmt2 = conn.prepare_cached(
            "SELECT DISTINCT campaign_name FROM raw_fb_ads_hierarchy
             WHERE campaign_name != ''",
        )?;
        for name in stmt2.query_map([], |r| r.get::<_, String>(0))? {
            overview_insert_name(&mut set, &name?);
        }
    }
    // 3. raw_fb_ads legacy — sub_id1..5 (cả ad_group lẫn campaign) + ad names.
    // Batch query ưu tiên ad_group over campaign cho cùng tuple, nên sub_id của
    // ad_group phải có trong dropdown. Query cả 2 levels qua sub_id columns.
    {
        let mut stmt = conn.prepare_cached(
            "SELECT DISTINCT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
             FROM raw_fb_ads",
        )?;
        for row in stmt.query_map([], |r| {
            Ok([
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ])
        })? {
            let parts: Vec<String> =
                row?.into_iter().filter(|s| !s.is_empty()).collect();
            if !parts.is_empty() {
                overview_insert_name(&mut set, &parts.join("-"));
            }
        }
        let mut stmt2 = conn.prepare_cached(
            "SELECT DISTINCT name FROM raw_fb_ads WHERE name != ''",
        )?;
        for name in stmt2.query_map([], |r| r.get::<_, String>(0))? {
            overview_insert_name(&mut set, &name?);
        }
    }

    // Đếm rows: UNION DISTINCT (sub_id_tuple, day_date) qua orders + manual.
    // Dùng idx_orders_day_subid + idx_manual_day → không scan toàn bảng.
    // Lưu ý: xấp xỉ UiRow count (prefix-matching có thể merge/split khác 1:1),
    // nhưng đủ chính xác cho hiển thị "X sản phẩm" trong Settings.
    let total_rows: i64 = conn.query_row(
        "SELECT COUNT(*) FROM (
             SELECT DISTINCT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date
             FROM raw_shopee_order_items
             UNION
             SELECT DISTINCT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date
             FROM raw_fb_ads_hierarchy
             UNION
             SELECT DISTINCT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date
             FROM raw_fb_ads
             UNION
             SELECT DISTINCT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date
             FROM manual_entries
         )",
        [],
        |r| r.get(0),
    )?;

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

/// Insert `name` + prefix hierarchy + individual parts vào `set`.
/// Tách ra helper tránh duplicate logic.
fn overview_insert_name(set: &mut HashSet<String>, name: &str) {
    set.insert(name.to_string());
    let parts: Vec<&str> = name.split('-').filter(|p| !p.is_empty()).collect();
    // "a-b-c" → "a", "a-b"
    for i in 1..parts.len() {
        set.insert(parts[..i].join("-"));
    }
    // Từng part riêng: "dammaxi" match cả "MuseStudio-dammaxi"
    for p in &parts {
        set.insert((*p).to_string());
    }
}
