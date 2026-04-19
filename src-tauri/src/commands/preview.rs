//! Preview commands — đếm số row sẽ INSERT mới vs REPLACE/UPDATE mà KHÔNG
//! ghi xuống DB. UI dùng để hiển thị bảng xác nhận trước khi commit.
//!
//! Mỗi kind có 1 preview command. Logic:
//! 1. Validate single-date (như import).
//! 2. Check file_hash dedup → error sớm nếu đã import exact file này rồi.
//! 3. Đếm trong DB các row có identity khớp với rows trong file:
//!    - click: `click_id`
//!    - order: `(checkout_id, item_id, model_id)`
//!    - fb_ad_group: `(day_date, ad_group_name)`
//!    - fb_campaign: `(day_date, campaign_name)`
//! 4. Trả `ImportPreview { total, new, replace, sampleReplace }`.

use std::collections::HashSet;

use rusqlite::{params, OptionalExtension};
use serde::Serialize;
use tauri::State;

use crate::db::DbState;

use super::imports::{
    extract_date, validate_fb_single_date, validate_single_date, ImportFbAdGroupsPayload,
    ImportFbCampaignsPayload, ImportShopeeClicksPayload, ImportShopeeOrdersPayload,
};
use super::{CmdError, CmdResult};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportPreview {
    pub kind: String,
    pub filename: String,
    pub day_date: String,
    pub total_rows: i64,
    pub new_rows: i64,
    pub replace_rows: i64,
    /// Identity string (tên ad_group, click_id, order_id...) của vài row sẽ bị
    /// replace — UI show sample cho user biết.
    pub sample_replace: Vec<String>,
    /// True nếu ngày này đã có data trong DB (cần cảnh báo override mạnh hơn).
    pub day_has_data: bool,
}

const SAMPLE_LIMIT: usize = 5;

fn check_not_imported(
    conn: &rusqlite::Connection,
    file_hash: &str,
    filename: &str,
) -> CmdResult<()> {
    let existing: Option<i64> = conn
        .query_row(
            "SELECT id FROM imported_files WHERE file_hash = ?",
            params![file_hash],
            |r| r.get(0),
        )
        .optional()?;
    if existing.is_some() {
        return Err(CmdError::msg(format!(
            "File '{filename}' đã được import trước đó (hash trùng). Vui lòng chọn file khác."
        )));
    }
    Ok(())
}

fn day_has_any_data(conn: &rusqlite::Connection, day_date: &str) -> CmdResult<bool> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM days WHERE date = ?",
            params![day_date],
            |r| r.get(0),
        )
        .unwrap_or(0);
    Ok(count > 0)
}

/// Hash SHA-256 hex cho file content (giống `imports::compute_hash`, nhưng module-private
/// nên mình copy logic đơn giản ở đây để tránh đụng pub).
fn compute_hash(content: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(content.as_bytes()))
}

// ============================================================
// Shopee clicks
// ============================================================

#[tauri::command]
pub fn preview_import_shopee_clicks(
    state: State<'_, DbState>,
    payload: ImportShopeeClicksPayload,
) -> CmdResult<ImportPreview> {
    let dates: Vec<String> = payload
        .rows
        .iter()
        .filter_map(|r| extract_date(&r.click_time))
        .collect();
    let day_date = validate_single_date(dates, "Thời gian Click")?;
    let hash = compute_hash(&payload.raw_content);

    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    check_not_imported(&conn, &hash, &payload.filename)?;

    // Load existing click_ids for this day → HashSet để check O(1).
    let mut existing: HashSet<String> = HashSet::new();
    let mut stmt =
        conn.prepare("SELECT click_id FROM raw_shopee_clicks WHERE day_date = ?")?;
    for row in stmt.query_map(params![day_date], |r| r.get::<_, String>(0))? {
        existing.insert(row?);
    }

    let mut replace: Vec<String> = Vec::new();
    for r in &payload.rows {
        if existing.contains(&r.click_id) {
            replace.push(r.click_id.clone());
        }
    }

    let total = payload.rows.len() as i64;
    let replace_rows = replace.len() as i64;
    let sample_replace: Vec<String> = replace.into_iter().take(SAMPLE_LIMIT).collect();

    Ok(ImportPreview {
        kind: "shopee_clicks".into(),
        filename: payload.filename,
        day_date: day_date.clone(),
        total_rows: total,
        new_rows: total - replace_rows,
        replace_rows,
        sample_replace,
        day_has_data: day_has_any_data(&conn, &day_date)?,
    })
}

// ============================================================
// Shopee orders
// ============================================================

#[tauri::command]
pub fn preview_import_shopee_orders(
    state: State<'_, DbState>,
    payload: ImportShopeeOrdersPayload,
) -> CmdResult<ImportPreview> {
    let dates: Vec<String> = payload
        .rows
        .iter()
        .filter_map(|r| extract_date(&r.order_time))
        .collect();
    let day_date = validate_single_date(dates, "Thời Gian Đặt Hàng")?;
    let hash = compute_hash(&payload.raw_content);

    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    check_not_imported(&conn, &hash, &payload.filename)?;

    let mut existing: HashSet<(String, String, String)> = HashSet::new();
    let mut stmt = conn.prepare(
        "SELECT checkout_id, item_id, model_id FROM raw_shopee_order_items
         WHERE day_date = ?",
    )?;
    for row in stmt.query_map(params![day_date], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })? {
        existing.insert(row?);
    }

    let mut replace: Vec<String> = Vec::new();
    for r in &payload.rows {
        let key = (r.checkout_id.clone(), r.item_id.clone(), r.model_id.clone());
        if existing.contains(&key) {
            replace.push(format!(
                "{} / {}{}",
                r.order_id,
                r.item_name.as_deref().unwrap_or(&r.item_id),
                if r.model_id.is_empty() {
                    String::new()
                } else {
                    format!(" (model {})", r.model_id)
                }
            ));
        }
    }

    let total = payload.rows.len() as i64;
    let replace_rows = replace.len() as i64;
    let sample_replace: Vec<String> = replace.into_iter().take(SAMPLE_LIMIT).collect();

    Ok(ImportPreview {
        kind: "shopee_commission".into(),
        filename: payload.filename,
        day_date: day_date.clone(),
        total_rows: total,
        new_rows: total - replace_rows,
        replace_rows,
        sample_replace,
        day_has_data: day_has_any_data(&conn, &day_date)?,
    })
}

// ============================================================
// FB ad_groups
// ============================================================

#[tauri::command]
pub fn preview_import_fb_ad_groups(
    state: State<'_, DbState>,
    payload: ImportFbAdGroupsPayload,
) -> CmdResult<ImportPreview> {
    let day_date = validate_fb_single_date(
        payload
            .rows
            .iter()
            .map(|r| (r.report_start.clone(), r.report_end.clone())),
        "FB Ad Group",
    )?;
    let hash = compute_hash(&payload.raw_content);

    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    check_not_imported(&conn, &hash, &payload.filename)?;

    let mut existing: HashSet<String> = HashSet::new();
    let mut stmt = conn.prepare(
        "SELECT name FROM raw_fb_ads WHERE day_date = ? AND level = 'ad_group'",
    )?;
    for row in stmt.query_map(params![day_date], |r| r.get::<_, String>(0))? {
        existing.insert(row?);
    }

    let mut replace: Vec<String> = Vec::new();
    for r in &payload.rows {
        if existing.contains(&r.ad_group_name) {
            replace.push(r.ad_group_name.clone());
        }
    }

    let total = payload.rows.len() as i64;
    let replace_rows = replace.len() as i64;
    let sample_replace: Vec<String> = replace.into_iter().take(SAMPLE_LIMIT).collect();

    Ok(ImportPreview {
        kind: "fb_ad_group".into(),
        filename: payload.filename,
        day_date: day_date.clone(),
        total_rows: total,
        new_rows: total - replace_rows,
        replace_rows,
        sample_replace,
        day_has_data: day_has_any_data(&conn, &day_date)?,
    })
}

// ============================================================
// FB campaigns
// ============================================================

#[tauri::command]
pub fn preview_import_fb_campaigns(
    state: State<'_, DbState>,
    payload: ImportFbCampaignsPayload,
) -> CmdResult<ImportPreview> {
    let day_date = validate_fb_single_date(
        payload
            .rows
            .iter()
            .map(|r| (r.report_start.clone(), r.report_end.clone())),
        "FB Campaign",
    )?;
    let hash = compute_hash(&payload.raw_content);

    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    check_not_imported(&conn, &hash, &payload.filename)?;

    let mut existing: HashSet<String> = HashSet::new();
    let mut stmt = conn.prepare(
        "SELECT name FROM raw_fb_ads WHERE day_date = ? AND level = 'campaign'",
    )?;
    for row in stmt.query_map(params![day_date], |r| r.get::<_, String>(0))? {
        existing.insert(row?);
    }

    let mut replace: Vec<String> = Vec::new();
    for r in &payload.rows {
        if existing.contains(&r.campaign_name) {
            replace.push(r.campaign_name.clone());
        }
    }

    let total = payload.rows.len() as i64;
    let replace_rows = replace.len() as i64;
    let sample_replace: Vec<String> = replace.into_iter().take(SAMPLE_LIMIT).collect();

    Ok(ImportPreview {
        kind: "fb_campaign".into(),
        filename: payload.filename,
        day_date: day_date.clone(),
        total_rows: total,
        new_rows: total - replace_rows,
        replace_rows,
        sample_replace,
        day_has_data: day_has_any_data(&conn, &day_date)?,
    })
}
