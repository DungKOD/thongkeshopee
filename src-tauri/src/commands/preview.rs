//! Preview commands — đếm số row sẽ INSERT mới vs REPLACE/UPDATE mà KHÔNG
//! ghi xuống DB. UI dùng để hiển thị bảng xác nhận trước khi commit.
//!
//! Mỗi kind có 1 preview command. Logic:
//! 1. Shopee (clicks/commission): multi-day OK, derive day_date per-row, trả
//!    `day_date_from`/`day_date_to` cho UI hiện range.
//!    FB (ad_group/campaign): giữ single-date validation.
//! 2. Check `file_hash` dedup → KHÔNG error, trả `already_imported: true` +
//!    `existing_day_date`. FE dialog highlight file "sẽ bỏ qua", user confirm
//!    commit thì FE filter ra không gửi xuống.
//! 3. Đếm trong DB các row có identity khớp (replace count).
//! 4. Trả `ImportPreview { total, new, replace, sampleReplace, ...}`.

use std::collections::HashSet;

use rusqlite::{params, OptionalExtension};
use serde::Serialize;
use tauri::State;

use crate::db::DbState;

use super::imports::{
    extract_date, validate_fb_single_date, ImportFbAdGroupsPayload,
    ImportFbCampaignsPayload, ImportShopeeClicksPayload, ImportShopeeOrdersPayload,
};
use super::{CmdError, CmdResult};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportPreview {
    pub kind: String,
    pub filename: String,
    /// Earliest date — backward compat cho FE code cũ còn dùng field này.
    pub day_date: String,
    /// Range của data trong file (Shopee multi-day). FB single-date thì
    /// day_date_from = day_date_to.
    pub day_date_from: String,
    pub day_date_to: String,
    pub total_rows: i64,
    pub new_rows: i64,
    pub replace_rows: i64,
    /// Identity string (tên ad_group, click_id, order_id...) của vài row sẽ bị
    /// replace — UI show sample cho user biết.
    pub sample_replace: Vec<String>,
    /// True nếu bất kỳ ngày nào trong file đã có data trong DB (cảnh báo override).
    pub day_has_data: bool,
    /// File đã import trước đó (hash trùng). FE hiện "sẽ bỏ qua" + skip commit.
    pub already_imported: bool,
    /// Nếu already_imported = true: day_date của lần import trước (informational).
    pub existing_day_date: Option<String>,
    /// Số rows không parse được date → skip khi import (Shopee multi-day only).
    pub skipped: i64,
}

const SAMPLE_LIMIT: usize = 5;

/// Check hash đã tồn tại trong imported_files chưa. Trả (bool, Option<existing_day_date>).
/// KHÔNG error — preview hiện thông tin, FE quyết định skip.
fn check_hash_imported(
    conn: &rusqlite::Connection,
    file_hash: &str,
) -> CmdResult<(bool, Option<String>)> {
    let existing: Option<Option<String>> = conn
        .query_row(
            "SELECT day_date FROM imported_files WHERE file_hash = ?",
            params![file_hash],
            |r| r.get(0),
        )
        .optional()?;
    match existing {
        Some(day) => Ok((true, day)),
        None => Ok((false, None)),
    }
}

fn any_day_has_data(conn: &rusqlite::Connection, dates: &[String]) -> CmdResult<bool> {
    if dates.is_empty() {
        return Ok(false);
    }
    // Build placeholder string "?,?,?" cho IN clause.
    let placeholders = std::iter::repeat("?")
        .take(dates.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("SELECT COUNT(*) FROM days WHERE date IN ({placeholders})");
    let params_vec: Vec<&dyn rusqlite::ToSql> =
        dates.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let count: i64 = conn
        .query_row(&sql, params_vec.as_slice(), |r| r.get(0))
        .unwrap_or(0);
    Ok(count > 0)
}

/// Hash SHA-256 hex cho file content.
fn compute_hash(content: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(content.as_bytes()))
}

/// Derive date range từ rows qua extract_date. Trả (from, to, skipped_count).
/// Err nếu không có date nào valid.
fn derive_shopee_date_range<T, F>(
    rows: &[T],
    label: &str,
    extract: F,
) -> CmdResult<(String, String, i64, Vec<String>)>
where
    F: Fn(&T) -> &str,
{
    let mut valid: Vec<String> = Vec::new();
    let mut skipped = 0i64;
    for r in rows {
        match extract_date(extract(r)) {
            Some(d) => valid.push(d),
            None => skipped += 1,
        }
    }
    if valid.is_empty() {
        return Err(CmdError::msg(format!(
            "File rỗng hoặc không có {label} hợp lệ"
        )));
    }
    valid.sort();
    let from = valid[0].clone();
    let to = valid[valid.len() - 1].clone();
    let mut distinct = valid.clone();
    distinct.dedup();
    Ok((from, to, skipped, distinct))
}

// ============================================================
// Shopee clicks
// ============================================================

#[tauri::command]
pub fn preview_import_shopee_clicks(
    state: State<'_, DbState>,
    payload: ImportShopeeClicksPayload,
) -> CmdResult<ImportPreview> {
    let (day_date_from, day_date_to, skipped, distinct_dates) =
        derive_shopee_date_range(&payload.rows, "Thời gian Click", |r| &r.click_time)?;
    let hash = compute_hash(&payload.raw_content);

    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let (already_imported, existing_day) = check_hash_imported(&conn, &hash)?;

    // Nếu đã import → KHÔNG đếm replace (sẽ skip commit). Return shape tối giản.
    if already_imported {
        return Ok(ImportPreview {
            kind: "shopee_clicks".into(),
            filename: payload.filename,
            day_date: day_date_from.clone(),
            day_date_from,
            day_date_to,
            total_rows: payload.rows.len() as i64,
            new_rows: 0,
            replace_rows: 0,
            sample_replace: Vec::new(),
            day_has_data: any_day_has_data(&conn, &distinct_dates)?,
            already_imported: true,
            existing_day_date: existing_day,
            skipped,
        });
    }

    // Load existing click_ids ACROSS all days in file range (multi-day aware).
    let placeholders = std::iter::repeat("?")
        .take(distinct_dates.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT click_id FROM raw_shopee_clicks WHERE day_date IN ({placeholders})"
    );
    let params_vec: Vec<&dyn rusqlite::ToSql> =
        distinct_dates.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let mut existing: HashSet<String> = HashSet::new();
    {
        let mut stmt = conn.prepare(&sql)?;
        for row in stmt.query_map(params_vec.as_slice(), |r| r.get::<_, String>(0))? {
            existing.insert(row?);
        }
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
        day_date: day_date_from.clone(),
        day_date_from,
        day_date_to,
        total_rows: total,
        new_rows: total - replace_rows - skipped,
        replace_rows,
        sample_replace,
        day_has_data: any_day_has_data(&conn, &distinct_dates)?,
        already_imported: false,
        existing_day_date: None,
        skipped,
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
    let (day_date_from, day_date_to, skipped, distinct_dates) = derive_shopee_date_range(
        &payload.rows,
        "Thời Gian Đặt Hàng",
        |r| &r.order_time,
    )?;
    let hash = compute_hash(&payload.raw_content);

    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let (already_imported, existing_day) = check_hash_imported(&conn, &hash)?;

    if already_imported {
        return Ok(ImportPreview {
            kind: "shopee_commission".into(),
            filename: payload.filename,
            day_date: day_date_from.clone(),
            day_date_from,
            day_date_to,
            total_rows: payload.rows.len() as i64,
            new_rows: 0,
            replace_rows: 0,
            sample_replace: Vec::new(),
            day_has_data: any_day_has_data(&conn, &distinct_dates)?,
            already_imported: true,
            existing_day_date: existing_day,
            skipped,
        });
    }

    let placeholders = std::iter::repeat("?")
        .take(distinct_dates.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT checkout_id, item_id, model_id FROM raw_shopee_order_items
         WHERE day_date IN ({placeholders})"
    );
    let params_vec: Vec<&dyn rusqlite::ToSql> =
        distinct_dates.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let mut existing: HashSet<(String, String, String)> = HashSet::new();
    {
        let mut stmt = conn.prepare(&sql)?;
        for row in stmt.query_map(params_vec.as_slice(), |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })? {
            existing.insert(row?);
        }
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
        day_date: day_date_from.clone(),
        day_date_from,
        day_date_to,
        total_rows: total,
        new_rows: total - replace_rows - skipped,
        replace_rows,
        sample_replace,
        day_has_data: any_day_has_data(&conn, &distinct_dates)?,
        already_imported: false,
        existing_day_date: None,
        skipped,
    })
}

// ============================================================
// FB ad_groups — giữ single-date validation
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
    let (already_imported, existing_day) = check_hash_imported(&conn, &hash)?;

    if already_imported {
        return Ok(ImportPreview {
            kind: "fb_ad_group".into(),
            filename: payload.filename,
            day_date: day_date.clone(),
            day_date_from: day_date.clone(),
            day_date_to: day_date.clone(),
            total_rows: payload.rows.len() as i64,
            new_rows: 0,
            replace_rows: 0,
            sample_replace: Vec::new(),
            day_has_data: any_day_has_data(&conn, &[day_date])?,
            already_imported: true,
            existing_day_date: existing_day,
            skipped: 0,
        });
    }

    let mut existing: HashSet<String> = HashSet::new();
    {
        let mut stmt = conn.prepare(
            "SELECT name FROM raw_fb_ads WHERE day_date = ? AND level = 'ad_group'",
        )?;
        for row in stmt.query_map(params![day_date], |r| r.get::<_, String>(0))? {
            existing.insert(row?);
        }
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
        day_date_from: day_date.clone(),
        day_date_to: day_date.clone(),
        total_rows: total,
        new_rows: total - replace_rows,
        replace_rows,
        sample_replace,
        day_has_data: any_day_has_data(&conn, &[day_date])?,
        already_imported: false,
        existing_day_date: None,
        skipped: 0,
    })
}

// ============================================================
// FB campaigns — giữ single-date validation
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
    let (already_imported, existing_day) = check_hash_imported(&conn, &hash)?;

    if already_imported {
        return Ok(ImportPreview {
            kind: "fb_campaign".into(),
            filename: payload.filename,
            day_date: day_date.clone(),
            day_date_from: day_date.clone(),
            day_date_to: day_date.clone(),
            total_rows: payload.rows.len() as i64,
            new_rows: 0,
            replace_rows: 0,
            sample_replace: Vec::new(),
            day_has_data: any_day_has_data(&conn, &[day_date])?,
            already_imported: true,
            existing_day_date: existing_day,
            skipped: 0,
        });
    }

    let mut existing: HashSet<String> = HashSet::new();
    {
        let mut stmt = conn.prepare(
            "SELECT name FROM raw_fb_ads WHERE day_date = ? AND level = 'campaign'",
        )?;
        for row in stmt.query_map(params![day_date], |r| r.get::<_, String>(0))? {
            existing.insert(row?);
        }
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
        day_date_from: day_date.clone(),
        day_date_to: day_date.clone(),
        total_rows: total,
        new_rows: total - replace_rows,
        replace_rows,
        sample_replace,
        day_has_data: any_day_has_data(&conn, &[day_date])?,
        already_imported: false,
        existing_day_date: None,
        skipped: 0,
    })
}
