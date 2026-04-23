//! Commands import CSV vào raw tables.
//!
//! TS bên frontend parse CSV (papaparse) → gửi structured rows + raw file content
//! sang Rust qua các command này. Rust:
//!   1. Compute SHA-256 hash của raw_content → chặn trùng qua `imported_files.file_hash`.
//!   2. Extract date field theo kind → validate toàn file chỉ 1 ngày duy nhất.
//!   3. Trong 1 transaction: upsert `days`, INSERT `imported_files`, copy raw CSV
//!      vào `imports/<hash>.csv`, INSERT từng raw row với `day_date` FK.
//!   4. Rollback nếu bất kỳ bước nào fail.
//!
//! Dedup behavior:
//! - `raw_shopee_clicks` PK = click_id → ON CONFLICT DO NOTHING (click natural unique).
//! - `raw_shopee_order_items` UNIQUE(checkout_id, item_id, model_id) → DO UPDATE
//!   (status có thể đổi Đang chờ → Đã hoàn thành, cập nhật field mới nhất).
//! - `raw_fb_*` UNIQUE(source_file_id, name) — file mới sẽ tạo row mới tự nhiên.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::State;

use crate::db::{resolve_active_imports_dir, DbState};

use super::{CmdError, CmdResult};

// ============================================================
// Shared helpers
// ============================================================

/// SHA-256 hex (64 chars) của raw CSV content.
fn compute_hash(content: &str) -> String {
    let digest = Sha256::digest(content.as_bytes());
    format!("{:x}", digest)
}

/// Extract `YYYY-MM-DD` từ chuỗi datetime dạng `"YYYY-MM-DD HH:MM:SS"` hoặc
/// `"YYYY-MM-DDTHH:MM:SS..."`. Trả None nếu không match format cơ bản.
pub(super) fn extract_date(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.len() < 10 {
        return None;
    }
    let head = &trimmed[..10];
    let bytes = head.as_bytes();
    let is_date = bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[8..10].iter().all(u8::is_ascii_digit);
    if is_date {
        Some(head.to_string())
    } else {
        None
    }
}

/// Validate tất cả date đầu vào phải cùng 1 ngày, trả ra date đó.
/// `label` đi vào error message để user biết field nào gây lỗi.
pub(super) fn validate_single_date(dates: Vec<String>, label: &str) -> CmdResult<String> {
    if dates.is_empty() {
        return Err(CmdError::msg(format!(
            "File rỗng hoặc không có {label} hợp lệ"
        )));
    }
    let mut unique: Vec<String> = dates;
    unique.sort();
    unique.dedup();
    if unique.len() == 1 {
        Ok(unique.remove(0))
    } else {
        let shown: Vec<String> = unique.iter().take(5).cloned().collect();
        let mut msg = format!(
            "File chứa nhiều ngày ({}): {}",
            label,
            shown.join(", ")
        );
        if unique.len() > 5 {
            msg.push_str(&format!(" ... (+{} ngày khác)", unique.len() - 5));
        }
        msg.push_str(". Vui lòng xuất từng ngày riêng lẻ từ Shopee/FB.");
        Err(CmdError::msg(msg))
    }
}

/// Ghi raw CSV content ra `<imports_dir>/<hash>.csv`. `imports_dir` thuộc
/// user folder hiện tại (resolve từ DB path qua `resolve_active_imports_dir`).
/// Trả relative path "imports/<hash>.csv" để lưu vào `imported_files.stored_path`.
fn save_raw_csv(imports_dir: &Path, hash: &str, content: &str) -> CmdResult<String> {
    let file_path: PathBuf = imports_dir.join(format!("{hash}.csv"));
    if !file_path.exists() {
        fs::write(&file_path, content)?;
    }
    Ok(format!("imports/{hash}.csv"))
}

/// Kết quả trả về UI sau khi import thành công.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportResult {
    pub imported_file_id: i64,
    /// Earliest date in file — backward compat cho FE toast. Single-day file
    /// = day_date_from = day_date_to.
    pub day_date: String,
    pub day_date_from: String,
    pub day_date_to: String,
    pub row_count: i64,
    pub inserted: i64,
    pub duplicated: i64,
    /// Rows bị skip vì extract_date thất bại (Shopee multi-day only; FB validate
    /// single date nên 0). FE hiện warning nếu > 0.
    #[serde(default)]
    pub skipped: i64,
}

/// Kind hợp lệ trong bảng `imported_files.kind`.
const KIND_SHOPEE_CLICKS: &str = "shopee_clicks";
const KIND_SHOPEE_COMMISSION: &str = "shopee_commission";
const KIND_FB_AD_GROUP: &str = "fb_ad_group";
const KIND_FB_CAMPAIGN: &str = "fb_campaign";

/// Upsert `days` row + INSERT (hoặc reuse) `imported_files`.
/// Trả về `source_file_id` để caller dùng làm FK cho raw rows.
///
/// Hash match handling: nếu file_hash đã tồn tại → **reuse** entry cũ
/// (không error, không UPDATE metadata). Dùng cho backfill scenario:
/// user xóa 1 ngày từ multi-day file → re-import cùng file để lấy lại
/// data đã bị xóa. Raw rows được UPSERT với source_file_id = id cũ.
#[allow(clippy::too_many_arguments)]
fn register_imported_file(
    tx: &rusqlite::Transaction,
    filename: &str,
    kind: &str,
    now: &str,
    hash: &str,
    stored_path: &str,
    day_date: &str,
    row_count: i64,
) -> CmdResult<i64> {
    tx.execute(
        "INSERT OR IGNORE INTO days(date, created_at) VALUES(?, ?)",
        params![day_date, now],
    )?;

    // Resurrect: import mới cho day X → huỷ tombstone 'day' nếu có.
    // Giữ tombstones 'ui_row' / 'manual_entry' vì import raw không chạm manual_entries.
    tx.execute(
        "DELETE FROM tombstones WHERE entity_type = 'day' AND entity_key = ?",
        params![day_date],
    )?;

    let existing: Option<i64> = tx
        .query_row(
            "SELECT id FROM imported_files WHERE file_hash = ?",
            params![hash],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(id) = existing {
        return Ok(id);
    }

    tx.execute(
        "INSERT INTO imported_files
         (filename, kind, imported_at, row_count, file_hash, stored_path, day_date)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![filename, kind, now, row_count, hash, stored_path, day_date],
    )?;
    Ok(tx.last_insert_rowid())
}

// ============================================================
// Shopee Clicks (WebsiteClickReport)
// ============================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShopeeClickRow {
    pub click_id: String,
    pub click_time: String,
    pub region: Option<String>,
    pub sub_id_raw: Option<String>,
    pub sub_ids: [String; 5],
    pub referrer: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportShopeeClicksPayload {
    pub filename: String,
    pub raw_content: String,
    pub rows: Vec<ShopeeClickRow>,
    /// ID của `shopee_accounts` mà toàn bộ rows trong file này thuộc về.
    /// Optional để preview command (dùng cùng struct) không fail — FE gửi
    /// missing field lúc preview. Import command bắt buộc có → None = reject.
    #[serde(default)]
    pub shopee_account_id: Option<i64>,
}

/// Import Shopee WebsiteClickReport.csv — 1 row/click, PK = click_id.
#[tauri::command]
pub fn import_shopee_clicks(
    state: State<'_, DbState>,
    payload: ImportShopeeClicksPayload,
) -> CmdResult<ImportResult> {
    let shopee_account_id = payload.shopee_account_id.ok_or_else(|| {
        CmdError::msg("shopeeAccountId bắt buộc — chọn TK trong dialog import")
    })?;

    // Multi-day: mỗi row tự derive day_date từ click_time. Rows không parse
    // được date → skip (không insert). Collect date range cho imported_files.
    let rows_with_dates: Vec<(&ShopeeClickRow, Option<String>)> = payload
        .rows
        .iter()
        .map(|r| (r, extract_date(&r.click_time)))
        .collect();
    let valid_dates: Vec<&str> = rows_with_dates
        .iter()
        .filter_map(|(_, d)| d.as_deref())
        .collect();
    if valid_dates.is_empty() {
        return Err(CmdError::msg("File rỗng hoặc không có Thời gian Click hợp lệ"));
    }
    let (day_date_from, day_date_to) = {
        let mut sorted: Vec<&str> = valid_dates.clone();
        sorted.sort();
        (sorted[0].to_string(), sorted[sorted.len() - 1].to_string())
    };

    let hash = compute_hash(&payload.raw_content);
    let now = Utc::now().to_rfc3339();

    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let imports_dir = resolve_active_imports_dir(&conn)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    let stored_path = save_raw_csv(&imports_dir, &hash, &payload.raw_content)?;
    let tx = conn.transaction()?;

    // Auto-insert days entries cho mọi date phân biệt trong file. Cần vì
    // raw_shopee_clicks.day_date vẫn FK tới days(date).
    {
        let mut distinct: Vec<&str> = valid_dates.clone();
        distinct.sort();
        distinct.dedup();
        let mut day_stmt =
            tx.prepare("INSERT OR IGNORE INTO days(date, created_at) VALUES(?, ?)")?;
        for d in &distinct {
            day_stmt.execute(params![d, now])?;
        }
    }

    let source_file_id = register_imported_file(
        &tx,
        &payload.filename,
        KIND_SHOPEE_CLICKS,
        &now,
        &hash,
        &stored_path,
        &day_date_from,
        payload.rows.len() as i64,
    )?;

    let mut inserted: i64 = 0;
    let mut duplicated: i64 = 0;
    let mut skipped: i64 = 0;
    {
        let mut stmt = tx.prepare(
            "INSERT OR IGNORE INTO raw_shopee_clicks
             (click_id, click_time, region, sub_id_raw,
              sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
              referrer, day_date, source_file_id, shopee_account_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )?;
        for (r, date_opt) in &rows_with_dates {
            let Some(day_date) = date_opt else {
                skipped += 1;
                continue;
            };
            let changes = stmt.execute(params![
                r.click_id,
                r.click_time,
                r.region,
                r.sub_id_raw,
                r.sub_ids[0],
                r.sub_ids[1],
                r.sub_ids[2],
                r.sub_ids[3],
                r.sub_ids[4],
                r.referrer,
                day_date,
                source_file_id,
                shopee_account_id,
            ])?;
            if changes > 0 {
                inserted += 1;
            } else {
                duplicated += 1;
            }
        }
    }

    tx.commit()?;

    Ok(ImportResult {
        imported_file_id: source_file_id,
        day_date: day_date_from.clone(),
        day_date_from,
        day_date_to,
        row_count: payload.rows.len() as i64,
        inserted,
        duplicated,
        skipped,
    })
}

// ============================================================
// Shopee Orders (AffiliateCommissionReport)
// ============================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShopeeOrderRow {
    pub order_id: String,
    pub checkout_id: String,
    pub item_id: String,
    #[serde(default)]
    pub model_id: String,
    pub order_status: Option<String>,
    pub order_time: String,
    pub completed_time: Option<String>,
    pub click_time: Option<String>,
    pub shop_id: Option<String>,
    pub shop_name: Option<String>,
    pub shop_type: Option<String>,
    pub item_name: Option<String>,
    pub category_l1: Option<String>,
    pub category_l2: Option<String>,
    pub category_l3: Option<String>,
    pub price: Option<f64>,
    pub quantity: Option<i64>,
    pub order_value: Option<f64>,
    pub refund_amount: Option<f64>,
    pub net_commission: Option<f64>,
    pub commission_total: Option<f64>,
    /// CSV col 31 "Tổng hoa hồng đơn hàng(₫)" — pre-MCN.
    pub order_commission_total: Option<f64>,
    /// CSV col 35 "Phí quản lý MCN(₫)" — Shopee cắt trước payout.
    pub mcn_fee: Option<f64>,
    pub sub_ids: [String; 5],
    pub channel: Option<String>,
    // raw_json removed v9 — CSV gốc lưu imports/<hash>.csv. FE vẫn có thể
    // send `rawJson` trong JSON payload, serde ignore field không biết.
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportShopeeOrdersPayload {
    pub filename: String,
    pub raw_content: String,
    pub rows: Vec<ShopeeOrderRow>,
    /// ID của `shopee_accounts`. Optional cho preview command share struct.
    /// Import bắt buộc có. Nếu row đã tồn tại (UPSERT theo checkout_id + item_id
    /// + model_id), account_id **cập nhật** theo file mới — ưu tiên intention mới.
    #[serde(default)]
    pub shopee_account_id: Option<i64>,
}

/// Import Shopee AffiliateCommissionReport.csv — 1 row/item trong order.
/// Dedup UNIQUE(checkout_id, item_id, model_id) → ON CONFLICT DO UPDATE (status/price/commission mới nhất).
#[tauri::command]
pub fn import_shopee_orders(
    state: State<'_, DbState>,
    payload: ImportShopeeOrdersPayload,
) -> CmdResult<ImportResult> {
    let shopee_account_id = payload.shopee_account_id.ok_or_else(|| {
        CmdError::msg("shopeeAccountId bắt buộc — chọn TK trong dialog import")
    })?;

    // Multi-day: mỗi row tự derive day_date từ order_time. Commission report
    // thường chứa đơn nhiều ngày (status update đơn cũ sau 10-30 ngày).
    let rows_with_dates: Vec<(&ShopeeOrderRow, Option<String>)> = payload
        .rows
        .iter()
        .map(|r| (r, extract_date(&r.order_time)))
        .collect();
    let valid_dates: Vec<&str> = rows_with_dates
        .iter()
        .filter_map(|(_, d)| d.as_deref())
        .collect();
    if valid_dates.is_empty() {
        return Err(CmdError::msg("File rỗng hoặc không có Thời Gian Đặt Hàng hợp lệ"));
    }
    let (day_date_from, day_date_to) = {
        let mut sorted: Vec<&str> = valid_dates.clone();
        sorted.sort();
        (sorted[0].to_string(), sorted[sorted.len() - 1].to_string())
    };

    let hash = compute_hash(&payload.raw_content);
    let now = Utc::now().to_rfc3339();

    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let imports_dir = resolve_active_imports_dir(&conn)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    let stored_path = save_raw_csv(&imports_dir, &hash, &payload.raw_content)?;
    let tx = conn.transaction()?;

    // Auto-insert days entries cho mọi date phân biệt trong file.
    {
        let mut distinct: Vec<&str> = valid_dates.clone();
        distinct.sort();
        distinct.dedup();
        let mut day_stmt =
            tx.prepare("INSERT OR IGNORE INTO days(date, created_at) VALUES(?, ?)")?;
        for d in &distinct {
            day_stmt.execute(params![d, now])?;
        }
    }

    let source_file_id = register_imported_file(
        &tx,
        &payload.filename,
        KIND_SHOPEE_COMMISSION,
        &now,
        &hash,
        &stored_path,
        &day_date_from,
        payload.rows.len() as i64,
    )?;

    // UPSERT: ON CONFLICT DO UPDATE cập nhật trạng thái + field mới nhất.
    let mut inserted: i64 = 0;
    let mut updated: i64 = 0;
    let mut skipped: i64 = 0;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO raw_shopee_order_items
             (order_id, checkout_id, item_id, model_id, order_status,
              order_time, completed_time, click_time,
              shop_id, shop_name, shop_type, item_name,
              category_l1, category_l2, category_l3,
              price, quantity, order_value, refund_amount,
              net_commission, commission_total, order_commission_total, mcn_fee,
              sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
              channel, day_date, source_file_id, shopee_account_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(checkout_id, item_id, model_id) DO UPDATE SET
                order_status   = excluded.order_status,
                order_time     = excluded.order_time,
                completed_time = excluded.completed_time,
                click_time     = excluded.click_time,
                price          = excluded.price,
                quantity       = excluded.quantity,
                order_value    = excluded.order_value,
                refund_amount  = excluded.refund_amount,
                net_commission = excluded.net_commission,
                commission_total = excluded.commission_total,
                order_commission_total = excluded.order_commission_total,
                mcn_fee        = excluded.mcn_fee,
                source_file_id = excluded.source_file_id,
                shopee_account_id = excluded.shopee_account_id,
                day_date       = excluded.day_date",
        )?;

        // Validation: Shopee spec `net_commission = order_commission_total − mcn_fee`.
        // Chỉ verify khi cả 3 field có giá trị. Tolerance 0.5đ cho rounding.
        // Lệch quá → log warning (không fail import — data vẫn ghi được, user
        // tự quyết định có refresh export lại hay không).
        let mut mcn_mismatch_count: i64 = 0;
        for (r, date_opt) in &rows_with_dates {
            let Some(day_date) = date_opt else {
                skipped += 1;
                continue;
            };
            let before: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM raw_shopee_order_items
                     WHERE checkout_id = ? AND item_id = ? AND model_id = ?",
                    params![r.checkout_id, r.item_id, r.model_id],
                    |row| row.get(0),
                )
                .unwrap_or(0);

            stmt.execute(params![
                r.order_id,
                r.checkout_id,
                r.item_id,
                r.model_id,
                r.order_status,
                r.order_time,
                r.completed_time,
                r.click_time,
                r.shop_id,
                r.shop_name,
                r.shop_type,
                r.item_name,
                r.category_l1,
                r.category_l2,
                r.category_l3,
                r.price,
                r.quantity,
                r.order_value,
                r.refund_amount,
                r.net_commission,
                r.commission_total,
                r.order_commission_total,
                r.mcn_fee,
                r.sub_ids[0],
                r.sub_ids[1],
                r.sub_ids[2],
                r.sub_ids[3],
                r.sub_ids[4],
                r.channel,
                day_date,
                source_file_id,
                shopee_account_id,
            ])?;

            if let (Some(net), Some(pre), Some(fee)) =
                (r.net_commission, r.order_commission_total, r.mcn_fee)
            {
                if (net - (pre - fee)).abs() > 0.5 {
                    mcn_mismatch_count += 1;
                    if mcn_mismatch_count <= 5 {
                        eprintln!(
                            "[shopee_commission] MCN mismatch order={} checkout={} item={}: \
                             net={} order_total={} mcn_fee={} expected={}",
                            r.order_id,
                            r.checkout_id,
                            r.item_id,
                            net,
                            pre,
                            fee,
                            pre - fee,
                        );
                    }
                }
            }

            if before == 0 {
                inserted += 1;
            } else {
                updated += 1;
            }
        }
        if mcn_mismatch_count > 5 {
            eprintln!(
                "[shopee_commission] + {} more MCN mismatches (suppressed)",
                mcn_mismatch_count - 5
            );
        }
    }

    tx.commit()?;

    Ok(ImportResult {
        imported_file_id: source_file_id,
        day_date: day_date_from.clone(),
        day_date_from,
        day_date_to,
        row_count: payload.rows.len() as i64,
        inserted,
        duplicated: updated, // ở đây = số row bị overwrite
        skipped,
    })
}

// ============================================================
// FB Ad Groups
// ============================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct FbAdGroupRow {
    pub ad_group_name: String,
    pub sub_ids: [String; 5],
    pub report_start: String,
    pub report_end: String,
    pub status: Option<String>,
    pub spend: Option<f64>,
    pub impressions: Option<i64>,
    pub reach: Option<i64>,
    pub frequency: Option<f64>,
    pub link_clicks: Option<i64>,
    pub shop_clicks: Option<i64>,
    pub all_clicks: Option<i64>,
    pub link_cpc: Option<f64>,
    pub all_cpc: Option<f64>,
    pub link_ctr: Option<f64>,
    pub all_ctr: Option<f64>,
    pub landing_views: Option<i64>,
    pub cpm: Option<f64>,
    pub result_count: Option<i64>,
    pub cost_per_result: Option<f64>,
    // raw_json removed v9 — CSV gốc lưu imports/<hash>.csv.
    // Các field unused (frequency/shop_clicks/ctr/cpm/landing_views) giữ lại để
    // FE parse CSV đầy đủ + future-proof khi muốn persist thêm metric.
}

/// Normalize click count: ưu tiên link_clicks → all_clicks → result_count.
/// Dùng cho cả ad_group lẫn campaign lúc INSERT vào raw_fb_ads.
fn normalize_clicks(
    link: Option<i64>,
    all: Option<i64>,
    result: Option<i64>,
) -> Option<i64> {
    link.or(all).or(result)
}

/// Normalize CPC: ưu tiên link_cpc → all_cpc → cost_per_result (FB-reported).
fn normalize_cpc(
    link: Option<f64>,
    all: Option<f64>,
    cost_per_result: Option<f64>,
) -> Option<f64> {
    link.or(all).or(cost_per_result)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportFbAdGroupsPayload {
    pub filename: String,
    pub raw_content: String,
    pub rows: Vec<FbAdGroupRow>,
}

/// Validate FB: report_start == report_end và toàn bộ row cùng ngày.
pub(super) fn validate_fb_single_date(
    rows: impl IntoIterator<Item = (String, String)>,
    name_for_error: &str,
) -> CmdResult<String> {
    let mut dates = Vec::new();
    for (start, end) in rows {
        let s = extract_date(&start);
        let e = extract_date(&end);
        match (s, e) {
            (Some(sd), Some(ed)) if sd == ed => dates.push(sd),
            (Some(sd), Some(ed)) => {
                return Err(CmdError::msg(format!(
                    "{name_for_error}: có row report_start ({sd}) khác report_end ({ed}). \
                     Vui lòng xuất báo cáo theo từng ngày riêng (report_start = report_end)."
                )));
            }
            _ => {
                return Err(CmdError::msg(format!(
                    "{name_for_error}: có row thiếu hoặc sai định dạng ngày báo cáo"
                )));
            }
        }
    }
    validate_single_date(dates, name_for_error)
}

#[tauri::command]
pub fn import_fb_ad_groups(
    state: State<'_, DbState>,
    payload: ImportFbAdGroupsPayload,
) -> CmdResult<ImportResult> {
    let day_date = validate_fb_single_date(
        payload
            .rows
            .iter()
            .map(|r| (r.report_start.clone(), r.report_end.clone())),
        "FB Ad Group",
    )?;

    let hash = compute_hash(&payload.raw_content);
    let now = Utc::now().to_rfc3339();

    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let imports_dir = resolve_active_imports_dir(&conn)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    let stored_path = save_raw_csv(&imports_dir, &hash, &payload.raw_content)?;
    let tx = conn.transaction()?;

    let source_file_id = register_imported_file(
        &tx,
        &payload.filename,
        KIND_FB_AD_GROUP,
        &now,
        &hash,
        &stored_path,
        &day_date,
        payload.rows.len() as i64,
    )?;

    let mut inserted: i64 = 0;
    let mut duplicated: i64 = 0;
    {
        let mut stmt = tx.prepare(FB_ADS_UPSERT_SQL)?;
        for r in &payload.rows {
            let before: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM raw_fb_ads
                     WHERE day_date = ? AND level = 'ad_group' AND name = ?",
                    params![day_date, r.ad_group_name],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            let clicks = normalize_clicks(r.link_clicks, r.all_clicks, r.result_count);
            let cpc = normalize_cpc(r.link_cpc, r.all_cpc, r.cost_per_result);
            stmt.execute(params![
                "ad_group",
                r.ad_group_name,
                r.sub_ids[0],
                r.sub_ids[1],
                r.sub_ids[2],
                r.sub_ids[3],
                r.sub_ids[4],
                r.report_start,
                r.report_end,
                r.status,
                r.spend,
                clicks,
                cpc,
                r.impressions,
                r.reach,
                day_date,
                source_file_id,
            ])?;
            if before == 0 {
                inserted += 1;
            } else {
                duplicated += 1;
            }
        }
    }

    tx.commit()?;

    Ok(ImportResult {
        imported_file_id: source_file_id,
        day_date: day_date.clone(),
        day_date_from: day_date.clone(),
        day_date_to: day_date,
        row_count: payload.rows.len() as i64,
        inserted,
        duplicated,
        skipped: 0,
    })
}

/// UPSERT template shared giữa `import_fb_ad_groups` và `import_fb_campaigns`.
/// ON CONFLICT theo `(day_date, level, name)` — 2 level cùng name không đụng nhau.
const FB_ADS_UPSERT_SQL: &str = "
    INSERT INTO raw_fb_ads
    (level, name,
     sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
     report_start, report_end, status,
     spend, clicks, cpc, impressions, reach,
     day_date, source_file_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(day_date, level, name) DO UPDATE SET
       sub_id1 = excluded.sub_id1, sub_id2 = excluded.sub_id2,
       sub_id3 = excluded.sub_id3, sub_id4 = excluded.sub_id4,
       sub_id5 = excluded.sub_id5,
       report_start = excluded.report_start, report_end = excluded.report_end,
       status = excluded.status,
       spend = excluded.spend, clicks = excluded.clicks, cpc = excluded.cpc,
       impressions = excluded.impressions, reach = excluded.reach,
       source_file_id = excluded.source_file_id
";

// ============================================================
// FB Campaigns
// ============================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct FbCampaignRow {
    pub campaign_name: String,
    pub sub_ids: [String; 5],
    pub report_start: String,
    pub report_end: String,
    pub status: Option<String>,
    pub spend: Option<f64>,
    pub impressions: Option<i64>,
    pub reach: Option<i64>,
    pub result_count: Option<i64>,
    pub result_indicator: Option<String>,
    pub link_clicks: Option<i64>,
    pub all_clicks: Option<i64>,
    pub link_cpc: Option<f64>,
    pub all_cpc: Option<f64>,
    pub cost_per_result: Option<f64>,
    // raw_json removed v9 — CSV gốc lưu imports/<hash>.csv.
    // result_indicator unused: FE parse CSV cột "Chỉ báo kết quả" nhưng BE chưa persist.
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportFbCampaignsPayload {
    pub filename: String,
    pub raw_content: String,
    pub rows: Vec<FbCampaignRow>,
}

#[tauri::command]
pub fn import_fb_campaigns(
    state: State<'_, DbState>,
    payload: ImportFbCampaignsPayload,
) -> CmdResult<ImportResult> {
    let day_date = validate_fb_single_date(
        payload
            .rows
            .iter()
            .map(|r| (r.report_start.clone(), r.report_end.clone())),
        "FB Campaign",
    )?;

    let hash = compute_hash(&payload.raw_content);
    let now = Utc::now().to_rfc3339();

    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let imports_dir = resolve_active_imports_dir(&conn)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    let stored_path = save_raw_csv(&imports_dir, &hash, &payload.raw_content)?;
    let tx = conn.transaction()?;

    let source_file_id = register_imported_file(
        &tx,
        &payload.filename,
        KIND_FB_CAMPAIGN,
        &now,
        &hash,
        &stored_path,
        &day_date,
        payload.rows.len() as i64,
    )?;

    let mut inserted: i64 = 0;
    let mut duplicated: i64 = 0;
    {
        let mut stmt = tx.prepare(FB_ADS_UPSERT_SQL)?;
        for r in &payload.rows {
            let before: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM raw_fb_ads
                     WHERE day_date = ? AND level = 'campaign' AND name = ?",
                    params![day_date, r.campaign_name],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            let clicks = normalize_clicks(r.link_clicks, r.all_clicks, r.result_count);
            let cpc = normalize_cpc(r.link_cpc, r.all_cpc, r.cost_per_result);
            stmt.execute(params![
                "campaign",
                r.campaign_name,
                r.sub_ids[0],
                r.sub_ids[1],
                r.sub_ids[2],
                r.sub_ids[3],
                r.sub_ids[4],
                r.report_start,
                r.report_end,
                r.status,
                r.spend,
                clicks,
                cpc,
                r.impressions,
                r.reach,
                day_date,
                source_file_id,
            ])?;
            if before == 0 {
                inserted += 1;
            } else {
                duplicated += 1;
            }
        }
    }

    tx.commit()?;

    Ok(ImportResult {
        imported_file_id: source_file_id,
        day_date: day_date.clone(),
        day_date_from: day_date.clone(),
        day_date_to: day_date,
        row_count: payload.rows.len() as i64,
        inserted,
        duplicated,
        skipped: 0,
    })
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_date_ok() {
        assert_eq!(
            extract_date("2026-04-17 23:59:54").as_deref(),
            Some("2026-04-17")
        );
        assert_eq!(
            extract_date("2026-04-17T10:00:00Z").as_deref(),
            Some("2026-04-17")
        );
        assert_eq!(extract_date("invalid"), None);
        assert_eq!(extract_date(""), None);
    }

    #[test]
    fn validate_single_date_ok() {
        let dates = vec!["2026-04-17".to_string(); 5];
        assert_eq!(validate_single_date(dates, "test").unwrap(), "2026-04-17");
    }

    #[test]
    fn validate_single_date_rejects_mixed() {
        let dates = vec!["2026-04-17".to_string(), "2026-04-18".to_string()];
        let err = validate_single_date(dates, "test").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("2026-04-17"), "msg: {msg}");
        assert!(msg.contains("2026-04-18"), "msg: {msg}");
    }

    #[test]
    fn validate_single_date_empty_rejects() {
        assert!(validate_single_date(vec![], "test").is_err());
    }

    #[test]
    fn hash_stable() {
        let a = compute_hash("abc");
        let b = compute_hash("abc");
        let c = compute_hash("abd");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 64);
    }
}
