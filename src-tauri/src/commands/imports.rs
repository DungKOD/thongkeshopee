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

use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::State;

use crate::db::{resolve_active_imports_dir, DbState};

use super::{assert_not_bootstrapping, CmdError, CmdResult};

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
    /// Shopee commission only: số row có `net_commission ≠ order_commission_total
    /// - mcn_fee` (tolerance 0.5đ). 0 cho các kind khác. FE hiện warning banner
    /// nếu > 0 để user biết export có thể lỗi, không silent trong stderr.
    #[serde(default)]
    pub mcn_mismatch_count: i64,
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
    shopee_account_id: Option<i64>,
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

    // v10: dedup chỉ check file ACTIVE (chưa revert). Row `reverted_at IS NOT NULL`
    // giữ cho history — user revert xong import lại cùng file → revive row đó.
    let existing: Option<i64> = tx
        .query_row(
            "SELECT id FROM imported_files WHERE file_hash = ? AND reverted_at IS NULL",
            params![hash],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(id) = existing {
        return Ok(id);
    }

    // v0.6.2 fix — revive reverted file (regression v0.6.1):
    // v13 dùng `id = content_id(hash)` deterministic, kết hợp với v0.6.1
    // chuyển revert/orphan-cleanup sang SOFT-MARK (UPDATE reverted_at thay
    // vì DELETE) → row cũ vẫn ở table với cùng id. INSERT mới sẽ UNIQUE PK
    // collision (`UNIQUE constraint failed: imported_files.id`).
    //
    // Path xảy ra:
    // 1. `revert_import` user-triggered (đã có trước v0.6.1).
    // 2. `batch_commit_deletes` xóa toàn bộ day → orphan file soft-mark
    //    (v0.6.1 fix).
    //
    // Fix: tìm row cùng hash đã reverted → UPDATE reverted_at=NULL +
    // refresh metadata (filename, stored_path, day_date, account, ...) +
    // clear local tombstone 'imported_file' (entity_key=id) để snapshot
    // replay không wipe lại.
    //
    // Edge case: 2 file khác hash trùng id (collision content_id 63-bit) —
    // xác suất gần 0 với SHA-256 input 8 byte; nếu xảy ra UNIQUE PK fail
    // sẽ surface qua error message bình thường.
    let id = crate::sync_v9::content_id::imported_file_id(hash);
    let reverted_id: Option<i64> = tx
        .query_row(
            "SELECT id FROM imported_files
             WHERE file_hash = ? AND reverted_at IS NOT NULL",
            params![hash],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(prev_id) = reverted_id {
        // Sanity check: id từ content_id(hash) phải match row cũ. Nếu khác
        // (legacy row pre-v13 với autoincrement id) → vẫn revive bằng
        // prev_id để tránh PK collision; raw rows mới sẽ FK về prev_id.
        let target_id = if prev_id == id { id } else { prev_id };
        tx.execute(
            "UPDATE imported_files
             SET reverted_at = NULL,
                 filename = ?,
                 kind = ?,
                 imported_at = ?,
                 row_count = ?,
                 stored_path = ?,
                 day_date = ?,
                 shopee_account_id = ?
             WHERE id = ?",
            params![
                filename, kind, now, row_count, stored_path, day_date,
                shopee_account_id, target_id
            ],
        )?;
        // Clear stale local tombstone — capture cursor `deleted_at` đã advance
        // qua tombstone cũ nên không re-push, nhưng dọn để snapshot replay
        // sau wipe + bootstrap không apply nhầm tombstone lên row vừa revive.
        tx.execute(
            "DELETE FROM tombstones
             WHERE entity_type = 'imported_file' AND entity_key = ?",
            params![target_id.to_string()],
        )?;
        return Ok(target_id);
    }

    // v13: id = content_id(file_hash) — deterministic cross-machine.
    tx.execute(
        "INSERT INTO imported_files
         (id, filename, kind, imported_at, row_count, file_hash, stored_path, day_date, shopee_account_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![id, filename, kind, now, row_count, hash, stored_path, day_date, shopee_account_id],
    )?;
    Ok(id)
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
    /// FE gửi string (content_id hash > 2^53).
    #[serde(default, deserialize_with = "deser_opt_i64_flexible")]
    pub shopee_account_id: Option<i64>,
}

fn deser_opt_i64_flexible<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Any { Num(i64), Str(String) }
    let opt: Option<Any> = Option::deserialize(d)?;
    match opt {
        None => Ok(None),
        Some(Any::Num(n)) => Ok(Some(n)),
        Some(Any::Str(s)) if s.is_empty() => Ok(None),
        Some(Any::Str(s)) => s.parse::<i64>().map(Some).map_err(serde::de::Error::custom),
    }
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
    // Bug F fix: chuẩn hoá `Z` format. Cursor column `imported_at` của
    // imported_files được sync_v9 capture/compare lex — mix `+00:00` với
    // `Z` (từ schema seed / accounts.rs) → lex sai → row có suffix `+`
    // bị xếp trước row có `Z` regardless of timestamp → cursor advance
    // bypass row → next sync skip.
    let now = crate::sync_v9::hlc::now_rfc3339_z();

    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    // Bug E fix: reject mutation trong bootstrap snapshot restore window.
    // Bypass guard → INSERT vào DB sắp bị swap → mất data sau swap.
    assert_not_bootstrapping(&conn)?;
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
        Some(shopee_account_id),
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
        // v10: mapping raw↔file. INSERT OR IGNORE mọi row (insert hoặc dup)
        // vì file X đều đã "touch" click đó — revert phải biết để giữ khi còn file Y.
        let mut map_stmt = tx.prepare(
            "INSERT OR IGNORE INTO clicks_to_file(click_id, file_id) VALUES(?, ?)",
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
            map_stmt.execute(params![r.click_id, source_file_id])?;
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
        mcn_mismatch_count: 0,
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
    /// FE gửi string (content_id hash > 2^53).
    #[serde(default, deserialize_with = "deser_opt_i64_flexible")]
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
    // Bug F fix: chuẩn hoá `Z` format. Cursor column `imported_at` của
    // imported_files được sync_v9 capture/compare lex — mix `+00:00` với
    // `Z` (từ schema seed / accounts.rs) → lex sai → row có suffix `+`
    // bị xếp trước row có `Z` regardless of timestamp → cursor advance
    // bypass row → next sync skip.
    let now = crate::sync_v9::hlc::now_rfc3339_z();

    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    // Bug E fix: reject mutation trong bootstrap snapshot restore window.
    // Bypass guard → INSERT vào DB sắp bị swap → mất data sau swap.
    assert_not_bootstrapping(&conn)?;
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
        Some(shopee_account_id),
    )?;

    // UPSERT: ON CONFLICT DO UPDATE cập nhật trạng thái + field mới nhất.
    // v10: RETURNING id để mapping orders_to_file biết row_id của UPSERTED row
    // (SQLite 3.35+ support trong cả DO UPDATE branch).
    let mut inserted: i64 = 0;
    let mut updated: i64 = 0;
    let mut skipped: i64 = 0;
    // Declare ngoài block để return vào ImportResult sau tx.commit.
    let mut mcn_mismatch_count: i64 = 0;
    {
        // v13: id = content_id(checkout_id, item_id, model_id). Cross-machine
        // deterministic — cùng order line trên A và B → cùng id → FK ổn định.
        let mut stmt = tx.prepare(
            "INSERT INTO raw_shopee_order_items
             (id, order_id, checkout_id, item_id, model_id, order_status,
              order_time, completed_time, click_time,
              shop_id, shop_name, shop_type, item_name,
              category_l1, category_l2, category_l3,
              price, quantity, order_value, refund_amount,
              net_commission, commission_total, order_commission_total, mcn_fee,
              sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
              channel, day_date, source_file_id, shopee_account_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                day_date       = excluded.day_date
             RETURNING id",
        )?;
        let mut map_stmt = tx.prepare(
            "INSERT OR IGNORE INTO orders_to_file(order_item_id, file_id) VALUES(?, ?)",
        )?;

        // Validation: Shopee spec `net_commission = order_commission_total − mcn_fee`.
        // Chỉ verify khi cả 3 field có giá trị. Tolerance 0.5đ cho rounding.
        // Lệch quá → log warning (không fail import — data vẫn ghi được, user
        // tự quyết định có refresh export lại hay không).
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

            let content_id = crate::sync_v9::content_id::order_item_id(
                &r.checkout_id,
                &r.item_id,
                &r.model_id,
            );
            let order_item_id: i64 = stmt.query_row(
                params![
                    content_id,
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
                ],
                |row| row.get(0),
            )?;
            map_stmt.execute(params![order_item_id, source_file_id])?;

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
        mcn_mismatch_count,
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

/// Validate tax_rate range: 0..=100. NaN/Infinity reject. Negative reject.
/// Trả về giá trị hợp lệ hoặc CmdError. UI cũng có client-side check nhưng BE
/// vẫn validate để chống payload nhân tạo / API call trực tiếp gửi sai.
pub(super) fn validate_tax_rate(rate: f64) -> CmdResult<f64> {
    if !rate.is_finite() {
        return Err(CmdError::msg(
            "tax_rate không hợp lệ (NaN/Infinity)",
        ));
    }
    if !(0.0..=100.0).contains(&rate) {
        return Err(CmdError::msg(format!(
            "tax_rate phải trong khoảng 0..100, nhận {rate}"
        )));
    }
    Ok(rate)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportFbAdGroupsPayload {
    pub filename: String,
    pub raw_content: String,
    pub rows: Vec<FbAdGroupRow>,
    /// Thuế % gắn cho cả file (TK FB chịu VAT/business tax). 0..=100, 2 decimal.
    /// Áp cho tất cả rows trong file. Default 0 nếu FE không gửi (backward compat).
    #[serde(default)]
    pub tax_rate: f64,
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
    let tax_rate = validate_tax_rate(payload.tax_rate)?;

    let hash = compute_hash(&payload.raw_content);
    // Bug F fix: chuẩn hoá `Z` format. Cursor column `imported_at` của
    // imported_files được sync_v9 capture/compare lex — mix `+00:00` với
    // `Z` (từ schema seed / accounts.rs) → lex sai → row có suffix `+`
    // bị xếp trước row có `Z` regardless of timestamp → cursor advance
    // bypass row → next sync skip.
    let now = crate::sync_v9::hlc::now_rfc3339_z();

    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    // Bug E fix: reject mutation trong bootstrap snapshot restore window.
    // Bypass guard → INSERT vào DB sắp bị swap → mất data sau swap.
    assert_not_bootstrapping(&conn)?;
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
        None,
    )?;

    let mut inserted: i64 = 0;
    let mut duplicated: i64 = 0;
    {
        let mut stmt = tx.prepare(FB_ADS_UPSERT_SQL)?;
        let mut map_stmt = tx.prepare(
            "INSERT OR IGNORE INTO fb_ads_to_file(fb_ad_id, file_id) VALUES(?, ?)",
        )?;
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
            let content_id =
                crate::sync_v9::content_id::fb_ad_id(&day_date, "ad_group", &r.ad_group_name);
            let fb_ad_id: i64 = stmt.query_row(
                params![
                    content_id,
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
                    tax_rate,
                    day_date,
                    source_file_id,
                ],
                |row| row.get(0),
            )?;
            map_stmt.execute(params![fb_ad_id, source_file_id])?;
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
        mcn_mismatch_count: 0,
    })
}

/// UPSERT template shared giữa `import_fb_ad_groups` và `import_fb_campaigns`.
/// ON CONFLICT theo `(day_date, level, name)` — 2 level cùng name không đụng nhau.
/// v10: RETURNING id để mapping fb_ads_to_file (SQLite 3.35+).
/// v13: id = content_id(day_date, level, name) — first param, cross-machine
/// deterministic. Caller compute qua `content_id::fb_ad_id`.
/// FB UPSERT với **no-overwrite-with-zero guard** cho metric numeric:
/// nếu excluded.spend (hoặc clicks/cpc/impressions/reach) là NULL hoặc 0
/// thì giữ existing value. Latest report > 0 mới replace.
///
/// Lý do: FB report export 2 lần cùng day+name có thể có spend=0 ở lần 2
/// (ad đã pause sau export 1, hoặc user nhầm import file rỗng). Không
/// guard → UPSERT đè value tốt thành 0 → mất data.
///
/// Identity fields (sub_ids, status, report dates) vẫn replace — vì
/// chúng reflect intent mới nhất, không có concept "tốt hơn = giữ".
const FB_ADS_UPSERT_SQL: &str = "
    INSERT INTO raw_fb_ads
    (id, level, name,
     sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
     report_start, report_end, status,
     spend, clicks, cpc, impressions, reach, tax_rate,
     day_date, source_file_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(day_date, level, name) DO UPDATE SET
       sub_id1 = excluded.sub_id1, sub_id2 = excluded.sub_id2,
       sub_id3 = excluded.sub_id3, sub_id4 = excluded.sub_id4,
       sub_id5 = excluded.sub_id5,
       report_start = excluded.report_start, report_end = excluded.report_end,
       status = excluded.status,
       spend       = CASE WHEN COALESCE(excluded.spend, 0)       > 0 THEN excluded.spend       ELSE spend       END,
       clicks      = CASE WHEN COALESCE(excluded.clicks, 0)      > 0 THEN excluded.clicks      ELSE clicks      END,
       cpc         = CASE WHEN COALESCE(excluded.cpc, 0)         > 0 THEN excluded.cpc         ELSE cpc         END,
       impressions = CASE WHEN COALESCE(excluded.impressions, 0) > 0 THEN excluded.impressions ELSE impressions END,
       reach       = CASE WHEN COALESCE(excluded.reach, 0)       > 0 THEN excluded.reach       ELSE reach       END,
       tax_rate    = excluded.tax_rate,
       source_file_id = excluded.source_file_id
    RETURNING id
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
    /// Thuế % gắn cho cả file. Xem doc ở `ImportFbAdGroupsPayload::tax_rate`.
    #[serde(default)]
    pub tax_rate: f64,
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
    let tax_rate = validate_tax_rate(payload.tax_rate)?;

    let hash = compute_hash(&payload.raw_content);
    // Bug F fix: chuẩn hoá `Z` format. Cursor column `imported_at` của
    // imported_files được sync_v9 capture/compare lex — mix `+00:00` với
    // `Z` (từ schema seed / accounts.rs) → lex sai → row có suffix `+`
    // bị xếp trước row có `Z` regardless of timestamp → cursor advance
    // bypass row → next sync skip.
    let now = crate::sync_v9::hlc::now_rfc3339_z();

    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    // Bug E fix: reject mutation trong bootstrap snapshot restore window.
    // Bypass guard → INSERT vào DB sắp bị swap → mất data sau swap.
    assert_not_bootstrapping(&conn)?;
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
        None,
    )?;

    let mut inserted: i64 = 0;
    let mut duplicated: i64 = 0;
    {
        let mut stmt = tx.prepare(FB_ADS_UPSERT_SQL)?;
        let mut map_stmt = tx.prepare(
            "INSERT OR IGNORE INTO fb_ads_to_file(fb_ad_id, file_id) VALUES(?, ?)",
        )?;
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
            let content_id =
                crate::sync_v9::content_id::fb_ad_id(&day_date, "campaign", &r.campaign_name);
            let fb_ad_id: i64 = stmt.query_row(
                params![
                    content_id,
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
                    tax_rate,
                    day_date,
                    source_file_id,
                ],
                |row| row.get(0),
            )?;
            map_stmt.execute(params![fb_ad_id, source_file_id])?;
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
        mcn_mismatch_count: 0,
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

    // ========================================================
    // FB UPSERT no-overwrite-with-zero guard
    // ========================================================

    fn fresh_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::db::migrate_for_tests(&conn).unwrap();
        // Seed day + imported_files cho FK.
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-17', '2026-04-17T00:00:00Z')",
            [],
        ).unwrap();
        let file_id = crate::sync_v9::content_id::imported_file_id("hash_x");
        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
             VALUES(?, 'a.csv', 'fb_ad_group', '2026-04-17T00:00:00Z', 'hash_x', '2026-04-17')",
            params![file_id],
        ).unwrap();
        conn
    }

    fn upsert_fb_ad(
        conn: &rusqlite::Connection,
        name: &str,
        spend: Option<f64>,
        clicks: Option<i64>,
    ) -> i64 {
        upsert_fb_ad_with_tax(conn, name, spend, clicks, 0.0)
    }

    /// Variant cho phép set tax_rate. `upsert_fb_ad` wrap với tax=0 (legacy).
    fn upsert_fb_ad_with_tax(
        conn: &rusqlite::Connection,
        name: &str,
        spend: Option<f64>,
        clicks: Option<i64>,
        tax_rate: f64,
    ) -> i64 {
        let file_id = crate::sync_v9::content_id::imported_file_id("hash_x");
        let id = crate::sync_v9::content_id::fb_ad_id("2026-04-17", "ad_group", name);
        conn.query_row(
            FB_ADS_UPSERT_SQL,
            params![
                id,                  // id
                "ad_group",          // level
                name,                // name
                "", "", "", "", "",  // sub_ids
                "2026-04-17", "2026-04-17",  // report dates
                Some("active"),      // status
                spend,               // spend
                clicks,              // clicks
                Option::<f64>::None, // cpc
                Option::<i64>::None, // impressions
                Option::<i64>::None, // reach
                tax_rate,            // tax_rate
                "2026-04-17",        // day_date
                file_id,             // source_file_id
            ],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
    }

    fn read_fb_ad(conn: &rusqlite::Connection, name: &str) -> (Option<f64>, Option<i64>) {
        conn.query_row(
            "SELECT spend, clicks FROM raw_fb_ads WHERE name = ? AND day_date = '2026-04-17'",
            [name],
            |r| Ok((r.get::<_, Option<f64>>(0)?, r.get::<_, Option<i64>>(1)?)),
        )
        .unwrap()
    }

    #[test]
    fn fb_upsert_zero_does_not_overwrite_positive_value() {
        let conn = fresh_db();
        // Lần 1: spend=1000, clicks=50 (file value).
        upsert_fb_ad(&conn, "ad_A", Some(1000.0), Some(50));
        let (s1, c1) = read_fb_ad(&conn, "ad_A");
        assert_eq!(s1, Some(1000.0));
        assert_eq!(c1, Some(50));

        // Lần 2: spend=0, clicks=0 (file rỗng) → KHÔNG đè.
        upsert_fb_ad(&conn, "ad_A", Some(0.0), Some(0));
        let (s2, c2) = read_fb_ad(&conn, "ad_A");
        assert_eq!(s2, Some(1000.0), "spend > 0 không được đè bằng 0");
        assert_eq!(c2, Some(50), "clicks > 0 không được đè bằng 0");
    }

    #[test]
    fn fb_upsert_null_does_not_overwrite_positive_value() {
        let conn = fresh_db();
        upsert_fb_ad(&conn, "ad_B", Some(500.0), Some(20));
        // NULL excluded → giữ existing.
        upsert_fb_ad(&conn, "ad_B", None, None);
        let (s, c) = read_fb_ad(&conn, "ad_B");
        assert_eq!(s, Some(500.0));
        assert_eq!(c, Some(20));
    }

    #[test]
    fn fb_upsert_higher_value_replaces_lower() {
        let conn = fresh_db();
        upsert_fb_ad(&conn, "ad_C", Some(100.0), Some(5));
        // Latest report > 0 → replace.
        upsert_fb_ad(&conn, "ad_C", Some(2000.0), Some(80));
        let (s, c) = read_fb_ad(&conn, "ad_C");
        assert_eq!(s, Some(2000.0), "value > 0 mới replace");
        assert_eq!(c, Some(80));
    }

    #[test]
    fn fb_upsert_initial_zero_inserts_zero() {
        let conn = fresh_db();
        // Insert đầu tiên với 0 — KHÔNG conflict, OK insert 0.
        upsert_fb_ad(&conn, "ad_D", Some(0.0), Some(0));
        let (s, c) = read_fb_ad(&conn, "ad_D");
        assert_eq!(s, Some(0.0), "lần đầu insert 0 OK (không có existing)");
        assert_eq!(c, Some(0));
    }

    #[test]
    fn fb_upsert_partial_zero_only_protects_those_fields() {
        let conn = fresh_db();
        upsert_fb_ad(&conn, "ad_E", Some(800.0), Some(40));
        // spend mới = 1500 (replace), clicks = 0 (giữ).
        upsert_fb_ad(&conn, "ad_E", Some(1500.0), Some(0));
        let (s, c) = read_fb_ad(&conn, "ad_E");
        assert_eq!(s, Some(1500.0), "spend > 0 replace");
        assert_eq!(c, Some(40), "clicks 0 không đè");
    }

    // ========================================================
    // tax_rate (v2) — precision + UPSERT semantics
    // ========================================================

    /// Đọc tax_rate column riêng cho test verify.
    fn read_tax_rate(conn: &rusqlite::Connection, name: &str) -> f64 {
        conn.query_row(
            "SELECT tax_rate FROM raw_fb_ads WHERE name = ? AND day_date = '2026-04-17'",
            [name],
            |r| r.get::<_, f64>(0),
        )
        .unwrap()
    }

    /// Đọc taxed spend qua expression giống query.rs (cents). Verify precision
    /// end-to-end qua cùng SQL pipeline production dùng.
    fn read_taxed_spend_cents(conn: &rusqlite::Connection, name: &str) -> i64 {
        conn.query_row(
            "SELECT CAST(ROUND(spend * (1.0 + tax_rate / 100.0) * 100) AS INTEGER)
             FROM raw_fb_ads WHERE name = ? AND day_date = '2026-04-17'",
            [name],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
    }

    #[test]
    fn tax_default_zero_preserves_legacy_behavior() {
        let conn = fresh_db();
        // upsert_fb_ad (legacy) → tax=0 → taxed spend = raw spend.
        upsert_fb_ad(&conn, "ad_T0", Some(1_000_000.0), Some(50));
        assert_eq!(read_tax_rate(&conn, "ad_T0"), 0.0);
        // 1,000,000 VND * 1.00 * 100 = 100,000,000 cents
        assert_eq!(read_taxed_spend_cents(&conn, "ad_T0"), 100_000_000);
    }

    #[test]
    fn tax_10pct_on_round_million_exact() {
        let conn = fresh_db();
        upsert_fb_ad_with_tax(&conn, "ad_T10", Some(1_000_000.0), Some(50), 10.0);
        assert_eq!(read_tax_rate(&conn, "ad_T10"), 10.0);
        // 1,000,000 * 1.10 * 100 = 110,000,000 cents = 1,100,000 VND exact
        assert_eq!(read_taxed_spend_cents(&conn, "ad_T10"), 110_000_000);
    }

    #[test]
    fn tax_8_25pct_no_drift() {
        let conn = fresh_db();
        upsert_fb_ad_with_tax(&conn, "ad_T825", Some(1_234_567.0), Some(100), 8.25);
        let cents = read_taxed_spend_cents(&conn, "ad_T825");
        // Manual: 1,234,567 × 0.0825 = 101,851.7775 (exact: 1234567×33/400)
        //         taxed = 1,234,567 + 101,851.7775 = 1,336,418.7775
        //         × 100 = 133,641,877.5; ROUND half-to-even (SQLite/IEEE 754) = 133,641,878
        // f64 noise quanh 1.0825: relative err ~1e-15, sau ×100 vẫn < 0.5 → ROUND ổn định.
        assert_eq!(cents, 133_641_878, "expected 133,641,878 (= 1,336,418.78 VND)");
    }

    #[test]
    fn tax_re_import_overwrites_rate() {
        let conn = fresh_db();
        // Lần 1: tax 5%
        upsert_fb_ad_with_tax(&conn, "ad_TR", Some(2_000_000.0), Some(80), 5.0);
        assert_eq!(read_tax_rate(&conn, "ad_TR"), 5.0);
        // Lần 2: re-import same file với tax 10% → đè (Q1 design = A)
        upsert_fb_ad_with_tax(&conn, "ad_TR", Some(2_000_000.0), Some(80), 10.0);
        assert_eq!(read_tax_rate(&conn, "ad_TR"), 10.0, "re-import phải đè tax");
        // taxed spend = 2,000,000 * 1.10 * 100 = 220,000,000 cents
        assert_eq!(read_taxed_spend_cents(&conn, "ad_TR"), 220_000_000);
    }

    #[test]
    fn tax_re_import_overwrites_to_zero() {
        let conn = fresh_db();
        // User lỡ tay nhập 10%, rồi correct về 0% → re-import phải đè 0 (KHÔNG
        // có guard "không đè bằng 0" cho tax_rate, khác spend/clicks).
        upsert_fb_ad_with_tax(&conn, "ad_TZ", Some(500_000.0), Some(20), 10.0);
        assert_eq!(read_tax_rate(&conn, "ad_TZ"), 10.0);
        upsert_fb_ad_with_tax(&conn, "ad_TZ", Some(500_000.0), Some(20), 0.0);
        assert_eq!(
            read_tax_rate(&conn, "ad_TZ"),
            0.0,
            "tax 0% phải đè (correction case)"
        );
    }

    #[test]
    fn tax_aggregate_mixed_rates_per_row_correct() {
        let conn = fresh_db();
        // 3 ad: tax khác nhau. SUM(taxed) phải = SUM(per-row taxed), không phải
        // SUM(spend) * (1 + avg_tax/100).
        upsert_fb_ad_with_tax(&conn, "ad_M1", Some(1_000_000.0), Some(50), 0.0);
        upsert_fb_ad_with_tax(&conn, "ad_M2", Some(1_000_000.0), Some(50), 5.0);
        upsert_fb_ad_with_tax(&conn, "ad_M3", Some(1_000_000.0), Some(50), 10.0);
        let total_cents: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(CAST(ROUND(spend * (1.0 + tax_rate / 100.0) * 100) AS INTEGER)), 0)
                 FROM raw_fb_ads WHERE day_date = '2026-04-17'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        // 1M*1.0 + 1M*1.05 + 1M*1.10 = 3,150,000 → 315,000,000 cents
        assert_eq!(total_cents, 315_000_000);
    }

    #[test]
    fn validate_tax_rate_range() {
        assert!(validate_tax_rate(0.0).is_ok());
        assert!(validate_tax_rate(10.0).is_ok());
        assert!(validate_tax_rate(100.0).is_ok());
        assert!(validate_tax_rate(8.25).is_ok());
        assert!(validate_tax_rate(-0.01).is_err(), "âm reject");
        assert!(validate_tax_rate(100.01).is_err(), "> 100 reject");
        assert!(validate_tax_rate(f64::NAN).is_err(), "NaN reject");
        assert!(validate_tax_rate(f64::INFINITY).is_err(), "Infinity reject");
    }

    #[test]
    fn tax_full_range_no_overflow() {
        let conn = fresh_db();
        // Spend lớn (1 tỷ) + tax max 100% — vẫn phải fit i64 cents.
        // 1,000,000,000 * 2.0 * 100 = 200,000,000,000 cents = 2*10^11 << i64::MAX (~9.2*10^18)
        upsert_fb_ad_with_tax(&conn, "ad_BIG", Some(1_000_000_000.0), Some(1), 100.0);
        assert_eq!(read_taxed_spend_cents(&conn, "ad_BIG"), 200_000_000_000);
    }

    /// Regression v0.6.2: import lại file đã bị soft-mark `reverted_at` (qua
    /// `revert_import` hoặc `batch_commit_deletes` v0.6.1 orphan cleanup) PHẢI
    /// revive row cũ thay vì INSERT mới. Pre-fix: UNIQUE PK collision vì
    /// `id = content_id(hash)` deterministic, row cũ vẫn ở table sau soft-mark.
    ///
    /// User-visible: "db: UNIQUE constraint failed: imported_files.id" khi
    /// import file thứ 2 (sau khi xóa toàn bộ ngày của file thứ nhất).
    #[test]
    fn register_imported_file_revives_reverted_row_same_hash() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::db::migrate_for_tests(&conn).unwrap();

        let hash = "h-revive-X";

        // Lần 1: import → row active.
        let tx = conn.transaction().unwrap();
        let id1 = register_imported_file(
            &tx,
            "A.csv",
            "shopee_clicks",
            "2026-04-25T08:00:00Z",
            hash,
            "imports/h-revive-X.csv",
            "2026-04-29",
            100,
            None,
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            id1,
            crate::sync_v9::content_id::imported_file_id(hash),
            "id = content_id(hash)"
        );

        // Soft-mark (mirror batch_commit_deletes v0.6.1 / revert_import).
        conn.execute(
            "UPDATE imported_files SET reverted_at = '2026-04-29T10:00:00Z',
             stored_path = NULL WHERE id = ?",
            params![id1],
        )
        .unwrap();
        // Cùng tombstone mà real flow tạo.
        conn.execute(
            "INSERT OR IGNORE INTO tombstones (entity_type, entity_key, deleted_at)
             VALUES ('imported_file', ?, '2026-04-29T10:00:00Z')",
            params![id1.to_string()],
        )
        .unwrap();

        // Lần 2: import lại cùng hash → MUST revive (không UNIQUE PK fail).
        let tx = conn.transaction().unwrap();
        let id2 = register_imported_file(
            &tx,
            "A-reimport.csv",
            "shopee_clicks",
            "2026-04-29T11:00:00Z",
            hash,
            "imports/h-revive-X-v2.csv",
            "2026-04-29",
            100,
            None,
        )
        .unwrap();
        tx.commit().unwrap();

        assert_eq!(id2, id1, "revive cùng id (content_id stable)");

        // Row active trở lại + metadata refresh.
        let (filename, reverted, stored_path): (String, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT filename, reverted_at, stored_path FROM imported_files WHERE id = ?",
                params![id1],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(reverted, None, "reverted_at cleared sau revive");
        assert_eq!(filename, "A-reimport.csv", "filename refresh từ import mới");
        assert_eq!(stored_path.as_deref(), Some("imports/h-revive-X-v2.csv"));

        // Local tombstone cleared để snapshot replay không wipe lại.
        let ts_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tombstones
                 WHERE entity_type = 'imported_file' AND entity_key = ?",
                params![id1.to_string()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ts_count, 0, "tombstone 'imported_file' cleared sau revive");

        // Đếm row total = 1 (revive, không INSERT mới).
        let total: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM imported_files WHERE file_hash = ?",
                [hash],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(total, 1, "chỉ 1 row, không tạo row dup");
    }

    /// Active match path vẫn hoạt động (regression không phá): import 2 lần
    /// liên tiếp không revert ở giữa → return id cũ, không INSERT mới.
    #[test]
    fn register_imported_file_returns_existing_active_id() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::db::migrate_for_tests(&conn).unwrap();

        let tx = conn.transaction().unwrap();
        let id1 = register_imported_file(
            &tx, "A.csv", "shopee_clicks", "now", "h-active",
            "p1", "2026-04-29", 10, None,
        )
        .unwrap();
        tx.commit().unwrap();

        let tx = conn.transaction().unwrap();
        let id2 = register_imported_file(
            &tx, "A-dup.csv", "shopee_clicks", "now2", "h-active",
            "p2", "2026-04-29", 10, None,
        )
        .unwrap();
        tx.commit().unwrap();

        assert_eq!(id1, id2, "active match: trả id cũ, không tạo mới");
        let total: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM imported_files WHERE file_hash = 'h-active'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(total, 1);
    }
}
