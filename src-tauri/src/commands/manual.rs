//! Commands CRUD `manual_entries`.
//!
//! - `save_manual_entry`: INSERT hoặc UPDATE theo UNIQUE(sub_ids, day_date).
//!   Tự upsert `days(date)` nếu chưa có.
//! - `delete_manual_entry`: xóa 1 row theo key. Commit ngay (dùng khi
//!   user huỷ pending change).
//!
//! Batch delete của "Lưu thay đổi" → xem `commands::batch`.

use rusqlite::{params, OptionalExtension};
use tauri::State;

use crate::db::types::{ManualEntryInput, ManualRowKey};
use crate::db::{tombstone_key_sub, DbState};

use crate::sync_v9::hlc::next_hlc_rfc3339;
use super::{assert_not_bootstrapping, CmdError, CmdResult};

/// Trả `true` nếu có thay đổi DB thực sự, `false` nếu input trùng row hiện
/// có (no-op). Caller (FE) dùng flag để gate `markMutation` — tránh "Chờ đồng
/// bộ" khi user mở dialog rồi bấm Lưu mà không sửa giá trị nào.
#[tauri::command]
pub fn save_manual_entry(
    state: State<'_, DbState>,
    input: ManualEntryInput,
) -> CmdResult<bool> {
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    assert_not_bootstrapping(&conn)?;
    let tx = conn.transaction()?;

    let key = tombstone_key_sub(&input.day_date, &input.sub_ids);

    // No-op detection: row hiện có khớp toàn bộ input field VÀ không có
    // tombstone nào cần huỷ → bỏ qua write để cursor `updated_at` không
    // advance → push delta không trigger.
    let has_tombstone: bool = tx
        .query_row(
            "SELECT 1 FROM tombstones
             WHERE (entity_type IN ('ui_row', 'manual_entry') AND entity_key = ?1)
                OR (entity_type = 'day' AND entity_key = ?2)
             LIMIT 1",
            params![key, input.day_date],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);

    type ExistingRow = (
        Option<String>,
        Option<i64>,
        Option<f64>,
        Option<f64>,
        Option<i64>,
        Option<f64>,
        Option<String>,
        Option<i64>,
    );
    let existing: Option<ExistingRow> = tx
        .query_row(
            "SELECT display_name, override_clicks, override_spend, override_cpc,
                    override_orders, override_commission, notes, shopee_account_id
             FROM manual_entries
             WHERE sub_id1 = ?1 AND sub_id2 = ?2 AND sub_id3 = ?3
               AND sub_id4 = ?4 AND sub_id5 = ?5 AND day_date = ?6",
            params![
                input.sub_ids[0],
                input.sub_ids[1],
                input.sub_ids[2],
                input.sub_ids[3],
                input.sub_ids[4],
                input.day_date,
            ],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            },
        )
        .optional()?;

    if !has_tombstone {
        if let Some((dn, oc, os, occ, oo, ocom, n, sa)) = &existing {
            if dn == &input.display_name
                && oc == &input.override_clicks
                && os == &input.override_spend
                && occ == &input.override_cpc
                && oo == &input.override_orders
                && ocom == &input.override_commission
                && n == &input.notes
                && *sa == Some(input.shopee_account_id)
            {
                return Ok(false);
            }
        }
    }

    // HLC-lite: ensure updated_at monotonic across machines. Thay `Utc::now`
    // bằng `next_hlc_rfc3339` để clock drift của máy local không làm edit này
    // "trông như" sớm hơn edit đã thấy từ remote.
    let now = next_hlc_rfc3339(&tx)?;

    tx.execute(
        "INSERT OR IGNORE INTO days(date, created_at) VALUES(?, ?)",
        params![input.day_date, now],
    )?;

    // Resurrect: nếu trước đó user đã xóa tuple này (tombstone 'ui_row' hoặc 'manual_entry')
    // thì save = huỷ tombstone — tránh merge cross-device xoá mất entry vừa tạo.
    // Cũng huỷ tombstone 'day' nếu có (save manual entry trên 1 ngày đã bị xóa).
    tx.execute(
        "DELETE FROM tombstones
         WHERE (entity_type IN ('ui_row', 'manual_entry') AND entity_key = ?)
            OR (entity_type = 'day' AND entity_key = ?)",
        params![key, input.day_date],
    )?;

    // UPSERT manual_entries — UNIQUE(sub_ids, day_date) → DO UPDATE.
    tx.execute(
        "INSERT INTO manual_entries
         (sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date,
          display_name, override_clicks, override_spend, override_cpc,
          override_orders, override_commission, notes, created_at, updated_at,
          shopee_account_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date) DO UPDATE SET
            display_name         = excluded.display_name,
            override_clicks      = excluded.override_clicks,
            override_spend       = excluded.override_spend,
            override_cpc         = excluded.override_cpc,
            override_orders      = excluded.override_orders,
            override_commission  = excluded.override_commission,
            notes                = excluded.notes,
            updated_at           = excluded.updated_at,
            shopee_account_id    = excluded.shopee_account_id",
        params![
            input.sub_ids[0],
            input.sub_ids[1],
            input.sub_ids[2],
            input.sub_ids[3],
            input.sub_ids[4],
            input.day_date,
            input.display_name,
            input.override_clicks,
            input.override_spend,
            input.override_cpc,
            input.override_orders,
            input.override_commission,
            input.notes,
            now,
            now,
            input.shopee_account_id,
        ],
    )?;

    tx.commit()?;
    Ok(true)
}

/// Xóa 1 manual entry theo key. Không ảnh hưởng raw tables.
/// Ghi tombstone 'manual_entry' để merge cross-device không hồi sinh override đã xóa.
#[tauri::command]
pub fn delete_manual_entry(
    state: State<'_, DbState>,
    key: ManualRowKey,
) -> CmdResult<()> {
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    assert_not_bootstrapping(&conn)?;
    let tx = conn.transaction()?;
    // HLC-lite: tombstone.deleted_at cũng phải monotonic để so sánh với
    // row.updated_at từ máy khác (xem apply_tombstones có check updated_at).
    let now = next_hlc_rfc3339(&tx)?;

    tx.execute(
        "DELETE FROM manual_entries
         WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
           AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?",
        params![
            key.sub_ids[0],
            key.sub_ids[1],
            key.sub_ids[2],
            key.sub_ids[3],
            key.sub_ids[4],
            key.day_date,
        ],
    )?;

    tx.execute(
        "INSERT OR IGNORE INTO tombstones (entity_type, entity_key, deleted_at)
         VALUES ('manual_entry', ?, ?)",
        params![tombstone_key_sub(&key.day_date, &key.sub_ids), now],
    )?;

    tx.commit()?;
    Ok(())
}

/// Kiểm tra có manual entry nào cho key này không (dùng cho UI detect state).
#[tauri::command]
pub fn has_manual_entry(
    state: State<'_, DbState>,
    key: ManualRowKey,
) -> CmdResult<bool> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let v: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM manual_entries
             WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
               AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?",
            params![
                key.sub_ids[0],
                key.sub_ids[1],
                key.sub_ids[2],
                key.sub_ids[3],
                key.sub_ids[4],
                key.day_date,
            ],
            |r| r.get(0),
        )
        .optional()?;
    Ok(v.is_some())
}
