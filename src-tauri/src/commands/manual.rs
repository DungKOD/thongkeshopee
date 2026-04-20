//! Commands CRUD `manual_entries`.
//!
//! - `save_manual_entry`: INSERT hoặc UPDATE theo UNIQUE(sub_ids, day_date).
//!   Tự upsert `days(date)` nếu chưa có.
//! - `delete_manual_entry`: xóa 1 row theo key. Commit ngay (dùng khi
//!   user huỷ pending change).
//!
//! Batch delete của "Lưu thay đổi" → xem `commands::batch`.

use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use tauri::State;

use crate::db::types::{ManualEntryInput, ManualRowKey};
use crate::db::{tombstone_key_sub, DbState};

use super::{CmdError, CmdResult};

#[tauri::command]
pub fn save_manual_entry(
    state: State<'_, DbState>,
    input: ManualEntryInput,
) -> CmdResult<()> {
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let tx = conn.transaction()?;
    let now = Utc::now().to_rfc3339();

    tx.execute(
        "INSERT OR IGNORE INTO days(date, created_at) VALUES(?, ?)",
        params![input.day_date, now],
    )?;

    // Resurrect: nếu trước đó user đã xóa tuple này (tombstone 'ui_row' hoặc 'manual_entry')
    // thì save = huỷ tombstone — tránh merge cross-device xoá mất entry vừa tạo.
    // Cũng huỷ tombstone 'day' nếu có (save manual entry trên 1 ngày đã bị xóa).
    let key = tombstone_key_sub(&input.day_date, &input.sub_ids);
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
          override_orders, override_commission, notes, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date) DO UPDATE SET
            display_name         = excluded.display_name,
            override_clicks      = excluded.override_clicks,
            override_spend       = excluded.override_spend,
            override_cpc         = excluded.override_cpc,
            override_orders      = excluded.override_orders,
            override_commission  = excluded.override_commission,
            notes                = excluded.notes,
            updated_at           = excluded.updated_at",
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
        ],
    )?;

    tx.commit()?;
    Ok(())
}

/// Xóa 1 manual entry theo key. Không ảnh hưởng raw tables.
/// Ghi tombstone 'manual_entry' để merge cross-device không hồi sinh override đã xóa.
#[tauri::command]
pub fn delete_manual_entry(
    state: State<'_, DbState>,
    key: ManualRowKey,
) -> CmdResult<()> {
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let tx = conn.transaction()?;
    let now = Utc::now().to_rfc3339();

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
