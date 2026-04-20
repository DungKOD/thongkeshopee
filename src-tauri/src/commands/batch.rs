//! Commit batch "Lưu thay đổi" — xóa toàn bộ pending (days, rows) trong 1
//! transaction atomic.
//!
//! **Xóa 1 "dòng UI"** (1 tuple sub_id canonical) phải xóa:
//! 1. `manual_entries` khớp tuple + day.
//! 2. Raw rows có canonical **prefix-compatible** với target (same logic như
//!    aggregate_rows_for_day — user thấy data gì trên UI thì xóa đúng chỗ đó).
//!
//! Sau khi xóa: cleanup `days` orphan (không còn raw/manual nào) → UI tự
//! không hiển thị day đó.

use chrono::Utc;
use rusqlite::params;
use tauri::State;

use crate::db::types::BatchDeletePayload;
use crate::db::{tombstone_key_sub, DbState};

use super::query::{is_prefix, to_canonical, Canonical};
use super::{CmdError, CmdResult};

#[tauri::command]
pub fn batch_commit_deletes(
    state: State<'_, DbState>,
    payload: BatchDeletePayload,
) -> CmdResult<BatchResult> {
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let tx = conn.transaction()?;
    let now = Utc::now().to_rfc3339();

    let mut days_deleted = 0_i64;
    for date in &payload.days {
        let n = tx.execute("DELETE FROM days WHERE date = ?", params![date])?;
        days_deleted += n as i64;

        // Tombstone 'day' — apply khi merge: xóa day ở peer, CASCADE raw rows.
        tx.execute(
            "INSERT OR IGNORE INTO tombstones (entity_type, entity_key, deleted_at)
             VALUES ('day', ?, ?)",
            params![date, now],
        )?;
    }

    let mut rows_deleted = 0_i64;
    for row in &payload.manual_rows {
        // Target canonical của UI row cần xóa.
        let target = to_canonical(row.sub_ids.clone());

        // 1. Xóa manual_entries khớp tuple chính xác (manual entries chỉ có 1 tuple riêng).
        tx.execute(
            "DELETE FROM manual_entries
             WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
               AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?",
            params![
                row.sub_ids[0],
                row.sub_ids[1],
                row.sub_ids[2],
                row.sub_ids[3],
                row.sub_ids[4],
                row.day_date,
            ],
        )?;

        // 2. Xóa raw rows có canonical prefix-compatible với target (trên cùng day).
        for table in [
            "raw_fb_ads",
            "raw_shopee_clicks",
            "raw_shopee_order_items",
        ] {
            rows_deleted +=
                delete_prefix_compatible(&tx, table, &row.day_date, &target)?;
        }

        // Tombstone 'ui_row' — apply khi merge: xóa manual + raw prefix-compatible ở peer.
        tx.execute(
            "INSERT OR IGNORE INTO tombstones (entity_type, entity_key, deleted_at)
             VALUES ('ui_row', ?, ?)",
            params![tombstone_key_sub(&row.day_date, &row.sub_ids), now],
        )?;
    }

    // Cleanup days orphan (không còn raw/manual nào) — auto-remove.
    tx.execute(
        "DELETE FROM days WHERE date NOT IN (
            SELECT day_date FROM raw_shopee_clicks UNION
            SELECT day_date FROM raw_shopee_order_items UNION
            SELECT day_date FROM raw_fb_ads UNION
            SELECT day_date FROM manual_entries
         )",
        [],
    )?;

    tx.commit()?;

    Ok(BatchResult {
        days_deleted,
        rows_deleted,
    })
}

/// Xóa rows trong `table` có canonical prefix-compatible với `target`, cùng `day_date`.
/// "Prefix-compatible" = target là prefix của row HOẶC row là prefix của target.
fn delete_prefix_compatible(
    tx: &rusqlite::Transaction,
    table: &str,
    day_date: &str,
    target: &Canonical,
) -> CmdResult<i64> {
    // Load tất cả tuple sub_id distinct trong day → filter compatible ở Rust.
    // Dùng DISTINCT để giảm số vòng lặp DELETE.
    let select_sql = format!(
        "SELECT DISTINCT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
         FROM {table} WHERE day_date = ?"
    );
    let mut stmt = tx.prepare(&select_sql)?;
    let tuples: Vec<[String; 5]> = stmt
        .query_map(params![day_date], |r| {
            Ok([
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            ])
        })?
        .collect::<Result<_, _>>()?;
    drop(stmt);

    let delete_sql = format!(
        "DELETE FROM {table}
         WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
           AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?"
    );

    let mut total: i64 = 0;
    for tuple in tuples {
        let canonical = to_canonical(tuple.clone());
        let compatible = is_prefix(&canonical, target) || is_prefix(target, &canonical);
        if !compatible {
            continue;
        }
        let n = tx.execute(
            &delete_sql,
            params![
                tuple[0], tuple[1], tuple[2], tuple[3], tuple[4], day_date
            ],
        )?;
        total += n as i64;
    }
    Ok(total)
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchResult {
    pub days_deleted: i64,
    pub rows_deleted: i64,
}
