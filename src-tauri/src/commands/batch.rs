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

use rusqlite::params;
use tauri::State;

use crate::db::types::BatchDeletePayload;
use crate::db::{resolve_active_imports_dir, tombstone_key_sub, DbState};

use super::query::{is_prefix, to_canonical, Canonical};
use crate::sync_v9::hlc::next_hlc_rfc3339;
use super::{CmdError, CmdResult};

#[tauri::command]
pub fn batch_commit_deletes(
    state: State<'_, DbState>,
    payload: BatchDeletePayload,
) -> CmdResult<BatchResult> {
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let tx = conn.transaction()?;
    // HLC-lite: tombstone.deleted_at monotonic → apply_tombstones compare với
    // manual_entries.updated_at từ remote chính xác bất chấp clock drift.
    let now = next_hlc_rfc3339(&tx)?;

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

    // Cleanup imported_files orphan — file không còn raw row nào reference.
    // v10: check qua MAPPING tables thay source_file_id (UPSERT có thể đè
    // source_file_id → file A trông orphan dù mapping còn entries). Giữ row
    // reverted_at IS NOT NULL cho lịch sử.
    // Multi-day file an toàn: mapping entries nằm trên raw rows từng day, chỉ
    // khi MỌI day trong file đã hết → mapping rỗng → file mới bị coi orphan.
    let orphan_files: Vec<(i64, Option<String>)> = {
        let mut stmt = tx.prepare(
            "SELECT id, stored_path FROM imported_files
             WHERE reverted_at IS NULL
               AND id NOT IN (
                   SELECT file_id FROM clicks_to_file UNION
                   SELECT file_id FROM orders_to_file UNION
                   SELECT file_id FROM fb_ads_to_file
               )",
        )?;
        let iter = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?)))?;
        iter.collect::<std::result::Result<_, _>>()?
    };
    for (id, _) in &orphan_files {
        tx.execute("DELETE FROM imported_files WHERE id = ?", params![id])?;
    }

    // Explicit bump sync_state — FK CASCADE DELETE trên raw tables có thể
    // không fire user triggers tùy SQLite config. Đảm bảo mọi thao tác xóa
    // của `batch_commit_deletes` đều mark dirty để sync flow upload.
    tx.execute(
        "UPDATE sync_state SET dirty = 1, change_id = change_id + 1 WHERE id = 1",
        [],
    )?;

    tx.commit()?;

    // Best-effort: xóa physical CSV file của orphan imported_files khỏi disk.
    // Resolve imports_dir từ connection đang mở (user-scoped folder hiện tại),
    // KHÔNG hardcode root app_data_dir vì sau v7+ DB ở `users/{uid}/`.
    let imports_base = resolve_active_imports_dir(&conn).ok();
    drop(conn);
    if let Some(base) = imports_base {
        for (_, path_opt) in orphan_files {
            if let Some(rel) = path_opt {
                let filename = rel.strip_prefix("imports/").unwrap_or(&rel);
                let abs = base.join(filename);
                if let Err(e) = std::fs::remove_file(&abs) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        eprintln!(
                            "[batch_commit_deletes] failed to remove orphan file {}: {e}",
                            abs.display()
                        );
                    }
                }
            }
        }
    }

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

/// Revert 1 file import (soft). Giữ row `imported_files` (reverted_at = now)
/// cho lịch sử, xóa raw rows CHỈ KHI không còn file active nào link tới chúng
/// (qua 3 bảng mapping `clicks_to_file` / `orders_to_file` / `fb_ads_to_file`).
///
/// 2 file trùng data → revert 1 file xong raw rows giữ nguyên vì mapping kia
/// vẫn tồn tại. Phải revert hết mới thật sự mất data.
///
/// Idempotent: revert 2 lần → lần 2 skip (reverted_at đã set).
#[tauri::command]
pub fn revert_import(
    state: State<'_, DbState>,
    file_id: i64,
) -> CmdResult<RevertResult> {
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;

    // Snapshot file info TRƯỚC khi revert — cần stored_path để xóa CSV khỏi disk.
    let (filename, stored_path_rel, already_reverted): (String, Option<String>, bool) = conn
        .query_row(
            "SELECT filename, stored_path, reverted_at IS NOT NULL
             FROM imported_files WHERE id = ?",
            params![file_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                CmdError::msg(format!("File ID {file_id} không tồn tại"))
            }
            other => CmdError::from(other),
        })?;

    if already_reverted {
        return Err(CmdError::msg(format!(
            "File '{filename}' đã được revert trước đó"
        )));
    }

    let tx = conn.transaction()?;
    // HLC cho sync cross-device + timestamp revert.
    let now = next_hlc_rfc3339(&tx)?;

    // 1. Xóa mapping của file này — raw rows nào chỉ link qua file X giờ orphan.
    tx.execute(
        "DELETE FROM clicks_to_file WHERE file_id = ?",
        params![file_id],
    )?;
    tx.execute(
        "DELETE FROM orders_to_file WHERE file_id = ?",
        params![file_id],
    )?;
    tx.execute(
        "DELETE FROM fb_ads_to_file WHERE file_id = ?",
        params![file_id],
    )?;

    // 2. Xóa raw rows orphan — không còn file active nào link.
    //    File trùng data với file X (cùng raw row) sẽ có mapping riêng → raw rows
    //    KHÔNG bị xóa. Correctness đảm bảo bởi WHERE NOT IN subquery.
    let clicks_deleted = tx.execute(
        "DELETE FROM raw_shopee_clicks
         WHERE click_id NOT IN (SELECT click_id FROM clicks_to_file)",
        [],
    )? as i64;
    let orders_deleted = tx.execute(
        "DELETE FROM raw_shopee_order_items
         WHERE id NOT IN (SELECT order_item_id FROM orders_to_file)",
        [],
    )? as i64;
    let fb_ads_deleted = tx.execute(
        "DELETE FROM raw_fb_ads
         WHERE id NOT IN (SELECT fb_ad_id FROM fb_ads_to_file)",
        [],
    )? as i64;

    // 3. Cleanup days orphan (không còn raw/manual nào).
    let days_deleted = tx.execute(
        "DELETE FROM days WHERE date NOT IN (
            SELECT day_date FROM raw_shopee_clicks UNION
            SELECT day_date FROM raw_shopee_order_items UNION
            SELECT day_date FROM raw_fb_ads UNION
            SELECT day_date FROM manual_entries
         )",
        [],
    )? as i64;

    // 4. Soft-mark file đã revert. `stored_path = NULL` để xóa CSV khỏi disk
    //    an toàn + list_imported_files không hiển thị đường dẫn cũ.
    tx.execute(
        "UPDATE imported_files
         SET reverted_at = ?, stored_path = NULL
         WHERE id = ?",
        params![now, file_id],
    )?;

    // 5. Bump sync_state → sync flow upload DB state mới (có reverted_at).
    tx.execute(
        "UPDATE sync_state SET dirty = 1, change_id = change_id + 1 WHERE id = 1",
        [],
    )?;

    tx.commit()?;

    // 6. Best-effort: xóa physical CSV file khỏi disk (outside tx).
    let imports_base = resolve_active_imports_dir(&conn).ok();
    drop(conn);
    if let (Some(base), Some(rel)) = (imports_base, stored_path_rel) {
        let filename = rel.strip_prefix("imports/").unwrap_or(&rel);
        let abs = base.join(filename);
        if let Err(e) = std::fs::remove_file(&abs) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "[revert_import] failed to remove CSV {}: {e}",
                    abs.display()
                );
            }
        }
    }

    Ok(RevertResult {
        file_id,
        filename,
        reverted_at: now,
        clicks_deleted,
        orders_deleted,
        fb_ads_deleted,
        days_deleted,
    })
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RevertResult {
    pub file_id: i64,
    pub filename: String,
    pub reverted_at: String,
    pub clicks_deleted: i64,
    pub orders_deleted: i64,
    pub fb_ads_deleted: i64,
    pub days_deleted: i64,
}
