//! Apply delta events vào local DB (pull side).
//!
//! Mỗi event `InsertEvent` / `UpsertEvent` / `TombstoneEvent` được dispatch
//! tới handler riêng. Caller wrap trong transaction (xem `pull::apply_delta_file`).
//!
//! Invariants (rule giữ data #1):
//! - Insert = INSERT OR IGNORE (PK conflict = idempotent replay, không mất local state)
//! - Upsert = INSERT OR REPLACE CHỈ khi `local.updated_at <= event.updated_at` (HLC wins)
//! - Tombstone day / manual_entry / ui_row = resurrect rule (v0.5.2):
//!   delete CHỈ data có timestamp ≤ deleted_at; data add SAU deletion (vd user
//!   xóa day rồi modify lại) survive. Day row tự xóa nếu không còn data ref.
//!
//! Re-use từ v8:
//! - `commands::query::{to_canonical, is_prefix}` — sub_id prefix matching
//!
//! NOT delete logic v8 `apply_tombstones` (đọc từ local tombstones table) —
//! v9 apply từ event trực tiếp. v8 logic vẫn còn trong sync.rs cho đến P8.

use anyhow::{anyhow, Context, Result};
use rusqlite::{params, params_from_iter, types::Value as SqlValue, Transaction};
use serde_json::{Map, Value};

use super::descriptors::{find_descriptor, DeltaOp};
use super::types::{DeltaEvent, InsertEvent, TombstoneEvent, UpsertEvent};

/// Kết quả apply 1 event. Caller dùng để build ApplyStats cho event log.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyOutcome {
    /// Row inserted hoặc upserted thành công.
    Applied,
    /// PK đã tồn tại (INSERT OR IGNORE) → idempotent.
    Skipped,
    /// Upsert: local.updated_at > event.updated_at → local giữ (HLC wins).
    SkippedByHlc,
    /// Tombstone không xóa row nào (resurrect hoặc không có target).
    TombstoneNoOp,
    /// Tombstone xóa ≥1 row.
    TombstoneApplied { rows_deleted: u64 },
}

/// Dispatch apply cho 1 DeltaEvent. Caller wrap TX + iterate event list.
pub fn apply_event(tx: &Transaction, event: &DeltaEvent) -> Result<ApplyOutcome> {
    match event {
        DeltaEvent::Insert(ev) => apply_insert(tx, ev),
        DeltaEvent::Upsert(ev) => apply_upsert(tx, ev),
        DeltaEvent::Tombstone(ev) => apply_tombstone(tx, ev),
    }
}

// =============================================================
// INSERT OR IGNORE — raw tables + imported_files + shopee_accounts
// =============================================================

fn apply_insert(tx: &Transaction, ev: &InsertEvent) -> Result<ApplyOutcome> {
    let descriptor = find_descriptor(&ev.table)
        .ok_or_else(|| anyhow!("unknown table '{}' trong insert event", ev.table))?;
    if descriptor.op != DeltaOp::Insert {
        anyhow::bail!(
            "table '{}' descriptor op = {:?}, không phải Insert",
            ev.table,
            descriptor.op
        );
    }

    let row_obj = ev
        .row
        .as_object()
        .ok_or_else(|| anyhow!("insert event row không phải JSON object"))?;

    // Auto-insert days row nếu raw/manual referenced day_date chưa tồn tại.
    if let Some(day_date) = row_obj.get("day_date").and_then(|v| v.as_str()) {
        ensure_day_exists(tx, day_date)?;
    }

    exec_insert_or_ignore(tx, &ev.table, row_obj)
}

fn exec_insert_or_ignore(
    tx: &Transaction,
    table: &str,
    row_obj: &Map<String, Value>,
) -> Result<ApplyOutcome> {
    let table_cols = local_table_columns(tx, table)?;

    // Strip `id` từ row nếu descriptor's pk_columns không có "id". Lý do: các
    // table có `INTEGER PRIMARY KEY AUTOINCREMENT id` NHƯNG pk_columns logical
    // khác (vd manual_entries pk = (sub_id1..5, day_date)) — id chỉ là surrogate
    // local, không nên sync cross-machine. Nếu đẩy id từ remote, có thể clash
    // với id autoincrement local (khác row) → INSERT OR IGNORE silently drop
    // → data loss.
    //
    // Content-id tables (imported_files, shopee_accounts, raw_shopee_order_items,
    // raw_fb_ads) có pk_columns bao gồm id hoặc natural keys hash→content_id ở
    // INSERT site, nên id deterministic cross-machine — KHÔNG strip ở đây.
    let desc = find_descriptor(table);
    let strip_id = desc
        .map(|d| !d.pk_columns.iter().any(|c| *c == "id"))
        .unwrap_or(false);

    let cols_to_insert: Vec<&str> = table_cols
        .iter()
        .filter(|c| row_obj.contains_key(c.as_str()))
        .filter(|c| !(strip_id && c.as_str() == "id"))
        .map(|s| s.as_str())
        .collect();

    if cols_to_insert.is_empty() {
        anyhow::bail!("không có column nào match giữa event và table {table}");
    }

    let placeholders: Vec<String> = (1..=cols_to_insert.len())
        .map(|i| format!("?{i}"))
        .collect();
    let sql = format!(
        "INSERT OR IGNORE INTO {table} ({cols}) VALUES ({ph})",
        cols = cols_to_insert.join(","),
        ph = placeholders.join(",")
    );

    let vals: Vec<SqlValue> = cols_to_insert
        .iter()
        .map(|c| json_to_sqlite(&row_obj[*c]))
        .collect();

    let affected = tx
        .execute(&sql, params_from_iter(vals.iter()))
        .with_context(|| {
            // Include row context để debug FK / NOT NULL failures. Giới hạn
            // 200 chars tránh log flood với row lớn.
            let row_summary = serde_json::to_string(row_obj)
                .map(|s| {
                    if s.len() > 200 {
                        format!("{}...", &s[..200])
                    } else {
                        s
                    }
                })
                .unwrap_or_else(|_| "<unserializable>".to_string());
            format!("INSERT OR IGNORE {table} (row={row_summary})")
        })?;
    Ok(if affected > 0 {
        ApplyOutcome::Applied
    } else {
        ApplyOutcome::Skipped
    })
}

// =============================================================
// UPSERT với HLC check — manual_entries, shopee_accounts
// =============================================================

fn apply_upsert(tx: &Transaction, ev: &UpsertEvent) -> Result<ApplyOutcome> {
    let descriptor = find_descriptor(&ev.table)
        .ok_or_else(|| anyhow!("unknown table '{}' upsert event", ev.table))?;

    let pk_obj = ev
        .pk
        .as_object()
        .ok_or_else(|| anyhow!("upsert event pk không phải JSON object"))?;
    let row_obj = ev
        .row
        .as_object()
        .ok_or_else(|| anyhow!("upsert event row không phải JSON object"))?;

    // HLC check: nếu local exists với updated_at > event.updated_at → skip.
    if let Some(local_updated_at) = read_local_updated_at(tx, &ev.table, descriptor.pk_columns, pk_obj)? {
        if local_updated_at.as_str() > ev.updated_at.as_str() {
            return Ok(ApplyOutcome::SkippedByHlc);
        }
    }

    // Ensure day row if needed.
    if let Some(day_date) = row_obj.get("day_date").and_then(|v| v.as_str()) {
        ensure_day_exists(tx, day_date)?;
    }

    // DELETE existing + INSERT new (avoid REPLACE because INSERT OR REPLACE on
    // tables with FK ON DELETE CASCADE would cascade-delete child rows).
    let delete_sql = build_pk_where_delete(&ev.table, descriptor.pk_columns);
    let pk_vals: Vec<SqlValue> = descriptor
        .pk_columns
        .iter()
        .map(|c| json_to_sqlite(pk_obj.get(*c).unwrap_or(&Value::Null)))
        .collect();
    tx.execute(&delete_sql, params_from_iter(pk_vals.iter()))
        .with_context(|| format!("upsert delete old {}", ev.table))?;

    let outcome = exec_insert_or_ignore(tx, &ev.table, row_obj)?;
    // Sau delete + insert, luôn là Applied (không thể Skipped vì đã clear).
    match outcome {
        ApplyOutcome::Applied | ApplyOutcome::Skipped => Ok(ApplyOutcome::Applied),
        other => Ok(other),
    }
}

fn read_local_updated_at(
    tx: &Transaction,
    table: &str,
    pk_cols: &[&str],
    pk_obj: &Map<String, Value>,
) -> Result<Option<String>> {
    let sql = format!(
        "SELECT updated_at FROM {table} WHERE {where_clause}",
        where_clause = pk_cols
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{c} = ?{}", i + 1))
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    let vals: Vec<SqlValue> = pk_cols
        .iter()
        .map(|c| json_to_sqlite(pk_obj.get(*c).unwrap_or(&Value::Null)))
        .collect();
    let updated_at: rusqlite::Result<String> =
        tx.query_row(&sql, params_from_iter(vals.iter()), |r| r.get(0));
    match updated_at {
        Ok(s) => Ok(Some(s)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e).context("read local updated_at"),
    }
}

fn build_pk_where_delete(table: &str, pk_cols: &[&str]) -> String {
    format!(
        "DELETE FROM {table} WHERE {where_clause}",
        where_clause = pk_cols
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{c} = ?{}", i + 1))
            .collect::<Vec<_>>()
            .join(" AND ")
    )
}

// =============================================================
// Tombstone apply — với resurrect rule cho manual_entry/ui_row
// =============================================================

fn apply_tombstone(tx: &Transaction, ev: &TombstoneEvent) -> Result<ApplyOutcome> {
    // Record tombstone vào local table (audit + future re-apply sau
    // snapshot restore). UNIQUE(entity_type, entity_key) idempotent.
    tx.execute(
        "INSERT OR IGNORE INTO tombstones (entity_type, entity_key, deleted_at)
         VALUES (?, ?, ?)",
        params![ev.entity_type, ev.entity_key, ev.deleted_at],
    )
    .context("insert tombstone audit row")?;

    let deleted = match ev.entity_type.as_str() {
        "day" => apply_day_tombstone(tx, &ev.entity_key, &ev.deleted_at)?,
        "manual_entry" => apply_manual_entry_tombstone(tx, &ev.entity_key, &ev.deleted_at)?,
        "ui_row" => apply_ui_row_tombstone(tx, &ev.entity_key, &ev.deleted_at)?,
        "imported_file" => apply_imported_file_tombstone(tx, &ev.entity_key, &ev.deleted_at)?,
        "shopee_account" => apply_shopee_account_tombstone(tx, &ev.entity_key)?,
        other => anyhow::bail!("unknown tombstone entity_type: {other}"),
    };
    Ok(if deleted > 0 {
        ApplyOutcome::TombstoneApplied {
            rows_deleted: deleted,
        }
    } else {
        ApplyOutcome::TombstoneNoOp
    })
}

/// Day tombstone — resurrect-aware delete (v0.5.2).
///
/// Trước đây UNCONDITIONAL CASCADE (`DELETE FROM days WHERE date=?` rồi FK
/// cascade kéo theo raw + manual). Vấn đề: nếu user xóa day rồi MODIFY day đó
/// (vd add manual entry mới), lúc apply tombstone replay (cùng máy hoặc máy
/// khác), modification cũng bị cascade xóa → mất data.
///
/// Semantic mới: "delete day = remove data EXISTING tại deleted_at; data ADDED
/// SAU survive". Implement bằng resurrect rule:
/// - manual_entries: xóa nếu `updated_at <= deleted_at`.
/// - raw_*: xóa nếu KHÔNG có mapping link tới file `imported_at > deleted_at`
///   AND `reverted_at IS NULL` (= file mới import sau deletion). Mapping-based
///   để cover case re-import file: PK conflict trong raw_* INSERT giữ
///   source_file_id cũ nhưng mapping mới → mapping decides "freshness".
/// - days: xóa nếu không còn raw/manual nào reference.
///
/// Idempotent: replay → DELETE same conditions → 0 rows.
fn apply_day_tombstone(tx: &Transaction, day_date: &str, deleted_at: &str) -> Result<u64> {
    let mut total: u64 = 0;

    // 1. Xóa manual_entries cũ (resurrect rule).
    total += tx.execute(
        "DELETE FROM manual_entries
         WHERE day_date = ? AND updated_at <= ?",
        params![day_date, deleted_at],
    )
    .context("delete old manual_entries")? as u64;

    // 2. Xóa raw rows cũ — không có mapping link tới file post-delete active.
    total += tx.execute(
        "DELETE FROM raw_shopee_clicks
         WHERE day_date = ?
           AND click_id NOT IN (
             SELECT cf.click_id FROM clicks_to_file cf
             JOIN imported_files f ON cf.file_id = f.id
             WHERE f.imported_at > ? AND f.reverted_at IS NULL
           )",
        params![day_date, deleted_at],
    )
    .context("delete old raw_shopee_clicks")? as u64;

    total += tx.execute(
        "DELETE FROM raw_shopee_order_items
         WHERE day_date = ?
           AND id NOT IN (
             SELECT of2.order_item_id FROM orders_to_file of2
             JOIN imported_files f ON of2.file_id = f.id
             WHERE f.imported_at > ? AND f.reverted_at IS NULL
           )",
        params![day_date, deleted_at],
    )
    .context("delete old raw_shopee_order_items")? as u64;

    total += tx.execute(
        "DELETE FROM raw_fb_ads
         WHERE day_date = ?
           AND id NOT IN (
             SELECT ff.fb_ad_id FROM fb_ads_to_file ff
             JOIN imported_files f ON ff.file_id = f.id
             WHERE f.imported_at > ? AND f.reverted_at IS NULL
           )",
        params![day_date, deleted_at],
    )
    .context("delete old raw_fb_ads")? as u64;

    // 3. Xóa day NẾU không còn data nào reference (raw hoặc manual).
    total += tx.execute(
        "DELETE FROM days WHERE date = ?
           AND date NOT IN (
             SELECT day_date FROM raw_shopee_clicks UNION
             SELECT day_date FROM raw_shopee_order_items UNION
             SELECT day_date FROM raw_fb_ads UNION
             SELECT day_date FROM manual_entries
           )",
        params![day_date],
    )
    .context("delete day if orphan")? as u64;

    Ok(total)
}

/// Manual entry tombstone — chỉ xóa nếu row.updated_at <= tombstone.deleted_at.
fn apply_manual_entry_tombstone(
    tx: &Transaction,
    key: &str,
    deleted_at: &str,
) -> Result<u64> {
    let Some((day, sub_ids)) = parse_tombstone_sub_key(key) else {
        return Ok(0);
    };
    let n = tx.execute(
        "DELETE FROM manual_entries
         WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
           AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?
           AND updated_at <= ?",
        params![
            sub_ids[0], sub_ids[1], sub_ids[2], sub_ids[3], sub_ids[4], day, deleted_at
        ],
    )
    .context("delete manual_entry (resurrect check)")?;
    Ok(n as u64)
}

/// UI row tombstone — DELETE manual_entries exact (resurrect check) + raw
/// prefix-compatible (unconditional vì raw không có updated_at).
///
/// Reuse logic từ v8 `apply_tombstones` (sync.rs L1286+). Sẽ delete khi v8 bị xóa ở P8.
fn apply_ui_row_tombstone(tx: &Transaction, key: &str, deleted_at: &str) -> Result<u64> {
    use crate::commands::query::{is_prefix, to_canonical};

    let Some((day, sub_ids)) = parse_tombstone_sub_key(key) else {
        return Ok(0);
    };
    let mut total: u64 = 0;

    total += tx
        .execute(
            "DELETE FROM manual_entries
             WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
               AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?
               AND updated_at <= ?",
            params![
                sub_ids[0], sub_ids[1], sub_ids[2], sub_ids[3], sub_ids[4], day, deleted_at
            ],
        )
        .context("ui_row delete manual_entries")? as u64;

    let target = to_canonical(sub_ids);
    for table in ["raw_fb_ads", "raw_shopee_clicks", "raw_shopee_order_items"] {
        let select_sql = format!(
            "SELECT DISTINCT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
             FROM {table} WHERE day_date = ?"
        );
        let tuples: Vec<[String; 5]> = {
            let mut stmt = tx.prepare(&select_sql)?;
            let rows = stmt.query_map(params![day], |r| {
                Ok([
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ])
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        let delete_sql = format!(
            "DELETE FROM {table}
             WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
               AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?"
        );
        for tuple in tuples {
            let canonical = to_canonical(tuple.clone());
            let compatible = is_prefix(&canonical, &target) || is_prefix(&target, &canonical);
            if !compatible {
                continue;
            }
            total += tx
                .execute(
                    &delete_sql,
                    params![tuple[0], tuple[1], tuple[2], tuple[3], tuple[4], day],
                )
                .with_context(|| format!("ui_row delete {table}"))? as u64;
        }
    }
    Ok(total)
}

/// Imported_file tombstone — máy A revert 1 file → A push tombstone → B apply
/// xóa mappings + raw_* orphan + soft-mark imported_files row, mirror logic
/// `commands::batch::revert_import` (DB side only — không chạm disk).
///
/// `entity_key` = `str(file_id)`. file_id = content_id(file_hash) deterministic
/// cross-machine → A's id == B's id cho cùng file.
///
/// Idempotent: máy A pull lại tombstone của chính mình → mappings đã rỗng,
/// raw orphan đã clean, UPDATE reverted_at có guard `IS NULL` → no-op.
/// Preserves history (giữ row imported_files với reverted_at marker — UI hiển
/// thị file đã revert).
fn apply_imported_file_tombstone(
    tx: &Transaction,
    key: &str,
    deleted_at: &str,
) -> Result<u64> {
    let file_id: i64 = match key.parse() {
        Ok(n) => n,
        Err(_) => return Ok(0), // key sai format → no-op (defensive)
    };

    let mut total: u64 = 0;

    // 1. Xóa mappings — raw_* nào chỉ link qua file này giờ orphan.
    total += tx.execute("DELETE FROM clicks_to_file WHERE file_id = ?", params![file_id])? as u64;
    total += tx.execute("DELETE FROM orders_to_file WHERE file_id = ?", params![file_id])? as u64;
    total += tx.execute("DELETE FROM fb_ads_to_file WHERE file_id = ?", params![file_id])? as u64;

    // 2. Xóa raw rows orphan — không còn file active nào link.
    total += tx.execute(
        "DELETE FROM raw_shopee_clicks
         WHERE click_id NOT IN (SELECT click_id FROM clicks_to_file)",
        [],
    )? as u64;
    total += tx.execute(
        "DELETE FROM raw_shopee_order_items
         WHERE id NOT IN (SELECT order_item_id FROM orders_to_file)",
        [],
    )? as u64;
    total += tx.execute(
        "DELETE FROM raw_fb_ads
         WHERE id NOT IN (SELECT fb_ad_id FROM fb_ads_to_file)",
        [],
    )? as u64;

    // 3. Soft-mark imported_files row (giữ history). Guard `reverted_at IS NULL`
    // → idempotent + KHÔNG đè reverted_at cũ nếu A đã revert ngày khác trước đó.
    // Nếu B chưa có row (delta chưa pull) → 0 rows updated, không sao —
    // cleanup downstream trên đã chạy.
    total += tx.execute(
        "UPDATE imported_files
         SET reverted_at = ?, stored_path = NULL
         WHERE id = ? AND reverted_at IS NULL",
        params![deleted_at, file_id],
    )? as u64;

    // 4. Cleanup days orphan.
    total += tx.execute(
        "DELETE FROM days WHERE date NOT IN (
            SELECT day_date FROM raw_shopee_clicks UNION
            SELECT day_date FROM raw_shopee_order_items UNION
            SELECT day_date FROM raw_fb_ads UNION
            SELECT day_date FROM manual_entries
         )",
        [],
    )? as u64;

    Ok(total)
}

/// Shopee_account tombstone — máy A xóa 1 account → A push tombstone → B
/// apply mirror logic `commands::accounts::delete_shopee_account` (DB side).
///
/// `entity_key` = `str(account_id)`. account_id = content_id(name)
/// deterministic cross-machine.
///
/// **CRITICAL — default account protection**: id của "Mặc định" KHÔNG BAO GIỜ
/// được xóa. Nếu tombstone trỏ tới default → no-op. Lý do: orphan rows
/// (chưa gán account) reassign về default → mất default = mất data.
/// commands::accounts cũng có guard này ở mutation site, nên defensive ở
/// apply layer cho sync path (nếu phía A buggy gửi tombstone default).
///
/// Idempotent: replay → DELETE no-op (rows đã xóa), shopee_accounts row đã
/// xóa hoặc chưa từng có → 0 rows affected.
fn apply_shopee_account_tombstone(tx: &Transaction, key: &str) -> Result<u64> {
    use crate::sync_v9::content_id;

    let id: i64 = match key.parse() {
        Ok(n) => n,
        Err(_) => return Ok(0),
    };

    // Default account protection — không bao giờ xóa, kể cả tombstone từ peer.
    let default_id = content_id::shopee_account_id(crate::db::DEFAULT_ACCOUNT_NAME);
    if id == default_id {
        return Ok(0);
    }

    let mut total: u64 = 0;

    // Xóa raw rows + manual entries gắn với account này.
    total += tx.execute(
        "DELETE FROM raw_shopee_clicks WHERE shopee_account_id = ?",
        params![id],
    )? as u64;
    total += tx.execute(
        "DELETE FROM raw_shopee_order_items WHERE shopee_account_id = ?",
        params![id],
    )? as u64;
    total += tx.execute(
        "DELETE FROM manual_entries WHERE shopee_account_id = ?",
        params![id],
    )? as u64;

    // Xóa account row.
    total += tx.execute(
        "DELETE FROM shopee_accounts WHERE id = ?",
        params![id],
    )? as u64;

    // Cleanup orphan mappings — clicks_to_file/orders_to_file/fb_ads_to_file
    // có thể còn link tới raw rows vừa xóa (mapping không có FK tới raw).
    // Mirror behavior production: delete_shopee_account KHÔNG cleanup mappings,
    // chỉ cleanup orphan imported_files. Để đồng bộ A/B, ta cũng skip mapping
    // cleanup ở đây (mappings sẽ bị kill khi imported_file orphan cleanup).
    //
    // Cleanup orphan imported_files (không còn raw row nào reference).
    total += tx.execute(
        "DELETE FROM imported_files
         WHERE id NOT IN (
             SELECT source_file_id FROM raw_shopee_clicks UNION
             SELECT source_file_id FROM raw_shopee_order_items UNION
             SELECT source_file_id FROM raw_fb_ads
         )",
        [],
    )? as u64;

    // Cleanup orphan days.
    total += tx.execute(
        "DELETE FROM days WHERE date NOT IN (
            SELECT day_date FROM raw_shopee_clicks UNION
            SELECT day_date FROM raw_shopee_order_items UNION
            SELECT day_date FROM raw_fb_ads UNION
            SELECT day_date FROM manual_entries
         )",
        [],
    )? as u64;

    Ok(total)
}

/// Parse tombstone `entity_key` format "{day}|{s1}|...|{s5}" → (day, sub_ids).
/// Copy từ v8 sync.rs (sẽ delete v8 version ở P8).
fn parse_tombstone_sub_key(key: &str) -> Option<(String, [String; 5])> {
    let parts: Vec<&str> = key.split('|').collect();
    if parts.len() != 6 {
        return None;
    }
    Some((
        parts[0].to_string(),
        [
            parts[1].to_string(),
            parts[2].to_string(),
            parts[3].to_string(),
            parts[4].to_string(),
            parts[5].to_string(),
        ],
    ))
}

// =============================================================
// Helpers
// =============================================================

/// INSERT OR IGNORE vào days — raw/manual events ref day_date qua FK.
fn ensure_day_exists(tx: &Transaction, day_date: &str) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO days(date, created_at) VALUES(?, ?)",
        params![day_date, chrono::Utc::now().to_rfc3339()],
    )
    .with_context(|| format!("ensure_day_exists({day_date})"))?;
    Ok(())
}

/// Query PRAGMA table_info để lấy column names thực tế trong local DB.
/// Cached không đáng vì chỉ gọi 1 lần per delta file (10-1000 events/file).
fn local_table_columns(tx: &Transaction, table: &str) -> Result<Vec<String>> {
    let mut stmt = tx
        .prepare(&format!("PRAGMA table_info({table})"))
        .with_context(|| format!("PRAGMA table_info({table})"))?;
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(1))
        .context("query table_info")?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("collect table_info")
}

/// Convert serde_json Value → rusqlite SqlValue. Numbers preserve i64 nếu fit,
/// else f64. Bool → 0/1 (SQLite convention). Arrays/objects → Null (defensive;
/// không expected trong raw table values).
fn json_to_sqlite(v: &Value) -> SqlValue {
    match v {
        Value::Null => SqlValue::Null,
        Value::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() {
                    SqlValue::Real(f)
                } else {
                    SqlValue::Null
                }
            } else {
                SqlValue::Null
            }
        }
        Value::String(s) => SqlValue::Text(s.clone()),
        Value::Array(_) | Value::Object(_) => SqlValue::Null,
    }
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate_for_tests;
    use crate::sync_v9::types::InsertEvent;
    use crate::sync_v9::SV_CURRENT;
    use rusqlite::Connection;
    use serde_json::json;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        let schema = include_str!("../db/schema.sql");
        conn.execute_batch(schema).unwrap();
        migrate_for_tests(&conn).unwrap();
        conn
    }

    fn insert_file_fixture(conn: &Connection, hash: &str) -> i64 {
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash)
             VALUES('f.csv', 'shopee_clicks', '2026-04-20T00:00:00Z', ?)",
            [hash],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    // ---------- INSERT ----------

    #[test]
    fn apply_insert_raw_click_auto_creates_day() {
        let mut conn = test_conn();
        let file_id = insert_file_fixture(&conn, "h1");
        let tx = conn.transaction().unwrap();

        let ev = DeltaEvent::Insert(InsertEvent {
            sv: SV_CURRENT,
            table: "raw_shopee_clicks".to_string(),
            pk: json!({"click_id": "c1"}),
            row: json!({
                "click_id": "c1",
                "click_time": "2026-04-20T10:00:00Z",
                "sub_id1": "s1",
                "day_date": "2026-04-20",
                "source_file_id": file_id,
            }),
            clock_ms: 1000,
        });
        let outcome = apply_event(&tx, &ev).unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied);

        let count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM raw_shopee_clicks WHERE click_id = 'c1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        // Day auto-inserted.
        let day_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM days WHERE date = '2026-04-20'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(day_count, 1);
    }

    #[test]
    fn apply_insert_dupe_pk_is_skipped() {
        let mut conn = test_conn();
        let file_id = insert_file_fixture(&conn, "h1");
        let tx = conn.transaction().unwrap();

        let ev = DeltaEvent::Insert(InsertEvent {
            sv: SV_CURRENT,
            table: "raw_shopee_clicks".to_string(),
            pk: json!({"click_id": "c1"}),
            row: json!({
                "click_id": "c1",
                "click_time": "2026-04-20T10:00:00Z",
                "day_date": "2026-04-20",
                "source_file_id": file_id,
            }),
            clock_ms: 1000,
        });
        assert_eq!(apply_event(&tx, &ev).unwrap(), ApplyOutcome::Applied);
        // Replay → skipped (PK conflict).
        assert_eq!(apply_event(&tx, &ev).unwrap(), ApplyOutcome::Skipped);
    }

    #[test]
    fn apply_insert_with_unknown_extra_column_is_tolerated() {
        // Event từ schema future có column 'new_col' mà local chưa có → skip col đó.
        let mut conn = test_conn();
        let file_id = insert_file_fixture(&conn, "h1");
        let tx = conn.transaction().unwrap();

        let ev = DeltaEvent::Insert(InsertEvent {
            sv: 99, // future schema version
            table: "raw_shopee_clicks".to_string(),
            pk: json!({"click_id": "c1"}),
            row: json!({
                "click_id": "c1",
                "click_time": "2026-04-20T10:00:00Z",
                "day_date": "2026-04-20",
                "source_file_id": file_id,
                "future_column": "ignore me",
            }),
            clock_ms: 1000,
        });
        assert_eq!(apply_event(&tx, &ev).unwrap(), ApplyOutcome::Applied);
    }

    // ---------- UPSERT ----------

    #[test]
    fn apply_upsert_hlc_wins_when_local_newer() {
        let mut conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, day_date, override_clicks, created_at, updated_at)
             VALUES('a', '2026-04-20', 999, 'now', '2026-04-24T10:00:00Z')",
            [],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        // Event older than local → should skip.
        let ev = DeltaEvent::Upsert(UpsertEvent {
            sv: SV_CURRENT,
            table: "manual_entries".to_string(),
            pk: json!({
                "sub_id1": "a", "sub_id2": "", "sub_id3": "",
                "sub_id4": "", "sub_id5": "", "day_date": "2026-04-20"
            }),
            row: json!({
                "sub_id1": "a", "sub_id2": "", "sub_id3": "",
                "sub_id4": "", "sub_id5": "",
                "day_date": "2026-04-20",
                "override_clicks": 111,
                "created_at": "older",
                "updated_at": "2026-04-24T08:00:00Z",
            }),
            updated_at: "2026-04-24T08:00:00Z".to_string(),
            clock_ms: 1000,
        });
        let outcome = apply_event(&tx, &ev).unwrap();
        assert_eq!(outcome, ApplyOutcome::SkippedByHlc);

        // Local value phải giữ nguyên (999, không 111).
        let clicks: i64 = tx
            .query_row(
                "SELECT override_clicks FROM manual_entries WHERE sub_id1 = 'a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(clicks, 999);
    }

    #[test]
    fn apply_upsert_applies_when_event_newer() {
        let mut conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, day_date, override_clicks, created_at, updated_at)
             VALUES('a', '2026-04-20', 100, 'now', '2026-04-24T08:00:00Z')",
            [],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        let ev = DeltaEvent::Upsert(UpsertEvent {
            sv: SV_CURRENT,
            table: "manual_entries".to_string(),
            pk: json!({
                "sub_id1": "a", "sub_id2": "", "sub_id3": "",
                "sub_id4": "", "sub_id5": "", "day_date": "2026-04-20"
            }),
            row: json!({
                "sub_id1": "a", "sub_id2": "", "sub_id3": "",
                "sub_id4": "", "sub_id5": "",
                "day_date": "2026-04-20",
                "override_clicks": 500,
                "created_at": "now",
                "updated_at": "2026-04-24T10:00:00Z",
            }),
            updated_at: "2026-04-24T10:00:00Z".to_string(),
            clock_ms: 2000,
        });
        assert_eq!(apply_event(&tx, &ev).unwrap(), ApplyOutcome::Applied);

        let clicks: i64 = tx
            .query_row(
                "SELECT override_clicks FROM manual_entries WHERE sub_id1 = 'a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(clicks, 500, "event thắng, value overwritten");
    }

    #[test]
    fn apply_upsert_inserts_when_no_local() {
        let mut conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        let ev = DeltaEvent::Upsert(UpsertEvent {
            sv: SV_CURRENT,
            table: "manual_entries".to_string(),
            pk: json!({
                "sub_id1": "new", "sub_id2": "", "sub_id3": "",
                "sub_id4": "", "sub_id5": "", "day_date": "2026-04-20"
            }),
            row: json!({
                "sub_id1": "new", "sub_id2": "", "sub_id3": "",
                "sub_id4": "", "sub_id5": "",
                "day_date": "2026-04-20",
                "override_clicks": 42,
                "created_at": "now",
                "updated_at": "2026-04-24T10:00:00Z",
            }),
            updated_at: "2026-04-24T10:00:00Z".to_string(),
            clock_ms: 1000,
        });
        assert_eq!(apply_event(&tx, &ev).unwrap(), ApplyOutcome::Applied);
    }

    // ---------- TOMBSTONES ----------

    #[test]
    fn apply_day_tombstone_cascades() {
        let mut conn = test_conn();
        let file_id = insert_file_fixture(&conn, "h1");
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO raw_shopee_clicks
             (click_id, click_time, day_date, source_file_id)
             VALUES('c1', 'now', '2026-04-20', ?)",
            [file_id],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "day".to_string(),
            entity_key: "2026-04-20".to_string(),
            deleted_at: "2026-04-24T10:00:00Z".to_string(),
            clock_ms: 1000,
        });
        let outcome = apply_event(&tx, &ev).unwrap();
        matches!(outcome, ApplyOutcome::TombstoneApplied { .. });

        // CASCADE: raw_shopee_clicks phải rỗng.
        let n: i64 = tx
            .query_row("SELECT COUNT(*) FROM raw_shopee_clicks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);

        // Tombstone audit row tồn tại.
        let n: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM tombstones WHERE entity_type = 'day' AND entity_key = '2026-04-20'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn apply_manual_entry_tombstone_respects_resurrect() {
        let mut conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        // Local row với updated_at NEWER than tombstone.
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, day_date, override_clicks, created_at, updated_at)
             VALUES('a', '2026-04-20', 1, 'now', '2026-04-24T12:00:00Z')",
            [],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "manual_entry".to_string(),
            entity_key: "2026-04-20|a||||".to_string(),
            deleted_at: "2026-04-24T10:00:00Z".to_string(), // older than local
            clock_ms: 1000,
        });
        let outcome = apply_event(&tx, &ev).unwrap();
        assert_eq!(outcome, ApplyOutcome::TombstoneNoOp, "resurrect: edit wins");

        let n: i64 = tx
            .query_row("SELECT COUNT(*) FROM manual_entries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "row survive");
    }

    #[test]
    fn apply_manual_entry_tombstone_deletes_older_row() {
        let mut conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, day_date, override_clicks, created_at, updated_at)
             VALUES('a', '2026-04-20', 1, 'now', '2026-04-24T08:00:00Z')",
            [],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "manual_entry".to_string(),
            entity_key: "2026-04-20|a||||".to_string(),
            deleted_at: "2026-04-24T10:00:00Z".to_string(), // newer than local
            clock_ms: 1000,
        });
        let outcome = apply_event(&tx, &ev).unwrap();
        matches!(outcome, ApplyOutcome::TombstoneApplied { rows_deleted: 1 });

        let n: i64 = tx
            .query_row("SELECT COUNT(*) FROM manual_entries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    // ---------- imported_file tombstone ----------

    /// Setup helper: máy B nhận được F1 (1 click + 1 mapping) từ delta replay.
    fn seed_file_with_click(conn: &Connection, hash: &str, click_id: &str) -> i64 {
        use crate::sync_v9::content_id;
        let file_id = content_id::imported_file_id(hash);
        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
             VALUES(?, 'f.csv', 'shopee_clicks', '2026-04-20T00:00:00Z', ?, '2026-04-20')",
            params![file_id, hash],
        )
        .unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO raw_shopee_clicks
             (click_id, click_time, day_date, source_file_id)
             VALUES(?, '2026-04-20T10:00:00Z', '2026-04-20', ?)",
            params![click_id, file_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO clicks_to_file(click_id, file_id) VALUES(?, ?)",
            params![click_id, file_id],
        )
        .unwrap();
        file_id
    }

    #[test]
    fn apply_imported_file_tombstone_cleans_orphan_raw_and_marks_reverted() {
        let mut conn = test_conn();
        let file_id = seed_file_with_click(&conn, "h-revert", "c1");

        let tx = conn.transaction().unwrap();
        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "imported_file".to_string(),
            entity_key: file_id.to_string(),
            deleted_at: "2026-04-24T10:00:00Z".to_string(),
            clock_ms: 1000,
        });
        let outcome = apply_event(&tx, &ev).unwrap();
        assert!(matches!(outcome, ApplyOutcome::TombstoneApplied { .. }));

        // Mapping + raw orphan + day cleanup.
        let map_count: i64 = tx
            .query_row("SELECT COUNT(*) FROM clicks_to_file", [], |r| r.get(0))
            .unwrap();
        assert_eq!(map_count, 0, "mapping bị xóa");
        let raw_count: i64 = tx
            .query_row("SELECT COUNT(*) FROM raw_shopee_clicks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(raw_count, 0, "raw click orphan bị xóa");
        let day_count: i64 = tx
            .query_row("SELECT COUNT(*) FROM days", [], |r| r.get(0))
            .unwrap();
        assert_eq!(day_count, 0, "day orphan bị xóa");

        // imported_files row vẫn tồn tại với reverted_at set (history preserved).
        let reverted_at: Option<String> = tx
            .query_row(
                "SELECT reverted_at FROM imported_files WHERE id = ?",
                [file_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(reverted_at.as_deref(), Some("2026-04-24T10:00:00Z"));
    }

    #[test]
    fn apply_imported_file_tombstone_preserves_shared_raw_rows() {
        // 2 file F1, F2 cùng hash khác nhau, cùng reference click "c-shared"
        // qua mapping. Revert F1 → click vẫn còn vì F2 mapping vẫn link.
        let mut conn = test_conn();
        let f1 = seed_file_with_click(&conn, "h1", "c-shared");
        let f2 = {
            use crate::sync_v9::content_id;
            let id = content_id::imported_file_id("h2");
            conn.execute(
                "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
                 VALUES(?, 'f2.csv', 'shopee_clicks', '2026-04-20T00:00:00Z', 'h2', '2026-04-20')",
                params![id],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c-shared', ?)",
                [id],
            )
            .unwrap();
            id
        };

        let tx = conn.transaction().unwrap();
        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "imported_file".to_string(),
            entity_key: f1.to_string(),
            deleted_at: "2026-04-24T10:00:00Z".to_string(),
            clock_ms: 1000,
        });
        apply_event(&tx, &ev).unwrap();

        // Click vẫn sống vì F2 mapping còn link.
        let raw_count: i64 = tx
            .query_row("SELECT COUNT(*) FROM raw_shopee_clicks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(raw_count, 1, "raw row giữ nguyên vì file khác còn link");

        // F1 mapping bị xóa, F2 mapping còn.
        let f2_maps: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM clicks_to_file WHERE file_id = ?",
                [f2],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(f2_maps, 1);
    }

    #[test]
    fn apply_imported_file_tombstone_idempotent() {
        // Replay tombstone 2 lần (case A pull lại tombstone của chính mình)
        // → không lỗi, reverted_at không bị đè timestamp mới hơn.
        let mut conn = test_conn();
        let file_id = seed_file_with_click(&conn, "h-idem", "c1");

        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "imported_file".to_string(),
            entity_key: file_id.to_string(),
            deleted_at: "2026-04-24T10:00:00Z".to_string(),
            clock_ms: 1000,
        });
        let tx = conn.transaction().unwrap();
        apply_event(&tx, &ev).unwrap();
        let outcome2 = apply_event(&tx, &ev).unwrap();
        // Replay 2: mọi DELETE no-op + UPDATE guard `IS NULL` skip.
        assert_eq!(outcome2, ApplyOutcome::TombstoneNoOp);

        let reverted_at: String = tx
            .query_row(
                "SELECT reverted_at FROM imported_files WHERE id = ?",
                [file_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            reverted_at, "2026-04-24T10:00:00Z",
            "reverted_at không bị thay đổi trên replay"
        );
    }

    // ---------- shopee_account tombstone ----------

    /// Setup: tạo account "Shop A" + raw click + manual_entry gắn về account đó.
    fn seed_account_with_data(conn: &Connection, name: &str, click_id: &str) -> i64 {
        use crate::sync_v9::content_id;
        let acc_id = content_id::shopee_account_id(name);
        conn.execute(
            "INSERT OR IGNORE INTO shopee_accounts (id, name, color, created_at)
             VALUES (?, ?, '#000', '2026-04-20T00:00:00Z')",
            params![acc_id, name],
        )
        .unwrap();
        let file_id = content_id::imported_file_id(&format!("h-{name}"));
        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date, shopee_account_id)
             VALUES(?, 'f.csv', 'shopee_clicks', '2026-04-20T00:00:00Z', ?, '2026-04-20', ?)",
            params![file_id, format!("h-{name}"), acc_id],
        )
        .unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO raw_shopee_clicks
             (click_id, click_time, day_date, source_file_id, shopee_account_id)
             VALUES(?, '2026-04-20T10:00:00Z', '2026-04-20', ?, ?)",
            params![click_id, file_id, acc_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, day_date, override_clicks, created_at, updated_at, shopee_account_id)
             VALUES(?, '2026-04-20', 5, 'now', '2026-04-24T08:00:00Z', ?)",
            params![format!("sub-{name}"), acc_id],
        )
        .unwrap();
        acc_id
    }

    #[test]
    fn apply_shopee_account_tombstone_deletes_data_and_account() {
        let mut conn = test_conn();
        let acc_id = seed_account_with_data(&conn, "Shop A", "c1");

        let tx = conn.transaction().unwrap();
        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "shopee_account".to_string(),
            entity_key: acc_id.to_string(),
            deleted_at: "2026-04-25T08:00:00Z".to_string(),
            clock_ms: 1000,
        });
        let outcome = apply_event(&tx, &ev).unwrap();
        assert!(matches!(outcome, ApplyOutcome::TombstoneApplied { .. }));

        // Account + data downstream xóa hết.
        let acc_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM shopee_accounts WHERE id = ?",
                [acc_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(acc_count, 0, "account row deleted");

        let raw_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM raw_shopee_clicks WHERE shopee_account_id = ?",
                [acc_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(raw_count, 0, "raw rows of account deleted");

        let manual_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM manual_entries WHERE shopee_account_id = ?",
                [acc_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(manual_count, 0, "manual_entries of account deleted");

        // Orphan imported_files (chỉ refer tới deleted raw rows) cũng deleted.
        let file_count: i64 = tx
            .query_row("SELECT COUNT(*) FROM imported_files", [], |r| r.get(0))
            .unwrap();
        assert_eq!(file_count, 0, "orphan file deleted");

        // Day cleanup.
        let day_count: i64 = tx
            .query_row("SELECT COUNT(*) FROM days", [], |r| r.get(0))
            .unwrap();
        assert_eq!(day_count, 0, "orphan day deleted");
    }

    #[test]
    fn apply_shopee_account_tombstone_protects_default_account() {
        // CRITICAL: tombstone trỏ tới default account → no-op, KHÔNG xóa.
        // Kể cả phía A buggy gửi tombstone default, B phải bảo vệ.
        use crate::sync_v9::content_id;
        let mut conn = test_conn();
        let default_id = content_id::shopee_account_id(crate::db::DEFAULT_ACCOUNT_NAME);

        // Seed data gắn về default account để test không bị xóa nhầm.
        conn.execute(
            "INSERT OR IGNORE INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        let file_id = content_id::imported_file_id("h-default");
        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date, shopee_account_id)
             VALUES(?, 'f.csv', 'shopee_clicks', '2026-04-20T00:00:00Z', 'h-default', '2026-04-20', ?)",
            params![file_id, default_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO raw_shopee_clicks
             (click_id, click_time, day_date, source_file_id, shopee_account_id)
             VALUES('c1', '2026-04-20T10:00:00Z', '2026-04-20', ?, ?)",
            params![file_id, default_id],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "shopee_account".to_string(),
            entity_key: default_id.to_string(),
            deleted_at: "2026-04-25T08:00:00Z".to_string(),
            clock_ms: 1000,
        });
        let outcome = apply_event(&tx, &ev).unwrap();
        assert_eq!(outcome, ApplyOutcome::TombstoneNoOp, "default protected");

        // Default account row + data preserved.
        let acc_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM shopee_accounts WHERE id = ?",
                [default_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(acc_count, 1, "default account row preserved");
        let raw_count: i64 = tx
            .query_row("SELECT COUNT(*) FROM raw_shopee_clicks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(raw_count, 1, "default account raw data preserved");
    }

    #[test]
    fn apply_shopee_account_tombstone_preserves_other_accounts() {
        // Xóa Shop A → KHÔNG ảnh hưởng default + Shop B + data của họ.
        let mut conn = test_conn();
        let _shop_a = seed_account_with_data(&conn, "Shop A", "c-a");
        let shop_b = seed_account_with_data(&conn, "Shop B", "c-b");

        // Default account đã seed sẵn từ migrate_for_tests, thêm 1 raw row default.
        use crate::sync_v9::content_id;
        let default_id = content_id::shopee_account_id(crate::db::DEFAULT_ACCOUNT_NAME);
        let file_default = content_id::imported_file_id("h-default");
        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date, shopee_account_id)
             VALUES(?, 'f.csv', 'shopee_clicks', '2026-04-20T00:00:00Z', 'h-default', '2026-04-20', ?)",
            params![file_default, default_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO raw_shopee_clicks
             (click_id, click_time, day_date, source_file_id, shopee_account_id)
             VALUES('c-default', '2026-04-20T10:00:00Z', '2026-04-20', ?, ?)",
            params![file_default, default_id],
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        let shop_a_id = content_id::shopee_account_id("Shop A");
        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "shopee_account".to_string(),
            entity_key: shop_a_id.to_string(),
            deleted_at: "2026-04-25T08:00:00Z".to_string(),
            clock_ms: 1000,
        });
        apply_event(&tx, &ev).unwrap();

        // Shop A's data gone.
        let a_raw: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM raw_shopee_clicks WHERE shopee_account_id = ?",
                [shop_a_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(a_raw, 0);

        // Shop B's data preserved.
        let b_raw: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM raw_shopee_clicks WHERE shopee_account_id = ?",
                [shop_b],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(b_raw, 1, "Shop B raw preserved");
        let b_acc: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM shopee_accounts WHERE id = ?",
                [shop_b],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(b_acc, 1, "Shop B account preserved");

        // Default account's data preserved.
        let d_raw: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM raw_shopee_clicks WHERE shopee_account_id = ?",
                [default_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(d_raw, 1, "default raw preserved");
    }

    #[test]
    fn apply_shopee_account_tombstone_idempotent() {
        let mut conn = test_conn();
        let acc_id = seed_account_with_data(&conn, "Shop A", "c1");

        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "shopee_account".to_string(),
            entity_key: acc_id.to_string(),
            deleted_at: "2026-04-25T08:00:00Z".to_string(),
            clock_ms: 1000,
        });
        let tx = conn.transaction().unwrap();
        apply_event(&tx, &ev).unwrap();
        let outcome2 = apply_event(&tx, &ev).unwrap();
        assert_eq!(outcome2, ApplyOutcome::TombstoneNoOp, "replay no-op");
    }

    #[test]
    fn apply_shopee_account_tombstone_invalid_key_no_op() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "shopee_account".to_string(),
            entity_key: "not-a-number".to_string(),
            deleted_at: "2026-04-25T08:00:00Z".to_string(),
            clock_ms: 1000,
        });
        assert_eq!(apply_event(&tx, &ev).unwrap(), ApplyOutcome::TombstoneNoOp);
    }

    #[test]
    fn apply_imported_file_tombstone_invalid_key_no_op() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();
        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "imported_file".to_string(),
            entity_key: "not-a-number".to_string(),
            deleted_at: "2026-04-24T10:00:00Z".to_string(),
            clock_ms: 1000,
        });
        let outcome = apply_event(&tx, &ev).unwrap();
        assert_eq!(outcome, ApplyOutcome::TombstoneNoOp);
    }

    #[test]
    fn apply_tombstone_insert_idempotent() {
        let mut conn = test_conn();
        let tx = conn.transaction().unwrap();

        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: SV_CURRENT,
            entity_type: "day".to_string(),
            entity_key: "2026-04-20".to_string(),
            deleted_at: "2026-04-24T10:00:00Z".to_string(),
            clock_ms: 1000,
        });
        apply_event(&tx, &ev).unwrap();
        apply_event(&tx, &ev).unwrap(); // replay — UNIQUE prevents dup

        let n: i64 = tx
            .query_row("SELECT COUNT(*) FROM tombstones", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
    }

    // ---------- JSON ↔ SQLite conversion ----------

    #[test]
    fn json_to_sqlite_preserves_types() {
        matches!(json_to_sqlite(&Value::Null), SqlValue::Null);
        matches!(json_to_sqlite(&json!(42)), SqlValue::Integer(42));
        matches!(json_to_sqlite(&json!(3.14)), SqlValue::Real(_));
        matches!(json_to_sqlite(&json!("abc")), SqlValue::Text(_));
        matches!(json_to_sqlite(&json!(true)), SqlValue::Integer(1));
        matches!(json_to_sqlite(&json!(false)), SqlValue::Integer(0));
        // Arrays/objects → Null (defensive).
        matches!(json_to_sqlite(&json!([1, 2])), SqlValue::Null);
        matches!(json_to_sqlite(&json!({"k": "v"})), SqlValue::Null);
    }

    #[test]
    fn parse_tombstone_sub_key_extracts_parts() {
        let (day, subs) =
            parse_tombstone_sub_key("2026-04-20|s1|s2||s4|").expect("valid format");
        assert_eq!(day, "2026-04-20");
        assert_eq!(subs, ["s1", "s2", "", "s4", ""]);
    }

    #[test]
    fn parse_tombstone_sub_key_rejects_wrong_parts() {
        assert!(parse_tombstone_sub_key("too|few").is_none());
        assert!(parse_tombstone_sub_key("a|b|c|d|e|f|g").is_none());
    }
}
