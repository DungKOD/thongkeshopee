//! Apply delta events vào local DB (pull side).
//!
//! Mỗi event `InsertEvent` / `UpsertEvent` / `TombstoneEvent` được dispatch
//! tới handler riêng. Caller wrap trong transaction (xem `pull::apply_delta_file`).
//!
//! Invariants (rule giữ data #1):
//! - Insert = INSERT OR IGNORE (PK conflict = idempotent replay, không mất local state)
//! - Upsert = INSERT OR REPLACE CHỈ khi `local.updated_at <= event.updated_at` (HLC wins)
//! - Tombstone day = CASCADE delete unconditional (user intent)
//! - Tombstone manual_entry / ui_row = DELETE CHỈ nếu `target.updated_at <= tombstone.deleted_at`
//!   (resurrect rule — edit sau delete → row survive)
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
        "day" => apply_day_tombstone(tx, &ev.entity_key)?,
        "manual_entry" => apply_manual_entry_tombstone(tx, &ev.entity_key, &ev.deleted_at)?,
        "ui_row" => apply_ui_row_tombstone(tx, &ev.entity_key, &ev.deleted_at)?,
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

/// Day tombstone = CASCADE unconditional (plan B5 locked).
fn apply_day_tombstone(tx: &Transaction, day_date: &str) -> Result<u64> {
    let n = tx.execute("DELETE FROM days WHERE date = ?", [day_date])
        .context("delete day")?;
    Ok(n as u64)
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
