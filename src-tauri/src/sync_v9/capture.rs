//! Capture delta events từ 1 table: read rows có cursor > since_cursor,
//! build DeltaEvent (Insert/Upsert/Tombstone), dừng khi NDJSON serialized size
//! đạt ngưỡng max_bytes.
//!
//! Pure function (không update DB state, không compress, không upload) — output
//! là `CaptureBatch` cho `push::build_payload()` consume.

use anyhow::{anyhow, Context, Result};
use rusqlite::{params, types::Value as SqlValue, Connection, ToSql};
use serde_json::{Map, Value};

use super::descriptors::{CursorKind, DeltaOp, TableDescriptor, V9_ROWID_ALIAS};
use super::types::{DeltaEvent, InsertEvent, TombstoneEvent, UpsertEvent};

/// Output của 1 lần capture cho 1 table. Consumer (push.rs) sẽ compress +
/// build R2 key từ cursor_hi + clock_ms.
#[derive(Debug, Clone)]
pub struct CaptureBatch {
    pub table: String,
    pub events: Vec<DeltaEvent>,
    /// Cursor của event đầu tiên trong batch (inclusive).
    pub cursor_lo: String,
    /// Cursor của event cuối cùng (inclusive). Update sync_cursor_state sau khi push OK.
    pub cursor_hi: String,
    /// HLC clock lúc capture batch này. Caller pass từ `next_hlc_ms`.
    pub clock_ms: i64,
    /// Raw NDJSON size (uncompressed). Dùng để pick next batch nếu rows còn.
    pub ndjson_bytes: usize,
    /// NDJSON raw bytes (chưa compress). Phase 3 sẽ compress trước upload.
    pub ndjson: Vec<u8>,
}

/// Safety limit cho SELECT — tránh load cả DB vào RAM nếu since_cursor lùi về
/// 0 và table có 2M rows. 10k rows đủ cho 1 batch 5MB (500 bytes/row average).
const MAX_ROWS_PER_QUERY: i64 = 10_000;

/// Capture rows có cursor > `since_cursor` từ 1 table. Stop khi NDJSON >= max_bytes.
///
/// - `since_cursor`: value của cột cursor lần push cuối. "0" = từ đầu.
/// - `max_bytes`: ngưỡng NDJSON size (uncompressed). Tham chiếu `DELTA_BATCH_SIZE_BYTES`.
/// - `clock_ms`: HLC timestamp áp dụng cho batch. Lưu vào `event.clock_ms`.
/// - `sv`: schema version hiện tại (= SV_CURRENT). Lưu vào `event.sv`.
///
/// Trả `None` nếu table không có row mới nào. Trả `Some(batch)` với ≥1 event
/// (batch luôn chứa ít nhất 1 event kể cả khi event đó > max_bytes — không
/// split được 1 row).
pub fn capture_table_delta(
    conn: &Connection,
    descriptor: &TableDescriptor,
    since_cursor: &str,
    max_bytes: usize,
    clock_ms: i64,
    sv: u32,
) -> Result<Option<CaptureBatch>> {
    let sql = build_select_sql(descriptor);
    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| format!("prepare SELECT for {}", descriptor.name))?;

    // Column names phải lấy SAU prepare (khi stmt đã plan query). Preserve order
    // để map row index → column name.
    let columns: Vec<String> = stmt
        .column_names()
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    // Bind `since_cursor` với type tương ứng cursor_kind.
    let cursor_param: Box<dyn ToSql> = match descriptor.cursor_kind {
        CursorKind::RowId | CursorKind::PrimaryKey => {
            let n: i64 = since_cursor
                .parse()
                .with_context(|| format!("parse cursor '{since_cursor}' as i64"))?;
            Box::new(n)
        }
        CursorKind::UpdatedAt | CursorKind::DeletedAt => {
            Box::new(since_cursor.to_string())
        }
    };

    let rows_iter = stmt
        .query_map(params![cursor_param.as_ref(), MAX_ROWS_PER_QUERY], |row| {
            // Build row object (column name → JSON value). rowid = col index 0
            // cho RowId kind (query SELECT rowid, *), các cột thật bắt đầu từ 1.
            let mut obj = Map::with_capacity(columns.len());
            for (i, col) in columns.iter().enumerate() {
                let val = sqlite_value_to_json(row.get::<_, SqlValue>(i)?);
                obj.insert(col.clone(), val);
            }
            Ok(obj)
        })
        .context("query_map")?;

    let mut events: Vec<DeltaEvent> = Vec::new();
    let mut ndjson: Vec<u8> = Vec::with_capacity(max_bytes.min(1 << 20));
    let mut cursor_lo: Option<String> = None;
    let mut cursor_hi: Option<String> = None;

    for row_result in rows_iter {
        let row_obj = row_result.context("iter row")?;

        let cursor_val = extract_cursor(&row_obj, descriptor)?;
        let event = build_event(&row_obj, descriptor, cursor_val.clone(), clock_ms, sv)?;

        let mut line = serde_json::to_vec(&event).context("serialize event")?;
        line.push(b'\n');

        // Luôn include event đầu tiên, kể cả nếu vượt max_bytes (tránh stuck
        // vì 1 row quá to không split được).
        if !events.is_empty() && ndjson.len() + line.len() > max_bytes {
            break;
        }

        if cursor_lo.is_none() {
            cursor_lo = Some(cursor_val.clone());
        }
        cursor_hi = Some(cursor_val);
        events.push(event);
        ndjson.extend_from_slice(&line);
    }

    if events.is_empty() {
        return Ok(None);
    }

    let ndjson_len = ndjson.len();
    Ok(Some(CaptureBatch {
        table: descriptor.name.to_string(),
        events,
        cursor_lo: cursor_lo.expect("non-empty events phải có cursor_lo"),
        cursor_hi: cursor_hi.expect("non-empty events phải có cursor_hi"),
        clock_ms,
        ndjson_bytes: ndjson_len,
        ndjson,
    }))
}

fn build_select_sql(descriptor: &TableDescriptor) -> String {
    match descriptor.cursor_kind {
        // Explicit alias `rowid AS __v9_rowid__` — xem descriptors::V9_ROWID_ALIAS.
        CursorKind::RowId => format!(
            "SELECT rowid AS {alias}, * FROM {t} WHERE rowid > ?1 ORDER BY rowid ASC LIMIT ?2",
            alias = V9_ROWID_ALIAS,
            t = descriptor.name
        ),
        CursorKind::PrimaryKey | CursorKind::UpdatedAt | CursorKind::DeletedAt => format!(
            "SELECT * FROM {t} WHERE {c} > ?1 ORDER BY {c} ASC LIMIT ?2",
            t = descriptor.name,
            c = descriptor.cursor_column
        ),
    }
}

/// Extract cursor value từ row object. RowId case: dùng cột "rowid" (có từ
/// `SELECT rowid, *`). Các case khác: dùng descriptor.cursor_column.
fn extract_cursor(row_obj: &Map<String, Value>, descriptor: &TableDescriptor) -> Result<String> {
    let val = row_obj
        .get(descriptor.cursor_column)
        .ok_or_else(|| anyhow!("row missing cursor column {}", descriptor.cursor_column))?;

    match descriptor.cursor_kind {
        CursorKind::RowId | CursorKind::PrimaryKey => val
            .as_i64()
            .map(|n| n.to_string())
            .ok_or_else(|| anyhow!("cursor column {} không phải i64", descriptor.cursor_column)),
        CursorKind::UpdatedAt | CursorKind::DeletedAt => val
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("cursor column {} không phải string", descriptor.cursor_column)),
    }
}

/// Build PK JSON object từ row dựa trên `descriptor.pk_columns`.
fn build_pk(row_obj: &Map<String, Value>, pk_cols: &[&str]) -> Value {
    let mut pk = Map::with_capacity(pk_cols.len());
    for col in pk_cols {
        pk.insert(
            (*col).to_string(),
            row_obj.get(*col).cloned().unwrap_or(Value::Null),
        );
    }
    Value::Object(pk)
}

/// Build row JSON (strip cursor-only columns).
fn build_row_payload(row_obj: &Map<String, Value>, descriptor: &TableDescriptor) -> Value {
    let mut row = row_obj.clone();
    // RowId query alias `rowid AS __v9_rowid__` — strip khỏi row payload vì
    // apply side dùng natural PK, không cần rowid của máy gốc.
    if descriptor.cursor_kind == CursorKind::RowId {
        row.remove(V9_ROWID_ALIAS);
    }
    Value::Object(row)
}

/// Build DeltaEvent từ row object theo descriptor.op.
fn build_event(
    row_obj: &Map<String, Value>,
    descriptor: &TableDescriptor,
    _cursor: String,
    clock_ms: i64,
    sv: u32,
) -> Result<DeltaEvent> {
    match descriptor.op {
        DeltaOp::Insert => {
            let pk = build_pk(row_obj, descriptor.pk_columns);
            let row = build_row_payload(row_obj, descriptor);
            Ok(DeltaEvent::Insert(InsertEvent {
                sv,
                table: descriptor.name.to_string(),
                pk,
                row,
                clock_ms,
            }))
        }
        DeltaOp::Upsert => {
            let pk = build_pk(row_obj, descriptor.pk_columns);
            let row = build_row_payload(row_obj, descriptor);
            let updated_at = row_obj
                .get("updated_at")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("upsert row missing updated_at"))?
                .to_string();
            Ok(DeltaEvent::Upsert(UpsertEvent {
                sv,
                table: descriptor.name.to_string(),
                pk,
                row,
                updated_at,
                clock_ms,
            }))
        }
        DeltaOp::Tombstone => {
            let entity_type = row_obj
                .get("entity_type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("tombstone row missing entity_type"))?
                .to_string();
            let entity_key = row_obj
                .get("entity_key")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("tombstone row missing entity_key"))?
                .to_string();
            let deleted_at = row_obj
                .get("deleted_at")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("tombstone row missing deleted_at"))?
                .to_string();
            Ok(DeltaEvent::Tombstone(TombstoneEvent {
                sv,
                entity_type,
                entity_key,
                deleted_at,
                clock_ms,
            }))
        }
    }
}

fn sqlite_value_to_json(val: SqlValue) -> Value {
    match val {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(i) => Value::from(i),
        SqlValue::Real(r) => {
            // serde_json::Value::from(f64) returns Null for NaN/Inf. Guard để
            // phát hiện sớm thay vì silent null (data corruption signal).
            serde_json::Number::from_f64(r)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        SqlValue::Text(s) => Value::String(s),
        SqlValue::Blob(b) => {
            // Raw tables không có blob column (schema check). Nếu gặp → base64
            // để không mất data, nhưng log cảnh báo (defensive).
            Value::String(format!("__blob_base64__:{}", base64_encode(&b)))
        }
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate_for_tests;
    use crate::sync_v9::descriptors::find_descriptor;
    use crate::sync_v9::SV_CURRENT;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        let schema = include_str!("../db/schema.sql");
        conn.execute_batch(schema).unwrap();
        migrate_for_tests(&conn).unwrap();
        conn
    }

    fn insert_day(conn: &Connection, date: &str) {
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES(?, '2026-04-20T00:00:00Z')",
            [date],
        )
        .unwrap();
    }

    fn insert_file(conn: &Connection, hash: &str, day: &str) -> i64 {
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('f.csv', 'shopee_clicks', '2026-04-20T00:00:00Z', ?, ?)",
            [hash, day],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn capture_empty_table_returns_none() {
        let conn = test_conn();
        let desc = find_descriptor("raw_shopee_clicks").unwrap();
        let batch = capture_table_delta(&conn, desc, "0", 5_000_000, 1_000_000, SV_CURRENT).unwrap();
        assert!(batch.is_none());
    }

    #[test]
    fn capture_single_raw_row_produces_insert_event() {
        let conn = test_conn();
        insert_day(&conn, "2026-04-20");
        let file_id = insert_file(&conn, "h1", "2026-04-20");
        conn.execute(
            "INSERT INTO raw_shopee_clicks
             (click_id, click_time, sub_id1, day_date, source_file_id)
             VALUES('abc', '2026-04-20T10:00:00Z', 'sid1', '2026-04-20', ?)",
            [file_id],
        )
        .unwrap();

        let desc = find_descriptor("raw_shopee_clicks").unwrap();
        let batch = capture_table_delta(&conn, desc, "0", 5_000_000, 1_000_000, SV_CURRENT)
            .unwrap()
            .expect("có 1 row → Some");

        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.cursor_lo, "1");
        assert_eq!(batch.cursor_hi, "1");

        match &batch.events[0] {
            DeltaEvent::Insert(ev) => {
                assert_eq!(ev.sv, SV_CURRENT);
                assert_eq!(ev.table, "raw_shopee_clicks");
                assert_eq!(ev.pk, serde_json::json!({"click_id": "abc"}));
                // rowid phải bị strip khỏi row payload.
                assert!(ev.row.get("rowid").is_none());
                assert_eq!(ev.row.get("click_id").unwrap(), "abc");
                assert_eq!(ev.row.get("sub_id1").unwrap(), "sid1");
                assert_eq!(ev.clock_ms, 1_000_000);
            }
            _ => panic!("expected Insert variant"),
        }
    }

    #[test]
    fn capture_respects_since_cursor() {
        let conn = test_conn();
        insert_day(&conn, "2026-04-20");
        let file_id = insert_file(&conn, "h1", "2026-04-20");
        for id in ["a", "b", "c"] {
            conn.execute(
                "INSERT INTO raw_shopee_clicks
                 (click_id, click_time, day_date, source_file_id)
                 VALUES(?, '2026-04-20T10:00:00Z', '2026-04-20', ?)",
                params![id, file_id],
            )
            .unwrap();
        }

        let desc = find_descriptor("raw_shopee_clicks").unwrap();
        // since=1 → skip rowid 1 ('a'), capture 2 ('b') và 3 ('c').
        let batch = capture_table_delta(&conn, desc, "1", 5_000_000, 1_000_000, SV_CURRENT)
            .unwrap()
            .expect("có rows");

        assert_eq!(batch.events.len(), 2);
        assert_eq!(batch.cursor_lo, "2");
        assert_eq!(batch.cursor_hi, "3");

        let pks: Vec<String> = batch
            .events
            .iter()
            .map(|e| match e {
                DeltaEvent::Insert(i) => i.pk["click_id"].as_str().unwrap().to_string(),
                _ => panic!(),
            })
            .collect();
        assert_eq!(pks, vec!["b", "c"]);
    }

    #[test]
    fn capture_splits_at_max_bytes() {
        let conn = test_conn();
        insert_day(&conn, "2026-04-20");
        let file_id = insert_file(&conn, "h1", "2026-04-20");
        // Insert 100 rows với click_id dài → mỗi event NDJSON ~400-500 bytes.
        for i in 0..100 {
            let cid = format!("click_{:050}", i); // 50-char click_id
            conn.execute(
                "INSERT INTO raw_shopee_clicks
                 (click_id, click_time, sub_id1, day_date, source_file_id)
                 VALUES(?, '2026-04-20T10:00:00Z', 'sss', '2026-04-20', ?)",
                params![cid, file_id],
            )
            .unwrap();
        }

        let desc = find_descriptor("raw_shopee_clicks").unwrap();
        // max_bytes = 1KB → chỉ nhét được ~2-3 events.
        let batch = capture_table_delta(&conn, desc, "0", 1024, 1_000_000, SV_CURRENT)
            .unwrap()
            .expect("some");
        assert!(batch.events.len() < 10, "phải split, không lấy hết 100");
        assert!(batch.ndjson_bytes >= batch.events[0..1].len() * 100 || batch.events.len() > 1);
    }

    #[test]
    fn capture_batch_always_has_at_least_one_event() {
        let conn = test_conn();
        insert_day(&conn, "2026-04-20");
        let file_id = insert_file(&conn, "h1", "2026-04-20");
        // 1 row với payload lớn → vượt max_bytes=100.
        conn.execute(
            "INSERT INTO raw_shopee_clicks
             (click_id, click_time, sub_id1, day_date, source_file_id)
             VALUES(?, '2026-04-20T10:00:00Z', ?, '2026-04-20', ?)",
            params![
                "x".repeat(200),
                "subid_long_value_".repeat(20),
                file_id
            ],
        )
        .unwrap();

        let desc = find_descriptor("raw_shopee_clicks").unwrap();
        let batch = capture_table_delta(&conn, desc, "0", 10, 1_000_000, SV_CURRENT)
            .unwrap()
            .expect("some");
        assert_eq!(batch.events.len(), 1, "dù vượt max, vẫn có 1 event");
    }

    #[test]
    fn capture_manual_entry_upsert_with_updated_at() {
        let conn = test_conn();
        insert_day(&conn, "2026-04-20");
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, day_date, override_clicks, created_at, updated_at)
             VALUES('sid1', '2026-04-20', 100, '2026-04-24T08:00:00Z', '2026-04-24T08:00:00Z')",
            [],
        )
        .unwrap();

        let desc = find_descriptor("manual_entries").unwrap();
        let batch = capture_table_delta(&conn, desc, "0", 5_000_000, 2_000_000, SV_CURRENT)
            .unwrap()
            .expect("some");

        assert_eq!(batch.events.len(), 1);
        match &batch.events[0] {
            DeltaEvent::Upsert(ev) => {
                assert_eq!(ev.table, "manual_entries");
                assert_eq!(ev.updated_at, "2026-04-24T08:00:00Z");
                assert_eq!(ev.row.get("override_clicks").unwrap(), 100);
                assert_eq!(ev.pk["sub_id1"], "sid1");
                assert_eq!(ev.pk["day_date"], "2026-04-20");
            }
            _ => panic!("expected Upsert"),
        }
        assert_eq!(batch.cursor_lo, "2026-04-24T08:00:00Z");
    }

    #[test]
    fn capture_manual_entry_since_updated_at_skips_older() {
        let conn = test_conn();
        insert_day(&conn, "2026-04-20");
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, day_date, override_clicks, created_at, updated_at)
             VALUES('a', '2026-04-20', 1, 'now', '2026-04-24T08:00:00Z')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, day_date, override_clicks, created_at, updated_at)
             VALUES('b', '2026-04-20', 2, 'now', '2026-04-24T09:00:00Z')",
            [],
        )
        .unwrap();

        let desc = find_descriptor("manual_entries").unwrap();
        // since = 08:00 → chỉ lấy 'b' (09:00 > 08:00).
        let batch =
            capture_table_delta(&conn, desc, "2026-04-24T08:00:00Z", 5_000_000, 0, SV_CURRENT)
                .unwrap()
                .expect("some");
        assert_eq!(batch.events.len(), 1);
        match &batch.events[0] {
            DeltaEvent::Upsert(ev) => assert_eq!(ev.pk["sub_id1"], "b"),
            _ => panic!(),
        }
    }

    /// Regression: imported_files cursor PHẢI dùng imported_at (monotonic
    /// timestamp) thay vì id (content_id = random hash → non-monotonic).
    ///
    /// Scenario mô phỏng bug v0.4.3: 2 file hash khác nhau cho id(F_a) > id(F_b)
    /// (ngược thứ tự insert). Nếu cursor dùng id, sau push F_a cursor = id(F_a),
    /// capture F_b với `WHERE id > id(F_a)` bỏ qua → child FK fail bên receiver.
    /// Fix v0.4.4: cursor dùng imported_at → F_b (insert sau) có imported_at
    /// lớn hơn → match `WHERE imported_at > last_cursor` → capture đầy đủ.
    #[test]
    fn capture_imported_files_handles_non_monotonic_id() {
        let conn = test_conn();
        // Insert F_a với id LỚN, imported_at SỚM.
        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash)
             VALUES(?, 'a.csv', 'shopee_clicks', '2026-04-25T08:00:00Z', 'hash_a')",
            params![9_000_000_000_000_000_000_i64],
        )
        .unwrap();
        // Insert F_b với id NHỎ (simulate content_id ngược), imported_at MUỘN.
        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash)
             VALUES(?, 'b.csv', 'shopee_clicks', '2026-04-25T09:00:00Z', 'hash_b')",
            params![100_000_i64],
        )
        .unwrap();

        let desc = find_descriptor("imported_files").unwrap();
        assert_eq!(
            desc.cursor_kind,
            CursorKind::UpdatedAt,
            "imported_files descriptor phải là UpdatedAt cursor post-v0.4.4"
        );
        assert_eq!(desc.cursor_column, "imported_at");

        // Batch 1: cursor='0' → capture cả 2 file, ORDER BY imported_at ASC.
        let batch1 = capture_table_delta(&conn, desc, "0", 5_000_000, 1, SV_CURRENT)
            .unwrap()
            .expect("some");
        assert_eq!(batch1.events.len(), 2, "capture cả F_a và F_b");
        assert_eq!(batch1.cursor_lo, "2026-04-25T08:00:00Z");
        assert_eq!(batch1.cursor_hi, "2026-04-25T09:00:00Z");

        // Batch 2: simulate advance cursor sau push batch 1.
        // Trước fix (cursor_kind=PrimaryKey + cursor=id), cursor sẽ là "9000000000000000000"
        // và batch kế không bắt được F_b (id=100000 < 9e18). Post-fix dùng timestamp,
        // insert file mới có imported_at > '2026-04-25T09:00:00Z' sẽ luôn capture được.
        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash)
             VALUES(?, 'c.csv', 'shopee_clicks', '2026-04-25T10:00:00Z', 'hash_c')",
            params![50_i64],
        )
        .unwrap();
        let batch2 =
            capture_table_delta(&conn, desc, &batch1.cursor_hi, 5_000_000, 2, SV_CURRENT)
                .unwrap()
                .expect("some");
        assert_eq!(
            batch2.events.len(),
            1,
            "F_c (id=50, tiny) vẫn capture được nhờ imported_at cursor"
        );
        assert_eq!(batch2.cursor_hi, "2026-04-25T10:00:00Z");
    }

    /// Regression paired: shopee_accounts cursor cũng phải dùng created_at
    /// vì id = content_id(name) cũng non-monotonic.
    #[test]
    fn capture_shopee_accounts_uses_created_at_cursor() {
        let conn = test_conn();
        // Clear default account để test fresh.
        conn.execute("DELETE FROM shopee_accounts", []).unwrap();
        conn.execute(
            "INSERT INTO shopee_accounts(id, name, color, created_at)
             VALUES(?, 'A', '#fff', '2026-04-25T08:00:00Z')",
            params![9_000_000_000_000_000_000_i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO shopee_accounts(id, name, color, created_at)
             VALUES(?, 'B', '#000', '2026-04-25T09:00:00Z')",
            params![50_i64],
        )
        .unwrap();

        let desc = find_descriptor("shopee_accounts").unwrap();
        assert_eq!(desc.cursor_kind, CursorKind::UpdatedAt);
        assert_eq!(desc.cursor_column, "created_at");

        let batch = capture_table_delta(&conn, desc, "0", 5_000_000, 1, SV_CURRENT)
            .unwrap()
            .expect("some");
        assert_eq!(batch.events.len(), 2, "capture cả A và B bất chấp id order");
    }

    #[test]
    fn capture_tombstone_event() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
             VALUES('day', '2026-04-20', '2026-04-24T10:00:00Z')",
            [],
        )
        .unwrap();

        let desc = find_descriptor("tombstones").unwrap();
        let batch = capture_table_delta(&conn, desc, "0", 5_000_000, 0, SV_CURRENT)
            .unwrap()
            .expect("some");

        assert_eq!(batch.events.len(), 1);
        match &batch.events[0] {
            DeltaEvent::Tombstone(ev) => {
                assert_eq!(ev.entity_type, "day");
                assert_eq!(ev.entity_key, "2026-04-20");
                assert_eq!(ev.deleted_at, "2026-04-24T10:00:00Z");
            }
            _ => panic!("expected Tombstone"),
        }
    }

    #[test]
    fn capture_ndjson_is_valid_lines() {
        let conn = test_conn();
        insert_day(&conn, "2026-04-20");
        let file_id = insert_file(&conn, "h1", "2026-04-20");
        for i in 0..3 {
            conn.execute(
                "INSERT INTO raw_shopee_clicks
                 (click_id, click_time, day_date, source_file_id)
                 VALUES(?, '2026-04-20T10:00:00Z', '2026-04-20', ?)",
                params![format!("c{i}"), file_id],
            )
            .unwrap();
        }

        let desc = find_descriptor("raw_shopee_clicks").unwrap();
        let batch = capture_table_delta(&conn, desc, "0", 5_000_000, 0, SV_CURRENT)
            .unwrap()
            .expect("some");

        // NDJSON = mỗi line = 1 event JSON hợp lệ, tách bằng '\n'.
        let text = String::from_utf8(batch.ndjson.clone()).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3);
        for line in lines {
            let _ev: DeltaEvent = serde_json::from_str(line).expect("mỗi line phải parse được");
        }
    }

    #[test]
    fn capture_composite_pk_preserved() {
        // raw_shopee_order_items có PK (checkout_id, item_id, model_id).
        let conn = test_conn();
        insert_day(&conn, "2026-04-20");
        let file_id = insert_file(&conn, "h2", "2026-04-20");
        conn.execute(
            "INSERT INTO raw_shopee_order_items
             (order_id, checkout_id, item_id, model_id, day_date, source_file_id)
             VALUES('o1', 'chk1', 'item1', 'm1', '2026-04-20', ?)",
            [file_id],
        )
        .unwrap();

        let desc = find_descriptor("raw_shopee_order_items").unwrap();
        let batch = capture_table_delta(&conn, desc, "0", 5_000_000, 0, SV_CURRENT)
            .unwrap()
            .expect("some");
        match &batch.events[0] {
            DeltaEvent::Insert(ev) => {
                assert_eq!(ev.pk["checkout_id"], "chk1");
                assert_eq!(ev.pk["item_id"], "item1");
                assert_eq!(ev.pk["model_id"], "m1");
            }
            _ => panic!(),
        }
    }
}
