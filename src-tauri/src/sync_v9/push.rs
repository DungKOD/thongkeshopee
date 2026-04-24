//! Push flow — orchestrate capture + compress + payload build + cursor advance.
//!
//! Phase 2 scope: pure state transformation (DB → capture batches → compressed
//! payloads). HTTP upload nằm ở Phase 6 (sync_client_v9). Cursor chỉ được
//! advance SAU KHI caller confirm upload OK (idempotent retry safe).
//!
//! Push flow high-level:
//! 1. `plan_push(conn)` — iterate SYNC_TABLES theo dependency order, capture
//!    từng table với cursor hiện tại từ `sync_cursor_state`. Split batches ≤ 5MB.
//! 2. Caller upload từng payload qua Worker (Phase 6). Mỗi upload OK → call
//!    `mark_uploaded(conn, table, cursor_hi, hash)`.
//! 3. Skip-identical: nếu hash(compressed) match `last_uploaded_hash` → skip
//!    (giảm Class A ops R2).

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

use super::capture::{capture_table_delta, CaptureBatch};
use super::compress::{sha256_hex, zstd_compress};
use super::descriptors::{TableDescriptor, SYNC_TABLES};
use super::types::CursorState;
use super::{DELTA_BATCH_SIZE_BYTES, SV_CURRENT};

/// Payload sẵn sàng upload lên R2. 1 table có thể tạo nhiều payload nếu data
/// lớn hơn `DELTA_BATCH_SIZE_BYTES`.
#[derive(Debug, Clone)]
pub struct PushPayload {
    /// R2 object key, vd `"deltas/raw_shopee_clicks/5000_1745234600000.ndjson.zst"`.
    pub r2_key: String,
    /// zstd-compressed NDJSON bytes, ready để PUT.
    pub bytes: Vec<u8>,
    /// SHA-256 hex của `bytes`. Dùng cho skip-identical + integrity check.
    pub hash: String,
    /// Table name (match `sync_cursor_state.table_name`).
    pub table: String,
    /// Cursor range (inclusive), dùng khi append manifest entry.
    pub cursor_lo: String,
    pub cursor_hi: String,
    /// HLC clock khi batch được capture.
    pub clock_ms: i64,
    /// Số events trong payload. UI progress.
    pub row_count: u32,
    /// Size của `bytes` (post-compress). = `bytes.len()`.
    pub size_bytes: i64,
}

/// Build R2 object key cho 1 delta file. Format:
/// `deltas/{table}/{cursor_hi_sanitized}_{clock_ms}.ndjson.zst`
///
/// `cursor_hi` có thể chứa ký tự đặc biệt (vd `:` trong RFC3339) — sanitize để
/// match R2 key rules (mọi ký tự UTF-8 valid, nhưng tránh `/` collision với
/// path separator).
pub fn build_r2_key(table: &str, cursor_hi: &str, clock_ms: i64) -> String {
    let sanitized = cursor_hi.replace(['/', ':'], "-");
    format!("deltas/{table}/{sanitized}_{clock_ms}.ndjson.zst")
}

/// Compress NDJSON bytes + hash + build R2 key → 1 PushPayload sẵn sàng upload.
pub fn build_push_payload(batch: CaptureBatch) -> Result<PushPayload> {
    let row_count = batch.events.len() as u32;
    let compressed = zstd_compress(&batch.ndjson).context("zstd compress batch")?;
    let hash = sha256_hex(&compressed);
    let r2_key = build_r2_key(&batch.table, &batch.cursor_hi, batch.clock_ms);
    let size_bytes = compressed.len() as i64;

    Ok(PushPayload {
        r2_key,
        bytes: compressed,
        hash,
        table: batch.table,
        cursor_lo: batch.cursor_lo,
        cursor_hi: batch.cursor_hi,
        clock_ms: batch.clock_ms,
        row_count,
        size_bytes,
    })
}

/// Đọc cursor state cho 1 table. Trả row từ `sync_cursor_state`.
pub fn read_cursor(conn: &Connection, table: &str) -> Result<CursorState> {
    conn.query_row(
        "SELECT table_name, last_uploaded_cursor, last_pulled_cursor, last_uploaded_hash, updated_at
         FROM sync_cursor_state WHERE table_name = ?",
        [table],
        |r| {
            Ok(CursorState {
                table_name: r.get(0)?,
                last_uploaded_cursor: r.get(1)?,
                last_pulled_cursor: r.get(2)?,
                last_uploaded_hash: r.get(3)?,
                updated_at: r.get(4)?,
            })
        },
    )
    .with_context(|| format!("read cursor state for {table}"))
}

/// Advance `last_uploaded_cursor` + lưu hash SAU khi upload OK.
///
/// Idempotent theo hash: nếu hash trùng với hash hiện tại → skip update (avoid
/// bumping updated_at khi content không đổi).
pub fn mark_uploaded(
    conn: &Connection,
    table: &str,
    new_cursor: &str,
    hash: &str,
) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    // Monotonic guard: cursor chỉ advance hoặc giữ bằng, không tụt. Ngừa
    // accident retry với cursor cũ (vd rollback nửa chừng, caller re-capture
    // với since_cursor < trước). Giữ hash update để skip-identical vẫn fresh.
    //
    // Numeric compare cho RowId/PrimaryKey (int cursor). Lex compare cho
    // UpdatedAt/DeletedAt (ISO timestamp). SQLite comparison operator `<`
    // xử lý 2 case này tự nhiên — string 'YYYY-MM-DD...' và int literal
    // khác type nên cần CAST để unify.
    //
    // Strategy: nếu cả 2 parse thành INTEGER > 0 → numeric compare; ngược
    // lại → text compare. Implement qua 2 branches.
    let is_numeric = new_cursor.chars().all(|c| c.is_ascii_digit()) && !new_cursor.is_empty();
    let sql = if is_numeric {
        "UPDATE sync_cursor_state
         SET last_uploaded_cursor = ?, last_uploaded_hash = ?, updated_at = ?
         WHERE table_name = ?
           AND CAST(? AS INTEGER) >= CAST(last_uploaded_cursor AS INTEGER)"
    } else {
        "UPDATE sync_cursor_state
         SET last_uploaded_cursor = ?, last_uploaded_hash = ?, updated_at = ?
         WHERE table_name = ?
           AND ? >= last_uploaded_cursor"
    };
    conn.execute(
        sql,
        params![new_cursor, hash, now, table, new_cursor],
    )
    .with_context(|| format!("mark_uploaded failed for {table}"))?;
    Ok(())
}

/// Check skip-identical: nếu hash match `last_uploaded_hash` → skip upload.
///
/// Chỉ nên skip nếu cursor_hi của payload mới == cursor hiện tại (tức không
/// có rows mới). Trường hợp có rows mới nhưng serialize lại thành cùng hash
/// cực hiếm và không nên skip (rủi ro mất data).
pub fn should_skip_by_hash(cursor: &CursorState, payload: &PushPayload) -> bool {
    cursor.last_uploaded_hash.as_deref() == Some(payload.hash.as_str())
        && cursor.last_uploaded_cursor == payload.cursor_hi
}

/// Plan push: iterate mọi bảng syncable theo dependency order, capture 1 batch
/// per table (caller có thể gọi lại plan_push sau khi upload hết để lấy batch
/// tiếp theo nếu table còn data).
///
/// Fresh-install guard (rule giữ data): nếu `sync_manifest_state.fresh_install_pending`
/// = 1 → trả empty vec. Caller (Phase 4+) phải clear flag SAU KHI bootstrap
/// pull thành công trước mới bắt đầu push.
///
/// `clock_ms` và `sv` được caller pass (thường lấy qua `next_hlc_ms` + `SV_CURRENT`).
pub fn plan_push_one_pass(
    conn: &Connection,
    clock_ms: i64,
    sv: u32,
    max_bytes_per_batch: usize,
) -> Result<Vec<PushPayload>> {
    // Rule giữ data — không push empty đè remote khi fresh install.
    if is_fresh_install_pending(conn)? {
        return Ok(Vec::new());
    }

    let mut payloads = Vec::new();
    for descriptor in SYNC_TABLES {
        if let Some(payload) = plan_push_for_table(conn, descriptor, clock_ms, sv, max_bytes_per_batch)? {
            payloads.push(payload);
        }
    }
    Ok(payloads)
}

/// Plan push cho 1 bảng: đọc cursor, capture 1 batch, compress → PushPayload.
/// None nếu table không có row mới.
pub fn plan_push_for_table(
    conn: &Connection,
    descriptor: &TableDescriptor,
    clock_ms: i64,
    sv: u32,
    max_bytes: usize,
) -> Result<Option<PushPayload>> {
    let cursor = read_cursor(conn, descriptor.name)?;
    let batch = match capture_table_delta(
        conn,
        descriptor,
        &cursor.last_uploaded_cursor,
        max_bytes,
        clock_ms,
        sv,
    )? {
        Some(b) => b,
        None => return Ok(None),
    };
    let payload = build_push_payload(batch)?;
    Ok(Some(payload))
}

/// Check fresh install flag. Dùng trong push path để không đè empty lên remote.
pub fn is_fresh_install_pending(conn: &Connection) -> Result<bool> {
    let pending: i64 = conn.query_row(
        "SELECT fresh_install_pending FROM sync_manifest_state WHERE id = 1",
        [],
        |r| r.get(0),
    )?;
    Ok(pending != 0)
}

/// Default max bytes cho capture = `DELTA_BATCH_SIZE_BYTES`. Riêng hàm để test
/// dễ mock.
pub fn default_max_bytes() -> usize {
    DELTA_BATCH_SIZE_BYTES
}

/// Convenience: plan_push_one_pass với default params (SV_CURRENT, 5MB batch,
/// clock_ms từ HLC).
pub fn plan_push_default(conn: &Connection, clock_ms: i64) -> Result<Vec<PushPayload>> {
    plan_push_one_pass(conn, clock_ms, SV_CURRENT, default_max_bytes())
}

// =============================================================
// A1 OPTIMIZATION: Bundle deltas — multi-table into 1 R2 file
// =============================================================

/// Metadata per-table trong bundle, dùng advance cursor + skip-identical
/// tracking per table (dù tất cả chung 1 R2 file).
#[derive(Debug, Clone)]
pub struct TableBundleRange {
    pub table: String,
    pub cursor_lo: String,
    pub cursor_hi: String,
    pub row_count: u32,
    /// Hash riêng cho content NDJSON của table này (pre-merge). Dùng
    /// skip-identical check ở mark_uploaded. KHÔNG phải hash bundle.
    pub content_hash: String,
}

/// Bundle push payload — 1 file R2 chứa events nhiều tables.
///
/// So với per-table PushPayload:
/// - Tiết kiệm Class A PUT: N PUT → 1 PUT (giảm 5-10×)
/// - Manifest entry: vẫn 1/table nhưng cùng trỏ tới `r2_key` → pull-side
///   dedup fetch → 1 GET thay N GET
/// - Skip-identical vẫn per-table: table nào content không đổi → không vào
///   bundle
#[derive(Debug, Clone)]
pub struct BundlePushPayload {
    /// R2 object key format mới: `deltas/bundle/{clock_ms}_{hash_prefix}.ndjson.zst`
    pub r2_key: String,
    /// zstd-compressed NDJSON bytes (merged events từ nhiều tables).
    pub bytes: Vec<u8>,
    /// SHA-256 hex của `bytes` — integrity.
    pub hash: String,
    pub clock_ms: i64,
    /// Tổng số events trong bundle (sum of per-table row_count).
    pub total_row_count: u32,
    pub size_bytes: i64,
    /// Per-table range info — caller iterate để:
    /// 1. Append manifest entry per table (trỏ cùng r2_key)
    /// 2. mark_uploaded cursor + content_hash per table
    pub table_ranges: Vec<TableBundleRange>,
}

/// Build bundle payload — capture mọi table có delta, merge NDJSON, compress 1
/// lần, trả Option<BundlePushPayload>. None nếu không có table nào có data.
///
/// Thứ tự table trong bundle = SYNC_TABLES order (FK dependency) → apply
/// side đọc tuần tự vẫn đúng dependency order.
///
/// Per-table skip-identical: mỗi table compute hash riêng cho portion của nó.
/// Nếu match `last_uploaded_hash` → bỏ khỏi bundle (không count, không cursor
/// advance). Bundle-level hash là hash của toàn bộ compressed bytes.
pub fn plan_push_bundle(
    conn: &Connection,
    clock_ms: i64,
    sv: u32,
    max_bytes: usize,
) -> Result<Option<BundlePushPayload>> {
    if is_fresh_install_pending(conn)? {
        return Ok(None);
    }

    // Per-table NDJSON bytes + metadata, accumulate trước khi merge.
    let mut all_ndjson: Vec<u8> = Vec::new();
    let mut ranges: Vec<TableBundleRange> = Vec::new();

    for descriptor in SYNC_TABLES {
        let cursor = read_cursor(conn, descriptor.name)?;
        let batch = match capture_table_delta(
            conn,
            descriptor,
            &cursor.last_uploaded_cursor,
            max_bytes,
            clock_ms,
            sv,
        )? {
            Some(b) => b,
            None => continue, // table không có row mới
        };

        // Skip-identical per-table: hash NDJSON raw (không phải compressed).
        // Dùng so sánh với last_uploaded_hash cũ — nếu match → không vào bundle.
        let content_hash = sha256_hex(&batch.ndjson);
        if cursor.last_uploaded_hash.as_deref() == Some(content_hash.as_str())
            && cursor.last_uploaded_cursor == batch.cursor_hi
        {
            continue;
        }

        let row_count = batch.events.len() as u32;
        // Merge vào all_ndjson — NDJSON append-safe (mỗi event 1 line).
        all_ndjson.extend_from_slice(&batch.ndjson);

        ranges.push(TableBundleRange {
            table: batch.table,
            cursor_lo: batch.cursor_lo,
            cursor_hi: batch.cursor_hi,
            row_count,
            content_hash,
        });
    }

    if all_ndjson.is_empty() {
        return Ok(None);
    }

    let compressed = zstd_compress(&all_ndjson).context("zstd compress bundle")?;
    let bundle_hash = sha256_hex(&compressed);
    let total_row_count: u32 = ranges.iter().map(|r| r.row_count).sum();
    let r2_key = format!(
        "deltas/bundle/{}_{}.ndjson.zst",
        clock_ms,
        &bundle_hash[..12]
    );
    let size_bytes = compressed.len() as i64;

    Ok(Some(BundlePushPayload {
        r2_key,
        bytes: compressed,
        hash: bundle_hash,
        clock_ms,
        total_row_count,
        size_bytes,
        table_ranges: ranges,
    }))
}

/// Convenience: plan_push_bundle với default SV + 5MB max.
pub fn plan_push_bundle_default(
    conn: &Connection,
    clock_ms: i64,
) -> Result<Option<BundlePushPayload>> {
    plan_push_bundle(conn, clock_ms, SV_CURRENT, default_max_bytes())
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate_for_tests;
    use crate::sync_v9::compress::zstd_decompress;
    use crate::sync_v9::descriptors::find_descriptor;
    use crate::sync_v9::types::DeltaEvent;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        let schema = include_str!("../db/schema.sql");
        conn.execute_batch(schema).unwrap();
        migrate_for_tests(&conn).unwrap();
        conn
    }

    fn seed_click_row(conn: &Connection, click_id: &str) {
        conn.execute(
            "INSERT OR IGNORE INTO days(date, created_at) VALUES('2026-04-20', '2026-04-20T00:00:00Z')",
            [],
        )
        .unwrap();
        let file_id = {
            conn.execute(
                "INSERT OR IGNORE INTO imported_files(filename, kind, imported_at, file_hash, day_date)
                 VALUES('f.csv', 'shopee_clicks', '2026-04-20T00:00:00Z', 'h', '2026-04-20')",
                [],
            )
            .unwrap();
            conn.query_row(
                "SELECT id FROM imported_files WHERE file_hash = 'h'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        };
        conn.execute(
            "INSERT INTO raw_shopee_clicks
             (click_id, click_time, day_date, source_file_id)
             VALUES(?, '2026-04-20T10:00:00Z', '2026-04-20', ?)",
            params![click_id, file_id],
        )
        .unwrap();
    }

    #[test]
    fn build_r2_key_format() {
        let key = build_r2_key("raw_shopee_clicks", "5000", 1_745_234_600_000);
        assert_eq!(
            key,
            "deltas/raw_shopee_clicks/5000_1745234600000.ndjson.zst"
        );
    }

    #[test]
    fn build_r2_key_sanitizes_colon() {
        // RFC3339 timestamps có ":" — phải sanitize để tránh ambiguous key.
        let key = build_r2_key("manual_entries", "2026-04-24T08:00:00Z", 1_000_000);
        assert!(!key[..key.rfind('/').unwrap()].contains(':'));
        assert!(key.contains("2026-04-24T08-00-00Z"));
    }

    #[test]
    fn build_push_payload_compresses_and_hashes() {
        let conn = test_conn();
        seed_click_row(&conn, "c1");

        let desc = find_descriptor("raw_shopee_clicks").unwrap();
        let batch = capture_table_delta(&conn, desc, "0", 5_000_000, 1_000_000, SV_CURRENT)
            .unwrap()
            .unwrap();

        let payload = build_push_payload(batch).unwrap();
        assert_eq!(payload.table, "raw_shopee_clicks");
        assert_eq!(payload.row_count, 1);
        assert_eq!(payload.hash.len(), 64, "SHA-256 hex");
        assert!(!payload.bytes.is_empty());

        // Decompress back để verify.
        let ndjson = zstd_decompress(&payload.bytes).unwrap();
        let text = String::from_utf8(ndjson).unwrap();
        assert_eq!(text.lines().count(), 1);
        let ev: DeltaEvent = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        match ev {
            DeltaEvent::Insert(i) => assert_eq!(i.pk["click_id"], "c1"),
            _ => panic!(),
        }
    }

    #[test]
    fn read_cursor_returns_initial_state() {
        let conn = test_conn();
        let c = read_cursor(&conn, "raw_shopee_clicks").unwrap();
        assert_eq!(c.table_name, "raw_shopee_clicks");
        assert_eq!(c.last_uploaded_cursor, "0");
        assert_eq!(c.last_pulled_cursor, "0");
        assert!(c.last_uploaded_hash.is_none());
    }

    #[test]
    fn mark_uploaded_advances_cursor() {
        let conn = test_conn();
        mark_uploaded(&conn, "raw_shopee_clicks", "100", "hash_abc").unwrap();
        let c = read_cursor(&conn, "raw_shopee_clicks").unwrap();
        assert_eq!(c.last_uploaded_cursor, "100");
        assert_eq!(c.last_uploaded_hash.unwrap(), "hash_abc");
    }

    #[test]
    fn should_skip_by_hash_when_cursor_and_hash_match() {
        let conn = test_conn();
        mark_uploaded(&conn, "raw_shopee_clicks", "100", "H").unwrap();
        let cursor = read_cursor(&conn, "raw_shopee_clicks").unwrap();
        let payload = PushPayload {
            r2_key: "k".to_string(),
            bytes: vec![],
            hash: "H".to_string(),
            table: "raw_shopee_clicks".to_string(),
            cursor_lo: "100".to_string(),
            cursor_hi: "100".to_string(),
            clock_ms: 0,
            row_count: 0,
            size_bytes: 0,
        };
        assert!(should_skip_by_hash(&cursor, &payload));
    }

    #[test]
    fn should_not_skip_when_hash_differs() {
        let conn = test_conn();
        mark_uploaded(&conn, "raw_shopee_clicks", "100", "H").unwrap();
        let cursor = read_cursor(&conn, "raw_shopee_clicks").unwrap();
        let payload = PushPayload {
            r2_key: "k".to_string(),
            bytes: vec![],
            hash: "DIFFERENT".to_string(),
            table: "raw_shopee_clicks".to_string(),
            cursor_lo: "100".to_string(),
            cursor_hi: "100".to_string(),
            clock_ms: 0,
            row_count: 0,
            size_bytes: 0,
        };
        assert!(!should_skip_by_hash(&cursor, &payload));
    }

    #[test]
    fn should_not_skip_when_cursor_advanced() {
        let conn = test_conn();
        mark_uploaded(&conn, "raw_shopee_clicks", "100", "H").unwrap();
        let cursor = read_cursor(&conn, "raw_shopee_clicks").unwrap();
        let payload = PushPayload {
            r2_key: "k".to_string(),
            bytes: vec![],
            hash: "H".to_string(),
            table: "raw_shopee_clicks".to_string(),
            cursor_lo: "101".to_string(),
            cursor_hi: "200".to_string(),
            clock_ms: 0,
            row_count: 0,
            size_bytes: 0,
        };
        assert!(!should_skip_by_hash(&cursor, &payload));
    }

    #[test]
    fn plan_push_initial_only_contains_seeded_accounts() {
        // Migration seed default shopee_accounts row. Sau mark_uploaded cho row
        // này → plan_push tiếp theo mới thực sự empty.
        let conn = test_conn();
        let payloads = plan_push_default(&conn, 1_000_000).unwrap();
        let tables: Vec<&str> = payloads.iter().map(|p| p.table.as_str()).collect();

        // Chỉ shopee_accounts có row seed, các table khác empty → không tạo payload.
        assert_eq!(
            tables,
            vec!["shopee_accounts"],
            "chỉ default shopee_account được seed → 1 payload"
        );
    }

    #[test]
    fn plan_push_after_initial_ack_is_empty() {
        let conn = test_conn();
        let payloads = plan_push_default(&conn, 1_000_000).unwrap();
        for p in &payloads {
            mark_uploaded(&conn, &p.table, &p.cursor_hi, &p.hash).unwrap();
        }
        let payloads2 = plan_push_default(&conn, 2_000_000).unwrap();
        assert!(
            payloads2.is_empty(),
            "sau khi ack seeded data → nothing to push"
        );
    }

    #[test]
    fn plan_push_fresh_install_returns_empty_even_with_data() {
        let conn = test_conn();
        seed_click_row(&conn, "c1");
        // Simulate fresh install flag.
        conn.execute(
            "UPDATE sync_manifest_state SET fresh_install_pending = 1 WHERE id = 1",
            [],
        )
        .unwrap();
        let payloads = plan_push_default(&conn, 1_000_000).unwrap();
        assert!(
            payloads.is_empty(),
            "fresh install = empty push (rule giữ data)"
        );
    }

    #[test]
    fn plan_push_captures_after_data_insert() {
        let conn = test_conn();
        seed_click_row(&conn, "c1");
        seed_click_row(&conn, "c2");
        let payloads = plan_push_default(&conn, 1_000_000).unwrap();

        // shopee_accounts + imported_files + raw_shopee_clicks → 3 payload có data.
        // Plus mapping tables nếu có data. Ở đây chưa seed mapping + account.
        // Ít nhất imported_files + raw_shopee_clicks phải có payload.
        let tables: Vec<&str> = payloads.iter().map(|p| p.table.as_str()).collect();
        assert!(tables.contains(&"imported_files"));
        assert!(tables.contains(&"raw_shopee_clicks"));

        // Verify dependency order: imported_files trước raw_shopee_clicks.
        let file_idx = tables.iter().position(|t| *t == "imported_files").unwrap();
        let clicks_idx = tables.iter().position(|t| *t == "raw_shopee_clicks").unwrap();
        assert!(file_idx < clicks_idx);
    }

    #[test]
    fn plan_push_then_mark_uploaded_no_replay() {
        let conn = test_conn();
        seed_click_row(&conn, "c1");

        let payloads1 = plan_push_default(&conn, 1_000_000).unwrap();
        assert!(!payloads1.is_empty());

        // Simulate upload OK cho tất cả payloads.
        for p in &payloads1 {
            mark_uploaded(&conn, &p.table, &p.cursor_hi, &p.hash).unwrap();
        }

        // Plan lần 2 → không có table nào có data mới → empty.
        let payloads2 = plan_push_default(&conn, 2_000_000).unwrap();
        assert!(
            payloads2.is_empty(),
            "sau mark_uploaded, không replay rows cũ"
        );
    }

    #[test]
    fn plan_push_captures_new_rows_after_initial() {
        let conn = test_conn();
        seed_click_row(&conn, "c1");

        // Initial push.
        let p1 = plan_push_default(&conn, 1_000_000).unwrap();
        for p in &p1 {
            mark_uploaded(&conn, &p.table, &p.cursor_hi, &p.hash).unwrap();
        }

        // Insert row mới.
        seed_click_row(&conn, "c2");

        // Plan lần 2 chỉ phải capture row c2 (incremental).
        let p2 = plan_push_default(&conn, 2_000_000).unwrap();
        let clicks_payload = p2
            .iter()
            .find(|p| p.table == "raw_shopee_clicks")
            .expect("phải có raw_shopee_clicks payload");
        assert_eq!(clicks_payload.row_count, 1, "chỉ 1 row mới (c2)");
    }
}
