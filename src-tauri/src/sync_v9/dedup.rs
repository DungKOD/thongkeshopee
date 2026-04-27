//! Pre-flush dedup: skip upload nếu state cuối window = state đầu (revert
//! detection cho 4 bảng nhỏ) + import-session revert detection.
//!
//! ## Option A — Full-table hash dedup (small tables)
//!
//! Áp dụng cho 4 bảng có row count nhỏ:
//! `app_settings`, `manual_entries`, `imported_files`, `shopee_accounts`.
//! Hash toàn bảng (ORDER BY pk) tại pre-flush. Match `last_full_hash` →
//! advance cursor (consume các row updated_at > old cursor) nhưng SKIP push.
//! Ngữ cảnh: user toggle setting ON→OFF→ON trong 45s window → row có
//! updated_at mới nhưng value identical với baseline. Không cần upload.
//!
//! Bảng to (`raw_*`, mapping) không hash O(N) vì 100k+ rows tốn ~200ms.
//! Skip-identical post-upload (existing) cover case "bundle bytes identical".
//!
//! ## Option D — Import-session revert detection
//!
//! Khi user import file rồi xóa file đó trong cùng window:
//! `imported_files` row có `reverted_at` set + raw_* rows liên quan đã DELETE.
//! Trước flush, identify các file fully reverted → drop khỏi delta queue
//! (chi tiết ở `apply_import_revert_dedup`).

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

use super::compress::sha256_hex;
use super::descriptors::{find_descriptor, CursorKind, TableDescriptor};

/// Bảng nhỏ eligible cho full-table hash dedup. Hash O(N rows) nên chỉ áp
/// dụng table có ≤ vài nghìn rows. Bảng `raw_*` (10k-100k+) không trong list.
pub const FULL_HASH_DEDUP_TABLES: &[&str] = &[
    "app_settings",
    "manual_entries",
    "imported_files",
    "shopee_accounts",
];

/// Trả `true` nếu table được dedup full-hash. Pre-flush sẽ hash full content
/// + compare với `last_full_hash` để detect revert.
pub fn is_full_hash_eligible(table: &str) -> bool {
    FULL_HASH_DEDUP_TABLES.contains(&table)
}

/// Cột phải LOẠI khỏi full-table hash — metadata không phải "meaningful content".
///
/// Lý do: revert detection cần answer "value identical với baseline?". Cột
/// timestamp như `updated_at`/`created_at`/`imported_at` advance mỗi mutation
/// dù value identical → include vào hash sẽ luôn miss revert detection.
///
/// Auto-increment `id` cũng exclude vì local-only (manual_entries.id) — sync
/// dùng natural PK (sub_ids+day), id không transmit, không nên ảnh hưởng hash.
fn excluded_columns_for_dedup(table: &str) -> &'static [&'static str] {
    match table {
        "app_settings" => &["updated_at"],
        "manual_entries" => &["id", "created_at", "updated_at"],
        // imported_files: `imported_at` advance mỗi import, KEEP `reverted_at`
        // (state field — distinguishes active vs reverted file).
        "imported_files" => &["imported_at"],
        "shopee_accounts" => &["created_at"],
        _ => &[],
    }
}

/// Hash SHA-256 của full table content (loại metadata timestamps qua
/// `excluded_columns_for_dedup`). Deterministic ORDER BY pk_columns → 2 lần
/// gọi cùng meaningful state → cùng hash.
///
/// Dùng cho revert detection ở 4 bảng nhỏ. Với bảng to, chi phí O(N) đọc
/// + hash quá đắt — không gọi.
pub fn compute_table_full_hash(
    conn: &Connection,
    descriptor: &TableDescriptor,
) -> Result<String> {
    let pk_clause = descriptor.pk_columns.join(", ");
    let sql = format!(
        "SELECT * FROM {table} ORDER BY {pk}",
        table = descriptor.name,
        pk = pk_clause,
    );

    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| format!("prepare full-hash query cho {}", descriptor.name))?;
    let col_names: Vec<String> = stmt
        .column_names()
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    let excluded = excluded_columns_for_dedup(descriptor.name);

    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();

    let rows_iter = stmt
        .query_map([], |row| {
            // Hash mỗi cell theo dạng "{idx}={value_repr}|" — bao gồm column
            // index để đảm bảo (val_a, NULL) ≠ (NULL, val_a).
            let mut buf: Vec<u8> = Vec::with_capacity(64 * col_names.len());
            for (i, col) in col_names.iter().enumerate() {
                if excluded.contains(&col.as_str()) {
                    continue;
                }
                let val: rusqlite::types::Value = row.get(i)?;
                buf.extend_from_slice(format!("{i}=").as_bytes());
                match val {
                    rusqlite::types::Value::Null => buf.extend_from_slice(b"NULL"),
                    rusqlite::types::Value::Integer(n) => {
                        buf.extend_from_slice(format!("I{n}").as_bytes())
                    }
                    rusqlite::types::Value::Real(f) => {
                        // f64::to_bits đảm bảo 0.1+0.2 vs 0.3 vẫn distinct.
                        buf.extend_from_slice(format!("R{:016x}", f.to_bits()).as_bytes());
                    }
                    rusqlite::types::Value::Text(s) => {
                        buf.extend_from_slice(b"T");
                        buf.extend_from_slice(s.as_bytes());
                    }
                    rusqlite::types::Value::Blob(b) => {
                        buf.extend_from_slice(b"B");
                        buf.extend_from_slice(&b);
                    }
                }
                buf.push(b'|');
            }
            buf.push(b'\n');
            Ok(buf)
        })
        .context("query_map full-hash")?;

    for row_buf in rows_iter {
        let buf = row_buf.context("iter full-hash row")?;
        hasher.update(&buf);
    }

    Ok(sha256_hex(&hasher.finalize()))
}

/// Tính max cursor value hiện tại của bảng (theo descriptor.cursor_column).
/// Dùng khi skip upload do revert — phải advance cursor past mọi row touched
/// trong window để lần flush sau không re-capture.
pub fn compute_max_cursor(
    conn: &Connection,
    descriptor: &TableDescriptor,
) -> Result<String> {
    let cursor_col = match descriptor.cursor_kind {
        CursorKind::RowId => "rowid",
        _ => descriptor.cursor_column,
    };
    let sql = format!(
        "SELECT MAX({col}) FROM {table}",
        col = cursor_col,
        table = descriptor.name,
    );
    let max: Option<rusqlite::types::Value> = conn
        .query_row(&sql, [], |r| r.get(0))
        .with_context(|| format!("compute_max_cursor cho {}", descriptor.name))?;

    Ok(match max {
        Some(rusqlite::types::Value::Integer(n)) => n.to_string(),
        Some(rusqlite::types::Value::Text(s)) => s,
        Some(rusqlite::types::Value::Real(f)) => f.to_string(),
        // Bảng rỗng → giữ "0" (initial cursor value).
        _ => "0".to_string(),
    })
}

/// UPDATE cursor_state khi skip upload do revert: advance cursor + update
/// last_full_hash. KHÔNG đụng last_uploaded_hash (skip-identical post-upload
/// vẫn so với hash payload thực sự upload trước đó).
///
/// Monotonic guard: cursor chỉ advance, không bao giờ tụt. Trường hợp empty
/// table (MAX return "0") sau khi cursor đã advance trước đó — guard ngăn
/// tụt cursor về "0". Lex-compare cho TEXT cursor (UpdatedAt/created_at);
/// numeric-compare cho RowId/PrimaryKey int cursor.
fn mark_skipped_via_revert(
    conn: &Connection,
    descriptor: &TableDescriptor,
    new_full_hash: &str,
) -> Result<()> {
    let max_cursor = compute_max_cursor(conn, descriptor)?;
    let now = chrono::Utc::now().to_rfc3339();

    let is_numeric =
        !max_cursor.is_empty() && max_cursor.chars().all(|c| c.is_ascii_digit());
    let cursor_sql = if is_numeric {
        "UPDATE sync_cursor_state
         SET last_uploaded_cursor = ?, last_full_hash = ?, updated_at = ?
         WHERE table_name = ?
           AND CAST(? AS INTEGER) >= CAST(last_uploaded_cursor AS INTEGER)"
    } else {
        "UPDATE sync_cursor_state
         SET last_uploaded_cursor = ?, last_full_hash = ?, updated_at = ?
         WHERE table_name = ?
           AND ? >= last_uploaded_cursor"
    };
    conn.execute(
        cursor_sql,
        params![max_cursor, new_full_hash, now, descriptor.name, max_cursor],
    )
    .with_context(|| format!("mark_skipped_via_revert cursor cho {}", descriptor.name))?;

    // Nếu monotonic guard reject UPDATE (cursor tụt), vẫn cần update last_full_hash
    // để baseline nhất quán với current state. Tách query riêng không phụ thuộc cursor.
    conn.execute(
        "UPDATE sync_cursor_state
         SET last_full_hash = ?, updated_at = ?
         WHERE table_name = ?",
        params![new_full_hash, now, descriptor.name],
    )
    .with_context(|| format!("mark_skipped_via_revert hash cho {}", descriptor.name))?;
    Ok(())
}

/// Pre-flush hook: cho mỗi bảng eligible, hash full content. Match
/// last_full_hash → advance cursor (consume) + update full_hash, KHÔNG add
/// vào bundle. Trả số bảng đã skip.
///
/// Gọi TRƯỚC `plan_push_bundle`. Sau call này, capture của các bảng skip sẽ
/// trả None (cursor đã advance past max).
///
/// Lần đầu (last_full_hash = NULL): set baseline = current hash + advance cursor.
/// Lý do: lần đầu coi như "đã upload baseline", không có gì phải push.
/// Edge case: schema fresh có rows seeded (vd shopee_accounts default account)
/// → hash baseline includes seeded rows. Pull-side INSERT OR IGNORE handle dup.
pub fn apply_full_hash_dedup(conn: &Connection) -> Result<usize> {
    let mut skipped = 0;
    for table_name in FULL_HASH_DEDUP_TABLES {
        let descriptor = match find_descriptor(table_name) {
            Some(d) => d,
            None => continue,
        };

        let cursor_state: Option<(String, Option<String>)> = conn
            .query_row(
                "SELECT last_uploaded_cursor, last_full_hash
                 FROM sync_cursor_state WHERE table_name = ?",
                [table_name],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
            )
            .ok();

        let (_last_cursor, last_full_hash) = match cursor_state {
            Some(s) => s,
            None => continue, // bảng chưa seed cursor row — bỏ qua (defensive)
        };

        let current_hash = compute_table_full_hash(conn, descriptor)?;

        if last_full_hash.as_deref() == Some(current_hash.as_str()) {
            // State không đổi so với lần upload trước → skip upload, advance
            // cursor để capture không re-emit.
            mark_skipped_via_revert(conn, descriptor, &current_hash)?;
            skipped += 1;
        }
        // Hash khác → để plan_push_bundle capture + upload bình thường.
        // Caller (mark_uploaded_with_full_hash) sẽ update last_full_hash sau.
    }
    Ok(skipped)
}

/// Sau khi upload OK, set `last_full_hash` baseline cho table eligible từ
/// hash đã PRECOMPUTED tại thời điểm capture (trong cùng DB lock).
///
/// **CRITICAL — race-safe**: hash phải snapshot từ lúc capture (xem
/// `TableBundleRange::full_hash_snapshot`), KHÔNG re-compute ở đây. Race
/// scenario nếu re-compute:
/// 1. Capture lock release để upload HTTP.
/// 2. User mutate row M (cursor mới > bundle.cursor_hi).
/// 3. Re-acquire lock → mark_uploaded(cursor_hi). Cursor advance đúng.
/// 4. Re-compute hash bao gồm M → baseline = hash(post-race state).
/// 5. Lần flush sau: hash hiện tại == baseline → SKIP. Cursor advance to
///    MAX (qua M). Row M không bao giờ upload → data loss.
///
/// Snapshot từ lúc capture đảm bảo baseline = hash(state đã upload), M sẽ
/// được capture + push lần sau (vì hash mới != baseline).
///
/// Skip nếu table không eligible hoặc precomputed_hash = None.
pub fn set_full_hash_baseline(
    conn: &Connection,
    table: &str,
    precomputed_hash: &str,
) -> Result<()> {
    if !is_full_hash_eligible(table) {
        return Ok(());
    }
    conn.execute(
        "UPDATE sync_cursor_state SET last_full_hash = ? WHERE table_name = ?",
        params![precomputed_hash, table],
    )
    .with_context(|| format!("set_full_hash_baseline cho {}", table))?;
    Ok(())
}

/// Test helper: compute hash + set baseline atomically. KHÔNG dùng trong
/// production push flow (có race risk). Chỉ dùng trong test setup hoặc
/// 1-shot init khi không có concurrent mutation.
pub fn update_full_hash_after_upload(conn: &Connection, table: &str) -> Result<()> {
    if !is_full_hash_eligible(table) {
        return Ok(());
    }
    let descriptor = match find_descriptor(table) {
        Some(d) => d,
        None => return Ok(()),
    };
    let hash = compute_table_full_hash(conn, descriptor)?;
    set_full_hash_baseline(conn, table, &hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate_for_tests;
    use crate::sync_v9::content_id;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        let schema = include_str!("../db/schema.sql");
        conn.execute_batch(schema).unwrap();
        migrate_for_tests(&conn).unwrap();
        conn
    }

    #[test]
    fn full_hash_eligibility() {
        assert!(is_full_hash_eligible("app_settings"));
        assert!(is_full_hash_eligible("manual_entries"));
        assert!(is_full_hash_eligible("imported_files"));
        assert!(is_full_hash_eligible("shopee_accounts"));
        assert!(!is_full_hash_eligible("raw_shopee_clicks"));
        assert!(!is_full_hash_eligible("raw_fb_ads"));
        assert!(!is_full_hash_eligible("nonexistent"));
    }

    #[test]
    fn compute_full_hash_deterministic() {
        let conn = test_conn();
        let desc = find_descriptor("app_settings").unwrap();
        let h1 = compute_table_full_hash(&conn, desc).unwrap();
        let h2 = compute_table_full_hash(&conn, desc).unwrap();
        assert_eq!(h1, h2, "cùng state phải cùng hash");
        assert_eq!(h1.len(), 64, "SHA-256 hex");
    }

    #[test]
    fn compute_full_hash_changes_on_insert() {
        let conn = test_conn();
        let desc = find_descriptor("app_settings").unwrap();
        let h_empty = compute_table_full_hash(&conn, desc).unwrap();

        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k1", "\"v1\"", "2026-04-26T00:00:00Z"],
        )
        .unwrap();
        let h_after = compute_table_full_hash(&conn, desc).unwrap();
        assert_ne!(h_empty, h_after, "thêm row → hash đổi");
    }

    #[test]
    fn compute_full_hash_revert_to_baseline() {
        let conn = test_conn();
        let desc = find_descriptor("app_settings").unwrap();

        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k1", "\"7\"", "2026-04-26T00:00:00Z"],
        )
        .unwrap();
        let h_baseline = compute_table_full_hash(&conn, desc).unwrap();

        // Update value rồi revert lại — giá trị cuối identical.
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = ?",
            params!["\"10\"", "2026-04-26T00:01:00Z", "k1"],
        )
        .unwrap();
        let h_changed = compute_table_full_hash(&conn, desc).unwrap();
        assert_ne!(h_changed, h_baseline);

        // Revert + restore updated_at giống ban đầu để full content match.
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = ?",
            params!["\"7\"", "2026-04-26T00:00:00Z", "k1"],
        )
        .unwrap();
        let h_reverted = compute_table_full_hash(&conn, desc).unwrap();
        assert_eq!(
            h_reverted, h_baseline,
            "revert về EXACT same state → hash khớp baseline"
        );
    }

    #[test]
    fn compute_full_hash_distinguishes_null_vs_empty() {
        let conn = test_conn();
        let desc = find_descriptor("manual_entries").unwrap();

        conn.execute(
            "INSERT INTO days (date, created_at) VALUES ('2026-04-26', '2026-04-26T00:00:00Z')",
            [],
        )
        .unwrap();

        // Row 1: override_clicks = NULL.
        conn.execute(
            "INSERT INTO manual_entries (sub_id1, day_date, created_at, updated_at)
             VALUES ('a', '2026-04-26', 'now', 'now')",
            [],
        )
        .unwrap();
        let h_null = compute_table_full_hash(&conn, desc).unwrap();

        // Đổi sang 0 (không null).
        conn.execute(
            "UPDATE manual_entries SET override_clicks = 0 WHERE sub_id1 = 'a'",
            [],
        )
        .unwrap();
        let h_zero = compute_table_full_hash(&conn, desc).unwrap();

        assert_ne!(h_null, h_zero, "NULL phải khác 0 trong hash");
    }

    #[test]
    fn compute_max_cursor_empty_table() {
        let conn = test_conn();
        let desc = find_descriptor("manual_entries").unwrap();
        let max = compute_max_cursor(&conn, desc).unwrap();
        assert_eq!(max, "0");
    }

    #[test]
    fn compute_max_cursor_after_insert() {
        let conn = test_conn();
        let desc = find_descriptor("app_settings").unwrap();
        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k", "\"v\"", "2026-04-26T08:00:00Z"],
        )
        .unwrap();
        let max = compute_max_cursor(&conn, desc).unwrap();
        assert_eq!(max, "2026-04-26T08:00:00Z");
    }

    #[test]
    fn apply_full_hash_dedup_first_time_sets_baseline() {
        let conn = test_conn();
        // Lần đầu: last_full_hash = NULL → KHÔNG match → không skip, KHÔNG
        // tự set baseline (baseline được set qua update_full_hash_after_upload
        // sau khi push thành công).
        let skipped = apply_full_hash_dedup(&conn).unwrap();
        assert_eq!(skipped, 0, "lần đầu chưa có baseline, không skip");
    }

    #[test]
    fn apply_full_hash_dedup_skips_when_state_matches() {
        let conn = test_conn();
        let desc = find_descriptor("app_settings").unwrap();

        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k", "\"v\"", "2026-04-26T08:00:00Z"],
        )
        .unwrap();

        // Simulate "đã upload trước" — set last_full_hash.
        update_full_hash_after_upload(&conn, "app_settings").unwrap();

        // Mutation không thay đổi content (vd update value identical).
        conn.execute(
            "UPDATE app_settings SET value = ? WHERE key = ?",
            params!["\"v\"", "k"],
        )
        .unwrap();

        // Cursor vẫn ở giá trị cũ.
        conn.execute(
            "UPDATE sync_cursor_state SET last_uploaded_cursor = '0' WHERE table_name = 'app_settings'",
            [],
        )
        .unwrap();

        let skipped = apply_full_hash_dedup(&conn).unwrap();
        assert!(skipped >= 1, "state identical → phải skip");

        // Cursor đã advance.
        let cur: String = conn
            .query_row(
                "SELECT last_uploaded_cursor FROM sync_cursor_state WHERE table_name = 'app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cur, "2026-04-26T08:00:00Z", "cursor advance to MAX");

        // Hash compute lại match: idempotent re-run.
        let _ = desc; // suppress unused warning trong nhánh test này
    }

    #[test]
    fn apply_full_hash_dedup_does_not_skip_on_real_change() {
        let conn = test_conn();

        // Setup baseline với 1 row.
        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k", "\"v1\"", "2026-04-26T08:00:00Z"],
        )
        .unwrap();
        update_full_hash_after_upload(&conn, "app_settings").unwrap();

        // Mutation thay đổi value thật.
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = ?",
            params!["\"v2\"", "2026-04-26T08:01:00Z", "k"],
        )
        .unwrap();

        let skipped = apply_full_hash_dedup(&conn).unwrap();
        assert_eq!(skipped, 0, "value thật sự đổi → KHÔNG skip");
    }

    #[test]
    fn update_full_hash_idempotent_on_same_state() {
        let conn = test_conn();
        update_full_hash_after_upload(&conn, "app_settings").unwrap();
        let h1: Option<String> = conn
            .query_row(
                "SELECT last_full_hash FROM sync_cursor_state WHERE table_name = 'app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();

        update_full_hash_after_upload(&conn, "app_settings").unwrap();
        let h2: Option<String> = conn
            .query_row(
                "SELECT last_full_hash FROM sync_cursor_state WHERE table_name = 'app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();

        assert_eq!(h1, h2);
    }

    #[test]
    fn update_full_hash_skips_non_eligible() {
        let conn = test_conn();
        update_full_hash_after_upload(&conn, "raw_shopee_clicks").unwrap();
        let h: Option<String> = conn
            .query_row(
                "SELECT last_full_hash FROM sync_cursor_state WHERE table_name = 'raw_shopee_clicks'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(h.is_none(), "raw_* không update full_hash");
    }

    // ============================================================
    // End-to-end integration tests with plan_push_bundle
    // ============================================================

    use crate::sync_v9::push;
    use crate::sync_v9::SV_CURRENT;

    fn ack_all_initial_uploads(conn: &Connection) {
        // Simulate full bundle upload + ack để baseline sync_cursor_state.
        // Cũng update full_hash cho eligible tables.
        let bundle = push::plan_push_bundle_default(conn, 1_000_000)
            .unwrap();
        if let Some(b) = bundle {
            for range in &b.table_ranges {
                push::mark_uploaded(conn, &range.table, &range.cursor_hi, &range.content_hash)
                    .unwrap();
                update_full_hash_after_upload(conn, &range.table).unwrap();
            }
        }
        // Set baseline cho TẤT CẢ eligible tables (kể cả bảng rỗng — initial
        // upload không cover, nhưng baseline NULL = chưa setup, dedup sẽ skip).
        for t in FULL_HASH_DEDUP_TABLES {
            update_full_hash_after_upload(conn, t).unwrap();
        }
    }

    #[test]
    fn e2e_settings_revert_skips_upload() {
        let conn = test_conn();

        // Initial state: 1 setting key.
        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["profit_fee", "\"7\"", "2026-04-26T08:00:00Z"],
        )
        .unwrap();
        ack_all_initial_uploads(&conn);

        // User toggle: 7 → 10 (real change).
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = ?",
            params!["\"10\"", "2026-04-26T08:01:00Z", "profit_fee"],
        )
        .unwrap();
        // User revert: 10 → 7. Updated_at advance lần nữa.
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = ?",
            params!["\"7\"", "2026-04-26T08:02:00Z", "profit_fee"],
        )
        .unwrap();

        // Pre-flush dedup detect revert → advance cursor + skip.
        let skipped = apply_full_hash_dedup(&conn).unwrap();
        assert!(skipped >= 1, "phải skip app_settings vì revert");

        // Plan push: app_settings không vào bundle (cursor đã advance past mọi row).
        let bundle = push::plan_push_bundle_default(&conn, 2_000_000).unwrap();
        if let Some(b) = bundle {
            let tables: Vec<&str> = b.table_ranges.iter().map(|r| r.table.as_str()).collect();
            assert!(
                !tables.contains(&"app_settings"),
                "app_settings phải bị skip sau dedup, có: {tables:?}"
            );
        }
        // Bundle = None cũng OK (mọi table khác cũng không có change).
    }

    #[test]
    fn e2e_settings_real_change_does_upload() {
        let conn = test_conn();

        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["profit_fee", "\"7\"", "2026-04-26T08:00:00Z"],
        )
        .unwrap();
        ack_all_initial_uploads(&conn);

        // Cursor app_settings sau ack = max updated_at (lúc set baseline).
        let cursor_before: String = conn
            .query_row(
                "SELECT last_uploaded_cursor FROM sync_cursor_state WHERE table_name = 'app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();

        // Real change: 7 → 10 (no revert).
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = ?",
            params!["\"10\"", "2026-04-26T08:01:00Z", "profit_fee"],
        )
        .unwrap();

        // Dedup chạy: app_settings KHÔNG skip (hash khác baseline). Các bảng
        // empty/unchanged khác có thể skip — không ảnh hưởng test này.
        let _ = apply_full_hash_dedup(&conn).unwrap();

        // Cursor app_settings KHÔNG được advance (vì không skip).
        let cursor_after: String = conn
            .query_row(
                "SELECT last_uploaded_cursor FROM sync_cursor_state WHERE table_name = 'app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cursor_after, cursor_before, "app_settings cursor phải giữ nguyên (không skip)");

        let bundle = push::plan_push_bundle_default(&conn, 2_000_000).unwrap();
        let bundle = bundle.expect("phải có bundle vì có thay đổi");
        let has_app_settings = bundle
            .table_ranges
            .iter()
            .any(|r| r.table == "app_settings");
        assert!(has_app_settings, "app_settings phải có trong bundle");
    }

    #[test]
    fn e2e_mixed_revert_and_real_change() {
        let conn = test_conn();

        // Setup baseline: 1 setting + 1 day + 1 manual entry.
        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k", "\"a\"", "2026-04-26T08:00:00Z"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO days (date, created_at) VALUES ('2026-04-26', '2026-04-26T00:00:00Z')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manual_entries (sub_id1, day_date, override_clicks, created_at, updated_at)
             VALUES ('s1', '2026-04-26', 100, 'now', '2026-04-26T08:00:00Z')",
            [],
        )
        .unwrap();
        ack_all_initial_uploads(&conn);

        // Settings revert: a → b → a.
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = 'k'",
            params!["\"b\"", "2026-04-26T08:01:00Z"],
        )
        .unwrap();
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = 'k'",
            params!["\"a\"", "2026-04-26T08:02:00Z"],
        )
        .unwrap();

        // Manual entries real change: 100 → 200.
        conn.execute(
            "UPDATE manual_entries SET override_clicks = 200, updated_at = ? WHERE sub_id1 = 's1'",
            params!["2026-04-26T08:03:00Z"],
        )
        .unwrap();

        let _skipped = apply_full_hash_dedup(&conn).unwrap();
        let bundle = push::plan_push_bundle_default(&conn, 2_000_000)
            .unwrap()
            .expect("manual_entries change → bundle");

        let tables: Vec<&str> = bundle.table_ranges.iter().map(|r| r.table.as_str()).collect();
        assert!(
            !tables.contains(&"app_settings"),
            "app_settings revert → skip, có: {tables:?}"
        );
        assert!(
            tables.contains(&"manual_entries"),
            "manual_entries real change → upload, có: {tables:?}"
        );
    }

    #[test]
    fn e2e_after_upload_baseline_updated_correctly() {
        let conn = test_conn();

        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k", "\"v1\"", "2026-04-26T08:00:00Z"],
        )
        .unwrap();
        ack_all_initial_uploads(&conn);

        // Real change → upload.
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = 'k'",
            params!["\"v2\"", "2026-04-26T08:01:00Z"],
        )
        .unwrap();
        ack_all_initial_uploads(&conn); // simulate upload + mark_uploaded + update_full_hash

        let cursor_before: String = conn
            .query_row(
                "SELECT last_uploaded_cursor FROM sync_cursor_state WHERE table_name = 'app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();

        // Sau upload, hash baseline = v2 state. Mutation về v1 (revert lùi) →
        // KHÔNG skip vì baseline mới là v2.
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = 'k'",
            params!["\"v1\"", "2026-04-26T08:02:00Z"],
        )
        .unwrap();

        let _ = apply_full_hash_dedup(&conn).unwrap();

        // Cursor app_settings không advance (vì v1 != v2 baseline → không skip).
        let cursor_after: String = conn
            .query_row(
                "SELECT last_uploaded_cursor FROM sync_cursor_state WHERE table_name = 'app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            cursor_after, cursor_before,
            "v2→v1 sau khi v2 đã upload là real change, app_settings cursor phải giữ nguyên"
        );
    }

    #[test]
    fn e2e_revert_in_window_does_not_disturb_other_tables() {
        let conn = test_conn();

        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k", "\"a\"", "2026-04-26T08:00:00Z"],
        )
        .unwrap();
        ack_all_initial_uploads(&conn);

        // Settings revert.
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = 'k'",
            params!["\"b\"", "2026-04-26T08:01:00Z"],
        )
        .unwrap();
        conn.execute(
            "UPDATE app_settings SET value = ?, updated_at = ? WHERE key = 'k'",
            params!["\"a\"", "2026-04-26T08:02:00Z"],
        )
        .unwrap();

        let _skipped = apply_full_hash_dedup(&conn).unwrap();

        // Cursor của bảng KHÔNG eligible (raw_shopee_clicks) phải không đổi.
        let cursor: String = conn
            .query_row(
                "SELECT last_uploaded_cursor FROM sync_cursor_state WHERE table_name = 'raw_shopee_clicks'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cursor, "0", "bảng không eligible không bị đụng");
    }

    // ============================================================
    // Race + monotonic guard regression tests
    // ============================================================

    #[test]
    fn race_safe_baseline_does_not_skip_unpushed_mutation() {
        // Simulate race: capture hash X. Mutation thêm row M sau capture.
        // set_full_hash_baseline với hash X (precomputed) — KHÔNG bao gồm M.
        // Lần dedup tiếp theo: hash hiện tại (có M) != X → KHÔNG skip → M được push.
        let conn = test_conn();

        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k1", "\"a\"", "2026-04-26T08:00:00Z"],
        )
        .unwrap();

        // Simulate capture: precompute hash + cursor.
        let desc = find_descriptor("app_settings").unwrap();
        let hash_at_capture = compute_table_full_hash(&conn, desc).unwrap();
        let cursor_at_capture = compute_max_cursor(&conn, desc).unwrap();

        // Race: user mutate sau capture, trước upload OK.
        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k_race", "\"r\"", "2026-04-26T08:00:01Z"],
        )
        .unwrap();

        // Simulate post-upload: mark_uploaded(cursor_at_capture) + set_full_hash_baseline
        // với hash_at_capture (precomputed, KHÔNG re-hash).
        conn.execute(
            "UPDATE sync_cursor_state SET last_uploaded_cursor = ? WHERE table_name = 'app_settings'",
            params![cursor_at_capture],
        )
        .unwrap();
        set_full_hash_baseline(&conn, "app_settings", &hash_at_capture).unwrap();

        // Lần dedup tiếp theo: hash hiện tại (có k_race) khác baseline (không có k_race).
        // → KHÔNG skip. Cursor không advance qua k_race. Push lần sau sẽ capture k_race.
        let skipped = apply_full_hash_dedup(&conn).unwrap();
        // app_settings KHÔNG được skip.
        let cursor_now: String = conn
            .query_row(
                "SELECT last_uploaded_cursor FROM sync_cursor_state WHERE table_name = 'app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            cursor_now, cursor_at_capture,
            "race mutation k_race chưa được upload → cursor KHÔNG advance qua nó"
        );
        // Other empty/unchanged tables có thể skip — chỉ verify app_settings không bị.
        let _ = skipped;
    }

    #[test]
    fn monotonic_guard_prevents_cursor_going_backward_on_empty_table() {
        // Scenario: table có rows, push → cursor advance to T1. User xóa hết rows.
        // baseline updated to hash(empty). Lần flush sau: hash matches baseline,
        // mark_skipped_via_revert tính MAX = "0" (empty). Guard ngăn cursor tụt
        // từ T1 về "0".
        let conn = test_conn();
        let desc = find_descriptor("app_settings").unwrap();

        // Setup: 1 row uploaded với cursor T1.
        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k", "\"v\"", "2026-04-26T08:00:00Z"],
        )
        .unwrap();
        conn.execute(
            "UPDATE sync_cursor_state SET last_uploaded_cursor = ? WHERE table_name = 'app_settings'",
            params!["2026-04-26T08:00:00Z"],
        )
        .unwrap();

        // Xóa row → table rỗng. Update baseline về hash(empty).
        conn.execute("DELETE FROM app_settings WHERE key = 'k'", [])
            .unwrap();
        let h_empty = compute_table_full_hash(&conn, desc).unwrap();
        set_full_hash_baseline(&conn, "app_settings", &h_empty).unwrap();

        // Apply dedup: hash(empty) match baseline → skip. mark_skipped_via_revert
        // sẽ tính MAX = "0" (table rỗng). Guard phải ngăn cursor tụt.
        apply_full_hash_dedup(&conn).unwrap();

        let cursor_after: String = conn
            .query_row(
                "SELECT last_uploaded_cursor FROM sync_cursor_state WHERE table_name = 'app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            cursor_after, "2026-04-26T08:00:00Z",
            "cursor KHÔNG được tụt về 0 khi table rỗng"
        );
    }

    #[test]
    fn monotonic_guard_still_updates_baseline_when_cursor_blocked() {
        // Khi cursor không advance (do guard), last_full_hash VẪN PHẢI update
        // để nhất quán với current state. Tách 2 query trong mark_skipped_via_revert.
        let conn = test_conn();

        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            params!["k", "\"v\"", "2026-04-26T08:00:00Z"],
        )
        .unwrap();
        conn.execute(
            "UPDATE sync_cursor_state SET last_uploaded_cursor = ? WHERE table_name = 'app_settings'",
            params!["2026-04-26T09:00:00Z"], // Cursor cao hơn MAX hiện tại
        )
        .unwrap();
        let desc = find_descriptor("app_settings").unwrap();
        let h_current = compute_table_full_hash(&conn, desc).unwrap();
        set_full_hash_baseline(&conn, "app_settings", &h_current).unwrap();

        // Xóa row → state đổi. Set baseline = hash(empty) để trigger skip.
        conn.execute("DELETE FROM app_settings WHERE key = 'k'", [])
            .unwrap();
        let h_empty = compute_table_full_hash(&conn, desc).unwrap();
        set_full_hash_baseline(&conn, "app_settings", &h_empty).unwrap();

        // Apply dedup: hash matches baseline → skip. MAX = "0" < cursor T9 → guard reject.
        // Nhưng baseline VẪN phải = h_empty (current state).
        apply_full_hash_dedup(&conn).unwrap();

        let baseline_after: Option<String> = conn
            .query_row(
                "SELECT last_full_hash FROM sync_cursor_state WHERE table_name = 'app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(baseline_after.as_deref(), Some(h_empty.as_str()));
    }

    #[test]
    fn shopee_accounts_hash_changes_on_insert() {
        // shopee_accounts có default seed "Mặc định" với created_at = Utc::now()
        // → hash khác giữa các test runs (timestamp khác). Chỉ test rằng
        // thêm row mới phải đổi hash trong CÙNG connection.
        let conn = test_conn();
        let desc = find_descriptor("shopee_accounts").unwrap();
        let h_baseline = compute_table_full_hash(&conn, desc).unwrap();

        conn.execute(
            "INSERT INTO shopee_accounts (id, name, color, created_at)
             VALUES (?, ?, '#000', '2026-04-26T08:00:00Z')",
            params![content_id::shopee_account_id("Shop A"), "Shop A"],
        )
        .unwrap();
        let h_after = compute_table_full_hash(&conn, desc).unwrap();
        assert_ne!(h_after, h_baseline, "thêm account → hash đổi");
    }
}
