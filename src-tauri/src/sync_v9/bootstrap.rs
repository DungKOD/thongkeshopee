//! Bootstrap orchestration — fresh install detection + `.pending.db` swap
//! + fresh_install_pending flag management.
//!
//! **Flow bootstrap** (plan 2.9 + case C1 + C2):
//! 1. Detect fresh install: `is_local_empty(conn)` + remote manifest có data
//! 2. `begin_bootstrap(conn)` → set fresh_install_pending = 1 (push path sẽ bypass,
//!    rule giữ data)
//! 3. Fetch snapshot (Phase 6 HTTP) → `restore_snapshot_to_pending(bytes, path)`
//! 4. (Caller) close conn + rename pending → live + reopen
//! 5. Set snapshot pointer + seed cursor_state từ snapshot metadata
//! 6. Pull + apply deltas sau snapshot (reuse pull.rs)
//! 7. `complete_bootstrap(conn)` → clear fresh_install_pending
//!
//! **Rule giữ data C2:** Guard fresh_install_pending = 1 suốt process. Nếu
//! crash giữa → next start detect + continue (bootstrap idempotent).

use anyhow::{Context, Result};
use rusqlite::Connection;

use super::descriptors::{CursorKind, SYNC_TABLES};
use super::manifest;

/// Check local DB có "empty" theo semantic bootstrap: không có raw data, không
/// có manual entries, không có tombstones, không có cursor advance.
///
/// `shopee_accounts` được schema seed 1 row "Mặc định" — bỏ qua khỏi check.
pub fn is_local_empty(conn: &Connection) -> Result<bool> {
    // Check các tables có thể chứa user data (không phải seed).
    let user_tables = [
        "raw_shopee_clicks",
        "raw_shopee_order_items",
        "raw_fb_ads",
        "imported_files",
        "manual_entries",
        "tombstones",
        "clicks_to_file",
        "orders_to_file",
        "fb_ads_to_file",
        "days",
    ];
    for table in user_tables {
        let count: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM {table}"),
            [],
            |r| r.get(0),
        )?;
        if count > 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Output của detect_fresh_install — phân biệt 3 state:
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapMode {
    /// Local + remote cùng empty → first-ever user. Normal push path,
    /// không cần bootstrap.
    FirstEver,
    /// Local empty nhưng remote có data → cần bootstrap (fetch snapshot).
    /// Case C1 (máy mới) hoặc C2 (reinstall same machine).
    NeedsBootstrap,
    /// Local có data → sync normal (pull + push).
    Normal,
}

/// Phân tích trạng thái local + remote manifest → quyết định mode.
///
/// - `remote_manifest_has_data`: caller pass `manifest.deltas.is_empty() && manifest.latest_snapshot.is_none()`
///   đảo ngược. Tức `true` nếu manifest non-empty.
pub fn detect_mode(conn: &Connection, remote_manifest_has_data: bool) -> Result<BootstrapMode> {
    let local_empty = is_local_empty(conn)?;
    Ok(match (local_empty, remote_manifest_has_data) {
        (true, true) => BootstrapMode::NeedsBootstrap,
        (true, false) => BootstrapMode::FirstEver,
        (false, _) => BootstrapMode::Normal,
    })
}

/// Bắt đầu bootstrap — set `fresh_install_pending = 1`.
///
/// Idempotent (nếu đã =1 từ crash trước, set lại cũng OK).
pub fn begin_bootstrap(conn: &Connection) -> Result<()> {
    manifest::set_fresh_install_pending(conn, true)
}

/// Hoàn tất bootstrap — clear flag + record snapshot pointer.
///
/// Caller gọi SAU KHI:
/// - snapshot đã restore vào live DB
/// - cursor_state đã seed từ snapshot (Phase 8 integration)
/// - deltas sau snapshot đã pull + apply xong
///
/// Sau khi gọi, push path được enable trở lại (không còn guard empty).
pub fn complete_bootstrap(
    conn: &Connection,
    snapshot_key: &str,
    snapshot_clock_ms: i64,
) -> Result<()> {
    manifest::set_snapshot(conn, snapshot_key, snapshot_clock_ms)?;
    manifest::set_fresh_install_pending(conn, false)?;
    Ok(())
}

/// Check flag. Dùng ở push path (rule giữ data — không push empty đè remote).
pub fn is_bootstrap_pending(conn: &Connection) -> Result<bool> {
    Ok(manifest::read_state(conn)?.fresh_install_pending)
}

/// Seed `sync_cursor_state.last_pulled_cursor` cho mọi bảng từ snapshot.
///
/// Sau khi restore snapshot, mọi bảng đã có rows tương ứng với state tại
/// snapshot.clock_ms. Next pull nên bắt đầu từ clock_ms đó (set qua
/// `manifest::advance_pulled_clock`), và cursor_state đã reflect state của
/// snapshot (vì snapshot chứa luôn sync_cursor_state row).
///
/// Hàm này idempotent — gọi nhiều lần không đổi state.
pub fn seed_cursor_after_restore(
    conn: &Connection,
    snapshot_clock_ms: i64,
) -> Result<()> {
    // Bug D fix: last_uploaded_cursor PHẢI = MAX(cursor_column) of LOCAL table
    // sau restore, KHÔNG phải = last_pulled_cursor.
    //
    // Lý do: cursor space khác nhau giữa 2 column.
    // - RowId tables: cursor = local rowid. Snapshot preserve A's rowids →
    //   B inherit cùng rowids. MAX(rowid) sau restore = A's max rowid.
    //   `last_pulled_cursor` của A là remote cursor (manifest entry's cursor_hi
    //   reference máy nguồn) — NOT cùng space với local rowid. Nếu copy
    //   last_pulled → last_uploaded, B's push sẽ thấy mọi rowid > last_pulled
    //   pending → re-upload toàn bộ snapshot rows = data flood, hệt Bug A.
    // - UpdatedAt/DeletedAt tables: cursor = RFC3339 string preserve qua
    //   serialize, A's last_pulled = max(updated_at) đã pull. Sau restore,
    //   MAX(local updated_at) bao gồm cả rows máy A push lẫn pull về →
    //   ≥ last_pulled. Dùng MAX cũng đúng cho kind này.
    //
    // Per-table MAX(cursor_column): chính xác cho mọi CursorKind. Idempotent.
    let now = chrono::Utc::now().to_rfc3339();
    for desc in SYNC_TABLES {
        let cursor_col = match desc.cursor_kind {
            CursorKind::RowId => "rowid",
            _ => desc.cursor_column,
        };
        let max_val: Option<rusqlite::types::Value> = conn
            .query_row(
                &format!("SELECT MAX({}) FROM {}", cursor_col, desc.name),
                [],
                |r| r.get(0),
            )
            .with_context(|| format!("seed_cursor MAX cho {}", desc.name))?;
        let max_str = match max_val {
            Some(rusqlite::types::Value::Integer(n)) => n.to_string(),
            Some(rusqlite::types::Value::Text(s)) => s,
            Some(rusqlite::types::Value::Real(f)) => f.to_string(),
            // Bảng rỗng → giữ "0" (initial). KHÔNG copy last_pulled_cursor
            // vì có thể trong remote-cursor space (RowId tables).
            _ => "0".to_string(),
        };
        // Reset last_uploaded_hash = NULL → next push compute hash mới (skip-
        // identical sẽ re-baseline). Reset last_full_hash = NULL → 4 bảng nhỏ
        // dedup chạy lại baseline lần đầu.
        conn.execute(
            "UPDATE sync_cursor_state
             SET last_uploaded_cursor = ?,
                 last_uploaded_hash = NULL,
                 last_full_hash = NULL,
                 updated_at = ?
             WHERE table_name = ?",
            rusqlite::params![max_str, now, desc.name],
        )
        .with_context(|| format!("seed_cursor UPDATE cho {}", desc.name))?;
    }

    // Advance manifest clock để pull không duplicate events đã trong snapshot.
    manifest::advance_pulled_clock(conn, snapshot_clock_ms)?;

    Ok(())
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate_for_tests;
    use crate::sync_v9::types::{Manifest, ManifestDeltaEntry, ManifestSnapshot};

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        let schema = include_str!("../db/schema.sql");
        conn.execute_batch(schema).unwrap();
        migrate_for_tests(&conn).unwrap();
        conn
    }

    fn seed_file(conn: &Connection, hash: &str) -> i64 {
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash)
             VALUES('f.csv', 'shopee_clicks', 'now', ?)",
            [hash],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn is_local_empty_true_for_fresh_db() {
        let conn = test_conn();
        // Seed default shopee_account exists nhưng không counted.
        assert!(is_local_empty(&conn).unwrap());
    }

    #[test]
    fn is_local_empty_false_after_adding_day() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        assert!(!is_local_empty(&conn).unwrap());
    }

    #[test]
    fn is_local_empty_false_after_import() {
        let conn = test_conn();
        seed_file(&conn, "h1");
        assert!(!is_local_empty(&conn).unwrap());
    }

    #[test]
    fn is_local_empty_false_after_manual_entry() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, day_date, created_at, updated_at)
             VALUES('a', '2026-04-20', 'now', 'now')",
            [],
        )
        .unwrap();
        assert!(!is_local_empty(&conn).unwrap());
    }

    // ---------- detect_mode ----------

    #[test]
    fn detect_mode_first_ever_local_and_remote_empty() {
        let conn = test_conn();
        assert_eq!(
            detect_mode(&conn, false).unwrap(),
            BootstrapMode::FirstEver
        );
    }

    #[test]
    fn detect_mode_needs_bootstrap_local_empty_remote_has_data() {
        let conn = test_conn();
        assert_eq!(
            detect_mode(&conn, true).unwrap(),
            BootstrapMode::NeedsBootstrap
        );
    }

    #[test]
    fn detect_mode_normal_when_local_has_data() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('d', 'now')",
            [],
        )
        .unwrap();
        assert_eq!(detect_mode(&conn, true).unwrap(), BootstrapMode::Normal);
        assert_eq!(detect_mode(&conn, false).unwrap(), BootstrapMode::Normal);
    }

    #[test]
    fn detect_mode_helper_manifest_has_data() {
        // Helper: caller build boolean từ manifest.
        let empty = Manifest::empty("uid".to_string());
        let has_data_empty = !empty.deltas.is_empty() || empty.latest_snapshot.is_some();
        assert!(!has_data_empty);

        let mut with_delta = Manifest::empty("uid".to_string());
        with_delta.deltas.push(ManifestDeltaEntry {
            table: "t".to_string(),
            key: "k".to_string(),
            cursor_lo: "0".to_string(),
            cursor_hi: "1".to_string(),
            clock_ms: 100,
            size_bytes: 0,
            row_count: 1,
        });
        let has_data = !with_delta.deltas.is_empty() || with_delta.latest_snapshot.is_some();
        assert!(has_data);

        let mut with_snap = Manifest::empty("uid".to_string());
        with_snap.latest_snapshot = Some(ManifestSnapshot {
            key: "s".to_string(),
            clock_ms: 0,
            size_bytes: 0,
        });
        let has_data = !with_snap.deltas.is_empty() || with_snap.latest_snapshot.is_some();
        assert!(has_data);
    }

    // ---------- bootstrap flags ----------

    #[test]
    fn begin_bootstrap_sets_flag() {
        let conn = test_conn();
        assert!(!is_bootstrap_pending(&conn).unwrap());
        begin_bootstrap(&conn).unwrap();
        assert!(is_bootstrap_pending(&conn).unwrap());
    }

    #[test]
    fn begin_bootstrap_idempotent() {
        let conn = test_conn();
        begin_bootstrap(&conn).unwrap();
        begin_bootstrap(&conn).unwrap(); // lần 2 OK
        assert!(is_bootstrap_pending(&conn).unwrap());
    }

    #[test]
    fn complete_bootstrap_clears_flag_and_sets_snapshot() {
        let conn = test_conn();
        begin_bootstrap(&conn).unwrap();
        complete_bootstrap(&conn, "snapshots/snap_x.db.zst", 123_456).unwrap();

        assert!(!is_bootstrap_pending(&conn).unwrap());
        let state = manifest::read_state(&conn).unwrap();
        assert_eq!(
            state.last_snapshot_key.as_deref(),
            Some("snapshots/snap_x.db.zst")
        );
        assert_eq!(state.last_snapshot_clock_ms, 123_456);
    }

    #[test]
    fn seed_cursor_after_restore_advances_manifest_clock() {
        let conn = test_conn();
        // Simulate snapshot restored với clock_ms = 500.
        seed_cursor_after_restore(&conn, 500).unwrap();
        let state = manifest::read_state(&conn).unwrap();
        assert_eq!(state.last_pulled_manifest_clock_ms, 500);
    }

    /// Bug D regression: post-fix, last_uploaded_cursor = MAX(cursor_column)
    /// LOCAL, không phải last_pulled_cursor. Cho RowId tables (raw_*),
    /// last_pulled là remote-cursor space → copy sai → push sẽ flood
    /// re-upload toàn bộ snapshot rows.
    #[test]
    fn seed_cursor_uses_local_max_not_pulled_cursor() {
        let conn = test_conn();
        // Snapshot từ máy A: A's last_pulled_cursor = 100 (REMOTE rowid space,
        // không relevant cho B's local rowid). raw_shopee_clicks rỗng.
        conn.execute(
            "UPDATE sync_cursor_state
             SET last_pulled_cursor = '100'
             WHERE table_name = 'raw_shopee_clicks'",
            [],
        )
        .unwrap();

        seed_cursor_after_restore(&conn, 0).unwrap();

        let uploaded: String = conn
            .query_row(
                "SELECT last_uploaded_cursor FROM sync_cursor_state
                 WHERE table_name = 'raw_shopee_clicks'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        // Table rỗng → "0" (KHÔNG phải "100"). Nếu = "100", push pipeline sẽ
        // capture rowid > 100 (mà rowid bắt đầu từ 1) → flood không cần thiết.
        assert_eq!(
            uploaded, "0",
            "last_uploaded = MAX(rowid) = 0 vì table rỗng, KHÔNG phải last_pulled"
        );
    }

    #[test]
    fn seed_cursor_uses_max_rowid_when_table_has_rows() {
        let conn = test_conn();
        // Simulate snapshot restore: rows trong raw_shopee_clicks (rowid 1-3).
        let file_id = seed_file(&conn, "h1");
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        for cid in ["c1", "c2", "c3"] {
            conn.execute(
                "INSERT INTO raw_shopee_clicks
                 (click_id, click_time, day_date, source_file_id)
                 VALUES(?, 'now', '2026-04-20', ?)",
                rusqlite::params![cid, file_id],
            )
            .unwrap();
        }
        // last_pulled từ A = 999 (irrelevant remote space).
        conn.execute(
            "UPDATE sync_cursor_state SET last_pulled_cursor = '999' WHERE table_name = 'raw_shopee_clicks'",
            [],
        )
        .unwrap();

        seed_cursor_after_restore(&conn, 0).unwrap();

        let uploaded: String = conn
            .query_row(
                "SELECT last_uploaded_cursor FROM sync_cursor_state WHERE table_name = 'raw_shopee_clicks'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(uploaded, "3", "MAX(rowid) = 3, không phải 999 (remote space)");
    }

    #[test]
    fn seed_cursor_handles_updated_at_table() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, day_date, created_at, updated_at)
             VALUES('a', '2026-04-20', 'now', '2026-04-26T08:00:00Z')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO manual_entries
             (sub_id1, day_date, created_at, updated_at)
             VALUES('b', '2026-04-20', 'now', '2026-04-26T09:00:00Z')",
            [],
        )
        .unwrap();

        seed_cursor_after_restore(&conn, 0).unwrap();

        let uploaded: String = conn
            .query_row(
                "SELECT last_uploaded_cursor FROM sync_cursor_state WHERE table_name = 'manual_entries'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(uploaded, "2026-04-26T09:00:00Z");
    }

    #[test]
    fn seed_cursor_resets_full_hash_baseline() {
        // Bug D fix side effect: last_full_hash NULL sau restore. Reason:
        // 4 bảng nhỏ dedup so hash full vs baseline. Snapshot bao gồm A's
        // last_full_hash → B inherit = hash của A's state. Nhưng B's content
        // = A's content sau restore → hash match → dedup skip lần đầu.
        // Nguy hiểm: B chưa upload bất kỳ delta nào, dedup advance cursor mà
        // không upload → nếu user mutate ngay sau restore, cursor đã quá xa
        // → mất data nếu state mới != baseline. Reset NULL ép baseline init lại.
        let conn = test_conn();
        conn.execute(
            "UPDATE sync_cursor_state SET last_full_hash = 'inherited_from_A'
             WHERE table_name = 'app_settings'",
            [],
        )
        .unwrap();
        seed_cursor_after_restore(&conn, 0).unwrap();
        let h: Option<String> = conn
            .query_row(
                "SELECT last_full_hash FROM sync_cursor_state WHERE table_name = 'app_settings'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(h.is_none(), "last_full_hash phải reset NULL sau restore");
    }

    #[test]
    fn seed_cursor_resets_uploaded_hash() {
        let conn = test_conn();
        conn.execute(
            "UPDATE sync_cursor_state SET last_uploaded_hash = 'stale_hash' WHERE table_name = 'raw_shopee_clicks'",
            [],
        )
        .unwrap();
        seed_cursor_after_restore(&conn, 0).unwrap();
        let hash: Option<String> = conn
            .query_row(
                "SELECT last_uploaded_hash FROM sync_cursor_state WHERE table_name = 'raw_shopee_clicks'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(hash.is_none(), "hash reset sau bootstrap (content đổi nguồn)");
    }

    // ---------- Integration flow sim ----------

    #[test]
    fn full_bootstrap_flow_simulation() {
        let conn = test_conn();
        // 1. Detect: fresh install (local empty, remote có data).
        assert_eq!(
            detect_mode(&conn, true).unwrap(),
            BootstrapMode::NeedsBootstrap
        );

        // 2. Begin.
        begin_bootstrap(&conn).unwrap();
        assert!(is_bootstrap_pending(&conn).unwrap());

        // 3. (Simulate restore happened — data đã trong DB).
        //    Phase 5 not doing actual file swap, just state.

        // 4. Seed cursor sau restore.
        seed_cursor_after_restore(&conn, 1_000).unwrap();

        // 5. Complete.
        complete_bootstrap(&conn, "snapshots/snap_1000.db.zst", 1_000).unwrap();
        assert!(!is_bootstrap_pending(&conn).unwrap());

        let state = manifest::read_state(&conn).unwrap();
        assert_eq!(state.last_snapshot_clock_ms, 1_000);
        assert_eq!(state.last_pulled_manifest_clock_ms, 1_000);
    }
}
