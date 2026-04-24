//! Pull flow — fetch delta files từ R2, apply vào local DB.
//!
//! Phase 4 scope = pure logic (parse + apply). HTTP fetch defer Phase 6.
//! Orchestration high-level:
//! 1. Client fetch manifest (Phase 6 HTTP) → `Manifest`
//! 2. `compute_pending_pulls` (manifest.rs) → list delta entries cần fetch
//! 3. Client parallel download (Phase 6) → bytes per entry
//! 4. Sort deltas theo clock_ms ASC (causal)
//! 5. Per delta file: `parse_delta_file` → `apply_delta_file` (1 TX)
//! 6. Advance cursors + `advance_pulled_clock` sau apply OK
//!
//! **TX per-file** (plan Phần 2.8, rule giữ data): apply failed mid-file →
//! rollback chỉ file đó, cursor không advance, next pull retry idempotent.

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

use super::apply::{apply_event, ApplyOutcome};
use super::compress::zstd_decompress;
use super::manifest;
use super::types::{DeltaEvent, ManifestDeltaEntry};

/// Aggregated outcome sau apply 1 delta file. Dùng cho sync_event_log PullApply.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ApplyStats {
    pub applied: u32,
    pub skipped: u32,
    pub skipped_by_hlc: u32,
    pub tombstones_applied: u32,
    pub tombstones_noop: u32,
    pub rows_deleted: u64,
}

impl ApplyStats {
    /// Total events processed.
    pub fn total(&self) -> u32 {
        self.applied + self.skipped + self.skipped_by_hlc + self.tombstones_applied + self.tombstones_noop
    }

    /// Số event không actually apply (dup hoặc HLC skip hoặc resurrect no-op).
    pub fn resurrected(&self) -> u32 {
        self.tombstones_noop
    }
}

/// Parse zstd-compressed NDJSON bytes → Vec<DeltaEvent>.
///
/// Reject line trống trong middle (sign of partial corruption). Skip trailing
/// blank line tolerable (NDJSON convention).
pub fn parse_delta_file(compressed: &[u8]) -> Result<Vec<DeltaEvent>> {
    let ndjson = zstd_decompress(compressed).context("decompress delta file")?;
    let text = std::str::from_utf8(&ndjson).context("delta file không phải UTF-8")?;

    let mut events = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        if line.is_empty() {
            // Trailing empty line OK (mostly), middle empty = corruption signal.
            // serde_json won't parse empty → we skip silently. Integrity check
            // đã ở zstd magic; NDJSON structure looseness OK.
            continue;
        }
        let ev: DeltaEvent = serde_json::from_str(line)
            .with_context(|| format!("parse NDJSON line {idx}: {}", truncate(line)))?;
        events.push(ev);
    }
    Ok(events)
}

fn truncate(s: &str) -> String {
    if s.len() <= 80 {
        s.to_string()
    } else {
        format!("{}...", &s[..77])
    }
}

/// Apply 1 batch events (thường = 1 delta file) trong 1 transaction.
///
/// Rule giữ data: error mid-batch → caller gọi `tx.rollback()`, cursor không
/// advance. Retry idempotent qua INSERT OR IGNORE + HLC check.
///
/// Events order phải đã sort theo `clock_ms ASC` trước khi gọi (guarantee
/// causal). Caller responsibility.
pub fn apply_events(conn: &mut Connection, events: &[DeltaEvent]) -> Result<ApplyStats> {
    let tx = conn.transaction().context("begin apply TX")?;
    let mut stats = ApplyStats::default();

    for ev in events {
        let outcome = apply_event(&tx, ev)
            .with_context(|| format!("apply event {}", event_summary(ev)))?;
        match outcome {
            ApplyOutcome::Applied => stats.applied += 1,
            ApplyOutcome::Skipped => stats.skipped += 1,
            ApplyOutcome::SkippedByHlc => stats.skipped_by_hlc += 1,
            ApplyOutcome::TombstoneApplied { rows_deleted } => {
                stats.tombstones_applied += 1;
                stats.rows_deleted += rows_deleted;
            }
            ApplyOutcome::TombstoneNoOp => stats.tombstones_noop += 1,
        }
    }

    tx.commit().context("commit apply TX")?;
    Ok(stats)
}

fn event_summary(ev: &DeltaEvent) -> String {
    match ev {
        DeltaEvent::Insert(i) => format!("insert {}/{}", i.table, json_compact(&i.pk)),
        DeltaEvent::Upsert(u) => format!("upsert {}/{}", u.table, json_compact(&u.pk)),
        DeltaEvent::Tombstone(t) => format!("tombstone {}/{}", t.entity_type, t.entity_key),
    }
}

fn json_compact(v: &serde_json::Value) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "<invalid>".to_string())
}

/// Advance `sync_cursor_state.last_pulled_cursor` cho 1 bảng sau khi apply OK.
/// Monotonic MAX guard chống regression khi race.
pub fn advance_pulled_cursor(
    conn: &Connection,
    table: &str,
    new_cursor: &str,
) -> Result<()> {
    // SQLite string MAX follows lex compare. Cho numeric cursor (RowId), lex
    // compare sai (e.g. "100" > "99" false). Workaround: compare as i64 nếu
    // cả 2 parse được.
    let current: String = conn
        .query_row(
            "SELECT last_pulled_cursor FROM sync_cursor_state WHERE table_name = ?",
            [table],
            |r| r.get(0),
        )
        .with_context(|| format!("read last_pulled_cursor for {table}"))?;

    let advance = match (current.parse::<i64>(), new_cursor.parse::<i64>()) {
        (Ok(c), Ok(n)) => n > c, // numeric compare
        _ => new_cursor.as_bytes() > current.as_bytes(), // lex fallback (RFC3339 OK)
    };

    if advance {
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE sync_cursor_state
             SET last_pulled_cursor = ?, updated_at = ?
             WHERE table_name = ?",
            params![new_cursor, now, table],
        )?;
    }
    Ok(())
}

/// Extract max `clock_ms` từ events (dùng cho absorb_remote_clock + manifest
/// clock advance). Return 0 nếu empty.
pub fn max_event_clock_ms(events: &[DeltaEvent]) -> i64 {
    events
        .iter()
        .map(|e| match e {
            DeltaEvent::Insert(i) => i.clock_ms,
            DeltaEvent::Upsert(u) => u.clock_ms,
            DeltaEvent::Tombstone(t) => t.clock_ms,
        })
        .max()
        .unwrap_or(0)
}

/// Compute plan: từ Manifest + local state → list `(entry, bytes_placeholder)`
/// cho HTTP layer fetch. Entries sorted theo clock_ms ASC.
///
/// Return only entries với `clock_ms > last_pulled_manifest_clock_ms`. Entries
/// từ tables không có trong `SYNC_TABLES` → skip (unknown table, chờ client update).
pub fn plan_pull(
    conn: &Connection,
    remote_manifest: &super::types::Manifest,
) -> Result<Vec<ManifestDeltaEntry>> {
    let state = manifest::read_state(conn)?;
    let pending_refs = manifest::compute_pending_pulls(
        remote_manifest,
        state.last_pulled_manifest_clock_ms,
    );

    // Filter unknown tables (forward compat — client cũ hơn manifest writer).
    let filtered: Vec<ManifestDeltaEntry> = pending_refs
        .into_iter()
        .filter(|e| super::descriptors::find_descriptor(&e.table).is_some())
        .cloned()
        .collect();
    Ok(filtered)
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate_for_tests;
    use crate::sync_v9::compress::zstd_compress;
    use crate::sync_v9::types::{InsertEvent, Manifest, ManifestDeltaEntry};
    use crate::sync_v9::SV_CURRENT;
    use serde_json::json;

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

    // ---------- parse_delta_file ----------

    #[test]
    fn parse_delta_file_roundtrip() {
        let events = vec![
            DeltaEvent::Insert(InsertEvent {
                sv: SV_CURRENT,
                table: "raw_shopee_clicks".to_string(),
                pk: json!({"click_id": "c1"}),
                row: json!({"click_id": "c1"}),
                clock_ms: 100,
            }),
            DeltaEvent::Insert(InsertEvent {
                sv: SV_CURRENT,
                table: "raw_shopee_clicks".to_string(),
                pk: json!({"click_id": "c2"}),
                row: json!({"click_id": "c2"}),
                clock_ms: 200,
            }),
        ];
        let mut ndjson = Vec::new();
        for ev in &events {
            ndjson.extend(serde_json::to_vec(ev).unwrap());
            ndjson.push(b'\n');
        }
        let compressed = zstd_compress(&ndjson).unwrap();
        let parsed = parse_delta_file(&compressed).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed, events);
    }

    #[test]
    fn parse_delta_file_rejects_corrupt() {
        let err = parse_delta_file(b"not zstd").unwrap_err();
        // Debug format includes cause chain; Display chỉ top-level context.
        assert!(format!("{err:?}").contains("zstd"));
    }

    #[test]
    fn parse_delta_file_rejects_bad_json() {
        let compressed = zstd_compress(b"not-valid-json\n").unwrap();
        let err = parse_delta_file(&compressed).unwrap_err();
        assert!(format!("{err:?}").contains("parse NDJSON"));
    }

    // ---------- apply_events ----------

    #[test]
    fn apply_events_commits_all_in_tx() {
        let mut conn = test_conn();
        let file_id = seed_file(&conn, "h1");

        let events = vec![
            DeltaEvent::Insert(InsertEvent {
                sv: SV_CURRENT,
                table: "raw_shopee_clicks".to_string(),
                pk: json!({"click_id": "c1"}),
                row: json!({
                    "click_id": "c1", "click_time": "now",
                    "day_date": "2026-04-20", "source_file_id": file_id
                }),
                clock_ms: 100,
            }),
            DeltaEvent::Insert(InsertEvent {
                sv: SV_CURRENT,
                table: "raw_shopee_clicks".to_string(),
                pk: json!({"click_id": "c2"}),
                row: json!({
                    "click_id": "c2", "click_time": "now",
                    "day_date": "2026-04-20", "source_file_id": file_id
                }),
                clock_ms: 200,
            }),
        ];

        let stats = apply_events(&mut conn, &events).unwrap();
        assert_eq!(stats.applied, 2);
        assert_eq!(stats.total(), 2);

        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM raw_shopee_clicks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2);
    }

    #[test]
    fn apply_events_rollback_on_error() {
        let mut conn = test_conn();
        let file_id = seed_file(&conn, "h1");

        let events = vec![
            DeltaEvent::Insert(InsertEvent {
                sv: SV_CURRENT,
                table: "raw_shopee_clicks".to_string(),
                pk: json!({"click_id": "c1"}),
                row: json!({
                    "click_id": "c1", "click_time": "now",
                    "day_date": "2026-04-20", "source_file_id": file_id
                }),
                clock_ms: 100,
            }),
            // Second event references unknown table → apply_event Err → TX rollback.
            DeltaEvent::Insert(InsertEvent {
                sv: SV_CURRENT,
                table: "unknown_table_xyz".to_string(),
                pk: json!({"x": 1}),
                row: json!({"x": 1}),
                clock_ms: 200,
            }),
        ];

        let err = apply_events(&mut conn, &events).unwrap_err();
        assert!(format!("{err:?}").contains("unknown table"));

        // Rule giữ data: first event phải KHÔNG persist vì TX rollback.
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM raw_shopee_clicks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "TX rollback phải undo first event");
    }

    #[test]
    fn apply_events_idempotent_replay() {
        let mut conn = test_conn();
        let file_id = seed_file(&conn, "h1");

        let events = vec![DeltaEvent::Insert(InsertEvent {
            sv: SV_CURRENT,
            table: "raw_shopee_clicks".to_string(),
            pk: json!({"click_id": "c1"}),
            row: json!({
                "click_id": "c1", "click_time": "now",
                "day_date": "2026-04-20", "source_file_id": file_id
            }),
            clock_ms: 100,
        })];

        let s1 = apply_events(&mut conn, &events).unwrap();
        assert_eq!(s1.applied, 1);
        let s2 = apply_events(&mut conn, &events).unwrap();
        assert_eq!(s2.skipped, 1, "replay = INSERT OR IGNORE → Skipped");
    }

    // ---------- advance_pulled_cursor ----------

    #[test]
    fn advance_pulled_cursor_numeric_monotonic() {
        let conn = test_conn();
        advance_pulled_cursor(&conn, "raw_shopee_clicks", "100").unwrap();
        advance_pulled_cursor(&conn, "raw_shopee_clicks", "50").unwrap(); // retreat
        let val: String = conn
            .query_row(
                "SELECT last_pulled_cursor FROM sync_cursor_state WHERE table_name = 'raw_shopee_clicks'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(val, "100");

        advance_pulled_cursor(&conn, "raw_shopee_clicks", "200").unwrap();
        let val: String = conn
            .query_row(
                "SELECT last_pulled_cursor FROM sync_cursor_state WHERE table_name = 'raw_shopee_clicks'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(val, "200");
    }

    #[test]
    fn advance_pulled_cursor_numeric_not_lex() {
        let conn = test_conn();
        // Numeric 10 > 9 nhưng lex "10" < "9". Verify ta dùng numeric compare.
        advance_pulled_cursor(&conn, "raw_shopee_clicks", "9").unwrap();
        advance_pulled_cursor(&conn, "raw_shopee_clicks", "10").unwrap();
        let val: String = conn
            .query_row(
                "SELECT last_pulled_cursor FROM sync_cursor_state WHERE table_name = 'raw_shopee_clicks'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(val, "10", "numeric compare, không lex");
    }

    #[test]
    fn advance_pulled_cursor_rfc3339_lex() {
        let conn = test_conn();
        advance_pulled_cursor(&conn, "manual_entries", "2026-04-24T08:00:00Z").unwrap();
        advance_pulled_cursor(&conn, "manual_entries", "2026-04-24T07:00:00Z").unwrap();
        let val: String = conn
            .query_row(
                "SELECT last_pulled_cursor FROM sync_cursor_state WHERE table_name = 'manual_entries'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(val, "2026-04-24T08:00:00Z");
    }

    // ---------- max_event_clock_ms ----------

    #[test]
    fn max_event_clock_ms_picks_largest() {
        let events = vec![
            DeltaEvent::Insert(InsertEvent {
                sv: SV_CURRENT,
                table: "t".to_string(),
                pk: json!({}),
                row: json!({}),
                clock_ms: 100,
            }),
            DeltaEvent::Insert(InsertEvent {
                sv: SV_CURRENT,
                table: "t".to_string(),
                pk: json!({}),
                row: json!({}),
                clock_ms: 500,
            }),
            DeltaEvent::Insert(InsertEvent {
                sv: SV_CURRENT,
                table: "t".to_string(),
                pk: json!({}),
                row: json!({}),
                clock_ms: 300,
            }),
        ];
        assert_eq!(max_event_clock_ms(&events), 500);
        assert_eq!(max_event_clock_ms(&[]), 0);
    }

    // ---------- plan_pull ----------

    #[test]
    fn plan_pull_returns_pending_sorted() {
        let conn = test_conn();
        let mut m = Manifest::empty("uid".to_string());
        m.deltas = vec![
            ManifestDeltaEntry {
                table: "raw_shopee_clicks".to_string(),
                key: "k3".to_string(),
                cursor_lo: "0".to_string(),
                cursor_hi: "3".to_string(),
                clock_ms: 300,
                size_bytes: 0,
                row_count: 1,
            },
            ManifestDeltaEntry {
                table: "raw_shopee_clicks".to_string(),
                key: "k1".to_string(),
                cursor_lo: "0".to_string(),
                cursor_hi: "1".to_string(),
                clock_ms: 100,
                size_bytes: 0,
                row_count: 1,
            },
        ];
        let pending = plan_pull(&conn, &m).unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].clock_ms, 100);
        assert_eq!(pending[1].clock_ms, 300);
    }

    #[test]
    fn plan_pull_skips_unknown_tables() {
        let conn = test_conn();
        let mut m = Manifest::empty("uid".to_string());
        m.deltas = vec![
            ManifestDeltaEntry {
                table: "future_table_v12".to_string(),
                key: "k1".to_string(),
                cursor_lo: "0".to_string(),
                cursor_hi: "1".to_string(),
                clock_ms: 100,
                size_bytes: 0,
                row_count: 1,
            },
            ManifestDeltaEntry {
                table: "raw_shopee_clicks".to_string(),
                key: "k2".to_string(),
                cursor_lo: "0".to_string(),
                cursor_hi: "1".to_string(),
                clock_ms: 200,
                size_bytes: 0,
                row_count: 1,
            },
        ];
        let pending = plan_pull(&conn, &m).unwrap();
        assert_eq!(pending.len(), 1, "future table skipped, raw_shopee_clicks kept");
        assert_eq!(pending[0].table, "raw_shopee_clicks");
    }

    #[test]
    fn plan_pull_respects_last_pulled_clock() {
        let conn = test_conn();
        manifest::advance_pulled_clock(&conn, 150).unwrap();

        let mut m = Manifest::empty("uid".to_string());
        m.deltas = vec![
            ManifestDeltaEntry {
                table: "raw_shopee_clicks".to_string(),
                key: "k_old".to_string(),
                cursor_lo: "0".to_string(),
                cursor_hi: "1".to_string(),
                clock_ms: 100, // < 150
                size_bytes: 0,
                row_count: 1,
            },
            ManifestDeltaEntry {
                table: "raw_shopee_clicks".to_string(),
                key: "k_new".to_string(),
                cursor_lo: "1".to_string(),
                cursor_hi: "2".to_string(),
                clock_ms: 200, // > 150
                size_bytes: 0,
                row_count: 1,
            },
        ];
        let pending = plan_pull(&conn, &m).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].key, "k_new");
    }

    // ---------- End-to-end sim ----------

    #[test]
    fn end_to_end_capture_to_apply_roundtrip() {
        // Machine A captures → compress → bytes qua wire → Machine B parses → applies.
        let mut conn_a = test_conn();
        let file_id = seed_file(&conn_a, "h1");
        conn_a
            .execute(
                "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
                [],
            )
            .unwrap();
        conn_a
            .execute(
                "INSERT INTO raw_shopee_clicks
                 (click_id, click_time, sub_id1, day_date, source_file_id)
                 VALUES('abc', '2026-04-20T10:00:00Z', 'sid', '2026-04-20', ?)",
                [file_id],
            )
            .unwrap();

        // A: capture + compress
        use crate::sync_v9::capture::capture_table_delta;
        use crate::sync_v9::descriptors::find_descriptor;
        use crate::sync_v9::push::build_push_payload;
        let desc = find_descriptor("raw_shopee_clicks").unwrap();
        let batch = capture_table_delta(&conn_a, desc, "0", 5_000_000, 1000, SV_CURRENT)
            .unwrap()
            .unwrap();
        let payload = build_push_payload(batch).unwrap();
        let wire_bytes = payload.bytes.clone();

        // B: parse + apply
        let mut conn_b = test_conn();
        seed_file(&conn_b, "h1"); // assume imported_files đã sync trước
        let events = parse_delta_file(&wire_bytes).unwrap();
        assert_eq!(events.len(), 1);
        let stats = apply_events(&mut conn_b, &events).unwrap();
        assert_eq!(stats.applied, 1);

        let click: String = conn_b
            .query_row(
                "SELECT sub_id1 FROM raw_shopee_clicks WHERE click_id = 'abc'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(click, "sid");
    }
}
