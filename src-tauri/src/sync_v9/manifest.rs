//! Manifest state + mutation helpers.
//!
//! Manifest = single source of truth trên R2 `users/{uid}/manifest.json`,
//! CAS-guarded qua etag. Phase 3 scope = **pure logic** (không HTTP):
//! - Read/write local `sync_manifest_state` table
//! - Append delta entries idempotent (dedup theo key, sort clock_ms)
//! - Diff pending pulls (manifest deltas chưa apply)
//! - Compact sau snapshot (drop deltas cũ hơn snapshot)
//! - Fresh install flag management
//!
//! HTTP client + CAS retry loop sẽ là Phase 6 (`sync_v9/client.rs`). Các
//! function ở đây là building blocks để HTTP layer gọi trong retry loop.

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

use super::types::{Manifest, ManifestDeltaEntry, ManifestSnapshot};

/// Snapshot của `sync_manifest_state` singleton row.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestState {
    pub last_remote_etag: Option<String>,
    pub last_pulled_manifest_clock_ms: i64,
    pub last_snapshot_key: Option<String>,
    pub last_snapshot_clock_ms: i64,
    pub fresh_install_pending: bool,
}

/// Đọc full manifest state từ DB.
pub fn read_state(conn: &Connection) -> Result<ManifestState> {
    conn.query_row(
        "SELECT last_remote_etag, last_pulled_manifest_clock_ms,
                last_snapshot_key, last_snapshot_clock_ms, fresh_install_pending
         FROM sync_manifest_state WHERE id = 1",
        [],
        |r| {
            Ok(ManifestState {
                last_remote_etag: r.get(0)?,
                last_pulled_manifest_clock_ms: r.get(1)?,
                last_snapshot_key: r.get(2)?,
                last_snapshot_clock_ms: r.get(3)?,
                fresh_install_pending: r.get::<_, i64>(4)? != 0,
            })
        },
    )
    .context("read sync_manifest_state")
}

/// Set `last_remote_etag` sau khi CAS PUT manifest OK.
pub fn set_etag(conn: &Connection, etag: &str) -> Result<()> {
    conn.execute(
        "UPDATE sync_manifest_state SET last_remote_etag = ? WHERE id = 1",
        [etag],
    )
    .context("set etag")?;
    Ok(())
}

/// Advance `last_pulled_manifest_clock_ms` sau khi apply batch deltas OK.
/// Monotonic — chỉ advance forward (guard chống revert nếu race).
pub fn advance_pulled_clock(conn: &Connection, new_clock_ms: i64) -> Result<()> {
    conn.execute(
        "UPDATE sync_manifest_state
         SET last_pulled_manifest_clock_ms = MAX(last_pulled_manifest_clock_ms, ?)
         WHERE id = 1",
        [new_clock_ms],
    )
    .context("advance pulled clock")?;
    Ok(())
}

/// Ghi snapshot pointer sau khi bootstrap hoặc compaction xong.
pub fn set_snapshot(conn: &Connection, key: &str, clock_ms: i64) -> Result<()> {
    conn.execute(
        "UPDATE sync_manifest_state
         SET last_snapshot_key = ?, last_snapshot_clock_ms = ?
         WHERE id = 1",
        params![key, clock_ms],
    )
    .context("set snapshot pointer")?;
    Ok(())
}

/// Set fresh install flag. Bật khi detect local empty + có remote manifest
/// (bootstrap mode), tắt sau khi bootstrap pull + apply xong.
///
/// Rule giữ data (plan C2): khi pending=1, push path BẮT BUỘC skip (không
/// đè empty lên remote). Xem `push::is_fresh_install_pending`.
pub fn set_fresh_install_pending(conn: &Connection, pending: bool) -> Result<()> {
    conn.execute(
        "UPDATE sync_manifest_state SET fresh_install_pending = ? WHERE id = 1",
        [pending as i64],
    )
    .context("set fresh_install_pending")?;
    Ok(())
}

// =============================================================
// Manifest mutation (pure functions, no DB)
// =============================================================

/// Append entries mới vào manifest.deltas. **Idempotent theo `key`** — nếu
/// key đã tồn tại, skip silent (không error, không update). Sort theo
/// `clock_ms ASC` sau append để apply-side consume đúng causal order.
///
/// Return số entries thực sự added (excluded duplicates).
pub fn append_delta_entries(
    manifest: &mut Manifest,
    new_entries: Vec<ManifestDeltaEntry>,
) -> usize {
    let mut added = 0;
    for entry in new_entries {
        if manifest.deltas.iter().any(|e| e.key == entry.key) {
            continue;
        }
        manifest.deltas.push(entry);
        added += 1;
    }
    manifest.deltas.sort_by_key(|e| e.clock_ms);
    added
}

/// Diff: delta entries trong manifest có `clock_ms > last_pulled_clock_ms`.
/// Đó là những deltas chưa apply local → cần fetch + apply.
///
/// Trả references sort theo clock_ms ASC (order trong manifest đã sort bởi
/// `append_delta_entries`, nhưng guarantee để apply-side không phải re-sort).
pub fn compute_pending_pulls<'a>(
    manifest: &'a Manifest,
    last_pulled_clock_ms: i64,
) -> Vec<&'a ManifestDeltaEntry> {
    let mut out: Vec<&ManifestDeltaEntry> = manifest
        .deltas
        .iter()
        .filter(|d| d.clock_ms > last_pulled_clock_ms)
        .collect();
    out.sort_by_key(|d| d.clock_ms);
    out
}

/// Detect: local có tụt hậu so với snapshot remote không? Nếu có →
/// trả về snapshot ref để caller trigger restore path.
///
/// Case trigger:
/// - Fresh install (local_clock=0) + remote có snapshot → bootstrap
/// - Long offline (local_clock < snap_clock, delta gap đã compact) → restore
/// - Schema mismatch sau reinstall → restore từ snapshot đồng bộ state
///
/// Case không trigger:
/// - Remote không có snapshot (early stage) → normal pull
/// - local_clock >= snap_clock → manifest.deltas đủ cover gap → normal pull
pub fn needs_snapshot_restore<'a>(
    manifest: &'a Manifest,
    local_state: &ManifestState,
) -> Option<&'a super::types::ManifestSnapshot> {
    match &manifest.latest_snapshot {
        Some(snap) if local_state.last_pulled_manifest_clock_ms < snap.clock_ms => {
            Some(snap)
        }
        _ => None,
    }
}

/// Sau compaction: drop deltas có clock_ms <= snapshot.clock_ms (chúng đã
/// được consolidate vào snapshot), set `latest_snapshot`. Return số deltas
/// dropped.
///
/// **Ordering rule (plan #1 giữ data):** Caller phải ensure snapshot đã
/// upload lên R2 + verify integrity TRƯỚC khi gọi hàm này → manifest mutate
/// → sau đó mới delete old delta R2 objects. Nếu hàm crash giữa → manifest
/// consistent, R2 state vẫn OK (delta files cũ còn đó, chỉ không ref nữa).
pub fn compact_after_snapshot(
    manifest: &mut Manifest,
    new_snapshot: ManifestSnapshot,
) -> usize {
    let threshold = new_snapshot.clock_ms;
    let before = manifest.deltas.len();
    manifest.deltas.retain(|d| d.clock_ms > threshold);
    manifest.latest_snapshot = Some(new_snapshot);
    before - manifest.deltas.len()
}

/// Update manifest.updated_at_ms cho CAS PUT. Phải gọi sau append/compact
/// để remote thấy clock thay đổi.
pub fn bump_updated_at(manifest: &mut Manifest, clock_ms: i64) {
    manifest.updated_at_ms = manifest.updated_at_ms.max(clock_ms);
}

// =============================================================
// CAS retry orchestration (state machine, HTTP-agnostic)
// =============================================================

/// Kế hoạch retry cho CAS PUT manifest. Dùng khi 412 Precondition Failed
/// (etag mismatch) → re-fetch + re-append + retry.
///
/// Max 3 retries per plan (giữ nguyên v8 behavior). Nếu exhausted, caller
/// trả error `MANIFEST_CAS_EXHAUSTED` để FE retry sau (user-initiated).
#[derive(Debug, Clone)]
pub struct CasRetryPlan {
    attempts: u32,
    max_attempts: u32,
}

impl CasRetryPlan {
    pub fn new() -> Self {
        Self {
            attempts: 0,
            max_attempts: 3,
        }
    }

    pub fn with_max(max_attempts: u32) -> Self {
        Self {
            attempts: 0,
            max_attempts,
        }
    }

    /// Record 1 attempt. Return `Ok(attempt_number)` nếu còn budget,
    /// `Err(exhausted)` nếu hết.
    pub fn try_attempt(&mut self) -> Result<u32> {
        if self.attempts >= self.max_attempts {
            anyhow::bail!("CAS retry exhausted after {} attempts", self.attempts);
        }
        self.attempts += 1;
        Ok(self.attempts)
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }
}

impl Default for CasRetryPlan {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate_for_tests;
    use crate::sync_v9::SV_CURRENT;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        let schema = include_str!("../db/schema.sql");
        conn.execute_batch(schema).unwrap();
        migrate_for_tests(&conn).unwrap();
        conn
    }

    fn make_entry(table: &str, key: &str, clock_ms: i64) -> ManifestDeltaEntry {
        ManifestDeltaEntry {
            table: table.to_string(),
            key: key.to_string(),
            cursor_lo: "0".to_string(),
            cursor_hi: "1".to_string(),
            clock_ms,
            size_bytes: 100,
            row_count: 1,
        }
    }

    // ---------- DB state helpers ----------

    #[test]
    fn read_state_returns_defaults_for_fresh_db() {
        let conn = test_conn();
        let s = read_state(&conn).unwrap();
        assert_eq!(s.last_remote_etag, None);
        assert_eq!(s.last_pulled_manifest_clock_ms, 0);
        assert_eq!(s.last_snapshot_key, None);
        assert_eq!(s.last_snapshot_clock_ms, 0);
        assert!(!s.fresh_install_pending);
    }

    #[test]
    fn set_etag_persists() {
        let conn = test_conn();
        set_etag(&conn, "abc123").unwrap();
        let s = read_state(&conn).unwrap();
        assert_eq!(s.last_remote_etag.as_deref(), Some("abc123"));
    }

    #[test]
    fn advance_pulled_clock_is_monotonic() {
        let conn = test_conn();
        advance_pulled_clock(&conn, 1000).unwrap();
        advance_pulled_clock(&conn, 500).unwrap(); // lùi → phải không ảnh hưởng
        let s = read_state(&conn).unwrap();
        assert_eq!(s.last_pulled_manifest_clock_ms, 1000);

        advance_pulled_clock(&conn, 2000).unwrap();
        let s = read_state(&conn).unwrap();
        assert_eq!(s.last_pulled_manifest_clock_ms, 2000);
    }

    #[test]
    fn set_snapshot_persists() {
        let conn = test_conn();
        set_snapshot(&conn, "snapshots/snap_1.db.zst", 1_234_567).unwrap();
        let s = read_state(&conn).unwrap();
        assert_eq!(s.last_snapshot_key.as_deref(), Some("snapshots/snap_1.db.zst"));
        assert_eq!(s.last_snapshot_clock_ms, 1_234_567);
    }

    #[test]
    fn fresh_install_flag_toggles() {
        let conn = test_conn();
        assert!(!read_state(&conn).unwrap().fresh_install_pending);
        set_fresh_install_pending(&conn, true).unwrap();
        assert!(read_state(&conn).unwrap().fresh_install_pending);
        set_fresh_install_pending(&conn, false).unwrap();
        assert!(!read_state(&conn).unwrap().fresh_install_pending);
    }

    // ---------- Append helpers ----------

    #[test]
    fn append_delta_entries_adds_new() {
        let mut m = Manifest::empty("uid".to_string());
        let added = append_delta_entries(
            &mut m,
            vec![
                make_entry("raw_shopee_clicks", "k1", 100),
                make_entry("manual_entries", "k2", 200),
            ],
        );
        assert_eq!(added, 2);
        assert_eq!(m.deltas.len(), 2);
    }

    #[test]
    fn append_delta_entries_dedups_by_key() {
        let mut m = Manifest::empty("uid".to_string());
        append_delta_entries(&mut m, vec![make_entry("t", "k1", 100)]);

        // Re-append same key → skip.
        let added = append_delta_entries(&mut m, vec![make_entry("t", "k1", 200)]);
        assert_eq!(added, 0, "dup key phải skip");
        assert_eq!(m.deltas.len(), 1);
        // clock_ms giữ giá trị cũ (không overwrite).
        assert_eq!(m.deltas[0].clock_ms, 100);
    }

    #[test]
    fn append_delta_entries_sorts_by_clock() {
        let mut m = Manifest::empty("uid".to_string());
        append_delta_entries(
            &mut m,
            vec![
                make_entry("t", "k3", 300),
                make_entry("t", "k1", 100),
                make_entry("t", "k2", 200),
            ],
        );
        let clocks: Vec<i64> = m.deltas.iter().map(|d| d.clock_ms).collect();
        assert_eq!(clocks, vec![100, 200, 300]);
    }

    #[test]
    fn append_delta_entries_idempotent_on_replay() {
        let mut m = Manifest::empty("uid".to_string());
        let batch = vec![
            make_entry("t", "k1", 100),
            make_entry("t", "k2", 200),
        ];
        append_delta_entries(&mut m, batch.clone());
        append_delta_entries(&mut m, batch.clone());
        append_delta_entries(&mut m, batch);
        assert_eq!(m.deltas.len(), 2, "multiple replay = same state");
    }

    // ---------- Diff for pull ----------

    // ---------- Snapshot restore detection (Option 1) ----------

    fn mk_state(last_clock: i64) -> ManifestState {
        ManifestState {
            last_remote_etag: None,
            last_pulled_manifest_clock_ms: last_clock,
            last_snapshot_key: None,
            last_snapshot_clock_ms: 0,
            fresh_install_pending: false,
        }
    }

    fn mk_snapshot(clock: i64, key: &str) -> ManifestSnapshot {
        ManifestSnapshot {
            key: key.to_string(),
            clock_ms: clock,
            size_bytes: 1024,
        }
    }

    #[test]
    fn needs_restore_none_when_no_remote_snapshot() {
        // Early-stage R2: chỉ có delta, chưa compact → không cần restore.
        let mut m = Manifest::empty("uid".to_string());
        append_delta_entries(&mut m, vec![make_entry("t", "k1", 100)]);
        assert!(m.latest_snapshot.is_none());

        let state = mk_state(50);
        assert!(
            needs_snapshot_restore(&m, &state).is_none(),
            "không snapshot remote → không trigger restore"
        );
    }

    #[test]
    fn needs_restore_triggers_when_local_behind_snapshot() {
        // Scenario: máy A offline dài, máy B đã compact lên clock 500.
        // Local đang ở clock 100 → gap 100-500 không còn trong manifest.deltas.
        let mut m = Manifest::empty("uid".to_string());
        m.latest_snapshot = Some(mk_snapshot(500, "snapshots/snap_500.db.zst"));

        let state = mk_state(100);
        let restore = needs_snapshot_restore(&m, &state);
        assert!(restore.is_some(), "local=100 < snap=500 → phải restore");
        assert_eq!(restore.unwrap().clock_ms, 500);
    }

    #[test]
    fn needs_restore_triggers_for_fresh_install() {
        // Fresh install: local_clock = 0 (default). Remote có snapshot → bootstrap.
        let mut m = Manifest::empty("uid".to_string());
        m.latest_snapshot = Some(mk_snapshot(1000, "snap_1000"));

        let state = mk_state(0);
        assert!(
            needs_snapshot_restore(&m, &state).is_some(),
            "local=0 + remote snap=1000 → trigger (covers fresh install)"
        );
    }

    #[test]
    fn needs_restore_skips_when_caught_up() {
        // Local đã apply tới snap_clock — delta sau đó vẫn trong manifest → normal pull.
        let mut m = Manifest::empty("uid".to_string());
        m.latest_snapshot = Some(mk_snapshot(500, "snap_500"));
        append_delta_entries(&mut m, vec![make_entry("t", "k1", 600)]);

        let state = mk_state(500);
        assert!(
            needs_snapshot_restore(&m, &state).is_none(),
            "local == snap_clock → deltas trong manifest đủ cover, không restore"
        );
    }

    #[test]
    fn needs_restore_skips_when_local_ahead_of_snapshot() {
        // Edge case: local đã pull deltas sau snapshot — không trigger restore again.
        let mut m = Manifest::empty("uid".to_string());
        m.latest_snapshot = Some(mk_snapshot(500, "snap_500"));

        let state = mk_state(700);
        assert!(
            needs_snapshot_restore(&m, &state).is_none(),
            "local ahead of snapshot → không cần restore"
        );
    }

    #[test]
    fn needs_restore_idempotent_across_calls() {
        // Gọi 2 lần cùng state → cùng kết quả.
        let mut m = Manifest::empty("uid".to_string());
        m.latest_snapshot = Some(mk_snapshot(500, "snap_500"));
        let state = mk_state(100);

        let r1 = needs_snapshot_restore(&m, &state);
        let r2 = needs_snapshot_restore(&m, &state);
        assert_eq!(r1.map(|s| s.clock_ms), r2.map(|s| s.clock_ms));
    }

    #[test]
    fn compute_pending_pulls_filters_by_clock() {
        let mut m = Manifest::empty("uid".to_string());
        append_delta_entries(
            &mut m,
            vec![
                make_entry("t", "k1", 100),
                make_entry("t", "k2", 200),
                make_entry("t", "k3", 300),
            ],
        );
        // local đã pull tới 150 → pending = k2, k3
        let pending = compute_pending_pulls(&m, 150);
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].key, "k2");
        assert_eq!(pending[1].key, "k3");
    }

    #[test]
    fn compute_pending_pulls_empty_when_caught_up() {
        let mut m = Manifest::empty("uid".to_string());
        append_delta_entries(&mut m, vec![make_entry("t", "k1", 100)]);
        let pending = compute_pending_pulls(&m, 100);
        assert!(pending.is_empty(), "clock == last_pulled → đã apply");
    }

    #[test]
    fn compute_pending_pulls_all_for_fresh_client() {
        let mut m = Manifest::empty("uid".to_string());
        append_delta_entries(
            &mut m,
            vec![make_entry("t", "k1", 100), make_entry("t", "k2", 200)],
        );
        let pending = compute_pending_pulls(&m, 0);
        assert_eq!(pending.len(), 2);
    }

    // ---------- Compaction ----------

    #[test]
    fn compact_after_snapshot_drops_old_deltas() {
        let mut m = Manifest::empty("uid".to_string());
        append_delta_entries(
            &mut m,
            vec![
                make_entry("t", "k1", 100),
                make_entry("t", "k2", 200),
                make_entry("t", "k3", 300),
            ],
        );

        let new_snap = ManifestSnapshot {
            key: "snap.zst".to_string(),
            clock_ms: 200,
            size_bytes: 500_000,
        };
        let dropped = compact_after_snapshot(&mut m, new_snap.clone());
        assert_eq!(dropped, 2, "k1 (100) và k2 (200) bị drop");
        assert_eq!(m.deltas.len(), 1);
        assert_eq!(m.deltas[0].key, "k3");
        assert_eq!(m.latest_snapshot, Some(new_snap));
    }

    #[test]
    fn compact_keeps_deltas_newer_than_snapshot() {
        let mut m = Manifest::empty("uid".to_string());
        append_delta_entries(&mut m, vec![make_entry("t", "future", 500)]);
        let snap = ManifestSnapshot {
            key: "s".to_string(),
            clock_ms: 100,
            size_bytes: 0,
        };
        let dropped = compact_after_snapshot(&mut m, snap);
        assert_eq!(dropped, 0);
        assert_eq!(m.deltas.len(), 1);
    }

    // ---------- bump_updated_at ----------

    #[test]
    fn bump_updated_at_is_monotonic() {
        let mut m = Manifest::empty("uid".to_string());
        bump_updated_at(&mut m, 100);
        bump_updated_at(&mut m, 50); // lùi → không apply
        assert_eq!(m.updated_at_ms, 100);
        bump_updated_at(&mut m, 200);
        assert_eq!(m.updated_at_ms, 200);
    }

    // ---------- CAS retry plan ----------

    #[test]
    fn cas_retry_grants_up_to_max() {
        let mut plan = CasRetryPlan::with_max(3);
        assert_eq!(plan.try_attempt().unwrap(), 1);
        assert_eq!(plan.try_attempt().unwrap(), 2);
        assert_eq!(plan.try_attempt().unwrap(), 3);
        assert!(plan.try_attempt().is_err(), "4th attempt phải fail");
    }

    #[test]
    fn cas_retry_default_is_3() {
        let plan = CasRetryPlan::default();
        assert_eq!(plan.attempts(), 0);
        let mut p = plan;
        let _ = p.try_attempt();
        let _ = p.try_attempt();
        let _ = p.try_attempt();
        assert!(p.try_attempt().is_err());
    }

    // ---------- Integration-ish: simulate push manifest flow ----------

    #[test]
    fn full_push_flow_simulation() {
        // Simulate: local push 2 deltas → append to remote manifest → verify.
        let mut remote_manifest = Manifest::empty("uid".to_string());
        remote_manifest.updated_at_ms = 50;

        let new_from_push = vec![
            ManifestDeltaEntry {
                table: "raw_shopee_clicks".to_string(),
                key: "deltas/raw_shopee_clicks/100_500.ndjson.zst".to_string(),
                cursor_lo: "1".to_string(),
                cursor_hi: "100".to_string(),
                clock_ms: 500,
                size_bytes: 1024,
                row_count: 100,
            },
            ManifestDeltaEntry {
                table: "manual_entries".to_string(),
                key: "deltas/manual_entries/2026-04-24_500.ndjson.zst".to_string(),
                cursor_lo: "2026-04-24T08:00:00Z".to_string(),
                cursor_hi: "2026-04-24T08:05:00Z".to_string(),
                clock_ms: 500,
                size_bytes: 512,
                row_count: 5,
            },
        ];

        let added = append_delta_entries(&mut remote_manifest, new_from_push);
        bump_updated_at(&mut remote_manifest, 500);

        assert_eq!(added, 2);
        assert_eq!(remote_manifest.deltas.len(), 2);
        assert_eq!(remote_manifest.updated_at_ms, 500);

        // Round-trip qua JSON (simulate PUT + subsequent GET).
        let json = serde_json::to_string(&remote_manifest).unwrap();
        let back: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back, remote_manifest);

        // Next machine pulls → compute_pending_pulls với last_pulled=0 → cả 2.
        let pending = compute_pending_pulls(&back, 0);
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn push_then_compact_flow() {
        let mut m = Manifest::empty("u".to_string());
        // 100 deltas accumulate over time
        let entries: Vec<_> = (0..100)
            .map(|i| make_entry("t", &format!("k{i}"), i as i64 * 10))
            .collect();
        append_delta_entries(&mut m, entries);
        assert_eq!(m.deltas.len(), 100);

        // Compaction: snapshot absorbs first 50 deltas (clock 0..490).
        let snap = ManifestSnapshot {
            key: "snap.zst".to_string(),
            clock_ms: 490,
            size_bytes: 100_000,
        };
        let dropped = compact_after_snapshot(&mut m, snap);
        assert_eq!(dropped, 50);
        assert_eq!(m.deltas.len(), 50);
        assert!(m.latest_snapshot.is_some());

        // Clock của delta còn lại >= 500.
        assert!(m.deltas.iter().all(|d| d.clock_ms >= 500));

        // Sanity: SV_CURRENT preserved qua compact.
        assert_eq!(m.version, SV_CURRENT);
    }
}
