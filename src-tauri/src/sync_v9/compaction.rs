//! Compaction — tạo snapshot mới + clear delta pointers cũ trong manifest.
//!
//! **Trigger:** `manifest.deltas.len() > COMPACTION_DELTA_THRESHOLD` (100).
//! Client-side, chạy sau push thành công.
//!
//! **Flow (plan 2.10):**
//! 1. Check trigger
//! 2. `create_snapshot(conn, temp_dir, clock_ms)` — VACUUM INTO + zstd
//! 3. Upload snapshot lên R2 qua `client::upload_snapshot`
//! 4. CAS update manifest: `latest_snapshot = {...}`, `deltas = []` (hoặc giữ
//!    với grace period tuỳ policy)
//! 5. Verify upload OK trước khi clear — rule giữ data #1
//!
//! **Rule giữ data:** KHÔNG delete R2 delta files trong compaction này. Chỉ
//! clear manifest.deltas (pointer). Actual file cleanup defer cho cron Worker
//! (P10 server-side scheduled task). Grace period cho user khác cùng account
//! còn cần fetch delta chưa apply.

use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

use super::snapshot::{create_snapshot, SnapshotArtifact};
use super::types::{Manifest, ManifestSnapshot};
use super::COMPACTION_DELTA_THRESHOLD;

/// Check có nên chạy compaction không.
///
/// Hai trigger:
/// 1. **Threshold-based** (steady-state): `manifest.deltas.len() > COMPACTION_DELTA_THRESHOLD`.
///    Light user dồn deltas lâu → compact periodic (~1 lần/tháng với threshold 30).
/// 2. **Eager initial snapshot**: `latest_snapshot is None && !deltas.is_empty()`.
///    User chưa từng compact + đã có ít nhất 1 push → ép compact ngay.
///    Lý do: admin view (`/v9/admin/snapshot`) trả 404 nếu user chưa có snapshot.
///    Eager trigger đảm bảo snapshot tồn tại từ push đầu tiên → admin luôn xem
///    được mọi user đã sync ít nhất 1 lần.
pub fn should_compact(manifest: &Manifest) -> bool {
    if manifest.deltas.len() > COMPACTION_DELTA_THRESHOLD {
        return true;
    }
    if manifest.latest_snapshot.is_none() && !manifest.deltas.is_empty() {
        return true;
    }
    false
}

/// Kết quả compaction cho FE/logging.
#[derive(Debug, Clone)]
pub struct CompactionResult {
    pub snapshot_key: String,
    pub snapshot_clock_ms: i64,
    pub snapshot_size_bytes: u64,
    pub deltas_cleared: u32,
}

/// Build snapshot + update manifest (in-memory, caller CAS put).
///
/// **Atomicity:** caller giữ responsibility:
/// 1. Gọi này → `(SnapshotArtifact, updated Manifest)`
/// 2. Upload snapshot qua HTTP
/// 3. CAS put manifest với retry
/// 4. Chỉ khi 2+3 OK, log compaction_complete event
///
/// Crash giữa (2) → next run: manifest chưa update → try compaction lại
/// (idempotent — snapshot overwritten bằng clock_ms mới, old delta pointers
/// còn nguyên). Crash giữa (3) → snapshot đã trên R2 nhưng manifest chưa trỏ
/// tới → next run re-compact (snapshot mới, delta cũ still referenced). Cả
/// hai case đều không mất data.
pub fn prepare_compaction(
    conn: &Connection,
    manifest: &Manifest,
    temp_dir: &Path,
    clock_ms: i64,
    owner_uid: &str,
) -> Result<(SnapshotArtifact, Manifest, CompactionResult)> {
    // 1. Create snapshot từ live DB.
    let artifact = create_snapshot(conn, temp_dir, clock_ms)
        .context("create snapshot for compaction")?;

    // 2. R2 key: `users/{uid}/snapshots/snap_<clock>.db.zst` — Worker path
    //    layout thêm `users/{uid}/` prefix ở route handler, client pass
    //    relative phần. Nhưng manifest.latest_snapshot.key lưu relative path
    //    không có prefix (worker auto-prepend).
    let snapshot_key = artifact.suggested_r2_key.clone();

    // 3. Build updated manifest.
    let mut new_manifest = manifest.clone();
    new_manifest.latest_snapshot = Some(ManifestSnapshot {
        key: snapshot_key.clone(),
        clock_ms,
        size_bytes: artifact.compressed_size_bytes as i64,
    });
    let deltas_cleared = new_manifest.deltas.len() as u32;
    new_manifest.deltas.clear();
    new_manifest.updated_at_ms = clock_ms;
    // Owner UID giữ nguyên (không đổi per compaction).
    let _ = owner_uid;

    let result = CompactionResult {
        snapshot_key,
        snapshot_clock_ms: clock_ms,
        snapshot_size_bytes: artifact.compressed_size_bytes,
        deltas_cleared,
    };

    Ok((artifact, new_manifest, result))
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_v9::types::{Manifest, ManifestDeltaEntry};

    fn entry(key: &str, clock: i64) -> ManifestDeltaEntry {
        ManifestDeltaEntry {
            table: "raw_shopee_clicks".to_string(),
            key: key.to_string(),
            cursor_lo: "0".to_string(),
            cursor_hi: "100".to_string(),
            clock_ms: clock,
            size_bytes: 0,
            row_count: 1,
        }
    }

    #[test]
    fn should_compact_below_threshold_with_existing_snapshot() {
        let mut m = Manifest::empty("uid".to_string());
        m.latest_snapshot = Some(crate::sync_v9::types::ManifestSnapshot {
            key: "snapshots/snap_1.db.zst".to_string(),
            clock_ms: 1,
            size_bytes: 100,
        });
        for i in 0..COMPACTION_DELTA_THRESHOLD {
            m.deltas.push(entry(&format!("k{i}"), i as i64));
        }
        assert!(
            !should_compact(&m),
            "≤ threshold + đã có snapshot → không trigger"
        );
    }

    #[test]
    fn should_compact_above_threshold() {
        let mut m = Manifest::empty("uid".to_string());
        for i in 0..=COMPACTION_DELTA_THRESHOLD {
            m.deltas.push(entry(&format!("k{i}"), i as i64));
        }
        assert!(should_compact(&m), "> threshold → trigger");
    }

    #[test]
    fn should_compact_eager_initial_snapshot() {
        // User chưa từng compact (latest_snapshot=None) + có ít nhất 1 delta
        // → trigger ngay, kể cả deltas << threshold. Cần thiết để admin view
        // work cho mọi user đã sync 1 lần (Worker /v9/admin/snapshot 404
        // nếu latest_snapshot is None).
        let mut m = Manifest::empty("uid".to_string());
        m.deltas.push(entry("k0", 0));
        assert_eq!(m.latest_snapshot, None);
        assert!(should_compact(&m), "no snapshot + 1 delta → eager compact");
    }

    #[test]
    fn should_not_compact_empty_manifest() {
        let m = Manifest::empty("uid".to_string());
        assert!(!should_compact(&m), "no snapshot + no deltas → no-op");
    }

    #[test]
    fn prepare_compaction_clears_deltas_in_new_manifest() {
        use tempfile::TempDir;
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE tmp (id INTEGER); INSERT INTO tmp VALUES (1);")
            .unwrap();

        let mut manifest = Manifest::empty("uid".to_string());
        for i in 0..5 {
            manifest.deltas.push(entry(&format!("k{i}"), i));
        }

        let tmp = TempDir::new().unwrap();
        let (artifact, new_manifest, result) =
            prepare_compaction(&conn, &manifest, tmp.path(), 1_000, "uid").unwrap();

        assert!(!artifact.bytes.is_empty());
        assert_eq!(artifact.clock_ms, 1_000);
        assert!(new_manifest.deltas.is_empty(), "deltas cleared");
        assert_eq!(
            new_manifest.latest_snapshot.as_ref().unwrap().key,
            artifact.suggested_r2_key
        );
        assert_eq!(result.deltas_cleared, 5);
        assert_eq!(new_manifest.updated_at_ms, 1_000);
    }

    #[test]
    fn prepare_compaction_preserves_owner_uid() {
        use tempfile::TempDir;
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE tmp (id INTEGER);").unwrap();

        let manifest = Manifest::empty("owner-xyz".to_string());
        let tmp = TempDir::new().unwrap();
        let (_art, new_m, _r) =
            prepare_compaction(&conn, &manifest, tmp.path(), 500, "owner-xyz").unwrap();
        assert_eq!(new_m.uid, "owner-xyz");
    }
}
