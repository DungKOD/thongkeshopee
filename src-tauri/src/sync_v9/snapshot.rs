//! Snapshot create/restore — cold bootstrap cho máy mới + compaction output.
//!
//! **Flow create:** `VACUUM INTO <tmp>` → read bytes → zstd compress →
//! delete tmp. Upload compressed bytes lên R2 ở Phase 6.
//!
//! **Flow restore:** decompress → write `<db>.pending.db` → PRAGMA
//! integrity_check → atomic rename. Tận dụng `.pending.db` pattern của v7
//! multi-tenant (apply_pending_sync).
//!
//! **Rule giữ data #1 (atomic restore):** integrity fail → delete pending,
//! giữ live DB cũ. Crash giữa download → pending partial → next start detect
//! qua integrity_check, reject + delete.

use anyhow::{anyhow, Context, Result};
use rusqlite::Connection;
use std::path::{Path, PathBuf};

use super::compress::{sha256_hex, zstd_compress, zstd_decompress};

/// Metadata + compressed bytes của 1 snapshot tạo mới. Caller upload lên R2.
#[derive(Debug, Clone)]
pub struct SnapshotArtifact {
    /// zstd-compressed SQLite file bytes, ready để PUT R2.
    pub bytes: Vec<u8>,
    /// Size của SQLite file raw (pre-compression).
    pub raw_size_bytes: u64,
    /// Size compressed = `bytes.len()`.
    pub compressed_size_bytes: u64,
    /// SHA-256 hex của `bytes` — integrity check + skip-identical.
    pub hash: String,
    /// HLC clock_ms lúc tạo. Caller pass từ `next_hlc_ms`.
    pub clock_ms: i64,
    /// Suggested R2 key theo plan layout.
    pub suggested_r2_key: String,
}

/// Tạo snapshot từ live DB connection.
///
/// Dùng `VACUUM INTO '<tmp_path>'` — sinh SQLite file sạch (no WAL, compact
/// freelist). Yêu cầu free disk ≥ current DB size (plan I2). Caller kiểm
/// tra space trước nếu cần (helper ở bootstrap.rs).
///
/// Temp file tạo trong `temp_dir`, auto-delete sau khi compress xong (hoặc
/// khi Drop — hàm không leak file ngay cả nếu compress fail).
pub fn create_snapshot(
    source: &Connection,
    temp_dir: &Path,
    clock_ms: i64,
) -> Result<SnapshotArtifact> {
    if !temp_dir.exists() {
        std::fs::create_dir_all(temp_dir)
            .with_context(|| format!("create temp_dir {}", temp_dir.display()))?;
    }

    // Temp path với clock_ms để tránh collision nếu có concurrent create.
    let temp_path = temp_dir.join(format!("v9_snapshot_{clock_ms}.tmp.db"));
    let _guard = TempFileGuard::new(temp_path.clone());

    // VACUUM INTO yêu cầu literal string, không parameter. Safe vì path
    // ta build — không từ user input. Escape single-quote defensive.
    let escaped_path = temp_path.to_string_lossy().replace('\'', "''");
    let sql = format!("VACUUM INTO '{escaped_path}'");
    source
        .execute(&sql, [])
        .context("VACUUM INTO snapshot temp file")?;

    let raw_bytes = std::fs::read(&temp_path)
        .with_context(|| format!("read snapshot tmp {}", temp_path.display()))?;
    let raw_size_bytes = raw_bytes.len() as u64;

    let compressed = zstd_compress(&raw_bytes).context("zstd compress snapshot")?;
    let hash = sha256_hex(&compressed);
    let compressed_size_bytes = compressed.len() as u64;

    let suggested_r2_key = format!("snapshots/snap_{clock_ms}.db.zst");

    Ok(SnapshotArtifact {
        bytes: compressed,
        raw_size_bytes,
        compressed_size_bytes,
        hash,
        clock_ms,
        suggested_r2_key,
    })
}

/// RAII guard — đảm bảo temp file bị delete dù function early-return/panic.
struct TempFileGuard {
    path: PathBuf,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Kết quả restore snapshot. Caller biết integrity OK + size thực + cần
/// apply pending hay không.
#[derive(Debug, Clone)]
pub struct RestoreOutcome {
    /// Path tới file `.pending.db` đã write. Swap vào live DB qua
    /// `apply_pending_sync` (v7 infra) hoặc atomic rename trực tiếp.
    pub pending_path: PathBuf,
    /// Size raw SQLite file sau decompress.
    pub raw_size_bytes: u64,
}

/// Restore snapshot bytes → write ra `pending_path` + verify integrity.
///
/// **Atomic invariant (rule giữ data):** Hàm KHÔNG swap với live DB. Chỉ
/// ghi vào `pending_path`. Integrity fail → delete pending, return Err,
/// caller giữ live DB cũ.
///
/// Caller (Phase 8 command) responsible cho:
/// - Pass `pending_path` = `<live_db>.pending.db`
/// - Sau khi hàm OK, close live conn + rename pending → live + reopen conn
///
/// Verify gồm:
/// - zstd decompress (reject non-zstd frame)
/// - PRAGMA integrity_check = 'ok'
/// - Schema version hiện diện (>= v1)
pub fn restore_snapshot_to_pending(
    compressed: &[u8],
    pending_path: &Path,
) -> Result<RestoreOutcome> {
    // Clean up cũ nếu có (trước lần crash trước).
    if pending_path.exists() {
        std::fs::remove_file(pending_path)
            .with_context(|| format!("remove stale pending {}", pending_path.display()))?;
    }

    let raw = zstd_decompress(compressed).context("decompress snapshot")?;
    let raw_size_bytes = raw.len() as u64;

    // Ensure parent exists (caller path có thể là fresh folder).
    if let Some(parent) = pending_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create parent {}", parent.display()))?;
        }
    }

    std::fs::write(pending_path, &raw)
        .with_context(|| format!("write pending {}", pending_path.display()))?;

    // Verify qua separate connection — không touch live DB.
    let guard = RestoreErrorGuard::new(pending_path.to_path_buf());
    verify_integrity(pending_path)?;
    verify_has_sync_state(pending_path)?;
    guard.dismiss();

    Ok(RestoreOutcome {
        pending_path: pending_path.to_path_buf(),
        raw_size_bytes,
    })
}

/// RAII — delete pending file nếu verify fail (Drop = cleanup).
struct RestoreErrorGuard {
    path: PathBuf,
    dismissed: bool,
}

impl RestoreErrorGuard {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            dismissed: false,
        }
    }
    fn dismiss(mut self) {
        self.dismissed = true;
    }
}

impl Drop for RestoreErrorGuard {
    fn drop(&mut self) {
        if !self.dismissed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Run `PRAGMA integrity_check` trên file DB. Return Err nếu kết quả != "ok".
pub fn verify_integrity(db_path: &Path) -> Result<()> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("open for integrity check: {}", db_path.display()))?;
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .context("run PRAGMA integrity_check")?;
    if result != "ok" {
        anyhow::bail!("integrity_check failed: {}", result);
    }
    Ok(())
}

/// Verify DB có `sync_state` table + singleton row.
/// Catch trường hợp snapshot bytes là random data nhưng happens to be valid SQLite.
fn verify_has_sync_state(db_path: &Path) -> Result<()> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("open for schema check: {}", db_path.display()))?;
    let has_table: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'sync_state'",
            [],
            |r| r.get(0),
        )
        .context("check sync_state table exists")?;
    if has_table == 0 {
        anyhow::bail!("snapshot missing sync_state table");
    }
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM sync_state", [], |r| r.get(0))
        .context("count sync_state rows")?;
    if count == 0 {
        anyhow::bail!("snapshot sync_state empty");
    }
    Ok(())
}

/// Đọc `owner_uid` từ snapshot file. Caller dùng để verify multi-tenant
/// match trước khi swap (rule A5: không restore DB user khác vào folder
/// user hiện tại).
pub fn read_snapshot_owner_uid(db_path: &Path) -> Result<Option<String>> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("open for owner check: {}", db_path.display()))?;
    conn.query_row(
        "SELECT owner_uid FROM sync_state WHERE id = 1",
        [],
        |r| r.get(0),
    )
    .context("read owner_uid")
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate_for_tests;
    use rusqlite::params;

    fn make_test_db() -> (tempfile::TempDir, PathBuf, Connection) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("source.db");
        let conn = Connection::open(&db_path).expect("open db");
        let schema = include_str!("../db/schema.sql");
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(schema).unwrap();
        migrate_for_tests(&conn).unwrap();
        (dir, db_path, conn)
    }

    #[test]
    fn create_snapshot_produces_valid_zstd() {
        let (dir, _db_path, conn) = make_test_db();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();

        let artifact = create_snapshot(&conn, dir.path(), 1_000_000).unwrap();
        assert!(!artifact.bytes.is_empty());
        assert!(artifact.raw_size_bytes > 0);
        assert!(artifact.compressed_size_bytes <= artifact.raw_size_bytes);
        assert_eq!(artifact.hash.len(), 64);
        assert_eq!(artifact.clock_ms, 1_000_000);
        assert!(artifact.suggested_r2_key.contains("snap_1000000"));
    }

    #[test]
    fn create_snapshot_cleans_up_temp() {
        let (dir, _db_path, conn) = make_test_db();
        let _ = create_snapshot(&conn, dir.path(), 1_000_000).unwrap();
        let leftover = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains("v9_snapshot_"))
            .count();
        assert_eq!(leftover, 0, "temp file phải được cleanup");
    }

    #[test]
    fn restore_snapshot_roundtrip() {
        let (dir, _src_path, src_conn) = make_test_db();
        src_conn
            .execute(
                "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
                [],
            )
            .unwrap();
        src_conn
            .execute(
                "INSERT INTO manual_entries
                 (sub_id1, day_date, override_clicks, created_at, updated_at)
                 VALUES('test_subid', '2026-04-20', 42, 'now', '2026-04-24T10:00:00Z')",
                [],
            )
            .unwrap();

        let artifact = create_snapshot(&src_conn, dir.path(), 1_000_000).unwrap();
        drop(src_conn);

        let target = dir.path().join("restored.db.pending.db");
        let outcome = restore_snapshot_to_pending(&artifact.bytes, &target).unwrap();
        assert!(outcome.pending_path.exists());

        // Verify data by opening restored DB.
        let conn = Connection::open(&target).unwrap();
        let clicks: i64 = conn
            .query_row(
                "SELECT override_clicks FROM manual_entries WHERE sub_id1 = 'test_subid'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(clicks, 42);
    }

    #[test]
    fn restore_rejects_non_zstd() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("x.db.pending.db");
        let err = restore_snapshot_to_pending(b"not zstd garbage", &target).unwrap_err();
        assert!(format!("{err:?}").contains("zstd"));
        assert!(!target.exists(), "pending phải cleanup khi decompress fail");
    }

    #[test]
    fn restore_rejects_non_sqlite_zstd_payload() {
        // Valid zstd frame nhưng payload không phải SQLite.
        let dir = tempfile::tempdir().unwrap();
        let bogus = zstd_compress(b"this is not a sqlite database").unwrap();
        let target = dir.path().join("y.db.pending.db");
        let err = restore_snapshot_to_pending(&bogus, &target).unwrap_err();
        // Integrity check hoặc schema_version check fail.
        let msg = format!("{err:?}");
        assert!(
            msg.contains("integrity") || msg.contains("sync_state") || msg.contains("schema"),
            "error phải rõ ràng: {msg}"
        );
        assert!(!target.exists(), "rule giữ data: pending cleanup khi verify fail");
    }

    #[test]
    fn verify_integrity_passes_for_valid_db() {
        let (_dir, db_path, _conn) = make_test_db();
        verify_integrity(&db_path).unwrap();
    }

    #[test]
    fn read_snapshot_owner_uid_returns_set_value() {
        let (dir, db_path, conn) = make_test_db();
        conn.execute(
            "UPDATE sync_state SET owner_uid = ? WHERE id = 1",
            params!["test_uid_abc"],
        )
        .unwrap();
        drop(conn);
        let uid = read_snapshot_owner_uid(&db_path).unwrap();
        assert_eq!(uid.as_deref(), Some("test_uid_abc"));
    }

    #[test]
    fn restore_overwrites_stale_pending() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("stale.db.pending.db");
        // Simulate stale pending from prior failed restore.
        std::fs::write(&target, b"stale data").unwrap();

        let (_src_dir, _src_path, src_conn) = make_test_db();
        let artifact = create_snapshot(&src_conn, _src_dir.path(), 1_000_000).unwrap();
        drop(src_conn);

        restore_snapshot_to_pending(&artifact.bytes, &target).unwrap();
        // File content phải là DB mới, không phải "stale data".
        let bytes = std::fs::read(&target).unwrap();
        assert!(
            bytes.starts_with(b"SQLite format 3\0"),
            "stale content phải bị ghi đè bằng SQLite file"
        );
    }
}
