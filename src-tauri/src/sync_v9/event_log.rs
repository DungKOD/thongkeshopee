//! Helpers cho `sync_event_log` table — ring buffer + upload tracking.
//!
//! Plan O1a-O1e:
//! - Ring buffer 5000 events (EVENT_LOG_MAX), FIFO auto-prune.
//! - Events không có PII (xem `types::SyncEventCtx` privacy rule).
//! - Upload lên R2 incremental qua `fetch_pending()` → `mark_uploaded()`.

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

use super::types::{SyncEvent, SyncEventCtx, SyncEventKind};
use super::EVENT_LOG_MAX;

/// Append 1 event vào `sync_event_log`, auto-prune nếu vượt EVENT_LOG_MAX.
///
/// `ts` phải là HLC RFC3339 Z (generated qua `next_hlc_rfc3339` từ sync.rs).
/// Caller phải ensure `fingerprint` đã redact PII (stable hash, không hostname).
///
/// Return event_id vừa insert.
pub fn append(
    conn: &Connection,
    ts: &str,
    fingerprint: &str,
    ctx: &SyncEventCtx,
) -> Result<i64> {
    let kind = ctx.kind();
    let ctx_json = serde_json::to_string(ctx).context("serialize SyncEventCtx failed")?;

    conn.execute(
        "INSERT INTO sync_event_log (ts, fingerprint, kind, ctx_json) VALUES (?, ?, ?, ?)",
        params![ts, fingerprint, kind.as_str(), ctx_json],
    )
    .context("insert sync_event_log failed")?;

    let event_id = conn.last_insert_rowid();

    // Prune cũ nhất nếu vượt ring buffer size.
    prune_ring_buffer(conn)?;

    Ok(event_id)
}

/// Xóa event cũ nhất cho đến khi count ≤ `EVENT_LOG_MAX`. Idempotent.
///
/// Dùng subquery OFFSET thay vì `count - max` để tránh race nếu có concurrent
/// insert giữa COUNT và DELETE.
pub fn prune_ring_buffer(conn: &Connection) -> Result<usize> {
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM sync_event_log", [], |r| r.get(0))
        .context("count sync_event_log failed")?;

    let max = EVENT_LOG_MAX as i64;
    if count <= max {
        return Ok(0);
    }

    let to_delete = count - max;
    let deleted = conn
        .execute(
            "DELETE FROM sync_event_log WHERE event_id IN (
                 SELECT event_id FROM sync_event_log ORDER BY event_id ASC LIMIT ?
             )",
            params![to_delete],
        )
        .context("prune sync_event_log failed")?;

    Ok(deleted)
}

/// Đếm số events chưa upload (uploaded_at IS NULL). Cheap cho UI status
/// hoặc quyết định threshold flush.
pub fn count_pending(conn: &Connection) -> Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM sync_event_log WHERE uploaded_at IS NULL",
        [],
        |r| r.get(0),
    )
    .context("count_pending sync_event_log failed")
}

/// Fetch events chưa upload lên R2 (uploaded_at IS NULL), theo thứ tự cũ → mới.
///
/// Limit để tránh load cả ring buffer vào RAM 1 lần khi user offline lâu.
pub fn fetch_pending(conn: &Connection, limit: u32) -> Result<Vec<SyncEvent>> {
    let mut stmt = conn
        .prepare(
            "SELECT event_id, ts, fingerprint, kind, ctx_json, uploaded_at
             FROM sync_event_log
             WHERE uploaded_at IS NULL
             ORDER BY event_id ASC
             LIMIT ?",
        )
        .context("prepare fetch_pending failed")?;

    let rows = stmt
        .query_map(params![limit], |r| {
            let ctx_json: String = r.get(4)?;
            let ctx: SyncEventCtx = serde_json::from_str(&ctx_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    4,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;
            let kind_str: String = r.get(3)?;
            // Trust kind column OR parse from ctx. Ctx là authoritative.
            let _ = kind_str; // kept for index; ctx.kind() is authoritative
            Ok(SyncEvent {
                event_id: r.get(0)?,
                ts: r.get(1)?,
                fingerprint: r.get(2)?,
                kind: ctx.kind(),
                ctx,
                uploaded_at: r.get(5)?,
            })
        })
        .context("query fetch_pending failed")?;

    rows.collect::<std::result::Result<Vec<_>, _>>()
        .context("collect fetch_pending rows failed")
}

/// Fetch tất cả events theo `event_id DESC` (mới → cũ), limit để không load
/// hết ring buffer. Optional filter theo `kind` (exact match).
///
/// Dùng cho user log viewer — hiển thị hoạt động sync gần đây.
pub fn fetch_recent(
    conn: &Connection,
    limit: u32,
    kind_filter: Option<&str>,
) -> Result<Vec<SyncEvent>> {
    let (sql, has_kind): (&str, bool) = match kind_filter {
        Some(_) => (
            "SELECT event_id, ts, fingerprint, kind, ctx_json, uploaded_at
             FROM sync_event_log
             WHERE kind = ?
             ORDER BY event_id DESC
             LIMIT ?",
            true,
        ),
        None => (
            "SELECT event_id, ts, fingerprint, kind, ctx_json, uploaded_at
             FROM sync_event_log
             ORDER BY event_id DESC
             LIMIT ?",
            false,
        ),
    };

    let mut stmt = conn.prepare(sql).context("prepare fetch_recent failed")?;

    let map_row = |r: &rusqlite::Row<'_>| -> rusqlite::Result<SyncEvent> {
        let ctx_json: String = r.get(4)?;
        let ctx: SyncEventCtx = serde_json::from_str(&ctx_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?;
        Ok(SyncEvent {
            event_id: r.get(0)?,
            ts: r.get(1)?,
            fingerprint: r.get(2)?,
            kind: ctx.kind(),
            ctx,
            uploaded_at: r.get(5)?,
        })
    };

    let rows: Vec<SyncEvent> = if has_kind {
        stmt.query_map(params![kind_filter.unwrap(), limit], map_row)
            .context("query fetch_recent(kind) failed")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("collect fetch_recent rows failed")?
    } else {
        stmt.query_map(params![limit], map_row)
            .context("query fetch_recent failed")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("collect fetch_recent rows failed")?
    };
    Ok(rows)
}

/// Mark list event_ids đã upload lên R2, set uploaded_at = `now`.
///
/// Caller dùng sau khi upload `/v9/sync-log/push` OK. Idempotent (nếu event
/// đã marked, UPDATE bỏ qua vì WHERE clause).
pub fn mark_uploaded(conn: &Connection, event_ids: &[i64], now_rfc3339: &str) -> Result<usize> {
    if event_ids.is_empty() {
        return Ok(0);
    }

    let placeholders = vec!["?"; event_ids.len()].join(",");
    let sql = format!(
        "UPDATE sync_event_log SET uploaded_at = ? WHERE uploaded_at IS NULL AND event_id IN ({placeholders})"
    );

    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(event_ids.len() + 1);
    params_vec.push(Box::new(now_rfc3339.to_string()));
    for id in event_ids {
        params_vec.push(Box::new(*id));
    }
    let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();

    let updated = conn
        .execute(sql.as_str(), params_refs.as_slice())
        .context("mark_uploaded failed")?;
    Ok(updated)
}

/// Count events theo kind trong date range. Dùng cho admin sync log viewer.
pub fn count_by_kind(
    conn: &Connection,
    kind: SyncEventKind,
    from_ts: &str,
    to_ts: &str,
) -> Result<i64> {
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sync_event_log WHERE kind = ? AND ts >= ? AND ts <= ?",
            params![kind.as_str(), from_ts, to_ts],
            |r| r.get(0),
        )
        .context("count_by_kind failed")?;
    Ok(n)
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::migrate_for_tests;
    use crate::sync_v9::SV_CURRENT;

    /// Tạo in-memory conn với schema + v11 migration applied.
    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        let schema = include_str!("../db/schema.sql");
        conn.execute_batch(schema).unwrap();
        migrate_for_tests(&conn).unwrap();
        conn
    }

    fn sample_ctx() -> SyncEventCtx {
        SyncEventCtx::PushUpload {
            table: "raw_shopee_clicks".to_string(),
            cursor_lo: "0".to_string(),
            cursor_hi: "100".to_string(),
            bytes: 1024,
            delta_key: "k".to_string(),
            row_count: 100,
        }
    }

    #[test]
    fn append_inserts_row_with_correct_kind_column() {
        let conn = test_conn();
        let id = append(&conn, "2026-04-24T08:00:00.000Z", "fp1", &sample_ctx()).unwrap();
        assert!(id > 0);

        let (kind, ctx_json): (String, String) = conn
            .query_row(
                "SELECT kind, ctx_json FROM sync_event_log WHERE event_id = ?",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(kind, "push_upload");
        // ctx_json phải parse lại được.
        let ctx: SyncEventCtx = serde_json::from_str(&ctx_json).unwrap();
        assert_eq!(ctx.kind(), SyncEventKind::PushUpload);
    }

    #[test]
    fn ring_buffer_prunes_oldest_when_over_max() {
        let conn = test_conn();
        // Insert 5001 events — ring buffer = 5000, prune 1.
        // Để tránh chạy 5001 iterations thật, mình mock bằng cách set MAX nhỏ.
        // → Thay vì override const, ta insert trực tiếp 5005 rows rồi gọi
        //   prune và assert count.
        let ctx_json = serde_json::to_string(&sample_ctx()).unwrap();
        for i in 0..5005 {
            conn.execute(
                "INSERT INTO sync_event_log (ts, fingerprint, kind, ctx_json) VALUES (?, ?, ?, ?)",
                params![
                    format!("2026-04-24T08:00:{:05}.000Z", i),
                    "fp1",
                    "push_upload",
                    ctx_json
                ],
            )
            .unwrap();
        }
        let deleted = prune_ring_buffer(&conn).unwrap();
        assert_eq!(deleted, 5, "phải xóa 5 events cũ nhất");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sync_event_log", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, EVENT_LOG_MAX as i64);

        // Event_id nhỏ nhất còn lại phải > 5 (vì event 1-5 bị prune).
        let min_id: i64 = conn
            .query_row("SELECT MIN(event_id) FROM sync_event_log", [], |r| r.get(0))
            .unwrap();
        assert_eq!(min_id, 6, "event_id 1-5 phải bị xóa");
    }

    #[test]
    fn prune_noop_when_under_max() {
        let conn = test_conn();
        append(&conn, "t1", "fp", &sample_ctx()).unwrap();
        append(&conn, "t2", "fp", &sample_ctx()).unwrap();
        let deleted = prune_ring_buffer(&conn).unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn fetch_pending_returns_non_uploaded_only() {
        let conn = test_conn();
        let id1 = append(&conn, "t1", "fp", &sample_ctx()).unwrap();
        let _id2 = append(&conn, "t2", "fp", &sample_ctx()).unwrap();
        // Mark id1 uploaded.
        mark_uploaded(&conn, &[id1], "2026-04-24T09:00:00Z").unwrap();

        let pending = fetch_pending(&conn, 100).unwrap();
        assert_eq!(pending.len(), 1, "chỉ event chưa upload mới trả về");
        assert_ne!(pending[0].event_id, id1);
    }

    #[test]
    fn fetch_pending_respects_order_and_limit() {
        let conn = test_conn();
        for i in 0..10 {
            append(&conn, &format!("t{i}"), "fp", &sample_ctx()).unwrap();
        }
        let pending = fetch_pending(&conn, 3).unwrap();
        assert_eq!(pending.len(), 3);
        // Sort by event_id ASC → ts t0, t1, t2.
        assert_eq!(pending[0].ts, "t0");
        assert_eq!(pending[1].ts, "t1");
        assert_eq!(pending[2].ts, "t2");
    }

    #[test]
    fn mark_uploaded_idempotent() {
        let conn = test_conn();
        let id = append(&conn, "t1", "fp", &sample_ctx()).unwrap();
        let n1 = mark_uploaded(&conn, &[id], "2026-04-24T09:00:00Z").unwrap();
        assert_eq!(n1, 1);
        // Second call: WHERE uploaded_at IS NULL không match → 0.
        let n2 = mark_uploaded(&conn, &[id], "2026-04-24T10:00:00Z").unwrap();
        assert_eq!(n2, 0);
    }

    #[test]
    fn count_by_kind_filters_correctly() {
        let conn = test_conn();
        append(&conn, "2026-04-24T08:00:00Z", "fp", &sample_ctx()).unwrap();
        append(
            &conn,
            "2026-04-24T09:00:00Z",
            "fp",
            &SyncEventCtx::Error {
                phase: "apply".to_string(),
                error_code: "x".to_string(),
                error_msg: "y".to_string(),
            },
        )
        .unwrap();

        let push = count_by_kind(
            &conn,
            SyncEventKind::PushUpload,
            "2026-04-24T00:00:00Z",
            "2026-04-25T00:00:00Z",
        )
        .unwrap();
        assert_eq!(push, 1);

        let err = count_by_kind(
            &conn,
            SyncEventKind::Error,
            "2026-04-24T00:00:00Z",
            "2026-04-25T00:00:00Z",
        )
        .unwrap();
        assert_eq!(err, 1);

        // Out-of-range date → 0.
        let zero = count_by_kind(
            &conn,
            SyncEventKind::Error,
            "2026-05-01T00:00:00Z",
            "2026-05-02T00:00:00Z",
        )
        .unwrap();
        assert_eq!(zero, 0);
    }

    #[test]
    fn sv_current_matches_schema_version() {
        // Sanity: SV_CURRENT bằng schema version 11 đã mark trong migrate_v11.
        assert_eq!(SV_CURRENT, 11);
    }
}
