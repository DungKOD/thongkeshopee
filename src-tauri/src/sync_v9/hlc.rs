//! HLC-lite (Hybrid Logical Clock) — timestamp monotonic counter chống
//! clock drift giữa máy. Relocate từ v8 `commands::sync` per nguyên tắc #4
//! exception (v9 reuse logic v8 nguyên vẹn).
//!
//! Nguyên tắc:
//! - `last_known_clock_ms` trong `sync_state` là counter. Mọi mutation
//!   sync-critical (manual_entries.updated_at, tombstones.deleted_at, delta
//!   event clock_ms) phải dùng `next_hlc_rfc3339` thay `Utc::now()`.
//! - Pull side gọi `absorb_remote_clock(max(remote timestamps))` để local
//!   clock không bao giờ tụt sau máy khác.
//! - Trade-off: máy A clock xa tương lai → máy B ăn theo sau sync. Không
//!   đúng wall time nhưng consistent ordering cross-device.

use rusqlite::Connection;

/// Next monotonic ms. Caller hold DB lock. Atomic với
/// `last_known_clock_ms` update trong cùng query.
pub fn next_hlc_ms(conn: &Connection) -> rusqlite::Result<i64> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let last: i64 = conn
        .query_row(
            "SELECT last_known_clock_ms FROM sync_state WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let next = std::cmp::max(now, last + 1);
    conn.execute(
        "UPDATE sync_state SET last_known_clock_ms = ?1 WHERE id = 1",
        [next],
    )?;
    Ok(next)
}

/// Convenience: next HLC timestamp as RFC3339 UTC string. Dùng cho
/// `manual_entries.updated_at` + `tombstones.deleted_at`.
pub fn next_hlc_rfc3339(conn: &Connection) -> rusqlite::Result<String> {
    let ms = next_hlc_ms(conn)?;
    Ok(ms_to_rfc3339(ms))
}

/// Sau merge, absorb max timestamp từ remote → local không tụt sau.
/// No-op nếu remote_max_ms <= 0 (parse fail fallback).
pub fn absorb_remote_clock(conn: &Connection, remote_max_ms: i64) -> rusqlite::Result<()> {
    if remote_max_ms <= 0 {
        return Ok(());
    }
    conn.execute(
        "UPDATE sync_state
         SET last_known_clock_ms = MAX(last_known_clock_ms, ?1)
         WHERE id = 1",
        [remote_max_ms],
    )?;
    Ok(())
}

/// ms → RFC3339 UTC string. Fallback Utc::now nếu ms invalid.
pub fn ms_to_rfc3339(ms: i64) -> String {
    use chrono::{TimeZone, Utc};
    Utc.timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339()
}

/// Parse RFC3339 → ms. 0 nếu parse fail (safe default cho absorb_remote_clock:
/// 0 < mọi real timestamp → no-op).
pub fn rfc3339_to_ms(s: &str) -> i64 {
    use chrono::DateTime;
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn_with_sync_state() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE sync_state (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 last_known_clock_ms INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO sync_state(id) VALUES(1);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn hlc_monotonic_in_one_process() {
        let conn = conn_with_sync_state();
        let a = next_hlc_ms(&conn).unwrap();
        let b = next_hlc_ms(&conn).unwrap();
        let c = next_hlc_ms(&conn).unwrap();
        assert!(b > a);
        assert!(c > b);
    }

    #[test]
    fn hlc_absorb_remote_clock_bumps_local() {
        let conn = conn_with_sync_state();
        let future_ms: i64 = 9_999_999_999_999;
        absorb_remote_clock(&conn, future_ms).unwrap();
        let next = next_hlc_ms(&conn).unwrap();
        assert!(next > future_ms);
    }

    #[test]
    fn hlc_no_backward_slip_with_remote_older() {
        let conn = conn_with_sync_state();
        let first = next_hlc_ms(&conn).unwrap();
        // Remote clock lùi 1h → local không được tụt.
        absorb_remote_clock(&conn, first - 3_600_000).unwrap();
        let next = next_hlc_ms(&conn).unwrap();
        assert!(next > first, "local không tụt sau remote cũ");
    }

    #[test]
    fn rfc3339_roundtrip() {
        let ms = 1_745_234_600_123_i64;
        let s = ms_to_rfc3339(ms);
        let back = rfc3339_to_ms(&s);
        assert_eq!(back, ms);
    }

    #[test]
    fn rfc3339_parse_fail_returns_zero() {
        assert_eq!(rfc3339_to_ms("not a date"), 0);
        assert_eq!(rfc3339_to_ms(""), 0);
    }

    #[test]
    fn absorb_zero_is_noop() {
        let conn = conn_with_sync_state();
        next_hlc_ms(&conn).unwrap(); // advance
        let before: i64 = conn
            .query_row("SELECT last_known_clock_ms FROM sync_state", [], |r| r.get(0))
            .unwrap();
        absorb_remote_clock(&conn, 0).unwrap();
        let after: i64 = conn
            .query_row("SELECT last_known_clock_ms FROM sync_state", [], |r| r.get(0))
            .unwrap();
        assert_eq!(before, after);
    }
}
