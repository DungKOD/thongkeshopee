//! Commands CRUD `app_settings` — key-value store cho user preferences sync.
//!
//! Keys hiện dùng (FE đặt convention dot-namespace):
//! - `profit_fee.tax_and_platform_rate` — số `"10.98"`
//! - `profit_fee.return_reserve_rate` — số `"9"`
//! - `auto_sync_enabled` — boolean `"true"` / `"false"`
//! - `click_source.<referrer>` — boolean enabled flag, key dynamic theo
//!   referrer thực user gặp (Facebook, TikTok, ...).
//!
//! Value JSON-encoded — FE parse: `JSON.parse(value)`. Không validate type
//! ở Rust để tránh schema rigid; FE responsibility.
//!
//! Sync: row qua sync_v9 descriptor `app_settings` (Upsert + UpdatedAt cursor).
//! LWW per-key qua updated_at HLC.

use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use tauri::State;

use crate::db::DbState;
use crate::sync_v9::hlc::next_hlc_rfc3339;
use super::{assert_not_bootstrapping, CmdError, CmdResult};

/// Pair (key, value) cho list endpoint. Camel case ở serialize.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppSettingEntry {
    pub key: String,
    pub value: String,
}

/// Đọc 1 setting. None nếu key chưa từng set (FE fallback default).
#[tauri::command]
pub fn get_app_setting(
    state: State<'_, DbState>,
    key: String,
) -> CmdResult<Option<String>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let v: Option<String> = conn
        .query_row(
            "SELECT value FROM app_settings WHERE key = ?",
            [key],
            |r| r.get(0),
        )
        .optional()?;
    Ok(v)
}

/// Liệt kê toàn bộ settings (dùng lúc app khởi động để hydrate state). Sort
/// theo key để output deterministic — dễ test + log.
#[tauri::command]
pub fn list_app_settings(
    state: State<'_, DbState>,
) -> CmdResult<Vec<AppSettingEntry>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn
        .prepare("SELECT key, value FROM app_settings ORDER BY key")?;
    let rows = stmt
        .query_map([], |r| {
            Ok(AppSettingEntry {
                key: r.get(0)?,
                value: r.get(1)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Set/UPDATE setting. UPSERT trên `key` PK với HLC-lite updated_at để
/// monotonic cross-machine. LWW conflict resolution: delta nào có updated_at
/// muộn hơn thắng (xem sync_v9::apply for Upsert handling).
///
/// `value` được trust nguyên dạng — FE đã JSON.stringify trước khi gửi.
///
/// Trả `true` nếu DB thực sự thay đổi, `false` nếu value trùng với row hiện
/// có. Caller dùng flag để bỏ qua mutation event → tránh "Chờ đồng bộ" khi
/// user bấm cùng giá trị cũ.
#[tauri::command]
pub fn set_app_setting(
    state: State<'_, DbState>,
    key: String,
    value: String,
) -> CmdResult<bool> {
    if key.is_empty() {
        return Err(CmdError::msg("app_setting key không được rỗng"));
    }
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    assert_not_bootstrapping(&conn)?;
    let tx = conn.transaction()?;
    let existing: Option<String> = tx
        .query_row(
            "SELECT value FROM app_settings WHERE key = ?",
            [&key],
            |r| r.get(0),
        )
        .optional()?;
    if existing.as_deref() == Some(value.as_str()) {
        return Ok(false);
    }
    let now = next_hlc_rfc3339(&tx)?;
    tx.execute(
        "INSERT INTO app_settings (key, value, updated_at)
         VALUES (?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET
            value      = excluded.value,
            updated_at = excluded.updated_at",
        params![key, value, now],
    )?;
    tx.commit()?;
    Ok(true)
}

/// Bulk set — atomic transaction cho migration localStorage → DB. Tránh fail
/// giữa chừng để lại state half-migrated. Mọi key dùng cùng HLC timestamp.
#[tauri::command]
pub fn set_app_settings_bulk(
    state: State<'_, DbState>,
    entries: Vec<AppSettingEntry>,
) -> CmdResult<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    assert_not_bootstrapping(&conn)?;
    let tx = conn.transaction()?;
    let now = next_hlc_rfc3339(&tx)?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO app_settings (key, value, updated_at)
             VALUES (?, ?, ?)
             ON CONFLICT(key) DO UPDATE SET
                value      = excluded.value,
                updated_at = excluded.updated_at",
        )?;
        for e in &entries {
            if e.key.is_empty() {
                return Err(CmdError::msg("app_setting key không được rỗng"));
            }
            stmt.execute(params![e.key, e.value, now])?;
        }
    }
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::db::migrate_for_tests(&conn).unwrap();
        conn
    }

    /// Direct UPSERT helper — bypass Tauri State. Tests low-level SQL.
    fn upsert(conn: &Connection, key: &str, value: &str) {
        let tx_now = next_hlc_rfc3339(conn).unwrap();
        conn.execute(
            "INSERT INTO app_settings (key, value, updated_at)
             VALUES (?, ?, ?)
             ON CONFLICT(key) DO UPDATE SET
                value      = excluded.value,
                updated_at = excluded.updated_at",
            params![key, value, tx_now],
        )
        .unwrap();
    }

    fn read(conn: &Connection, key: &str) -> Option<String> {
        conn.query_row(
            "SELECT value FROM app_settings WHERE key = ?",
            [key],
            |r| r.get::<_, String>(0),
        )
        .optional()
        .unwrap()
    }

    #[test]
    fn upsert_inserts_new_key() {
        let conn = fresh_db();
        upsert(&conn, "profit_fee.tax_and_platform_rate", "10.98");
        assert_eq!(read(&conn, "profit_fee.tax_and_platform_rate"),
                   Some("10.98".to_string()));
    }

    #[test]
    fn upsert_overwrites_existing_key() {
        let conn = fresh_db();
        upsert(&conn, "auto_sync_enabled", "true");
        upsert(&conn, "auto_sync_enabled", "false");
        assert_eq!(read(&conn, "auto_sync_enabled"), Some("false".to_string()));
    }

    #[test]
    fn upsert_updated_at_monotonic() {
        let conn = fresh_db();
        upsert(&conn, "k", "v1");
        let t1: String = conn.query_row(
            "SELECT updated_at FROM app_settings WHERE key = 'k'", [], |r| r.get(0),
        ).unwrap();
        upsert(&conn, "k", "v2");
        let t2: String = conn.query_row(
            "SELECT updated_at FROM app_settings WHERE key = 'k'", [], |r| r.get(0),
        ).unwrap();
        assert!(t2 > t1, "HLC monotonic: t2 ({t2}) phải > t1 ({t1})");
    }

    #[test]
    fn dynamic_click_source_keys() {
        let conn = fresh_db();
        // FE có thể tạo arbitrary key dynamic theo referrer thực user gặp.
        upsert(&conn, "click_source.Facebook", "true");
        upsert(&conn, "click_source.TikTok", "false");
        upsert(&conn, "click_source.facebook.com", "true");
        upsert(&conn, "click_source.youtube.com/shorts", "true");

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM app_settings WHERE key LIKE 'click_source.%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 4);
    }

    #[test]
    fn missing_key_returns_none() {
        let conn = fresh_db();
        assert_eq!(read(&conn, "never_set"), None);
    }

    #[test]
    fn list_sorted_by_key() {
        let conn = fresh_db();
        upsert(&conn, "z_key", "z");
        upsert(&conn, "a_key", "a");
        upsert(&conn, "m_key", "m");

        let mut stmt = conn
            .prepare("SELECT key FROM app_settings ORDER BY key")
            .unwrap();
        let keys: Vec<String> = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(keys, vec!["a_key", "m_key", "z_key"]);
    }
}
