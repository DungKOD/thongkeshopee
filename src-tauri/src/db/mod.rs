//! Module quản lý SQLite database cho ThongKeShopee.
//!
//! - `init_db_at(path)`: mở/tạo DB file, apply `schema.sql` (idempotent).
//! - `DbState`: Tauri managed state (`Arc<Mutex<Connection>>`).
//!
//! **No migrations** — app fresh-install only. Schema đổi = user xóa DB tay.
//! Theo kiến trúc ELT: raw tables + manual_entries là source of truth, query
//! on-the-fly cho UI.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use rusqlite::Connection;
use tauri::{AppHandle, Manager};

pub mod content_id;
pub mod types;
pub mod video_db;

pub use video_db::VideoDbState;

const SCHEMA_SQL: &str = include_str!("schema.sql");

const DB_FILENAME: &str = "thongkeshopee.db";

/// Subfolder chứa raw CSV đã import (`imported_files.stored_path`).
pub const IMPORTS_SUBDIR: &str = "imports";

/// Tên account "Mặc định" được seed lần đầu startup. Orphan data (raw Shopee
/// rows không có account cụ thể) được gán vào account này.
pub const DEFAULT_ACCOUNT_NAME: &str = "Mặc định";

/// State quản lý connection, wrap `Mutex` để share giữa các Tauri command.
pub struct DbState(pub Mutex<Connection>);

/// Format `Utc::now()` theo RFC3339 với suffix `Z` + millisecond precision.
/// Convention chuẩn cho mọi `created_at` / `updated_at` trong DB.
pub fn now_rfc3339_z() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Resolve app_data_dir root (base cho mọi path khác).
fn app_data_root(app: &AppHandle) -> Result<PathBuf> {
    let base = app
        .path()
        .app_data_dir()
        .context("không lấy được app_data_dir")?;
    fs::create_dir_all(&base)
        .with_context(|| format!("không tạo được thư mục app_data_dir: {}", base.display()))?;
    Ok(base)
}

/// DB path single-file ở root app_data dir.
pub fn resolve_db_path(app: &AppHandle) -> Result<PathBuf> {
    Ok(app_data_root(app)?.join(DB_FILENAME))
}

/// Active DB path — query `PRAGMA database_list` từ connection đang mở.
pub fn resolve_active_db_path(conn: &Connection) -> Result<PathBuf> {
    let path_str: String = conn
        .query_row("PRAGMA database_list", [], |r| r.get::<_, String>(2))
        .context("không đọc được DB path từ PRAGMA database_list")?;
    if path_str.is_empty() {
        anyhow::bail!("DB đang ở :memory: — không có physical path");
    }
    Ok(PathBuf::from(path_str))
}

/// Active imports folder — parent của DB path + "imports/".
pub fn resolve_active_imports_dir(conn: &Connection) -> Result<PathBuf> {
    let db_path = resolve_active_db_path(conn)?;
    let parent = db_path.parent().context("DB path không có parent")?;
    let dir = parent.join(IMPORTS_SUBDIR);
    fs::create_dir_all(&dir)
        .with_context(|| format!("không tạo được thư mục imports: {}", dir.display()))?;
    Ok(dir)
}

/// Mở hoặc tạo DB tại `path`, apply PRAGMA + `schema.sql`, seed "Mặc định".
/// Idempotent — schema dùng `IF NOT EXISTS`, seed dùng `INSERT OR IGNORE`.
pub fn init_db_at(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("không mở được DB tại {}", path.display()))?;

    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;
         PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = -32768;
         PRAGMA mmap_size = 268435456;",
    )
    .context("không apply được PRAGMA")?;

    conn.execute_batch(SCHEMA_SQL)
        .context("không apply được schema")?;

    seed_default_account(&conn).context("seed default account thất bại")?;

    // Cập nhật statistics cho query planner — giúp chọn đúng index khi data lớn.
    // Chạy sau schema/seed để stats có dữ liệu thực.
    let _ = conn.execute_batch("ANALYZE");


    Ok(conn)
}

/// Seed account "Mặc định" với id = content_id(name). Idempotent.
fn seed_default_account(conn: &Connection) -> Result<()> {
    let id = content_id::shopee_account_id(DEFAULT_ACCOUNT_NAME);
    let now = now_rfc3339_z();
    conn.execute(
        "INSERT OR IGNORE INTO shopee_accounts (id, name, color, created_at)
         VALUES (?, ?, ?, ?)",
        rusqlite::params![id, DEFAULT_ACCOUNT_NAME, "#888888", now],
    )?;
    Ok(())
}

/// Setup hook cho `tauri::Builder`: init main DB ở root + video DB + state.
pub fn setup(app: &AppHandle) -> Result<()> {
    let path = resolve_db_path(app)?;
    let conn = init_db_at(&path)?;
    app.manage(DbState(Mutex::new(conn)));
    video_db::setup(app)?;
    Ok(())
}

/// Test helper: apply schema + seed trên connection in-memory có sẵn.
#[cfg(test)]
pub fn migrate_for_tests(conn: &Connection) -> Result<()> {
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    conn.execute_batch(SCHEMA_SQL)?;
    seed_default_account(conn)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        migrate_for_tests(&conn).unwrap();
        conn
    }

    #[test]
    fn schema_creates_all_tables() {
        let conn = test_conn();
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        let expected = [
            "app_settings",
            "clicks_to_file",
            "days",
            "fb_ads_to_file",
            "imported_files",
            "manual_entries",
            "orders_to_file",
            "raw_fb_ads",
            "raw_shopee_clicks",
            "raw_shopee_order_items",
            "shopee_accounts",
        ];
        for name in expected {
            assert!(tables.iter().any(|t| t == name), "missing table {name}");
        }
    }

    #[test]
    fn default_account_seeded_with_content_id() {
        let conn = test_conn();
        let id: i64 = conn
            .query_row(
                "SELECT id FROM shopee_accounts WHERE name = ?",
                [DEFAULT_ACCOUNT_NAME],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(id, content_id::shopee_account_id(DEFAULT_ACCOUNT_NAME));
    }

    #[test]
    fn schema_idempotent() {
        let conn = test_conn();
        conn.execute_batch(SCHEMA_SQL).unwrap();
        seed_default_account(&conn).unwrap();

        let default_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM shopee_accounts WHERE name = ?",
                [DEFAULT_ACCOUNT_NAME],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(default_count, 1, "Mặc định chỉ được seed 1 lần");
    }

    #[test]
    fn cascade_delete_day_removes_raw_rows() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-17', '2026-04-17T00:00:00Z')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
             VALUES(?, 'f.csv', 'shopee_clicks', '2026-04-17T00:00:00Z', 'abc', '2026-04-17')",
            [content_id::imported_file_id("abc")],
        )
        .unwrap();
        let file_id = content_id::imported_file_id("abc");
        conn.execute(
            "INSERT INTO raw_shopee_clicks(click_id, click_time, day_date, source_file_id)
             VALUES('c1', '2026-04-17T10:00:00Z', '2026-04-17', ?)",
            [file_id],
        )
        .unwrap();

        conn.execute("DELETE FROM days WHERE date='2026-04-17'", [])
            .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM raw_shopee_clicks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "CASCADE phải xóa raw_shopee_clicks");

        let file_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM imported_files", [], |r| r.get(0))
            .unwrap();
        assert_eq!(file_count, 1, "imported_files KHÔNG cascade từ day");
    }

    #[test]
    fn manual_entry_unique_by_subid_and_date() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-17', '2026-04-17T00:00:00Z')",
            [],
        )
        .unwrap();
        let insert = |sub2: &str| {
            conn.execute(
                "INSERT INTO manual_entries
                 (sub_id1, sub_id2, day_date, created_at, updated_at)
                 VALUES('shop', ?, '2026-04-17', 'now', 'now')",
                [sub2],
            )
        };
        insert("a").unwrap();
        assert!(insert("a").is_err(), "dup (sub_ids, date) phải fail");
        insert("b").unwrap();
    }

    #[test]
    fn partial_unique_hash_allows_reimport_after_revert() {
        let conn = test_conn();

        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
             VALUES(?, 'A.csv', 'shopee_clicks', 'now', 'hash-X', '2026-04-17')",
            [content_id::imported_file_id("hash-X")],
        )
        .unwrap();

        let dup = conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
             VALUES(?, 'A-dup.csv', 'shopee_clicks', 'now', 'hash-X', '2026-04-17')",
            [content_id::imported_file_id("hash-X") + 1],
        );
        assert!(dup.is_err(), "dedup phải chặn 2 file cùng hash cùng active");

        conn.execute(
            "UPDATE imported_files SET reverted_at='now' WHERE file_hash='hash-X'",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
             VALUES(?, 'A-reimport.csv', 'shopee_clicks', 'now', 'hash-X', '2026-04-17')",
            [content_id::imported_file_id("hash-X") + 1],
        )
        .unwrap();

        let active_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM imported_files
                 WHERE file_hash='hash-X' AND reverted_at IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(active_count, 1, "chỉ 1 row active sau re-import");
    }

    #[test]
    fn day_delete_cascades_cleanup_click_mapping() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-17', 'now')",
            [],
        )
        .unwrap();
        let file_id = content_id::imported_file_id("hA");
        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
             VALUES(?, 'A.csv', 'shopee_clicks', 'now', 'hA', '2026-04-17')",
            [file_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO raw_shopee_clicks(click_id, click_time, day_date, source_file_id)
             VALUES('c1', 'now', '2026-04-17', ?)",
            [file_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c1', ?)",
            [file_id],
        )
        .unwrap();

        conn.execute("DELETE FROM days WHERE date='2026-04-17'", [])
            .unwrap();

        let raw_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM raw_shopee_clicks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(raw_count, 0, "raw clicks phải bị CASCADE");

        let map_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM clicks_to_file", [], |r| r.get(0))
            .unwrap();
        assert_eq!(map_count, 0, "trigger phải cleanup mapping khi CASCADE");
    }

    #[test]
    fn mapping_cascades_on_imported_files_delete() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-17', 'now')",
            [],
        )
        .unwrap();
        let file_id = content_id::imported_file_id("h");
        conn.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
             VALUES(?, 'x.csv', 'shopee_clicks', 'now', 'h', '2026-04-17')",
            [file_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c1', ?)",
            [file_id],
        )
        .unwrap();

        conn.execute("DELETE FROM imported_files WHERE id = ?", [file_id])
            .unwrap();
        let map_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM clicks_to_file", [], |r| r.get(0))
            .unwrap();
        assert_eq!(map_count, 0, "mapping phải CASCADE theo imported_files");
    }
}
