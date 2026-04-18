//! Module quản lý SQLite database cho ThongKeShopee.
//!
//! - `init_db(app_handle)`: mở/tạo DB file trong app_data_dir, apply schema.
//! - `DbState`: Tauri managed state (`Arc<Mutex<Connection>>`).
//!
//! Schema chi tiết xem `schema.sql`. Theo kiến trúc ELT:
//! raw tables + manual_entries là source of truth, query on-the-fly cho UI.

use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::Connection;
use tauri::{AppHandle, Manager};

pub mod types;

/// SQL script áp dụng khi khởi động app (idempotent, safe để chạy mỗi lần).
const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Tên file DB trong app_data_dir.
const DB_FILENAME: &str = "thongkeshopee.db";

/// Subfolder chứa raw CSV đã import (dùng cho `imported_files.stored_path`).
pub const IMPORTS_SUBDIR: &str = "imports";

/// State quản lý connection, wrap `Mutex` để share giữa các Tauri command.
pub struct DbState(pub Mutex<Connection>);

/// Resolve đường dẫn file DB dựa trên `app_data_dir()` của platform.
/// Tạo folder cha nếu chưa có.
pub fn resolve_db_path(app: &AppHandle) -> Result<PathBuf> {
    let base = app
        .path()
        .app_data_dir()
        .context("không lấy được app_data_dir")?;
    fs::create_dir_all(&base)
        .with_context(|| format!("không tạo được thư mục app_data_dir: {}", base.display()))?;
    Ok(base.join(DB_FILENAME))
}

/// Resolve folder lưu raw CSV gốc (app_data_dir/imports/).
pub fn resolve_imports_dir(app: &AppHandle) -> Result<PathBuf> {
    let base = app
        .path()
        .app_data_dir()
        .context("không lấy được app_data_dir")?;
    let dir = base.join(IMPORTS_SUBDIR);
    fs::create_dir_all(&dir)
        .with_context(|| format!("không tạo được thư mục imports: {}", dir.display()))?;
    Ok(dir)
}

/// Mở hoặc tạo DB, apply schema + PRAGMA cần thiết. Gọi 1 lần khi app start.
pub fn init_db(app: &AppHandle) -> Result<Connection> {
    let path = resolve_db_path(app)?;
    let conn = Connection::open(&path)
        .with_context(|| format!("không mở được DB tại {}", path.display()))?;

    // PRAGMAs: WAL cho concurrent read, foreign_keys bật để CASCADE hoạt động.
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;
         PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;",
    )
    .context("không apply được PRAGMA")?;

    // Apply schema (idempotent — có `IF NOT EXISTS` nên không override bảng đã có).
    conn.execute_batch(SCHEMA_SQL)
        .context("không apply được schema")?;

    // Migrate dựa trên DDL hiện tại (không dùng version counter để tránh
    // rủi ro version tăng nhưng table chưa update — case "IF NOT EXISTS" skip).
    migrate(&conn).context("migration thất bại")?;

    Ok(conn)
}

/// Check DDL hiện tại của các bảng, fix nếu phát hiện schema cũ.
/// Idempotent — chạy mỗi lần startup an toàn.
fn migrate(conn: &Connection) -> Result<()> {
    // Check FB ad_groups UNIQUE: cũ là `(source_file_id, ad_group_name)`,
    // mới là `(day_date, ad_group_name)`. Pattern match DDL để detect.
    let needs_fb_rebuild = has_legacy_fb_unique(conn, "raw_fb_ad_groups", "source_file_id, ad_group_name")?
        || has_legacy_fb_unique(conn, "raw_fb_campaigns", "source_file_id, campaign_name")?;

    if needs_fb_rebuild {
        // Data FB cũ sẽ mất (orphan imported_files rows cũng dọn luôn).
        conn.execute_batch(
            "DROP TABLE IF EXISTS raw_fb_ad_groups;
             DROP TABLE IF EXISTS raw_fb_campaigns;
             DELETE FROM imported_files WHERE kind IN ('fb_ad_group', 'fb_campaign');",
        )?;
        // Re-apply schema → recreate 2 bảng đã drop với UNIQUE mới.
        conn.execute_batch(SCHEMA_SQL)?;

        conn.execute(
            "INSERT OR IGNORE INTO _schema_version(version, applied_at)
             VALUES(2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            [],
        )?;
    }

    Ok(())
}

fn has_legacy_fb_unique(conn: &Connection, table: &str, legacy: &str) -> Result<bool> {
    let sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?",
            [table],
            |r| r.get(0),
        )
        .ok();
    Ok(sql.as_deref().map(|s| s.contains(legacy)).unwrap_or(false))
}

/// Setup hook cho `tauri::Builder`: init DB + manage state.
pub fn setup(app: &AppHandle) -> Result<()> {
    let conn = init_db(app)?;
    app.manage(DbState(Mutex::new(conn)));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
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
            "_schema_version",
            "days",
            "imported_files",
            "manual_entries",
            "raw_fb_ad_groups",
            "raw_fb_campaigns",
            "raw_shopee_clicks",
            "raw_shopee_order_items",
        ];
        for name in expected {
            assert!(tables.iter().any(|t| t == name), "missing table {name}");
        }
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
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('f.csv', 'shopee_clicks', '2026-04-17T00:00:00Z', 'abc', '2026-04-17')",
            [],
        )
        .unwrap();
        let file_id = conn.last_insert_rowid();
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
        assert_eq!(file_count, 0, "CASCADE phải xóa imported_files");
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
}
