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
    // Legacy UNIQUE cũ (pre-v2): `(source_file_id, name)`. Nếu còn phát hiện,
    // data FB cũ bỏ luôn — về trước v2 không còn user thực nào giữ data.
    let needs_fb_drop = has_legacy_fb_unique(conn, "raw_fb_ad_groups", "source_file_id, ad_group_name")?
        || has_legacy_fb_unique(conn, "raw_fb_campaigns", "source_file_id, campaign_name")?;

    if needs_fb_drop {
        conn.execute_batch(
            "DROP TABLE IF EXISTS raw_fb_ad_groups;
             DROP TABLE IF EXISTS raw_fb_campaigns;
             DELETE FROM imported_files WHERE kind IN ('fb_ad_group', 'fb_campaign');",
        )?;
    }

    // v3: gộp 2 bảng FB → 1 bảng `raw_fb_ads`, normalize clicks + cpc lúc INSERT.
    // Nếu còn bảng cũ → copy data sang, drop cũ.
    migrate_fb_unify(conn).context("fb unify migration failed")?;
    migrate_sync_state(conn).context("sync_state migration failed")?;

    Ok(())
}

/// v3 migration: copy raw_fb_ad_groups + raw_fb_campaigns → raw_fb_ads, drop cũ.
/// Normalize clicks/cpc lúc copy.
fn migrate_fb_unify(conn: &Connection) -> Result<()> {
    let has_ad_groups = table_exists(conn, "raw_fb_ad_groups")?;
    let has_campaigns = table_exists(conn, "raw_fb_campaigns")?;
    if !has_ad_groups && !has_campaigns {
        return Ok(());
    }

    if has_ad_groups {
        // Check cột tồn tại — bảng cũ có thể thiếu link_clicks/all_clicks etc.
        let cols = table_columns(conn, "raw_fb_ad_groups")?;
        let has = |c: &str| cols.iter().any(|x| x == c);

        // Tính biểu thức clicks + cpc theo cột sẵn có (fallback 0/NULL nếu thiếu).
        let clicks_expr = build_coalesce(&[
            has("link_clicks").then_some("link_clicks"),
            has("all_clicks").then_some("all_clicks"),
        ]);
        let cpc_expr = build_coalesce(&[
            has("link_cpc").then_some("link_cpc"),
            has("all_cpc").then_some("all_cpc"),
        ]);

        let sql = format!(
            "INSERT OR IGNORE INTO raw_fb_ads
             (level, name, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
              report_start, report_end, status,
              spend, clicks, cpc, impressions, reach,
              raw_json, day_date, source_file_id)
             SELECT 'ad_group', ad_group_name,
                    sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                    report_start, report_end, status,
                    spend, {clicks_expr}, {cpc_expr}, impressions, reach,
                    raw_json, day_date, source_file_id
             FROM raw_fb_ad_groups"
        );
        conn.execute(&sql, [])?;
        conn.execute("DROP TABLE raw_fb_ad_groups", [])?;
    }

    if has_campaigns {
        let cols = table_columns(conn, "raw_fb_campaigns")?;
        let has = |c: &str| cols.iter().any(|x| x == c);

        let clicks_expr = build_coalesce(&[
            has("link_clicks").then_some("link_clicks"),
            has("all_clicks").then_some("all_clicks"),
            has("result_count").then_some("result_count"),
        ]);
        let cpc_expr = build_coalesce(&[
            has("link_cpc").then_some("link_cpc"),
            has("all_cpc").then_some("all_cpc"),
        ]);

        let sql = format!(
            "INSERT OR IGNORE INTO raw_fb_ads
             (level, name, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
              report_start, report_end, status,
              spend, clicks, cpc, impressions, reach,
              raw_json, day_date, source_file_id)
             SELECT 'campaign', campaign_name,
                    sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                    report_start, report_end, status,
                    spend, {clicks_expr}, {cpc_expr}, impressions, reach,
                    raw_json, day_date, source_file_id
             FROM raw_fb_campaigns"
        );
        conn.execute(&sql, [])?;
        conn.execute("DROP TABLE raw_fb_campaigns", [])?;
    }

    conn.execute(
        "INSERT OR IGNORE INTO _schema_version(version, applied_at)
         VALUES(3, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        [],
    )?;
    Ok(())
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?",
        [name],
        |r| r.get(0),
    )?;
    Ok(count > 0)
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(1))?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

/// Build SQL `COALESCE(a, b, c)` từ list Option<&str> — bỏ None. Nếu hết → "NULL".
fn build_coalesce(opts: &[Option<&str>]) -> String {
    let parts: Vec<&str> = opts.iter().filter_map(|x| *x).collect();
    match parts.len() {
        0 => "NULL".to_string(),
        1 => parts[0].to_string(),
        _ => format!("COALESCE({})", parts.join(", ")),
    }
}

/// Đảm bảo sync_state có đủ cột mới + triggers luôn up-to-date.
/// Idempotent: ALTER TABLE ADD COLUMN chỉ chạy nếu cột chưa có;
/// DROP + CREATE triggers chạy mỗi lần (rẻ, đảm bảo body match code).
fn migrate_sync_state(conn: &Connection) -> Result<()> {
    // Check cột tồn tại chưa (sync_state cũ chỉ có 5 cột).
    let cols: Vec<String> = {
        let mut stmt = conn.prepare("PRAGMA table_info(sync_state)")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(1))?;
        rows.collect::<std::result::Result<_, _>>()?
    };

    if !cols.iter().any(|c| c == "change_id") {
        conn.execute(
            "ALTER TABLE sync_state ADD COLUMN change_id INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    if !cols.iter().any(|c| c == "last_uploaded_change_id") {
        conn.execute(
            "ALTER TABLE sync_state ADD COLUMN last_uploaded_change_id INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }

    // Drop + re-create sync triggers để đảm bảo body match code hiện tại.
    // Triggers increment cả dirty lẫn change_id (cho CAS pattern).
    let trigger_specs: &[(&str, &str, &str)] = &[
        ("trg_sync_clicks_ins",   "INSERT", "raw_shopee_clicks"),
        ("trg_sync_clicks_upd",   "UPDATE", "raw_shopee_clicks"),
        ("trg_sync_clicks_del",   "DELETE", "raw_shopee_clicks"),
        ("trg_sync_orders_ins",   "INSERT", "raw_shopee_order_items"),
        ("trg_sync_orders_upd",   "UPDATE", "raw_shopee_order_items"),
        ("trg_sync_orders_del",   "DELETE", "raw_shopee_order_items"),
        ("trg_sync_fb_ads_ins",   "INSERT", "raw_fb_ads"),
        ("trg_sync_fb_ads_upd",   "UPDATE", "raw_fb_ads"),
        ("trg_sync_fb_ads_del",   "DELETE", "raw_fb_ads"),
        ("trg_sync_manual_ins",   "INSERT", "manual_entries"),
        ("trg_sync_manual_upd",   "UPDATE", "manual_entries"),
        ("trg_sync_manual_del",   "DELETE", "manual_entries"),
        ("trg_sync_video_ins",    "INSERT", "video_downloads"),
        ("trg_sync_video_upd",    "UPDATE", "video_downloads"),
        ("trg_sync_video_del",    "DELETE", "video_downloads"),
    ];

    // Drop legacy triggers trên bảng FB cũ (đã gộp vào raw_fb_ads ở v3).
    for name in [
        "trg_sync_fb_ad_ins",
        "trg_sync_fb_ad_upd",
        "trg_sync_fb_ad_del",
        "trg_sync_fb_camp_ins",
        "trg_sync_fb_camp_upd",
        "trg_sync_fb_camp_del",
    ] {
        conn.execute(&format!("DROP TRIGGER IF EXISTS {name}"), [])?;
    }

    for (name, event, table) in trigger_specs {
        conn.execute(&format!("DROP TRIGGER IF EXISTS {name}"), [])?;
        conn.execute(
            &format!(
                "CREATE TRIGGER {name} AFTER {event} ON {table}
                 BEGIN UPDATE sync_state SET dirty = 1, change_id = change_id + 1 WHERE id = 1; END"
            ),
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
            "raw_fb_ads",
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
