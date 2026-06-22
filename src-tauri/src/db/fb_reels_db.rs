//! Separate SQLite DB cho Facebook Reels: pages + post history.
//! Lưu local-only ở `{app_data}/fb_reels.db`.
//!
//! Token Page lưu plain text — file này được exclude khỏi export/import DB
//! để tránh leak khi user share backup.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::Connection;
use tauri::{AppHandle, Manager};

pub const DB_FILENAME: &str = "fb_reels.db";

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS fb_pages (
    page_id      TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    access_token TEXT NOT NULL,
    added_at_ms  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fb_reel_posts (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    page_id           TEXT NOT NULL,
    page_name         TEXT NOT NULL,
    file_path         TEXT NOT NULL,
    file_size         INTEGER NOT NULL,
    caption           TEXT,
    scheduled_time_ms INTEGER,
    status            TEXT NOT NULL CHECK(status IN
                          ('pending','uploading','publishing',
                           'scheduled','published','failed')),
    progress          INTEGER NOT NULL DEFAULT 0
                          CHECK(progress BETWEEN 0 AND 100),
    fb_video_id       TEXT,
    fb_permalink      TEXT,
    error_message     TEXT,
    created_at_ms     INTEGER NOT NULL,
    published_at_ms   INTEGER
);

CREATE INDEX IF NOT EXISTS idx_fb_posts_status_created
    ON fb_reel_posts(status, created_at_ms DESC);

CREATE INDEX IF NOT EXISTS idx_fb_posts_page_created
    ON fb_reel_posts(page_id, created_at_ms DESC);
"#;

/// Tauri managed state cho FB Reels DB connection.
pub struct FbReelsDbState(pub Mutex<Connection>);

fn app_data_root(app: &AppHandle) -> Result<PathBuf> {
    let base = app
        .path()
        .app_data_dir()
        .context("không lấy được app_data_dir")?;
    fs::create_dir_all(&base).with_context(|| {
        format!("không tạo được thư mục app_data_dir: {}", base.display())
    })?;
    Ok(base)
}

/// DB path cho FB Reels ở root app_data.
pub fn resolve_fb_reels_db_path(app: &AppHandle) -> Result<PathBuf> {
    Ok(app_data_root(app)?.join(DB_FILENAME))
}

/// Mở hoặc tạo FB Reels DB tại `path`, apply PRAGMA + schema.
pub fn init_fb_reels_db_at(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("không mở được FB Reels DB tại {}", path.display()))?;

    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;",
    )
    .context("không apply được PRAGMA cho FB Reels DB")?;

    conn.execute_batch(SCHEMA_SQL)
        .context("không apply được schema cho FB Reels DB")?;

    Ok(conn)
}

/// Setup hook — init DB + manage state.
pub fn setup(app: &AppHandle) -> Result<()> {
    let path = resolve_fb_reels_db_path(app)?;
    let conn = init_fb_reels_db_at(&path)?;
    app.manage(FbReelsDbState(Mutex::new(conn)));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
        conn
    }

    #[test]
    fn schema_creates_tables() {
        let conn = test_conn();
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(tables.iter().any(|t| t == "fb_pages"));
        assert!(tables.iter().any(|t| t == "fb_reel_posts"));
    }

    #[test]
    fn page_upsert_replaces_token() {
        let conn = test_conn();
        let insert = |token: &str| {
            conn.execute(
                "INSERT INTO fb_pages(page_id, name, access_token, added_at_ms)
                 VALUES(?1, ?2, ?3, ?4)
                 ON CONFLICT(page_id) DO UPDATE SET
                     name = excluded.name,
                     access_token = excluded.access_token,
                     added_at_ms = excluded.added_at_ms",
                params!["1234", "My Page", token, 100_i64],
            )
        };
        insert("tok1").unwrap();
        insert("tok2").unwrap();

        let (count, token): (i64, String) = conn
            .query_row(
                "SELECT COUNT(*), MAX(access_token) FROM fb_pages",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(token, "tok2");
    }

    #[test]
    fn post_status_check_rejects_invalid() {
        let conn = test_conn();
        let r = conn.execute(
            "INSERT INTO fb_reel_posts
             (page_id, page_name, file_path, file_size, status, created_at_ms)
             VALUES('p', 'P', '/x', 1, 'bogus', 1)",
            [],
        );
        assert!(r.is_err());
    }

    #[test]
    fn post_progress_check_rejects_out_of_range() {
        let conn = test_conn();
        let r = conn.execute(
            "INSERT INTO fb_reel_posts
             (page_id, page_name, file_path, file_size, status, progress, created_at_ms)
             VALUES('p', 'P', '/x', 1, 'pending', 150, 1)",
            [],
        );
        assert!(r.is_err());
    }

    #[test]
    fn list_posts_order_desc_by_created() {
        let conn = test_conn();
        for (i, ts) in [("a", 100), ("b", 300), ("c", 200)] {
            conn.execute(
                "INSERT INTO fb_reel_posts
                 (page_id, page_name, file_path, file_size, status, created_at_ms)
                 VALUES('p', 'P', ?1, 1, 'pending', ?2)",
                params![i, ts as i64],
            )
            .unwrap();
        }
        let paths: Vec<String> = conn
            .prepare(
                "SELECT file_path FROM fb_reel_posts
                 ORDER BY created_at_ms DESC",
            )
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(paths, vec!["b", "c", "a"]);
    }
}
