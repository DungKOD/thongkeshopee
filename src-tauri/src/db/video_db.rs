//! Separate SQLite DB cho video download logs.
//! Lưu local-only ở `{app_data}/video_logs.db`.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::Connection;
use tauri::{AppHandle, Manager};

const DB_FILENAME: &str = "video_logs.db";

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS video_downloads (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    url                TEXT NOT NULL UNIQUE,
    downloaded_at_ms   INTEGER NOT NULL,
    status             TEXT NOT NULL CHECK(status IN ('success','failed'))
);

CREATE INDEX IF NOT EXISTS idx_video_downloads_time
    ON video_downloads(downloaded_at_ms DESC);
"#;

/// Tauri managed state cho video DB connection.
pub struct VideoDbState(pub Mutex<Connection>);

/// DB path cho video logs trong workspace folder.
pub fn resolve_video_db_path_in(workspace_root: &Path) -> PathBuf {
    workspace_root.join(DB_FILENAME)
}

/// Mở hoặc tạo video DB tại `path`, apply PRAGMA + schema.
pub fn init_video_db_at(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("không mở được video DB tại {}", path.display()))?;

    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;",
    )
    .context("không apply được PRAGMA cho video DB")?;

    conn.execute_batch(SCHEMA_SQL)
        .context("không apply được schema cho video DB")?;

    Ok(conn)
}

/// Mở video DB đã tồn tại — chỉ apply PRAGMA, KHÔNG re-apply schema. Cho
/// workspace hot-swap (xem doc `crate::db::open_existing_db`).
pub fn open_existing_video_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("không mở được video DB tại {}", path.display()))?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;",
    )
    .context("không apply được PRAGMA cho video DB khi open existing")?;
    Ok(conn)
}

/// Setup hook — init DB trong workspace folder + manage state.
pub fn setup_in(app: &AppHandle, workspace_root: &Path) -> Result<()> {
    let path = resolve_video_db_path_in(workspace_root);
    let conn = init_video_db_at(&path)?;
    app.manage(VideoDbState(Mutex::new(conn)));
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
    fn video_downloads_insert_and_query_desc() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO video_downloads(url, downloaded_at_ms, status)
             VALUES(?1, ?2, ?3)",
            params!["https://tiktok.com/a", 100_i64, "success"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO video_downloads(url, downloaded_at_ms, status)
             VALUES(?1, ?2, ?3)",
            params!["https://tiktok.com/b", 200_i64, "failed"],
        )
        .unwrap();

        let urls: Vec<String> = conn
            .prepare("SELECT url FROM video_downloads ORDER BY downloaded_at_ms DESC")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(urls, vec!["https://tiktok.com/b", "https://tiktok.com/a"]);
    }

    #[test]
    fn video_downloads_upsert_keeps_latest_by_url() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO video_downloads(url, downloaded_at_ms, status)
             VALUES(?1, ?2, ?3)
             ON CONFLICT(url) DO UPDATE SET
                 downloaded_at_ms = excluded.downloaded_at_ms,
                 status = excluded.status",
            params!["https://douyin.com/video/xyz", 100_i64, "failed"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO video_downloads(url, downloaded_at_ms, status)
             VALUES(?1, ?2, ?3)
             ON CONFLICT(url) DO UPDATE SET
                 downloaded_at_ms = excluded.downloaded_at_ms,
                 status = excluded.status",
            params!["https://douyin.com/video/xyz", 200_i64, "success"],
        )
        .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM video_downloads", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);

        let (ts, status): (i64, String) = conn
            .query_row(
                "SELECT downloaded_at_ms, status FROM video_downloads",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(ts, 200);
        assert_eq!(status, "success");
    }

    #[test]
    fn video_downloads_status_check_rejects_invalid() {
        let conn = test_conn();
        let r = conn.execute(
            "INSERT INTO video_downloads(url, downloaded_at_ms, status)
             VALUES('x', 1, 'bogus')",
            [],
        );
        assert!(r.is_err());
    }
}
