//! Separate SQLite DB cho video download logs.
//!
//! Video logs KHÔNG nằm trong main DB (`thongkeshopee.db`) để:
//! 1. Main DB sync lên Drive gọn nhẹ — chỉ chứa data thống kê affiliate.
//! 2. Primary audit store cho video logs là Google Sheet (qua Apps Script).
//!    DB này chỉ là local fallback để user xem lại history của chính mình.
//!
//! v8+ multi-tenant: DB nằm trong folder per-user `{app_data}/users/{uid}/video_logs.db`.
//! Setup() mở pre-auth placeholder ở `_pre_auth/`; `switch_db_to_user` swap sang
//! folder user thật sau khi auth ready (migrate legacy root DB nếu có).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::Connection;
use tauri::{AppHandle, Manager};

use super::{PRE_AUTH_SUBDIR, USERS_SUBDIR};

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

-- Admin cache: bản sao log của user khác lấy từ Google Sheet.
-- Flow: admin click user → read cache; empty → auto fetch Sheet → replace cache → UI render từ cache.
-- `row_order` giữ thứ tự từ Sheet (AS trả DESC — 0 = mới nhất).
CREATE TABLE IF NOT EXISTS admin_user_log_cache (
    local_part TEXT NOT NULL,
    row_order  INTEGER NOT NULL,
    timestamp  TEXT NOT NULL,
    url        TEXT NOT NULL,
    status     TEXT NOT NULL,
    PRIMARY KEY (local_part, row_order)
);

CREATE INDEX IF NOT EXISTS idx_admin_user_log_cache_lp
    ON admin_user_log_cache(local_part);

-- Metadata per user: lần fetch gần nhất + tổng row count để UI hiển thị.
CREATE TABLE IF NOT EXISTS admin_user_log_fetch_meta (
    local_part     TEXT PRIMARY KEY,
    fetched_at_ms  INTEGER NOT NULL,
    row_count      INTEGER NOT NULL
);

-- User list cache (singleton) — admin xem danh sách user. Stale-while-revalidate:
-- UI render từ cache ngay; fetch Firestore qua AS background → update cache +
-- re-render. Lưu JSON blob để FE tự parse (không cần normalize schema).
CREATE TABLE IF NOT EXISTS admin_user_list_cache (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    data_json      TEXT NOT NULL,
    fetched_at_ms  INTEGER NOT NULL
);
"#;

/// Tauri managed state cho video DB connection.
pub struct VideoDbState(pub Mutex<Connection>);

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

/// Resolve video DB path cho 1 user: `{app_data}/users/{uid}/video_logs.db`.
/// Caller đã verify UID qua `resolve_user_dir` (alphanumeric + - + _).
pub fn resolve_video_db_path_for_user(app: &AppHandle, uid: &str) -> Result<PathBuf> {
    let safe = uid
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !safe || uid.is_empty() {
        anyhow::bail!("UID không hợp lệ: {uid}");
    }
    let dir = app_data_root(app)?.join(USERS_SUBDIR).join(uid);
    fs::create_dir_all(&dir)
        .with_context(|| format!("không tạo được thư mục user: {}", dir.display()))?;
    Ok(dir.join(DB_FILENAME))
}

/// Placeholder pre-auth video DB path — dùng khi app start chưa có user UID.
/// Swap sau khi `switch_db_to_user`.
pub fn resolve_pre_auth_video_db_path(app: &AppHandle) -> Result<PathBuf> {
    let dir = app_data_root(app)?.join(PRE_AUTH_SUBDIR);
    fs::create_dir_all(&dir)
        .with_context(|| format!("không tạo được thư mục pre_auth: {}", dir.display()))?;
    Ok(dir.join(DB_FILENAME))
}

/// Legacy video DB path ở root `{app_data}/video_logs.db` — dùng cho migration
/// lần đầu sau upgrade lên v8+. KHÔNG dùng ngoài migration block.
pub fn resolve_legacy_video_db_path(app: &AppHandle) -> Result<PathBuf> {
    Ok(app_data_root(app)?.join(DB_FILENAME))
}

/// Mở hoặc tạo video DB tại `path`, apply PRAGMA + schema + migrations.
/// Idempotent — chạy mỗi lần switch user.
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

    migrate_video_downloads_unique_url(&conn)
        .context("migrate video_downloads UNIQUE(url) thất bại")?;

    Ok(conn)
}

/// v9 migration: add UNIQUE(url) cho `video_downloads` để enable UPSERT theo
/// URL (nhất quán với AS Sheet upsert). DB cũ không có constraint này → có
/// thể có duplicate cùng URL → dedupe keep-latest theo downloaded_at_ms.
fn migrate_video_downloads_unique_url(conn: &Connection) -> Result<()> {
    // Check existing table DDL để xem đã có UNIQUE(url) chưa.
    let existing_sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='video_downloads'",
            [],
            |r| r.get(0),
        )
        .ok();
    let has_unique = match existing_sql.as_deref() {
        Some(s) => s.contains("UNIQUE"),
        None => return Ok(()), // bảng chưa tồn tại (DB mới hoàn toàn)
    };
    if has_unique {
        return Ok(());
    }

    // Rebuild table với UNIQUE. Dedupe: giữ row latest per URL qua correlated
    // subquery chọn `id` có `downloaded_at_ms` lớn nhất (tiebreak id DESC).
    conn.execute_batch(
        "CREATE TABLE video_downloads_new (
             id                 INTEGER PRIMARY KEY AUTOINCREMENT,
             url                TEXT NOT NULL UNIQUE,
             downloaded_at_ms   INTEGER NOT NULL,
             status             TEXT NOT NULL CHECK(status IN ('success','failed'))
         );
         INSERT INTO video_downloads_new (url, downloaded_at_ms, status)
         SELECT vd.url, vd.downloaded_at_ms, vd.status
         FROM video_downloads vd
         WHERE vd.id = (
             SELECT id FROM video_downloads
             WHERE url = vd.url
             ORDER BY downloaded_at_ms DESC, id DESC
             LIMIT 1
         );
         DROP TABLE video_downloads;
         ALTER TABLE video_downloads_new RENAME TO video_downloads;
         CREATE INDEX IF NOT EXISTS idx_video_downloads_time
             ON video_downloads(downloaded_at_ms DESC);",
    )?;

    Ok(())
}

/// Setup hook — init pre-auth DB + manage state. Swap sang user DB qua
/// `switch_db_to_user` sau khi FE auth ready.
pub fn setup(app: &AppHandle) -> Result<()> {
    let path = resolve_pre_auth_video_db_path(app)?;
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
            .prepare(
                "SELECT url FROM video_downloads ORDER BY downloaded_at_ms DESC",
            )
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
        // UPSERT cùng URL 2 lần → chỉ 1 row, giữ timestamp + status mới nhất.
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
        assert_eq!(count, 1, "UPSERT phải giữ 1 row per URL");

        let (ts, status): (i64, String) = conn
            .query_row(
                "SELECT downloaded_at_ms, status FROM video_downloads",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(ts, 200, "giữ timestamp mới nhất");
        assert_eq!(status, "success", "giữ status mới nhất");
    }

    #[test]
    fn migrate_video_downloads_dedupe_and_add_unique() {
        // Tạo DB cũ KHÔNG có UNIQUE(url) — simulate legacy DB trước migration.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE video_downloads (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                url                TEXT NOT NULL,
                downloaded_at_ms   INTEGER NOT NULL,
                status             TEXT NOT NULL CHECK(status IN ('success','failed'))
            );",
        )
        .unwrap();

        // Insert 3 rows: 2 cùng URL (cần dedupe), 1 URL khác.
        conn.execute(
            "INSERT INTO video_downloads(url, downloaded_at_ms, status)
             VALUES(?1, ?2, ?3)",
            params!["https://a.com/1", 100_i64, "success"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO video_downloads(url, downloaded_at_ms, status)
             VALUES(?1, ?2, ?3)",
            params!["https://a.com/1", 300_i64, "failed"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO video_downloads(url, downloaded_at_ms, status)
             VALUES(?1, ?2, ?3)",
            params!["https://b.com/2", 200_i64, "success"],
        )
        .unwrap();

        migrate_video_downloads_unique_url(&conn).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM video_downloads", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2, "dedupe phải merge 2 rows cùng URL thành 1");

        let (ts, status): (i64, String) = conn
            .query_row(
                "SELECT downloaded_at_ms, status FROM video_downloads WHERE url = ?1",
                ["https://a.com/1"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(ts, 300, "giữ timestamp latest");
        assert_eq!(status, "failed", "giữ status latest");

        // Verify UNIQUE constraint đã enable — insert duplicate URL fail.
        let r = conn.execute(
            "INSERT INTO video_downloads(url, downloaded_at_ms, status)
             VALUES(?1, ?2, ?3)",
            params!["https://a.com/1", 400_i64, "success"],
        );
        assert!(r.is_err(), "UNIQUE constraint phải reject duplicate URL");

        // Migration idempotent — chạy lần 2 không crash.
        migrate_video_downloads_unique_url(&conn).unwrap();
        let count2: i64 = conn
            .query_row("SELECT COUNT(*) FROM video_downloads", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count2, 2, "migration idempotent");
    }

    #[test]
    fn video_downloads_status_check_rejects_invalid() {
        let conn = test_conn();
        let r = conn.execute(
            "INSERT INTO video_downloads(url, downloaded_at_ms, status)
             VALUES('x', 1, 'bogus')",
            [],
        );
        assert!(r.is_err(), "CHECK constraint phải reject status lạ");
    }

    #[test]
    fn admin_cache_replace_atomically() {
        let conn = test_conn();

        // Lần 1: insert 3 rows cho user "vnz.luffy".
        for (i, ts, url) in [
            (0, "14:01:00 20/04/2026", "u1"),
            (1, "14:00:00 20/04/2026", "u2"),
            (2, "13:59:00 20/04/2026", "u3"),
        ] {
            conn.execute(
                "INSERT INTO admin_user_log_cache
                 (local_part, row_order, timestamp, url, status)
                 VALUES(?1, ?2, ?3, ?4, 'thành công')",
                params!["vnz.luffy", i as i64, ts, url],
            )
            .unwrap();
        }

        // Simulate fetch lại: DELETE + INSERT mới trong transaction.
        let new_rows = vec![
            ("15:00:00 20/04/2026", "new1"),
            ("14:59:00 20/04/2026", "new2"),
        ];
        conn.execute(
            "DELETE FROM admin_user_log_cache WHERE local_part = ?",
            ["vnz.luffy"],
        )
        .unwrap();
        for (i, (ts, url)) in new_rows.iter().enumerate() {
            conn.execute(
                "INSERT INTO admin_user_log_cache
                 (local_part, row_order, timestamp, url, status)
                 VALUES(?1, ?2, ?3, ?4, 'thành công')",
                params!["vnz.luffy", i as i64, ts, url],
            )
            .unwrap();
        }

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM admin_user_log_cache WHERE local_part = ?",
                ["vnz.luffy"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2, "replace phải xóa 3 rows cũ và có 2 rows mới");
    }

    #[test]
    fn admin_cache_pagination_by_row_order() {
        let conn = test_conn();
        for i in 0..5 {
            conn.execute(
                "INSERT INTO admin_user_log_cache
                 (local_part, row_order, timestamp, url, status)
                 VALUES('a', ?1, ?2, ?3, 'thành công')",
                params![i as i64, format!("ts{i}"), format!("url{i}")],
            )
            .unwrap();
        }

        // Page 1: limit 2, offset 0 → row_order 0, 1.
        let page1: Vec<String> = conn
            .prepare(
                "SELECT url FROM admin_user_log_cache
                 WHERE local_part = ?1
                 ORDER BY row_order ASC
                 LIMIT ?2 OFFSET ?3",
            )
            .unwrap()
            .query_map(params!["a", 2_i64, 0_i64], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(page1, vec!["url0", "url1"]);

        // Page 2: limit 2, offset 2 → row_order 2, 3.
        let page2: Vec<String> = conn
            .prepare(
                "SELECT url FROM admin_user_log_cache
                 WHERE local_part = ?1
                 ORDER BY row_order ASC
                 LIMIT ?2 OFFSET ?3",
            )
            .unwrap()
            .query_map(params!["a", 2_i64, 2_i64], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(page2, vec!["url2", "url3"]);
    }

    #[test]
    fn admin_fetch_meta_upsert() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO admin_user_log_fetch_meta
             (local_part, fetched_at_ms, row_count)
             VALUES('a', 100, 5)
             ON CONFLICT(local_part) DO UPDATE SET
                 fetched_at_ms = excluded.fetched_at_ms,
                 row_count = excluded.row_count",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO admin_user_log_fetch_meta
             (local_part, fetched_at_ms, row_count)
             VALUES('a', 200, 7)
             ON CONFLICT(local_part) DO UPDATE SET
                 fetched_at_ms = excluded.fetched_at_ms,
                 row_count = excluded.row_count",
            [],
        )
        .unwrap();

        let (ts, cnt): (i64, i64) = conn
            .query_row(
                "SELECT fetched_at_ms, row_count FROM admin_user_log_fetch_meta WHERE local_part='a'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(ts, 200, "UPSERT phải update fetched_at_ms");
        assert_eq!(cnt, 7, "UPSERT phải update row_count");
    }
}
