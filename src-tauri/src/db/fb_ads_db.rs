//! Separate SQLite DB cho FB Ads bulk camp creator.
//! Lưu local-only ở `{app_data}/fb_ads.db`.
//!
//! 5 bảng:
//! - `fb_ad_accounts` — Meta App + Ad Account user lưu (kèm access_token plain text)
//! - `fb_camp_templates` — snapshot 1 campaign mẫu để clone, cache JSON
//! - `fb_camp_drafts` — bản nháp spreadsheet user đang edit, auto-save
//! - `fb_camp_batches` — 1 lần chạy batch, status tổng
//! - `fb_camp_jobs` — 1 row trong batch = 1 camp clone, có FB IDs trả về
//!
//! Token Page plain text — exclude khỏi backup/restore main DB.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::Connection;
use tauri::{AppHandle, Manager};

pub const DB_FILENAME: &str = "fb_ads.db";

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS fb_ad_accounts (
    account_id    TEXT PRIMARY KEY,   -- "act_123456789"
    name          TEXT NOT NULL,
    currency      TEXT,
    timezone_name TEXT,
    access_token  TEXT NOT NULL,
    added_at_ms   INTEGER NOT NULL
);

-- User Token user paste vào "Xác thực token" cho FB Ads (scope ads_management).
-- Khác với access_token trên fb_ad_accounts (token mỗi account riêng — từ /me/adaccounts).
-- 1 User Token quản N Ad Accounts.
CREATE TABLE IF NOT EXISTS fb_ads_auth_tokens (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    label         TEXT NOT NULL,
    access_token  TEXT NOT NULL,
    token_hash    TEXT NOT NULL UNIQUE,
    added_at_ms   INTEGER NOT NULL,
    expired       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fb_camp_templates (
    template_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id       TEXT NOT NULL,
    fb_campaign_id   TEXT NOT NULL,
    name             TEXT NOT NULL,
    objective        TEXT,
    snapshot_json    TEXT NOT NULL,
    cached_at_ms     INTEGER NOT NULL,
    UNIQUE(account_id, fb_campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_fb_templates_account
    ON fb_camp_templates(account_id, cached_at_ms DESC);

CREATE TABLE IF NOT EXISTS fb_camp_drafts (
    draft_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    template_id   INTEGER NOT NULL,
    name          TEXT NOT NULL,
    rows_json     TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fb_drafts_updated
    ON fb_camp_drafts(updated_at_ms DESC);

CREATE TABLE IF NOT EXISTS fb_camp_batches (
    batch_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    draft_id        INTEGER,
    template_id     INTEGER NOT NULL,
    template_name   TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    total_rows      INTEGER NOT NULL,
    success_count   INTEGER NOT NULL DEFAULT 0,
    failed_count    INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL CHECK(status IN
                        ('pending','running','completed','cancelled','failed')),
    started_at_ms   INTEGER NOT NULL,
    finished_at_ms  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_fb_batches_started
    ON fb_camp_batches(started_at_ms DESC);

CREATE TABLE IF NOT EXISTS fb_camp_jobs (
    job_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id        INTEGER NOT NULL,
    row_index       INTEGER NOT NULL,
    camp_name       TEXT NOT NULL,
    adset_name      TEXT NOT NULL,
    ad_name         TEXT NOT NULL,
    caption         TEXT,
    video_path      TEXT NOT NULL,
    sub_id          TEXT NOT NULL,
    status          TEXT NOT NULL CHECK(status IN
                        ('pending','uploading_video','creating_creative',
                         'creating_campaign','creating_adset','creating_ad',
                         'done','failed')),
    progress        INTEGER NOT NULL DEFAULT 0 CHECK(progress BETWEEN 0 AND 100),
    fb_video_id     TEXT,
    fb_creative_id  TEXT,
    fb_campaign_id  TEXT,
    fb_adset_id     TEXT,
    fb_ad_id        TEXT,
    error_message   TEXT,
    started_at_ms   INTEGER,
    finished_at_ms  INTEGER,
    FOREIGN KEY (batch_id) REFERENCES fb_camp_batches(batch_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_fb_jobs_batch
    ON fb_camp_jobs(batch_id, row_index);

CREATE INDEX IF NOT EXISTS idx_fb_jobs_status
    ON fb_camp_jobs(status, started_at_ms DESC);
"#;

/// Tauri managed state cho FB Ads DB connection.
pub struct FbAdsDbState(pub Mutex<Connection>);

/// DB path cho FB Ads trong workspace folder.
pub fn resolve_fb_ads_db_path_in(workspace_root: &Path) -> PathBuf {
    workspace_root.join(DB_FILENAME)
}

/// Mở hoặc tạo FB Ads DB tại `path`, apply PRAGMA + schema.
pub fn init_fb_ads_db_at(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("không mở được FB Ads DB tại {}", path.display()))?;

    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;
         PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;",
    )
    .context("không apply được PRAGMA cho FB Ads DB")?;

    conn.execute_batch(SCHEMA_SQL)
        .context("không apply được schema cho FB Ads DB")?;

    Ok(conn)
}

/// Mở FB Ads DB đã tồn tại — chỉ apply PRAGMA, KHÔNG re-apply schema. Cho
/// workspace hot-swap (xem doc `crate::db::open_existing_db`).
pub fn open_existing_fb_ads_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("không mở được FB Ads DB tại {}", path.display()))?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA foreign_keys = ON;
         PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;",
    )
    .context("không apply được PRAGMA cho FB Ads DB khi open existing")?;
    Ok(conn)
}

/// Setup hook — init DB trong workspace folder + manage state.
pub fn setup_in(app: &AppHandle, workspace_root: &Path) -> Result<()> {
    let path = resolve_fb_ads_db_path_in(workspace_root);
    let conn = init_fb_ads_db_at(&path)?;
    app.manage(FbAdsDbState(Mutex::new(conn)));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

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
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        for expected in [
            "fb_ad_accounts",
            "fb_camp_batches",
            "fb_camp_drafts",
            "fb_camp_jobs",
            "fb_camp_templates",
        ] {
            assert!(tables.iter().any(|t| t == expected), "missing {expected}");
        }
    }

    #[test]
    fn account_upsert_replaces_token() {
        let conn = test_conn();
        let insert = |tok: &str| {
            conn.execute(
                "INSERT INTO fb_ad_accounts
                 (account_id, name, currency, timezone_name, access_token, added_at_ms)
                 VALUES('act_1', 'Acc', 'VND', 'Asia/Ho_Chi_Minh', ?1, 100)
                 ON CONFLICT(account_id) DO UPDATE SET
                     access_token = excluded.access_token",
                params![tok],
            )
        };
        insert("tok1").unwrap();
        insert("tok2").unwrap();
        let (cnt, tok): (i64, String) = conn
            .query_row(
                "SELECT COUNT(*), MAX(access_token) FROM fb_ad_accounts",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(cnt, 1);
        assert_eq!(tok, "tok2");
    }

    #[test]
    fn template_unique_per_account_camp() {
        let conn = test_conn();
        let insert_tpl = || {
            conn.execute(
                "INSERT INTO fb_camp_templates
                 (account_id, fb_campaign_id, name, objective, snapshot_json, cached_at_ms)
                 VALUES('act_1', 'cam_1', 'T1', 'OUTCOME_TRAFFIC', '{}', 100)",
                [],
            )
        };
        insert_tpl().unwrap();
        assert!(insert_tpl().is_err(), "trùng (account, fb_camp) phải fail");
    }

    #[test]
    fn jobs_cascade_on_batch_delete() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO fb_camp_batches
             (batch_id, template_id, template_name, account_id, total_rows, status, started_at_ms)
             VALUES(1, 1, 'T', 'act_1', 1, 'pending', 100)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO fb_camp_jobs
             (batch_id, row_index, camp_name, adset_name, ad_name, video_path,
              sub_id, status)
             VALUES(1, 0, 'C', 'AS', 'A', '/x.mp4', 'sub1', 'pending')",
            [],
        )
        .unwrap();
        conn.execute("DELETE FROM fb_camp_batches WHERE batch_id = 1", [])
            .unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM fb_camp_jobs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "CASCADE phải xóa jobs");
    }

    #[test]
    fn job_status_check_rejects_invalid() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO fb_camp_batches
             (batch_id, template_id, template_name, account_id, total_rows, status, started_at_ms)
             VALUES(1, 1, 'T', 'act_1', 1, 'pending', 100)",
            [],
        )
        .unwrap();
        let r = conn.execute(
            "INSERT INTO fb_camp_jobs
             (batch_id, row_index, camp_name, adset_name, ad_name, video_path,
              sub_id, status)
             VALUES(1, 0, 'C', 'AS', 'A', '/x.mp4', 'sub1', 'bogus')",
            [],
        );
        assert!(r.is_err());
    }

    #[test]
    fn draft_rows_json_persists() {
        let conn = test_conn();
        let rows = r#"[{"camp_name":"C1","adset_name":"A1","ad_name":"AD1"}]"#;
        conn.execute(
            "INSERT INTO fb_camp_drafts
             (template_id, name, rows_json, created_at_ms, updated_at_ms)
             VALUES(1, 'D1', ?1, 100, 100)",
            params![rows],
        )
        .unwrap();
        let stored: String = conn
            .query_row("SELECT rows_json FROM fb_camp_drafts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(stored, rows);
    }
}
