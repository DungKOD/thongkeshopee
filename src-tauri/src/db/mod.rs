//! Module quản lý SQLite database cho ThongKeShopee.
//!
//! - `init_db(app_handle)`: mở/tạo DB file trong app_data_dir, apply schema.
//! - `DbState`: Tauri managed state (`Arc<Mutex<Connection>>`).
//!
//! Schema chi tiết xem `schema.sql`. Theo kiến trúc ELT:
//! raw tables + manual_entries là source of truth, query on-the-fly cho UI.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::Connection;
use tauri::{AppHandle, Manager};

pub mod types;
pub mod video_db;

pub use video_db::VideoDbState;

/// SQL script áp dụng khi khởi động app (idempotent, safe để chạy mỗi lần).
const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Tên file DB trong app_data_dir.
const DB_FILENAME: &str = "thongkeshopee.db";

/// Subfolder chứa raw CSV đã import (dùng cho `imported_files.stored_path`).
pub const IMPORTS_SUBDIR: &str = "imports";

/// Subfolder root chứa DB + imports của từng Firebase user (multi-tenant).
/// Layout: `{app_data}/users/{uid}/thongkeshopee.db` + `{app_data}/users/{uid}/imports/`.
pub const USERS_SUBDIR: &str = "users";

/// Folder placeholder khi app mới start, chưa biết user UID (pre-auth).
/// Tauri setup() bắt buộc có DbState managed, nên ta mở DB tạm ở đây.
/// Sau khi user login, `switch_db_to_user` reopen ở user folder thật.
pub const PRE_AUTH_SUBDIR: &str = "_pre_auth";

/// State quản lý connection, wrap `Mutex` để share giữa các Tauri command.
pub struct DbState(pub Mutex<Connection>);

/// Build tombstone `entity_key` cho sub_id tuple + day. Dùng cho `ui_row`
/// và `manual_entry`: `{day}|{s1}|{s2}|{s3}|{s4}|{s5}`. Separator `|` không
/// xuất hiện trong sub_id thực tế (sub_id không chứa ký tự `|`).
pub fn tombstone_key_sub(day_date: &str, sub_ids: &[String; 5]) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        day_date, sub_ids[0], sub_ids[1], sub_ids[2], sub_ids[3], sub_ids[4]
    )
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

/// Resolve user-scoped folder: `{app_data}/users/{uid}/`. Sanitize UID chống
/// path traversal (Firebase UID toàn alphanumeric nên thực tế an toàn, nhưng
/// verify cho chắc).
pub fn resolve_user_dir(app: &AppHandle, uid: &str) -> Result<PathBuf> {
    let safe = uid
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !safe || uid.is_empty() {
        anyhow::bail!("UID không hợp lệ: {uid}");
    }
    let dir = app_data_root(app)?.join(USERS_SUBDIR).join(uid);
    fs::create_dir_all(&dir)
        .with_context(|| format!("không tạo được thư mục user: {}", dir.display()))?;
    Ok(dir)
}

/// Resolve DB path cho 1 user cụ thể. Caller bảo đảm UID hợp lệ qua
/// `resolve_user_dir`.
pub fn resolve_db_path_for_user(app: &AppHandle, uid: &str) -> Result<PathBuf> {
    Ok(resolve_user_dir(app, uid)?.join(DB_FILENAME))
}

/// Resolve imports folder cho 1 user: `{app_data}/users/{uid}/imports/`.
/// Mọi CSV gốc user import lưu ở đây; stored_path trong DB là "imports/<hash>.csv"
/// relative, resolve tại runtime qua fn này.
pub fn resolve_imports_dir_for_user(app: &AppHandle, uid: &str) -> Result<PathBuf> {
    let dir = resolve_user_dir(app, uid)?.join(IMPORTS_SUBDIR);
    fs::create_dir_all(&dir)
        .with_context(|| format!("không tạo được thư mục imports: {}", dir.display()))?;
    Ok(dir)
}

/// Legacy DB path ở root app_data_dir (trước khi multi-tenant folder).
/// Dùng để migration: lần đầu user login sau upgrade, move root DB sang user folder.
pub fn resolve_legacy_db_path(app: &AppHandle) -> Result<PathBuf> {
    Ok(app_data_root(app)?.join(DB_FILENAME))
}

/// Legacy imports folder ở root (migration source).
pub fn resolve_legacy_imports_dir(app: &AppHandle) -> Result<PathBuf> {
    Ok(app_data_root(app)?.join(IMPORTS_SUBDIR))
}

/// Placeholder DB path dùng khi app start chưa có user UID. DbState phải có
/// connection để Tauri managed, nên init tạm ở đây; swap sau khi user login.
pub fn resolve_pre_auth_db_path(app: &AppHandle) -> Result<PathBuf> {
    let dir = app_data_root(app)?.join(PRE_AUTH_SUBDIR);
    fs::create_dir_all(&dir)
        .with_context(|| format!("không tạo được thư mục pre_auth: {}", dir.display()))?;
    Ok(dir.join(DB_FILENAME))
}

/// Active DB path — query `PRAGMA database_list` từ connection đang mở.
/// Dùng khi command cần physical path (sync snapshot, imports folder, admin view reopen)
/// mà không muốn truyền uid qua param.
pub fn resolve_active_db_path(conn: &Connection) -> Result<PathBuf> {
    let path_str: String = conn
        .query_row("PRAGMA database_list", [], |r| r.get::<_, String>(2))
        .context("không đọc được DB path từ PRAGMA database_list")?;
    if path_str.is_empty() {
        anyhow::bail!("DB đang ở :memory: — không có physical path");
    }
    Ok(PathBuf::from(path_str))
}

/// Active imports folder — parent của DB path + "imports/". Luôn đồng bộ với
/// DB path hiện tại (switch user → DB + imports đi cùng folder).
pub fn resolve_active_imports_dir(conn: &Connection) -> Result<PathBuf> {
    let db_path = resolve_active_db_path(conn)?;
    let parent = db_path
        .parent()
        .context("DB path không có parent")?;
    let dir = parent.join(IMPORTS_SUBDIR);
    fs::create_dir_all(&dir)
        .with_context(|| format!("không tạo được thư mục imports: {}", dir.display()))?;
    Ok(dir)
}

/// Mở hoặc tạo DB tại `path`, apply schema + PRAGMA + migrations.
/// Idempotent — chạy an toàn trên DB cũ hay mới.
pub fn init_db_at(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
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
/// Public alias cho tests — tests cần chạy migrate trên in-memory DB.
#[cfg(test)]
pub fn migrate_for_tests(conn: &Connection) -> Result<()> {
    migrate(conn)
}

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
    migrate_drop_video_downloads(conn).context("drop video_downloads failed")?;
    // v5: thêm multi-account (shopee_accounts + FK cột trên raw tables).
    migrate_shopee_accounts(conn).context("shopee_accounts migration failed")?;
    // v6: drop FK CASCADE trên imported_files.day_date để support multi-day file.
    migrate_imported_files_drop_day_fk(conn)
        .context("imported_files drop day_fk migration failed")?;
    // v7: MCN fee visibility — lưu cột 31 (pre-MCN) + cột 35 (phí MCN) từ CSV.
    migrate_mcn_columns(conn).context("mcn columns migration failed")?;
    // v9: drop `raw_json` columns — không read ở đâu, dedup với CSV file đã
    // lưu trên disk (imports/<hash>.csv), giảm 55-70% DB size.
    migrate_drop_raw_json(conn).context("drop raw_json columns failed")?;
    // Dọn orphan imported_files mỗi startup — idempotent. Xử lý legacy DB
    // của user đã xóa ngày trước khi batch_commit_deletes biết cleanup
    // (kẻo re-import cùng file bị chặn bởi hash dedup).
    cleanup_orphan_imported_files(conn).context("cleanup orphan imported_files failed")?;

    Ok(())
}

/// v9 migration: drop `raw_json` columns khỏi `raw_shopee_order_items` +
/// `raw_fb_ads`. Data vẫn còn trên disk ở `imports/<hash>.csv` — tương lai
/// cần re-extract field nào có thể đọc lại CSV. Idempotent (check trước drop).
///
/// SQLite 3.35+ support `ALTER TABLE DROP COLUMN`. rusqlite bundled v3.46+ OK.
/// Sau drop, cần VACUUM để reclaim disk space thực sự — chạy 1 lần sau migrate.
fn migrate_drop_raw_json(conn: &Connection) -> Result<()> {
    let mut vacuum_needed = false;
    for table in ["raw_shopee_order_items", "raw_fb_ads"] {
        if table_has_column(conn, table, "raw_json")? {
            conn.execute(&format!("ALTER TABLE {table} DROP COLUMN raw_json"), [])?;
            vacuum_needed = true;
        }
    }
    if vacuum_needed {
        // VACUUM reclaim free pages từ DROP COLUMN. Tốn thời gian (~10-30s cho
        // 500MB DB) nhưng chỉ 1 lần sau migration v9. Subsequent startup skip
        // vì table_has_column return false.
        conn.execute_batch("VACUUM")?;
    }
    conn.execute(
        "INSERT OR IGNORE INTO _schema_version(version, applied_at)
         VALUES(9, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        [],
    )?;
    Ok(())
}

/// DELETE imported_files không còn raw row nào reference qua source_file_id.
/// Chạy mỗi startup vì DELETE orphan là idempotent — chi phí rẻ và đảm bảo
/// user re-import được file sau khi xóa data, kể cả với DB legacy.
fn cleanup_orphan_imported_files(conn: &Connection) -> Result<()> {
    conn.execute(
        "DELETE FROM imported_files
         WHERE id NOT IN (
             SELECT source_file_id FROM raw_shopee_clicks UNION
             SELECT source_file_id FROM raw_shopee_order_items UNION
             SELECT source_file_id FROM raw_fb_ads
         )",
        [],
    )?;
    Ok(())
}

/// v7 migration: MCN fee visibility.
/// ALTER TABLE ADD 2 cột mới (idempotent check-before-add):
/// - `order_commission_total` — CSV col 31 "Tổng hoa hồng đơn hàng(₫)" (pre-MCN).
/// - `mcn_fee` — CSV col 35 "Phí quản lý MCN(₫)" (Shopee đã cắt sẵn khỏi net).
/// Data cũ NULL → query aggregate COALESCE(..., 0) khi SUM.
fn migrate_mcn_columns(conn: &Connection) -> Result<()> {
    if !table_has_column(conn, "raw_shopee_order_items", "order_commission_total")? {
        conn.execute(
            "ALTER TABLE raw_shopee_order_items ADD COLUMN order_commission_total REAL",
            [],
        )?;
    }
    if !table_has_column(conn, "raw_shopee_order_items", "mcn_fee")? {
        conn.execute(
            "ALTER TABLE raw_shopee_order_items ADD COLUMN mcn_fee REAL",
            [],
        )?;
    }
    conn.execute(
        "INSERT OR IGNORE INTO _schema_version(version, applied_at)
         VALUES(7, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        [],
    )?;
    Ok(())
}

/// v6 migration: imported_files.day_date — rebuild table để drop `REFERENCES
/// days(date) ON DELETE CASCADE` + `NOT NULL`. Cho phép file Shopee multi-day
/// (commission report update đơn nhiều ngày trong 1 lần export).
///
/// Pre-fix bug: xóa 1 ngày → CASCADE xóa imported_files entry có day_date=đó
/// → CASCADE xóa raw_shopee_* rows theo source_file_id → mất data của NGÀY
/// KHÁC cùng file. Post-fix: day_date chỉ là info, không cascade.
fn migrate_imported_files_drop_day_fk(conn: &Connection) -> Result<()> {
    let existing_sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='imported_files'",
            [],
            |r| r.get(0),
        )
        .ok();
    let needs_migrate = match existing_sql.as_deref() {
        Some(s) => s.contains("REFERENCES days(date)"),
        None => false,
    };
    if !needs_migrate {
        return Ok(());
    }

    // Disable FK checks trong quá trình rebuild. Làm ngoài transaction vì
    // pragma không có hiệu lực trong transaction đang mở trong SQLite.
    conn.execute_batch(
        "PRAGMA foreign_keys = OFF;
         CREATE TABLE imported_files_new (
             id           INTEGER PRIMARY KEY AUTOINCREMENT,
             filename     TEXT NOT NULL,
             kind         TEXT NOT NULL,
             imported_at  TEXT NOT NULL,
             row_count    INTEGER NOT NULL DEFAULT 0,
             file_hash    TEXT NOT NULL,
             stored_path  TEXT,
             day_date     TEXT,
             notes        TEXT,
             UNIQUE(file_hash)
         );
         INSERT INTO imported_files_new
           (id, filename, kind, imported_at, row_count, file_hash, stored_path, day_date, notes)
         SELECT
           id, filename, kind, imported_at, row_count, file_hash, stored_path, day_date, notes
         FROM imported_files;
         DROP TABLE imported_files;
         ALTER TABLE imported_files_new RENAME TO imported_files;
         CREATE INDEX IF NOT EXISTS idx_imported_day  ON imported_files(day_date);
         CREATE INDEX IF NOT EXISTS idx_imported_kind ON imported_files(kind);
         PRAGMA foreign_keys = ON;",
    )?;

    conn.execute(
        "INSERT OR IGNORE INTO _schema_version(version, applied_at)
         VALUES(6, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        [],
    )?;
    Ok(())
}

/// v5 migration: multi-tenant cho Shopee affiliate account.
/// - Tạo bảng `shopee_accounts` (schema.sql chạy trước nên có rồi, idempotent).
/// - Seed default account (id=1) nếu chưa có — để row cũ có default khi ALTER.
/// - ALTER TABLE ADD COLUMN `shopee_account_id` với DEFAULT 1 cho 3 bảng raw
///   Shopee (clicks/orders/manual). SQLite auto backfill = 1 cho mọi row cũ.
/// - KHÔNG thêm FK constraint (ALTER TABLE ADD COLUMN không support FK trong
///   SQLite — app-layer enforce referential integrity).
fn migrate_shopee_accounts(conn: &Connection) -> Result<()> {
    // Seed default account — dùng INSERT OR IGNORE để idempotent.
    // Literal id=1 để FK cột DEFAULT 1 ở ALTER TABLE luôn trỏ đúng.
    conn.execute(
        "INSERT OR IGNORE INTO shopee_accounts (id, name, color, created_at)
         VALUES (1, 'Mặc định', '#ff6b35', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        [],
    )?;

    for table in ["raw_shopee_clicks", "raw_shopee_order_items", "manual_entries"] {
        if !table_has_column(conn, table, "shopee_account_id")? {
            conn.execute(
                &format!(
                    "ALTER TABLE {table} ADD COLUMN shopee_account_id INTEGER NOT NULL DEFAULT 1"
                ),
                [],
            )?;
        }
    }

    // Index phụ cho filter query theo account.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_clicks_account
         ON raw_shopee_clicks(shopee_account_id, day_date)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_orders_account
         ON raw_shopee_order_items(shopee_account_id, day_date)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_manual_account
         ON manual_entries(shopee_account_id, day_date)",
        [],
    )?;

    conn.execute(
        "INSERT OR IGNORE INTO _schema_version(version, applied_at)
         VALUES(5, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        [],
    )?;
    Ok(())
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let cols: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<std::result::Result<_, _>>()?;
    Ok(cols.iter().any(|c| c == column))
}

/// v4 migration: drop `video_downloads` khỏi main DB. Table đã move sang
/// `video_logs.db` riêng để main DB backup Drive gọn nhẹ hơn.
/// Data cũ KHÔNG copy sang (audit storage mới là Google Sheet).
fn migrate_drop_video_downloads(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "DROP TRIGGER IF EXISTS trg_sync_video_ins;
         DROP TRIGGER IF EXISTS trg_sync_video_upd;
         DROP TRIGGER IF EXISTS trg_sync_video_del;
         DROP TABLE IF EXISTS video_downloads;",
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO _schema_version(version, applied_at)
         VALUES(4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        [],
    )?;
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
              day_date, source_file_id)
             SELECT 'ad_group', ad_group_name,
                    sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                    report_start, report_end, status,
                    spend, {clicks_expr}, {cpc_expr}, impressions, reach,
                    day_date, source_file_id
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
              day_date, source_file_id)
             SELECT 'campaign', campaign_name,
                    sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                    report_start, report_end, status,
                    spend, {clicks_expr}, {cpc_expr}, impressions, reach,
                    day_date, source_file_id
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
    // owner_uid: Firebase UID của user cuối cùng sync/sở hữu DB này. Dùng detect
    // multi-tenant khi nhiều user login cùng máy → FE check owner != current
    // user sẽ wipe local DB trước khi sync (tránh B thấy data của A).
    if !cols.iter().any(|c| c == "owner_uid") {
        conn.execute(
            "ALTER TABLE sync_state ADD COLUMN owner_uid TEXT",
            [],
        )?;
    }
    // v8: HLC-lite clock (chống clock drift giữa 2 máy). Mỗi mutation lấy
    // `max(now_ms, last_known_clock_ms + 1)` → timestamp monotonic kể cả khi
    // local wall clock chậm hơn remote. Sau merge, absorb max remote timestamp
    // → edit sau merge luôn > mọi edit remote đã thấy.
    if !cols.iter().any(|c| c == "last_known_clock_ms") {
        conn.execute(
            "ALTER TABLE sync_state ADD COLUMN last_known_clock_ms INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    // v8: CAS upload guard. Store etag từ R2 lần cuối pull/upload thành công.
    // Next upload attach expected_etag → Worker reject 412 nếu R2 đã thay đổi
    // (máy khác upload trong lúc này) → FE force pull-merge-push + retry.
    if !cols.iter().any(|c| c == "last_remote_etag") {
        conn.execute(
            "ALTER TABLE sync_state ADD COLUMN last_remote_etag TEXT",
            [],
        )?;
    }
    // v8.1: skip-identical hash. MD5 của compressed bytes lần upload gần nhất.
    // Next upload compute hash trước → match → skip (no-op sync), save bandwidth
    // cho case user edit-revert-edit-revert tạo identical snapshots.
    if !cols.iter().any(|c| c == "last_uploaded_hash") {
        conn.execute(
            "ALTER TABLE sync_state ADD COLUMN last_uploaded_hash TEXT",
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
        // Tombstones: INSERT khi user xóa → cần sync qua Drive. DELETE khi user
        // restore (tương lai) hoặc compact — cũng mark dirty.
        ("trg_sync_tombstones_ins", "INSERT", "tombstones"),
        ("trg_sync_tombstones_del", "DELETE", "tombstones"),
        // Shopee accounts: CRUD account cần đồng bộ cross-device (2 máy tạo
        // cùng TK → sync merge theo UNIQUE name).
        ("trg_sync_accounts_ins", "INSERT", "shopee_accounts"),
        ("trg_sync_accounts_upd", "UPDATE", "shopee_accounts"),
        ("trg_sync_accounts_del", "DELETE", "shopee_accounts"),
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

/// Setup hook cho `tauri::Builder`: init DB placeholder ở `_pre_auth/` folder
/// + video logs DB + managed state. Main DB sẽ được swap sang user folder thật
/// khi `switch_db_to_user(uid)` chạy sau khi FE auth ready.
///
/// Placeholder có migrations + schema đầy đủ → commands pre-auth technically
/// work nhưng data bị isolate trong `_pre_auth/` folder, không leak sang user.
pub fn setup(app: &AppHandle) -> Result<()> {
    let path = resolve_pre_auth_db_path(app)?;
    let conn = init_db_at(&path)?;
    app.manage(DbState(Mutex::new(conn)));
    video_db::setup(app)?;
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

        // v6: imported_files không còn FK CASCADE qua day_date → metadata survive
        // sau khi day bị xóa. Cần thiết để support file Shopee multi-day.
        let file_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM imported_files", [], |r| r.get(0))
            .unwrap();
        assert_eq!(file_count, 1, "imported_files KHÔNG cascade từ day (v6)");
    }

    #[test]
    fn tombstones_unique_and_key_format() {
        let conn = test_conn();
        // Unique theo (entity_type, entity_key).
        conn.execute(
            "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
             VALUES('day', '2026-04-20', 'now')",
            [],
        )
        .unwrap();
        let dup = conn.execute(
            "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
             VALUES('day', '2026-04-20', 'now2')",
            [],
        );
        assert!(dup.is_err(), "tombstone (type, key) phải unique");

        // Khác entity_type cùng key → OK (ui_row vs manual_entry riêng).
        let sub_ids = ["shop".into(), "a".into(), "".into(), "".into(), "".into()];
        let k = tombstone_key_sub("2026-04-20", &sub_ids);
        assert_eq!(k, "2026-04-20|shop|a|||");
        conn.execute(
            "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
             VALUES('ui_row', ?, 'now')",
            [&k],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
             VALUES('manual_entry', ?, 'now')",
            [&k],
        )
        .unwrap();

        // Check constraint — type không hợp lệ phải reject.
        let bad = conn.execute(
            "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
             VALUES('invalid', 'x', 'now')",
            [],
        );
        assert!(bad.is_err(), "entity_type không hợp lệ phải reject");
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
