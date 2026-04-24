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
    // v10: import history + many-to-many mapping cho revert correctness.
    // Rebuild `imported_files` để drop UNIQUE(file_hash) inline (thay bằng
    // partial index WHERE reverted_at IS NULL), add cột reverted_at, backfill
    // 3 bảng mapping từ source_file_id hiện có.
    migrate_import_history_v10(conn).context("import history v10 migration failed")?;
    // v11 = Sync v9 infrastructure (additive only ở Phase 1): thêm 3 table mới
    // (sync_cursor_state, sync_manifest_state, sync_event_log).
    migrate_v11_sync_infra(conn).context("v11 sync v9 infra migration failed")?;
    // v12 (P8b) = drop v8 sync_state columns + v8 triggers. v9 tracking giờ
    // hoàn toàn qua sync_cursor_state + sync_manifest_state.
    migrate_v12_drop_v8_sync(conn).context("v12 drop v8 sync migration failed")?;
    // v13 = content-based ids cho FK-referenced tables. Fix data-loss bug
    // khi 2 máy fresh-install cùng autoincrement id collision. Rewrite existing
    // rows sang content_id + add ON UPDATE CASCADE cho FK tới imported_files.id.
    migrate_v13_content_ids(conn).context("v13 content-id migration failed")?;
    // v13 rebuild raw_* tables → triggers attach on old table dropped.
    // Recreate mapping cleanup triggers để day delete cascade vẫn clean
    // orphan mapping rows.
    create_mapping_cleanup_triggers(conn)
        .context("recreate mapping cleanup triggers after v13 failed")?;
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

/// DELETE imported_files không còn raw row nào reference qua mapping tables.
/// Chạy mỗi startup vì DELETE orphan là idempotent — chi phí rẻ và đảm bảo
/// user re-import được file sau khi xóa data, kể cả với DB legacy.
///
/// v10: check qua MAPPING tables thay `source_file_id` vì UPSERT từ file B có
/// thể ghi đè source_file_id trên row từ file A → file A "trông như orphan"
/// dù mapping(row, A) vẫn có. Giữ row `reverted_at IS NOT NULL` (lịch sử).
fn cleanup_orphan_imported_files(conn: &Connection) -> Result<()> {
    conn.execute(
        "DELETE FROM imported_files
         WHERE reverted_at IS NULL
           AND id NOT IN (
               SELECT file_id FROM clicks_to_file UNION
               SELECT file_id FROM orders_to_file UNION
               SELECT file_id FROM fb_ads_to_file
           )",
        [],
    )?;
    Ok(())
}

/// v10 migration: import history + many-to-many mapping raw↔file.
///
/// Thay đổi:
/// 1. Rebuild `imported_files` để drop UNIQUE(file_hash) inline (bị khóa
///    sau revert không cho re-import cùng file) → thay bằng partial unique
///    index `WHERE reverted_at IS NULL`.
/// 2. Add cột `reverted_at TEXT` để soft-mark file đã revert (giữ history).
/// 3. Backfill 3 bảng mapping từ `source_file_id` hiện có — data cũ chỉ
///    biết 1 file nguồn duy nhất (không tracked many-to-many trước v10).
///    Data mới import sau migration có đầy đủ link.
///
/// Idempotent — chạy lại skip nếu đã không còn UNIQUE inline + reverted_at
/// đã có. Backfill dùng INSERT OR IGNORE nên chạy N lần cho ra cùng kết quả.
fn migrate_import_history_v10(conn: &Connection) -> Result<()> {
    let existing_sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='imported_files'",
            [],
            |r| r.get(0),
        )
        .ok();
    let has_inline_unique = match existing_sql.as_deref() {
        Some(s) => s.contains("UNIQUE(file_hash)") || s.contains("UNIQUE (file_hash)"),
        None => return Ok(()),
    };
    let has_reverted_at = table_has_column(conn, "imported_files", "reverted_at")?;

    if has_inline_unique {
        // Rebuild toàn bộ bảng: drop UNIQUE constraint, thêm cột reverted_at +
        // shopee_account_id. Disable FK trong rebuild vì raw tables có FK
        // ON DELETE CASCADE trỏ tới.
        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;
             CREATE TABLE imported_files_new (
                 id                INTEGER PRIMARY KEY AUTOINCREMENT,
                 filename          TEXT NOT NULL,
                 kind              TEXT NOT NULL,
                 imported_at       TEXT NOT NULL,
                 row_count         INTEGER NOT NULL DEFAULT 0,
                 file_hash         TEXT NOT NULL,
                 stored_path       TEXT,
                 day_date          TEXT,
                 notes             TEXT,
                 reverted_at       TEXT,
                 shopee_account_id INTEGER
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
             CREATE UNIQUE INDEX IF NOT EXISTS idx_imported_hash_active
                 ON imported_files(file_hash) WHERE reverted_at IS NULL;
             PRAGMA foreign_keys = ON;",
        )?;
    } else {
        // Defensive: nếu table đã không còn UNIQUE inline nhưng cũng chưa có
        // reverted_at (edge case giữa migration cũ và mới) → ADD COLUMN thường.
        if !has_reverted_at {
            conn.execute(
                "ALTER TABLE imported_files ADD COLUMN reverted_at TEXT",
                [],
            )?;
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_imported_hash_active
                 ON imported_files(file_hash) WHERE reverted_at IS NULL",
                [],
            )?;
        }
    }
    // Idempotent thêm cột shopee_account_id nếu chưa có (rebuild path đã có rồi).
    if !table_has_column(conn, "imported_files", "shopee_account_id")? {
        conn.execute(
            "ALTER TABLE imported_files ADD COLUMN shopee_account_id INTEGER",
            [],
        )?;
    }

    // Unconditional: đảm bảo partial unique index tồn tại cho cả fresh install
    // (schema.sql không tạo vì reverted_at là cột v10) lẫn upgrade. Idempotent.
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_imported_hash_active
         ON imported_files(file_hash) WHERE reverted_at IS NULL",
        [],
    )?;
    // Backfill shopee_account_id cho data cũ — lấy account phổ biến nhất trong
    // raw rows của file. Chỉ shopee_* kind mới có account; fb_* giữ NULL.
    conn.execute(
        "UPDATE imported_files
         SET shopee_account_id = (
             SELECT shopee_account_id FROM raw_shopee_clicks
             WHERE source_file_id = imported_files.id
             GROUP BY shopee_account_id ORDER BY COUNT(*) DESC LIMIT 1
         )
         WHERE kind = 'shopee_clicks' AND shopee_account_id IS NULL",
        [],
    )?;
    conn.execute(
        "UPDATE imported_files
         SET shopee_account_id = (
             SELECT shopee_account_id FROM raw_shopee_order_items
             WHERE source_file_id = imported_files.id
             GROUP BY shopee_account_id ORDER BY COUNT(*) DESC LIMIT 1
         )
         WHERE kind = 'shopee_commission' AND shopee_account_id IS NULL",
        [],
    )?;

    // Backfill mapping từ source_file_id. INSERT OR IGNORE: nếu chạy lại
    // không double-insert. Data cũ chỉ track 1 file/row → ít tối ưu nhưng
    // không sai (correctness = data mới sẽ đúng 100%).
    conn.execute(
        "INSERT OR IGNORE INTO clicks_to_file(click_id, file_id)
         SELECT click_id, source_file_id FROM raw_shopee_clicks",
        [],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO orders_to_file(order_item_id, file_id)
         SELECT id, source_file_id FROM raw_shopee_order_items",
        [],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO fb_ads_to_file(fb_ad_id, file_id)
         SELECT id, source_file_id FROM raw_fb_ads",
        [],
    )?;

    conn.execute(
        "INSERT OR IGNORE INTO _schema_version(version, applied_at)
         VALUES(10, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
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

/// Đảm bảo sync_state có cột v9-essential + mapping cleanup triggers.
///
/// P8b: đã gỡ v8 column additions (dirty/change_id/last_*) + v8 triggers
/// (trg_sync_*). Cột v8 thừa sẽ được `migrate_v12_drop_v8_sync` drop khỏi
/// DB legacy. v9 sync tracking qua `sync_cursor_state` + `sync_manifest_state`.
fn migrate_sync_state(conn: &Connection) -> Result<()> {
    let cols: Vec<String> = {
        let mut stmt = conn.prepare("PRAGMA table_info(sync_state)")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(1))?;
        rows.collect::<std::result::Result<_, _>>()?
    };

    // owner_uid: Firebase UID của user sở hữu DB này. Dùng detect multi-tenant
    // khi nhiều user login cùng máy → FE check owner != current user sẽ wipe
    // local DB trước khi sync.
    if !cols.iter().any(|c| c == "owner_uid") {
        conn.execute("ALTER TABLE sync_state ADD COLUMN owner_uid TEXT", [])?;
    }
    // HLC-lite clock (giữ từ v8, v9 reuse): timestamp monotonic counter chống
    // clock drift giữa 2 máy. Mỗi mutation lấy max(now_ms, last_known_clock_ms+1).
    if !cols.iter().any(|c| c == "last_known_clock_ms") {
        conn.execute(
            "ALTER TABLE sync_state ADD COLUMN last_known_clock_ms INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }

    create_mapping_cleanup_triggers(conn)?;

    Ok(())
}

/// Tạo (hoặc DROP+recreate) triggers dọn mapping tables khi raw row bị
/// xóa. Gọi từ migrate_sync_state HOẶC sau migrate_v13 (v13 rebuild raw
/// tables → triggers cũ mất → phải recreate).
///
/// v10 correctness: mapping tables phải drop khi raw row bị xóa (direct hoặc
/// CASCADE từ days.date). Nếu không: mapping rỗng chỉ tới row đã xóa →
/// revert orphan query miss + re-import (orders/fb_ads có AUTOINCREMENT id)
/// tạo row mới id không khớp mapping cũ.
///
/// Lưu ý: SQLite trigger FIRE cả với CASCADE delete (khác MySQL). Vậy nên
/// day delete → raw CASCADE → trigger fire → mapping cleanup. Atomic.
fn create_mapping_cleanup_triggers(conn: &Connection) -> Result<()> {
    let specs: &[(&str, &str, &str, &str)] = &[
        (
            "trg_cleanup_click_mapping",
            "raw_shopee_clicks",
            "clicks_to_file",
            "click_id",
        ),
        (
            "trg_cleanup_order_mapping",
            "raw_shopee_order_items",
            "orders_to_file",
            "order_item_id",
        ),
        (
            "trg_cleanup_fb_ad_mapping",
            "raw_fb_ads",
            "fb_ads_to_file",
            "fb_ad_id",
        ),
    ];
    for (trg, src_tbl, map_tbl, map_key) in specs {
        let old_col = if *map_key == "click_id" { "click_id" } else { "id" };
        conn.execute(&format!("DROP TRIGGER IF EXISTS {trg}"), [])?;
        conn.execute(
            &format!(
                "CREATE TRIGGER {trg} AFTER DELETE ON {src_tbl}
                 BEGIN DELETE FROM {map_tbl} WHERE {map_key} = OLD.{old_col}; END"
            ),
            [],
        )?;
    }
    Ok(())
}

/// v12 (P8b) — drop v8 sync artifacts sau khi v9 thay sync layer xong.
///
/// Idempotent: DROP TRIGGER IF EXISTS + check column existence trước DROP.
/// Safe chạy nhiều lần (startup sau migration đầu sẽ no-op).
///
/// Drop:
/// - 17 triggers `trg_sync_*` trên raw_*, manual_entries, tombstones,
///   shopee_accounts (bump dirty/change_id — v9 không dùng).
/// - 8 cột v8 trong `sync_state` (dirty, change_id, last_uploaded_change_id,
///   last_uploaded_hash, last_remote_etag, last_synced_at_ms,
///   last_synced_remote_mtime_ms, last_error).
///
/// Giữ: id, owner_uid, last_known_clock_ms.
fn migrate_v12_drop_v8_sync(conn: &Connection) -> Result<()> {
    let v8_triggers = [
        "trg_sync_clicks_ins",
        "trg_sync_clicks_upd",
        "trg_sync_clicks_del",
        "trg_sync_orders_ins",
        "trg_sync_orders_upd",
        "trg_sync_orders_del",
        "trg_sync_fb_ads_ins",
        "trg_sync_fb_ads_upd",
        "trg_sync_fb_ads_del",
        "trg_sync_manual_ins",
        "trg_sync_manual_upd",
        "trg_sync_manual_del",
        "trg_sync_tombstones_ins",
        "trg_sync_tombstones_del",
        "trg_sync_accounts_ins",
        "trg_sync_accounts_upd",
        "trg_sync_accounts_del",
    ];
    for name in v8_triggers {
        conn.execute(&format!("DROP TRIGGER IF EXISTS {name}"), [])?;
    }

    let cols: Vec<String> = {
        let mut stmt = conn.prepare("PRAGMA table_info(sync_state)")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(1))?;
        rows.collect::<std::result::Result<_, _>>()?
    };

    let v8_cols = [
        "dirty",
        "change_id",
        "last_uploaded_change_id",
        "last_uploaded_hash",
        "last_remote_etag",
        "last_synced_at_ms",
        "last_synced_remote_mtime_ms",
        "last_error",
    ];
    for col in v8_cols {
        if cols.iter().any(|c| c == col) {
            // SQLite 3.35+ support DROP COLUMN (rusqlite bundled v3.46+ OK).
            conn.execute(&format!("ALTER TABLE sync_state DROP COLUMN {col}"), [])?;
        }
    }

    Ok(())
}

/// v13 = content-based deterministic ids cho 4 tables có autoincrement id
/// + được reference qua FK / informal cross-table pointer.
///
/// **Fix bug data-loss cross-machine:** 2 máy fresh-install cùng
/// autoincrement = 1 → apply-side INSERT OR IGNORE silently drop. Sau
/// migration: id = hash(natural_key), cùng content → cùng id mọi máy.
///
/// **Scope:**
/// - `imported_files` id = `content_id.imported_file_id(file_hash)`
/// - `shopee_accounts` id = `content_id.shopee_account_id(name)`
/// - `raw_shopee_order_items` id = `content_id.order_item_id(...)`
/// - `raw_fb_ads` id = `content_id.fb_ad_id(...)`
///
/// **FK cascade:**
/// - 6 tables FK tới `imported_files.id` → rebuild với `ON UPDATE CASCADE`
///   để UPDATE parent.id tự propagate children.
/// - `orders_to_file.order_item_id`, `fb_ads_to_file.fb_ad_id`,
///   `raw_*.shopee_account_id`, `imported_files.shopee_account_id` là
///   informal pointer (không declared FK) → migration update thủ công.
///
/// Idempotent: check FK `on_update` = CASCADE trước → đã migrate → skip.
fn migrate_v13_content_ids(conn: &Connection) -> Result<()> {
    // ---------- Idempotency: đã chạy chưa? ----------
    let already_migrated = raw_shopee_clicks_fk_has_cascade(conn)?;
    if already_migrated {
        return Ok(());
    }

    // ---------- Wrap TX + disable FK cho schema rebuild ----------
    // PRAGMA foreign_keys OFF bắt buộc — schema rebuild (DROP + RENAME) sẽ
    // vi phạm FK tạm thời. Phải re-enable sau commit kể cả khi Err.
    conn.execute_batch("PRAGMA foreign_keys = OFF;")?;

    let result = (|| -> Result<()> {
        let tx = conn.unchecked_transaction()?;

        // PART A: Rebuild 6 tables thêm ON UPDATE CASCADE FK tới imported_files.id.
        rebuild_raw_shopee_clicks_with_cascade(&tx)?;
        rebuild_raw_shopee_order_items_with_cascade(&tx)?;
        rebuild_raw_fb_ads_with_cascade(&tx)?;
        rebuild_clicks_to_file_with_cascade(&tx)?;
        rebuild_orders_to_file_with_cascade(&tx)?;
        rebuild_fb_ads_to_file_with_cascade(&tx)?;

        // PART B: Rewrite ids. Order: leaf tables trước, imported_files cuối vì
        // ON UPDATE CASCADE chỉ có sau PART A.
        rewrite_shopee_accounts_ids(&tx)?;
        rewrite_order_items_ids(&tx)?;
        rewrite_fb_ads_ids(&tx)?;
        rewrite_imported_files_ids(&tx)?;

        tx.commit()?;
        Ok(())
    })();

    // Always re-enable FK — init_db_at ALSO apply `PRAGMA foreign_keys=ON`
    // nhưng explicit re-enable ở đây để test helper (migrate_for_tests) cũng
    // restore đúng state.
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    result
}

/// Check FK của raw_shopee_clicks.source_file_id — nếu on_update = CASCADE
/// nghĩa là v13 đã chạy (rebuild schema).
fn raw_shopee_clicks_fk_has_cascade(conn: &Connection) -> Result<bool> {
    let mut stmt = conn.prepare("PRAGMA foreign_key_list(raw_shopee_clicks)")?;
    let found = stmt
        .query_map([], |r| {
            let from: String = r.get(3)?;
            let on_update: String = r.get(5)?;
            Ok((from, on_update))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(found
        .iter()
        .any(|(from, upd)| from == "source_file_id" && upd == "CASCADE"))
}

// =============================================================
// PART A — Table rebuild cho ON UPDATE CASCADE
// =============================================================

fn rebuild_raw_shopee_clicks_with_cascade(tx: &rusqlite::Transaction) -> Result<()> {
    tx.execute_batch(
        "CREATE TABLE raw_shopee_clicks_v13 (
             click_id         TEXT PRIMARY KEY,
             click_time       TEXT NOT NULL,
             region           TEXT,
             sub_id_raw       TEXT,
             sub_id1          TEXT NOT NULL DEFAULT '',
             sub_id2          TEXT NOT NULL DEFAULT '',
             sub_id3          TEXT NOT NULL DEFAULT '',
             sub_id4          TEXT NOT NULL DEFAULT '',
             sub_id5          TEXT NOT NULL DEFAULT '',
             referrer         TEXT,
             day_date         TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
             source_file_id   INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
             shopee_account_id INTEGER
         );
         INSERT INTO raw_shopee_clicks_v13 SELECT
             click_id, click_time, region, sub_id_raw,
             sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
             referrer, day_date, source_file_id,
             NULL AS shopee_account_id
         FROM raw_shopee_clicks;",
    )?;
    // Note: shopee_account_id column có thể chưa tồn tại trong legacy schema.
    // Nếu tồn tại, overwrite dòng SELECT. Kiểm tra trước.
    let has_account_col: bool = {
        let cols: Vec<String> = tx
            .prepare("PRAGMA table_info(raw_shopee_clicks)")?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<std::result::Result<_, _>>()?;
        cols.iter().any(|c| c == "shopee_account_id")
    };
    if has_account_col {
        tx.execute_batch(
            "DELETE FROM raw_shopee_clicks_v13;
             INSERT INTO raw_shopee_clicks_v13 SELECT
                 click_id, click_time, region, sub_id_raw,
                 sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
                 referrer, day_date, source_file_id, shopee_account_id
             FROM raw_shopee_clicks;",
        )?;
    }
    tx.execute_batch(
        "DROP TABLE raw_shopee_clicks;
         ALTER TABLE raw_shopee_clicks_v13 RENAME TO raw_shopee_clicks;
         CREATE INDEX IF NOT EXISTS idx_clicks_day ON raw_shopee_clicks(day_date);
         CREATE INDEX IF NOT EXISTS idx_clicks_subid ON raw_shopee_clicks(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
         CREATE INDEX IF NOT EXISTS idx_clicks_day_subid ON raw_shopee_clicks(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);",
    )?;
    Ok(())
}

fn rebuild_raw_shopee_order_items_with_cascade(tx: &rusqlite::Transaction) -> Result<()> {
    // Dynamically pull column list + drop/recreate giữ nguyên cols để forward
    // compat với v7 MCN + v8/v9 ADD COLUMN. Dùng SELECT * và same order.
    let cols = collect_column_names(tx, "raw_shopee_order_items")?;
    let cols_csv = cols.join(", ");

    // Rebuild với FK update CASCADE.
    let mut ddl = String::from("CREATE TABLE raw_shopee_order_items_v13 (\n");
    for col in &cols {
        ddl.push_str(&format!("    {col} "));
        match col.as_str() {
            "id" => ddl.push_str("INTEGER PRIMARY KEY AUTOINCREMENT"),
            "source_file_id" => ddl.push_str(
                "INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE",
            ),
            "day_date" => {
                ddl.push_str("TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE")
            }
            "order_id" | "checkout_id" | "item_id" => ddl.push_str("TEXT NOT NULL"),
            "model_id" => ddl.push_str("TEXT NOT NULL DEFAULT ''"),
            c if c.starts_with("mcn_") => ddl.push_str("REAL"),
            _ => {
                // Các col còn lại giữ shape cũ — pull type từ pragma.
                let dtype = column_type(tx, "raw_shopee_order_items", col)?;
                ddl.push_str(&dtype);
            }
        }
        ddl.push_str(",\n");
    }
    // UNIQUE giữ nguyên từ schema.
    ddl.push_str("    UNIQUE(checkout_id, item_id, model_id)\n);");
    tx.execute_batch(&ddl)?;

    tx.execute_batch(&format!(
        "INSERT INTO raw_shopee_order_items_v13 ({cols_csv}) SELECT {cols_csv} FROM raw_shopee_order_items;
         DROP TABLE raw_shopee_order_items;
         ALTER TABLE raw_shopee_order_items_v13 RENAME TO raw_shopee_order_items;
         CREATE INDEX IF NOT EXISTS idx_orders_checkout_item ON raw_shopee_order_items(checkout_id, item_id, model_id);
         CREATE INDEX IF NOT EXISTS idx_orders_day ON raw_shopee_order_items(day_date);
         CREATE INDEX IF NOT EXISTS idx_orders_subid ON raw_shopee_order_items(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
         CREATE INDEX IF NOT EXISTS idx_orders_day_subid ON raw_shopee_order_items(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);"
    ))?;
    Ok(())
}

fn rebuild_raw_fb_ads_with_cascade(tx: &rusqlite::Transaction) -> Result<()> {
    let cols = collect_column_names(tx, "raw_fb_ads")?;
    let cols_csv = cols.join(", ");

    let mut ddl = String::from("CREATE TABLE raw_fb_ads_v13 (\n");
    for col in &cols {
        ddl.push_str(&format!("    {col} "));
        match col.as_str() {
            "id" => ddl.push_str("INTEGER PRIMARY KEY AUTOINCREMENT"),
            "source_file_id" => ddl.push_str(
                "INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE",
            ),
            "day_date" => ddl.push_str("TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE"),
            "level" | "name" => ddl.push_str("TEXT NOT NULL"),
            _ => {
                let dtype = column_type(tx, "raw_fb_ads", col)?;
                ddl.push_str(&dtype);
            }
        }
        ddl.push_str(",\n");
    }
    ddl.push_str("    UNIQUE(day_date, level, name)\n);");
    tx.execute_batch(&ddl)?;

    tx.execute_batch(&format!(
        "INSERT INTO raw_fb_ads_v13 ({cols_csv}) SELECT {cols_csv} FROM raw_fb_ads;
         DROP TABLE raw_fb_ads;
         ALTER TABLE raw_fb_ads_v13 RENAME TO raw_fb_ads;
         CREATE INDEX IF NOT EXISTS idx_fb_ads_day ON raw_fb_ads(day_date);
         CREATE INDEX IF NOT EXISTS idx_fb_ads_level ON raw_fb_ads(day_date, level);
         CREATE INDEX IF NOT EXISTS idx_fb_ads_subid ON raw_fb_ads(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
         CREATE INDEX IF NOT EXISTS idx_fb_ads_day_subid ON raw_fb_ads(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);"
    ))?;
    Ok(())
}

fn rebuild_clicks_to_file_with_cascade(tx: &rusqlite::Transaction) -> Result<()> {
    tx.execute_batch(
        "CREATE TABLE clicks_to_file_v13 (
             click_id   TEXT    NOT NULL,
             file_id    INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
             PRIMARY KEY(click_id, file_id)
         );
         INSERT INTO clicks_to_file_v13 SELECT click_id, file_id FROM clicks_to_file;
         DROP TABLE clicks_to_file;
         ALTER TABLE clicks_to_file_v13 RENAME TO clicks_to_file;
         CREATE INDEX IF NOT EXISTS idx_clicks_to_file_file ON clicks_to_file(file_id);",
    )?;
    Ok(())
}

fn rebuild_orders_to_file_with_cascade(tx: &rusqlite::Transaction) -> Result<()> {
    tx.execute_batch(
        "CREATE TABLE orders_to_file_v13 (
             order_item_id INTEGER NOT NULL,
             file_id       INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
             PRIMARY KEY(order_item_id, file_id)
         );
         INSERT INTO orders_to_file_v13 SELECT order_item_id, file_id FROM orders_to_file;
         DROP TABLE orders_to_file;
         ALTER TABLE orders_to_file_v13 RENAME TO orders_to_file;
         CREATE INDEX IF NOT EXISTS idx_orders_to_file_file ON orders_to_file(file_id);",
    )?;
    Ok(())
}

fn rebuild_fb_ads_to_file_with_cascade(tx: &rusqlite::Transaction) -> Result<()> {
    tx.execute_batch(
        "CREATE TABLE fb_ads_to_file_v13 (
             fb_ad_id INTEGER NOT NULL,
             file_id  INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
             PRIMARY KEY(fb_ad_id, file_id)
         );
         INSERT INTO fb_ads_to_file_v13 SELECT fb_ad_id, file_id FROM fb_ads_to_file;
         DROP TABLE fb_ads_to_file;
         ALTER TABLE fb_ads_to_file_v13 RENAME TO fb_ads_to_file;
         CREATE INDEX IF NOT EXISTS idx_fb_ads_to_file_file ON fb_ads_to_file(file_id);",
    )?;
    Ok(())
}

// =============================================================
// PART B — Rewrite ids → content_id
// =============================================================

fn rewrite_shopee_accounts_ids(tx: &rusqlite::Transaction) -> Result<()> {
    let rows: Vec<(i64, String)> = tx
        .prepare("SELECT id, name FROM shopee_accounts")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    for (old_id, name) in rows {
        let new_id = crate::sync_v9::content_id::shopee_account_id(&name);
        if new_id == old_id {
            continue;
        }
        tx.execute(
            "UPDATE shopee_accounts SET id = ? WHERE id = ?",
            rusqlite::params![new_id, old_id],
        )?;
        // Informal refs: raw_shopee_clicks.shopee_account_id,
        //                raw_shopee_order_items.shopee_account_id,
        //                raw_fb_ads.shopee_account_id (nếu có),
        //                imported_files.shopee_account_id.
        for table in [
            "raw_shopee_clicks",
            "raw_shopee_order_items",
            "raw_fb_ads",
            "imported_files",
        ] {
            let has_col = tx_table_has_column(tx, table, "shopee_account_id")?;
            if has_col {
                tx.execute(
                    &format!("UPDATE {table} SET shopee_account_id = ? WHERE shopee_account_id = ?"),
                    rusqlite::params![new_id, old_id],
                )?;
            }
        }
    }
    Ok(())
}

fn rewrite_order_items_ids(tx: &rusqlite::Transaction) -> Result<()> {
    let rows: Vec<(i64, String, String, String)> = tx
        .prepare(
            "SELECT id, checkout_id, item_id, COALESCE(model_id, '') FROM raw_shopee_order_items",
        )?
        .query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    for (old_id, checkout, item, model) in rows {
        let new_id = crate::sync_v9::content_id::order_item_id(&checkout, &item, &model);
        if new_id == old_id {
            continue;
        }
        tx.execute(
            "UPDATE raw_shopee_order_items SET id = ? WHERE id = ?",
            rusqlite::params![new_id, old_id],
        )?;
        // Informal ref: orders_to_file.order_item_id.
        tx.execute(
            "UPDATE orders_to_file SET order_item_id = ? WHERE order_item_id = ?",
            rusqlite::params![new_id, old_id],
        )?;
    }
    Ok(())
}

fn rewrite_fb_ads_ids(tx: &rusqlite::Transaction) -> Result<()> {
    let rows: Vec<(i64, String, String, String)> = tx
        .prepare("SELECT id, day_date, level, name FROM raw_fb_ads")?
        .query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    for (old_id, day_date, level, name) in rows {
        let new_id = crate::sync_v9::content_id::fb_ad_id(&day_date, &level, &name);
        if new_id == old_id {
            continue;
        }
        tx.execute(
            "UPDATE raw_fb_ads SET id = ? WHERE id = ?",
            rusqlite::params![new_id, old_id],
        )?;
        tx.execute(
            "UPDATE fb_ads_to_file SET fb_ad_id = ? WHERE fb_ad_id = ?",
            rusqlite::params![new_id, old_id],
        )?;
    }
    Ok(())
}

fn rewrite_imported_files_ids(tx: &rusqlite::Transaction) -> Result<()> {
    let rows: Vec<(i64, String)> = tx
        .prepare("SELECT id, file_hash FROM imported_files")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    for (old_id, file_hash) in rows {
        let new_id = crate::sync_v9::content_id::imported_file_id(&file_hash);
        if new_id == old_id {
            continue;
        }
        // ON UPDATE CASCADE (vừa add ở Part A) propagate children tự động:
        // raw_shopee_clicks.source_file_id, raw_shopee_order_items.source_file_id,
        // raw_fb_ads.source_file_id, clicks_to_file.file_id,
        // orders_to_file.file_id, fb_ads_to_file.file_id.
        tx.execute(
            "UPDATE imported_files SET id = ? WHERE id = ?",
            rusqlite::params![new_id, old_id],
        )?;
    }
    Ok(())
}

// =============================================================
// Helpers
// =============================================================

fn collect_column_names(tx: &rusqlite::Transaction, table: &str) -> Result<Vec<String>> {
    let cols: Vec<String> = tx
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<std::result::Result<_, _>>()?;
    Ok(cols)
}

fn column_type(tx: &rusqlite::Transaction, table: &str, col: &str) -> Result<String> {
    let t: Option<String> = tx
        .query_row(
            &format!("SELECT type FROM pragma_table_info('{table}') WHERE name = ?"),
            [col],
            |r| r.get(0),
        )
        .ok();
    Ok(t.unwrap_or_else(|| "TEXT".to_string()))
}

fn tx_table_has_column(tx: &rusqlite::Transaction, table: &str, col: &str) -> Result<bool> {
    let cols = collect_column_names(tx, table)?;
    Ok(cols.iter().any(|c| c == col))
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

/// v11 — Sync v9 infrastructure: thêm các bảng cần cho per-table incremental
/// delta sync. Additive only (Phase 1): không động sync_state v8 + triggers cũ
/// để sync engine cũ vẫn compile & chạy được đến khi Phase 2 thay xong.
///
/// 3 bảng mới:
/// - `sync_cursor_state` — per-table high-water-mark cho push/pull
/// - `sync_manifest_state` — singleton: manifest etag + snapshot pointer + fresh-install flag
/// - `sync_event_log` — ring buffer debug events (5000 entries max)
///
/// Idempotent: dùng CREATE TABLE IF NOT EXISTS, INSERT OR IGNORE.
fn migrate_v11_sync_infra(conn: &Connection) -> Result<()> {
    // Singleton manifest state. fresh_install_pending=1 ngăn push empty đè
    // remote trong bootstrap (rule giữ data).
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sync_manifest_state (
             id                            INTEGER PRIMARY KEY CHECK (id = 1),
             last_remote_etag              TEXT,
             last_pulled_manifest_clock_ms INTEGER NOT NULL DEFAULT 0,
             last_snapshot_key             TEXT,
             last_snapshot_clock_ms        INTEGER NOT NULL DEFAULT 0,
             fresh_install_pending         INTEGER NOT NULL DEFAULT 0
         );
         INSERT OR IGNORE INTO sync_manifest_state (id) VALUES (1);

         CREATE TABLE IF NOT EXISTS sync_cursor_state (
             table_name            TEXT PRIMARY KEY,
             last_uploaded_cursor  TEXT NOT NULL DEFAULT '0',
             last_pulled_cursor    TEXT NOT NULL DEFAULT '0',
             last_uploaded_hash    TEXT,
             updated_at            TEXT NOT NULL
         );

         CREATE TABLE IF NOT EXISTS sync_event_log (
             event_id     INTEGER PRIMARY KEY AUTOINCREMENT,
             ts           TEXT NOT NULL,
             fingerprint  TEXT NOT NULL,
             kind         TEXT NOT NULL,
             ctx_json     TEXT NOT NULL,
             uploaded_at  TEXT
         );
         CREATE INDEX IF NOT EXISTS idx_sync_event_log_pending
             ON sync_event_log(uploaded_at) WHERE uploaded_at IS NULL;
         CREATE INDEX IF NOT EXISTS idx_sync_event_log_ts
             ON sync_event_log(ts);
         CREATE INDEX IF NOT EXISTS idx_sync_event_log_kind
             ON sync_event_log(kind);",
    )?;

    // Seed cursor state cho 10 table syncable. updated_at=now để distinguish
    // "chưa init" (không có row) vs "đã init và chưa có gì push" (row có cursor='0').
    let now = chrono::Utc::now().to_rfc3339();
    let tables = [
        "imported_files",
        "raw_shopee_clicks",
        "raw_shopee_order_items",
        "raw_fb_ads",
        "clicks_to_file",
        "orders_to_file",
        "fb_ads_to_file",
        "manual_entries",
        "shopee_accounts",
        "tombstones",
    ];
    for table in tables {
        conn.execute(
            "INSERT OR IGNORE INTO sync_cursor_state
                 (table_name, last_uploaded_cursor, last_pulled_cursor, updated_at)
             VALUES (?, '0', '0', ?)",
            rusqlite::params![table, now],
        )?;
    }

    conn.execute(
        "INSERT OR IGNORE INTO _schema_version(version, applied_at)
         VALUES(11, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        [],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
        // Chạy migrate() để tạo partial unique index v10 (schema.sql không tạo
        // vì reverted_at là cột v10 — phải ALTER TABLE ADD COLUMN trước).
        migrate(&conn).unwrap();
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
    fn v11_sync_infra_tables_created() {
        let conn = test_conn();
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        for name in ["sync_cursor_state", "sync_manifest_state", "sync_event_log"] {
            assert!(tables.iter().any(|t| t == name), "v11 missing table {name}");
        }

        // Singleton row cho manifest_state phải được seed.
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sync_manifest_state", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "sync_manifest_state phải có singleton row");

        // Cursor state phải seed cho 10 bảng syncable.
        let cursor_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sync_cursor_state", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cursor_count, 10, "sync_cursor_state phải seed 10 bảng");

        // Verify schema version 11 đã mark.
        let v11_marked: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _schema_version WHERE version = 11",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(v11_marked, 1, "_schema_version phải có entry 11");
    }

    #[test]
    fn v11_migration_idempotent() {
        let conn = test_conn();
        // Chạy migrate() lần 2 — không được fail, không duplicate.
        migrate(&conn).expect("migrate lần 2 không được fail");
        let cursor_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sync_cursor_state", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cursor_count, 10, "idempotent: không duplicate cursor rows");
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

    /// Setup: 2 file A và B đều import cùng click "c1" (trùng data).
    /// Revert A → click "c1" GIỮ NGUYÊN vì B vẫn link tới nó.
    /// Revert B thêm → click "c1" mới bị xóa.
    #[test]
    fn revert_preserves_rows_linked_by_another_file() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-17', 'now')",
            [],
        )
        .unwrap();

        // File A
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('A.csv', 'shopee_clicks', 'now', 'hashA', '2026-04-17')",
            [],
        )
        .unwrap();
        let a_id = conn.last_insert_rowid();

        // File B
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('B.csv', 'shopee_clicks', 'now', 'hashB', '2026-04-17')",
            [],
        )
        .unwrap();
        let b_id = conn.last_insert_rowid();

        // Click c1 tồn tại 1 lần trong raw (source_file_id = A) nhưng MAPPING cả A và B.
        conn.execute(
            "INSERT INTO raw_shopee_clicks(click_id, click_time, day_date, source_file_id)
             VALUES('c1', 'now', '2026-04-17', ?)",
            [a_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c1', ?)",
            [a_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c1', ?)",
            [b_id],
        )
        .unwrap();

        // Simulate revert A: xóa mapping file_id=A + xóa raw orphan.
        conn.execute("DELETE FROM clicks_to_file WHERE file_id = ?", [a_id])
            .unwrap();
        let orphan_deleted = conn
            .execute(
                "DELETE FROM raw_shopee_clicks
                 WHERE click_id NOT IN (SELECT click_id FROM clicks_to_file)",
                [],
            )
            .unwrap();

        // c1 CÒN vì B vẫn link.
        assert_eq!(orphan_deleted, 0, "revert A không được xóa click c1");
        let c1_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM raw_shopee_clicks WHERE click_id='c1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(c1_count, 1, "c1 phải còn sau revert A (B vẫn link)");

        // Revert B tiếp.
        conn.execute("DELETE FROM clicks_to_file WHERE file_id = ?", [b_id])
            .unwrap();
        conn.execute(
            "DELETE FROM raw_shopee_clicks
             WHERE click_id NOT IN (SELECT click_id FROM clicks_to_file)",
            [],
        )
        .unwrap();
        let c1_count2: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM raw_shopee_clicks WHERE click_id='c1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(c1_count2, 0, "c1 phải mất sau khi revert B (hết link)");
    }

    /// Partial unique index `idx_imported_hash_active` cho phép nhiều row cùng
    /// file_hash nếu chỉ 1 row active (reverted_at IS NULL).
    #[test]
    fn partial_unique_hash_allows_reimport_after_revert() {
        let conn = test_conn();

        // File A active.
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('A.csv', 'shopee_clicks', 'now', 'hash-X', '2026-04-17')",
            [],
        )
        .unwrap();

        // File A lần 2 cùng hash — reject (vẫn active).
        let dup = conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('A-dup.csv', 'shopee_clicks', 'now', 'hash-X', '2026-04-17')",
            [],
        );
        assert!(dup.is_err(), "dedup phải chặn 2 file cùng hash cùng active");

        // Mark A as reverted.
        conn.execute(
            "UPDATE imported_files SET reverted_at='now' WHERE file_hash='hash-X'",
            [],
        )
        .unwrap();

        // Giờ re-import A được → partial unique cho phép vì row cũ reverted.
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('A-reimport.csv', 'shopee_clicks', 'now', 'hash-X', '2026-04-17')",
            [],
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

        let total_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM imported_files WHERE file_hash='hash-X'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(total_count, 2, "row cũ reverted giữ lại cho history");
    }

    /// Day delete CASCADE raw row → trigger cleanup mapping atomic.
    /// Ngăn chặn stale mapping sau khi user xóa ngày.
    #[test]
    fn day_delete_cascades_cleanup_click_mapping() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-17', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('A.csv', 'shopee_clicks', 'now', 'hA', '2026-04-17')",
            [],
        )
        .unwrap();
        let file_id = conn.last_insert_rowid();
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

        // Xóa day → CASCADE raw_shopee_clicks → trigger fire → mapping xóa.
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

    /// Scenario cốt lõi của user: 2 file trùng 1 dòng order.
    /// File A import → file B UPSERT ghi đè `source_file_id`. Revert A thì
    /// KHÔNG được hard-delete file A (cleanup orphan phải dùng mapping, không
    /// dựa source_file_id).
    #[test]
    fn upsert_overwrites_source_file_id_but_mapping_preserves_history() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-17', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('A.csv', 'shopee_commission', 'now', 'hA', '2026-04-17')",
            [],
        )
        .unwrap();
        let a_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('B.csv', 'shopee_commission', 'now', 'hB', '2026-04-17')",
            [],
        )
        .unwrap();
        let b_id = conn.last_insert_rowid();

        // File A import order X.
        conn.execute(
            "INSERT INTO raw_shopee_order_items
               (order_id, checkout_id, item_id, order_status, order_time, day_date, source_file_id)
             VALUES ('o1', 'chk1', 'it1', 'Đang chờ', 'now', '2026-04-17', ?)",
            [a_id],
        )
        .unwrap();
        let order_row_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO orders_to_file(order_item_id, file_id) VALUES(?, ?)",
            [order_row_id, a_id],
        )
        .unwrap();

        // File B UPSERT → ghi đè source_file_id thành B. Mapping thêm (row, B).
        conn.execute(
            "UPDATE raw_shopee_order_items SET order_status='Hoàn thành',
                source_file_id = ? WHERE id = ?",
            [b_id, order_row_id],
        )
        .unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO orders_to_file(order_item_id, file_id) VALUES(?, ?)",
            [order_row_id, b_id],
        )
        .unwrap();

        // Check: source_file_id giờ = B, nhưng mapping có cả A và B.
        let sfi: i64 = conn
            .query_row(
                "SELECT source_file_id FROM raw_shopee_order_items WHERE id = ?",
                [order_row_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(sfi, b_id, "UPSERT ghi đè source_file_id");
        let map_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM orders_to_file WHERE order_item_id = ?",
                [order_row_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(map_count, 2, "mapping phải có cả A và B");

        // Cleanup orphan qua MAPPING (chuẩn v10). File A phải còn (mapping có entry).
        cleanup_orphan_imported_files(&conn).unwrap();
        let a_still: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM imported_files WHERE id = ?",
                [a_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            a_still, 1,
            "file A KHÔNG được orphan (mapping còn), cleanup phải dựa mapping"
        );

        // Simulate revert A: xóa mapping(A), raw row còn (mapping(B) còn).
        conn.execute(
            "DELETE FROM orders_to_file WHERE file_id = ?",
            [a_id],
        )
        .unwrap();
        conn.execute(
            "DELETE FROM raw_shopee_order_items
             WHERE id NOT IN (SELECT order_item_id FROM orders_to_file)",
            [],
        )
        .unwrap();

        let row_still: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM raw_shopee_order_items WHERE id = ?",
                [order_row_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(row_still, 1, "raw row phải còn sau revert A (mapping B giữ)");
    }

    /// Commission report scenario: file X (xuất ngày 10) + file Y (xuất ngày 15,
    /// chứa đơn cũ đã update status + đơn mới). Revert Y phải GIỮ đơn cũ
    /// (vì có ở X) + XÓA đơn mới (chỉ ở Y).
    #[test]
    fn commission_report_revert_preserves_shared_orders() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-10', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-12', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('X.csv', 'shopee_commission', 'now', 'hX', '2026-04-10')",
            [],
        )
        .unwrap();
        let x_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('Y.csv', 'shopee_commission', 'now', 'hY', '2026-04-10')",
            [],
        )
        .unwrap();
        let y_id = conn.last_insert_rowid();

        // File X imports order_old (2026-04-10).
        conn.execute(
            "INSERT INTO raw_shopee_order_items
               (order_id, checkout_id, item_id, order_time, day_date, source_file_id)
             VALUES ('old', 'chk-old', 'it', 'now', '2026-04-10', ?)",
            [x_id],
        )
        .unwrap();
        let old_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO orders_to_file(order_item_id, file_id) VALUES(?, ?)",
            [old_id, x_id],
        )
        .unwrap();

        // File Y re-imports order_old (status update) + adds order_new.
        conn.execute(
            "UPDATE raw_shopee_order_items SET source_file_id = ? WHERE id = ?",
            [y_id, old_id],
        )
        .unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO orders_to_file(order_item_id, file_id) VALUES(?, ?)",
            [old_id, y_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO raw_shopee_order_items
               (order_id, checkout_id, item_id, order_time, day_date, source_file_id)
             VALUES ('new', 'chk-new', 'it', 'now', '2026-04-12', ?)",
            [y_id],
        )
        .unwrap();
        let new_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO orders_to_file(order_item_id, file_id) VALUES(?, ?)",
            [new_id, y_id],
        )
        .unwrap();

        // Revert Y: xóa mapping(Y) + orphan raw.
        conn.execute(
            "DELETE FROM orders_to_file WHERE file_id = ?",
            [y_id],
        )
        .unwrap();
        conn.execute(
            "DELETE FROM raw_shopee_order_items
             WHERE id NOT IN (SELECT order_item_id FROM orders_to_file)",
            [],
        )
        .unwrap();

        // old: mapping(X) còn → giữ. new: chỉ Y → mất.
        let old_still: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM raw_shopee_order_items WHERE order_id='old'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(old_still, 1, "order 'old' phải giữ (shared với X)");
        let new_still: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM raw_shopee_order_items WHERE order_id='new'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(new_still, 0, "order 'new' phải mất (chỉ ở Y)");
    }

    /// Mapping tables phải CASCADE khi hard-delete imported_files (cleanup orphan).
    #[test]
    fn mapping_cascades_on_imported_files_delete() {
        let conn = test_conn();
        conn.execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-17', 'now')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO imported_files(filename, kind, imported_at, file_hash, day_date)
             VALUES('x.csv', 'shopee_clicks', 'now', 'h', '2026-04-17')",
            [],
        )
        .unwrap();
        let file_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c1', ?)",
            [file_id],
        )
        .unwrap();

        // Hard delete imported_files row → mapping CASCADE.
        conn.execute(
            "DELETE FROM imported_files WHERE id = ?",
            [file_id],
        )
        .unwrap();
        let map_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM clicks_to_file", [], |r| r.get(0))
            .unwrap();
        assert_eq!(map_count, 0, "mapping phải CASCADE theo imported_files");
    }
}
