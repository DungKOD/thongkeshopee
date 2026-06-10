//! Export / Import toàn bộ SQLite database.
//!
//! - `export_db`: backup DB đang chạy ra file do user chọn (WAL-safe).
//! - `import_db`: nhận file `.db` mới, validate schema, thay thế, restart app.

use std::path::PathBuf;

use rusqlite::{backup::Backup, Connection};
use tauri::State;

use crate::db::{resolve_active_db_path, DbState};

use super::{CmdError, CmdResult};

/// Backup DB đang chạy sang `dest_path` do frontend chọn qua dialog.
/// Dùng SQLite Backup API — an toàn với WAL mode và concurrent reads.
#[tauri::command]
pub fn export_db(db: State<'_, DbState>, dest_path: String) -> CmdResult<()> {
    let dest = PathBuf::from(&dest_path);

    if let Some(parent) = dest.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| CmdError::msg(format!("không tạo được thư mục đích: {e}")))?;
        }
    }

    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut dest_conn = Connection::open(&dest)
        .map_err(|e| CmdError::msg(format!("không tạo được file export: {e}")))?;

    let backup = Backup::new(&conn, &mut dest_conn)
        .map_err(|e| CmdError::msg(format!("không khởi tạo được backup: {e}")))?;

    backup
        .run_to_completion(100, std::time::Duration::from_millis(0), None)
        .map_err(|e| CmdError::msg(format!("backup thất bại: {e}")))?;

    Ok(())
}

/// Validate file `.db` do user chọn, thay thế DB hiện tại, trả Ok(()).
/// Frontend gọi `window.location.reload()` sau khi nhận Ok — không dùng
/// `app.restart()` vì tauri_plugin_single_instance gây race condition
/// (process mới bị detect là "second instance" → tự thoát).
#[tauri::command]
pub fn import_db(db: State<'_, DbState>, src_path: String) -> CmdResult<()> {
    let src = PathBuf::from(&src_path);

    if !src.exists() {
        return Err(CmdError::msg("File không tồn tại"));
    }

    validate_import_file(&src)?;

    // Lock mutex, lấy path, copy, mở connection mới, swap — tất cả trong 1
    // critical section để không có query nào chạy giữa chừng.
    let mut conn_guard = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;

    let dest_path = resolve_active_db_path(&conn_guard)
        .map_err(|e| CmdError::msg(e.to_string()))?;

    std::fs::copy(&src, &dest_path)
        .map_err(|e| CmdError::msg(format!("không copy được file DB: {e}")))?;

    // Mở connection mới trên file vừa copy, apply PRAGMA + schema (idempotent).
    let new_conn = crate::db::init_db_at(&dest_path)
        .map_err(|e| CmdError::msg(format!("không mở được DB mới: {e}")))?;

    *conn_guard = new_conn;

    Ok(())
}

/// Kiểm tra file SQLite hợp lệ và là DB của ThongKeShopee.
///
/// Chỉ require các bảng "fingerprint" tồn tại từ phiên bản đầu tiên
/// (`days` + `raw_shopee_clicks` + `raw_shopee_order_items`) — đủ để chứng
/// minh đây là DB của app, không phải file SQLite ngẫu nhiên.
///
/// Các bảng được thêm ở version sau (`shopee_accounts`, `manual_entries`,
/// `app_settings`, `imported_files`, `raw_fb_ads`, `raw_fb_ads_hierarchy`, các
/// mapping table) KHÔNG check ở đây — sau khi `import_db` copy file,
/// `init_db_at` sẽ chạy `schema.sql` (toàn bộ `CREATE TABLE IF NOT EXISTS`)
/// để tự tạo bảng thiếu. Cột mới trên bảng cũ (vd `shopee_account_id`) đều
/// nullable nên data cũ không bị break. Cho phép import DB từ các bản cũ
/// (v0.8.x trở xuống) mà không cần migrate tay.
fn validate_import_file(path: &PathBuf) -> CmdResult<()> {
    let conn = Connection::open(path)
        .map_err(|e| CmdError::msg(format!("không mở được file DB: {e}")))?;

    let fingerprint_tables = [
        "days",
        "raw_shopee_clicks",
        "raw_shopee_order_items",
    ];

    let existing: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table'")
        .map_err(|e| CmdError::msg(e.to_string()))?
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| CmdError::msg(e.to_string()))?
        .filter_map(|r| r.ok())
        .collect();

    for table in fingerprint_tables {
        if !existing.iter().any(|t| t == table) {
            return Err(CmdError::msg(format!(
                "File DB không phải của ThongKeShopee (thiếu bảng '{table}'). \
                 Hãy chọn file .db được Export từ chính app này."
            )));
        }
    }

    Ok(())
}
