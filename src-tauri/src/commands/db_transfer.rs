//! Export / Import toàn bộ SQLite database.
//!
//! - `export_db`: backup DB đang chạy ra file do user chọn (WAL-safe).
//! - `import_db`: nhận file `.db` mới, validate schema, thay thế, restart app.

use std::path::{Path, PathBuf};

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
///
/// **Windows mmap quirk:** `init_db_at` set `PRAGMA mmap_size = 256MB` →
/// connection memory-map file đích. `std::fs::copy` sang file đang được mmap
/// fail với `ERROR_USER_MAPPED_FILE` (os error 1224). Bắt buộc phải drop
/// connection cũ TRƯỚC khi copy, sau đó mở lại trên file mới.
#[tauri::command]
pub fn import_db(db: State<'_, DbState>, src_path: String) -> CmdResult<()> {
    let src = PathBuf::from(&src_path);

    if !src.exists() {
        return Err(CmdError::msg("File không tồn tại"));
    }

    validate_import_file(&src)?;

    // Lock mutex, đóng conn cũ, copy, mở conn mới, swap — tất cả trong 1
    // critical section để không có query nào chạy giữa chừng.
    let mut conn_guard = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;

    let dest_path = resolve_active_db_path(&conn_guard)
        .map_err(|e| CmdError::msg(e.to_string()))?;

    // Thay connection hiện tại bằng dummy in-memory để drop conn cũ → release
    // mmap section trên dest_path. Giữ invariant "*conn_guard luôn hợp lệ"
    // (không thể drop MutexGuard giữa chừng vì borrow checker).
    let dummy = Connection::open_in_memory()
        .map_err(|e| CmdError::msg(format!("không tạo được dummy conn: {e}")))?;
    let old_conn = std::mem::replace(&mut *conn_guard, dummy);
    drop(old_conn);

    // Best-effort cleanup WAL sidecar — SQLite checkpoint khi close conn,
    // nhưng trên Windows shm có thể vẫn bị lock vài chục ms bởi indexer/AV.
    // File mới sẽ tự tạo wal/shm khi connection mới mở.
    let _ = std::fs::remove_file(sidecar_path(&dest_path, "-wal"));
    let _ = std::fs::remove_file(sidecar_path(&dest_path, "-shm"));

    if let Err(e) = std::fs::copy(&src, &dest_path) {
        // Khôi phục: reopen file cũ tại dest_path. Có 2 case:
        // - dest_path chưa bị ghi (copy fail trước khi mở dest): mở lại OK.
        // - dest_path bị ghi 1 phần (rất hiếm vì copy mở dest trước khi
        //   write): init_db_at có thể fail → conn_guard giữ dummy, user phải
        //   restart app (đã có TaskOutput cảnh báo qua error message).
        if let Ok(restored) = crate::db::init_db_at(&dest_path) {
            *conn_guard = restored;
        }
        return Err(CmdError::msg(format!("không copy được file DB: {e}")));
    }

    // Mở connection mới trên file vừa copy, apply PRAGMA + schema (idempotent).
    let new_conn = crate::db::init_db_at(&dest_path)
        .map_err(|e| CmdError::msg(format!("không mở được DB mới: {e}")))?;

    *conn_guard = new_conn;

    Ok(())
}

/// Build sidecar path cho SQLite WAL/SHM: `thongkeshopee.db` + `-wal` →
/// `thongkeshopee.db-wal`. Append vào filename (KHÔNG dùng `set_extension`
/// vì `set_extension` thay extension hiện tại — `.db` → `.wal` → sai tên).
fn sidecar_path(db_path: &Path, suffix: &str) -> PathBuf {
    let mut name = db_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("thongkeshopee.db")
        .to_string();
    name.push_str(suffix);
    db_path.with_file_name(name)
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
