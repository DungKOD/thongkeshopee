//! Admin-only: xem DB của user khác (Phase B).
//!
//! Flow: admin gọi `admin_view_user_db(target_uid, target_local_part, target_email)` →
//! Worker `/admin/download?uid=<uid>` verify admin + trả base64 → Rust decode
//! và ghi vào `app_data_dir/admin_view/<uid>.db` → mở connection read-only →
//! swap `DbState` sang connection mới (connection cũ drop, handles release).
//!
//! Khi admin exit → `admin_exit_view_user_db` reopen DB gốc (`resolve_db_path`)
//! ở chế độ read-write với PRAGMA + migrate như bình thường → swap back.
//!
//! Safety:
//! - Mở read-only (`SQLITE_OPEN_READ_ONLY`) → mọi mutation command sẽ fail
//!   với `SQLITE_READONLY` nếu FE không tắt nút — 2 lớp bảo vệ.
//! - Cloud sync PHẢI được tắt ở FE (`useCloudSync({ enabled: false })`) trong
//!   lúc view mode — nếu không, dirty flag của DB user khác sẽ trigger upload
//!   lên R2 của admin.
//! - AdminViewState lưu trong Tauri state (memory). Restart app → state mất
//!   → `db::setup` load DB gốc như bình thường, không cần cleanup logic.

use std::path::PathBuf;
use std::sync::Mutex;

use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use tauri::{AppHandle, Manager, State};
use tokio::fs;

use super::sync::zstd_decompress;
use super::sync_client;
use super::{CmdError, CmdResult};
use crate::db::{resolve_active_db_path, DbState};

/// Tauri managed state — track user đang được admin xem.
/// `None` = chế độ bình thường (DB của chính admin).
pub struct AdminViewState(pub Mutex<Option<AdminViewInfo>>);

/// Metadata user đang được xem. FE dùng để render banner.
#[derive(Debug, Clone, Serialize)]
pub struct AdminViewInfo {
    pub uid: String,
    pub email: Option<String>,
    pub local_part: String,
    pub db_path: String,
    pub size_bytes: u64,
    pub last_modified_ms: i64,
    pub entered_at_ms: i64,
    /// Path DB của chính admin TRƯỚC khi swap sang snapshot — dùng ở
    /// `admin_exit_view_user_db` để reopen đúng folder (multi-tenant layout
    /// `users/{admin_uid}/thongkeshopee.db`, không hardcode root path).
    #[serde(skip)]
    pub admin_db_path_backup: PathBuf,
}

/// Resolve folder chứa snapshot DB của user đang được admin xem.
/// `app_data_dir/admin_view/`. Mỗi user 1 file `<uid>.db`.
fn admin_view_dir(app: &AppHandle) -> CmdResult<PathBuf> {
    let base = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?;
    Ok(base.join("admin_view"))
}

/// Admin-only: download DB của user target về local + swap `DbState` sang
/// read-only connection. Trả về metadata để FE hiển thị banner.
///
/// Worker verify admin qua `ADMIN_UIDS` secret (whitelist Firebase UID),
/// 403 nếu caller không phải admin, 404 nếu user target chưa có backup trên R2.
#[tauri::command]
pub async fn admin_view_user_db(
    app: AppHandle,
    db: State<'_, DbState>,
    view_state: State<'_, AdminViewState>,
    sync_api_url: String,
    id_token: String,
    target_uid: String,
    target_local_part: String,
    target_email: Option<String>,
) -> CmdResult<AdminViewInfo> {
    // 1. Gọi Worker /admin/download?uid=<uid> — verify admin + trả raw zstd bytes.
    let (compressed, size_bytes, last_modified_ms) =
        sync_client::admin_download(&sync_api_url, &id_token, &target_uid).await?;

    // R2 lưu zstd bytes. Decompress trước khi write làm SQLite file.
    let bytes = zstd_decompress(&compressed)?;
    eprintln!(
        "admin_view download: payload={} KB → sqlite={} KB",
        compressed.len() / 1024,
        bytes.len() / 1024,
    );

    // 2. Ghi file ra disk.
    let dir = admin_view_dir(&app)?;
    fs::create_dir_all(&dir).await.map_err(CmdError::from)?;
    let target_path = dir.join(format!("{target_uid}.db"));

    // Xóa WAL/SHM cũ của lần view trước (nếu có) — tránh SQLite đọc nhầm.
    for ext in &["db-wal", "db-shm"] {
        let _ = fs::remove_file(target_path.with_extension(ext)).await;
    }
    fs::write(&target_path, &bytes).await.map_err(CmdError::from)?;

    // 3. Mở connection read-only. `SQLITE_OPEN_READ_ONLY` flag đảm bảo mọi
    //    statement INSERT/UPDATE/DELETE fail với SQLITE_READONLY — lớp bảo vệ
    //    cuối cùng phòng khi FE lộ nút mutation.
    let new_conn = Connection::open_with_flags(
        &target_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|e| CmdError::msg(format!("open RO: {e}")))?;

    // PRAGMA foreign_keys OK với read-only; journal_mode không đổi được nhưng
    // không cần (không ghi). query_only bật cho chắc.
    new_conn
        .execute_batch("PRAGMA query_only = ON;")
        .map_err(CmdError::from)?;

    // 4. Swap DbState — connection cũ drop khi out-of-scope. Lưu path DB
    //    của admin TRƯỚC khi swap để `admin_exit` reopen đúng (không hardcode
    //    root path, compatible với layout `users/{uid}/`).
    let admin_db_path_backup: PathBuf = {
        let mut slot = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let backup = resolve_active_db_path(&slot)
            .map_err(|e| CmdError::msg(e.to_string()))?;
        *slot = new_conn;
        backup
    };

    // 5. Ghi AdminViewState.
    let info = AdminViewInfo {
        uid: target_uid,
        email: target_email,
        local_part: target_local_part,
        db_path: target_path.to_string_lossy().into_owned(),
        size_bytes,
        last_modified_ms,
        entered_at_ms: now_ms(),
        admin_db_path_backup,
    };
    {
        let mut slot = view_state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        *slot = Some(info.clone());
    }

    Ok(info)
}

/// Exit admin-view mode — reopen DB gốc read-write, swap lại, clear state.
#[tauri::command]
pub async fn admin_exit_view_user_db(
    db: State<'_, DbState>,
    view_state: State<'_, AdminViewState>,
) -> CmdResult<()> {
    // Lấy path DB admin đã lưu ở lúc enter. Nếu không có → không thể exit clean
    // (fallback không safe với layout per-user — user phải restart app).
    let path = {
        let slot = view_state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        slot.as_ref()
            .map(|info| info.admin_db_path_backup.clone())
            .ok_or_else(|| CmdError::msg("Không ở chế độ admin-view, không có path để reopen"))?
    };

    // Open DB gốc + apply PRAGMA như `db::init_db` (không migrate — đã làm lúc startup).
    let new_conn = Connection::open(&path)
        .map_err(|e| CmdError::msg(format!("open rw: {e}")))?;
    new_conn
        .execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA synchronous = NORMAL;
             PRAGMA temp_store = MEMORY;",
        )
        .map_err(CmdError::from)?;

    {
        let mut slot = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        *slot = new_conn;
    }
    {
        let mut slot = view_state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        *slot = None;
    }
    Ok(())
}

/// FE query: đang ở admin-view mode nào. `None` = bình thường.
#[tauri::command]
pub fn admin_view_state_get(
    view_state: State<'_, AdminViewState>,
) -> CmdResult<Option<AdminViewInfo>> {
    let slot = view_state
        .0
        .lock()
        .map_err(|_| CmdError::LockPoisoned)?;
    Ok(slot.clone())
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
