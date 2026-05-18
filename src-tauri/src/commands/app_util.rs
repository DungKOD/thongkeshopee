//! App utility commands — restart helper + app data paths cho UI.

use serde::Serialize;
use tauri::{AppHandle, Manager, State};

use crate::db::{resolve_active_db_path, resolve_active_imports_dir, DbState};

use super::{CmdError, CmdResult};

/// Đường dẫn data app lưu local — phục vụ UI "Copy path" cho support/debug.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppDataPaths {
    pub app_data_dir: String,
    pub active_db_path: String,
    pub active_imports_dir: String,
}

/// Query đường dẫn data app cho UI. Gọi khi mở SettingsDialog.
#[tauri::command]
pub fn get_app_data_paths(
    app: AppHandle,
    db: State<'_, DbState>,
) -> CmdResult<AppDataPaths> {
    let app_data_dir = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?
        .to_string_lossy()
        .to_string();
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let active_db_path = resolve_active_db_path(&conn)
        .map_err(|e| CmdError::msg(e.to_string()))?
        .to_string_lossy()
        .to_string();
    let active_imports_dir = resolve_active_imports_dir(&conn)
        .map_err(|e| CmdError::msg(e.to_string()))?
        .to_string_lossy()
        .to_string();
    Ok(AppDataPaths {
        app_data_dir,
        active_db_path,
        active_imports_dir,
    })
}

/// Restart app — dùng sau khi user import data hoặc cần reload state.
#[tauri::command]
pub fn restart_app(app: AppHandle) {
    app.restart();
}

/// Xóa toàn bộ data (DB + CSV imports) nhưng giữ nguyên login Firebase.
/// Trả `Ok(())` — frontend tự gọi `window.location.reload()` để reset state.
/// Không dùng `app.restart()` vì tauri_plugin_single_instance gây race: process
/// mới bị detect là "second instance" (process cũ chưa exit kịp) → tự thoát.
#[tauri::command]
pub fn clear_app_data(app: AppHandle, db: State<'_, DbState>) -> CmdResult<()> {
    {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;

        conn.execute_batch(
            "PRAGMA foreign_keys = OFF;
             DELETE FROM fb_ads_hier_to_file;
             DELETE FROM clicks_to_file;
             DELETE FROM orders_to_file;
             DELETE FROM fb_ads_to_file;
             DELETE FROM raw_fb_ads_hierarchy;
             DELETE FROM raw_shopee_clicks;
             DELETE FROM raw_shopee_order_items;
             DELETE FROM raw_fb_ads;
             DELETE FROM imported_files;
             DELETE FROM manual_entries;
             DELETE FROM days;
             DELETE FROM shopee_accounts;
             DELETE FROM app_settings;
             PRAGMA foreign_keys = ON;",
        )
        .map_err(|e| CmdError::msg(e.to_string()))?;

        let id = crate::db::content_id::shopee_account_id(crate::db::DEFAULT_ACCOUNT_NAME);
        let now = crate::db::now_rfc3339_z();
        conn.execute(
            "INSERT OR IGNORE INTO shopee_accounts (id, name, color, created_at) VALUES (?, ?, ?, ?)",
            rusqlite::params![id, crate::db::DEFAULT_ACCOUNT_NAME, "#888888", now],
        )
        .map_err(|e| CmdError::msg(e.to_string()))?;
    }

    let imports_dir = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(e.to_string()))?
        .join(crate::db::IMPORTS_SUBDIR);

    if imports_dir.exists() {
        std::fs::remove_dir_all(&imports_dir).map_err(|e| CmdError::msg(e.to_string()))?;
    }

    Ok(())
}
