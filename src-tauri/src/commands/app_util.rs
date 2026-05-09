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
