//! Google Drive sync commands qua Apps Script Web App proxy.
//!
//! Frontend lấy Firebase ID token từ JS SDK, pass vào các command dưới đây.
//! Apps Script verify token → thao tác Drive dưới tài khoản owner.

use std::path::PathBuf;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Manager, State};
use tokio::fs;

use super::{CmdError, CmdResult};
use crate::db::DbState;

/// Timeout mặc định cho mọi HTTP call tới Apps Script.
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Serialize, Deserialize)]
struct AppsScriptRequest<'a> {
    action: &'a str,
    #[serde(rename = "idToken")]
    id_token: &'a str,
    #[serde(rename = "base64Data", skip_serializing_if = "Option::is_none")]
    base64_data: Option<String>,
    #[serde(rename = "mtimeMs", skip_serializing_if = "Option::is_none")]
    mtime_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AppsScriptResponse {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    code: Option<u16>,

    #[serde(default, rename = "fileId")]
    file_id: Option<String>,
    #[serde(default, rename = "sizeBytes")]
    size_bytes: Option<u64>,
    #[serde(default, rename = "lastModified")]
    last_modified: Option<i64>,
    #[serde(default)]
    existed: Option<bool>,
    #[serde(default)]
    exists: Option<bool>,
    #[serde(default, rename = "base64Data")]
    base64_data: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DriveCheckResult {
    pub existed: bool,
    pub file_id: String,
    pub size_bytes: u64,
    pub last_modified_ms: i64,
}

#[derive(Debug, Serialize)]
pub struct DriveMetadataResult {
    pub exists: bool,
    pub file_id: Option<String>,
    pub size_bytes: Option<u64>,
    pub last_modified_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct DriveUploadResult {
    pub file_id: String,
    pub size_bytes: u64,
    pub last_modified_ms: i64,
}

#[derive(Debug, Serialize)]
pub struct DriveDownloadResult {
    pub target_path: String,
    pub size_bytes: u64,
    pub last_modified_ms: i64,
}

/// Gọi Apps Script Web App với action + id_token, trả về response đã parse.
/// Raise lỗi nếu HTTP non-2xx hoặc `ok: false`.
async fn call_apps_script(
    url: &str,
    req: AppsScriptRequest<'_>,
) -> CmdResult<AppsScriptResponse> {
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(CmdError::from)?;

    let res = client
        .post(url)
        .json(&req)
        .send()
        .await
        .map_err(CmdError::from)?;

    let status = res.status();
    let text = res.text().await.map_err(CmdError::from)?;

    if !status.is_success() {
        return Err(CmdError::msg(format!(
            "Apps Script HTTP {}: {}",
            status.as_u16(),
            text
        )));
    }

    let parsed: AppsScriptResponse = serde_json::from_str(&text)
        .map_err(|e| CmdError::msg(format!("parse response: {e} — body: {text}")))?;

    if !parsed.ok {
        let code = parsed.code.unwrap_or(500);
        let msg = parsed.error.unwrap_or_else(|| "unknown".into());
        return Err(CmdError::msg(format!("Apps Script {}: {}", code, msg)));
    }
    Ok(parsed)
}

/// Kiểm tra DB file của user có trên Drive chưa. Nếu chưa, tạo empty file.
#[tauri::command]
pub async fn drive_check_or_create(
    apps_script_url: String,
    id_token: String,
) -> CmdResult<DriveCheckResult> {
    let res = call_apps_script(
        &apps_script_url,
        AppsScriptRequest {
            action: "checkOrCreate",
            id_token: &id_token,
            base64_data: None,
            mtime_ms: None,
        },
    )
    .await?;

    Ok(DriveCheckResult {
        existed: res.existed.unwrap_or(false),
        file_id: res
            .file_id
            .ok_or_else(|| CmdError::msg("missing fileId"))?,
        size_bytes: res.size_bytes.unwrap_or(0),
        last_modified_ms: res.last_modified.unwrap_or(0),
    })
}

/// Metadata-only check: có file chưa, kích thước, mtime. Không tạo mới.
#[tauri::command]
pub async fn drive_metadata(
    apps_script_url: String,
    id_token: String,
) -> CmdResult<DriveMetadataResult> {
    let res = call_apps_script(
        &apps_script_url,
        AppsScriptRequest {
            action: "metadata",
            id_token: &id_token,
            base64_data: None,
            mtime_ms: None,
        },
    )
    .await?;

    Ok(DriveMetadataResult {
        exists: res.exists.unwrap_or(false),
        file_id: res.file_id,
        size_bytes: res.size_bytes,
        last_modified_ms: res.last_modified,
    })
}

/// Upload snapshot DB lên Drive. Dùng VACUUM INTO để tạo file consistent
/// (không đụng WAL của DB đang live).
#[tauri::command]
pub async fn drive_upload_db(
    app: AppHandle,
    db: State<'_, DbState>,
    apps_script_url: String,
    id_token: String,
) -> CmdResult<DriveUploadResult> {
    let snapshot_path = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?
        .join("thongkeshopee.backup.db");

    // Xóa snapshot cũ nếu còn lại từ lần trước thất bại.
    let _ = fs::remove_file(&snapshot_path).await;

    // VACUUM INTO — SQLite tạo clean copy không có WAL.
    {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let path_str = snapshot_path
            .to_str()
            .ok_or_else(|| CmdError::msg("snapshot path không phải UTF-8"))?;
        conn.execute("VACUUM INTO ?1", params![path_str])
            .map_err(CmdError::from)?;
    }

    let bytes = fs::read(&snapshot_path).await.map_err(CmdError::from)?;
    let mtime_ms = now_ms();
    let base64 = BASE64.encode(&bytes);

    let res = call_apps_script(
        &apps_script_url,
        AppsScriptRequest {
            action: "upload",
            id_token: &id_token,
            base64_data: Some(base64),
            mtime_ms: Some(mtime_ms),
        },
    )
    .await?;

    // Cleanup snapshot sau khi upload xong.
    let _ = fs::remove_file(&snapshot_path).await;

    Ok(DriveUploadResult {
        file_id: res
            .file_id
            .ok_or_else(|| CmdError::msg("missing fileId"))?,
        size_bytes: res.size_bytes.unwrap_or(bytes.len() as u64),
        last_modified_ms: res.last_modified.unwrap_or(mtime_ms),
    })
}

/// Download DB file từ Drive, ghi vào `pending_db_path` trong app_data_dir.
/// KHÔNG ghi đè DB đang live — frontend phải prompt user restart app để apply.
#[tauri::command]
pub async fn drive_download_db(
    app: AppHandle,
    apps_script_url: String,
    id_token: String,
) -> CmdResult<DriveDownloadResult> {
    let res = call_apps_script(
        &apps_script_url,
        AppsScriptRequest {
            action: "download",
            id_token: &id_token,
            base64_data: None,
            mtime_ms: None,
        },
    )
    .await?;

    let base64 = res
        .base64_data
        .ok_or_else(|| CmdError::msg("missing base64Data"))?;
    let bytes = BASE64
        .decode(base64.as_bytes())
        .map_err(|e| CmdError::msg(format!("base64 decode: {e}")))?;

    let target = pending_db_path(&app)?;
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).await.map_err(CmdError::from)?;
    }
    fs::write(&target, &bytes).await.map_err(CmdError::from)?;

    Ok(DriveDownloadResult {
        target_path: target.to_string_lossy().into_owned(),
        size_bytes: bytes.len() as u64,
        last_modified_ms: res.last_modified.unwrap_or_else(now_ms),
    })
}

/// Apply pending DB file (từ `drive_download_db`) — rename thành DB chính.
/// Gọi khi app đang ở trạng thái an toàn (startup trước khi init DB).
#[tauri::command]
pub async fn drive_apply_pending(app: AppHandle) -> CmdResult<bool> {
    let pending = pending_db_path(&app)?;
    if !pending.exists() {
        return Ok(false);
    }
    let live = crate::db::resolve_db_path(&app)
        .map_err(|e| CmdError::msg(format!("resolve db path: {e}")))?;

    // Backup live DB (nếu có) sang .pre-restore để user rollback nếu cần.
    if live.exists() {
        let backup = live.with_extension("pre-restore.db");
        let _ = fs::remove_file(&backup).await;
        fs::rename(&live, &backup).await.map_err(CmdError::from)?;
    }

    fs::rename(&pending, &live).await.map_err(CmdError::from)?;
    Ok(true)
}

fn pending_db_path(app: &AppHandle) -> CmdResult<PathBuf> {
    let base = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?;
    Ok(base.join("thongkeshopee.pending.db"))
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
