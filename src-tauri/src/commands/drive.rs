//! Google Drive sync commands qua Apps Script Web App proxy.
//!
//! Frontend lấy Firebase ID token từ JS SDK, pass vào các command dưới đây.
//! Apps Script verify token → thao tác Drive dưới tài khoản owner.

use std::path::PathBuf;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use md5::{Digest, Md5};
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
    #[serde(skip_serializing_if = "Option::is_none")]
    fingerprint: Option<String>,
    #[serde(rename = "targetLocalPart", skip_serializing_if = "Option::is_none")]
    target_local_part: Option<String>,
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
    #[serde(default)]
    users: Option<Vec<UserListEntry>>,
    #[serde(default)]
    fingerprint: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UserListEntry {
    pub uid: String,
    pub email: Option<String>,
    #[serde(rename = "localPart")]
    pub local_part: Option<String>,
    #[serde(default)]
    pub premium: bool,
    #[serde(default)]
    pub admin: bool,
    #[serde(rename = "expiredAt")]
    pub expired_at: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: Option<String>,
    pub file: Option<UserListFileMeta>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UserListFileMeta {
    #[serde(rename = "fileId")]
    pub file_id: String,
    #[serde(rename = "sizeBytes")]
    pub size_bytes: u64,
    #[serde(rename = "lastModified")]
    pub last_modified: i64,
}

#[derive(Debug, Serialize)]
pub struct DriveCheckResult {
    pub existed: bool,
    pub file_id: String,
    pub size_bytes: u64,
    pub last_modified_ms: i64,
    pub fingerprint: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DriveMetadataResult {
    pub exists: bool,
    pub file_id: Option<String>,
    pub size_bytes: Option<u64>,
    pub last_modified_ms: Option<i64>,
    pub fingerprint: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DriveUploadResult {
    pub file_id: String,
    pub size_bytes: u64,
    pub last_modified_ms: i64,
    pub fingerprint: String,
}

#[derive(Debug, Serialize)]
pub struct SyncState {
    pub dirty: bool,
    pub last_synced_at_ms: Option<i64>,
    pub last_synced_remote_mtime_ms: Option<i64>,
    pub last_error: Option<String>,
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

/// Compute machine fingerprint: MD5(os | hostname | machine-uid) → hex string.
/// Ổn định across restarts trên cùng máy, khác giữa các máy khác nhau.
pub fn machine_fingerprint_raw() -> String {
    let os = std::env::consts::OS;
    let hostname = whoami_hostname().unwrap_or_else(|| "unknown-host".into());
    let machine_id = machine_uid::get().unwrap_or_else(|_| "unknown-uid".into());
    let input = format!("{os}|{hostname}|{machine_id}");
    let digest = Md5::digest(input.as_bytes());
    hex::encode(digest)
}

fn whoami_hostname() -> Option<String> {
    // std::env không có hostname API cross-platform. Thử HOSTNAME / COMPUTERNAME.
    std::env::var("COMPUTERNAME")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
}

/// Tauri command — frontend query fingerprint của máy hiện tại.
#[tauri::command]
pub fn machine_fingerprint() -> String {
    machine_fingerprint_raw()
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
            fingerprint: None,
            target_local_part: None,
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
        fingerprint: res.fingerprint,
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
            fingerprint: None,
            target_local_part: None,
        },
    )
    .await?;

    Ok(DriveMetadataResult {
        exists: res.exists.unwrap_or(false),
        file_id: res.file_id,
        size_bytes: res.size_bytes,
        last_modified_ms: res.last_modified,
        fingerprint: res.fingerprint,
    })
}

/// Đọc trạng thái sync từ bảng `sync_state` (singleton row).
#[tauri::command]
pub async fn sync_state_get(db: State<'_, DbState>) -> CmdResult<SyncState> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let row = conn.query_row(
        "SELECT dirty, last_synced_at_ms, last_synced_remote_mtime_ms, last_error
         FROM sync_state WHERE id = 1",
        [],
        |r| {
            Ok((
                r.get::<_, i64>(0)? != 0,
                r.get::<_, Option<i64>>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        },
    )?;
    Ok(SyncState {
        dirty: row.0,
        last_synced_at_ms: row.1,
        last_synced_remote_mtime_ms: row.2,
        last_error: row.3,
    })
}

/// Ghi lỗi sync vào sync_state.last_error (không đổi dirty). Dùng khi upload fail.
#[tauri::command]
pub async fn sync_state_record_error(
    db: State<'_, DbState>,
    message: String,
) -> CmdResult<()> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "UPDATE sync_state SET last_error = ?1 WHERE id = 1",
        params![message],
    )?;
    Ok(())
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
    // Đồng thời đọc change_id tại thời điểm snapshot để CAS sau upload.
    let change_id_at_snapshot: i64 = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let path_str = snapshot_path
            .to_str()
            .ok_or_else(|| CmdError::msg("snapshot path không phải UTF-8"))?;
        conn.execute("VACUUM INTO ?1", params![path_str])
            .map_err(CmdError::from)?;
        conn.query_row(
            "SELECT change_id FROM sync_state WHERE id = 1",
            [],
            |r| r.get::<_, i64>(0),
        )?
    };

    let bytes = fs::read(&snapshot_path).await.map_err(CmdError::from)?;
    let mtime_ms = now_ms();
    let fingerprint = machine_fingerprint_raw();
    let base64 = BASE64.encode(&bytes);

    let res = call_apps_script(
        &apps_script_url,
        AppsScriptRequest {
            action: "upload",
            id_token: &id_token,
            base64_data: Some(base64),
            mtime_ms: Some(mtime_ms),
            fingerprint: Some(fingerprint.clone()),
            target_local_part: None,
        },
    )
    .await?;

    // Cleanup snapshot sau khi upload xong.
    let _ = fs::remove_file(&snapshot_path).await;

    let remote_mtime = res.last_modified.unwrap_or(mtime_ms);

    // Upload thành công → CAS clear dirty CHỈ KHI change_id chưa tăng từ snapshot.
    // Nếu mutation xảy ra trong lúc upload → change_id > snapshot → dirty giữ = 1
    // để lần upload tiếp theo xử lý. Tránh race: upload cũ KHÔNG chứa mutation sau snapshot.
    {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        conn.execute(
            "UPDATE sync_state
             SET dirty = CASE WHEN change_id > ?1 THEN 1 ELSE 0 END,
                 last_uploaded_change_id = ?1,
                 last_synced_at_ms = ?2,
                 last_synced_remote_mtime_ms = ?3,
                 last_error = NULL
             WHERE id = 1",
            params![change_id_at_snapshot, now_ms(), remote_mtime],
        )?;
    }

    Ok(DriveUploadResult {
        file_id: res
            .file_id
            .ok_or_else(|| CmdError::msg("missing fileId"))?,
        size_bytes: res.size_bytes.unwrap_or(bytes.len() as u64),
        last_modified_ms: remote_mtime,
        fingerprint: res.fingerprint.unwrap_or(fingerprint),
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
            fingerprint: None,
            target_local_part: None,
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
    apply_pending_sync(&app).map_err(|e| CmdError::msg(e.to_string()))
}

/// Sync version dùng trong `tauri::Builder.setup()` — chạy TRƯỚC `db::setup`.
/// Nếu có file pending → backup live + rename pending → live. Không lỗi nếu không có pending.
pub fn apply_pending_sync(app: &AppHandle) -> anyhow::Result<bool> {
    use std::fs as std_fs;

    let base = app
        .path()
        .app_data_dir()
        .map_err(|e| anyhow::anyhow!("app_data_dir: {e}"))?;
    let pending = base.join("thongkeshopee.pending.db");
    if !pending.exists() {
        return Ok(false);
    }

    let live = crate::db::resolve_db_path(app)?;

    // Backup live DB sang .pre-restore.db để user rollback nếu muốn.
    if live.exists() {
        let backup = live.with_extension("pre-restore.db");
        let _ = std_fs::remove_file(&backup);
        std_fs::rename(&live, &backup)?;
    }

    // WAL và SHM cũ (nếu có) không còn hợp lệ với DB mới → xóa.
    for ext in &["db-wal", "db-shm"] {
        let aux = live.with_extension(ext);
        let _ = std_fs::remove_file(&aux);
    }

    std_fs::rename(&pending, &live)?;
    Ok(true)
}

/// Restart app — dùng sau khi apply pending DB (yêu cầu close connection cũ).
#[tauri::command]
pub fn restart_app(app: AppHandle) {
    app.restart();
}

/// Admin-only: list toàn bộ users + metadata file trong Drive.
/// Verify admin server-side trong Apps Script qua Firestore rules.
#[tauri::command]
pub async fn drive_list_users(
    apps_script_url: String,
    id_token: String,
) -> CmdResult<Vec<UserListEntry>> {
    let res = call_apps_script(
        &apps_script_url,
        AppsScriptRequest {
            action: "listUsers",
            id_token: &id_token,
            base64_data: None,
            mtime_ms: None,
            fingerprint: None,
            target_local_part: None,
        },
    )
    .await?;
    Ok(res.users.unwrap_or_default())
}

/// Admin-only: download DB file của user khác, lưu vào temp path, trả path.
/// Apps Script verify admin status trước khi cho download.
#[tauri::command]
pub async fn admin_download_user_db(
    app: AppHandle,
    apps_script_url: String,
    id_token: String,
    target_local_part: String,
) -> CmdResult<String> {
    let res = call_apps_script(
        &apps_script_url,
        AppsScriptRequest {
            action: "downloadForUser",
            id_token: &id_token,
            base64_data: None,
            mtime_ms: None,
            fingerprint: None,
            target_local_part: Some(target_local_part.clone()),
        },
    )
    .await?;

    let base64 = res
        .base64_data
        .ok_or_else(|| CmdError::msg("missing base64Data"))?;
    let bytes = BASE64
        .decode(base64.as_bytes())
        .map_err(|e| CmdError::msg(format!("base64 decode: {e}")))?;

    let base_dir = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?;
    let admin_dir = base_dir.join("admin_view");
    fs::create_dir_all(&admin_dir).await.map_err(CmdError::from)?;

    // Lưu theo local_part cho dễ trace. Overwrite nếu admin xem lại cùng user.
    let target_path = admin_dir.join(format!("{target_local_part}.db"));
    // Xóa WAL/SHM cũ nếu có.
    for ext in &["db-wal", "db-shm"] {
        let _ = fs::remove_file(target_path.with_extension(ext)).await;
    }
    fs::write(&target_path, &bytes)
        .await
        .map_err(CmdError::from)?;

    Ok(target_path.to_string_lossy().into_owned())
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
