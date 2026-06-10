//! Device identification — phục vụ chính sách "1 tài khoản 1 máy".
//!
//! `get_device_id` build ra một `device_id` SHA-256 hex từ 2 thành phần:
//! 1. **machine UID** (Windows: `HKLM\Software\Microsoft\Cryptography\MachineGuid`,
//!    Linux: `/etc/machine-id`, macOS: IOPlatformUUID) qua crate `machine-uid`.
//! 2. **install_id**: UUID v4 random sinh lần đầu app chạy, persist trong
//!    `app_data_dir/install_id`. Mục đích: chống disk-clone — 2 máy ghost
//!    image / VM clone (chưa sysprep) sẽ có cùng MachineGuid; bằng cách
//!    combine với install_id sinh sau khi app chạy lần đầu, mỗi máy sẽ có
//!    `device_id` khác nhau.
//!
//! Hai thành phần này được nối bằng dấu `|` rồi hash → 64 ký tự hex. Hostname
//! (tên máy) chỉ dùng làm `device_name` hiển thị, KHÔNG dùng làm seed (vì
//! hai máy Windows mới cài thường có hostname trùng dạng `DESKTOP-XXXXXX`).
//!
//! Kết quả cache bằng `OnceLock` — install_id file chỉ đọc 1 lần per run.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::Serialize;
use sha2::{Digest, Sha256};
use tauri::{AppHandle, Manager};
use uuid::Uuid;

use super::{CmdError, CmdResult};

/// Thông tin định danh thiết bị gửi về UI + Firestore.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceInfo {
    /// SHA-256 hash của `{machine_uid}|{install_id}` (hex, 64 ký tự).
    pub device_id: String,
    /// Hostname máy (plain) — chỉ dùng hiển thị cho user nhận diện thiết bị.
    pub device_name: String,
    /// Platform string: `windows` / `macos` / `linux` / `unknown`.
    pub platform: String,
    /// True nếu machine UID lỗi HOẶC install_id không persist được (rơi vào
    /// ephemeral seed) — UI có thể hiển thị warning, admin cần kiểm tra.
    pub is_fallback: bool,
}

static CACHED: OnceLock<DeviceInfo> = OnceLock::new();

/// Tên file lưu install_id trong `app_data_dir`. Không có extension để dễ
/// nhận diện (không nhầm với DB/log).
const INSTALL_ID_FILE: &str = "install_id";

/// Lấy SHA-256 hex của input string.
fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Pure version (path-based) — đọc file nếu có nội dung hợp lệ, ngược lại
/// tạo UUID v4 mới rồi ghi đè. Trả về install_id dạng string.
///
/// Idempotent: gọi nhiều lần trên cùng path → luôn trả cùng giá trị (lần đầu
/// tạo, các lần sau đọc lại).
fn read_or_create_install_id_at(path: &Path) -> Result<String, CmdError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Ok(content) = fs::read_to_string(path) {
        let trimmed = content.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    let new_id = Uuid::new_v4().to_string();
    fs::write(path, &new_id)?;
    Ok(new_id)
}

/// Resolve `app_data_dir/install_id` rồi gọi `read_or_create_install_id_at`.
fn install_id_for_app(app: &AppHandle) -> Result<String, CmdError> {
    let base: PathBuf = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?;
    let file_path = base.join(INSTALL_ID_FILE);
    read_or_create_install_id_at(&file_path)
}

/// Build `DeviceInfo` từ machine UID + install_id.
///
/// Nếu install_id không persist được (vd permission lỗi trên app_data_dir),
/// fallback sang seed ephemeral dựa trên nanos + pid — vẫn ra `device_id`
/// hợp lệ để app không crash, nhưng đánh dấu `is_fallback=true`. Lưu ý
/// device_id ephemeral sẽ đổi giữa các lần restart app → user sẽ bị kick
/// chính mình; cần fix permission để khắc phục.
fn build_device_info(app: &AppHandle) -> DeviceInfo {
    let hostname = hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "unknown-host".to_string());

    let platform = if cfg!(target_os = "windows") {
        "windows"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else {
        "unknown"
    }
    .to_string();

    let (install_id, install_id_fail) = match install_id_for_app(app) {
        Ok(id) => (id, false),
        Err(e) => {
            eprintln!("[device] install_id persist failed: {e}");
            let nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
            let pid = std::process::id();
            (format!("ephemeral-{nanos}-{pid}"), true)
        }
    };

    let (raw_uid, machine_uid_fail) = match machine_uid::get() {
        Ok(uid) => (uid, false),
        Err(e) => {
            eprintln!("[device] machine_uid lookup failed: {e}");
            (format!("no-machine-uid:{hostname}"), true)
        }
    };

    let combined = format!("{raw_uid}|{install_id}");
    DeviceInfo {
        device_id: sha256_hex(&combined),
        device_name: hostname,
        platform,
        is_fallback: machine_uid_fail || install_id_fail,
    }
}

/// Trả về thông tin định danh thiết bị hiện tại (cached lần đầu).
///
/// Frontend gọi command này để claim Firestore session document và để
/// realtime listener so sánh máy hiện tại vs máy đang giữ session.
#[tauri::command]
pub fn get_device_id(app: AppHandle) -> CmdResult<DeviceInfo> {
    if let Some(cached) = CACHED.get() {
        return Ok(cached.clone());
    }
    let info = build_device_info(&app);
    let _ = CACHED.set(info.clone());
    Ok(info)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn sha256_hex_is_64_chars() {
        let h = sha256_hex("hello");
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn install_id_is_persisted_across_reads() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("install_id");
        let id1 = read_or_create_install_id_at(&path).expect("first call");
        let id2 = read_or_create_install_id_at(&path).expect("second call");
        assert_eq!(id1, id2, "install_id must persist across reads");
        // Sanity: UUID v4 hyphenated = 36 ký tự.
        assert_eq!(id1.len(), 36);
    }

    #[test]
    fn install_id_regenerates_when_file_empty() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("install_id");
        fs::write(&path, "   \n\t").expect("write empty");
        let id = read_or_create_install_id_at(&path).expect("regen");
        assert_eq!(id.len(), 36);
        let saved = fs::read_to_string(&path).expect("read back");
        assert_eq!(saved.trim(), id);
    }

    #[test]
    fn install_id_creates_parent_dir() {
        let dir = tempdir().expect("tempdir");
        let nested = dir.path().join("nested/sub/install_id");
        let id = read_or_create_install_id_at(&nested).expect("create");
        assert_eq!(id.len(), 36);
        assert!(nested.exists());
    }
}
