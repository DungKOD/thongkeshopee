//! Device identification — phục vụ chính sách "1 tài khoản 1 máy".
//!
//! `get_device_id` đọc machine-uid (Windows: `MachineGuid` registry key) rồi
//! SHA-256 hash trước khi expose ra UI/Firestore — không lộ raw machine ID.
//! Hostname (tên máy) trả về plain để user nhận diện thiết bị trong Settings.
//!
//! Kết quả cache bằng `OnceLock` (machine ID không đổi runtime → không tốn IO
//! mỗi lần gọi). Lỗi đọc machine-uid được fallback bằng hostname-hash để app
//! vẫn dùng được — đánh dấu `is_fallback=true` để UI biết.

use std::sync::OnceLock;

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::CmdResult;

/// Thông tin định danh thiết bị gửi về UI + Firestore.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceInfo {
    /// SHA-256 hash của machine UID (hex, 64 ký tự).
    pub device_id: String,
    /// Hostname máy (plain) — chỉ dùng hiển thị cho user nhận diện.
    pub device_name: String,
    /// Platform string: `windows` / `macos` / `linux` / `unknown`.
    pub platform: String,
    /// True nếu machine-uid lỗi và đang dùng hostname-hash fallback.
    pub is_fallback: bool,
}

static CACHED: OnceLock<DeviceInfo> = OnceLock::new();

/// Lấy SHA-256 hex của input string.
fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Build `DeviceInfo` từ machine UID + hostname.
fn build_device_info() -> DeviceInfo {
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

    match machine_uid::get() {
        Ok(raw_uid) => DeviceInfo {
            device_id: sha256_hex(&raw_uid),
            device_name: hostname,
            platform,
            is_fallback: false,
        },
        Err(_) => DeviceInfo {
            device_id: sha256_hex(&format!("fallback-host:{}", hostname)),
            device_name: hostname,
            platform,
            is_fallback: true,
        },
    }
}

/// Trả về thông tin định danh thiết bị hiện tại (cached).
///
/// Frontend gọi command này để claim Firestore session document và để
/// realtime listener so sánh máy hiện tại vs máy đang giữ session.
#[tauri::command]
pub fn get_device_id() -> CmdResult<DeviceInfo> {
    Ok(CACHED.get_or_init(build_device_info).clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_hex_is_64_chars() {
        let h = sha256_hex("hello");
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn get_device_id_returns_consistent_value() {
        let a = get_device_id().expect("first call");
        let b = get_device_id().expect("second call");
        assert_eq!(a.device_id, b.device_id);
        assert_eq!(a.device_name, b.device_name);
    }
}

