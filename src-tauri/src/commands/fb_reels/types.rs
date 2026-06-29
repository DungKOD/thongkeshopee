//! DTO cho FB Reels — payload qua tauri::command + emit progress events.

use serde::{Deserialize, Serialize};

/// Page hiển thị cho UI — không có access_token vì UI không cần. Kèm flag
/// `token_expired` để UI hiện badge "Token hết hạn" + chặn upload.
///
/// `token_hash` là 8 ký tự hex đầu của SHA-256(access_token) — UI dùng để tô
/// màu các page chia sẻ cùng 1 token (vd cùng được fetch từ 1 User Token).
/// Không leak token thật: hash 32-bit không reversible cho chuỗi 200 ký tự.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbPage {
    pub page_id: String,
    pub name: String,
    pub token_expired: bool,
    pub token_hash: String,
}

/// Page kèm access_token — chỉ dùng khi save sau khi validate token,
/// hoặc khi frontend hiển thị danh sách Pages từ token mới paste vào.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbPageWithToken {
    pub page_id: String,
    pub name: String,
    pub access_token: String,
}

/// User Token đã lưu — meta only, không expose raw token. UI dùng `token_hash`
/// (8 hex) làm hue cho màu phân biệt. Mỗi auth token quản N Pages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbAuthToken {
    pub id: i64,
    pub label: String,
    pub token_hash: String,
    pub added_at_ms: i64,
    pub expired: bool,
}

/// 1 record trong bảng `fb_reel_posts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbReelPost {
    pub id: i64,
    pub page_id: String,
    pub page_name: String,
    pub file_path: String,
    pub file_size: i64,
    pub caption: Option<String>,
    pub scheduled_time_ms: Option<i64>,
    pub status: String,
    pub progress: i64,
    pub fb_video_id: Option<String>,
    pub fb_permalink: Option<String>,
    pub error_message: Option<String>,
    pub created_at_ms: i64,
    pub published_at_ms: Option<i64>,
}

/// Payload emit qua event `fb_upload_progress` — UI subscribe để render
/// progress bar realtime. Throttle ở backend nên không spam.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadProgress {
    pub post_id: i64,
    pub status: String,
    pub progress: i64,
    pub bytes_uploaded: u64,
    pub bytes_total: u64,
}
