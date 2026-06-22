//! DTO cho FB Reels — payload qua tauri::command + emit progress events.

use serde::{Deserialize, Serialize};

/// Page hiển thị cho UI — không có access_token vì UI không cần.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbPage {
    pub page_id: String,
    pub name: String,
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
