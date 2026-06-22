//! Upload video ad creative qua `/act_xxx/advideos`.
//!
//! Khác Reels: endpoint khác, dùng multipart/form-data thay vì 3-step
//! resumable upload. Cho file ≤ 100MB là OK với 1-shot upload.

use anyhow::{Context, Result};
use reqwest::multipart::{Form, Part};
use reqwest::Client;
use serde::Deserialize;

const API_VERSION: &str = "v21.0";
const GRAPH_BASE: &str = "https://graph.facebook.com";

#[derive(Deserialize)]
struct UploadResponse {
    id: String,
}

/// Upload 1 video file lên Ad Account để dùng làm creative.
///
/// Load toàn file vào RAM (giới hạn 100MB ở backend đã enforce) — simple
/// multipart. Progress chỉ emit ở stage level (uploading_video → done),
/// không track bytes vì 1-shot multipart.
///
/// Returns `video_id` để bind vào ad creative.
pub async fn upload_ad_video(
    client: &Client,
    account_id: &str,
    access_token: &str,
    file_path: &str,
) -> Result<String> {
    let bytes = tokio::fs::read(file_path)
        .await
        .with_context(|| format!("không đọc được file {file_path}"))?;

    let file_name = std::path::Path::new(file_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("video.mp4")
        .to_string();

    let part = Part::bytes(bytes)
        .file_name(file_name)
        .mime_str("application/octet-stream")?;
    let form = Form::new()
        .text("access_token", access_token.to_string())
        .part("source", part);

    let url = format!("{GRAPH_BASE}/{API_VERSION}/{account_id}/advideos");
    let resp = client
        .post(&url)
        .multipart(form)
        .send()
        .await
        .context("không gọi được /advideos")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("Upload ad video lỗi {}: {}", status, body);
    }
    let parsed: UploadResponse =
        serde_json::from_str(&body).with_context(|| format!("parse advideos: {body}"))?;
    Ok(parsed.id)
}
