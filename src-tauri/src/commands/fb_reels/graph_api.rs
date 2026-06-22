//! HTTP client cho Facebook Graph API — Reels endpoints.
//!
//! Tách khỏi commands.rs để giữ logic API pure (không động đến DB/state),
//! dễ test riêng và dễ swap version Graph API về sau.

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;

use super::types::FbPageWithToken;

const API_VERSION: &str = "v21.0";
const GRAPH_BASE: &str = "https://graph.facebook.com";

#[derive(Deserialize)]
struct AccountsResponse {
    data: Vec<RawPage>,
}

#[derive(Deserialize)]
struct RawPage {
    id: String,
    name: String,
    access_token: String,
}

/// Response của `upload_phase=start`.
#[derive(Deserialize, Debug)]
pub struct StartUploadResponse {
    pub video_id: String,
    pub upload_url: String,
}

/// Gọi `GET /me/accounts` để lấy danh sách Pages user quản lý + Page Token
/// riêng cho từng Page.
///
/// `user_token` có thể là User Access Token hoặc 1 Page Token — API vẫn trả
/// danh sách Pages mà token có quyền truy cập. Page Token trả về trong field
/// `access_token` là long-lived nếu input là long-lived.
pub async fn list_pages_from_token(
    client: &Client,
    user_token: &str,
) -> Result<Vec<FbPageWithToken>> {
    let url = format!("{}/{}/me/accounts", GRAPH_BASE, API_VERSION);
    let resp = client
        .get(&url)
        .query(&[
            ("access_token", user_token),
            ("fields", "id,name,access_token"),
        ])
        .send()
        .await
        .context("không gọi được Graph API /me/accounts")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("Graph API trả lỗi {}: {}", status, body);
    }
    let parsed: AccountsResponse = serde_json::from_str(&body)
        .with_context(|| format!("không parse được /me/accounts response: {body}"))?;
    Ok(parsed
        .data
        .into_iter()
        .map(|p| FbPageWithToken {
            page_id: p.id,
            name: p.name,
            access_token: p.access_token,
        })
        .collect())
}

/// Bước 1: tạo upload session — trả `video_id` + `upload_url` để stream binary.
pub async fn start_upload(
    client: &Client,
    page_id: &str,
    page_token: &str,
) -> Result<StartUploadResponse> {
    let url = format!("{}/{}/{}/video_reels", GRAPH_BASE, API_VERSION, page_id);
    let resp = client
        .post(&url)
        .query(&[
            ("upload_phase", "start"),
            ("access_token", page_token),
        ])
        .send()
        .await
        .context("không gọi được start upload")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("Start upload thất bại {}: {}", status, body);
    }
    serde_json::from_str(&body).with_context(|| format!("parse start_upload: {body}"))
}

/// Bước 3: finalize + publish (hoặc schedule).
///
/// `scheduled_time_sec`: Unix timestamp giây. Nếu Some → SCHEDULED + FB tự đăng,
/// nếu None → PUBLISHED ngay.
pub async fn finish_upload(
    client: &Client,
    page_id: &str,
    video_id: &str,
    page_token: &str,
    description: Option<&str>,
    scheduled_time_sec: Option<i64>,
) -> Result<()> {
    let url = format!("{}/{}/{}/video_reels", GRAPH_BASE, API_VERSION, page_id);
    let mut query: Vec<(&str, String)> = vec![
        ("upload_phase", "finish".to_string()),
        ("video_id", video_id.to_string()),
        ("access_token", page_token.to_string()),
    ];
    if let Some(t) = scheduled_time_sec {
        query.push(("video_state", "SCHEDULED".to_string()));
        query.push(("scheduled_publish_time", t.to_string()));
    } else {
        query.push(("video_state", "PUBLISHED".to_string()));
    }
    if let Some(desc) = description {
        query.push(("description", desc.to_string()));
    }

    let resp = client
        .post(&url)
        .query(&query)
        .send()
        .await
        .context("không gọi được finish upload")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("Finish upload thất bại {}: {}", status, body);
    }
    Ok(())
}

/// Best-effort lấy `permalink_url` của video sau khi publish.
/// Có thể fail nếu video chưa available — caller nên ignore lỗi và để
/// permalink null trong DB, user vẫn xem được trên FB qua video_id.
pub async fn fetch_permalink(
    client: &Client,
    video_id: &str,
    page_token: &str,
) -> Result<Option<String>> {
    let url = format!("{}/{}/{}", GRAPH_BASE, API_VERSION, video_id);
    let resp = client
        .get(&url)
        .query(&[
            ("fields", "permalink_url"),
            ("access_token", page_token),
        ])
        .send()
        .await?;
    if !resp.status().is_success() {
        return Ok(None);
    }
    #[derive(Deserialize)]
    struct R {
        permalink_url: Option<String>,
    }
    let parsed: R = resp.json().await.unwrap_or(R { permalink_url: None });
    Ok(parsed.permalink_url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accounts_response() {
        let body = r#"{"data":[
            {"id":"123","name":"Page A","access_token":"tokA"},
            {"id":"456","name":"Page B","access_token":"tokB"}
        ]}"#;
        let parsed: AccountsResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.data.len(), 2);
        assert_eq!(parsed.data[0].id, "123");
        assert_eq!(parsed.data[1].access_token, "tokB");
    }

    #[test]
    fn parse_start_upload_response() {
        let body = r#"{"video_id":"v_999","upload_url":"https://rupload.facebook.com/abc"}"#;
        let parsed: StartUploadResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.video_id, "v_999");
        assert!(parsed.upload_url.starts_with("https://"));
    }
}
