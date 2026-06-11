//! Proxy gọi Apps Script Web App qua Rust reqwest để bypass webview CORS +
//! redirect issue.
//!
//! Tại sao cần proxy:
//! 1. Apps Script Web App POST trả 302 redirect tới `script.googleusercontent.com`
//!    để chạy doPost. fetch trong webview gặp redirect cross-origin sẽ block
//!    đọc body (CORS expose-headers fail).
//! 2. reqwest từ Rust không thuộc browser context → no CORS, auto follow
//!    redirect và trả body cuối cùng cho FE.
//!
//! Auth app-level qua Firebase idToken trong body (verify server-side bằng
//! Identity Toolkit). KHÔNG dùng Bearer Google OAuth — Apps Script Web App
//! phải deploy access="Anyone", không phải "Anyone with Google account"
//! (Bearer Google OAuth TTL ~1h, không refresh silent được).
use serde::{Deserialize, Serialize};

use super::{CmdError, CmdResult};

/// Response wrapper: status code + body raw. FE tự parse JSON từ body.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyResponse {
    pub status: u16,
    pub body: String,
}

/// POST tới Apps Script Web App. Content-Type cố định `text/plain;charset=utf-8`
/// — Apps Script `doPost` đọc raw body từ `e.postData.contents` nên không
/// quan tâm content-type, và `text/plain` là CORS-safelisted nên không trigger
/// preflight OPTIONS (vô dụng vì Apps Script không handle OPTIONS).
#[tauri::command]
pub async fn proxy_apps_script(url: String, body: String) -> CmdResult<ProxyResponse> {
    let client = reqwest::Client::builder()
        .build()
        .map_err(|e| CmdError::msg(format!("HTTP client build: {}", e)))?;

    let res = client
        .post(&url)
        .header("Content-Type", "text/plain;charset=utf-8")
        .body(body)
        .send()
        .await?;
    let status = res.status().as_u16();
    let body = res.text().await?;
    Ok(ProxyResponse { status, body })
}
