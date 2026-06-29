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
///
/// **Bắt buộc** gửi `description` non-empty khi `video_state=PUBLISHED`. FB
/// Reels silently giữ video ở DRAFT nếu thiếu — publishing_phase không bao giờ
/// complete, user thấy video kẹt `processing` mãi. Caller pass `None` → ta gửi
/// `"."` (1 ký tự, FB chấp nhận) làm placeholder thay vì bỏ field.
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
    // Default "." khi description rỗng — FB cần field này để publish, nếu thiếu
    // sẽ keep DRAFT và publishing_phase kẹt mãi.
    let desc = description
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(".");
    query.push(("description", desc.to_string()));

    let resp = client
        .post(&url)
        .query(&query)
        .send()
        .await
        .context("không gọi được finish upload")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    // Log raw response cho debugging — user/dev có thể tail log để xem FB trả gì.
    eprintln!(
        "[fb_reels] finish_upload page={page_id} video={video_id} status={status} body={body}"
    );
    if !status.is_success() {
        anyhow::bail!("Finish upload thất bại {}: {}", status, body);
    }
    // Một số trường hợp FB trả 200 nhưng body có "success":false hoặc lỗi —
    // catch để không silent fail.
    if body.contains("\"success\":false") || body.to_lowercase().contains("\"error\"") {
        anyhow::bail!("Finish upload báo lỗi trong body: {body}");
    }
    Ok(())
}

/// Best-effort lấy `permalink_url` của video sau khi publish.
/// Trả `Err` chỉ khi network error hoặc token issue (caller có thể detect
/// token_expired). Trả `Ok(None)` khi FB chưa transcode xong → caller có
/// thể retry sau (background task chạy mỗi 60s).
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
        .await
        .context("không gọi được Graph API fetch_permalink")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        // Propagate token errors để caller mark token_expired.
        if is_token_invalid(&body) {
            anyhow::bail!("token invalid hoặc hết hạn: {body}");
        }
        return Ok(None);
    }
    #[derive(Deserialize)]
    struct R {
        permalink_url: Option<String>,
    }
    let parsed: R = serde_json::from_str(&body).unwrap_or(R { permalink_url: None });
    Ok(parsed.permalink_url)
}

/// Trạng thái 1 video sau publish/schedule — dùng cho background poll.
pub enum VideoStatus {
    /// FB đã publish + có permalink (hoặc chưa có permalink nhưng status OK).
    Published { permalink_url: Option<String> },
    /// Đang xử lý (uploading, in progress, scheduled chưa tới giờ).
    Pending,
    /// Token Page hết hạn — caller mark `token_expired=1`.
    TokenExpired,
    /// FB từ chối video sau khi nhận (transcoding fail, sai format, vi phạm
    /// content policy...). `reason` là message ngắn từ FB error array.
    Failed { reason: String },
}

/// Query trạng thái thực tế của video trên FB.
///
/// FB Reels trả `status` dạng object multi-phase:
/// ```json
/// {
///   "status": {
///     "video_status": "processing" | "ready" | ...,
///     "uploading_phase":  { "status": "complete" | "in_progress" | "error", "errors": [...] },
///     "processing_phase": { "status": "complete" | "in_progress" | "error", "errors": [...] },
///     "publishing_phase": { "status": "complete" | "in_progress" | "error", "errors": [...] }
///   },
///   "permalink_url": "...",
///   "published": true/false
/// }
/// ```
///
/// Trả `Published` khi: `publishing_phase=complete` HOẶC `video_status=ready/processed`
/// HOẶC `published=true`.
/// Trả `Failed` khi: bất kỳ phase nào `status=error` (FB từ chối hẳn).
/// Còn lại → `Pending` (đang xử lý).
pub async fn fetch_video_status(
    client: &Client,
    video_id: &str,
    page_token: &str,
) -> Result<VideoStatus> {
    let url = format!("{}/{}/{}", GRAPH_BASE, API_VERSION, video_id);
    let resp = client
        .get(&url)
        .query(&[
            ("fields", "status,permalink_url,published"),
            ("access_token", page_token),
        ])
        .send()
        .await
        .context("không gọi được fetch_video_status")?;
    let status_code = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if !status_code.is_success() {
        if is_token_invalid(&body) {
            return Ok(VideoStatus::TokenExpired);
        }
        anyhow::bail!("fetch_video_status {status_code}: {body}");
    }

    // Log raw response cho debugging — khi processing kẹt mãi, user/dev có thể
    // tail log thấy chính xác FB trả structure gì để fix parser.
    eprintln!(
        "[fb_reels] fetch_video_status video={video_id} status={status_code} body={body}"
    );

    let parsed: serde_json::Value = serde_json::from_str(&body)
        .with_context(|| format!("parse fetch_video_status: {body}"))?;

    let permalink_url = parsed
        .get("permalink_url")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let published_flag = parsed.get("published").and_then(|v| v.as_bool()) == Some(true);

    let status_obj = parsed.get("status");
    let video_status = status_obj
        .and_then(|s| s.get("video_status"))
        .and_then(|s| s.as_str());

    let phase_status = |name: &str| -> Option<&str> {
        status_obj
            .and_then(|s| s.get(name))
            .and_then(|p| p.get("status"))
            .and_then(|v| v.as_str())
    };
    let phase_error_msg = |name: &str| -> Option<String> {
        status_obj
            .and_then(|s| s.get(name))
            .and_then(|p| p.get("errors"))
            .and_then(|errs| errs.as_array())
            .and_then(|arr| arr.first())
            .map(|err| {
                err.get("message")
                    .and_then(|m| m.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| err.to_string())
            })
    };

    let uploading_phase = phase_status("uploading_phase");
    let processing_phase = phase_status("processing_phase");
    let publishing_phase = phase_status("publishing_phase");

    // FB report error trên bất kỳ phase nào → video không lên được.
    let phase_error = uploading_phase == Some("error")
        || processing_phase == Some("error")
        || publishing_phase == Some("error");

    if phase_error {
        let reason = phase_error_msg("publishing_phase")
            .or_else(|| phase_error_msg("processing_phase"))
            .or_else(|| phase_error_msg("uploading_phase"))
            .unwrap_or_else(|| {
                format!(
                    "FB report error (uploading={uploading_phase:?}, \
                     processing={processing_phase:?}, publishing={publishing_phase:?})"
                )
            });
        return Ok(VideoStatus::Failed { reason });
    }

    let processed = matches!(
        video_status,
        Some("ready") | Some("READY") | Some("processed")
    ) || published_flag
        || publishing_phase == Some("complete");

    if processed {
        Ok(VideoStatus::Published { permalink_url })
    } else {
        Ok(VideoStatus::Pending)
    }
}

/// Fetch raw JSON GET /{video_id}?fields=status,permalink_url,published — KHÔNG
/// parse, KHÔNG classify. Trả nguyên text body để UI hiển thị cho user
/// copy/share debug khi `processing` kẹt mãi.
///
/// Lý do tách: `fetch_video_status` ăn JSON rồi classify → mất chi tiết về
/// errors[]/publish_status. Hàm này dump nguyên xi cho diagnostic.
pub async fn fetch_video_info_raw(
    client: &Client,
    video_id: &str,
    page_token: &str,
) -> Result<String> {
    let url = format!("{}/{}/{}", GRAPH_BASE, API_VERSION, video_id);
    let resp = client
        .get(&url)
        .query(&[
            (
                "fields",
                "status,permalink_url,published,id,created_time,description",
            ),
            ("access_token", page_token),
        ])
        .send()
        .await
        .context("không gọi được fetch_video_info_raw")?;
    let status_code = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status_code.is_success() {
        anyhow::bail!("fetch_video_info_raw {status_code}: {body}");
    }
    Ok(body)
}

/// Detect lỗi token Graph API. FB trả body JSON với `error.code` — code 190
/// là "Invalid OAuth access token" (expired/revoked). Cũng check substring
/// "Invalid OAuth" cho robustness khi schema thay đổi.
pub fn is_token_invalid(body: &str) -> bool {
    body.contains("\"code\":190")
        || body.contains("Invalid OAuth")
        || body.contains("Session has expired")
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

    /// Helper: build VideoStatus từ body JSON như fetch_video_status làm,
    /// nhưng tách parse logic ra để test thuần (không cần HTTP stub).
    fn classify_body(body: &str) -> VideoStatus {
        let parsed: serde_json::Value = serde_json::from_str(body).unwrap();
        let permalink_url = parsed
            .get("permalink_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let published_flag = parsed.get("published").and_then(|v| v.as_bool()) == Some(true);
        let status_obj = parsed.get("status");
        let video_status = status_obj
            .and_then(|s| s.get("video_status"))
            .and_then(|s| s.as_str());
        let phase = |name: &str| {
            status_obj
                .and_then(|s| s.get(name))
                .and_then(|p| p.get("status"))
                .and_then(|v| v.as_str())
        };
        let phase_err = |name: &str| -> Option<String> {
            status_obj
                .and_then(|s| s.get(name))
                .and_then(|p| p.get("errors"))
                .and_then(|errs| errs.as_array())
                .and_then(|arr| arr.first())
                .map(|err| {
                    err.get("message")
                        .and_then(|m| m.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| err.to_string())
                })
        };
        let u = phase("uploading_phase");
        let pr = phase("processing_phase");
        let pb = phase("publishing_phase");
        if u == Some("error") || pr == Some("error") || pb == Some("error") {
            let reason = phase_err("publishing_phase")
                .or_else(|| phase_err("processing_phase"))
                .or_else(|| phase_err("uploading_phase"))
                .unwrap_or_default();
            return VideoStatus::Failed { reason };
        }
        let done = matches!(
            video_status,
            Some("ready") | Some("READY") | Some("processed")
        ) || published_flag
            || pb == Some("complete");
        if done {
            VideoStatus::Published { permalink_url }
        } else {
            VideoStatus::Pending
        }
    }

    #[test]
    fn classify_reels_publishing_complete() {
        let body = r#"{
            "status": {
                "video_status": "processing",
                "uploading_phase":  {"status":"complete"},
                "processing_phase": {"status":"complete"},
                "publishing_phase": {"status":"complete"}
            },
            "permalink_url": "https://fb.com/reel/123"
        }"#;
        match classify_body(body) {
            VideoStatus::Published { permalink_url } => {
                assert_eq!(permalink_url.as_deref(), Some("https://fb.com/reel/123"));
            }
            _ => panic!("phải Published"),
        }
    }

    #[test]
    fn classify_reels_processing_in_progress() {
        let body = r#"{
            "status": {
                "video_status": "processing",
                "uploading_phase":  {"status":"complete"},
                "processing_phase": {"status":"in_progress"},
                "publishing_phase": {"status":"not_started"}
            }
        }"#;
        assert!(matches!(classify_body(body), VideoStatus::Pending));
    }

    #[test]
    fn classify_reels_processing_error() {
        let body = r#"{
            "status": {
                "video_status": "error",
                "uploading_phase":  {"status":"complete"},
                "processing_phase": {"status":"error",
                    "errors":[{"message":"Video format không hỗ trợ"}]},
                "publishing_phase": {"status":"not_started"}
            }
        }"#;
        match classify_body(body) {
            VideoStatus::Failed { reason } => {
                assert!(reason.contains("Video format"), "got: {reason}");
            }
            _ => panic!("phải Failed"),
        }
    }

    #[test]
    fn classify_legacy_video_status_ready() {
        // DB version cũ của Graph API trả status dạng object đơn giản chỉ có
        // video_status. Giữ tương thích ngược.
        let body = r#"{"status":{"video_status":"ready"},"permalink_url":"https://fb.com/v"}"#;
        match classify_body(body) {
            VideoStatus::Published { permalink_url } => {
                assert_eq!(permalink_url.as_deref(), Some("https://fb.com/v"));
            }
            _ => panic!("phải Published"),
        }
    }

    #[test]
    fn classify_published_flag_true() {
        let body = r#"{"status":{},"published":true,"permalink_url":"https://fb.com/x"}"#;
        assert!(matches!(classify_body(body), VideoStatus::Published { .. }));
    }
}
