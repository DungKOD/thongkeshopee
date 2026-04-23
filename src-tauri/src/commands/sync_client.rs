//! HTTP client cho Cloudflare Worker (`thongkeshopee-sync`) — tầng transport
//! cho DB sync R2. Tách riêng khỏi `sync.rs` để `sync.rs` chỉ giữ business
//! logic (merge, tombstones, sync_state), grep `reqwest` chỉ ra file này.
//!
//! Endpoint map với Worker (`worker/src/index.ts`):
//!   POST /metadata            — HEAD object, trả exists/size/mtime/fingerprint
//!   POST /upload              — body raw zstd bytes (v8.1+)
//!   POST /download            — body {}, trả raw zstd bytes + mtime
//!   POST /admin/users         — list objects `users/*/db.zst` (admin only)
//!   POST /admin/cleanup-orphans — xoá R2 files khi UID không có trong Firestore
//!   GET  /admin/download?uid= — download DB của user target (admin only)

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::{CmdError, CmdResult};

const HTTP_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Deserialize)]
struct WorkerResponseEnvelope {
    ok: bool,
    #[serde(default)]
    code: Option<u16>,
    #[serde(default)]
    error: Option<String>,
    #[serde(flatten)]
    extra: serde_json::Value,
}

/// Metadata của DB backup trên R2. Shape khớp `MetadataResponse` ở Worker.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerMetadata {
    pub exists: bool,
    pub file_id: Option<String>,
    pub size_bytes: Option<u64>,
    pub last_modified: Option<i64>,
    pub fingerprint: Option<String>,
    /// CAS etag (v8+). null nếu object không tồn tại hoặc Worker chưa support.
    #[serde(default)]
    pub etag: Option<String>,
}

/// Kết quả upload — Worker trả JSON body (upload request là binary).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerUpload {
    pub file_id: String,
    pub size_bytes: u64,
    pub last_modified: i64,
    pub fingerprint: String,
    /// Etag mới sau upload — client lưu vào sync_state cho upload kế tiếp.
    #[serde(default)]
    pub etag: Option<String>,
}

/// Kết quả download — v8.1+ raw bytes (zstd), metadata trong HTTP headers.
/// Không còn JSON envelope + base64.
#[derive(Debug)]
pub struct WorkerDownload {
    /// Raw zstd-compressed SQLite bytes.
    pub bytes: Vec<u8>,
    pub last_modified: i64,
    /// Etag tại thời điểm download — client lưu làm expectedEtag cho upload kế tiếp.
    pub etag: Option<String>,
}

/// Entry trong admin user list — shape giữ backward compat với `UserListEntry`
/// của Apps Script cũ (FE đã render theo shape này).
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

#[derive(Debug, Deserialize)]
struct UsersResponseBody {
    #[serde(default)]
    users: Vec<UserListEntry>,
}

/// Custom error variant — caller cần biết 412 conflict để trigger pull-merge-push.
#[derive(Debug)]
pub enum UploadError {
    /// R2 etag mismatch — client cần pull-merge-push + retry.
    EtagConflict(String),
    /// Lỗi khác (HTTP, network, parse, v.v.).
    Other(CmdError),
}

impl From<CmdError> for UploadError {
    fn from(e: CmdError) -> Self {
        UploadError::Other(e)
    }
}

fn http_client() -> CmdResult<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(CmdError::from)
}

/// Low-level: POST/GET tới Worker với Bearer token. Parse envelope `{ok, ...}`.
/// Non-2xx HTTP hoặc `ok: false` → bail với message có HTTP code + body.
async fn call_worker<TBody: Serialize>(
    base_url: &str,
    path: &str,
    method: reqwest::Method,
    id_token: &str,
    body: Option<&TBody>,
) -> CmdResult<serde_json::Value> {
    let url = format!("{}{}", base_url.trim_end_matches('/'), path);
    let client = http_client()?;
    let mut req = client
        .request(method, &url)
        .bearer_auth(id_token)
        .header("Content-Type", "application/json");
    if let Some(b) = body {
        req = req.json(b);
    }
    let res = req.send().await.map_err(CmdError::from)?;
    let status = res.status();
    let text = res.text().await.map_err(CmdError::from)?;

    if !status.is_success() {
        // Parse envelope để lấy error message nếu có, fallback sang text gốc.
        let detail = serde_json::from_str::<WorkerResponseEnvelope>(&text)
            .ok()
            .and_then(|e| e.error)
            .unwrap_or_else(|| text.clone());
        return Err(CmdError::msg(format!(
            "Worker HTTP {} {path}: {detail}",
            status.as_u16()
        )));
    }

    let env: WorkerResponseEnvelope = serde_json::from_str(&text)
        .map_err(|e| CmdError::msg(format!("parse Worker response: {e} — body: {text}")))?;
    if !env.ok {
        let code = env.code.unwrap_or(500);
        let msg = env.error.unwrap_or_else(|| "unknown".into());
        return Err(CmdError::msg(format!("Worker {code} {path}: {msg}")));
    }
    Ok(env.extra)
}

/// `POST /metadata` — trả shape thống nhất kể cả khi `exists=false`.
pub async fn metadata(base_url: &str, id_token: &str) -> CmdResult<WorkerMetadata> {
    let extra = call_worker::<()>(
        base_url,
        "/metadata",
        reqwest::Method::POST,
        id_token,
        Some(&()),
    )
    .await?;
    serde_json::from_value(extra).map_err(CmdError::from)
}

/// `POST /upload` — body raw zstd bytes (v8.1+, không còn base64/gzip).
/// Metadata trong HTTP headers: `X-Mtime-Ms`, `X-Fingerprint`, `X-Expected-Etag`.
/// CAS guard: `expected_etag` = etag client expected. Worker trả 412 nếu R2
/// etag hiện tại khác → return [`UploadError::EtagConflict`].
pub async fn upload(
    base_url: &str,
    id_token: &str,
    compressed_bytes: &[u8],
    mtime_ms: i64,
    fingerprint: &str,
    expected_etag: Option<&str>,
) -> Result<WorkerUpload, UploadError> {
    let url = format!("{}/upload", base_url.trim_end_matches('/'));
    let client = http_client()?;
    let mut req = client
        .post(&url)
        .bearer_auth(id_token)
        .header("Content-Type", "application/octet-stream")
        .header("X-Mtime-Ms", mtime_ms.to_string())
        .header("X-Fingerprint", fingerprint)
        .body(compressed_bytes.to_vec());
    if let Some(etag) = expected_etag {
        req = req.header("X-Expected-Etag", etag);
    }
    let res = req.send().await.map_err(CmdError::from)?;
    let status = res.status();
    let text = res.text().await.map_err(CmdError::from)?;

    if status.as_u16() == 412 {
        let detail = serde_json::from_str::<WorkerResponseEnvelope>(&text)
            .ok()
            .and_then(|e| e.error)
            .unwrap_or_else(|| text.clone());
        return Err(UploadError::EtagConflict(detail));
    }

    if !status.is_success() {
        let detail = serde_json::from_str::<WorkerResponseEnvelope>(&text)
            .ok()
            .and_then(|e| e.error)
            .unwrap_or_else(|| text.clone());
        return Err(UploadError::Other(CmdError::msg(format!(
            "Worker HTTP {} /upload: {detail}",
            status.as_u16()
        ))));
    }

    let env: WorkerResponseEnvelope = serde_json::from_str(&text)
        .map_err(|e| CmdError::msg(format!("parse Worker response: {e} — body: {text}")))?;
    if !env.ok {
        let code = env.code.unwrap_or(500);
        let msg = env.error.unwrap_or_else(|| "unknown".into());
        return Err(UploadError::Other(CmdError::msg(format!(
            "Worker {code} /upload: {msg}"
        ))));
    }

    serde_json::from_value(env.extra)
        .map_err(|e| UploadError::Other(CmdError::from(e)))
}

/// `POST /download` — v8.1+ trả raw zstd bytes, metadata trong response headers.
/// Headers: `X-Size-Bytes`, `X-Last-Modified-Ms`, `ETag`.
pub async fn download(base_url: &str, id_token: &str) -> CmdResult<WorkerDownload> {
    let url = format!("{}/download", base_url.trim_end_matches('/'));
    let client = http_client()?;
    let res = client
        .post(&url)
        .bearer_auth(id_token)
        .send()
        .await
        .map_err(CmdError::from)?;
    let status = res.status();
    if !status.is_success() {
        let text = res.text().await.unwrap_or_default();
        let detail = serde_json::from_str::<WorkerResponseEnvelope>(&text)
            .ok()
            .and_then(|e| e.error)
            .unwrap_or(text);
        return Err(CmdError::msg(format!(
            "Worker HTTP {} /download: {detail}",
            status.as_u16()
        )));
    }
    let last_modified = res
        .headers()
        .get("X-Last-Modified-Ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let etag = res
        .headers()
        .get("ETag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_matches('"').to_string());
    let bytes = res.bytes().await.map_err(CmdError::from)?.to_vec();
    Ok(WorkerDownload {
        bytes,
        last_modified,
        etag,
    })
}

/// `POST /admin/users` — admin only. Trả list user + metadata file.
/// Worker hiện stub các field profile (email/premium/expiredAt) null — Phase 2
/// sẽ bổ sung nguồn data (KV hoặc Firestore REST).
pub async fn admin_list_users(base_url: &str, id_token: &str) -> CmdResult<Vec<UserListEntry>> {
    let extra = call_worker::<()>(
        base_url,
        "/admin/users",
        reqwest::Method::POST,
        id_token,
        Some(&()),
    )
    .await?;
    let body: UsersResponseBody = serde_json::from_value(extra).map_err(CmdError::from)?;
    Ok(body.users)
}

/// `POST /admin/cleanup-orphans` — admin only. Worker list R2 `users/` prefix,
/// xoá `db.zst` của UID không tồn tại trong Firestore. Trả list UIDs đã xóa.
pub async fn admin_cleanup_orphans(
    base_url: &str,
    id_token: &str,
) -> CmdResult<Vec<String>> {
    let extra = call_worker::<()>(
        base_url,
        "/admin/cleanup-orphans",
        reqwest::Method::POST,
        id_token,
        Some(&()),
    )
    .await?;
    #[derive(serde::Deserialize)]
    struct CleanupBody {
        deleted: Vec<String>,
    }
    let body: CleanupBody = serde_json::from_value(extra).map_err(CmdError::from)?;
    Ok(body.deleted)
}

/// `GET /admin/download?uid=<uid>` — admin only. v8.1+ raw zstd bytes.
/// Tuple output: (bytes, size, last_modified_ms). Admin view decompress qua
/// `zstd_decompress` trong `admin_view::admin_view_user_db`.
pub async fn admin_download(
    base_url: &str,
    id_token: &str,
    target_uid: &str,
) -> CmdResult<(Vec<u8>, u64, i64)> {
    let path = format!("/admin/download?uid={}", urlencoding_encode(target_uid));
    let url = format!("{}{}", base_url.trim_end_matches('/'), path);
    let client = http_client()?;
    let res = client
        .get(&url)
        .bearer_auth(id_token)
        .send()
        .await
        .map_err(CmdError::from)?;
    let status = res.status();
    if !status.is_success() {
        let text = res.text().await.unwrap_or_default();
        let detail = serde_json::from_str::<WorkerResponseEnvelope>(&text)
            .ok()
            .and_then(|e| e.error)
            .unwrap_or(text);
        return Err(CmdError::msg(format!(
            "Worker HTTP {} /admin/download: {detail}",
            status.as_u16()
        )));
    }
    let last_modified = res
        .headers()
        .get("X-Last-Modified-Ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let bytes = res.bytes().await.map_err(CmdError::from)?.to_vec();
    let size = bytes.len() as u64;
    Ok((bytes, size, last_modified))
}

/// Minimal URL-encoding cho uid (Firebase UID chỉ có a-z0-9 nên tĩnh là an
/// toàn, nhưng escape defensively để không break khi Firebase đổi format).
fn urlencoding_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}
