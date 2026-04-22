//! HTTP client cho Cloudflare Worker (`thongkeshopee-sync`) — tầng transport
//! cho DB sync R2. Tách riêng khỏi `sync.rs` để `sync.rs` chỉ giữ business
//! logic (merge, tombstones, sync_state), grep `reqwest` chỉ ra file này.
//!
//! Endpoint map với Worker (`worker/src/index.ts`):
//!   POST /metadata            — HEAD object, trả exists/size/mtime/fingerprint
//!   POST /upload              — body {base64Data, mtimeMs, fingerprint}
//!   POST /download            — body {}, trả base64Data + mtime
//!   POST /admin/users         — list objects `users/*/db.gz` (admin only)
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
}

/// Kết quả upload — shape khớp `UploadResponse` ở Worker.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerUpload {
    pub file_id: String,
    pub size_bytes: u64,
    pub last_modified: i64,
    pub fingerprint: String,
}

/// Kết quả download — base64-encoded bytes y nguyên đã lưu R2 (gzipped SQLite).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerDownload {
    pub base64_data: String,
    pub size_bytes: u64,
    pub last_modified: i64,
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadBody<'a> {
    base64_data: &'a str,
    mtime_ms: i64,
    fingerprint: &'a str,
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

/// `POST /upload` — body base64 (gzipped SQLite), mtime, fingerprint.
pub async fn upload(
    base_url: &str,
    id_token: &str,
    base64_data: &str,
    mtime_ms: i64,
    fingerprint: &str,
) -> CmdResult<WorkerUpload> {
    let body = UploadBody {
        base64_data,
        mtime_ms,
        fingerprint,
    };
    let extra = call_worker(
        base_url,
        "/upload",
        reqwest::Method::POST,
        id_token,
        Some(&body),
    )
    .await?;
    serde_json::from_value(extra).map_err(CmdError::from)
}

/// `POST /download` — trả base64 của gzipped bytes trên R2.
pub async fn download(base_url: &str, id_token: &str) -> CmdResult<WorkerDownload> {
    let extra = call_worker::<()>(
        base_url,
        "/download",
        reqwest::Method::POST,
        id_token,
        Some(&()),
    )
    .await?;
    serde_json::from_value(extra).map_err(CmdError::from)
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
/// xoá `db.gz` của UID không tồn tại trong Firestore. Trả list UIDs đã xóa.
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

/// `GET /admin/download?uid=<uid>` — admin only. Trả base64 + size + mtime.
/// Tuple output giữ backward-compat với `as_download_for_user` cũ để
/// `admin_view.rs` không phải refactor tough.
pub async fn admin_download(
    base_url: &str,
    id_token: &str,
    target_uid: &str,
) -> CmdResult<(String, u64, i64)> {
    let path = format!("/admin/download?uid={}", urlencoding_encode(target_uid));
    let extra = call_worker::<()>(base_url, &path, reqwest::Method::GET, id_token, None).await?;
    let dl: WorkerDownload = serde_json::from_value(extra).map_err(CmdError::from)?;
    Ok((dl.base64_data, dl.size_bytes, dl.last_modified))
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
