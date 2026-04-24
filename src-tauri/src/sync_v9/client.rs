//! HTTP client cho Worker v9 endpoints (`worker/src/routes/v9.ts`).
//!
//! Endpoint map:
//! - POST /v9/manifest/get → `(Manifest, etag)`
//! - POST /v9/manifest/put → new etag | CAS conflict (412)
//! - POST /v9/delta/upload?key → ()
//! - GET  /v9/delta/fetch?key → bytes
//! - POST /v9/snapshot/upload?key → ()
//! - GET  /v9/snapshot/fetch?key → bytes (stream)
//! - POST /v9/sync-log/push?date → ()
//!
//! Layer transport — orchestration + HLC + cursor update defer Tauri commands
//! (`commands/sync_v9_cmds.rs`).

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::time::Duration;

use super::types::Manifest;

const HTTP_TIMEOUT: Duration = Duration::from_secs(120);
/// Longer timeout cho snapshot upload/fetch (500MB+ mạng chậm).
const HTTP_TIMEOUT_SNAPSHOT: Duration = Duration::from_secs(600);

/// Error signal cho CAS retry loop — Worker trả 412 khi etag mismatch.
pub const CAS_CONFLICT: &str = "V9_CAS_CONFLICT";

fn build_client(timeout: Duration) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .context("build reqwest client")
}

fn url(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
}

#[derive(Debug, Deserialize)]
struct Envelope {
    ok: bool,
    #[serde(default)]
    code: Option<u16>,
    #[serde(default)]
    error: Option<String>,
    #[serde(flatten)]
    extra: serde_json::Value,
}

async fn parse_envelope(res: reqwest::Response, path: &str) -> Result<serde_json::Value> {
    let status = res.status();
    let text = res.text().await.context("read response body")?;
    if !status.is_success() {
        // 412 special → caller check CAS.
        if status.as_u16() == 412 {
            return Err(anyhow!("{CAS_CONFLICT} {path}: {text}"));
        }
        let detail = serde_json::from_str::<Envelope>(&text)
            .ok()
            .and_then(|e| e.error)
            .unwrap_or_else(|| text.clone());
        return Err(anyhow!("HTTP {} {path}: {detail}", status.as_u16()));
    }
    let env: Envelope = serde_json::from_str(&text)
        .with_context(|| format!("parse envelope from {path}: {text}"))?;
    if !env.ok {
        let code = env.code.unwrap_or(500);
        let msg = env.error.unwrap_or_else(|| "unknown".into());
        return Err(anyhow!("Worker {code} {path}: {msg}"));
    }
    Ok(env.extra)
}

// =============================================================
// MANIFEST
// =============================================================

#[derive(Debug, Clone)]
pub struct ManifestFetch {
    /// None nếu manifest chưa tồn tại (first sync).
    pub manifest: Option<Manifest>,
    /// None nếu manifest chưa tồn tại. Pass `expectedEtag=None` ở put đầu tiên.
    pub etag: Option<String>,
}

/// POST /v9/manifest/get
pub async fn get_manifest(base_url: &str, id_token: &str) -> Result<ManifestFetch> {
    let path = "/v9/manifest/get";
    let res = build_client(HTTP_TIMEOUT)?
        .post(url(base_url, path))
        .bearer_auth(id_token)
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .context("get_manifest send")?;
    let extra = parse_envelope(res, path).await?;

    #[derive(Deserialize)]
    struct ManifestResp {
        manifest: Option<Manifest>,
        etag: Option<String>,
    }
    let body: ManifestResp = serde_json::from_value(extra).context("parse ManifestResp")?;
    Ok(ManifestFetch {
        manifest: body.manifest,
        etag: body.etag,
    })
}

/// POST /v9/manifest/put — CAS via expectedEtag.
///
/// Return new etag nếu OK. Return Err với `CAS_CONFLICT` prefix nếu 412.
/// Caller (Tauri command) catch prefix → re-fetch + re-append + retry.
pub async fn put_manifest(
    base_url: &str,
    id_token: &str,
    manifest: &Manifest,
    expected_etag: Option<&str>,
) -> Result<String> {
    let path = "/v9/manifest/put";
    let body = serde_json::json!({
        "manifest": manifest,
        "expectedEtag": expected_etag,
    });
    let res = build_client(HTTP_TIMEOUT)?
        .post(url(base_url, path))
        .bearer_auth(id_token)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .context("put_manifest send")?;
    let extra = parse_envelope(res, path).await?;

    #[derive(Deserialize)]
    struct PutResp {
        etag: String,
    }
    let body: PutResp = serde_json::from_value(extra).context("parse PutResp")?;
    Ok(body.etag)
}

// =============================================================
// DELTA
// =============================================================

/// POST /v9/delta/upload?key=... body: raw zstd bytes.
pub async fn upload_delta(
    base_url: &str,
    id_token: &str,
    key: &str,
    bytes: &[u8],
) -> Result<()> {
    let path = format!("/v9/delta/upload?key={}", urlencoding_encode(key));
    let res = build_client(HTTP_TIMEOUT)?
        .post(url(base_url, &path))
        .bearer_auth(id_token)
        .header("Content-Type", "application/octet-stream")
        .body(bytes.to_vec())
        .send()
        .await
        .context("upload_delta send")?;
    let _ = parse_envelope(res, &path).await?;
    Ok(())
}

/// GET /v9/delta/fetch?key=... return zstd bytes.
pub async fn fetch_delta(base_url: &str, id_token: &str, key: &str) -> Result<Vec<u8>> {
    let path = format!("/v9/delta/fetch?key={}", urlencoding_encode(key));
    let res = build_client(HTTP_TIMEOUT)?
        .get(url(base_url, &path))
        .bearer_auth(id_token)
        .send()
        .await
        .context("fetch_delta send")?;
    let status = res.status();
    if !status.is_success() {
        let body = res.text().await.unwrap_or_default();
        return Err(anyhow!("HTTP {} fetch_delta {key}: {body}", status.as_u16()));
    }
    let bytes = res.bytes().await.context("read delta bytes")?;
    Ok(bytes.to_vec())
}

// =============================================================
// SNAPSHOT
// =============================================================

/// POST /v9/snapshot/upload?key=... body: raw zstd bytes.
pub async fn upload_snapshot(
    base_url: &str,
    id_token: &str,
    key: &str,
    bytes: &[u8],
) -> Result<()> {
    let path = format!("/v9/snapshot/upload?key={}", urlencoding_encode(key));
    let res = build_client(HTTP_TIMEOUT_SNAPSHOT)?
        .post(url(base_url, &path))
        .bearer_auth(id_token)
        .header("Content-Type", "application/octet-stream")
        .body(bytes.to_vec())
        .send()
        .await
        .context("upload_snapshot send")?;
    let _ = parse_envelope(res, &path).await?;
    Ok(())
}

/// GET /v9/snapshot/fetch?key=... stream to Vec (caller tự write file nếu cần).
pub async fn fetch_snapshot(base_url: &str, id_token: &str, key: &str) -> Result<Vec<u8>> {
    let path = format!("/v9/snapshot/fetch?key={}", urlencoding_encode(key));
    let res = build_client(HTTP_TIMEOUT_SNAPSHOT)?
        .get(url(base_url, &path))
        .bearer_auth(id_token)
        .send()
        .await
        .context("fetch_snapshot send")?;
    let status = res.status();
    if !status.is_success() {
        let body = res.text().await.unwrap_or_default();
        return Err(anyhow!(
            "HTTP {} fetch_snapshot {key}: {body}",
            status.as_u16()
        ));
    }
    let bytes = res.bytes().await.context("read snapshot bytes")?;
    Ok(bytes.to_vec())
}

// =============================================================
// SYNC LOG
// =============================================================

/// POST /v9/sync-log/push?date=yyyy-mm-dd body: zstd NDJSON.
pub async fn push_sync_log(
    base_url: &str,
    id_token: &str,
    date: &str,
    bytes: &[u8],
) -> Result<String> {
    let path = format!("/v9/sync-log/push?date={date}");
    let res = build_client(HTTP_TIMEOUT)?
        .post(url(base_url, &path))
        .bearer_auth(id_token)
        .header("Content-Type", "application/octet-stream")
        .body(bytes.to_vec())
        .send()
        .await
        .context("push_sync_log send")?;
    let extra = parse_envelope(res, &path).await?;

    #[derive(Deserialize)]
    struct LogResp {
        key: String,
    }
    let body: LogResp = serde_json::from_value(extra).context("parse LogResp")?;
    Ok(body.key)
}

// =============================================================
// Utility
// =============================================================

fn urlencoding_encode(s: &str) -> String {
    // Minimal percent encoding cho path segment. Đủ cho key format v9
    // (`deltas/.../<digits>_<clock>.ndjson.zst`) không có unicode.
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '/' => out.push(c),
            _ => {
                let mut buf = [0u8; 4];
                for b in c.encode_utf8(&mut buf).as_bytes() {
                    out.push_str(&format!("%{b:02X}"));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_trims_trailing_slash() {
        assert_eq!(url("https://api.example.com/", "/v9/manifest/get"),
                   "https://api.example.com/v9/manifest/get");
        assert_eq!(url("https://api.example.com", "/v9/manifest/get"),
                   "https://api.example.com/v9/manifest/get");
    }

    #[test]
    fn urlencoding_preserves_safe_chars() {
        let key = "deltas/raw_shopee_clicks/5000_1745234600000.ndjson.zst";
        assert_eq!(urlencoding_encode(key), key);
    }

    #[test]
    fn urlencoding_escapes_unsafe() {
        assert_eq!(urlencoding_encode("a b"), "a%20b");
        assert_eq!(urlencoding_encode("a:b"), "a%3Ab");
    }

    #[test]
    fn cas_conflict_signal_consistent() {
        // Stringly-typed protocol với Tauri commands — đảm bảo không đổi.
        assert_eq!(CAS_CONFLICT, "V9_CAS_CONFLICT");
    }
}
