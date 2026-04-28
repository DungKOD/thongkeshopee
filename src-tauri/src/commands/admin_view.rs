//! Admin-only: xem DB của user khác (Phase B).
//!
//! Flow (v9): admin gọi `admin_view_user_db(target_uid, ...)`. Có 2 path:
//!
//! **Snapshot path** (target user đã compact ≥ 1 lần):
//! - Worker `GET /v9/admin/snapshot?uid=<uid>` stream zstd bytes của snapshot
//!   mới nhất → Rust decompress + ghi vào `app_data_dir/admin_view/<uid>.db`.
//!
//! **Delta-replay path** (target user chưa compact):
//! - Worker `GET /v9/admin/manifest?uid=<uid>` lấy manifest JSON.
//! - Init empty DB (schema + seed) tại `app_data_dir/admin_view/<uid>.db`.
//! - Sort `manifest.deltas` theo `clock_ms` ASC → fetch từng file qua
//!   `GET /v9/admin/delta-fetch?uid=<uid>&key=...` → parse + apply per
//!   transaction (idempotent, INSERT OR IGNORE + HLC).
//! - Path này đảm bảo admin xem được MỌI user đã sync ≥ 1 lần, không phụ
//!   thuộc vào việc target user có hit COMPACTION_DELTA_THRESHOLD hay chưa.
//!
//! Cả 2 path xong → mở connection read-only → swap `DbState`. Khi admin
//! exit → `admin_exit_view_user_db` reopen DB gốc ở chế độ RW với PRAGMA.
//!
//! Safety:
//! - Mở read-only (`SQLITE_OPEN_READ_ONLY`) → mọi mutation command fail
//!   với `SQLITE_READONLY`.
//! - Cloud sync PHẢI tắt ở FE (`useCloudSync({ enabled: false })`).
//! - AdminViewState lưu trong Tauri state (memory); restart app → state mất.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use tauri::{AppHandle, Manager, State};
use tokio::fs;

use super::{CmdError, CmdResult};
use crate::db::{init_db_at, resolve_active_db_path, DbState};
use crate::sync_v9::compress::zstd_decompress;
use crate::sync_v9::pull::{apply_events, parse_delta_file};
use crate::sync_v9::types::Manifest;

/// Tauri managed state — track user đang được admin xem.
/// `None` = chế độ bình thường (DB của chính admin).
pub struct AdminViewState(pub Mutex<Option<AdminViewInfo>>);

/// Metadata user đang được xem. FE dùng để render banner.
#[derive(Debug, Clone, Serialize)]
pub struct AdminViewInfo {
    pub uid: String,
    pub email: Option<String>,
    pub local_part: String,
    pub db_path: String,
    pub size_bytes: u64,
    pub last_modified_ms: i64,
    pub entered_at_ms: i64,
    /// Path DB của chính admin TRƯỚC khi swap sang snapshot — dùng ở
    /// `admin_exit_view_user_db` để reopen đúng folder (multi-tenant layout
    /// `users/{admin_uid}/thongkeshopee.db`, không hardcode root path).
    #[serde(skip)]
    pub admin_db_path_backup: PathBuf,
}

/// Resolve folder chứa snapshot DB của user đang được admin xem.
/// `app_data_dir/admin_view/`. Mỗi user 1 file `<uid>.db`.
fn admin_view_dir(app: &AppHandle) -> CmdResult<PathBuf> {
    let base = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?;
    Ok(base.join("admin_view"))
}

/// Admin-only: download DB của user target về local + swap `DbState` sang
/// read-only connection. Trả về metadata để FE hiển thị banner.
///
/// Worker verify admin qua `ADMIN_UIDS` secret (whitelist Firebase UID),
/// 403 nếu caller không phải admin, 404 nếu user target chưa có manifest.
///
/// Hai code path:
/// 1. **Snapshot fast-path** (target có `latest_snapshot`): fetch + decompress
///    snapshot bytes → file SQLite.
/// 2. **Delta replay** (chưa snapshot): init empty DB + apply tất cả deltas
///    theo thứ tự `clock_ms` ASC. Đảm bảo admin xem được mọi user đã push ≥ 1
///    lần, kể cả user dùng nhẹ chưa hit `COMPACTION_DELTA_THRESHOLD`.
#[tauri::command]
pub async fn admin_view_user_db(
    app: AppHandle,
    db: State<'_, DbState>,
    view_state: State<'_, AdminViewState>,
    sync_api_url: String,
    id_token: String,
    target_uid: String,
    target_local_part: String,
    target_email: Option<String>,
) -> CmdResult<AdminViewInfo> {
    // 1. Fetch manifest trước — quyết định snapshot path hay delta-replay path.
    let manifest = admin_fetch_user_manifest(&sync_api_url, &id_token, &target_uid)
        .await
        .map_err(|e| CmdError::msg(e.to_string()))?;

    // 2. Chuẩn bị file path + xóa stale WAL/SHM.
    let dir = admin_view_dir(&app)?;
    fs::create_dir_all(&dir).await.map_err(CmdError::from)?;
    let target_path = dir.join(format!("{target_uid}.db"));
    for ext in &["db-wal", "db-shm"] {
        let _ = fs::remove_file(target_path.with_extension(ext)).await;
    }
    // Xóa file cũ nếu có (lần view trước) — đảm bảo restore từ đầu.
    let _ = fs::remove_file(&target_path).await;

    // 3. Build DB ở `target_path`.
    let (size_bytes, last_modified_ms) = if let Some(snap) = manifest.latest_snapshot.as_ref() {
        // Snapshot fast-path.
        let compressed =
            admin_fetch_user_snapshot(&sync_api_url, &id_token, &target_uid)
                .await
                .map_err(|e| CmdError::msg(e.to_string()))?;
        let bytes = zstd_decompress(&compressed).map_err(|e| CmdError::msg(e.to_string()))?;
        eprintln!(
            "admin_view (snapshot): payload={} KB → sqlite={} KB",
            compressed.len() / 1024,
            bytes.len() / 1024,
        );
        fs::write(&target_path, &bytes).await.map_err(CmdError::from)?;
        (compressed.len() as u64, snap.clock_ms)
    } else if !manifest.deltas.is_empty() {
        // Delta replay path. Init schema + apply.
        replay_deltas_into_new_db(
            &target_path,
            &sync_api_url,
            &id_token,
            &target_uid,
            &manifest,
        )
        .await
        .map_err(|e| CmdError::msg(format!("delta replay: {e:#}")))?;
        let size = fs::metadata(&target_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        (size, manifest.updated_at_ms)
    } else {
        return Err(CmdError::msg(
            "Target user chưa có data nào trên R2 (manifest rỗng)".to_string(),
        ));
    };

    // 4. Mở connection read-only.
    let new_conn = Connection::open_with_flags(
        &target_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|e| CmdError::msg(format!("open RO: {e}")))?;
    new_conn
        .execute_batch("PRAGMA query_only = ON;")
        .map_err(CmdError::from)?;

    // 5. Swap DbState. Lưu path DB của admin TRƯỚC khi swap để `admin_exit`
    //    reopen đúng (compatible với layout `users/{uid}/`).
    let admin_db_path_backup: PathBuf = {
        let mut slot = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let backup = resolve_active_db_path(&slot)
            .map_err(|e| CmdError::msg(e.to_string()))?;
        *slot = new_conn;
        backup
    };

    // 6. Ghi AdminViewState.
    let info = AdminViewInfo {
        uid: target_uid,
        email: target_email,
        local_part: target_local_part,
        db_path: target_path.to_string_lossy().into_owned(),
        size_bytes,
        last_modified_ms,
        entered_at_ms: now_ms(),
        admin_db_path_backup,
    };
    {
        let mut slot = view_state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        *slot = Some(info.clone());
    }

    Ok(info)
}

/// Init schema fresh + apply tất cả deltas của target user theo `clock_ms`
/// ASC. Mỗi delta apply trong 1 TX (idempotent qua INSERT OR IGNORE + HLC).
/// Caller responsibility: target_path KHÔNG được tồn tại trước call (đã xóa
/// ở step 2 của `admin_view_user_db`).
async fn replay_deltas_into_new_db(
    target_path: &Path,
    sync_api_url: &str,
    id_token: &str,
    target_uid: &str,
    manifest: &Manifest,
) -> anyhow::Result<()> {
    // Init DB với schema + seed (block thread vì rusqlite không async).
    let path_owned = target_path.to_path_buf();
    let mut conn = tokio::task::spawn_blocking(move || init_db_at(&path_owned))
        .await
        .context("spawn_blocking init_db_at")?
        .context("init_db_at cho admin view")?;

    // Sort deltas theo clock_ms ASC (manifest invariant nhưng defensive).
    let mut entries = manifest.deltas.clone();
    entries.sort_by_key(|d| d.clock_ms);

    let total = entries.len();
    eprintln!("admin_view (delta replay): {total} delta files cho uid={target_uid}");

    let mut total_applied = 0u32;
    let mut total_bytes = 0u64;
    for (idx, entry) in entries.iter().enumerate() {
        let bytes =
            admin_fetch_delta(sync_api_url, id_token, target_uid, &entry.key)
                .await
                .with_context(|| format!("fetch delta {} ({}/{total})", entry.key, idx + 1))?;
        total_bytes += bytes.len() as u64;

        // Parse + apply trong blocking task (rusqlite không async).
        let bytes_owned = bytes;
        let stats = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let events = parse_delta_file(&bytes_owned).context("parse delta NDJSON")?;
            apply_events(&mut conn, &events).context("apply events vào admin view DB")?;
            Ok((conn, events.len() as u32))
        })
        .await
        .context("spawn_blocking apply_events")?
        .context("apply delta")?;
        conn = stats.0;
        total_applied += stats.1;
    }

    eprintln!(
        "admin_view (delta replay): applied {total_applied} events, {} KB compressed",
        total_bytes / 1024
    );
    Ok(())
}

/// Exit admin-view mode — reopen DB gốc read-write, swap lại, clear state.
#[tauri::command]
pub async fn admin_exit_view_user_db(
    db: State<'_, DbState>,
    view_state: State<'_, AdminViewState>,
) -> CmdResult<()> {
    // Lấy path DB admin đã lưu ở lúc enter. Nếu không có → không thể exit clean
    // (fallback không safe với layout per-user — user phải restart app).
    let path = {
        let slot = view_state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        slot.as_ref()
            .map(|info| info.admin_db_path_backup.clone())
            .ok_or_else(|| CmdError::msg("Không ở chế độ admin-view, không có path để reopen"))?
    };

    // Open DB gốc + apply PRAGMA như `db::init_db` (không migrate — đã làm lúc startup).
    let new_conn = Connection::open(&path)
        .map_err(|e| CmdError::msg(format!("open rw: {e}")))?;
    new_conn
        .execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA synchronous = NORMAL;
             PRAGMA temp_store = MEMORY;",
        )
        .map_err(CmdError::from)?;

    {
        let mut slot = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        *slot = new_conn;
    }
    {
        let mut slot = view_state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        *slot = None;
    }
    Ok(())
}

/// FE query: đang ở admin-view mode nào. `None` = bình thường.
#[tauri::command]
pub fn admin_view_state_get(
    view_state: State<'_, AdminViewState>,
) -> CmdResult<Option<AdminViewInfo>> {
    let slot = view_state
        .0
        .lock()
        .map_err(|_| CmdError::LockPoisoned)?;
    Ok(slot.clone())
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

const ADMIN_HTTP_TIMEOUT: Duration = Duration::from_secs(600);

/// Build `reqwest::Client` shared cho mọi admin HTTP call. Timeout 600s
/// đủ cho snapshot lớn (vài trăm MB) hoặc series delta files.
fn admin_http_client() -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(ADMIN_HTTP_TIMEOUT)
        .build()
        .context("build reqwest")
}

/// Fetch manifest JSON của target user qua Worker.
async fn admin_fetch_user_manifest(
    sync_api_url: &str,
    id_token: &str,
    target_uid: &str,
) -> anyhow::Result<Manifest> {
    let url = format!(
        "{}/v9/admin/manifest?uid={}",
        sync_api_url.trim_end_matches('/'),
        urlencoding_encode(target_uid),
    );
    let res = admin_http_client()?
        .get(&url)
        .bearer_auth(id_token)
        .send()
        .await
        .context("admin manifest fetch")?;
    let status = res.status();
    if !status.is_success() {
        let body = res.text().await.unwrap_or_default();
        if status.as_u16() == 404 {
            // Phân biệt 404 do route không tồn tại (Worker chưa deploy) vs
            // R2 thật sự không có manifest — body khác nhau.
            if body.contains("No route") {
                anyhow::bail!(
                    "Worker route /v9/admin/manifest chưa được deploy. Run: `cd worker && wrangler deploy`. Body: {body}"
                );
            }
            anyhow::bail!(
                "Target user chưa có manifest trên R2 (chưa sync v9 lần nào, hoặc data còn ở local máy user chưa push). Yêu cầu user mở app + bấm \"Đồng bộ ngay\" trong SyncBadge. Worker body: {body}"
            );
        }
        anyhow::bail!("HTTP {} admin manifest fetch: {body}", status.as_u16());
    }
    let manifest: Manifest = res.json().await.context("parse manifest JSON")?;
    Ok(manifest)
}

/// Fetch raw zstd bytes của 1 delta file qua admin endpoint.
async fn admin_fetch_delta(
    sync_api_url: &str,
    id_token: &str,
    target_uid: &str,
    delta_key: &str,
) -> anyhow::Result<Vec<u8>> {
    let url = format!(
        "{}/v9/admin/delta-fetch?uid={}&key={}",
        sync_api_url.trim_end_matches('/'),
        urlencoding_encode(target_uid),
        urlencoding_encode(delta_key),
    );
    let res = admin_http_client()?
        .get(&url)
        .bearer_auth(id_token)
        .send()
        .await
        .context("admin delta fetch")?;
    let status = res.status();
    if !status.is_success() {
        let body = res.text().await.unwrap_or_default();
        anyhow::bail!(
            "HTTP {} admin delta fetch ({delta_key}): {body}",
            status.as_u16()
        );
    }
    let bytes = res.bytes().await.context("read delta bytes")?;
    Ok(bytes.to_vec())
}

/// Fetch snapshot của user target từ Worker. Trả raw zstd bytes (compressed).
async fn admin_fetch_user_snapshot(
    sync_api_url: &str,
    id_token: &str,
    target_uid: &str,
) -> anyhow::Result<Vec<u8>> {
    let url = format!(
        "{}/v9/admin/snapshot?uid={}",
        sync_api_url.trim_end_matches('/'),
        urlencoding_encode(target_uid),
    );
    let res = admin_http_client()?
        .get(&url)
        .bearer_auth(id_token)
        .send()
        .await
        .context("admin snapshot fetch")?;
    let status = res.status();
    if !status.is_success() {
        let body = res.text().await.unwrap_or_default();
        if status.as_u16() == 404 {
            anyhow::bail!(
                "User target chưa có snapshot. Đáng lẽ caller đã check manifest.latest_snapshot trước."
            );
        }
        anyhow::bail!("HTTP {} admin snapshot fetch: {body}", status.as_u16());
    }
    let bytes = res.bytes().await.context("read snapshot bytes")?;
    Ok(bytes.to_vec())
}

fn urlencoding_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' => out.push(c),
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
