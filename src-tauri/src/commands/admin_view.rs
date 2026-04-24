//! Admin-only: xem DB của user khác (Phase B).
//!
//! Flow (v9): admin gọi `admin_view_user_db(target_uid, ...)` → Worker
//! `GET /v9/admin/snapshot?uid=<uid>` verify admin + stream zstd bytes của
//! snapshot mới nhất → Rust decompress + ghi vào
//! `app_data_dir/admin_view/<uid>.db` → mở connection read-only → swap
//! `DbState` sang connection mới (connection cũ drop, release handles).
//!
//! Khi admin exit → `admin_exit_view_user_db` reopen DB gốc ở chế độ RW
//! với PRAGMA.
//!
//! v9 snapshot = P10 compaction output. Nếu user target chưa compact lần
//! nào → Worker trả 404, admin hiện thấy error "chưa có snapshot".
//!
//! Safety:
//! - Mở read-only (`SQLITE_OPEN_READ_ONLY`) → mọi mutation command fail
//!   với `SQLITE_READONLY`.
//! - Cloud sync PHẢI tắt ở FE (`useCloudSync({ enabled: false })`).
//! - AdminViewState lưu trong Tauri state (memory); restart app → state mất.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use tauri::{AppHandle, Manager, State};
use tokio::fs;

use super::{CmdError, CmdResult};
use crate::db::{resolve_active_db_path, DbState};
use crate::sync_v9::compress::zstd_decompress;

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
/// 403 nếu caller không phải admin, 404 nếu user target chưa có backup trên R2.
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
    // 1. Gọi Worker GET /v9/admin/snapshot?uid=<uid> — verify admin + stream
    //    bytes snapshot mới nhất. 404 nếu user chưa compact lần nào.
    let (compressed, size_bytes, last_modified_ms) =
        admin_fetch_user_snapshot(&sync_api_url, &id_token, &target_uid)
            .await
            .map_err(|e| CmdError::msg(e.to_string()))?;

    // R2 lưu zstd bytes. Decompress trước khi write làm SQLite file.
    let bytes = zstd_decompress(&compressed).map_err(|e| CmdError::msg(e.to_string()))?;
    eprintln!(
        "admin_view download: payload={} KB → sqlite={} KB",
        compressed.len() / 1024,
        bytes.len() / 1024,
    );

    // 2. Ghi file ra disk.
    let dir = admin_view_dir(&app)?;
    fs::create_dir_all(&dir).await.map_err(CmdError::from)?;
    let target_path = dir.join(format!("{target_uid}.db"));

    // Xóa WAL/SHM cũ của lần view trước (nếu có) — tránh SQLite đọc nhầm.
    for ext in &["db-wal", "db-shm"] {
        let _ = fs::remove_file(target_path.with_extension(ext)).await;
    }
    fs::write(&target_path, &bytes).await.map_err(CmdError::from)?;

    // 3. Mở connection read-only. `SQLITE_OPEN_READ_ONLY` flag đảm bảo mọi
    //    statement INSERT/UPDATE/DELETE fail với SQLITE_READONLY — lớp bảo vệ
    //    cuối cùng phòng khi FE lộ nút mutation.
    let new_conn = Connection::open_with_flags(
        &target_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|e| CmdError::msg(format!("open RO: {e}")))?;

    // PRAGMA foreign_keys OK với read-only; journal_mode không đổi được nhưng
    // không cần (không ghi). query_only bật cho chắc.
    new_conn
        .execute_batch("PRAGMA query_only = ON;")
        .map_err(CmdError::from)?;

    // 4. Swap DbState — connection cũ drop khi out-of-scope. Lưu path DB
    //    của admin TRƯỚC khi swap để `admin_exit` reopen đúng (không hardcode
    //    root path, compatible với layout `users/{uid}/`).
    let admin_db_path_backup: PathBuf = {
        let mut slot = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let backup = resolve_active_db_path(&slot)
            .map_err(|e| CmdError::msg(e.to_string()))?;
        *slot = new_conn;
        backup
    };

    // 5. Ghi AdminViewState.
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

/// Fetch snapshot của user target từ Worker. Trả raw zstd bytes + size +
/// last_modified_ms (từ header `X-Snapshot-Clock-Ms` nếu có, fallback now).
///
/// 404 (chưa có snapshot) → error rõ ràng cho admin UI.
async fn admin_fetch_user_snapshot(
    sync_api_url: &str,
    id_token: &str,
    target_uid: &str,
) -> anyhow::Result<(Vec<u8>, u64, i64)> {
    let url = format!(
        "{}/v9/admin/snapshot?uid={}",
        sync_api_url.trim_end_matches('/'),
        urlencoding_encode(target_uid),
    );
    let client = reqwest::Client::builder()
        .timeout(ADMIN_HTTP_TIMEOUT)
        .build()
        .context("build reqwest")?;
    let res = client
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
                "User target chưa có snapshot (chưa chạy compaction P10). Chỉ xem được user đã có snapshot."
            );
        }
        anyhow::bail!("HTTP {} admin snapshot fetch: {body}", status.as_u16());
    }
    let snapshot_clock_ms: i64 = res
        .headers()
        .get("X-Snapshot-Clock-Ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or_else(now_ms);
    let bytes = res.bytes().await.context("read snapshot bytes")?;
    let size_bytes = bytes.len() as u64;
    Ok((bytes.to_vec(), size_bytes, snapshot_clock_ms))
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
