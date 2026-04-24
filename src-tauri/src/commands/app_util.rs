//! App utility commands — multi-tenant DB swap, machine fingerprint,
//! restart helper, admin user list (v9 HTTP).
//!
//! Các helpers này từng ở `commands/sync.rs` (v8). P8b move sang đây vì
//! không còn thuộc sync layer — chúng là app lifecycle utilities dùng chung.
//! v9 sync riêng ở `commands/sync_v9_cmds.rs` + `sync_v9/`.

use md5::{Digest, Md5};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tauri::{AppHandle, Manager, State};

use crate::db::{resolve_active_db_path, resolve_active_imports_dir, DbState, VideoDbState};

use super::{CmdError, CmdResult};

// =============================================================
// Machine fingerprint
// =============================================================

/// Raw fingerprint (MD5 hex của os+machine_uid). Stable across app rename /
/// reinstall. Non-PII — không chứa hostname / username / IP.
///
/// Dùng cho `sync_event_log.fingerprint` + admin debug. v9 push path có
/// compute riêng trong `sync_v9_cmds::machine_fingerprint_stable` (SHA-256
/// truncated); hàm này giữ shape MD5 hex cũ để không phá event_log cũ.
pub fn machine_fingerprint_raw() -> String {
    let os = std::env::consts::OS;
    let machine_id = machine_uid::get().unwrap_or_else(|_| "unknown-uid".into());
    let input = format!("{os}|{machine_id}");
    let digest = Md5::digest(input.as_bytes());
    hex::encode(digest)
}

/// Tauri command — FE query fingerprint của máy hiện tại.
#[tauri::command]
pub fn machine_fingerprint() -> String {
    machine_fingerprint_raw()
}

// =============================================================
// App data paths (UI debug / support)
// =============================================================

/// Đường dẫn data app lưu local — phục vụ UI "Copy path" cho support/debug.
/// Tất cả đều là absolute path, UTF-8 string (Windows backslashes preserved).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppDataPaths {
    /// Root folder app data — chứa mọi thứ app persist.
    pub app_data_dir: String,
    /// DB hiện tại đang mở (user-scoped: `users/{uid}/thongkeshopee.db`).
    pub active_db_path: String,
    /// Folder CSV imports của user hiện tại.
    pub active_imports_dir: String,
}

/// Query đường dẫn data app cho UI. Gọi khi mở SettingsDialog.
#[tauri::command]
pub fn get_app_data_paths(
    app: AppHandle,
    db: State<'_, DbState>,
) -> CmdResult<AppDataPaths> {
    let app_data_dir = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?
        .to_string_lossy()
        .to_string();
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let active_db_path = resolve_active_db_path(&conn)
        .map_err(|e| CmdError::msg(e.to_string()))?
        .to_string_lossy()
        .to_string();
    let active_imports_dir = resolve_active_imports_dir(&conn)
        .map_err(|e| CmdError::msg(e.to_string()))?
        .to_string_lossy()
        .to_string();
    Ok(AppDataPaths {
        app_data_dir,
        active_db_path,
        active_imports_dir,
    })
}

// =============================================================
// Persistent net log — append request entry vào file daily
// =============================================================

/// Folder chứa daily net log files. `{app_data}/net_log/`.
const NET_LOG_SUBDIR: &str = "net_log";

/// Payload từ FE — 1 entry cho 1 request log.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetLogPayload {
    pub ts_start: String,        // ISO timestamp
    pub duration_ms: u64,
    pub kind: String,            // "tauri_sync_push" | "firebase_token" | etc.
    pub label: String,           // "sync_v9_push_all" | etc.
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub bytes: Option<u64>,
    #[serde(default)]
    pub meta: Option<serde_json::Value>,
}

/// Resolve path file log của ngày (YYYY-MM-DD). Tạo folder nếu chưa có.
fn resolve_net_log_path(app: &AppHandle, date: &str) -> anyhow::Result<std::path::PathBuf> {
    let base = app
        .path()
        .app_data_dir()
        .map_err(|e| anyhow::anyhow!("app_data_dir: {e}"))?;
    let dir = base.join(NET_LOG_SUBDIR);
    std::fs::create_dir_all(&dir)?;
    Ok(dir.join(format!("{date}.log")))
}

/// Format 1 line cho file. Plain text TSV-like, dễ grep/đọc bằng tay.
/// Format: `[ISO_TS] [KIND] LABEL dur=Nms ok=B [bytes=X] [error="..."] [meta={...}]`
fn format_log_line(p: &NetLogPayload) -> String {
    let mut line = format!(
        "[{}] [{}] {} dur={}ms ok={}",
        p.ts_start, p.kind, p.label, p.duration_ms, p.ok
    );
    if let Some(b) = p.bytes {
        line.push_str(&format!(" bytes={b}"));
    }
    if let Some(err) = &p.error {
        // Escape newlines + quote để 1 entry = 1 line.
        let safe = err.replace('\n', "\\n").replace('"', "\\\"");
        line.push_str(&format!(" error=\"{safe}\""));
    }
    if let Some(meta) = &p.meta {
        if !meta.is_null() {
            let s = serde_json::to_string(meta).unwrap_or_default();
            line.push_str(&format!(" meta={s}"));
        }
    }
    line.push('\n');
    line
}

/// Append 1 request log entry vào file daily. Fire-and-forget — FE không
/// chờ kết quả, lỗi I/O chỉ log console (không break user flow).
///
/// File: `{app_data}/net_log/YYYY-MM-DD.log`. Per-day rotation tự động —
/// mỗi date mới ghi vào file riêng. User có thể đọc/grep/phân tích sau.
#[tauri::command]
pub fn app_log_request(app: AppHandle, payload: NetLogPayload) -> CmdResult<()> {
    use std::io::Write;
    // Date từ ts_start (YYYY-MM-DD prefix). Fallback "unknown" nếu format sai.
    let date = payload.ts_start.get(..10).unwrap_or("unknown");
    let path = resolve_net_log_path(&app, date)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    let line = format_log_line(&payload);
    // OpenOptions append mode — concurrent writes safe ở OS level cho 1 line.
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| CmdError::msg(format!("open log file {}: {e}", path.display())))?;
    file.write_all(line.as_bytes())
        .map_err(|e| CmdError::msg(format!("write log: {e}")))?;
    Ok(())
}

/// Resolve path folder net log — UI hiển thị cho user mở Explorer.
#[tauri::command]
pub fn get_net_log_dir(app: AppHandle) -> CmdResult<String> {
    let base = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?;
    let dir = base.join(NET_LOG_SUBDIR);
    std::fs::create_dir_all(&dir)
        .map_err(|e| CmdError::msg(format!("create dir: {e}")))?;
    Ok(dir.to_string_lossy().to_string())
}

// =============================================================
// App restart
// =============================================================

/// Restart app — dùng sau khi download DB mới xong (P8b: còn lại cho FE
/// sau lỗi nặng cần reload toàn bộ state).
#[tauri::command]
pub fn restart_app(app: AppHandle) {
    app.restart();
}

// =============================================================
// Multi-tenant DB swap (live DB switching khi user login)
// =============================================================

/// Swap `<live>.pending.db` → `<live>` nếu có pending từ lần download trước.
/// Gọi ở `switch_db_to_user` trước khi open user DB.
///
/// v9 sync không dùng pending.db flow (snapshot restore qua
/// `sync_v9/snapshot::restore_snapshot_to_pending` có path riêng). Helper
/// này giữ cho legacy migration — user từ v8 upgrade lên v9 có thể có
/// pending.db sót lại từ lần download cũ.
pub fn apply_pending_sync(live_path: &std::path::Path) -> anyhow::Result<bool> {
    use rusqlite::Connection;
    use std::fs as std_fs;

    let pending = live_path.with_extension("pending.db");
    if !pending.exists() {
        return Ok(false);
    }

    // Validate pending DB integrity TRƯỚC khi swap — tránh file corrupt
    // làm app crash ở startup.
    {
        let conn = Connection::open(&pending)
            .map_err(|e| anyhow::anyhow!("mở pending DB thất bại: {e}"))?;
        let result: String = conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .map_err(|e| anyhow::anyhow!("integrity_check failed: {e}"))?;
        if result != "ok" {
            let _ = std_fs::remove_file(&pending);
            anyhow::bail!(
                "pending DB corrupt ({}), đã xóa — giữ live DB cũ an toàn",
                result
            );
        }
    }

    // Backup live DB sang .pre-restore.db để user rollback nếu muốn.
    if live_path.exists() {
        let backup = live_path.with_extension("pre-restore.db");
        let _ = std_fs::remove_file(&backup);
        std_fs::rename(live_path, &backup)?;
    }

    // WAL và SHM cũ không còn hợp lệ với DB mới → xóa.
    for ext in &["db-wal", "db-shm"] {
        let aux = live_path.with_extension(ext);
        let _ = std_fs::remove_file(aux);
    }

    std_fs::rename(&pending, live_path)?;
    Ok(true)
}

/// Multi-tenant switch — swap DbState + VideoDbState sang `users/{uid}/` folder.
///
/// Flow:
/// 1. Resolve user DB path + imports folder.
/// 2. Legacy migrate root DB → user folder nếu lần đầu login sau upgrade.
/// 3. Apply pending.db nếu tồn tại.
/// 4. Check owner_uid cũ trong DbState để trả `owner_changed` flag.
/// 5. Open user DB với schema + migrations, stamp owner_uid.
/// 6. Swap DbState + VideoDbState connections.
///
/// Trả `owner_changed = true` nếu UID khác session DbState trước đó. FE
/// dùng flag này để clear localStorage + refetch UI (không leak data user A
/// sang user B trên cùng máy).
#[tauri::command]
pub async fn switch_db_to_user(
    app: AppHandle,
    db: State<'_, DbState>,
    video_db: State<'_, VideoDbState>,
    new_uid: String,
) -> CmdResult<bool> {
    if new_uid.is_empty() {
        return Err(CmdError::msg("new_uid rỗng — phải là Firebase UID hợp lệ"));
    }

    // 1. Resolve path của user DB mới.
    let user_db_path = crate::db::resolve_db_path_for_user(&app, &new_uid)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    let user_imports_dir = crate::db::resolve_imports_dir_for_user(&app, &new_uid)
        .map_err(|e| CmdError::msg(e.to_string()))?;

    // 2. Migration từ legacy root DB (chỉ lần đầu sau khi upgrade lên v7+).
    //    Điều kiện an toàn: user folder chưa có DB VÀ root DB tồn tại VÀ
    //    root.owner_uid khớp new_uid (hoặc null). Nếu root thuộc user khác
    //    thì KHÔNG đụng — để lần họ login sau tự migrate.
    use std::fs as std_fs;
    if !user_db_path.exists() {
        if let Ok(legacy_db) = crate::db::resolve_legacy_db_path(&app) {
            if legacy_db.exists() && legacy_db != user_db_path {
                let legacy_owner: Option<String> =
                    rusqlite::Connection::open(&legacy_db)
                        .ok()
                        .and_then(|c| {
                            c.query_row(
                                "SELECT owner_uid FROM sync_state WHERE id = 1",
                                [],
                                |r| r.get::<_, Option<String>>(0),
                            )
                            .ok()
                            .flatten()
                        });
                let can_migrate = match legacy_owner.as_deref() {
                    Some(owner) => owner == new_uid,
                    None => true, // pre-v7 DB không có owner_uid → assume current user
                };
                if can_migrate {
                    eprintln!(
                        "[switch_db] migrating legacy root DB → {}",
                        user_db_path.display()
                    );
                    let _ = std_fs::rename(&legacy_db, &user_db_path);
                    for ext in &["db-wal", "db-shm"] {
                        let src = legacy_db.with_extension(ext);
                        if src.exists() {
                            let _ = std_fs::rename(&src, user_db_path.with_extension(ext));
                        }
                    }
                    if let Ok(legacy_imports) = crate::db::resolve_legacy_imports_dir(&app) {
                        if legacy_imports.exists() && legacy_imports != user_imports_dir {
                            if let Ok(entries) = std_fs::read_dir(&legacy_imports) {
                                for entry in entries.flatten() {
                                    let name = entry.file_name();
                                    let dst = user_imports_dir.join(&name);
                                    let _ = std_fs::rename(entry.path(), &dst);
                                }
                                let _ = std_fs::remove_dir(&legacy_imports);
                            }
                        }
                    }
                }
            }
        }
    }

    // 3. Apply pending.db nếu có.
    apply_pending_sync(&user_db_path).map_err(|e| CmdError::msg(e.to_string()))?;

    // 4. Check owner hiện tại trong DbState để trả `owner_changed`.
    let old_owner: Option<String> = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        conn.query_row(
            "SELECT owner_uid FROM sync_state WHERE id = 1",
            [],
            |r| r.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten()
    };
    let owner_changed = old_owner.as_deref() != Some(new_uid.as_str());

    // 5. Open user DB với schema + migrations.
    let new_conn = crate::db::init_db_at(&user_db_path)
        .map_err(|e| CmdError::msg(e.to_string()))?;

    // 6. Stamp owner_uid (fresh DB từ migrations đã có sync_state row seed).
    new_conn.execute(
        "UPDATE sync_state SET owner_uid = ?1 WHERE id = 1",
        params![new_uid],
    )?;

    // 7. Swap vào DbState — connection cũ drop, lock release files.
    {
        let mut slot = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        *slot = new_conn;
    }

    // 7b. Invalidate sync_v9 manifest cache — cache cũ của user khác hoặc
    // session trước, không valid cho user mới. Tránh leak hoặc CAS sai.
    crate::sync_v9::manifest_cache::cache_invalidate();

    // 8. Swap video DB (v8+ multi-tenant video logs per-user).
    let user_video_db_path = crate::db::video_db::resolve_video_db_path_for_user(&app, &new_uid)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    if !user_video_db_path.exists() {
        if let Ok(legacy_video) = crate::db::video_db::resolve_legacy_video_db_path(&app) {
            if legacy_video.exists() && legacy_video != user_video_db_path {
                eprintln!(
                    "[switch_db] migrating legacy video DB → {}",
                    user_video_db_path.display()
                );
                let _ = std_fs::rename(&legacy_video, &user_video_db_path);
                for ext in &["db-wal", "db-shm"] {
                    let src = legacy_video.with_extension(ext);
                    if src.exists() {
                        let _ = std_fs::rename(&src, user_video_db_path.with_extension(ext));
                    }
                }
            }
        }
    }
    let new_video_conn = crate::db::video_db::init_video_db_at(&user_video_db_path)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    {
        let mut slot = video_db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        *slot = new_video_conn;
    }

    Ok(owner_changed)
}

// =============================================================
// Admin user list (v9 Worker /v9/admin/users)
// =============================================================

/// Sync meta of 1 user — derived từ R2 object presence (manifest.json +
/// snapshots/ prefix). Null khi user chưa sync lần nào.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserListSyncMeta {
    pub has_manifest: bool,
    pub has_snapshot: bool,
    pub last_modified_ms: Option<i64>,
}

/// Entry trong admin user list — v9 shape. `sync` = null nếu user chưa sync
/// (không có manifest.json trên R2).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserListEntry {
    pub uid: String,
    pub email: Option<String>,
    pub local_part: Option<String>,
    #[serde(default)]
    pub premium: bool,
    #[serde(default)]
    pub admin: bool,
    pub expired_at: Option<String>,
    pub created_at: Option<String>,
    pub sync: Option<UserListSyncMeta>,
}

#[derive(Debug, Deserialize)]
struct UsersResponseBody {
    #[serde(default)]
    users: Vec<UserListEntry>,
}

#[derive(Debug, Deserialize)]
struct V9Envelope {
    ok: bool,
    #[serde(default)]
    code: Option<u16>,
    #[serde(default)]
    error: Option<String>,
    #[serde(flatten)]
    extra: serde_json::Value,
}

const ADMIN_HTTP_TIMEOUT: Duration = Duration::from_secs(120);

/// Admin-only: list toàn bộ users + sync metadata trên R2. Worker verify
/// admin qua claim / env `ADMIN_UIDS` (multi-source).
///
/// Thay thế v8 `/admin/users` → `/v9/admin/users`. Shape `sync: {...}` thay
/// cho `file: {...}` của v8 (v9 không còn single-DB file).
#[tauri::command]
pub async fn admin_list_users(
    sync_api_url: String,
    id_token: String,
) -> CmdResult<Vec<UserListEntry>> {
    let client = reqwest::Client::builder()
        .timeout(ADMIN_HTTP_TIMEOUT)
        .build()
        .map_err(CmdError::from)?;
    let url = format!(
        "{}/v9/admin/users",
        sync_api_url.trim_end_matches('/')
    );
    let res = client
        .post(&url)
        .bearer_auth(&id_token)
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .map_err(CmdError::from)?;
    let status = res.status();
    let text = res.text().await.map_err(CmdError::from)?;
    if !status.is_success() {
        let detail = serde_json::from_str::<V9Envelope>(&text)
            .ok()
            .and_then(|e| e.error)
            .unwrap_or_else(|| text.clone());
        return Err(CmdError::msg(format!(
            "HTTP {} /v9/admin/users: {detail}",
            status.as_u16()
        )));
    }
    let env: V9Envelope = serde_json::from_str(&text)
        .map_err(|e| CmdError::msg(format!("parse envelope: {e} — body: {text}")))?;
    if !env.ok {
        let code = env.code.unwrap_or(500);
        let msg = env.error.unwrap_or_else(|| "unknown".into());
        return Err(CmdError::msg(format!("Worker {code} /v9/admin/users: {msg}")));
    }
    let body: UsersResponseBody = serde_json::from_value(env.extra)
        .map_err(|e| CmdError::msg(format!("parse UsersResponseBody: {e}")))?;
    Ok(body.users)
}
