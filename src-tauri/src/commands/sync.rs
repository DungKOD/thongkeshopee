//! DB sync commands — backup SQLite lên Cloudflare R2 qua Worker proxy.
//!
//! Transport layer trong `sync_client`. File này giữ business logic:
//! - Tauri command handlers (`sync_*`, `admin_*`)
//! - sync_state CRUD (dirty flag, change_id, last_synced_*)
//! - Snapshot tạo bằng `VACUUM INTO` (consistent, không đụng WAL)
//! - gzip compress/decompress với backward-compat cho SQLite raw cũ
//! - Pull-merge-push flow (cross-device safe, tombstones CASCADE)
//! - Guard: fresh-install + remote có data → reject upload, route sang merge

use std::io::{Read, Write};
use std::path::PathBuf;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use md5::{Digest, Md5};
use rusqlite::params;
use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::fs;

use super::sync_client::{self, UserListEntry};
use super::{CmdError, CmdResult};
use crate::db::DbState;

/// Magic bytes đầu file SQLite — "SQLite format 3\0". Backward-compat detection:
/// file cũ (uncompressed) bắt đầu bằng magic này, file mới (gzipped) bằng `1f 8b`.
const SQLITE_MAGIC: [u8; 6] = [0x53, 0x51, 0x4C, 0x69, 0x74, 0x65];
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Gzip compress level 3 — fast, SQLite (nhiều repetition) nén ~3-4× là đủ.
fn gzip_compress(input: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(input.len() / 3), Compression::new(3));
    encoder.write_all(input)?;
    encoder.finish()
}

/// Detect magic + decompress nếu cần. Backward compat: bản DB cũ (raw SQLite
/// chưa gzip) upload lên Drive thời kỳ đầu vẫn restore được.
/// Pub(crate) để `admin_view::admin_view_user_db` reuse — payload base64 từ
/// Worker luôn gzipped, cần decompress trước khi write làm SQLite file.
pub(crate) fn gunzip_if_needed(input: &[u8]) -> CmdResult<Vec<u8>> {
    if input.len() >= 2 && input[0..2] == GZIP_MAGIC {
        let mut decoder = GzDecoder::new(input);
        let mut out = Vec::with_capacity(input.len() * 3);
        decoder
            .read_to_end(&mut out)
            .map_err(|e| CmdError::msg(format!("gunzip: {e}")))?;
        return Ok(out);
    }
    if input.len() >= SQLITE_MAGIC.len() && input[0..SQLITE_MAGIC.len()] == SQLITE_MAGIC {
        return Ok(input.to_vec());
    }
    let head_hex: String = input
        .iter()
        .take(16)
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ");
    Err(CmdError::msg(format!(
        "payload không phải SQLite/gzip — size={} bytes, head=[{}]",
        input.len(),
        head_hex,
    )))
}

#[derive(Debug, Serialize)]
pub struct SyncMetadataResult {
    pub exists: bool,
    pub file_id: Option<String>,
    pub size_bytes: Option<u64>,
    pub last_modified_ms: Option<i64>,
    pub fingerprint: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SyncUploadResult {
    pub file_id: String,
    pub size_bytes: u64,
    pub last_modified_ms: i64,
    pub fingerprint: String,
}

#[derive(Debug, Serialize)]
pub struct SyncDownloadResult {
    pub target_path: String,
    pub size_bytes: u64,
    pub last_modified_ms: i64,
}

#[derive(Debug, Serialize)]
pub struct SyncState {
    pub dirty: bool,
    /// Mutation counter — tăng mỗi lần ghi DB. =0 nghĩa là DB **fresh**
    /// (chưa có mutation nào từ install này) → FE dùng để detect scenario
    /// "reinstall trên máy cũ" và BẮT BUỘC pull-merge-push thay vì upload đè.
    #[serde(rename = "changeId")]
    pub change_id: i64,
    #[serde(rename = "lastUploadedChangeId")]
    pub last_uploaded_change_id: i64,
    pub last_synced_at_ms: Option<i64>,
    pub last_synced_remote_mtime_ms: Option<i64>,
    pub last_error: Option<String>,
    /// Firebase UID cuối cùng sở hữu/sync DB này. Null = DB vừa init chưa có
    /// user (pre-migration). FE compare với current user — khác = wipe.
    #[serde(rename = "ownerUid")]
    pub owner_uid: Option<String>,
}

/// Compute machine fingerprint: MD5(os | hostname | machine-uid) → hex string.
/// Ổn định across restarts trên cùng máy, khác giữa các máy khác nhau.
pub fn machine_fingerprint_raw() -> String {
    let os = std::env::consts::OS;
    let hostname = whoami_hostname().unwrap_or_else(|| "unknown-host".into());
    let machine_id = machine_uid::get().unwrap_or_else(|_| "unknown-uid".into());
    let input = format!("{os}|{hostname}|{machine_id}");
    let digest = Md5::digest(input.as_bytes());
    hex::encode(digest)
}

fn whoami_hostname() -> Option<String> {
    std::env::var("COMPUTERNAME")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
}

/// Tauri command — frontend query fingerprint của máy hiện tại.
#[tauri::command]
pub fn machine_fingerprint() -> String {
    machine_fingerprint_raw()
}

/// Metadata-only check (HEAD object trên R2). Không tạo gì cả — R2 lazy-create
/// khi upload, không cần endpoint "checkOrCreate" như Drive cũ.
#[tauri::command]
pub async fn sync_metadata(
    sync_api_url: String,
    id_token: String,
) -> CmdResult<SyncMetadataResult> {
    let m = sync_client::metadata(&sync_api_url, &id_token).await?;
    Ok(SyncMetadataResult {
        exists: m.exists,
        file_id: m.file_id,
        size_bytes: m.size_bytes,
        last_modified_ms: m.last_modified,
        fingerprint: m.fingerprint,
    })
}

/// Đọc trạng thái sync từ bảng `sync_state` (singleton row).
#[tauri::command]
pub async fn sync_state_get(db: State<'_, DbState>) -> CmdResult<SyncState> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let row = conn.query_row(
        "SELECT dirty, change_id, last_uploaded_change_id,
                last_synced_at_ms, last_synced_remote_mtime_ms, last_error, owner_uid
         FROM sync_state WHERE id = 1",
        [],
        |r| {
            Ok((
                r.get::<_, i64>(0)? != 0,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, Option<i64>>(3)?,
                r.get::<_, Option<i64>>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, Option<String>>(6)?,
            ))
        },
    )?;
    Ok(SyncState {
        dirty: row.0,
        change_id: row.1,
        last_uploaded_change_id: row.2,
        last_synced_at_ms: row.3,
        last_synced_remote_mtime_ms: row.4,
        last_error: row.5,
        owner_uid: row.6,
    })
}

/// Multi-tenant switch — swap DbState connection sang `users/{uid}/` folder.
///
/// Flow:
/// 1. Resolve user DB path: `{app_data}/users/{uid}/thongkeshopee.db`.
/// 2. Nếu user folder chưa có + legacy root DB tồn tại + owner_uid khớp (hoặc
///    null = pre-v7) → migrate: move root DB + imports/ sang user folder.
/// 3. Apply pending.db nếu có (user download lần login trước, chưa apply).
/// 4. Open connection, apply schema + migrations, seed Default account,
///    stamp owner_uid trong sync_state.
/// 5. Swap vào DbState (drop connection cũ).
///
/// Trả `owner_changed = true` nếu UID khác so với session DbState trước đó.
/// FE dùng flag này để clear localStorage + refetch UI.
#[tauri::command]
pub async fn switch_db_to_user(
    app: AppHandle,
    db: State<'_, DbState>,
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
                    // Move DB file + WAL/SHM aux files.
                    let _ = std_fs::rename(&legacy_db, &user_db_path);
                    for ext in &["db-wal", "db-shm"] {
                        let src = legacy_db.with_extension(ext);
                        if src.exists() {
                            let _ = std_fs::rename(&src, user_db_path.with_extension(ext));
                        }
                    }
                    // Move imports folder.
                    if let Ok(legacy_imports) = crate::db::resolve_legacy_imports_dir(&app) {
                        if legacy_imports.exists() && legacy_imports != user_imports_dir {
                            // Move từng file để tránh fail nếu target dir tồn tại.
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

    // 3. Apply pending.db nếu có (user download DB lần trước, chưa apply).
    apply_pending_sync(&user_db_path).map_err(|e| CmdError::msg(e.to_string()))?;

    // 4. Check owner hiện tại trong DbState (để trả flag owner_changed cho FE).
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

    Ok(owner_changed)
}

/// Ghi lỗi sync vào sync_state.last_error (không đổi dirty). Dùng khi upload fail.
#[tauri::command]
pub async fn sync_state_record_error(
    db: State<'_, DbState>,
    message: String,
) -> CmdResult<()> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "UPDATE sync_state SET last_error = ?1 WHERE id = 1",
        params![message],
    )?;
    Ok(())
}

/// Upload snapshot DB lên R2. VACUUM INTO tạo file consistent (không đụng WAL).
///
/// DEFENSIVE GUARD: nếu `change_id = 0 AND last_uploaded_change_id = 0`, DB là
/// fresh (reinstall chưa có mutation) → reject upload để không đè data có sẵn
/// trên R2. FE có trách nhiệm route sang `sync_pull_merge_push`.
#[tauri::command]
pub async fn sync_upload_db(
    app: AppHandle,
    db: State<'_, DbState>,
    sync_api_url: String,
    id_token: String,
    remote_exists: bool,
) -> CmdResult<SyncUploadResult> {
    let _ = app.emit("sync-phase", "uploading");
    let snapshot_path = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?
        .join("thongkeshopee.backup.db");

    let _ = fs::remove_file(&snapshot_path).await;

    let change_id_at_snapshot: i64 = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let (change_id, last_uploaded): (i64, i64) = conn.query_row(
            "SELECT change_id, last_uploaded_change_id FROM sync_state WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        // Chặn case duy nhất gây mất data: local fresh (chưa có mutation nào
        // từ install này) + remote ĐÃ có data → upload sẽ đè mất backup cũ.
        // User mới hoàn toàn (remote_exists=false) vẫn upload được để khởi tạo.
        if change_id == 0 && last_uploaded == 0 && remote_exists {
            return Err(CmdError::msg(
                "DB fresh + R2 đã có data — route sang sync_pull_merge_push để restore, tránh đè.",
            ));
        }
        let path_str = snapshot_path
            .to_str()
            .ok_or_else(|| CmdError::msg("snapshot path không phải UTF-8"))?;
        conn.execute("VACUUM INTO ?1", params![path_str])
            .map_err(CmdError::from)?;
        change_id
    };

    let bytes = fs::read(&snapshot_path).await.map_err(CmdError::from)?;
    let raw_len = bytes.len();
    let mtime_ms = now_ms();
    let fingerprint = machine_fingerprint_raw();

    let gzipped = gzip_compress(&bytes)
        .map_err(|e| CmdError::msg(format!("gzip: {e}")))?;
    eprintln!(
        "sync upload: raw={} KB → gzipped={} KB ({:.1}%)",
        raw_len / 1024,
        gzipped.len() / 1024,
        (gzipped.len() as f64 / raw_len.max(1) as f64) * 100.0,
    );
    let base64 = BASE64.encode(&gzipped);

    let res = sync_client::upload(&sync_api_url, &id_token, &base64, mtime_ms, &fingerprint)
        .await?;

    let _ = fs::remove_file(&snapshot_path).await;
    let remote_mtime = res.last_modified;

    // CAS clear dirty CHỈ KHI change_id chưa tăng từ snapshot — tránh race với
    // mutation xảy ra trong lúc upload.
    {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        conn.execute(
            "UPDATE sync_state
             SET dirty = CASE WHEN change_id > ?1 THEN 1 ELSE 0 END,
                 last_uploaded_change_id = ?1,
                 last_synced_at_ms = ?2,
                 last_synced_remote_mtime_ms = ?3,
                 last_error = NULL
             WHERE id = 1",
            params![change_id_at_snapshot, now_ms(), remote_mtime],
        )?;
    }

    Ok(SyncUploadResult {
        file_id: res.file_id,
        size_bytes: res.size_bytes,
        last_modified_ms: remote_mtime,
        fingerprint: res.fingerprint,
    })
}

/// Download DB từ R2, ghi vào `<active_db>.pending.db` (cùng folder user).
/// KHÔNG ghi đè DB đang live — FE phải prompt user restart app để apply.
#[tauri::command]
pub async fn sync_download_db(
    db: State<'_, DbState>,
    sync_api_url: String,
    id_token: String,
) -> CmdResult<SyncDownloadResult> {
    let dl = sync_client::download(&sync_api_url, &id_token).await?;

    let raw_payload = BASE64
        .decode(dl.base64_data.as_bytes())
        .map_err(|e| CmdError::msg(format!("base64 decode: {e}")))?;

    let bytes = gunzip_if_needed(&raw_payload)?;
    eprintln!(
        "sync_download_db: payload={} KB → sqlite={} KB",
        raw_payload.len() / 1024,
        bytes.len() / 1024,
    );

    let target = pending_db_path(&db)?;
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).await.map_err(CmdError::from)?;
    }
    fs::write(&target, &bytes).await.map_err(CmdError::from)?;

    Ok(SyncDownloadResult {
        target_path: target.to_string_lossy().into_owned(),
        size_bytes: bytes.len() as u64,
        last_modified_ms: dl.last_modified,
    })
}

/// Apply pending DB file (từ `sync_download_db`) — rename thành DB chính.
/// Dùng user-scoped pending path (resolve từ active DB path).
#[tauri::command]
pub async fn sync_apply_pending(db: State<'_, DbState>) -> CmdResult<bool> {
    let live = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        crate::db::resolve_active_db_path(&conn)
            .map_err(|e| CmdError::msg(e.to_string()))?
    };
    apply_pending_sync(&live).map_err(|e| CmdError::msg(e.to_string()))
}

/// Swap `<live>.pending.db` → `<live>` sau khi user download DB về.
/// Pending nằm cùng folder với live để rename atomic trong filesystem.
/// Gọi từ `sync_apply_pending` (manual trigger) hoặc `switch_db_to_user`
/// (apply trước khi open user DB nếu download đã xảy ra lần login trước).
pub fn apply_pending_sync(live_path: &std::path::Path) -> anyhow::Result<bool> {
    use rusqlite::Connection;
    use std::fs as std_fs;

    let pending = live_path.with_extension("pending.db");
    if !pending.exists() {
        return Ok(false);
    }

    // Validate pending DB integrity TRƯỚC khi swap — tránh file corrupt
    // (download dở, gzip partial, disk error) làm app crash ở startup.
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

/// Restart app — dùng sau khi `sync_download_db` ghi file pending.
/// `apply_pending_sync` chạy trong `setup()` trước `db::init_db` sẽ swap pending → live.
#[tauri::command]
pub fn restart_app(app: AppHandle) {
    app.restart();
}

/// Admin-only: list toàn bộ users + metadata file trên R2.
/// Worker verify admin qua claim/Firestore/env `ADMIN_UIDS` (multi-source).
#[tauri::command]
pub async fn admin_list_users(
    sync_api_url: String,
    id_token: String,
) -> CmdResult<Vec<UserListEntry>> {
    sync_client::admin_list_users(&sync_api_url, &id_token).await
}

/// Admin-only: xóa R2 orphan files (UID không còn trong Firestore).
/// Dọn data cũ sau khi đổi Firebase project. Trả list UIDs đã xóa.
#[tauri::command]
pub async fn admin_cleanup_orphans(
    sync_api_url: String,
    id_token: String,
) -> CmdResult<Vec<String>> {
    sync_client::admin_cleanup_orphans(&sync_api_url, &id_token).await
}

/// Resolve pending DB path từ active DB path (user-scoped folder).
/// Dùng ở sync_download: ghi file tạm cùng folder với DB live để khi
/// `apply_pending_sync` rename → swap trong cùng filesystem (atomic).
fn pending_db_path(db: &State<'_, DbState>) -> CmdResult<PathBuf> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let live = crate::db::resolve_active_db_path(&conn)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    Ok(live.with_extension("pending.db"))
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// =============================================================
// Pull + Merge + Push flow (sync v2).
//
// 1. Metadata check. Nếu remote chưa tồn tại → skip pull, upload thẳng.
// 2. Download remote → file temp cùng app_data_dir.
// 3. ATTACH temp DB → merge theo rule local-win (INSERT OR IGNORE).
// 4. Apply tombstones (local + từ remote) → xóa row đã bị đánh dấu xóa.
// 5. DETACH, xóa file temp.
// 6. VACUUM INTO snapshot → upload → update `sync_state`.
//
// Re-map source_file_id: AUTO_INCREMENT id giữa 2 DB có thể khác, phải JOIN
// qua `imported_files.file_hash` để tìm id local tương ứng.
// =============================================================

#[tauri::command]
pub async fn sync_pull_merge_push(
    app: AppHandle,
    db: State<'_, DbState>,
    sync_api_url: String,
    id_token: String,
) -> CmdResult<SyncUploadResult> {
    eprintln!("=== sync_pull_merge_push: START ===");
    // 1. Metadata check.
    let meta = sync_client::metadata(&sync_api_url, &id_token).await?;
    let remote_exists = meta.exists;
    eprintln!(
        "  metadata: exists={}, size={:?}, mtime={:?}",
        remote_exists, meta.size_bytes, meta.last_modified
    );

    // 2. Download remote → temp path (nếu tồn tại).
    let temp_path_opt: Option<PathBuf> = if remote_exists {
        let _ = app.emit("sync-phase", "downloading");
        let dl = sync_client::download(&sync_api_url, &id_token).await?;
        let raw_payload = BASE64
            .decode(dl.base64_data.as_bytes())
            .map_err(|e| CmdError::msg(format!("base64 decode: {e}")))?;
        let bytes = gunzip_if_needed(&raw_payload)?;
        eprintln!(
            "  download: payload={} KB → sqlite={} KB",
            raw_payload.len() / 1024,
            bytes.len() / 1024,
        );
        let temp_path = app
            .path()
            .app_data_dir()
            .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?
            .join("thongkeshopee.merge.db");
        let _ = fs::remove_file(&temp_path).await;
        for ext in &["db-wal", "db-shm"] {
            let _ = fs::remove_file(temp_path.with_extension(ext)).await;
        }
        fs::write(&temp_path, &bytes).await.map_err(CmdError::from)?;
        Some(temp_path)
    } else {
        None
    };

    // 3. Merge + snapshot (hold Mutex suốt, serialize với mọi mutation khác).
    let snapshot_path = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?
        .join("thongkeshopee.backup.db");
    let _ = fs::remove_file(&snapshot_path).await;

    let change_id_at_snapshot: i64 = {
        let mut conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;

        if let Some(ref temp) = temp_path_opt {
            let _ = app.emit("sync-phase", "merging");
            let path_str = temp
                .to_str()
                .ok_or_else(|| CmdError::msg("remote path không UTF-8"))?;

            conn.execute("ATTACH DATABASE ?1 AS remote", params![path_str])
                .map_err(CmdError::from)?;

            let merge_res: Result<(), CmdError> = (|| {
                let before_days: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM main.days",
                    [],
                    |r| r.get(0),
                )?;
                let remote_days: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM remote.days",
                    [],
                    |r| r.get(0),
                )?;
                let remote_clicks: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM remote.raw_shopee_clicks",
                    [],
                    |r| r.get(0),
                )?;
                let remote_orders: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM remote.raw_shopee_order_items",
                    [],
                    |r| r.get(0),
                )?;
                let remote_fb: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM remote.raw_fb_ads",
                    [],
                    |r| r.get(0),
                )?;
                let remote_manual: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM remote.manual_entries",
                    [],
                    |r| r.get(0),
                )?;
                let remote_tombstones: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM remote.tombstones",
                    [],
                    |r| r.get(0),
                )?;
                eprintln!(
                    "  pre-merge: local days={}, remote days={}, clicks={}, orders={}, fb_ads={}, manual={}, tombstones={}",
                    before_days, remote_days, remote_clicks, remote_orders, remote_fb, remote_manual, remote_tombstones,
                );

                let tx = conn.transaction().map_err(CmdError::from)?;
                merge_remote_into_local(&tx)?;
                apply_tombstones(&tx)?;
                tx.execute(
                    "DELETE FROM days WHERE date NOT IN (
                        SELECT day_date FROM raw_shopee_clicks UNION
                        SELECT day_date FROM raw_shopee_order_items UNION
                        SELECT day_date FROM raw_fb_ads UNION
                        SELECT day_date FROM manual_entries
                     )",
                    [],
                )
                .map_err(CmdError::from)?;
                tx.commit().map_err(CmdError::from)?;

                let after_days: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM main.days",
                    [],
                    |r| r.get(0),
                )?;
                let after_clicks: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM main.raw_shopee_clicks",
                    [],
                    |r| r.get(0),
                )?;
                let after_orders: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM main.raw_shopee_order_items",
                    [],
                    |r| r.get(0),
                )?;
                let after_fb: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM main.raw_fb_ads",
                    [],
                    |r| r.get(0),
                )?;
                let after_manual: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM main.manual_entries",
                    [],
                    |r| r.get(0),
                )?;
                eprintln!(
                    "  post-merge: days={}, clicks={}, orders={}, fb_ads={}, manual={}",
                    after_days, after_clicks, after_orders, after_fb, after_manual,
                );
                Ok(())
            })();

            let _ = conn.execute("DETACH DATABASE remote", []);
            merge_res?;
        }

        let change_id: i64 = conn.query_row(
            "SELECT change_id FROM sync_state WHERE id = 1",
            [],
            |r| r.get::<_, i64>(0),
        )?;

        let snap_str = snapshot_path
            .to_str()
            .ok_or_else(|| CmdError::msg("snapshot path không UTF-8"))?;
        conn.execute("VACUUM INTO ?1", params![snap_str])
            .map_err(CmdError::from)?;

        change_id
    };

    if let Some(temp) = temp_path_opt.as_ref() {
        let _ = fs::remove_file(temp).await;
    }

    let _ = app.emit("sync-phase", "uploading");
    let bytes = fs::read(&snapshot_path).await.map_err(CmdError::from)?;
    let raw_len = bytes.len();
    let mtime_ms = now_ms();
    let fingerprint = machine_fingerprint_raw();

    let gzipped = gzip_compress(&bytes)
        .map_err(|e| CmdError::msg(format!("gzip: {e}")))?;
    eprintln!(
        "sync upload: raw={} KB → gzipped={} KB ({:.1}%)",
        raw_len / 1024,
        gzipped.len() / 1024,
        (gzipped.len() as f64 / raw_len.max(1) as f64) * 100.0,
    );
    let base64 = BASE64.encode(&gzipped);

    let res = sync_client::upload(&sync_api_url, &id_token, &base64, mtime_ms, &fingerprint)
        .await?;

    let _ = fs::remove_file(&snapshot_path).await;
    let remote_mtime = res.last_modified;

    {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        conn.execute(
            "UPDATE sync_state
             SET dirty = CASE WHEN change_id > ?1 THEN 1 ELSE 0 END,
                 last_uploaded_change_id = ?1,
                 last_synced_at_ms = ?2,
                 last_synced_remote_mtime_ms = ?3,
                 last_error = NULL
             WHERE id = 1",
            params![change_id_at_snapshot, now_ms(), remote_mtime],
        )?;
    }

    Ok(SyncUploadResult {
        file_id: res.file_id,
        size_bytes: res.size_bytes,
        last_modified_ms: remote_mtime,
        fingerprint: res.fingerprint,
    })
}

/// Merge tất cả bảng từ `remote.*` vào `main.*` theo rule INSERT OR IGNORE.
/// Raw rows re-map `source_file_id` qua JOIN `imported_files.file_hash`.
/// Raw Shopee rows re-map `shopee_account_id` qua JOIN `shopee_accounts.name`.
fn merge_remote_into_local(tx: &rusqlite::Transaction) -> CmdResult<()> {
    // 0. shopee_accounts — UNIQUE(name). Merge trước để FK sau có sẵn id.
    // Detect remote table có tồn tại không (backward compat với DB pre-v5).
    let remote_has_accounts: bool = tx
        .query_row(
            "SELECT 1 FROM remote.sqlite_master WHERE type='table' AND name='shopee_accounts'",
            [],
            |_| Ok(true),
        )
        .unwrap_or(false);
    if remote_has_accounts {
        tx.execute(
            "INSERT OR IGNORE INTO main.shopee_accounts (name, color, created_at)
             SELECT name, color, created_at FROM remote.shopee_accounts",
            [],
        )?;
    }

    // 1. days — PK natural (date).
    tx.execute(
        "INSERT OR IGNORE INTO main.days (date, created_at, notes)
         SELECT date, created_at, notes FROM remote.days",
        [],
    )?;

    // 2. imported_files — UNIQUE(file_hash). AUTO_INCREMENT id tự gán.
    tx.execute(
        "INSERT OR IGNORE INTO main.imported_files
         (filename, kind, imported_at, row_count, file_hash, stored_path, day_date, notes)
         SELECT filename, kind, imported_at, row_count, file_hash, stored_path, day_date, notes
         FROM remote.imported_files",
        [],
    )?;

    // 3. raw_shopee_clicks — PK click_id. Re-map source_file_id qua file_hash.
    // shopee_account_id: nếu remote có cột (v5+), re-map qua JOIN account name.
    // Nếu remote chưa có cột → COALESCE về default account id=1.
    let remote_clicks_has_account = remote_table_has_column(tx, "raw_shopee_clicks", "shopee_account_id")?;
    if remote_clicks_has_account && remote_has_accounts {
        tx.execute(
            "INSERT OR IGNORE INTO main.raw_shopee_clicks
             (click_id, click_time, region, sub_id_raw, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
              referrer, day_date, source_file_id, shopee_account_id)
             SELECT r.click_id, r.click_time, r.region, r.sub_id_raw,
                    r.sub_id1, r.sub_id2, r.sub_id3, r.sub_id4, r.sub_id5,
                    r.referrer, r.day_date, lif.id,
                    COALESCE(la.id, 1)
             FROM remote.raw_shopee_clicks r
             JOIN remote.imported_files rif ON rif.id = r.source_file_id
             JOIN main.imported_files lif ON lif.file_hash = rif.file_hash
             LEFT JOIN remote.shopee_accounts ra ON ra.id = r.shopee_account_id
             LEFT JOIN main.shopee_accounts la ON la.name = ra.name",
            [],
        )?;
    } else {
        tx.execute(
            "INSERT OR IGNORE INTO main.raw_shopee_clicks
             (click_id, click_time, region, sub_id_raw, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
              referrer, day_date, source_file_id, shopee_account_id)
             SELECT r.click_id, r.click_time, r.region, r.sub_id_raw,
                    r.sub_id1, r.sub_id2, r.sub_id3, r.sub_id4, r.sub_id5,
                    r.referrer, r.day_date, lif.id, 1
             FROM remote.raw_shopee_clicks r
             JOIN remote.imported_files rif ON rif.id = r.source_file_id
             JOIN main.imported_files lif ON lif.file_hash = rif.file_hash",
            [],
        )?;
    }

    // 4. raw_shopee_order_items — UNIQUE (checkout_id, item_id, model_id).
    let remote_orders_has_account = remote_table_has_column(tx, "raw_shopee_order_items", "shopee_account_id")?;
    // v7 columns: remote DB cũ (pre-v7) chưa có 2 cột này — dùng NULL fallback.
    let remote_has_mcn = remote_table_has_column(tx, "raw_shopee_order_items", "mcn_fee")?;
    let mcn_select = if remote_has_mcn {
        "r.order_commission_total, r.mcn_fee"
    } else {
        "NULL, NULL"
    };
    if remote_orders_has_account && remote_has_accounts {
        tx.execute(
            &format!(
                "INSERT OR IGNORE INTO main.raw_shopee_order_items
                 (order_id, checkout_id, item_id, model_id, order_status, order_time, completed_time,
                  click_time, shop_id, shop_name, shop_type, item_name, category_l1, category_l2, category_l3,
                  price, quantity, order_value, refund_amount, net_commission, commission_total,
                  order_commission_total, mcn_fee,
                  sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, channel, raw_json, day_date, source_file_id,
                  shopee_account_id)
                 SELECT r.order_id, r.checkout_id, r.item_id, r.model_id, r.order_status, r.order_time, r.completed_time,
                        r.click_time, r.shop_id, r.shop_name, r.shop_type, r.item_name, r.category_l1, r.category_l2, r.category_l3,
                        r.price, r.quantity, r.order_value, r.refund_amount, r.net_commission, r.commission_total,
                        {mcn_select},
                        r.sub_id1, r.sub_id2, r.sub_id3, r.sub_id4, r.sub_id5, r.channel, r.raw_json, r.day_date, lif.id,
                        COALESCE(la.id, 1)
                 FROM remote.raw_shopee_order_items r
                 JOIN remote.imported_files rif ON rif.id = r.source_file_id
                 JOIN main.imported_files lif ON lif.file_hash = rif.file_hash
                 LEFT JOIN remote.shopee_accounts ra ON ra.id = r.shopee_account_id
                 LEFT JOIN main.shopee_accounts la ON la.name = ra.name"
            ),
            [],
        )?;
    } else {
        tx.execute(
            &format!(
                "INSERT OR IGNORE INTO main.raw_shopee_order_items
                 (order_id, checkout_id, item_id, model_id, order_status, order_time, completed_time,
                  click_time, shop_id, shop_name, shop_type, item_name, category_l1, category_l2, category_l3,
                  price, quantity, order_value, refund_amount, net_commission, commission_total,
                  order_commission_total, mcn_fee,
                  sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, channel, raw_json, day_date, source_file_id,
                  shopee_account_id)
                 SELECT r.order_id, r.checkout_id, r.item_id, r.model_id, r.order_status, r.order_time, r.completed_time,
                        r.click_time, r.shop_id, r.shop_name, r.shop_type, r.item_name, r.category_l1, r.category_l2, r.category_l3,
                        r.price, r.quantity, r.order_value, r.refund_amount, r.net_commission, r.commission_total,
                        {mcn_select},
                        r.sub_id1, r.sub_id2, r.sub_id3, r.sub_id4, r.sub_id5, r.channel, r.raw_json, r.day_date, lif.id, 1
                 FROM remote.raw_shopee_order_items r
                 JOIN remote.imported_files rif ON rif.id = r.source_file_id
                 JOIN main.imported_files lif ON lif.file_hash = rif.file_hash"
            ),
            [],
        )?;
    }

    // 5. raw_fb_ads — UNIQUE (day_date, level, name).
    tx.execute(
        "INSERT OR IGNORE INTO main.raw_fb_ads
         (level, name, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
          report_start, report_end, status, spend, clicks, cpc, impressions, reach,
          raw_json, day_date, source_file_id)
         SELECT r.level, r.name, r.sub_id1, r.sub_id2, r.sub_id3, r.sub_id4, r.sub_id5,
                r.report_start, r.report_end, r.status, r.spend, r.clicks, r.cpc, r.impressions, r.reach,
                r.raw_json, r.day_date, lif.id
         FROM remote.raw_fb_ads r
         JOIN remote.imported_files rif ON rif.id = r.source_file_id
         JOIN main.imported_files lif ON lif.file_hash = rif.file_hash",
        [],
    )?;

    // 6. manual_entries — UNIQUE(sub_ids, day_date). UPSERT last-write-wins
    // theo `updated_at` (ISO8601 → string compare đúng trình tự).
    // shopee_account_id re-map qua name JOIN tương tự Shopee tables.
    let remote_manual_has_account = remote_table_has_column(tx, "manual_entries", "shopee_account_id")?;
    if remote_manual_has_account && remote_has_accounts {
        tx.execute(
            "INSERT INTO main.manual_entries
             (sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date, display_name,
              override_clicks, override_spend, override_cpc, override_orders, override_commission,
              notes, created_at, updated_at, shopee_account_id)
             SELECT r.sub_id1, r.sub_id2, r.sub_id3, r.sub_id4, r.sub_id5, r.day_date, r.display_name,
                    r.override_clicks, r.override_spend, r.override_cpc, r.override_orders, r.override_commission,
                    r.notes, r.created_at, r.updated_at,
                    COALESCE(la.id, 1)
             FROM remote.manual_entries r
             LEFT JOIN remote.shopee_accounts ra ON ra.id = r.shopee_account_id
             LEFT JOIN main.shopee_accounts la ON la.name = ra.name
             WHERE true
             ON CONFLICT(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date) DO UPDATE SET
               display_name        = excluded.display_name,
               override_clicks     = excluded.override_clicks,
               override_spend      = excluded.override_spend,
               override_cpc        = excluded.override_cpc,
               override_orders     = excluded.override_orders,
               override_commission = excluded.override_commission,
               notes               = excluded.notes,
               updated_at          = excluded.updated_at,
               shopee_account_id   = excluded.shopee_account_id
             WHERE excluded.updated_at > manual_entries.updated_at",
            [],
        )?;
    } else {
        tx.execute(
            "INSERT INTO main.manual_entries
             (sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date, display_name,
              override_clicks, override_spend, override_cpc, override_orders, override_commission,
              notes, created_at, updated_at, shopee_account_id)
             SELECT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date, display_name,
                    override_clicks, override_spend, override_cpc, override_orders, override_commission,
                    notes, created_at, updated_at, 1
             FROM remote.manual_entries WHERE true
             ON CONFLICT(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date) DO UPDATE SET
               display_name        = excluded.display_name,
               override_clicks     = excluded.override_clicks,
               override_spend      = excluded.override_spend,
               override_cpc        = excluded.override_cpc,
               override_orders     = excluded.override_orders,
               override_commission = excluded.override_commission,
               notes               = excluded.notes,
               updated_at          = excluded.updated_at
             WHERE excluded.updated_at > manual_entries.updated_at",
            [],
        )?;
    }

    // 7. tombstones — UNIQUE(entity_type, entity_key).
    // (video_downloads đã move sang video_logs.db ở migration v4 — không merge
    // trong flow DB sync, video log đồng bộ qua Apps Script Google Sheet riêng.)
    tx.execute(
        "INSERT OR IGNORE INTO main.tombstones (entity_type, entity_key, deleted_at)
         SELECT entity_type, entity_key, deleted_at FROM remote.tombstones",
        [],
    )?;

    Ok(())
}

/// Apply tất cả tombstones trong local DB. Thứ tự: 'day' (CASCADE raw) →
/// 'manual_entry' (exact key) → 'ui_row' (exact manual + prefix-compatible raw).
fn apply_tombstones(tx: &rusqlite::Transaction) -> CmdResult<()> {
    use crate::commands::query::{is_prefix, to_canonical};

    // 1. 'day' tombstones — xóa days (CASCADE xóa imported_files, raw_*, manual_entries).
    tx.execute(
        "DELETE FROM days WHERE date IN (
            SELECT entity_key FROM tombstones WHERE entity_type = 'day'
         )",
        [],
    )?;

    // 2. 'manual_entry' tombstones — parse key, DELETE manual_entries exact match.
    let manual_keys: Vec<String> = {
        let mut stmt = tx.prepare(
            "SELECT entity_key FROM tombstones WHERE entity_type = 'manual_entry'",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for key in manual_keys {
        if let Some((day, sub_ids)) = parse_tombstone_sub_key(&key) {
            tx.execute(
                "DELETE FROM manual_entries
                 WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
                   AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?",
                params![sub_ids[0], sub_ids[1], sub_ids[2], sub_ids[3], sub_ids[4], day],
            )?;
        }
    }

    // 3. 'ui_row' tombstones — parse key, DELETE manual_entries exact + raw prefix-compatible.
    let ui_keys: Vec<String> = {
        let mut stmt = tx.prepare(
            "SELECT entity_key FROM tombstones WHERE entity_type = 'ui_row'",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for key in ui_keys {
        let Some((day, sub_ids)) = parse_tombstone_sub_key(&key) else {
            continue;
        };

        tx.execute(
            "DELETE FROM manual_entries
             WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
               AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?",
            params![sub_ids[0], sub_ids[1], sub_ids[2], sub_ids[3], sub_ids[4], day],
        )?;

        let target = to_canonical(sub_ids);
        for table in ["raw_fb_ads", "raw_shopee_clicks", "raw_shopee_order_items"] {
            let select_sql = format!(
                "SELECT DISTINCT sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
                 FROM {table} WHERE day_date = ?"
            );
            let tuples: Vec<[String; 5]> = {
                let mut stmt = tx.prepare(&select_sql)?;
                let rows = stmt.query_map(params![day], |r| {
                    Ok([
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                    ])
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };

            let delete_sql = format!(
                "DELETE FROM {table}
                 WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
                   AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?"
            );
            for tuple in tuples {
                let canonical = to_canonical(tuple.clone());
                let compatible = is_prefix(&canonical, &target) || is_prefix(&target, &canonical);
                if !compatible {
                    continue;
                }
                tx.execute(
                    &delete_sql,
                    params![tuple[0], tuple[1], tuple[2], tuple[3], tuple[4], day],
                )?;
            }
        }
    }

    Ok(())
}

/// Check cột tồn tại trong `remote.<table>` (DB đã ATTACH).
/// Dùng backward-compat khi merge DB từ R2 mà schema cũ chưa có cột mới.
fn remote_table_has_column(
    tx: &rusqlite::Transaction,
    table: &str,
    column: &str,
) -> CmdResult<bool> {
    let mut stmt = tx.prepare(&format!("PRAGMA remote.table_info({table})"))?;
    let cols: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(cols.iter().any(|c| c == column))
}

/// Parse tombstone `entity_key` format `{day}|{s1}|{s2}|{s3}|{s4}|{s5}`.
fn parse_tombstone_sub_key(key: &str) -> Option<(String, [String; 5])> {
    let parts: Vec<&str> = key.split('|').collect();
    if parts.len() != 6 {
        return None;
    }
    Some((
        parts[0].to_string(),
        [
            parts[1].to_string(),
            parts[2].to_string(),
            parts[3].to_string(),
            parts[4].to_string(),
            parts[5].to_string(),
        ],
    ))
}
