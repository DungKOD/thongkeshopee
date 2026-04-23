//! DB sync commands — backup SQLite lên Cloudflare R2 qua Worker proxy.
//!
//! Transport layer trong `sync_client`. File này giữ business logic:
//! - Tauri command handlers (`sync_*`, `admin_*`)
//! - sync_state CRUD (dirty flag, change_id, last_synced_*)
//! - Snapshot tạo bằng `VACUUM INTO` (consistent, không đụng WAL)
//! - zstd multi-thread compress/decompress (v8.1+, replaces gzip)
//! - Pull-merge-push flow (cross-device safe, tombstones CASCADE)
//! - Guard: fresh-install + remote có data → reject upload, route sang merge
//! - Skip-identical: hash compressed bytes trước upload, skip nếu match lần trước

use std::path::PathBuf;

use md5::{Digest, Md5};
use rusqlite::params;
use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::fs;

use super::sync_client::{self, UserListEntry};
use super::{CmdError, CmdResult};
use crate::db::{DbState, VideoDbState};

// =============================================================
// HLC-lite — timestamp monotonic cross-machine
// =============================================================
//
// Problem: 2 máy cùng account, clock drift 5 phút → edit UTC ISO8601 timestamp
// so sánh lexicographic → máy clock chậm hơn luôn lose merge UPSERT.
//
// Solution: mỗi DB giữ `sync_state.last_known_clock_ms`. Mỗi edit lấy:
//   ts = max(now_ms, last_known_clock_ms + 1)
//   last_known_clock_ms = ts
//
// Sau merge remote, absorb: last_known_clock_ms = max(local, max remote ts).
// Kết quả: edit sau merge luôn > mọi edit remote, không bao giờ flip thứ tự
// bởi clock drift. Wall clock vẫn dùng khi forward-flowing, chỉ clamp lên khi
// remote ahead.
//
// Trade-off: nếu máy A clock rất nhanh → timestamp xa tương lai → máy B cũng
// ăn theo sau khi sync. Không "đúng" wall time nhưng consistent ordering.

/// Next monotonic ms — caller phải hold DB lock (modifies sync_state).
/// Return value + tự update `last_known_clock_ms` atomically.
pub fn next_hlc_ms(conn: &rusqlite::Connection) -> rusqlite::Result<i64> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let last: i64 = conn
        .query_row(
            "SELECT last_known_clock_ms FROM sync_state WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let next = std::cmp::max(now, last + 1);
    conn.execute(
        "UPDATE sync_state SET last_known_clock_ms = ?1 WHERE id = 1",
        [next],
    )?;
    Ok(next)
}

/// Convenience: next HLC timestamp as RFC3339 string (cho manual_entries.updated_at).
pub fn next_hlc_rfc3339(conn: &rusqlite::Connection) -> rusqlite::Result<String> {
    let ms = next_hlc_ms(conn)?;
    Ok(ms_to_rfc3339(ms))
}

/// Sau merge, absorb max timestamp từ remote → local clock không bao giờ tụt
/// sau máy khác. Silent no-op nếu remote_max_ms <= local.
pub fn absorb_remote_clock(
    conn: &rusqlite::Connection,
    remote_max_ms: i64,
) -> rusqlite::Result<()> {
    if remote_max_ms <= 0 {
        return Ok(());
    }
    conn.execute(
        "UPDATE sync_state
         SET last_known_clock_ms = MAX(last_known_clock_ms, ?1)
         WHERE id = 1",
        [remote_max_ms],
    )?;
    Ok(())
}

/// Convert ms → RFC3339 UTC string. Fallback to Utc::now nếu invalid timestamp.
pub fn ms_to_rfc3339(ms: i64) -> String {
    use chrono::{TimeZone, Utc};
    Utc.timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339()
}

/// Parse RFC3339 → ms. 0 on parse fail (safe default: không ảnh hưởng
/// absorb_remote_clock vì 0 < mọi real timestamp).
pub fn rfc3339_to_ms(s: &str) -> i64 {
    use chrono::DateTime;
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or(0)
}

/// zstd magic bytes `28 B5 2F FD` — dùng để validate payload là zstd frame
/// (detect corrupt hoặc wrong-format upload).
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

/// Error message prefix signal FE rằng CAS etag mismatch → nên retry qua
/// `sync_pull_merge_push`. FE check `msg.startsWith(ETAG_CONFLICT_PREFIX)`.
pub const ETAG_CONFLICT_PREFIX: &str = "ETAG_CONFLICT";

/// zstd compress level 3 — fast, ratio tương đương gzip L6. Multi-thread
/// qua `NbWorkers` param để tận dụng CPU — trên laptop 4-8 core, compress
/// 500MB DB từ ~60s (gzip single-thread) xuống ~15s.
///
/// Workers = `num_cpus / 2` để không độc chiếm CPU (UI còn responsive).
/// Min 1 worker nếu máy 1-2 core.
fn zstd_compress_mt(input: &[u8]) -> std::io::Result<Vec<u8>> {
    let workers = std::cmp::max(1, (num_cpus::get() / 2) as u32);
    let mut compressor = zstd::bulk::Compressor::new(3)?;
    // NbWorkers=0 = single-thread. >0 = MT mode.
    let _ = compressor
        .set_parameter(zstd::stream::raw::CParameter::NbWorkers(workers));
    compressor.compress(input)
}

/// Decompress zstd payload. Validate magic trước để error message clear nếu
/// ai đó upload sai format (vd file raw SQLite hay gzip legacy).
///
/// Dùng stream decode (grow buffer dynamic) thay vì bulk với fixed capacity
/// — tránh fail khi data ratio cao (vd repetitive buffer nén >20×).
pub(crate) fn zstd_decompress(input: &[u8]) -> CmdResult<Vec<u8>> {
    use std::io::Read;
    if input.len() < 4 || input[0..4] != ZSTD_MAGIC {
        let head_hex: String = input
            .iter()
            .take(16)
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ");
        return Err(CmdError::msg(format!(
            "payload không phải zstd frame — size={} bytes, head=[{}]",
            input.len(),
            head_hex,
        )));
    }
    let mut decoder = zstd::stream::read::Decoder::new(input)
        .map_err(|e| CmdError::msg(format!("zstd decoder init: {e}")))?;
    let mut out = Vec::with_capacity(input.len() * 5);
    decoder
        .read_to_end(&mut out)
        .map_err(|e| CmdError::msg(format!("zstd decompress: {e}")))?;
    Ok(out)
}

/// Hash helper cho skip-identical: MD5 của compressed bytes → hex string.
/// MD5 đủ cho dedup (collision practical ~0 với small bucket per-user), không
/// cần crypto strength.
fn md5_hex(bytes: &[u8]) -> String {
    let digest = Md5::digest(bytes);
    hex::encode(digest)
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

/// Compute machine fingerprint: MD5(os | machine-uid) → hex string.
/// Ổn định tối đa qua hardware re-config. Trước đây có kèm `hostname` nhưng bỏ
/// vì user rename máy (Windows `COMPUTERNAME` đổi) làm fingerprint đổi → trigger
/// merge loop vô ích mỗi startup.
///
/// `machine_uid` crate đọc MachineGuid từ registry (Windows) / /etc/machine-id
/// (Linux) / IOPlatformUUID (macOS) — stable until OS reinstall. `os` component
/// giữ để 1 ổ cứng boot dual-boot có fingerprint khác nhau (hiếm).
///
/// Migration: user hiện tại đã có fingerprint cũ lưu ở R2 customMetadata →
/// sau upgrade, fingerprint mới khác cũ → trigger 1 lần pull-merge-push extra
/// (one-time heal). Không mất data.
pub fn machine_fingerprint_raw() -> String {
    let os = std::env::consts::OS;
    let machine_id = machine_uid::get().unwrap_or_else(|_| "unknown-uid".into());
    let input = format!("{os}|{machine_id}");
    let digest = Md5::digest(input.as_bytes());
    hex::encode(digest)
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
    db: State<'_, DbState>,
    sync_api_url: String,
    id_token: String,
) -> CmdResult<SyncMetadataResult> {
    let m = sync_client::metadata(&sync_api_url, &id_token).await?;
    // Persist etag vào sync_state cho CAS upload kế tiếp. Nếu remote không
    // tồn tại (etag=None) — giữ nguyên last_remote_etag cũ (có thể là của
    // prior state trước khi user khác xóa object).
    if let Some(ref etag) = m.etag {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        conn.execute(
            "UPDATE sync_state SET last_remote_etag = ?1 WHERE id = 1",
            params![etag],
        )?;
    }
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

    // 8. Swap video DB tương tự main DB (v8+ multi-tenant video logs).
    //    Folder per-user: `users/{uid}/video_logs.db`. Isolation để User B login
    //    cùng máy KHÔNG thấy download history của User A.
    //    Migration legacy shared `{app_data}/video_logs.db`: chỉ move nếu user
    //    này là user đầu tiên login sau upgrade (user's video DB chưa tồn tại).
    //    User tiếp theo trên cùng máy sẽ start fresh (legacy đã bị move đi).
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
                        let _ = std_fs::rename(
                            &src,
                            user_video_db_path.with_extension(ext),
                        );
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

    // Đọc change_id + expected_etag (CAS) + last_uploaded_hash (skip-identical).
    let (change_id_at_snapshot, expected_etag, last_uploaded_hash) = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let (change_id, last_uploaded, etag, hash): (i64, i64, Option<String>, Option<String>) =
            conn.query_row(
                "SELECT change_id, last_uploaded_change_id, last_remote_etag, last_uploaded_hash
                 FROM sync_state WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
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
        (change_id, etag, hash)
    };

    let bytes = fs::read(&snapshot_path).await.map_err(CmdError::from)?;
    let raw_len = bytes.len();
    let mtime_ms = now_ms();
    let fingerprint = machine_fingerprint_raw();

    // Compress với zstd multi-thread. Large DB benefit đáng kể từ parallel.
    let compressed = zstd_compress_mt(&bytes)
        .map_err(|e| CmdError::msg(format!("zstd compress: {e}")))?;
    eprintln!(
        "sync upload: raw={} KB → zstd={} KB ({:.1}%)",
        raw_len / 1024,
        compressed.len() / 1024,
        (compressed.len() as f64 / raw_len.max(1) as f64) * 100.0,
    );

    // Skip-identical: compute hash trước upload. Match last_uploaded_hash →
    // payload giống hệt lần trước → skip upload (save bandwidth + Worker CPU).
    // Chỉ update last_synced_at_ms để UI show "đã sync", clear dirty flag.
    let payload_hash = md5_hex(&compressed);
    if last_uploaded_hash.as_deref() == Some(payload_hash.as_str()) {
        eprintln!("sync upload: payload identical to last — skip upload");
        let _ = fs::remove_file(&snapshot_path).await;
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        // Clear dirty (CAS guard: chỉ clear nếu change_id không tăng thêm
        // trong lúc compute).
        conn.execute(
            "UPDATE sync_state
             SET dirty = CASE WHEN change_id > ?1 THEN 1 ELSE 0 END,
                 last_uploaded_change_id = ?1,
                 last_synced_at_ms = ?2,
                 last_error = NULL
             WHERE id = 1",
            params![change_id_at_snapshot, now_ms()],
        )?;
        // Return synthetic result — không có upload thực sự nên file_id/size
        // từ last known state. last_modified_ms = now để UI badge refresh.
        return Ok(SyncUploadResult {
            file_id: format!("users/{}/db.zst", "skipped"),
            size_bytes: compressed.len() as u64,
            last_modified_ms: now_ms(),
            fingerprint,
        });
    }

    // CAS upload: nếu etag R2 hiện tại khác expected_etag (máy khác upload
    // trong lúc này), Worker trả 412 → trả lỗi `ETAG_CONFLICT:` để FE route
    // sang `sync_pull_merge_push` + retry. Tránh ghi đè mất data máy khác.
    let res = match sync_client::upload(
        &sync_api_url,
        &id_token,
        &compressed,
        mtime_ms,
        &fingerprint,
        expected_etag.as_deref(),
    )
    .await
    {
        Ok(r) => r,
        Err(sync_client::UploadError::EtagConflict(msg)) => {
            let _ = fs::remove_file(&snapshot_path).await;
            return Err(CmdError::msg(format!("{ETAG_CONFLICT_PREFIX}: {msg}")));
        }
        Err(sync_client::UploadError::Other(e)) => {
            let _ = fs::remove_file(&snapshot_path).await;
            return Err(e);
        }
    };

    let _ = fs::remove_file(&snapshot_path).await;
    let remote_mtime = res.last_modified;

    // CAS clear dirty CHỈ KHI change_id chưa tăng từ snapshot. Lưu etag MỚI
    // cho CAS upload lần sau + hash để skip-identical lần sau.
    {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        conn.execute(
            "UPDATE sync_state
             SET dirty = CASE WHEN change_id > ?1 THEN 1 ELSE 0 END,
                 last_uploaded_change_id = ?1,
                 last_synced_at_ms = ?2,
                 last_synced_remote_mtime_ms = ?3,
                 last_remote_etag = ?4,
                 last_uploaded_hash = ?5,
                 last_error = NULL
             WHERE id = 1",
            params![
                change_id_at_snapshot,
                now_ms(),
                remote_mtime,
                res.etag,
                payload_hash
            ],
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

    // v8.1: payload là raw zstd bytes (không base64, không gzip).
    let bytes = zstd_decompress(&dl.bytes)?;
    eprintln!(
        "sync_download_db: payload={} KB → sqlite={} KB",
        dl.bytes.len() / 1024,
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

/// Pull-merge-push với CAS retry (max 3 lần). Nếu 2 máy cùng upload song
/// song, máy thua sẽ bị 412 khi push cuối → loop restart (re-metadata,
/// re-download, re-merge, re-upload với etag mới). Merge idempotent
/// (INSERT OR IGNORE + UPSERT by updated_at) nên re-merge không corrupt.
/// Sau 3 lần thất bại → trả lỗi "ETAG_CONFLICT_EXHAUSTED" cho FE show warning.
#[tauri::command]
pub async fn sync_pull_merge_push(
    app: AppHandle,
    db: State<'_, DbState>,
    sync_api_url: String,
    id_token: String,
) -> CmdResult<SyncUploadResult> {
    const MAX_RETRIES: u32 = 3;
    for attempt in 1..=MAX_RETRIES {
        match sync_pull_merge_push_attempt(&app, &db, &sync_api_url, &id_token).await {
            Ok(res) => return Ok(res),
            Err(CmdError::Msg(msg)) if msg.starts_with(ETAG_CONFLICT_PREFIX) => {
                if attempt >= MAX_RETRIES {
                    return Err(CmdError::msg(format!(
                        "ETAG_CONFLICT_EXHAUSTED: retried {MAX_RETRIES} lần vẫn bị máy khác đè. \
                         Thử lại sau vài giây. Gốc: {msg}"
                    )));
                }
                eprintln!(
                    "[sync] CAS conflict attempt {attempt}/{MAX_RETRIES}, retry pull-merge-push"
                );
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("retry loop exits via return")
}

/// 1 attempt của pull-merge-push. Caller wrapper retry khi return
/// `ETAG_CONFLICT` error.
async fn sync_pull_merge_push_attempt(
    app: &AppHandle,
    db: &State<'_, DbState>,
    sync_api_url: &str,
    id_token: &str,
) -> CmdResult<SyncUploadResult> {
    eprintln!("=== sync_pull_merge_push: START ===");
    // 1. Metadata check.
    let meta = sync_client::metadata(sync_api_url, id_token).await?;
    let remote_exists = meta.exists;
    eprintln!(
        "  metadata: exists={}, size={:?}, mtime={:?}, etag={:?}",
        remote_exists, meta.size_bytes, meta.last_modified, meta.etag
    );

    // 2. Download remote → temp path (nếu tồn tại).
    // `expected_etag` = etag tại thời điểm download — upload cuối dùng làm CAS guard.
    let mut expected_etag: Option<String> = None;
    let temp_path_opt: Option<PathBuf> = if remote_exists {
        let _ = app.emit("sync-phase", "downloading");
        let dl = sync_client::download(sync_api_url, id_token).await?;
        expected_etag = dl.etag.clone();
        // v8.1: raw zstd bytes — không còn base64/gzip.
        let bytes = zstd_decompress(&dl.bytes)?;
        eprintln!(
            "  download: payload={} KB → sqlite={} KB",
            dl.bytes.len() / 1024,
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
                // Read max remote timestamps TRƯỚC merge — HLC absorb step.
                // Parse RFC3339 → ms để so sánh + update last_known_clock_ms.
                // Fail silent (0) nếu remote bảng rỗng hoặc format lạ.
                let remote_max_manual_ts: String = conn
                    .query_row(
                        "SELECT COALESCE(MAX(updated_at), '') FROM remote.manual_entries",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or_default();
                let remote_max_tomb_ts: String = conn
                    .query_row(
                        "SELECT COALESCE(MAX(deleted_at), '') FROM remote.tombstones",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap_or_default();
                let remote_max_ms = std::cmp::max(
                    rfc3339_to_ms(&remote_max_manual_ts),
                    rfc3339_to_ms(&remote_max_tomb_ts),
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
                // HLC absorb: bump last_known_clock_ms lên max remote ts.
                // Mọi edit sau merge trên máy này sẽ > ts này → không flip.
                absorb_remote_clock(&tx, remote_max_ms)
                    .map_err(CmdError::from)?;
                tx.commit().map_err(CmdError::from)?;
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

    let compressed = zstd_compress_mt(&bytes)
        .map_err(|e| CmdError::msg(format!("zstd compress: {e}")))?;
    eprintln!(
        "sync upload: raw={} KB → zstd={} KB ({:.1}%)",
        raw_len / 1024,
        compressed.len() / 1024,
        (compressed.len() as f64 / raw_len.max(1) as f64) * 100.0,
    );

    // Hash for skip-identical check trong merge flow: thường merge LUÔN khác
    // snapshot trước (vì merge bring data mới), nên skip ít khi fire — nhưng
    // vẫn tính cho edge case "pull-merge-push nhưng không có gì đổi thực sự".
    let payload_hash = md5_hex(&compressed);

    // Upload với CAS guard — `expected_etag` từ download (nếu remote exists).
    // Remote không tồn tại → expected_etag=None → unconditional PUT (first init).
    let res = match sync_client::upload(
        sync_api_url,
        id_token,
        &compressed,
        mtime_ms,
        &fingerprint,
        expected_etag.as_deref(),
    )
    .await
    {
        Ok(r) => r,
        Err(sync_client::UploadError::EtagConflict(msg)) => {
            let _ = fs::remove_file(&snapshot_path).await;
            return Err(CmdError::msg(format!("{ETAG_CONFLICT_PREFIX}: {msg}")));
        }
        Err(sync_client::UploadError::Other(e)) => {
            let _ = fs::remove_file(&snapshot_path).await;
            return Err(e);
        }
    };

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
                 last_remote_etag = ?4,
                 last_uploaded_hash = ?5,
                 last_error = NULL
             WHERE id = 1",
            params![
                change_id_at_snapshot,
                now_ms(),
                remote_mtime,
                res.etag,
                payload_hash
            ],
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
                  sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, channel, day_date, source_file_id,
                  shopee_account_id)
                 SELECT r.order_id, r.checkout_id, r.item_id, r.model_id, r.order_status, r.order_time, r.completed_time,
                        r.click_time, r.shop_id, r.shop_name, r.shop_type, r.item_name, r.category_l1, r.category_l2, r.category_l3,
                        r.price, r.quantity, r.order_value, r.refund_amount, r.net_commission, r.commission_total,
                        {mcn_select},
                        r.sub_id1, r.sub_id2, r.sub_id3, r.sub_id4, r.sub_id5, r.channel, r.day_date, lif.id,
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
                  sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, channel, day_date, source_file_id,
                  shopee_account_id)
                 SELECT r.order_id, r.checkout_id, r.item_id, r.model_id, r.order_status, r.order_time, r.completed_time,
                        r.click_time, r.shop_id, r.shop_name, r.shop_type, r.item_name, r.category_l1, r.category_l2, r.category_l3,
                        r.price, r.quantity, r.order_value, r.refund_amount, r.net_commission, r.commission_total,
                        {mcn_select},
                        r.sub_id1, r.sub_id2, r.sub_id3, r.sub_id4, r.sub_id5, r.channel, r.day_date, lif.id, 1
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
          day_date, source_file_id)
         SELECT r.level, r.name, r.sub_id1, r.sub_id2, r.sub_id3, r.sub_id4, r.sub_id5,
                r.report_start, r.report_end, r.status, r.spend, r.clicks, r.cpc, r.impressions, r.reach,
                r.day_date, lif.id
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
///
/// **Timestamp semantics (v8+):** Với manual_entries (có `updated_at`), chỉ
/// DELETE nếu `updated_at <= tombstone.deleted_at` — tức là delete tombstone
/// chỉ thắng khi row chưa có edit mới hơn. Nếu máy B edit row SAU khi máy A
/// delete, edit của B sẽ "resurrect" row khi merge → không bị mất edit.
///
/// Raw tables KHÔNG có `updated_at` → vẫn DELETE unconditional. Acceptable vì
/// raw data đến từ CSV, immutable — user re-import sẽ tạo row mới với PK giống.
///
/// 'day' tombstone vẫn CASCADE unconditional — delete cả ngày là user intent
/// rõ ràng hơn, không nên auto-resurrect dựa trên edit nhỏ.
fn apply_tombstones(tx: &rusqlite::Transaction) -> CmdResult<()> {
    use crate::commands::query::{is_prefix, to_canonical};

    // 1. 'day' tombstones — xóa days (CASCADE xóa imported_files, raw_*, manual_entries).
    tx.execute(
        "DELETE FROM days WHERE date IN (
            SELECT entity_key FROM tombstones WHERE entity_type = 'day'
         )",
        [],
    )?;

    // 2. 'manual_entry' tombstones — parse key, DELETE manual_entries exact match
    //    CHỈ khi row.updated_at <= tombstone.deleted_at (resurrect rule).
    let manual_keys: Vec<(String, String)> = {
        let mut stmt = tx.prepare(
            "SELECT entity_key, deleted_at FROM tombstones WHERE entity_type = 'manual_entry'",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for (key, deleted_at) in manual_keys {
        if let Some((day, sub_ids)) = parse_tombstone_sub_key(&key) {
            tx.execute(
                "DELETE FROM manual_entries
                 WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
                   AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?
                   AND updated_at <= ?",
                params![
                    sub_ids[0],
                    sub_ids[1],
                    sub_ids[2],
                    sub_ids[3],
                    sub_ids[4],
                    day,
                    deleted_at
                ],
            )?;
        }
    }

    // 3. 'ui_row' tombstones — parse key, DELETE manual_entries exact (timestamp
    //    guard) + raw prefix-compatible (unconditional vì raw không có updated_at).
    let ui_keys: Vec<(String, String)> = {
        let mut stmt = tx.prepare(
            "SELECT entity_key, deleted_at FROM tombstones WHERE entity_type = 'ui_row'",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for (key, deleted_at) in ui_keys {
        let Some((day, sub_ids)) = parse_tombstone_sub_key(&key) else {
            continue;
        };

        tx.execute(
            "DELETE FROM manual_entries
             WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
               AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?
               AND updated_at <= ?",
            params![
                sub_ids[0],
                sub_ids[1],
                sub_ids[2],
                sub_ids[3],
                sub_ids[4],
                day,
                deleted_at
            ],
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn test_conn_with_sync_state() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE sync_state (
                id                            INTEGER PRIMARY KEY CHECK (id = 1),
                dirty                         INTEGER NOT NULL DEFAULT 1,
                last_synced_at_ms             INTEGER,
                last_synced_remote_mtime_ms   INTEGER,
                last_error                    TEXT,
                change_id                     INTEGER NOT NULL DEFAULT 0,
                last_uploaded_change_id       INTEGER NOT NULL DEFAULT 0,
                owner_uid                     TEXT,
                last_known_clock_ms           INTEGER NOT NULL DEFAULT 0,
                last_remote_etag              TEXT
            );
            INSERT INTO sync_state (id) VALUES (1);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn hlc_monotonic_in_one_process() {
        // next_hlc_ms phải luôn trả value tăng monotone kể cả khi gọi nhanh
        // trong cùng 1ms (counter phần).
        let conn = test_conn_with_sync_state();
        let a = next_hlc_ms(&conn).unwrap();
        let b = next_hlc_ms(&conn).unwrap();
        let c = next_hlc_ms(&conn).unwrap();
        assert!(a < b, "HLC must be monotonic: {a} < {b}");
        assert!(b < c, "HLC must be monotonic: {b} < {c}");
    }

    #[test]
    fn hlc_absorb_remote_clock_bumps_local() {
        // Simulate merge scenario: remote có timestamp 10 phút future → local
        // absorb → next HLC phải > remote.
        let conn = test_conn_with_sync_state();
        let future_ms = chrono::Utc::now().timestamp_millis() + 10 * 60 * 1000;
        absorb_remote_clock(&conn, future_ms).unwrap();
        let next = next_hlc_ms(&conn).unwrap();
        assert!(
            next > future_ms,
            "after absorb remote future clock, next HLC {next} must > {future_ms}"
        );
    }

    #[test]
    fn hlc_no_backward_slip_with_remote_older() {
        // Remote timestamp cũ hơn local → absorb no-op, không gây tụt clock.
        let conn = test_conn_with_sync_state();
        let first = next_hlc_ms(&conn).unwrap();
        // Giả sử remote từ 1 giờ trước.
        absorb_remote_clock(&conn, first - 3_600_000).unwrap();
        let next = next_hlc_ms(&conn).unwrap();
        assert!(
            next > first,
            "absorb older remote không được tụt clock: next {next} must > first {first}"
        );
    }

    #[test]
    fn rfc3339_roundtrip() {
        let original_ms = 1_700_000_000_000_i64; // 2023-11-14 22:13:20 UTC
        let s = ms_to_rfc3339(original_ms);
        let back = rfc3339_to_ms(&s);
        assert_eq!(back, original_ms, "RFC3339 round-trip must preserve ms");
    }

    #[test]
    fn rfc3339_parse_invalid_returns_zero() {
        assert_eq!(rfc3339_to_ms(""), 0);
        assert_eq!(rfc3339_to_ms("not-a-date"), 0);
    }

    #[test]
    fn apply_tombstones_respects_updated_at() {
        // Setup: DB có manual_entries row với updated_at mới hơn deleted_at
        // của tombstone tương ứng → apply_tombstones phải giữ row (resurrect rule).
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE manual_entries (
                sub_id1 TEXT NOT NULL, sub_id2 TEXT NOT NULL, sub_id3 TEXT NOT NULL,
                sub_id4 TEXT NOT NULL, sub_id5 TEXT NOT NULL, day_date TEXT NOT NULL,
                display_name TEXT, override_clicks INTEGER, override_spend REAL,
                override_cpc REAL, override_orders INTEGER, override_commission REAL,
                notes TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                shopee_account_id INTEGER NOT NULL DEFAULT 1,
                UNIQUE(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date)
            );
            CREATE TABLE days (date TEXT PRIMARY KEY, created_at TEXT, notes TEXT);
            CREATE TABLE tombstones (
                entity_type TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                deleted_at TEXT NOT NULL,
                UNIQUE(entity_type, entity_key)
            );
            INSERT INTO days(date) VALUES ('2026-04-23');
            -- Row với updated_at NEW hơn tombstone deleted_at
            INSERT INTO manual_entries
              (sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date,
               created_at, updated_at)
              VALUES ('a', 'b', '', '', '', '2026-04-23',
                      '2026-04-23T10:00:00Z', '2026-04-23T12:00:00Z');
            -- Tombstone với deleted_at SỚM hơn row edit → row should survive
            INSERT INTO tombstones(entity_type, entity_key, deleted_at)
              VALUES ('manual_entry', '2026-04-23|a|b|||', '2026-04-23T11:00:00Z');",
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        apply_tombstones(&tx).unwrap();
        tx.commit().unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM manual_entries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 1,
            "row với updated_at > tombstone.deleted_at phải survive apply_tombstones"
        );
    }

    #[test]
    fn apply_tombstones_deletes_older_row() {
        // Opposite case: row updated_at OLDER than tombstone deleted_at → row
        // phải bị xóa (delete came after edit).
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE manual_entries (
                sub_id1 TEXT NOT NULL, sub_id2 TEXT NOT NULL, sub_id3 TEXT NOT NULL,
                sub_id4 TEXT NOT NULL, sub_id5 TEXT NOT NULL, day_date TEXT NOT NULL,
                display_name TEXT, override_clicks INTEGER, override_spend REAL,
                override_cpc REAL, override_orders INTEGER, override_commission REAL,
                notes TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                shopee_account_id INTEGER NOT NULL DEFAULT 1,
                UNIQUE(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date)
            );
            CREATE TABLE days (date TEXT PRIMARY KEY, created_at TEXT, notes TEXT);
            CREATE TABLE tombstones (
                entity_type TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                deleted_at TEXT NOT NULL,
                UNIQUE(entity_type, entity_key)
            );
            INSERT INTO days(date) VALUES ('2026-04-23');
            -- Row với updated_at OLDER than tombstone deleted_at
            INSERT INTO manual_entries
              (sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date,
               created_at, updated_at)
              VALUES ('a', 'b', '', '', '', '2026-04-23',
                      '2026-04-23T09:00:00Z', '2026-04-23T10:00:00Z');
            -- Tombstone deleted AFTER edit → delete wins
            INSERT INTO tombstones(entity_type, entity_key, deleted_at)
              VALUES ('manual_entry', '2026-04-23|a|b|||', '2026-04-23T11:00:00Z');",
        )
        .unwrap();

        let tx = conn.transaction().unwrap();
        apply_tombstones(&tx).unwrap();
        tx.commit().unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM manual_entries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 0,
            "row với updated_at < tombstone.deleted_at phải bị xóa"
        );
    }

    #[test]
    fn fingerprint_stable_across_calls() {
        // Fingerprint phải deterministic trên cùng máy — gọi 2 lần ra kết quả giống.
        let fp1 = machine_fingerprint_raw();
        let fp2 = machine_fingerprint_raw();
        assert_eq!(fp1, fp2, "fingerprint phải stable trong cùng máy");
        assert_eq!(fp1.len(), 32, "MD5 hex = 32 chars");
    }

    #[test]
    fn zstd_roundtrip_preserves_bytes() {
        // Compress + decompress phải return bytes y hệt.
        let input = b"SQLite format 3\0\x10\x00\x01\x01\x00\x40\x20\x20".repeat(1000);
        let compressed = zstd_compress_mt(&input).unwrap();
        assert!(compressed.len() < input.len(), "nén phải smaller");
        let head = &compressed[0..4];
        assert_eq!(head, ZSTD_MAGIC, "compressed output phải có zstd magic");
        let decompressed = zstd_decompress(&compressed).unwrap();
        assert_eq!(decompressed, input, "zstd round-trip phải preserve bytes");
    }

    #[test]
    fn zstd_decompress_rejects_non_zstd() {
        // Not zstd magic → error với message clear.
        let fake = b"\x1f\x8b\x08\x00random gzip-ish".to_vec();
        let err = zstd_decompress(&fake).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("zstd") || msg.contains("payload"),
            "error message phải mention format: {msg}"
        );
    }

    #[test]
    fn zstd_compresses_large_ratio() {
        // SQLite-like repetitive data nén zstd ratio > 5×.
        let input = vec![0u8; 100_000];
        let compressed = zstd_compress_mt(&input).unwrap();
        let ratio = input.len() as f64 / compressed.len() as f64;
        assert!(
            ratio > 5.0,
            "zstd trên data repetitive phải ratio > 5× (got {ratio:.1}×)"
        );
    }

    #[test]
    fn md5_hex_deterministic() {
        let a = md5_hex(b"hello world");
        let b = md5_hex(b"hello world");
        let c = md5_hex(b"hello worlD"); // different
        assert_eq!(a, b, "MD5 deterministic");
        assert_ne!(a, c, "different input → different hash");
        assert_eq!(a.len(), 32, "MD5 hex = 32 chars");
    }
}
