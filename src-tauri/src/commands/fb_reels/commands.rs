//! Tauri commands cho FB Reels — gọi từ React UI qua wrapper `src/lib/tauri.ts`.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rusqlite::params;
use sha2::{Digest, Sha256};
use tauri::{AppHandle, Emitter, State};

use crate::commands::{CmdError, CmdResult};
use crate::db::FbReelsDbState;

use super::graph_api;
use super::maintenance::{mark_token_expired, UploadLocks};
use super::types::*;
use super::upload;

const MAX_FILE_SIZE: i64 = 100 * 1024 * 1024;

/// Số lần retry tối đa cho mỗi bước (start/finish). Bước stream binary có retry
/// riêng bên trong `upload::stream_upload_file` không apply ở đây. Backoff
/// exponential 2s, 4s.
const HTTP_MAX_RETRIES: u32 = 2;

fn user_agent() -> String {
    format!(
        "ThongKeShopee/{} (FbReelsUploader)",
        env!("CARGO_PKG_VERSION")
    )
}

fn http_client() -> CmdResult<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(user_agent())
        .timeout(Duration::from_secs(600))
        .build()
        .map_err(|e| CmdError::msg(format!("không tạo được HTTP client: {e}")))
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Validate token + trả về danh sách Pages user quản lý.
/// Không lưu DB — frontend tick chọn Page nào rồi gọi `fb_save_pages` sau.
#[tauri::command]
pub async fn fb_validate_token(token: String) -> CmdResult<Vec<FbPageWithToken>> {
    let client = http_client()?;
    graph_api::list_pages_from_token(&client, &token)
        .await
        .map_err(|e| CmdError::msg(e.to_string()))
}

/// Lưu/upsert danh sách Pages vào DB. Reset `token_expired=0` vì user vừa
/// validate token mới → tài khoản OK trở lại.
#[tauri::command]
pub async fn fb_save_pages(
    db: State<'_, FbReelsDbState>,
    pages: Vec<FbPageWithToken>,
) -> CmdResult<()> {
    let mut conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let tx = conn.transaction()?;
    let now = now_ms();
    for p in &pages {
        tx.execute(
            "INSERT INTO fb_pages(page_id, name, access_token, added_at_ms, token_expired)
             VALUES(?1, ?2, ?3, ?4, 0)
             ON CONFLICT(page_id) DO UPDATE SET
                 name = excluded.name,
                 access_token = excluded.access_token,
                 added_at_ms = excluded.added_at_ms,
                 token_expired = 0",
            params![p.page_id, p.name, p.access_token, now],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Hash 8 hex đầu của SHA-256(token) — dùng cho UI group page theo token.
/// Không leak token thật (sha256 không reversible).
fn token_short_hash(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    // 4 bytes đầu → 8 hex chars. Đủ phân biệt token khác nhau cho 1 user (~50
    // page tối đa), collision chance 1/2^32.
    let mut out = String::with_capacity(8);
    for b in &digest[..4] {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// List Pages đã lưu — không trả raw token, kèm `token_expired` + `token_hash`
/// (8 hex SHA-256). UI dùng hash để tô màu page cùng token.
#[tauri::command]
pub async fn fb_list_pages(
    db: State<'_, FbReelsDbState>,
) -> CmdResult<Vec<FbPage>> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare(
        "SELECT page_id, name, token_expired, access_token
         FROM fb_pages ORDER BY added_at_ms ASC",
    )?;
    let pages: Vec<FbPage> = stmt
        .query_map([], |r| {
            let token: String = r.get(3)?;
            Ok(FbPage {
                page_id: r.get(0)?,
                name: r.get(1)?,
                token_expired: r.get::<_, i64>(2)? != 0,
                token_hash: token_short_hash(&token),
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(pages)
}

/// Lưu/upsert 1 User Token. Dedupe theo `token_hash` — paste lại cùng token →
/// trả lại `id` cũ thay vì insert mới. Label optional; auto-gen từ timestamp
/// nếu không truyền.
#[tauri::command]
pub async fn fb_save_auth_token(
    db: State<'_, FbReelsDbState>,
    token: String,
    label: Option<String>,
) -> CmdResult<i64> {
    let token = token.trim().to_string();
    if token.is_empty() {
        return Err(CmdError::msg("Token rỗng"));
    }
    let hash = token_short_hash(&token);
    let label = label.unwrap_or_else(|| format!("Token #{}", &hash));

    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    if let Some(id) = conn
        .query_row(
            "SELECT id FROM fb_auth_tokens WHERE token_hash = ?1",
            params![hash],
            |r| r.get::<_, i64>(0),
        )
        .ok()
    {
        // Token đã có — reset expired flag (user vừa re-validate thành công).
        conn.execute(
            "UPDATE fb_auth_tokens SET expired = 0 WHERE id = ?1",
            params![id],
        )?;
        return Ok(id);
    }
    conn.execute(
        "INSERT INTO fb_auth_tokens(label, access_token, token_hash, added_at_ms, expired)
         VALUES(?1, ?2, ?3, ?4, 0)",
        params![label, token, hash, now_ms()],
    )?;
    Ok(conn.last_insert_rowid())
}

/// List User Tokens đã lưu — không trả raw token; kèm `token_hash` cho color
/// và `expired` cho badge.
#[tauri::command]
pub async fn fb_list_auth_tokens(
    db: State<'_, FbReelsDbState>,
) -> CmdResult<Vec<FbAuthToken>> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare(
        "SELECT id, label, token_hash, added_at_ms, expired
         FROM fb_auth_tokens ORDER BY added_at_ms ASC",
    )?;
    let tokens: Vec<FbAuthToken> = stmt
        .query_map([], |r| {
            Ok(FbAuthToken {
                id: r.get(0)?,
                label: r.get(1)?,
                token_hash: r.get(2)?,
                added_at_ms: r.get(3)?,
                expired: r.get::<_, i64>(4)? != 0,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(tokens)
}

/// Trả raw access_token của 1 auth token — UI dùng cho copy/reveal.
#[tauri::command]
pub async fn fb_get_auth_token(
    db: State<'_, FbReelsDbState>,
    id: i64,
) -> CmdResult<String> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let token: String = conn
        .query_row(
            "SELECT access_token FROM fb_auth_tokens WHERE id = ?1",
            params![id],
            |r| r.get(0),
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                CmdError::msg("Auth token không tồn tại")
            }
            other => CmdError::Db(other),
        })?;
    Ok(token)
}

/// Đổi label hiển thị của auth token (vd "Personal", "Business").
#[tauri::command]
pub async fn fb_update_auth_token_label(
    db: State<'_, FbReelsDbState>,
    id: i64,
    label: String,
) -> CmdResult<()> {
    let label = label.trim();
    if label.is_empty() {
        return Err(CmdError::msg("Label rỗng"));
    }
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "UPDATE fb_auth_tokens SET label = ?1 WHERE id = ?2",
        params![label, id],
    )?;
    Ok(())
}

/// Xóa auth token — KHÔNG cascade Page (Page giữ access_token riêng, vẫn hoạt
/// động độc lập). User muốn xóa Page thì xóa riêng.
#[tauri::command]
pub async fn fb_delete_auth_token(
    db: State<'_, FbReelsDbState>,
    id: i64,
) -> CmdResult<()> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "DELETE FROM fb_auth_tokens WHERE id = ?1",
        params![id],
    )?;
    Ok(())
}

/// Lấy access_token đã lưu của 1 Page — dùng cho UI hiển thị/copy token
/// (user khôi phục/migrate sang máy khác mà không cần làm lại 3 bước Graph API).
/// Trả lỗi rõ ràng nếu page không tồn tại.
#[tauri::command]
pub async fn fb_get_page_token(
    db: State<'_, FbReelsDbState>,
    page_id: String,
) -> CmdResult<String> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let token: String = conn
        .query_row(
            "SELECT access_token FROM fb_pages WHERE page_id = ?1",
            params![page_id],
            |r| r.get(0),
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                CmdError::msg("Page không tồn tại trong DB")
            }
            other => CmdError::Db(other),
        })?;
    Ok(token)
}

/// Xóa Page khỏi DB (không ảnh hưởng posts đã đăng/lưu lịch sử).
#[tauri::command]
pub async fn fb_delete_page(
    db: State<'_, FbReelsDbState>,
    page_id: String,
) -> CmdResult<()> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute("DELETE FROM fb_pages WHERE page_id = ?1", params![page_id])?;
    Ok(())
}

/// Thêm 1 post vào queue (status `pending`). Trả về `post_id` để frontend
/// theo dõi qua progress events.
///
/// Validate: file tồn tại + size ≤ 100MB. Schedule time tùy frontend check.
#[tauri::command]
pub async fn fb_enqueue_reel(
    db: State<'_, FbReelsDbState>,
    page_id: String,
    file_path: String,
    caption: Option<String>,
    scheduled_time_ms: Option<i64>,
) -> CmdResult<i64> {
    let file_size = std::fs::metadata(&file_path)
        .map_err(|e| CmdError::msg(format!("không đọc được file: {e}")))?
        .len() as i64;

    if file_size > MAX_FILE_SIZE {
        return Err(CmdError::msg(format!(
            "File quá lớn ({} MB > 100 MB). Phiên bản này chỉ hỗ trợ file ≤ 100MB.",
            file_size / 1_048_576
        )));
    }

    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let page_name: String = conn
        .query_row(
            "SELECT name FROM fb_pages WHERE page_id = ?1",
            params![page_id],
            |r| r.get(0),
        )
        .map_err(|_| CmdError::msg("Page không tồn tại — hãy thêm Page trước"))?;

    conn.execute(
        "INSERT INTO fb_reel_posts
         (page_id, page_name, file_path, file_size, caption, scheduled_time_ms,
          status, progress, created_at_ms)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, 'pending', 0, ?7)",
        params![
            page_id, page_name, file_path, file_size, caption, scheduled_time_ms, now_ms()
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Job data load 1 lần đầu upload — không hold lock qua await.
struct UploadJobData {
    page_id: String,
    page_token: String,
    file_path: String,
    file_size: i64,
    caption: Option<String>,
    scheduled_time_ms: Option<i64>,
}

fn load_job(conn: &rusqlite::Connection, post_id: i64) -> CmdResult<UploadJobData> {
    let row = conn
        .query_row(
            "SELECT p.page_id, p.access_token,
                    r.file_path, r.file_size, r.caption, r.scheduled_time_ms,
                    p.token_expired
             FROM fb_reel_posts r
             JOIN fb_pages p ON p.page_id = r.page_id
             WHERE r.id = ?1",
            params![post_id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                    r.get::<_, i64>(6)?,
                ))
            },
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                CmdError::msg("Post không tồn tại hoặc Page đã bị xóa")
            }
            other => CmdError::Db(other),
        })?;
    if row.6 != 0 {
        return Err(CmdError::msg(
            "Token của Page đã hết hạn — vào Quản lý Page để cập nhật token mới",
        ));
    }
    Ok(UploadJobData {
        page_id: row.0,
        page_token: row.1,
        file_path: row.2,
        file_size: row.3,
        caption: row.4,
        scheduled_time_ms: row.5,
    })
}

/// Update fields chính của post trong DB. `progress=None` → giữ nguyên,
/// `progress=Some(n)` → ghi đè bất kể giá trị cũ (cho phép reset 100→0 lúc
/// retry). `error_message=None` → wipe (cho retry/clear), `Some` → set.
#[allow(clippy::too_many_arguments)]
fn update_post_status(
    db: &FbReelsDbState,
    post_id: i64,
    status: &str,
    progress: Option<i64>,
    fb_video_id: Option<&str>,
    fb_permalink: Option<&str>,
    error_message: Option<&str>,
    set_published_at: bool,
) -> CmdResult<()> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let published_at: Option<i64> = if set_published_at { Some(now_ms()) } else { None };
    // `progress` KHÔNG dùng COALESCE — caller chủ động pass None để giữ giá
    // trị cũ, Some(n) để OVERWRITE (vd retry phải reset 100→0 ngay).
    if let Some(p) = progress {
        conn.execute(
            "UPDATE fb_reel_posts SET
                 status          = ?1,
                 progress        = ?2,
                 fb_video_id     = COALESCE(?3, fb_video_id),
                 fb_permalink    = COALESCE(?4, fb_permalink),
                 error_message   = ?5,
                 published_at_ms = COALESCE(?6, published_at_ms)
             WHERE id = ?7",
            params![
                status,
                p,
                fb_video_id,
                fb_permalink,
                error_message,
                published_at,
                post_id
            ],
        )?;
    } else {
        conn.execute(
            "UPDATE fb_reel_posts SET
                 status          = ?1,
                 fb_video_id     = COALESCE(?2, fb_video_id),
                 fb_permalink    = COALESCE(?3, fb_permalink),
                 error_message   = ?4,
                 published_at_ms = COALESCE(?5, published_at_ms)
             WHERE id = ?6",
            params![
                status,
                fb_video_id,
                fb_permalink,
                error_message,
                published_at,
                post_id
            ],
        )?;
    }
    Ok(())
}

/// Helper: emit progress event qua Tauri.
fn emit_progress(
    app: &AppHandle,
    post_id: i64,
    status: &str,
    progress: i64,
    bytes_uploaded: u64,
    bytes_total: u64,
) {
    let _ = app.emit(
        "fb_upload_progress",
        UploadProgress {
            post_id,
            status: status.into(),
            progress,
            bytes_uploaded,
            bytes_total,
        },
    );
}

/// Detect lỗi token expired từ message error, mark page → background polls
/// + UI sẽ pickup.
fn maybe_mark_token_expired(db: &FbReelsDbState, page_id: &str, err_msg: &str) {
    if graph_api::is_token_invalid(err_msg) {
        let _ = mark_token_expired(db, page_id);
    }
}

/// RAII guard tự release lock khi drop — tránh leak lock khi early-return.
struct LockGuard {
    locks: Arc<UploadLocks>,
    post_id: i64,
}
impl Drop for LockGuard {
    fn drop(&mut self) {
        self.locks.release(self.post_id);
    }
}

/// Retry HTTP call với exponential backoff. Chỉ retry lỗi network/timeout
/// (không retry lỗi auth/4xx — fix root cause, không spam). `op` là async
/// closure trả `Result`; lỗi mà message chứa keyword retryable thì retry.
async fn with_retry<F, Fut, T>(label: &str, mut op: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut delay_secs = 2u64;
    for attempt in 0..=HTTP_MAX_RETRIES {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let msg = e.to_string();
                let retryable = !graph_api::is_token_invalid(&msg)
                    && (msg.contains("timeout")
                        || msg.contains("connection")
                        || msg.contains("dns")
                        || msg.contains("network")
                        || msg.contains("reset")
                        || msg.contains("broken pipe")
                        || msg.contains("EOF")
                        || msg.contains("upload binary thất bại")
                        || msg.contains("không gọi được"));
                if attempt < HTTP_MAX_RETRIES && retryable {
                    eprintln!(
                        "[fb_reels] {label} retry {}/{} sau lỗi: {msg}",
                        attempt + 1,
                        HTTP_MAX_RETRIES
                    );
                    tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                    delay_secs *= 2;
                    continue;
                }
                return Err(e);
            }
        }
    }
    unreachable!()
}

/// Upload 1 post lên FB. Frontend gọi sequential cho từng post trong queue.
///
/// Emit `fb_upload_progress` các bước: uploading (0→100), publishing,
/// published/scheduled, failed.
///
/// **Lock per-post** qua `UploadLocks`: cùng `post_id` không thể chạy 2 invoke
/// song song (user spam Retry / publishNow đồng thời) → tránh double-upload,
/// progress event chồng chéo, video_id leak.
#[tauri::command]
pub async fn fb_upload_reel(
    app: AppHandle,
    db: State<'_, FbReelsDbState>,
    locks: State<'_, Arc<UploadLocks>>,
    post_id: i64,
) -> CmdResult<()> {
    // Acquire per-post lock. RAII guard tự release khi function return.
    if !locks.try_acquire(post_id) {
        return Err(CmdError::msg(
            "Post đang được upload — chờ session hiện tại xong",
        ));
    }
    let _guard = LockGuard {
        locks: locks.inner().clone(),
        post_id,
    };

    let job = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        load_job(&conn, post_id)?
    };

    let client = http_client()?;
    let total = job.file_size as u64;
    let file_name = Path::new(&job.file_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("video.mp4")
        .to_string();

    // Reset progress về 0 + clear error trước khi start. `progress=Some(0)`
    // overwrite giá trị cũ (vd retry sau failed publishing để lại progress=100).
    update_post_status(&db, post_id, "uploading", Some(0), None, None, None, false)?;
    emit_progress(&app, post_id, "uploading", 0, 0, total);

    // Bước 1: start session (có retry).
    let start_resp = match with_retry("start_upload", || {
        graph_api::start_upload(&client, &job.page_id, &job.page_token)
    })
    .await
    {
        Ok(r) => r,
        Err(e) => {
            let msg = e.to_string();
            maybe_mark_token_expired(&db, &job.page_id, &msg);
            let _ = update_post_status(
                &db, post_id, "failed", None, None, None, Some(&msg), false,
            );
            emit_progress(&app, post_id, "failed", 0, 0, total);
            return Err(CmdError::msg(msg));
        }
    };

    // Lưu video_id NGAY khi start succeeded — đảm bảo consistency cho tất cả
    // failure branch sau này (không bao giờ có post fail mà thiếu video_id
    // khi FB đã cấp).
    let _ = update_post_status(
        &db,
        post_id,
        "uploading",
        None,
        Some(&start_resp.video_id),
        None,
        None,
        false,
    );

    // Bước 2: stream binary với progress + retry. Mỗi attempt mở file lại từ
    // đầu (không resumable trong v1, chấp nhận re-upload full để đơn giản).
    let upload_result = with_retry("stream_upload", || {
        upload::stream_upload_file(
            &client,
            &start_resp.upload_url,
            &job.page_token,
            &job.file_path,
            &file_name,
            total,
            app.clone(),
            post_id,
        )
    })
    .await;

    if let Err(e) = upload_result {
        let msg = e.to_string();
        maybe_mark_token_expired(&db, &job.page_id, &msg);
        let _ = update_post_status(
            &db,
            post_id,
            "failed",
            None,
            Some(&start_resp.video_id),
            None,
            Some(&msg),
            false,
        );
        emit_progress(&app, post_id, "failed", 0, 0, total);
        return Err(CmdError::msg(msg));
    }

    // Bước 3: finish (có retry).
    update_post_status(
        &db,
        post_id,
        "publishing",
        Some(100),
        Some(&start_resp.video_id),
        None,
        None,
        false,
    )?;
    emit_progress(&app, post_id, "publishing", 100, total, total);

    let scheduled_sec = job.scheduled_time_ms.map(|ms| ms / 1000);
    let finish_result = with_retry("finish_upload", || {
        graph_api::finish_upload(
            &client,
            &job.page_id,
            &start_resp.video_id,
            &job.page_token,
            job.caption.as_deref(),
            scheduled_sec,
        )
    })
    .await;

    if let Err(e) = finish_result {
        let msg = e.to_string();
        maybe_mark_token_expired(&db, &job.page_id, &msg);
        let _ = update_post_status(
            &db,
            post_id,
            "failed",
            None,
            Some(&start_resp.video_id),
            None,
            Some(&msg),
            false,
        );
        emit_progress(&app, post_id, "failed", 100, total, total);
        return Err(CmdError::msg(msg));
    }

    // Bước 4: xác định status thực tế.
    //
    // KHÔNG mark `published` ngay sau khi finish_upload trả 2xx — FB còn phải
    // transcode + validate, có thể fail ở bước này (sai codec, vi phạm content
    // policy, ratio sai). Thay vào đó:
    //
    // - Scheduled video: set `scheduled`, background poll check khi tới giờ.
    // - Immediate publish: set `processing` rồi verify ngay 1 lần. Nếu FB đã
    //   xong → `published` luôn cho UX tốt. Nếu chưa → giữ `processing`,
    //   background poll (mỗi 60s) tiếp tục check tới khi published/failed.
    if scheduled_sec.is_some() {
        update_post_status(
            &db,
            post_id,
            "scheduled",
            Some(100),
            Some(&start_resp.video_id),
            None,
            None,
            false,
        )?;
        emit_progress(&app, post_id, "scheduled", 100, total, total);
        return Ok(());
    }

    // Default: processing — đảm bảo có row chính xác kể cả nếu fetch_video_status
    // dưới đây lỗi network (UI vẫn thấy trạng thái phân biệt với "publishing").
    update_post_status(
        &db,
        post_id,
        "processing",
        Some(100),
        Some(&start_resp.video_id),
        None,
        None,
        false,
    )?;
    emit_progress(&app, post_id, "processing", 100, total, total);

    // Immediate verify (best-effort, không retry — background poll cover case
    // FB chậm). Nếu FB đã processed + published xong trong lần gọi đầu thì
    // skip luôn processing UI.
    match graph_api::fetch_video_status(&client, &start_resp.video_id, &job.page_token).await {
        Ok(graph_api::VideoStatus::Published { permalink_url }) => {
            update_post_status(
                &db,
                post_id,
                "published",
                Some(100),
                None,
                permalink_url.as_deref(),
                None,
                true,
            )?;
            emit_progress(&app, post_id, "published", 100, total, total);
        }
        Ok(graph_api::VideoStatus::Failed { reason }) => {
            let msg = format!("FB từ chối video: {reason}");
            update_post_status(
                &db,
                post_id,
                "failed",
                None,
                None,
                None,
                Some(&msg),
                false,
            )?;
            emit_progress(&app, post_id, "failed", 100, total, total);
            return Err(CmdError::msg(msg));
        }
        Ok(graph_api::VideoStatus::TokenExpired) => {
            let _ = mark_token_expired(&db, &job.page_id);
            let msg = "Token Page hết hạn ngay sau publish — cập nhật token và Cập nhật trạng thái thủ công".to_string();
            update_post_status(
                &db,
                post_id,
                "failed",
                None,
                None,
                None,
                Some(&msg),
                false,
            )?;
            emit_progress(&app, post_id, "failed", 100, total, total);
            return Err(CmdError::msg(msg));
        }
        Ok(graph_api::VideoStatus::Pending) | Err(_) => {
            // Pending: giữ `processing` — background poll handle.
            // Err network: cũng giữ `processing` (poll sẽ retry).
        }
    }

    Ok(())
}

/// Filter cho `fb_list_posts`.
#[derive(serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListPostsFilter {
    pub page_id: Option<String>,
    pub status: Option<String>,
    pub limit: Option<i64>,
}

/// List post history — filter optional theo page/status, paginate qua `limit`.
#[tauri::command]
pub async fn fb_list_posts(
    db: State<'_, FbReelsDbState>,
    filter: Option<ListPostsFilter>,
) -> CmdResult<Vec<FbReelPost>> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let f = filter.unwrap_or_default();

    let mut sql = String::from(
        "SELECT id, page_id, page_name, file_path, file_size, caption,
                scheduled_time_ms, status, progress, fb_video_id, fb_permalink,
                error_message, created_at_ms, published_at_ms
         FROM fb_reel_posts WHERE 1=1",
    );
    let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![];
    if let Some(p) = f.page_id.filter(|s| !s.is_empty()) {
        sql.push_str(" AND page_id = ?");
        args.push(Box::new(p));
    }
    if let Some(s) = f.status.filter(|s| !s.is_empty()) {
        sql.push_str(" AND status = ?");
        args.push(Box::new(s));
    }
    sql.push_str(" ORDER BY created_at_ms DESC");
    let limit = f.limit.unwrap_or(200).clamp(1, 1000);
    sql.push_str(&format!(" LIMIT {limit}"));

    let arg_refs: Vec<&dyn rusqlite::ToSql> = args.iter().map(|b| b.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let posts: Vec<FbReelPost> = stmt
        .query_map(rusqlite::params_from_iter(arg_refs), |r| {
            Ok(FbReelPost {
                id: r.get(0)?,
                page_id: r.get(1)?,
                page_name: r.get(2)?,
                file_path: r.get(3)?,
                file_size: r.get(4)?,
                caption: r.get(5)?,
                scheduled_time_ms: r.get(6)?,
                status: r.get(7)?,
                progress: r.get(8)?,
                fb_video_id: r.get(9)?,
                fb_permalink: r.get(10)?,
                error_message: r.get(11)?,
                created_at_ms: r.get(12)?,
                published_at_ms: r.get(13)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(posts)
}

/// Xóa 1 post khỏi history. Không un-publish được trên FB — chỉ xóa local record.
#[tauri::command]
pub async fn fb_delete_post(
    db: State<'_, FbReelsDbState>,
    post_id: i64,
) -> CmdResult<()> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "DELETE FROM fb_reel_posts WHERE id = ?1",
        params![post_id],
    )?;
    Ok(())
}

/// Re-fetch permalink + status cho 1 post on-demand — UI button "Cập nhật"
/// để user trigger thủ công thay vì đợi background poll (~60s).
#[tauri::command]
pub async fn fb_refetch_post_status(
    app: AppHandle,
    db: State<'_, FbReelsDbState>,
    post_id: i64,
) -> CmdResult<()> {
    let (video_id, token, page_id, current_status): (String, String, String, String) = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        conn.query_row(
            "SELECT r.fb_video_id, p.access_token, p.page_id, r.status
             FROM fb_reel_posts r JOIN fb_pages p ON p.page_id = r.page_id
             WHERE r.id = ?1 AND r.fb_video_id IS NOT NULL",
            params![post_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(|_| {
            CmdError::msg("Post chưa có video_id — chưa upload thành công lần nào")
        })?
    };

    let client = http_client()?;
    match graph_api::fetch_video_status(&client, &video_id, &token).await {
        Ok(graph_api::VideoStatus::Published { permalink_url }) => {
            let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
            conn.execute(
                "UPDATE fb_reel_posts SET
                     status = 'published',
                     fb_permalink = COALESCE(?1, fb_permalink),
                     published_at_ms = COALESCE(published_at_ms, ?2),
                     error_message = NULL
                 WHERE id = ?3",
                params![permalink_url, now_ms(), post_id],
            )?;
            drop(conn);
            emit_progress(&app, post_id, "published", 100, 0, 0);
            Ok(())
        }
        Ok(graph_api::VideoStatus::Pending) => {
            // FB vẫn đang xử lý — không đổi status, chỉ thông báo.
            Err(CmdError::msg(format!(
                "FB vẫn đang xử lý video (status hiện tại: {current_status})"
            )))
        }
        Ok(graph_api::VideoStatus::Failed { reason }) => {
            // FB từ chối video — đánh dấu failed local + lưu reason để UI hiện.
            let msg = format!("FB từ chối video: {reason}");
            let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
            conn.execute(
                "UPDATE fb_reel_posts SET
                     status = 'failed',
                     error_message = ?1
                 WHERE id = ?2",
                params![msg, post_id],
            )?;
            drop(conn);
            emit_progress(&app, post_id, "failed", 100, 0, 0);
            Err(CmdError::msg(msg))
        }
        Ok(graph_api::VideoStatus::TokenExpired) => {
            let _ = mark_token_expired(&db, &page_id);
            Err(CmdError::msg(
                "Token Page hết hạn — cập nhật token trong Quản lý Page",
            ))
        }
        Err(e) => Err(CmdError::msg(e.to_string())),
    }
}

/// Trả raw JSON FB Graph API trả về cho video — dùng cho UI hiển thị diagnostic
/// khi post kẹt `processing` hoặc `failed`. User copy paste cho developer xem
/// chính xác FB return structure gì (errors[], publish_status, phase status...).
#[tauri::command]
pub async fn fb_debug_video_info(
    db: State<'_, FbReelsDbState>,
    post_id: i64,
) -> CmdResult<String> {
    let (video_id, token): (String, String) = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        conn.query_row(
            "SELECT r.fb_video_id, p.access_token
             FROM fb_reel_posts r JOIN fb_pages p ON p.page_id = r.page_id
             WHERE r.id = ?1 AND r.fb_video_id IS NOT NULL",
            params![post_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(|_| {
            CmdError::msg("Post chưa có video_id — chưa upload thành công lần nào")
        })?
    };

    let client = http_client()?;
    let body = graph_api::fetch_video_info_raw(&client, &video_id, &token)
        .await
        .map_err(|e| CmdError::msg(e.to_string()))?;
    // Pretty-print JSON nếu parse được, để UI hiển thị dễ đọc; fallback raw text.
    match serde_json::from_str::<serde_json::Value>(&body) {
        Ok(v) => Ok(
            serde_json::to_string_pretty(&v).unwrap_or(body),
        ),
        Err(_) => Ok(body),
    }
}
