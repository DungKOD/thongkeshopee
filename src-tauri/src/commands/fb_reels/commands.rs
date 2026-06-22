//! Tauri commands cho FB Reels — gọi từ React UI qua wrapper `src/lib/tauri.ts`.

use rusqlite::params;
use tauri::{AppHandle, Emitter, State};

use crate::commands::{CmdError, CmdResult};
use crate::db::FbReelsDbState;

use super::graph_api;
use super::types::*;
use super::upload;

const USER_AGENT: &str = "ThongKeShopee/0.11.0 (FbReelsUploader)";
const MAX_FILE_SIZE: i64 = 100 * 1024 * 1024;

fn http_client() -> CmdResult<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(std::time::Duration::from_secs(600))
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

/// Lưu/upsert danh sách Pages vào DB.
#[tauri::command]
pub fn fb_save_pages(
    db: State<'_, FbReelsDbState>,
    pages: Vec<FbPageWithToken>,
) -> CmdResult<()> {
    let mut conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let tx = conn.transaction()?;
    let now = now_ms();
    for p in &pages {
        tx.execute(
            "INSERT INTO fb_pages(page_id, name, access_token, added_at_ms)
             VALUES(?1, ?2, ?3, ?4)
             ON CONFLICT(page_id) DO UPDATE SET
                 name = excluded.name,
                 access_token = excluded.access_token,
                 added_at_ms = excluded.added_at_ms",
            params![p.page_id, p.name, p.access_token, now],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// List Pages đã lưu — không trả token, frontend chỉ cần page_id + name.
#[tauri::command]
pub fn fb_list_pages(db: State<'_, FbReelsDbState>) -> CmdResult<Vec<FbPage>> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn
        .prepare("SELECT page_id, name FROM fb_pages ORDER BY added_at_ms ASC")?;
    let pages: Vec<FbPage> = stmt
        .query_map([], |r| {
            Ok(FbPage {
                page_id: r.get(0)?,
                name: r.get(1)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(pages)
}

/// Xóa Page khỏi DB (không ảnh hưởng posts đã đăng/lưu lịch sử).
#[tauri::command]
pub fn fb_delete_page(
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
pub fn fb_enqueue_reel(
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
                    r.file_path, r.file_size, r.caption, r.scheduled_time_ms
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
                ))
            },
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                CmdError::msg("Post không tồn tại hoặc Page đã bị xóa")
            }
            other => CmdError::Db(other),
        })?;
    Ok(UploadJobData {
        page_id: row.0,
        page_token: row.1,
        file_path: row.2,
        file_size: row.3,
        caption: row.4,
        scheduled_time_ms: row.5,
    })
}

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
    conn.execute(
        "UPDATE fb_reel_posts SET
             status          = ?1,
             progress        = COALESCE(?2, progress),
             fb_video_id     = COALESCE(?3, fb_video_id),
             fb_permalink    = COALESCE(?4, fb_permalink),
             error_message   = ?5,
             published_at_ms = COALESCE(?6, published_at_ms)
         WHERE id = ?7",
        params![
            status,
            progress,
            fb_video_id,
            fb_permalink,
            error_message,
            published_at,
            post_id
        ],
    )?;
    Ok(())
}

/// Upload 1 post lên FB. Frontend gọi sequential cho từng post trong queue.
///
/// Emit `fb_upload_progress` các bước: uploading (0→100), publishing,
/// published/scheduled, failed.
#[tauri::command]
pub async fn fb_upload_reel(
    app: AppHandle,
    db: State<'_, FbReelsDbState>,
    post_id: i64,
) -> CmdResult<()> {
    let job = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        load_job(&conn, post_id)?
    };

    let client = http_client()?;
    let total = job.file_size as u64;

    // Reset error + chuyển status=uploading.
    update_post_status(&db, post_id, "uploading", Some(0), None, None, None, false)?;
    let _ = app.emit(
        "fb_upload_progress",
        UploadProgress {
            post_id,
            status: "uploading".into(),
            progress: 0,
            bytes_uploaded: 0,
            bytes_total: total,
        },
    );

    // Bước 1: start session.
    let start_resp =
        match graph_api::start_upload(&client, &job.page_id, &job.page_token).await {
            Ok(r) => r,
            Err(e) => {
                let msg = e.to_string();
                let _ = update_post_status(
                    &db, post_id, "failed", None, None, None, Some(&msg), false,
                );
                let _ = app.emit(
                    "fb_upload_progress",
                    UploadProgress {
                        post_id,
                        status: "failed".into(),
                        progress: 0,
                        bytes_uploaded: 0,
                        bytes_total: total,
                    },
                );
                return Err(CmdError::msg(msg));
            }
        };

    // Bước 2: stream binary với progress.
    if let Err(e) = upload::stream_upload_file(
        &client,
        &start_resp.upload_url,
        &job.page_token,
        &job.file_path,
        total,
        app.clone(),
        post_id,
    )
    .await
    {
        let msg = e.to_string();
        let _ = update_post_status(
            &db, post_id, "failed", None, Some(&start_resp.video_id), None, Some(&msg), false,
        );
        let _ = app.emit(
            "fb_upload_progress",
            UploadProgress {
                post_id,
                status: "failed".into(),
                progress: 0,
                bytes_uploaded: 0,
                bytes_total: total,
            },
        );
        return Err(CmdError::msg(msg));
    }

    // Bước 3: finish.
    update_post_status(
        &db, post_id, "publishing", Some(100), Some(&start_resp.video_id), None, None, false,
    )?;
    let _ = app.emit(
        "fb_upload_progress",
        UploadProgress {
            post_id,
            status: "publishing".into(),
            progress: 100,
            bytes_uploaded: total,
            bytes_total: total,
        },
    );

    let scheduled_sec = job.scheduled_time_ms.map(|ms| ms / 1000);
    if let Err(e) = graph_api::finish_upload(
        &client,
        &job.page_id,
        &start_resp.video_id,
        &job.page_token,
        job.caption.as_deref(),
        scheduled_sec,
    )
    .await
    {
        let msg = e.to_string();
        let _ = update_post_status(
            &db, post_id, "failed", None, Some(&start_resp.video_id), None, Some(&msg), false,
        );
        let _ = app.emit(
            "fb_upload_progress",
            UploadProgress {
                post_id,
                status: "failed".into(),
                progress: 100,
                bytes_uploaded: total,
                bytes_total: total,
            },
        );
        return Err(CmdError::msg(msg));
    }

    // Bước 4: fetch permalink (best-effort).
    let permalink = graph_api::fetch_permalink(&client, &start_resp.video_id, &job.page_token)
        .await
        .ok()
        .flatten();

    let final_status = if scheduled_sec.is_some() {
        "scheduled"
    } else {
        "published"
    };
    update_post_status(
        &db,
        post_id,
        final_status,
        Some(100),
        Some(&start_resp.video_id),
        permalink.as_deref(),
        None,
        true,
    )?;
    let _ = app.emit(
        "fb_upload_progress",
        UploadProgress {
            post_id,
            status: final_status.into(),
            progress: 100,
            bytes_uploaded: total,
            bytes_total: total,
        },
    );

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
pub fn fb_list_posts(
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
pub fn fb_delete_post(
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
