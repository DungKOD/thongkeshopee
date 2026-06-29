//! Background maintenance cho FB Reels: startup recovery + scheduled polling +
//! permalink re-fetch + token expired check.
//!
//! Tách ra module riêng để giữ `commands.rs` chỉ chứa Tauri command surface,
//! còn logic chạy nền tách rõ ràng. Tất cả task background dùng chung 1
//! `reqwest::Client` (kept short-lived per task để config riêng), DB state
//! lấy qua `app.state::<FbReelsDbState>()`.

use std::collections::HashSet;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use anyhow::Result;
use rusqlite::params;
use tauri::{AppHandle, Emitter, Manager};

use crate::db::FbReelsDbState;

use super::graph_api;
use super::types::UploadProgress;

/// Lock per-post tránh user spam Retry / "Đăng ngay" chồng nhau → 2 invoke
/// `fb_upload_reel(same id)` start 2 upload session cho cùng row → double
/// video_id trên FB + progress event chồng chéo. Set chứa post_id đang được
/// upload; insert thất bại = đã có upload → reject.
///
/// Dùng `std::sync::Mutex<HashSet>` thay vì `tokio::sync::Mutex` vì lock chỉ
/// giữ trong ~µs (insert/remove HashSet), không cần await-friendly.
pub struct UploadLocks(pub StdMutex<HashSet<i64>>);

impl UploadLocks {
    pub fn new() -> Self {
        Self(StdMutex::new(HashSet::new()))
    }

    /// Try-acquire lock cho `post_id`. Trả `true` nếu acquire thành công,
    /// `false` nếu đã có upload đang chạy cho post này.
    pub fn try_acquire(&self, post_id: i64) -> bool {
        let mut set = self.0.lock().unwrap_or_else(|p| p.into_inner());
        set.insert(post_id)
    }

    /// Release lock — phải gọi sau khi upload kết thúc (cả success lẫn fail).
    pub fn release(&self, post_id: i64) {
        let mut set = self.0.lock().unwrap_or_else(|p| p.into_inner());
        set.remove(&post_id);
    }
}

/// Reset rows kẹt `uploading`/`publishing` → `failed`. Trường hợp xảy ra: app
/// bị tắt (crash, force close, restart) khi upload đang chạy → row giữ status
/// "uploading" mãi mãi, UI hiện animation kẹt, KHÔNG có nút retry.
///
/// Chạy 1 lần lúc startup (sau khi setup DB) → user thấy row failed với nút
/// retry rõ ràng.
pub fn run_startup_recovery(state: &FbReelsDbState) -> Result<usize> {
    let conn = state.0.lock().unwrap_or_else(|p| p.into_inner());
    let n = conn.execute(
        "UPDATE fb_reel_posts
         SET status = 'failed',
             error_message = COALESCE(error_message, 'Bị gián đoạn khi app đóng — bấm Thử lại để upload lại')
         WHERE status IN ('uploading', 'publishing')",
        [],
    )?;
    Ok(n)
}

/// Spawn task background poll FB cho:
/// - Posts `scheduled` đã quá `scheduled_time_ms` + grace 5 phút → check trên
///   FB xem đã `published` thật chưa → cập nhật status local.
/// - Posts `published` mà `fb_permalink` còn null → re-fetch (FB cần ~30-60s
///   transcoding mới có permalink).
/// - Pages có `token_expired=1` không poll (đợi user refresh token).
///
/// Tick mỗi 60s — đủ rare để không spam Graph API quota, đủ frequent để user
/// không phải đợi lâu. Tất cả error log eprintln + continue (không crash task).
pub fn spawn_background_maintenance(app: AppHandle) {
    // Setup hook chạy sync trước khi tokio runtime enter context trên thread
    // này → `tokio::spawn` panic ("no reactor running"). `tauri::async_runtime::spawn`
    // submit task vào Tauri's tokio runtime, sau đó task chạy trong tokio
    // context bình thường (tokio::time::interval / tokio::time::sleep dùng được).
    tauri::async_runtime::spawn(async move {
        let client = match reqwest::Client::builder()
            .user_agent(format!(
                "ThongKeShopee/{} (FbReelsBg)",
                env!("CARGO_PKG_VERSION")
            ))
            .timeout(Duration::from_secs(30))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[fb_reels bg] build client fail: {e}");
                return;
            }
        };

        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            if let Err(e) = check_scheduled_posts(&app, &client).await {
                eprintln!("[fb_reels bg] check_scheduled error: {e}");
            }
            if let Err(e) = check_processing_posts(&app, &client).await {
                eprintln!("[fb_reels bg] check_processing error: {e}");
            }
            if let Err(e) = refetch_missing_permalinks(&app, &client).await {
                eprintln!("[fb_reels bg] refetch_permalink error: {e}");
            }
        }
    });
}

/// Query Graph API cho mỗi post `scheduled` đã qua thời điểm scheduled +
/// 5 phút grace (FB cần thời gian xử lý). Nếu API trả `status="VIDEO_STATUS_PROCESSED"`
/// hoặc `published=true` thì update DB.
async fn check_scheduled_posts(app: &AppHandle, client: &reqwest::Client) -> Result<()> {
    let state = app.state::<FbReelsDbState>();

    let now = chrono::Utc::now().timestamp_millis();
    let grace_cutoff = now - 5 * 60 * 1000;

    let candidates: Vec<(i64, String, String, String)> = {
        let conn = state.0.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt = conn.prepare(
            "SELECT r.id, r.fb_video_id, p.access_token, p.page_id
             FROM fb_reel_posts r
             JOIN fb_pages p ON p.page_id = r.page_id
             WHERE r.status = 'scheduled'
               AND r.fb_video_id IS NOT NULL
               AND r.scheduled_time_ms IS NOT NULL
               AND r.scheduled_time_ms < ?1
               AND p.token_expired = 0
             LIMIT 20",
        )?;
        let rows: Vec<(i64, Option<String>, String, String)> = stmt
            .query_map(params![grace_cutoff], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<_, _>>()?;
        rows.into_iter()
            .filter_map(|(id, vid, tok, pg)| vid.map(|v| (id, v, tok, pg)))
            .collect()
    };

    for (post_id, video_id, token, page_id) in candidates {
        match graph_api::fetch_video_status(client, &video_id, &token).await {
            Ok(graph_api::VideoStatus::Published { permalink_url }) => {
                let permalink = permalink_url;
                let conn = state.0.lock().unwrap_or_else(|p| p.into_inner());
                conn.execute(
                    "UPDATE fb_reel_posts SET
                         status = 'published',
                         fb_permalink = COALESCE(?1, fb_permalink),
                         published_at_ms = COALESCE(published_at_ms, ?2),
                         error_message = NULL
                     WHERE id = ?3 AND status = 'scheduled'",
                    params![permalink, chrono::Utc::now().timestamp_millis(), post_id],
                )?;
                drop(conn);
                let _ = app.emit(
                    "fb_upload_progress",
                    UploadProgress {
                        post_id,
                        status: "published".into(),
                        progress: 100,
                        bytes_uploaded: 0,
                        bytes_total: 0,
                    },
                );
            }
            Ok(graph_api::VideoStatus::Failed { reason }) => {
                // FB từ chối video sau khi nhận — không bao giờ publish nữa
                // dù scheduled time đã tới. Mark failed local.
                let msg = format!("FB từ chối video: {reason}");
                let conn = state.0.lock().unwrap_or_else(|p| p.into_inner());
                conn.execute(
                    "UPDATE fb_reel_posts SET
                         status = 'failed',
                         error_message = ?1
                     WHERE id = ?2 AND status = 'scheduled'",
                    params![msg, post_id],
                )?;
                drop(conn);
                let _ = app.emit(
                    "fb_upload_progress",
                    UploadProgress {
                        post_id,
                        status: "failed".into(),
                        progress: 100,
                        bytes_uploaded: 0,
                        bytes_total: 0,
                    },
                );
            }
            Ok(graph_api::VideoStatus::Pending) => {
                // FB chưa xử lý xong — chờ tick sau.
            }
            Ok(graph_api::VideoStatus::TokenExpired) => {
                mark_token_expired(&state, &page_id)?;
            }
            Err(e) => {
                eprintln!(
                    "[fb_reels bg] check video status post={post_id}: {e}"
                );
            }
        }
    }
    Ok(())
}

/// Poll FB cho posts đang ở status `processing` (đã finish_upload xong nhưng
/// FB chưa confirm publish). Transition:
/// - `Published` → status=`published` + permalink
/// - `Failed`    → status=`failed` + error_message (FB transcode/validate reject)
/// - `Pending`   → giữ nguyên `processing`, tick sau retry
///
/// Cap 30 post/tick — đủ cho user spam upload mà không quá tải Graph API quota.
async fn check_processing_posts(app: &AppHandle, client: &reqwest::Client) -> Result<()> {
    let state = app.state::<FbReelsDbState>();

    let candidates: Vec<(i64, String, String, String)> = {
        let conn = state.0.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt = conn.prepare(
            "SELECT r.id, r.fb_video_id, p.access_token, p.page_id
             FROM fb_reel_posts r
             JOIN fb_pages p ON p.page_id = r.page_id
             WHERE r.status = 'processing'
               AND r.fb_video_id IS NOT NULL
               AND p.token_expired = 0
             ORDER BY r.created_at_ms ASC
             LIMIT 30",
        )?;
        let rows: Vec<(i64, Option<String>, String, String)> = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<_, _>>()?;
        rows.into_iter()
            .filter_map(|(id, vid, tok, pg)| vid.map(|v| (id, v, tok, pg)))
            .collect()
    };

    for (post_id, video_id, token, page_id) in candidates {
        match graph_api::fetch_video_status(client, &video_id, &token).await {
            Ok(graph_api::VideoStatus::Published { permalink_url }) => {
                let conn = state.0.lock().unwrap_or_else(|p| p.into_inner());
                conn.execute(
                    "UPDATE fb_reel_posts SET
                         status = 'published',
                         fb_permalink = COALESCE(?1, fb_permalink),
                         published_at_ms = COALESCE(published_at_ms, ?2),
                         error_message = NULL
                     WHERE id = ?3 AND status = 'processing'",
                    params![permalink_url, chrono::Utc::now().timestamp_millis(), post_id],
                )?;
                drop(conn);
                let _ = app.emit(
                    "fb_upload_progress",
                    UploadProgress {
                        post_id,
                        status: "published".into(),
                        progress: 100,
                        bytes_uploaded: 0,
                        bytes_total: 0,
                    },
                );
            }
            Ok(graph_api::VideoStatus::Failed { reason }) => {
                let msg = format!("FB từ chối video: {reason}");
                let conn = state.0.lock().unwrap_or_else(|p| p.into_inner());
                conn.execute(
                    "UPDATE fb_reel_posts SET
                         status = 'failed',
                         error_message = ?1
                     WHERE id = ?2 AND status = 'processing'",
                    params![msg, post_id],
                )?;
                drop(conn);
                let _ = app.emit(
                    "fb_upload_progress",
                    UploadProgress {
                        post_id,
                        status: "failed".into(),
                        progress: 100,
                        bytes_uploaded: 0,
                        bytes_total: 0,
                    },
                );
            }
            Ok(graph_api::VideoStatus::Pending) => {
                // FB vẫn đang transcode — chờ tick sau.
            }
            Ok(graph_api::VideoStatus::TokenExpired) => {
                mark_token_expired(&state, &page_id)?;
            }
            Err(e) => {
                eprintln!("[fb_reels bg] check processing post={post_id}: {e}");
            }
        }
    }
    Ok(())
}

/// Posts `published` mà permalink null (lần fetch ngay sau publish bị FB
/// trả null vì transcoding) → re-fetch. Cap 30 post/tick để không spam API.
async fn refetch_missing_permalinks(
    app: &AppHandle,
    client: &reqwest::Client,
) -> Result<()> {
    let state = app.state::<FbReelsDbState>();

    let candidates: Vec<(i64, String, String, String)> = {
        let conn = state.0.lock().unwrap_or_else(|p| p.into_inner());
        let mut stmt = conn.prepare(
            "SELECT r.id, r.fb_video_id, p.access_token, p.page_id
             FROM fb_reel_posts r
             JOIN fb_pages p ON p.page_id = r.page_id
             WHERE r.status = 'published'
               AND r.fb_permalink IS NULL
               AND r.fb_video_id IS NOT NULL
               AND p.token_expired = 0
             ORDER BY r.published_at_ms DESC NULLS LAST
             LIMIT 30",
        )?;
        let rows: Vec<(i64, Option<String>, String, String)> = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<_, _>>()?;
        rows.into_iter()
            .filter_map(|(id, vid, tok, pg)| vid.map(|v| (id, v, tok, pg)))
            .collect()
    };

    for (post_id, video_id, token, page_id) in candidates {
        match graph_api::fetch_permalink(client, &video_id, &token).await {
            Ok(Some(url)) => {
                let conn = state.0.lock().unwrap_or_else(|p| p.into_inner());
                conn.execute(
                    "UPDATE fb_reel_posts SET fb_permalink = ?1 WHERE id = ?2",
                    params![url, post_id],
                )?;
            }
            Ok(None) => {
                // Vẫn chưa có — tick sau retry.
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("\"code\":190") || msg.contains("Invalid OAuth") {
                    mark_token_expired(&state, &page_id)?;
                }
            }
        }
    }
    Ok(())
}

/// Mark `token_expired=1` cho page → background polls + UI sẽ skip page này.
pub fn mark_token_expired(state: &FbReelsDbState, page_id: &str) -> Result<()> {
    let conn = state.0.lock().unwrap_or_else(|p| p.into_inner());
    conn.execute(
        "UPDATE fb_pages SET token_expired = 1 WHERE page_id = ?1",
        params![page_id],
    )?;
    Ok(())
}
