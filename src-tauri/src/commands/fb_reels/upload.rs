//! Stream upload binary lên FB upload_url với progress events.
//!
//! Đọc file theo chunk 64KB, tổng hợp bytes_uploaded vào AtomicU64,
//! emit `fb_upload_progress` mỗi ~512KB để UI render progress bar
//! mà không spam re-render.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use futures_util::stream;
use reqwest::Client;
use tauri::{AppHandle, Emitter};
use tokio::io::AsyncReadExt;

use super::types::UploadProgress;

const CHUNK_SIZE: usize = 64 * 1024;
const EMIT_EVERY_BYTES: u64 = 512 * 1024;

/// Map extension → MIME type. FB Reels transcoder cần biết chính xác kind file
/// đang upload, nếu chỉ gửi `application/octet-stream` thì pipeline xử lý chậm
/// hoặc reject silently. Default `video/mp4` cho extension không biết — đa số
/// container Reels là mp4 nên fallback hợp lý.
fn mime_for_path(file_name: &str) -> &'static str {
    let ext = file_name
        .rsplit('.')
        .next()
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "mp4" | "m4v" => "video/mp4",
        "mov" => "video/quicktime",
        "webm" => "video/webm",
        _ => "video/mp4",
    }
}

/// Stream toàn bộ file lên `upload_url` với header FB Reels yêu cầu.
/// Emit `fb_upload_progress` events trong khi upload.
///
/// `file_name` truyền vào `X-Entity-Name` header — FB không dùng tên này để
/// store nhưng dùng cho debug log + content-disposition. Truyền tên thật của
/// file local thay vì hardcode "video.mp4" cho debugging dễ hơn.
#[allow(clippy::too_many_arguments)]
pub async fn stream_upload_file(
    client: &Client,
    upload_url: &str,
    page_token: &str,
    file_path: &str,
    file_name: &str,
    file_size: u64,
    app: AppHandle,
    post_id: i64,
) -> Result<()> {
    let file = tokio::fs::File::open(file_path)
        .await
        .with_context(|| format!("không mở được file {file_path}"))?;
    let uploaded = Arc::new(AtomicU64::new(0));
    let last_emit = Arc::new(AtomicU64::new(0));

    let stream = stream::unfold(
        (file, uploaded, last_emit, app, post_id, file_size),
        move |(mut file, uploaded, last_emit, app, post_id, total)| async move {
            let mut buf = vec![0u8; CHUNK_SIZE];
            match file.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => {
                    buf.truncate(n);
                    let now = uploaded.fetch_add(n as u64, Ordering::SeqCst) + n as u64;
                    let prev_emit = last_emit.load(Ordering::SeqCst);
                    let should_emit =
                        now == total || now.saturating_sub(prev_emit) >= EMIT_EVERY_BYTES;
                    if should_emit {
                        last_emit.store(now, Ordering::SeqCst);
                        let progress = (now * 100)
                            .checked_div(total)
                            .unwrap_or(0)
                            .min(100) as i64;
                        let _ = app.emit(
                            "fb_upload_progress",
                            UploadProgress {
                                post_id,
                                status: "uploading".to_string(),
                                progress,
                                bytes_uploaded: now,
                                bytes_total: total,
                            },
                        );
                    }
                    Some((
                        Ok::<Vec<u8>, std::io::Error>(buf),
                        (file, uploaded, last_emit, app, post_id, total),
                    ))
                }
                Err(e) => Some((Err(e), (file, uploaded, last_emit, app, post_id, total))),
            }
        },
    );

    // FB Reels upload yêu cầu RÕ RÀNG `Content-Length` + `X-Entity-Length`
    // (resumable upload protocol). reqwest streaming body mặc định dùng
    // `Transfer-Encoding: chunked` → FB reject với error
    // "Invalid Header format: expected either both Content-Length and
    //  X-Entity-Length, or Transfer-Encoding alone".
    //
    // Cách fix: set explicit Content-Length cho stream (reqwest sẽ bỏ
    // chunked encoding khi có Content-Length) + thêm X-Entity-Length cho
    // FB protocol.
    //
    // X-Entity-Type/Content-Type dùng MIME thực sự của video (video/mp4,
    // video/quicktime) thay vì application/octet-stream — FB cần kind file
    // đúng để route đúng transcoder, octet-stream làm pipeline xử lý chậm
    // hoặc DRAFT silently.
    let mime = mime_for_path(file_name);
    let body = reqwest::Body::wrap_stream(stream);
    let resp = client
        .post(upload_url)
        .header("Authorization", format!("OAuth {page_token}"))
        .header("offset", "0")
        .header("file_size", file_size.to_string())
        .header("X-Entity-Length", file_size.to_string())
        .header("X-Entity-Name", file_name)
        .header("X-Entity-Type", mime)
        .header("Content-Type", mime)
        .header(reqwest::header::CONTENT_LENGTH, file_size)
        .body(body)
        .send()
        .await
        .context("upload binary thất bại")?;

    let status = resp.status();
    let txt = resp.text().await.unwrap_or_default();
    eprintln!(
        "[fb_reels] upload binary post={post_id} size={file_size} mime={mime} status={status} body={txt}"
    );
    if !status.is_success() {
        anyhow::bail!("FB upload trả lỗi {}: {}", status, txt);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mime_detection() {
        assert_eq!(mime_for_path("clip.mp4"), "video/mp4");
        assert_eq!(mime_for_path("CLIP.MP4"), "video/mp4");
        assert_eq!(mime_for_path("clip.mov"), "video/quicktime");
        assert_eq!(mime_for_path("clip.MOV"), "video/quicktime");
        assert_eq!(mime_for_path("clip.webm"), "video/webm");
        assert_eq!(mime_for_path("noext"), "video/mp4");
        assert_eq!(mime_for_path("weird.xyz"), "video/mp4");
    }
}
