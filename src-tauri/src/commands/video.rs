//! Tải video từ nhiều nền tảng (TikTok, Douyin, Xiaohongshu, FB, IG, YouTube, X, Reddit...).
//! Port từ `analyzer_shopee/src-tauri/src/commands/tiktok.rs`.
//!
//! Flow:
//! 1. `get_video_info(url)` — parse URL, detect platform, fetch info (title/cover/duration/download_url)
//!    qua API cụ thể cho từng platform (TikWM, savetik, iesdouyin, XHS HTML scrape) hoặc
//!    fallback Cobalt API đa nền tảng.
//! 2. `download_video(download_url, save_path)` — HTTP GET với Referer phù hợp, ghi file.
//!
//! Không cần OAuth/API key — dùng API public + HTML scraping.

use std::sync::OnceLock;

use rusqlite::params;
use serde::{Deserialize, Serialize};
use tauri::State;
use tokio::sync::Semaphore;

use crate::db::VideoDbState;

use super::{CmdError, CmdResult};

const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";
const MOBILE_UA: &str = "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1";

fn http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .user_agent(UA)
        .build()
        .map_err(|e| format!("HTTP client error: {}", e))
}

/// Thông tin video từ nhiều nền tảng.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VideoInfo {
    pub title: String,
    pub author: String,
    pub cover: String,
    pub duration: i64,
    pub platform: String,
    pub download_url: String,
    pub filename: String,
}

// ===== Cobalt API (đa nền tảng) =====

// Cobalt trả `error` là object {"code": "..."}, không phải string
// → dùng Value để tránh parse fail
#[derive(Debug, Deserialize)]
struct CobaltResponse {
    status: Option<String>,
    url: Option<String>,
    filename: Option<String>,
    #[serde(default)]
    error: serde_json::Value,
    picker: Option<Vec<CobaltPicker>>,
}

#[derive(Debug, Deserialize)]
struct CobaltPicker {
    url: Option<String>,
}

// ===== TikWM API (TikTok fallback) =====

#[derive(Debug, Deserialize)]
struct TikwmResponse {
    code: i32,
    msg: Option<String>,
    data: Option<TikwmData>,
}

#[derive(Debug, Deserialize)]
struct TikwmData {
    title: Option<String>,
    duration: Option<i64>,
    cover: Option<String>,
    play: Option<String>,
    hdplay: Option<String>,
    author: Option<TikwmAuthor>,
}

#[derive(Debug, Deserialize)]
struct TikwmAuthor {
    nickname: Option<String>,
}

fn normalize_xhs_url(url: &str) -> String {
    let patterns = ["/discovery/item/", "/explore/"];
    for pat in patterns {
        if let Some(pos) = url.find(pat) {
            let after = &url[pos + pat.len()..];
            let note_id = after.split(&['?', '/', '#'][..]).next().unwrap_or("");
            if !note_id.is_empty() {
                return format!("https://www.xiaohongshu.com/explore/{}", note_id);
            }
        }
    }
    url.to_string()
}

fn detect_platform(url: &str) -> &str {
    let u = url.to_lowercase();
    if u.contains("douyin.com") { "Douyin" }
    else if u.contains("tiktok.com") { "TikTok" }
    else if u.contains("youtube.com") || u.contains("youtu.be") { "YouTube" }
    else if u.contains("facebook.com") || u.contains("fb.watch") { "Facebook" }
    else if u.contains("instagram.com") { "Instagram" }
    else if u.contains("twitter.com") || u.contains("x.com") { "Twitter/X" }
    else if u.contains("reddit.com") { "Reddit" }
    else if u.contains("vimeo.com") { "Vimeo" }
    else if u.contains("xiaohongshu.com") || u.contains("xhslink.com") { "Xiaohongshu" }
    // Shopee: domain quốc gia khác nhau (shopee.vn / shopee.com / shopee.com.my
    // / shopee.co.id ...) — check prefix "shopee." chung. Cũng bắt link short
    // shp.ee mà Shopee dùng để chia sẻ trong app.
    else if u.contains("shopee.") || u.contains("shp.ee") { "Shopee" }
    else { "Khác" }
}

static COBALT_SEM: OnceLock<Semaphore> = OnceLock::new();
fn cobalt_sem() -> &'static Semaphore {
    COBALT_SEM.get_or_init(|| Semaphore::new(2))
}

// ===== TikTok multi-API rotation =====

/// Round-robin counter — xen kẽ TikWM / tiklydown để phân tải.
static TIKTOK_API_IDX: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Rate limiter TikWM: free tier chỉ cho 1 req/sec.
static TIKWM_LAST: OnceLock<tokio::sync::Mutex<std::time::Instant>> = OnceLock::new();
fn tikwm_mutex() -> &'static tokio::sync::Mutex<std::time::Instant> {
    TIKWM_LAST.get_or_init(|| {
        tokio::sync::Mutex::new(
            std::time::Instant::now() - std::time::Duration::from_secs(2),
        )
    })
}
async fn tikwm_wait() {
    let mut last = tikwm_mutex().lock().await;
    let elapsed = last.elapsed();
    const GAP: std::time::Duration = std::time::Duration::from_millis(1100);
    if elapsed < GAP {
        tokio::time::sleep(GAP - elapsed).await;
    }
    *last = std::time::Instant::now();
}

async fn fetch_cobalt(url: &str) -> Result<VideoInfo, String> {
    // Tối đa 2 request Cobalt song song — tránh rate limit khi tải hàng loạt
    let _permit = cobalt_sem().acquire().await.ok();

    let client = http_client()?;

    let body = serde_json::json!({
        "url": url,
        "videoQuality": "max",
        "filenameStyle": "basic",
    });

    let resp = client
        .post("https://api.cobalt.tools")
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("Lỗi kết nối Cobalt: {}", e))?;

    if resp.status() == 429 {
        return Err("Cobalt API bị rate limit, thử lại sau vài giây.".to_string());
    }

    // Đọc text trước để có thể log nếu parse JSON thất bại
    let text = resp.text().await.map_err(|e| format!("Lỗi đọc Cobalt: {}", e))?;
    let data: CobaltResponse = serde_json::from_str(&text).map_err(|e| {
        let preview = &text[..text.len().min(200)];
        format!("Cobalt trả về không hợp lệ ({e}): {preview}")
    })?;

    let status = data.status.unwrap_or_default();

    if status == "error" {
        let code = match &data.error {
            serde_json::Value::Object(o) => o
                .get("code")
                .and_then(|v| v.as_str())
                .unwrap_or("error.unknown")
                .to_string(),
            serde_json::Value::String(s) => s.clone(),
            _ => "error.unknown".to_string(),
        };
        let msg = match code.as_str() {
            "error.api.auth.jwt.missing" | "error.api.auth.jwt.invalid" =>
                "Cobalt API yêu cầu xác thực (instance công khai đã bị khoá). Video không tải được qua Cobalt.".to_string(),
            "error.api.content.video.unavailable" =>
                "Video không tồn tại hoặc đã bị gỡ.".to_string(),
            "error.api.content.video.private" =>
                "Video riêng tư, không tải được.".to_string(),
            "error.api.content.video.age" =>
                "Video giới hạn độ tuổi, không tải được.".to_string(),
            "error.api.link.unsupported" =>
                "Link này chưa được Cobalt hỗ trợ.".to_string(),
            _ => code,
        };
        return Err(msg);
    }

    let download_url = if let Some(u) = data.url {
        u
    } else if let Some(picks) = data.picker {
        picks.into_iter()
            .find_map(|p| p.url)
            .ok_or("Không tìm thấy video trong picker")?
    } else {
        return Err("Cobalt không trả về link tải".to_string());
    };

    let platform = detect_platform(url);
    let filename = data.filename.unwrap_or_else(|| format!("{}_video.mp4", platform.to_lowercase()));

    Ok(VideoInfo {
        title: String::new(),
        author: String::new(),
        cover: String::new(),
        duration: 0,
        platform: platform.to_string(),
        download_url,
        filename,
    })
}

async fn fetch_tikwm(url: &str) -> Result<VideoInfo, String> {
    let api_url = format!("https://www.tikwm.com/api/?url={}&hd=1", urlencoding(url));
    let client = http_client()?;

    // snaptik.vn retry pattern: TikWM free tier 1 req/sec → retry ≤10 lần, delay 1.2s
    let mut body: TikwmResponse;
    let mut attempts = 0u32;
    loop {
        let resp = client.get(&api_url).send().await.map_err(|e| e.to_string())?;
        body = resp.json().await.map_err(|e| e.to_string())?;
        attempts += 1;
        let is_rate_limit = body.code == -1
            && body
                .msg
                .as_deref()
                .unwrap_or("")
                .to_lowercase()
                .contains("free api limit");
        if !is_rate_limit || attempts >= 10 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    }

    if body.code != 0 {
        let msg = body.msg.unwrap_or_else(|| format!("code {}", body.code));
        return Err(format!("TikWM: {}", msg));
    }

    let data = body.data.ok_or("Không có data")?;
    let download_url = data.hdplay.unwrap_or_default();
    let download_url = if download_url.is_empty() {
        data.play.unwrap_or_default()
    } else {
        download_url
    };

    Ok(VideoInfo {
        title: data.title.unwrap_or_default(),
        author: data.author.map(|a| a.nickname.unwrap_or_default()).unwrap_or_default(),
        cover: data.cover.unwrap_or_default(),
        duration: data.duration.unwrap_or(0),
        platform: "TikTok".to_string(),
        download_url,
        filename: String::new(),
    })
}

/// API phụ: tiklydown.eu.org — không cần auth, rate limit riêng.
async fn fetch_tiklydown(url: &str) -> Result<VideoInfo, String> {
    let api_url = format!(
        "https://api.tiklydown.eu.org/api/download?url={}",
        urlencoding(url)
    );
    let client = http_client()?;
    let resp = client
        .get(&api_url)
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|e| format!("tiklydown: {}", e))?;

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("tiklydown parse: {}", e))?;

    let download_url = body
        .pointer("/video/noWatermark")
        .or_else(|| body.pointer("/video/noWatermark2"))
        .or_else(|| body.pointer("/video/play"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    if download_url.is_empty() {
        return Err("tiklydown: không tìm thấy link tải".to_string());
    }

    Ok(VideoInfo {
        title: body
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        author: body
            .pointer("/author/name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        cover: body
            .pointer("/video/cover")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        duration: body
            .get("duration")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        platform: "TikTok".to_string(),
        download_url,
        filename: String::new(),
    })
}

/// Lấy title + cover từ TikTok oEmbed — dùng sau khi zcdn trả videoId.
async fn fetch_tiktok_oembed(video_id: &str) -> (String, String) {
    let oembed_url = format!(
        "https://www.tiktok.com/oembed?url={}",
        urlencoding(&format!("https://www.tiktok.com/video/{}", video_id))
    );
    let client = match http_client() {
        Ok(c) => c,
        Err(_) => return (String::new(), String::new()),
    };
    match client.get(&oembed_url).send().await {
        Ok(resp) => resp
            .json::<serde_json::Value>()
            .await
            .map(|d| {
                let title = d
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let cover = d
                    .get("thumbnail_url")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                (title, cover)
            })
            .unwrap_or_default(),
        Err(_) => (String::new(), String::new()),
    }
}

/// Fallback snaptik: gọi d.zcdn.top/api/tiktok không cần CF token.
/// snaptik.vn dùng endpoint này làm secondary — truyền cf_token: null khi không có captcha.
async fn fetch_zcdn(url: &str) -> Result<VideoInfo, String> {
    let client = http_client()?;
    let resp = client
        .post("https://d.zcdn.top/api/tiktok")
        .header("Content-Type", "application/json")
        .header("Referer", "https://snaptik.vn/")
        .header("Origin", "https://snaptik.vn")
        .json(&serde_json::json!({"url": url, "cf_token": null}))
        .send()
        .await
        .map_err(|e| format!("zcdn: {}", e))?;

    let data: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("zcdn parse: {}", e))?;

    // {"errors": {"field": "message"}}
    if let Some(errs) = data.get("errors").and_then(|v| v.as_object()) {
        let msg = errs
            .values()
            .next()
            .and_then(|v| v.as_str())
            .unwrap_or("zcdn error");
        return Err(format!("zcdn: {}", msg));
    }

    let video_id = data
        .get("videoId")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let links = data
        .get("links")
        .and_then(|v| v.as_array())
        .ok_or("zcdn: no links")?;

    if video_id.is_empty() {
        return Err("zcdn: no videoId".to_string());
    }

    // {"label": "No Watermark", "url": "..."}
    let find_link = |keyword: &str| -> String {
        links
            .iter()
            .find(|l| {
                l.get("label")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_lowercase().contains(keyword))
                    .unwrap_or(false)
            })
            .and_then(|l| l.get("url").and_then(|v| v.as_str()))
            .unwrap_or_default()
            .to_string()
    };

    let download_url = {
        let u = find_link("no watermark");
        if u.is_empty() { find_link("with watermark") } else { u }
    };

    if download_url.is_empty() {
        return Err("zcdn: không có link tải".to_string());
    }

    let (title, cover) = fetch_tiktok_oembed(video_id).await;

    Ok(VideoInfo {
        title,
        author: String::new(),
        cover,
        duration: 0,
        platform: "TikTok".to_string(),
        download_url,
        filename: String::new(),
    })
}

/// Tải TikTok với xen kẽ TikWM / tiklydown để tránh rate limit.
async fn fetch_tiktok(url: &str) -> Result<VideoInfo, String> {
    use std::sync::atomic::Ordering;
    let use_tikwm_first = TIKTOK_API_IDX.fetch_add(1, Ordering::Relaxed).is_multiple_of(2);

    // Thử API thứ nhất
    let first = if use_tikwm_first {
        tikwm_wait().await;
        fetch_tikwm(url).await
    } else {
        fetch_tiklydown(url).await
    };

    if let Ok(ref info) = first {
        if !info.download_url.is_empty() {
            return first;
        }
    }

    // Fallback sang API thứ hai
    let second = if use_tikwm_first {
        fetch_tiklydown(url).await
    } else {
        tikwm_wait().await;
        fetch_tikwm(url).await
    };

    if let Ok(ref info) = second {
        if !info.download_url.is_empty() {
            return second;
        }
    }

    // Tầng cuối: d.zcdn.top — backend của snaptik.vn, thử không cần CF token
    fetch_zcdn(url).await
}

fn xhs_manual_hint() -> &'static str {
    "Cách tải thủ công:\n\
     1. Mở link trong Chrome\n\
     2. Nhấn F12 → tab Network → lọc \"media\"\n\
     3. Phát video, tìm file .mp4 trong danh sách\n\
     4. Chuột phải → Open in new tab → Ctrl+S để lưu"
}

async fn fetch_xiaohongshu(url: &str) -> Result<VideoInfo, String> {
    let jar = std::sync::Arc::new(reqwest::cookie::Jar::default());
    let client = reqwest::Client::builder()
        .user_agent(UA)
        .cookie_provider(jar.clone())
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .map_err(|e| e.to_string())?;

    let _ = client
        .get("https://www.xiaohongshu.com/")
        .header("Accept", "text/html")
        .send()
        .await;

    let urls_to_try = vec![url.to_string(), normalize_xhs_url(url)];

    let mut html = String::new();
    let mut redirected_to_404 = false;
    for try_url in &urls_to_try {
        let resp = client
            .get(try_url)
            .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
            .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
            .header("Referer", "https://www.xiaohongshu.com/explore")
            .send()
            .await;

        if let Ok(r) = resp {
            let final_url = r.url().to_string();
            if final_url.contains("/404") || final_url.contains("errorCode=") {
                redirected_to_404 = true;
                continue;
            }
            if r.status().is_success() {
                if let Ok(text) = r.text().await {
                    if text.contains("originVideoKey") || text.contains("og:video") || text.contains("xhscdn.com") || text.contains("og:image") {
                        html = text;
                        break;
                    }
                    if html.is_empty() {
                        html = text;
                    }
                }
            }
        }
    }

    if redirected_to_404 && html.is_empty() {
        return Err("Link Xiaohongshu đã hết hạn hoặc bài viết bị gỡ. Mở link trong browser để kiểm tra, hoặc copy link mới (xsec_token thường chỉ valid trong vài giờ).".to_string());
    }

    if html.is_empty() {
        return Err("Không thể truy cập bài viết Xiaohongshu. Thử copy lại link mới.".to_string());
    }

    let mut title = String::new();
    let mut author = String::new();
    let mut cover = String::new();
    let mut video_url = String::new();

    let extract_meta = |html: &str, prop: &str| -> String {
        html.find(prop).and_then(|pos| {
            let chunk = &html[pos..std::cmp::min(pos + 500, html.len())];
            chunk.find("content=\"").and_then(|c| {
                let start = c + 9;
                chunk[start..].find('"').map(|end| chunk[start..start + end].to_string())
            })
        }).unwrap_or_default()
    };

    for marker in ["__INITIAL_SSR_STATE__=", "__INITIAL_STATE__="] {
        if video_url.is_empty() {
            if let Some(pos) = html.find(marker) {
                let after = &html[pos + marker.len()..];
                if let Some(obj_start) = after.find('{') {
                    let json_area = &after[obj_start..];
                    let mut depth = 0;
                    let mut end_pos = 0;
                    for (i, c) in json_area.char_indices() {
                        match c { '{' => depth += 1, '}' => { depth -= 1; if depth == 0 { end_pos = i + 1; break; } }, _ => {} }
                        if i > 500_000 { break; }
                    }
                    if end_pos > 0 {
                        let json_str = &json_area[..end_pos];
                        if let Ok(state) = serde_json::from_str::<serde_json::Value>(json_str) {
                            if let Some(map) = state.pointer("/note/noteDetailMap").and_then(|v| v.as_object()) {
                                for (_, detail) in map {
                                    if video_url.is_empty() {
                                        for path in ["/note/video/consumer/originVideoKey", "/note/video/consumer/videoKey"] {
                                            if let Some(key) = detail.pointer(path).and_then(|v| v.as_str()) {
                                                if !key.is_empty() {
                                                    video_url = format!("https://sns-video-bd.xhscdn.com/{}", key);
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    if video_url.is_empty() {
                                        for codec in ["h264", "h265", "av1"] {
                                            let stream_path = format!("/note/video/media/stream/{}", codec);
                                            if let Some(arr) = detail.pointer(&stream_path).and_then(|v| v.as_array()) {
                                                for item in arr {
                                                    if let Some(u) = item.get("masterUrl").and_then(|v| v.as_str()) {
                                                        if !u.is_empty() { video_url = u.to_string(); break; }
                                                    }
                                                    if let Some(u) = item.get("backupUrls").and_then(|v| v.as_array()).and_then(|a| a.first()).and_then(|v| v.as_str()) {
                                                        if !u.is_empty() { video_url = u.to_string(); break; }
                                                    }
                                                }
                                            }
                                            if !video_url.is_empty() { break; }
                                        }
                                    }
                                    if title.is_empty() {
                                        if let Some(t) = detail.pointer("/note/title").and_then(|v| v.as_str()) { title = t.to_string(); }
                                    }
                                    if title.is_empty() {
                                        if let Some(d) = detail.pointer("/note/desc").and_then(|v| v.as_str()) { title = d.chars().take(100).collect(); }
                                    }
                                    if author.is_empty() {
                                        if let Some(n) = detail.pointer("/note/user/nickname").and_then(|v| v.as_str()) { author = n.to_string(); }
                                    }
                                    if cover.is_empty() {
                                        if let Some(img) = detail.pointer("/note/imageList/0/urlDefault").and_then(|v| v.as_str()) { cover = img.to_string(); }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if video_url.is_empty() {
        let og_video = extract_meta(&html, "\"og:video\"");
        if !og_video.is_empty() && og_video.contains("xhscdn.com") {
            video_url = og_video;
        }
    }
    if title.is_empty() { title = extract_meta(&html, "og:title"); }
    if title.is_empty() {
        if let Some(pos) = html.find("<title>") {
            let start = pos + 7;
            if let Some(end) = html[start..].find("</title>") { title = html[start..start + end].to_string(); }
        }
    }
    if cover.is_empty() { cover = extract_meta(&html, "og:image"); }
    if author.is_empty() { author = extract_meta(&html, "xhs:note:author"); }

    let clean_url = |u: String| -> String {
        u.replace("\\u002F", "/").replace("\\u003A", ":").replace("\\/", "/").replace("\\u0026", "&")
    };
    video_url = clean_url(video_url);
    cover = clean_url(cover);

    if !video_url.is_empty() && !video_url.starts_with("http") {
        video_url = format!("https://{}", video_url.trim_start_matches('/'));
    }
    if !cover.is_empty() && !cover.starts_with("http") {
        cover = format!("https://{}", cover.trim_start_matches('/'));
    }

    if video_url.is_empty() {
        return Err("Không tìm thấy video Xiaohongshu. Có thể bài viết chỉ có ảnh, hoặc link đã hết hạn. Thử copy link mới.".to_string());
    }

    let ext = if video_url.contains("video") { "mp4" } else { "jpg" };

    Ok(VideoInfo {
        title,
        author,
        cover,
        duration: 0,
        platform: "Xiaohongshu".to_string(),
        download_url: video_url,
        filename: format!("xiaohongshu_{}.{}", chrono::Utc::now().timestamp(), ext),
    })
}

// ===== Douyin =====

fn extract_douyin_id(url: &str) -> Option<String> {
    if let Some(pos) = url.find("/video/") {
        let after = &url[pos + 7..];
        let id: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        if id.len() >= 15 { return Some(id); }
    }
    if let Some(pos) = url.find("modal_id=") {
        let after = &url[pos + 9..];
        let id: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        if id.len() >= 15 { return Some(id); }
    }
    None
}

async fn fetch_douyin(url: &str) -> Result<VideoInfo, String> {
    let client = reqwest::Client::builder()
        .user_agent(UA)
        .redirect(reqwest::redirect::Policy::limited(10))
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| e.to_string())?;

    let full_url = if url.contains("v.douyin.com") {
        let resp = client.get(url).send().await.map_err(|e| format!("Lỗi resolve URL: {}", e))?;
        resp.url().to_string()
    } else {
        url.to_string()
    };

    let aweme_id = extract_douyin_id(&full_url)
        .ok_or("Không tìm thấy video ID trong URL Douyin")?;

    let savetik_url = format!("https://www.douyin.com/video/{}", aweme_id);
    if let Ok(info) = fetch_douyin_savetik(&savetik_url).await {
        if !info.download_url.is_empty() {
            return Ok(info);
        }
    }

    fetch_douyin_mobile(&aweme_id).await
}

async fn fetch_douyin_savetik(url: &str) -> Result<VideoInfo, String> {
    let client = http_client()?;
    let resp = client
        .post("https://savetik.io/api/ajaxSearch")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Referer", "https://savetik.io/vi/douyin-video-downloader")
        .body(format!("q={}&lang=vi", urlencoding(url)))
        .send()
        .await
        .map_err(|e| format!("savetik error: {}", e))?;

    let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
    let html = body.get("data").and_then(|v| v.as_str()).unwrap_or("");
    if html.is_empty() { return Err("savetik: no data".to_string()); }

    let title = html.find("<h3>").and_then(|s| {
        let start = s + 4;
        html[start..].find("</h3>").map(|end| html[start..start + end].to_string())
    }).unwrap_or_default();

    let cover = html.find("<img src=\"").and_then(|s| {
        let start = s + 10;
        html[start..].find('"').map(|end| html[start..start + end].replace("&amp;", "&"))
    }).unwrap_or_default();

    let mut download_urls: Vec<String> = Vec::new();
    let mut pos = 0;
    while let Some(p) = html[pos..].find("href=\"") {
        let start = pos + p + 6;
        if let Some(end) = html[start..].find('"') {
            let href = html[start..start + end].replace("&amp;", "&");
            if (href.contains("snapcdn.app") || href.contains("zjcdn.com") || href.contains("douyinvod.com"))
                && !download_urls.contains(&href) {
                download_urls.push(href);
            }
        }
        pos = start + 1;
    }

    if download_urls.is_empty() { return Err("savetik: no download links".to_string()); }

    Ok(VideoInfo {
        title,
        author: String::new(),
        cover,
        duration: 0,
        platform: "Douyin".to_string(),
        download_url: download_urls.first().unwrap().clone(),
        filename: String::new(),
    })
}

async fn fetch_douyin_mobile(aweme_id: &str) -> Result<VideoInfo, String> {
    let client = reqwest::Client::builder()
        .user_agent(MOBILE_UA)
        .redirect(reqwest::redirect::Policy::limited(10))
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| e.to_string())?;

    let url = format!("https://www.iesdouyin.com/share/video/{}", aweme_id);
    let resp = client.get(&url).send().await
        .map_err(|e| format!("Lỗi kết nối Douyin: {}", e))?;

    let html = resp.text().await.map_err(|e| e.to_string())?;

    let json_str = if let Some(pos) = html.find("RENDER_DATA") {
        html[pos..].find('>').and_then(|s| {
            let start = pos + s + 1;
            html[start..].find('<').map(|end| {
                let encoded = &html[start..start + end];
                urlencoding_decode(encoded)
            })
        })
    } else if let Some(pos) = html.find("_ROUTER_DATA") {
        html[pos..].find('>').and_then(|s| {
            let start = pos + s + 1;
            html[start..].find('<').map(|end| {
                let encoded = &html[start..start + end];
                urlencoding_decode(encoded)
            })
        })
    } else {
        None
    };

    let mut video_url = String::new();
    let mut title = String::new();
    let mut author = String::new();
    let mut cover = String::new();

    if let Some(json) = json_str {
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(&json) {
            find_in_json(&data, &mut video_url, &mut title, &mut author, &mut cover, 0);
        }
    }

    if video_url.is_empty() {
        if let Some(pos) = html.find("play_addr") {
            if let Some(url_start) = html[pos..].find("url_list") {
                let after = &html[pos + url_start..];
                if let Some(s) = after.find("http") {
                    let url_part: String = after[s..].chars()
                        .take_while(|c| *c != '"' && *c != '\\' && *c != '\'')
                        .collect();
                    video_url = url_part.replace("\\u002F", "/").replace("\\u0026", "&");
                }
            }
        }
    }

    if video_url.is_empty() {
        return Err("Không tìm thấy video Douyin. Video có thể bị giới hạn quyền xem.".to_string());
    }

    video_url = video_url.replace("\\u002F", "/").replace("\\u0026", "&").replace("\\/", "/");

    Ok(VideoInfo {
        title,
        author,
        cover,
        duration: 0,
        platform: "Douyin".to_string(),
        download_url: video_url,
        filename: format!("douyin_{}.mp4", aweme_id),
    })
}

fn find_in_json(val: &serde_json::Value, video_url: &mut String, title: &mut String, author: &mut String, cover: &mut String, depth: usize) {
    if depth > 15 { return; }
    match val {
        serde_json::Value::Object(map) => {
            if video_url.is_empty() {
                if let Some(pa) = map.get("play_addr").or(map.get("playApi")) {
                    if let Some(urls) = pa.pointer("/url_list/0").and_then(|v| v.as_str()) {
                        *video_url = urls.to_string();
                    } else if let Some(u) = pa.as_str() {
                        if u.contains("douyinvod") || u.contains("snssdk") {
                            *video_url = u.to_string();
                        }
                    }
                }
            }
            if title.is_empty() {
                if let Some(d) = map.get("desc").and_then(|v| v.as_str()) {
                    if !d.is_empty() { *title = d.to_string(); }
                }
            }
            if author.is_empty() {
                if let Some(a) = map.get("author") {
                    if let Some(n) = a.get("nickname").or(a.get("nick_name")).and_then(|v| v.as_str()) {
                        *author = n.to_string();
                    }
                }
            }
            if cover.is_empty() {
                if let Some(c) = map.get("cover").or(map.get("origin_cover")) {
                    if let Some(u) = c.pointer("/url_list/0").and_then(|v| v.as_str()) {
                        *cover = u.to_string();
                    }
                }
            }
            let all_found = !video_url.is_empty() && !title.is_empty() && !author.is_empty() && !cover.is_empty();
            if !all_found {
                for v in map.values() {
                    find_in_json(v, video_url, title, author, cover, depth + 1);
                    if !video_url.is_empty() && !title.is_empty() && !author.is_empty() && !cover.is_empty() {
                        break;
                    }
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                if !video_url.is_empty() { break; }
                find_in_json(v, video_url, title, author, cover, depth + 1);
            }
        }
        _ => {}
    }
}

fn urlencoding_decode(s: &str) -> String {
    let src = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(src.len());
    let mut i = 0;
    while i < src.len() {
        if src[i] == b'%' && i + 2 < src.len() {
            if let Ok(byte) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(src[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn extract_url(input: &str) -> String {
    for word in input.split_whitespace() {
        let w = word.trim_matches(|c: char| !c.is_alphanumeric() && c != ':' && c != '/' && c != '.' && c != '?' && c != '&' && c != '=' && c != '-' && c != '_' && c != '%' && c != '#' && c != '@');
        if w.starts_with("http://") || w.starts_with("https://") {
            return w.to_string();
        }
    }
    input.trim().to_string()
}

/// Resolve short URL bằng cách follow redirect — dùng cho vt.tiktok.com / vm.tiktok.com.
async fn resolve_short_url(url: &str) -> String {
    let client = match reqwest::Client::builder()
        .user_agent(MOBILE_UA)
        .redirect(reqwest::redirect::Policy::limited(5))
        .timeout(std::time::Duration::from_secs(8))
        .build()
    {
        Ok(c) => c,
        Err(_) => return url.to_string(),
    };
    match client.get(url).send().await {
        Ok(resp) => resp.url().to_string(),
        Err(_) => url.to_string(),
    }
}

/// Thử lại async fn tối đa `max` lần với delay `delay_ms` ms giữa các lần.
/// Dừng sớm nếu gặp lỗi permanent (video riêng tư, JWT auth, v.v.).
async fn retry<F, Fut, T>(max: u32, delay_ms: u64, f: F) -> Result<T, String>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let mut last_err = String::new();
    for i in 0..max {
        if i > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if is_permanent_error(&e) {
                    return Err(e);
                }
                last_err = e;
            }
        }
    }
    Err(last_err)
}

// ===== Shopee video — API qua 4anm.top =====
//
// 4anm.top/downloadshopee là front-end web cho phép paste link sản phẩm Shopee
// (vd https://shopee.vn/product/...) hoặc link video, trả về URL trực tiếp
// của file mp4. Endpoint nội bộ:
//
//   POST https://4anm.top/get_download_shopee.php
//   Content-Type: application/json
//   Body: { "urls": ["..."], "token": "<base64 từ <input name=token>>" }
//   Response: { "download_link": [...] } hoặc { "error": "...", "message": "..." }
//
// Token là 1 base64 string ẩn trong HTML — không phải CSRF per-session mà có vẻ
// là access token tĩnh per-instance, nhưng vẫn cache có TTL 1h + auto-refetch
// khi API trả lỗi auth để app không kẹt khi 4anm.top rotate token.

const SHOPEE_DL_PAGE: &str = "https://4anm.top/downloadshopee";
const SHOPEE_DL_API: &str = "https://4anm.top/get_download_shopee.php";
const SHOPEE_TOKEN_TTL: std::time::Duration = std::time::Duration::from_secs(3600);

#[derive(Debug, Deserialize)]
struct ShopeeApiResponse {
    #[serde(default)]
    download_link: Vec<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Serialize)]
struct ShopeeApiRequest<'a> {
    urls: Vec<&'a str>,
    token: &'a str,
}

static SHOPEE_TOKEN_CACHE: OnceLock<
    tokio::sync::Mutex<Option<(String, std::time::Instant)>>,
> = OnceLock::new();

fn shopee_token_cache() -> &'static tokio::sync::Mutex<Option<(String, std::time::Instant)>> {
    SHOPEE_TOKEN_CACHE.get_or_init(|| tokio::sync::Mutex::new(None))
}

/// Scrape token từ `<input type="hidden" name="token" value="...">`. Trang trả
/// HTML đầy đủ → string-search nhanh hơn HTML parser cho 1 trường duy nhất.
async fn fetch_shopee_token(client: &reqwest::Client) -> Result<String, String> {
    let html = client
        .get(SHOPEE_DL_PAGE)
        .send()
        .await
        .map_err(|e| format!("Lỗi tải trang 4anm.top: {}", e))?
        .text()
        .await
        .map_err(|e| format!("Lỗi đọc HTML 4anm.top: {}", e))?;

    let token = html
        .split("name=\"token\"")
        .nth(1)
        .and_then(|s| s.split("value=\"").nth(1))
        .and_then(|s| s.split('"').next())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "Không tìm thấy token Shopee trong HTML 4anm.top".to_string())?;

    Ok(token.to_string())
}

/// Trả token cached (fresh < 1h) hoặc refetch. `force_refresh=true` bỏ qua
/// cache — dùng khi API trả lỗi token để retry với token mới.
async fn get_shopee_token(client: &reqwest::Client, force_refresh: bool) -> Result<String, String> {
    let cache = shopee_token_cache();
    if !force_refresh {
        let guard = cache.lock().await;
        if let Some((token, at)) = guard.as_ref() {
            if at.elapsed() < SHOPEE_TOKEN_TTL {
                return Ok(token.clone());
            }
        }
    }
    let token = fetch_shopee_token(client).await?;
    *cache.lock().await = Some((token.clone(), std::time::Instant::now()));
    Ok(token)
}

/// Derive filename từ URL CDN (lấy basename + extension nếu có).
/// Fallback: `shopee_video_{timestamp}.mp4`.
fn shopee_filename_from(download_url: &str) -> String {
    let basename = download_url
        .rsplit('/')
        .next()
        .and_then(|last| last.split('?').next())
        .unwrap_or("");
    if !basename.is_empty() && basename.contains('.') {
        return basename.to_string();
    }
    format!("shopee_video_{}.mp4", chrono::Utc::now().timestamp())
}

/// 1 request POST tới 4anm.top — tách thành helper để retry khi token sai.
async fn call_shopee_api(
    client: &reqwest::Client,
    url: &str,
    token: &str,
) -> Result<ShopeeApiResponse, String> {
    let payload = ShopeeApiRequest {
        urls: vec![url],
        token,
    };
    let resp = client
        .post(SHOPEE_DL_API)
        .header("Referer", SHOPEE_DL_PAGE)
        .header("Origin", "https://4anm.top")
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("Lỗi gọi API 4anm.top: {}", e))?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("Lỗi đọc response 4anm.top: {}", e))?;

    if !status.is_success() {
        return Err(format!("4anm.top HTTP {}: {}", status, text));
    }

    serde_json::from_str::<ShopeeApiResponse>(&text)
        .map_err(|e| format!("Lỗi parse JSON 4anm.top: {} (body: {})", e, text))
}

async fn fetch_shopee(url: &str) -> Result<VideoInfo, String> {
    let client = http_client()?;

    // Lần 1: dùng token cached/fetch lần đầu.
    let token = get_shopee_token(&client, false).await?;
    let mut data = call_shopee_api(&client, url, &token).await?;

    // Nếu API trả error liên quan token, refetch token + retry 1 lần. Các từ
    // khoá bao quát các cách 4anm.top có thể báo lỗi auth.
    let looks_like_token_error = |d: &ShopeeApiResponse| -> bool {
        d.download_link.is_empty()
            && d.error
                .as_deref()
                .or(d.message.as_deref())
                .map(|s| {
                    let s = s.to_lowercase();
                    s.contains("token") || s.contains("auth") || s.contains("unauthorized")
                })
                .unwrap_or(false)
    };

    if looks_like_token_error(&data) {
        let fresh_token = get_shopee_token(&client, true).await?;
        data = call_shopee_api(&client, url, &fresh_token).await?;
    }

    if data.download_link.is_empty() {
        let msg = data
            .message
            .or(data.error)
            .unwrap_or_else(|| "API 4anm.top không trả URL nào".to_string());
        return Err(format!("Shopee: {}", msg));
    }

    let download_url = data.download_link.into_iter().next().unwrap();
    let filename = shopee_filename_from(&download_url);

    Ok(VideoInfo {
        title: "Video Shopee".to_string(),
        author: "Shopee".to_string(),
        cover: String::new(),
        duration: 0,
        platform: "Shopee".to_string(),
        download_url,
        filename,
    })
}

/// Lỗi permanent — không nên retry: video riêng tư, auth, link hết hạn, v.v.
fn is_permanent_error(e: &str) -> bool {
    let e = e.to_lowercase();
    e.contains("jwt")
        || e.contains("auth")
        || e.contains("riêng tư")
        || e.contains("private")
        || e.contains("không tồn tại")
        || e.contains("bị gỡ")
        || e.contains("hết hạn")
        || e.contains("unavailable")
        || e.contains("độ tuổi")
        || e.contains("age restrict")
        || e.contains("unsupported")
}

/// Lấy info video — support TikTok, Douyin, Xiaohongshu, YouTube, FB, IG, X, Reddit, v.v.
#[tauri::command]
pub async fn get_video_info(url: String) -> Result<VideoInfo, String> {
    let url = extract_url(&url);

    // vt.tiktok.com / vm.tiktok.com là short link — resolve về full URL trước
    // để TikWM xử lý được (cần dạng /video/{id})
    let url = if url.contains("vt.tiktok.com") || url.contains("vm.tiktok.com") {
        resolve_short_url(&url).await
    } else {
        url
    };

    let platform = detect_platform(&url);

    match platform {
        // TikTok: fetch_tiktok đã có retry nội bộ (TikWM ×10 + tiklydown + zcdn)
        "TikTok" => fetch_tiktok(&url).await,

        // Douyin: 3 lần retry, delay 2s
        "Douyin" => retry(3, 2_000, || fetch_douyin(&url)).await,

        // Shopee: 2 lần retry, delay 3s (API 4anm.top có rate limit nhẹ)
        "Shopee" => retry(2, 3_000, || fetch_shopee(&url)).await,

        // Xiaohongshu: 2 lần retry, delay 3s (token xhslink hết hạn nhanh nên ít retry)
        "Xiaohongshu" => {
            let result = retry(2, 3_000, || fetch_xiaohongshu(&url)).await;
            match result {
                Ok(info) if !info.download_url.is_empty() => Ok(info),
                Ok(_) => Err(format!("Không lấy được URL video.\n\n{}", xhs_manual_hint())),
                Err(e) => Err(format!("{}\n\n{}", e, xhs_manual_hint())),
            }
        }

        // YouTube / Facebook / Instagram / Twitter / Reddit / Vimeo / Khác: Cobalt, 3 lần retry, delay 2s
        _ => retry(3, 2_000, || fetch_cobalt(&url)).await,
    }
}

/// Payload event `download-progress`. `download_id` dùng để frontend route đúng item khi tải hàng loạt.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProgress {
    pub download_id: String,
    pub downloaded: u64,
    /// 0 nếu server không trả `Content-Length` → UI fallback indeterminate.
    pub total: u64,
}

/// Tải video về máy — stream bytes + emit `download-progress` event mỗi chunk.
/// `download_id` là ID do frontend tạo (dùng cho batch), forward vào payload để UI route đúng item.
/// Retry tối đa 3 lần khi gặp lỗi mạng/stream tạm thời.
#[tauri::command]
pub async fn download_video(
    app: tauri::AppHandle,
    download_url: String,
    save_path: String,
    download_id: String,
) -> Result<String, String> {
    use futures_util::StreamExt;
    use tauri::Emitter;
    use tokio::io::AsyncWriteExt;

    let client = http_client()?;

    let referer: &str = if download_url.contains("xhscdn.com") {
        "https://www.xiaohongshu.com/"
    } else if download_url.contains("douyinvod.com") || download_url.contains("douyin") {
        "https://www.douyin.com/"
    } else if download_url.contains("tiktok") || download_url.contains("tikwm") {
        "https://www.tiktok.com/"
    } else if download_url.contains("shopee") || download_url.contains("cf.shopee") {
        // Shopee CDN không bắt buộc referer cho hầu hết video, nhưng set sẵn
        // phòng trường hợp họ siết kiểm tra. Dùng shopee.vn vì link product
        // VN phổ biến nhất trong tệp user.
        "https://shopee.vn/"
    } else {
        ""
    };

    const MAX_RETRIES: u32 = 3;
    let mut last_err = String::new();

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(2_000)).await;
            // Reset progress về 0 để frontend biết đang retry
            let _ = app.emit("download-progress", DownloadProgress {
                download_id: download_id.clone(),
                downloaded: 0,
                total: 0,
            });
        }

        let mut req = client.get(&download_url);
        if !referer.is_empty() {
            req = req.header("Referer", referer);
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                last_err = format!("Lỗi kết nối (lần {}): {}", attempt + 1, e);
                continue;
            }
        };

        // 4xx = lỗi permanent (link hết hạn, 403...), không retry
        if resp.status().is_client_error() {
            return Err(format!("Server từ chối: {}", resp.status()));
        }
        if !resp.status().is_success() {
            last_err = format!("Server lỗi {} (lần {})", resp.status(), attempt + 1);
            continue;
        }

        let total = resp.content_length().unwrap_or(0);
        let mut file = match tokio::fs::File::create(&save_path).await {
            Ok(f) => f,
            Err(e) => return Err(format!("Lỗi tạo file: {}", e)),
        };

        let mut stream = resp.bytes_stream();
        let mut downloaded: u64 = 0;
        let mut last_emit: u64 = 0;
        const EMIT_STEP: u64 = 64 * 1024;

        let stream_result: Result<(), String> = async {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| format!("Lỗi đọc stream: {}", e))?;
                file.write_all(&chunk)
                    .await
                    .map_err(|e| format!("Lỗi ghi file: {}", e))?;
                downloaded += chunk.len() as u64;

                if downloaded - last_emit >= EMIT_STEP {
                    let _ = app.emit("download-progress", DownloadProgress {
                        download_id: download_id.clone(),
                        downloaded,
                        total,
                    });
                    last_emit = downloaded;
                }
            }
            file.flush().await.map_err(|e| format!("Lỗi flush file: {}", e))?;
            Ok(())
        }
        .await;

        match stream_result {
            Ok(()) => {
                let _ = app.emit("download-progress", DownloadProgress {
                    download_id,
                    downloaded,
                    total: if total > 0 { total } else { downloaded },
                });
                return Ok(save_path);
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&save_path).await;
                last_err = format!("{} (lần {})", e, attempt + 1);
            }
        }
    }

    Err(last_err)
}

fn urlencoding(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for byte in s.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            _ => out.push_str(&format!("%{:02X}", byte)),
        }
    }
    out
}

// ============================================================
// Video download logging (admin-only audit)
// ============================================================

#[derive(Debug, Clone, Serialize)]
pub struct VideoDownloadLog {
    pub id: i64,
    pub url: String,
    pub downloaded_at_ms: i64,
    pub status: String,
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Log 1 lần download video — ghi local DB. UPSERT theo URL.
#[tauri::command]
pub async fn log_video_download(
    video_db: State<'_, VideoDbState>,
    url: String,
    status: String,
) -> CmdResult<()> {
    if status != "success" && status != "failed" {
        return Err(CmdError::msg(format!(
            "Invalid status: {status} (must be success/failed)"
        )));
    }
    let clean_url = extract_url(&url);
    let conn = video_db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "INSERT INTO video_downloads (url, downloaded_at_ms, status)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(url) DO UPDATE SET
             downloaded_at_ms = excluded.downloaded_at_ms,
             status = excluded.status",
        params![&clean_url, now_ms(), &status],
    )?;
    Ok(())
}

/// Query local video_downloads từ `video_logs.db`.
#[tauri::command]
pub async fn list_video_downloads(
    video_db: State<'_, VideoDbState>,
    limit: i64,
    offset: i64,
) -> CmdResult<Vec<VideoDownloadLog>> {
    let conn = video_db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare(
        "SELECT id, url, downloaded_at_ms, status
         FROM video_downloads
         ORDER BY downloaded_at_ms DESC
         LIMIT ?1 OFFSET ?2",
    )?;
    let rows = stmt.query_map(params![limit, offset], |r| {
        Ok(VideoDownloadLog {
            id: r.get(0)?,
            url: r.get(1)?,
            downloaded_at_ms: r.get(2)?,
            status: r.get(3)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(CmdError::from)
}
