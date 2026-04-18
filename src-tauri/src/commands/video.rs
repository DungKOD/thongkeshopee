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

use serde::{Deserialize, Serialize};

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

#[derive(Debug, Deserialize)]
struct CobaltResponse {
    status: Option<String>,
    url: Option<String>,
    filename: Option<String>,
    error: Option<String>,
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
    else { "Khác" }
}

async fn fetch_cobalt(url: &str) -> Result<VideoInfo, String> {
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

    let data: CobaltResponse = resp
        .json()
        .await
        .map_err(|e| format!("Lỗi parse Cobalt: {}", e))?;

    let status = data.status.unwrap_or_default();

    if status == "error" {
        return Err(data.error.unwrap_or("Cobalt API lỗi".to_string()));
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
    let resp = client.get(&api_url).send().await.map_err(|e| e.to_string())?;
    let body: TikwmResponse = resp.json().await.map_err(|e| e.to_string())?;

    if body.code != 0 {
        return Err("TikWM API lỗi".to_string());
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
        download_url: download_urls.last().unwrap().clone(),
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
            for v in map.values() {
                if !video_url.is_empty() { break; }
                find_in_json(v, video_url, title, author, cover, depth + 1);
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
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                result.push(byte as char);
            }
        } else {
            result.push(c);
        }
    }
    result
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

/// Lấy info video — support TikTok, Douyin, Xiaohongshu, YouTube, FB, IG, X, Reddit, v.v.
#[tauri::command]
pub async fn get_video_info(url: String) -> Result<VideoInfo, String> {
    let url = extract_url(&url);
    let platform = detect_platform(&url);

    match platform {
        "Douyin" => fetch_douyin(&url).await,
        "TikTok" => match fetch_tikwm(&url).await {
            Ok(info) if !info.download_url.is_empty() => Ok(info),
            _ => fetch_cobalt(&url).await,
        },
        "Xiaohongshu" => match fetch_xiaohongshu(&url).await {
            Ok(info) if !info.download_url.is_empty() => Ok(info),
            Ok(_) => Err(format!("Không lấy được URL video.\n\n{}", xhs_manual_hint())),
            Err(e) => Err(format!("{}\n\n{}", e, xhs_manual_hint())),
        },
        _ => fetch_cobalt(&url).await,
    }
}

/// Payload event `download-progress` (Tauri) để UI hiển thị thanh % real-time.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProgress {
    pub downloaded: u64,
    /// Có thể là 0 nếu server không trả `Content-Length` → UI fallback indeterminate.
    pub total: u64,
}

/// Tải video về máy — stream bytes + emit `download-progress` event mỗi chunk.
#[tauri::command]
pub async fn download_video(
    app: tauri::AppHandle,
    download_url: String,
    save_path: String,
) -> Result<String, String> {
    use futures_util::StreamExt;
    use tauri::Emitter;
    use tokio::io::AsyncWriteExt;

    let client = http_client()?;

    let referer = if download_url.contains("xhscdn.com") {
        "https://www.xiaohongshu.com/"
    } else if download_url.contains("douyinvod.com") || download_url.contains("douyin") {
        "https://www.douyin.com/"
    } else if download_url.contains("tiktok") || download_url.contains("tikwm") {
        "https://www.tiktok.com/"
    } else {
        ""
    };

    let mut req = client.get(&download_url);
    if !referer.is_empty() {
        req = req.header("Referer", referer);
    }

    let resp = req.send().await.map_err(|e| {
        format!(
            "Lỗi tải video ({}): {}",
            &download_url[..60.min(download_url.len())],
            e
        )
    })?;

    if !resp.status().is_success() {
        return Err(format!("Server trả lỗi: {}", resp.status()));
    }

    let total = resp.content_length().unwrap_or(0);
    let mut file = tokio::fs::File::create(&save_path)
        .await
        .map_err(|e| format!("Lỗi tạo file: {}", e))?;

    let mut stream = resp.bytes_stream();
    let mut downloaded: u64 = 0;
    // Throttle emit: chỉ emit khi download thêm >=64KB hoặc xong, tránh spam event.
    let mut last_emit: u64 = 0;
    const EMIT_STEP: u64 = 64 * 1024;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("Lỗi đọc stream: {}", e))?;
        file.write_all(&chunk)
            .await
            .map_err(|e| format!("Lỗi ghi file: {}", e))?;
        downloaded += chunk.len() as u64;

        if downloaded - last_emit >= EMIT_STEP {
            let _ = app.emit("download-progress", DownloadProgress { downloaded, total });
            last_emit = downloaded;
        }
    }

    file.flush()
        .await
        .map_err(|e| format!("Lỗi flush file: {}", e))?;

    // Emit final 100%
    let _ = app.emit(
        "download-progress",
        DownloadProgress { downloaded, total: if total > 0 { total } else { downloaded } },
    );

    Ok(save_path)
}

fn urlencoding(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            _ => format!("%{:02X}", c as u32),
        })
        .collect()
}
