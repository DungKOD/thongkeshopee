//! Tự động gắn logo Page lên video sau khi tải xong.
//!
//! Flow:
//! 1. `ensure_ffmpeg(app)` — kiểm tra/tải ffmpeg về `app_data_dir/bin/` (1 lần).
//! 2. `ensure_page_logo_cached(app, page_id)` — fetch logo Page qua Graph API
//!    public picture endpoint, cache vào `app_data_dir/fb_pages_cache/`.
//! 3. `probe_video(...)` — đọc width/height/duration qua ffmpeg quick probe.
//! 4. `run_overlay(...)` — chạy ffmpeg filter_complex overlay + emit progress.
//! 5. Atomic rename ghi đè file gốc.
//!
//! Tối ưu:
//! - Logo cache theo `page_id` — không re-download cho video sau.
//! - Audio stream copy (`-c:a copy`) — không re-encode âm thanh.
//! - libx264 `preset=veryfast crf=23` + `+faststart` cho play instant.
//! - `OnceLock<AsyncMutex>` serialize lần download ffmpeg đầu tiên — tránh
//!   nhiều task tải song song khi user batch nhiều video.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use ffmpeg_sidecar::command::FfmpegCommand;
use ffmpeg_sidecar::event::{FfmpegEvent, LogLevel};
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::sync::Mutex as AsyncMutex;

use crate::commands::{CmdError, CmdResult};
use crate::db::FbReelsDbState;

static FFMPEG_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();

fn ffmpeg_lock() -> &'static AsyncMutex<()> {
    FFMPEG_LOCK.get_or_init(|| AsyncMutex::new(()))
}

/// Lock cho thao tác populate logo cache. N task song song dùng cùng page_id
/// (vd MAX_CONCURRENT=3 trong DownloadVideoPage) cùng cố `write` tmp file
/// `{page_id}.png.tmp` → Windows trả ACCESS DENIED (os error 5/32). Serialize
/// fetch ở đây, fast-path cache hit KHÔNG acquire lock nên không chậm.
static LOGO_CACHE_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();

fn logo_cache_lock() -> &'static AsyncMutex<()> {
    LOGO_CACHE_LOCK.get_or_init(|| AsyncMutex::new(()))
}

const PAGE_LOGO_SUBDIR: &str = "fb_pages_cache";
const FFMPEG_SUBDIR: &str = "bin";
const WATERMARK_TMP_SUBDIR: &str = "tmp_watermark";

/// Tham số gắn logo — UI persist trong app_settings, FE truyền vào mỗi lần gọi.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatermarkOptions {
    /// % chiều rộng video (1..=50). Default UI: 12.
    pub size_pct: f32,
    /// Opacity 0.0..=1.0. Default UI: 0.9.
    pub opacity: f32,
    /// % chiều rộng video cho padding trên + phải. Default UI: 4.
    pub padding_pct: f32,
    /// Chế độ chống ăn chôm: logo nhảy 4 góc mỗi 5s (chu kỳ 20s).
    /// Trộm video phải crop cả 4 góc = mất nội dung trung tâm.
    /// Default UI: false.
    #[serde(default)]
    pub anti_theft: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct StageEvent {
    watermark_id: String,
    stage: &'static str,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProgressEvent {
    watermark_id: String,
    percent: f32,
}

fn emit_stage(app: &AppHandle, id: &str, stage: &'static str, msg: impl Into<String>) {
    let _ = app.emit(
        "watermark-stage",
        StageEvent {
            watermark_id: id.to_string(),
            stage,
            message: msg.into(),
        },
    );
}

fn emit_progress(app: &AppHandle, id: &str, percent: f32) {
    let _ = app.emit(
        "watermark-progress",
        ProgressEvent {
            watermark_id: id.to_string(),
            percent: percent.clamp(0.0, 100.0),
        },
    );
}

/// Trả về `app_data_dir/bin`. Tạo nếu chưa có.
fn ffmpeg_dir(app: &AppHandle) -> CmdResult<PathBuf> {
    let base = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?;
    let dir = base.join(FFMPEG_SUBDIR);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Trả về `app_data_dir/fb_pages_cache`. Tạo nếu chưa có.
fn page_logo_dir(app: &AppHandle) -> CmdResult<PathBuf> {
    let base = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?;
    let dir = base.join(PAGE_LOGO_SUBDIR);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Trả về `app_data_dir/tmp_watermark`. ffmpeg ghi output vào đây thay vì
/// thư mục Downloads — Defender real-time protection thường KHÔNG scan
/// %APPDATA% aggressively (vs Downloads = "internet zone" → quarantine ngay).
/// Sau khi ffmpeg xong, `std::fs::copy` (CopyFileExW) atomic-overwrite file
/// gốc trong Downloads, không có race window với AV scanner.
fn watermark_tmp_dir(app: &AppHandle) -> CmdResult<PathBuf> {
    let base = app
        .path()
        .app_data_dir()
        .map_err(|e| CmdError::msg(format!("app_data_dir: {e}")))?;
    let dir = base.join(WATERMARK_TMP_SUBDIR);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

#[cfg(target_os = "windows")]
const FFMPEG_BIN_NAME: &str = "ffmpeg.exe";
#[cfg(not(target_os = "windows"))]
const FFMPEG_BIN_NAME: &str = "ffmpeg";

/// Path ffmpeg trong app data dir. Có thể chưa tồn tại.
fn local_ffmpeg_path(app: &AppHandle) -> CmdResult<PathBuf> {
    Ok(ffmpeg_dir(app)?.join(FFMPEG_BIN_NAME))
}

/// Resolve ffmpeg để chạy: ưu tiên binary local; nếu không có, fallback PATH.
fn resolve_ffmpeg(app: &AppHandle) -> Option<PathBuf> {
    if let Ok(p) = local_ffmpeg_path(app) {
        if p.exists() {
            return Some(p);
        }
    }
    if ffmpeg_sidecar::command::ffmpeg_is_installed() {
        return Some(PathBuf::from("ffmpeg"));
    }
    None
}

/// Tải ffmpeg về `app_data_bin/`. Idempotent — nếu đã tồn tại trả ngay.
/// Blocking — caller wrap bằng `spawn_blocking`.
fn download_ffmpeg_to(app_data_bin: &Path) -> Result<PathBuf, String> {
    let target = app_data_bin.join(FFMPEG_BIN_NAME);
    if target.exists() {
        return Ok(target);
    }

    let url = ffmpeg_sidecar::download::ffmpeg_download_url()
        .map_err(|e| format!("không xác định được URL ffmpeg: {e}"))?;
    let archive = ffmpeg_sidecar::download::download_ffmpeg_package(url, app_data_bin)
        .map_err(|e| format!("tải ffmpeg thất bại: {e}"))?;
    ffmpeg_sidecar::download::unpack_ffmpeg(&archive, app_data_bin)
        .map_err(|e| format!("giải nén ffmpeg thất bại: {e}"))?;

    if !target.exists() {
        return Err(format!(
            "ffmpeg unpack xong nhưng không thấy binary tại {}",
            target.display()
        ));
    }
    let _ = std::fs::remove_file(&archive);
    Ok(target)
}

/// Đảm bảo ffmpeg sẵn sàng. Lần đầu sẽ tải (~80MB) → emit stage
/// "downloading_ffmpeg" để UI hiển thị. Sau đó cache vĩnh viễn trong app data.
async fn ensure_ffmpeg(app: &AppHandle, watermark_id: &str) -> CmdResult<PathBuf> {
    if let Some(p) = resolve_ffmpeg(app) {
        return Ok(p);
    }

    let _guard = ffmpeg_lock().lock().await;

    // Double-check sau khi lấy lock — task khác có thể đã tải xong.
    if let Some(p) = resolve_ffmpeg(app) {
        return Ok(p);
    }

    emit_stage(
        app,
        watermark_id,
        "downloading_ffmpeg",
        "Đang tải ffmpeg (~80MB, chỉ 1 lần)...",
    );

    let bin_dir = ffmpeg_dir(app)?;
    tokio::task::spawn_blocking(move || download_ffmpeg_to(&bin_dir))
        .await
        .map_err(|e| CmdError::msg(format!("download task panicked: {e}")))?
        .map_err(CmdError::msg)
}

/// Đọc 12 byte đầu của file — đủ để check magic image format mà không
/// read toàn bộ file (PNG/JPG có thể ~100-500KB).
fn read_image_header(path: &Path) -> Option<[u8; 12]> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).ok()?;
    let mut buf = [0u8; 12];
    file.read_exact(&mut buf).ok()?;
    Some(buf)
}

/// Cache hit khi file tồn tại + có magic bytes hợp lệ. Atomic rename pattern
/// của writer đảm bảo file ở `path` LUÔN có content đầy đủ — không có race
/// nửa-vời do reader chỉ thấy `path` SAU khi rename xong.
fn logo_cache_is_valid(path: &Path) -> bool {
    read_image_header(path)
        .map(|h| is_supported_image(&h))
        .unwrap_or(false)
}

/// Tải logo Page về cache local nếu chưa có. Trả path PNG đã cache.
///
/// Endpoint Graph API picture: KHI KHÔNG có `access_token`, FB trả default
/// placeholder ("?" silhouette) cho hầu hết Pages. Phải truyền `page_token`
/// để lấy avatar thật. Token được fetch từ `fb_pages.access_token` ở caller.
///
/// `redirect=true` (mặc định) để reqwest tự follow → final = binary image.
///
/// Filename versioning (`.v2.png`): bump khi đổi logic fetch để invalidate
/// caches cũ. Hiện tại bump từ v1 (no-token, dấu "?") → v2 (with token).
///
/// Concurrency: fast-path cache hit không acquire lock. Cache miss serialize
/// qua `LOGO_CACHE_LOCK` để N task song song không race trên file tmp.
async fn ensure_page_logo_cached(
    app: &AppHandle,
    page_id: &str,
    page_token: Option<&str>,
) -> CmdResult<PathBuf> {
    if page_id.is_empty() || page_id.chars().any(|c| !c.is_ascii_alphanumeric()) {
        return Err(CmdError::msg("page_id không hợp lệ"));
    }
    let dir = page_logo_dir(app)?;
    let path = dir.join(format!("{page_id}.v2.png"));

    // Fast path — cache hit. Zero contention, không acquire lock.
    if logo_cache_is_valid(&path) {
        return Ok(path);
    }

    // Cache miss → acquire lock để serialize fetch+write. Tránh race giữa N
    // task watermark song song khi cùng đang lần đầu fetch logo cho 1 page.
    let _guard = logo_cache_lock().lock().await;

    // Double-check sau khi giữ lock — task khác có thể đã populate cache.
    if logo_cache_is_valid(&path) {
        return Ok(path);
    }
    // Xóa file cache hỏng (nếu có) — bỏ qua lỗi NotFound.
    if path.exists() {
        if let Err(e) = std::fs::remove_file(&path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(CmdError::msg(format!(
                    "không xóa được cache hỏng {}: {e}",
                    path.display()
                )));
            }
        }
    }
    // Cleanup cache v1 cũ ("?" placeholder) — best effort. Không ảnh hưởng
    // logic nếu fail.
    let _ = std::fs::remove_file(dir.join(format!("{page_id}.png")));

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| CmdError::msg(format!("tạo HTTP client: {e}")))?;

    // Build URL với access_token nếu có. Reqwest's `query` method tự URL-encode.
    let mut req = client
        .get(format!("https://graph.facebook.com/{page_id}/picture"))
        .query(&[
            ("width", "512"),
            ("height", "512"),
            ("redirect", "true"),
        ]);
    if let Some(token) = page_token.filter(|t| !t.is_empty()) {
        req = req.query(&[("access_token", token)]);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| CmdError::msg(format!("fetch logo: {e}")))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(CmdError::msg(format!(
            "không tải được logo Page {page_id}: HTTP {status}"
        )));
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| CmdError::msg(format!("đọc body logo: {e}")))?;
    if bytes.is_empty() {
        return Err(CmdError::msg("logo Page rỗng"));
    }
    // Validate magic bytes — Graph API trả JSON error nếu page_id bị restrict
    // hoặc redirect chain hỏng. Khi đó save xong, ffmpeg sẽ AVERROR khó hiểu.
    if !is_supported_image(&bytes) {
        let head: String = bytes
            .iter()
            .take(120)
            .map(|b| {
                if b.is_ascii() && !b.is_ascii_control() {
                    *b as char
                } else {
                    '.'
                }
            })
            .collect();
        return Err(CmdError::msg(format!(
            "logo Page không phải PNG/JPG/WebP (đầu file: \"{head}\")"
        )));
    }

    // Atomic write: tmp file → rename. Lock ở trên đảm bảo chỉ 1 task ở đây
    // tại 1 thời điểm → không cần unique tmp suffix. Cleanup orphan tmp từ
    // lần process trước (cực hiếm — chỉ khi app crash giữa write).
    let tmp = path.with_extension("png.tmp");
    if tmp.exists() {
        let _ = std::fs::remove_file(&tmp);
    }
    std::fs::write(&tmp, &bytes).map_err(|e| {
        CmdError::msg(format!(
            "ghi tmp logo {}: {e}",
            tmp.display()
        ))
    })?;
    std::fs::rename(&tmp, &path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        CmdError::msg(format!(
            "rename tmp → cache {}: {e}",
            path.display()
        ))
    })?;
    Ok(path)
}

/// Detect PNG / JPEG / WebP / GIF qua magic bytes. ffmpeg đọc được hết các
/// format này nên `.png` extension chỉ là quy ước local — không quan trọng
/// content thực là gì miễn là 1 trong các format này.
fn is_supported_image(bytes: &[u8]) -> bool {
    if bytes.len() < 12 {
        return false;
    }
    // PNG: 89 50 4E 47 0D 0A 1A 0A
    if bytes.starts_with(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]) {
        return true;
    }
    // JPEG: FF D8 FF
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return true;
    }
    // GIF: "GIF87a" hoặc "GIF89a"
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return true;
    }
    // WebP: "RIFF...WEBP"
    if bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        return true;
    }
    false
}

/// Kết quả probe video — đủ thông tin cho run_overlay tính filter + chọn fps.
#[derive(Debug, Default, Clone, Copy)]
struct VideoProbe {
    width: u32,
    height: u32,
    /// fps gốc của input. Có thể 0 nếu không detect được → caller fallback 30.
    fps: f32,
    duration_s: f32,
    /// Input có audio stream không. Dùng quyết định `-map "0:a?"` an toàn.
    has_audio: bool,
}

/// Probe video qua ffmpeg quick scan: width/height/fps/duration/has_audio.
///
/// Strategy 2 lớp: ưu tiên `ParsedInputStream`/`ParsedDuration` của
/// ffmpeg_sidecar (đã decode sẵn struct). Fallback regex parse trực tiếp
/// stderr Log lines — vì ffmpeg_sidecar 2.x đôi khi MISS stream khi:
/// - Codec HEVC/AV1 (TikTok hay xuất HEVC từ iPhone)
/// - Stream line có side-data dài (Dolby Vision, HDR10+)
/// - Banner format đổi giữa các phiên bản ffmpeg
///
/// Khi event parser miss, raw stderr vẫn có `Stream #0:0(...): Video: ...
/// 720x1280 ...` — regex catch được.
async fn probe_video(ffmpeg_bin: &Path, video_path: &Path) -> CmdResult<VideoProbe> {
    let bin = ffmpeg_bin.to_path_buf();
    let video = video_path.to_path_buf();

    tokio::task::spawn_blocking(move || -> Result<VideoProbe, String> {
        let mut cmd = FfmpegCommand::new_with_path(bin);
        cmd.hide_banner()
            .arg("-i")
            .arg(&video)
            .args(["-t", "0", "-f", "null", "-"]);
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("spawn ffmpeg probe: {e}"))?;
        let mut probe = VideoProbe::default();
        // Collect raw log lines để fallback regex parse nếu event parser miss.
        let mut log_lines: Vec<String> = Vec::new();
        let iter = child
            .iter()
            .map_err(|e| format!("ffmpeg iter: {e}"))?;
        for event in iter {
            match event {
                FfmpegEvent::ParsedInputStream(s) => {
                    if let Some(v) = s.video_data() {
                        if v.width > 0 && probe.width == 0 {
                            probe.width = v.width;
                            probe.height = v.height;
                            // fps gốc — VFR có thể là average, vẫn dùng được
                            // làm target CFR.
                            if v.fps.is_finite() && v.fps > 0.0 {
                                probe.fps = v.fps;
                            }
                        }
                    } else if s.is_audio() {
                        probe.has_audio = true;
                    }
                }
                FfmpegEvent::ParsedDuration(d) => {
                    if d.duration > 0.0 {
                        probe.duration_s = d.duration as f32;
                    }
                }
                FfmpegEvent::Log(_, msg) | FfmpegEvent::ParsedStreamMapping(msg) => {
                    log_lines.push(msg);
                }
                _ => {}
            }
        }
        let _ = child.wait();

        // Fallback regex parse từ stderr log lines.
        if probe.width == 0 || probe.height == 0 {
            for line in &log_lines {
                parse_stream_line_fallback(line, &mut probe);
                if probe.width > 0 {
                    break;
                }
            }
        }
        if probe.duration_s == 0.0 {
            for line in &log_lines {
                if let Some(d) = parse_duration_line_fallback(line) {
                    probe.duration_s = d;
                    break;
                }
            }
        }
        if !probe.has_audio {
            // "Stream #0:1(und): Audio: aac ..."
            probe.has_audio = log_lines
                .iter()
                .any(|l| l.contains("Stream #") && l.contains(": Audio:"));
        }

        if probe.width == 0 || probe.height == 0 {
            // Trả luôn tail log để debug (3 dòng cuối) — tránh "không đọc được
            // kích thước" mà user không biết vì sao.
            let tail = log_lines
                .iter()
                .rev()
                .take(3)
                .rev()
                .cloned()
                .collect::<Vec<_>>()
                .join(" | ");
            return Err(format!(
                "không đọc được kích thước video (ffmpeg log: {})",
                if tail.is_empty() { "<empty>" } else { &tail }
            ));
        }
        Ok(probe)
    })
    .await
    .map_err(|e| CmdError::msg(format!("probe task panicked: {e}")))?
    .map_err(CmdError::msg)
}

/// Regex parse 1 dòng log để tìm WxH + fps của video stream.
/// Pattern khớp: `Stream #0:0[0x1](und): Video: h264 (High), yuv420p, 720x1280 [SAR 1:1 DAR 9:16], 1234 kb/s, 30 fps, ...`
/// hoặc gọn hơn `... Video: hevc ..., 1080x1920, ..., 30 fps`.
fn parse_stream_line_fallback(line: &str, probe: &mut VideoProbe) {
    if !line.contains("Stream #") || !line.contains(": Video:") {
        return;
    }
    // WxH: số x số, mỗi vế 2-5 chữ số, không nằm trong identifier (kèm word
    // boundary để không match `0x12abc`).
    static DIM_RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = DIM_RE.get_or_init(|| regex::Regex::new(r"\b(\d{2,5})x(\d{2,5})\b").unwrap());
    if let Some(c) = re.captures(line) {
        let w: u32 = c[1].parse().unwrap_or(0);
        let h: u32 = c[2].parse().unwrap_or(0);
        if w > 0 && h > 0 {
            probe.width = w;
            probe.height = h;
        }
    }
    // fps: `30 fps` hoặc `29.97 fps`.
    static FPS_RE: OnceLock<regex::Regex> = OnceLock::new();
    let fps_re = FPS_RE.get_or_init(|| regex::Regex::new(r"(\d+(?:\.\d+)?)\s*fps").unwrap());
    if probe.fps == 0.0 {
        if let Some(c) = fps_re.captures(line) {
            if let Ok(f) = c[1].parse::<f32>() {
                if f.is_finite() && f > 0.0 {
                    probe.fps = f;
                }
            }
        }
    }
}

/// Parse `Duration: 00:01:23.45, start: ...` → giây.
/// Robust với log prefix `[info]`/`[warning]` mà ffmpeg_sidecar bật bằng
/// `-loglevel level+info`.
fn parse_duration_line_fallback(line: &str) -> Option<f32> {
    // Skip log level prefix nếu có (e.g. "[info]   Duration: ...").
    let after_prefix = line
        .split_once(']')
        .map(|(_, rest)| rest)
        .unwrap_or(line);
    let trimmed = after_prefix.trim_start();
    let rest = trimmed.strip_prefix("Duration:")?.trim_start();
    let comma = rest.find(',').unwrap_or(rest.len());
    let ts = rest[..comma].trim();
    if ts == "N/A" {
        return None;
    }
    let secs = parse_ffmpeg_time(ts);
    if secs > 0.0 {
        Some(secs)
    } else {
        None
    }
}

/// Chạy ffmpeg overlay → emit progress events `watermark-progress`.
#[allow(clippy::too_many_arguments)]
async fn run_overlay(
    app: &AppHandle,
    ffmpeg_bin: &Path,
    video_path: &Path,
    logo_path: &Path,
    out_path: &Path,
    logo_w_px: u32,
    pad_px: u32,
    opacity: f32,
    duration_s: f32,
    fps: f32,
    has_audio: bool,
    anti_theft: bool,
    watermark_id: &str,
) -> CmdResult<()> {
    let bin = ffmpeg_bin.to_path_buf();
    let video = video_path.to_path_buf();
    let logo = logo_path.to_path_buf();
    let out = out_path.to_path_buf();
    let app_clone = app.clone();
    let id = watermark_id.to_string();
    let opacity = opacity.clamp(0.0, 1.0);
    let duration = if duration_s > 0.0 { duration_s } else { 1.0 };
    // Target fps cho CFR conversion. Fallback 30 nếu probe fail. Clamp tránh
    // giá trị extreme (vd 1000fps từ container metadata sai).
    let target_fps: f32 = if fps.is_finite() && (5.0..=120.0).contains(&fps) {
        fps
    } else {
        30.0
    };

    // filter_complex — logo Page dạng tròn (avatar style):
    //  [1:v] (logo gốc 512x512 từ Graph API)
    //   ├ format=rgba: thêm alpha channel kể cả khi source là JPG
    //   ├ setsar=1: vuông pixel, tránh ellipse khi overlay
    //   ├ geq: per-pixel mask. Alpha = original * clip(R - hypot, 0, 1)
    //   │      R = min(W,H)/2 = bán kính. `+0.5` cho soft 1px edge → AA mềm.
    //   │      Mask vẽ ở resolution gốc 512x512 → khi scale-down lanczos
    //   │      thành ~60-100px, viền tròn cực mượt (effective SSAA free).
    //   ├ scale: xuống `logo_w_px` × `logo_w_px` (vuông) bằng lanczos
    //   └ colorchannelmixer aa=opacity: nhân alpha cho độ trong tổng thể
    //  [0:v] (video) ← overlay [wm] ở (W-w-pad, pad) = góc trên phải.
    //  -map "0:a?" giữ audio gốc nếu có.
    //
    //  Anti-theft mode: overlay x/y dùng expression theo `t` (PTS giây) thay
    //  vì hằng số → logo nhảy 4 góc theo chu kỳ 20s. Thứ tự: top-right →
    //  bottom-left → top-left → bottom-right. ffmpeg eval=frame (default)
    //  cập nhật vị trí mỗi frame nên transition là instant teleport mỗi 5s.
    //  Trộm muốn loại logo phải crop 4 góc = mất nội dung trung tâm.
    //
    //  Lưu ý escape: commas trong if()/mod()/min()/hypot()/clip() phải nằm
    //  trong single quotes ('...') để ffmpeg parser không coi là filter
    //  separator.
    // Tính frame number per segment dựa trên target_fps. 1 segment = 5s.
    // Dùng `n` (frame number, monotonic 0,1,2,...) thay vì `t` (PTS-dependent,
    // có thể NaN/non-monotonic với video XHS có PTS reset). `n` ROBUST hoàn
    // toàn với mọi video — không bao giờ làm output bị cut.
    let frames_per_segment: u32 = (target_fps * 5.0).round() as u32;
    let frames_per_segment = frames_per_segment.max(30); // tránh /0 nếu target_fps cực nhỏ

    let overlay_xy = if anti_theft {
        // Anti-theft pattern:
        //   - Seg 0 (5s đầu) = TR cố định (trông tự nhiên cho viewer)
        //   - Seg ≥1 = RANDOM pick từ 3 góc còn lại {TL, BR, BL}
        //
        // Vì sao chiến lược này tối ưu? Trộm xem video, thấy logo ở TR → crop
        // chừa TR ra. Nhưng sau 5s logo nhảy vào 1 trong 3 góc còn lại → crop
        // TR chỉ giấu được 5 giây đầu, mọi đoạn sau LỘ LOGO. Muốn xóa hết phải
        // crop CẢ 4 góc → mất nội dung trung tâm.
        //
        // segment = floor(n / frames_per_segment). Dùng `n` (frame number)
        // thay vì `t` (PTS giây) — XHS/TikTok có PTS reset hoặc non-monotonic,
        // `t` jump → expression sai hoặc ffmpeg cut output sớm. `n` LUÔN tăng
        // đều, immune mọi PTS quirk.
        //
        // hash3(seg) = mod(floor(abs(sin(seg*12.9898))*100000), 3) ∈ {0,1,2}
        // Vì sao `abs()` + scale 100000 + floor + mod? Không có abs, ffmpeg
        // `mod(âm, ..)` trả âm (fmod giữ dấu) → distribution lệch. Floor +
        // mod 3 đảm bảo integer index uniform {0,1,2}.
        //
        // Mapping hash3 → corner (cho seg ≥ 1):
        //   0 → TL (left, top)
        //   1 → BR (right, bottom)
        //   2 → BL (left, bottom)
        //
        // X: seg=0 → right(TR); else hash3=1 → right(BR); else left(TL/BL)
        // Y: seg=0 → top(TR);   else hash3=0 → top(TL);   else bottom(BR/BL)
        //
        // Check seg=0 = `lt(n, fps_seg)` (5s đầu = frames 0..fps_seg-1).
        // Cheaper hơn `eq(floor(n/fps_seg), 0)` vì né floor division.
        //
        // `eval=frame` ép re-eval mỗi frame; `eof_action=repeat` để logo
        // (input 1-frame static) tự repeat suốt video.
        format!(
            "x='if(lt(n,{fps_seg}),W-w-{pad_px},if(eq(mod(floor(abs(sin(floor(n/{fps_seg})*12.9898))*100000),3),1),W-w-{pad_px},{pad_px}))':\
y='if(lt(n,{fps_seg}),{pad_px},if(eq(mod(floor(abs(sin(floor(n/{fps_seg})*12.9898))*100000),3),0),{pad_px},H-h-{pad_px}))':\
eval=frame:eof_action=repeat",
            fps_seg = frames_per_segment
        )
    } else {
        format!("x=W-w-{pad_px}:y={pad_px}:eof_action=repeat")
    };

    // Filter chain MINIMAL — chỉ convert pixel format trên video chính, KHÔNG
    // đụng PTS hoặc fps. Lý do:
    //   - `setpts=PTS-STARTPTS` có thể gây cut output 5s nếu input có PTS
    //     non-monotonic (XHS hay có frame PTS reset)
    //   - `fps=fps=N` filter cũng truncate khi gặp PTS gap lớn
    //   - Để ffmpeg muxer tự handle timing với `-fflags +genpts` (fill missing
    //     PTS) + `-avoid_negative_ts make_zero` (shift về ≥0)
    //   - Pixel format conversion (yuv420p) là an toàn, không ảnh hưởng duration
    //
    // Logo (`[1:v]`) qua mask tròn + scale + opacity như cũ — KHÔNG ảnh hưởng
    // duration vì là static image input.
    let filter = format!(
        "[0:v]format=yuv420p[v0];\
         [1:v]format=rgba,setsar=1,\
         geq=r='r(X,Y)':g='g(X,Y)':b='b(X,Y)':\
a='alpha(X,Y)*clip(min(W,H)/2-hypot(X-W/2,Y-H/2)+0.5,0,1)',\
         scale={logo_w_px}:{logo_w_px}:flags=lanczos,\
         colorchannelmixer=aa={opacity:.3}[wm];\
         [v0][wm]overlay={overlay_xy}[vout]"
    );

    // Audio: COPY codec gốc, không re-encode. Lý do:
    //   - AAC re-encode + resample (-ar 44100) có thể fail/cut với 1 số XHS
    //     có audio codec lạ → output bị cut theo audio duration.
    //   - `-c:a copy` giữ nguyên audio stream → audio sync 100% với video gốc,
    //     duration không đổi.
    //   - Nếu có audio: map stream đầu tiên + copy. Nếu không: explicit `-an`.
    let audio_args: Vec<&str> = if has_audio {
        vec!["-map", "0:a:0?", "-c:a", "copy"]
    } else {
        vec!["-an"]
    };

    tokio::task::spawn_blocking(move || -> Result<(), String> {
        let mut cmd = FfmpegCommand::new_with_path(bin);
        cmd.hide_banner()
            .overwrite()
            // -fflags +genpts+igndts: sinh PTS missing + bỏ DTS sai. Tránh
            //   filter nhận PTS=NaN → expression overlay rơi nhánh else.
            // -err_detect ignore_err: tiếp tục decode khi gặp frame corrupt
            //   thay vì abort — 1 số XHS có frame cuối hỏng nhưng nội dung
            //   chính OK.
            // -max_error_rate 1.0: nâng ngưỡng decode error từ 2/3 (default)
            //   lên 100%. Nếu không có flag này, video XHS bị bitstream corrupt
            //   nặng (vd >67% frame hỏng) sẽ trigger
            //   "[fatal] Decode error rate 0.87 exceeds maximum 0.66" → exit 69.
            //   Với flag này, ffmpeg cố decode tới cùng và conceal frame hỏng.
            // -ec guess_mvs+deblock+favor_inter: error concealment cho H264.
            //   Khi 1 macroblock hỏng, decoder đoán motion vector từ neighbor
            //   frames thay vì để vùng đó xanh/đen → output xem được, không
            //   crash. `favor_inter` ưu tiên dùng frame trước thay vì spatial
            //   interpolation (thường mượt hơn cho real-world video).
            .args(["-fflags", "+genpts+igndts"])
            .args(["-err_detect", "ignore_err"])
            .args(["-max_error_rate", "1.0"])
            .args(["-ec", "guess_mvs+deblock+favor_inter"])
            .arg("-i")
            .arg(&video)
            // KHÔNG dùng `-loop 1` cho logo: sẽ làm logo input vô hạn, không
            // có `-shortest` thì output cũng vô hạn (hang). Để mặc định: logo
            // = 1 frame, overlay filter dùng `eof_action=repeat` (default) →
            // tự repeat last frame qua toàn bộ video chính.
            .arg("-i")
            .arg(&logo)
            .args(["-filter_complex", &filter])
            .args(["-map", "[vout]"]);

        // Audio map + codec (re-encode AAC nếu có, drop nếu không).
        for arg in &audio_args {
            cmd.arg(*arg);
        }

        cmd.args(["-c:v", "libx264"])
            .args(["-preset", "veryfast"])
            .args(["-crf", "23"])
            .args(["-pix_fmt", "yuv420p"])
            // -profile:v high -level 4.0: tương thích rộng (Reels/IG/TikTok
            //   đều OK). Tránh main10/high10 cần 10-bit decoder không phổ
            //   biến trên mobile.
            .args(["-profile:v", "high"])
            .args(["-level", "4.0"])
            .args(["-movflags", "+faststart"])
            // -avoid_negative_ts make_zero: shift PTS về ≥0 → player đọc
            //   duration metadata đúng. Fix "mất thời gian" cho XHS PTS lệch.
            .args(["-avoid_negative_ts", "make_zero"])
            // -max_muxing_queue_size 9999: tăng buffer muxer khi audio/video
            //   PTS lệch lớn. Không có flag này → fail với "Too many packets
            //   buffered" cho 1 số container quirk.
            .args(["-max_muxing_queue_size", "9999"])
            // -shortest KHÔNG dùng — nếu input audio dài hơn video (XHS quirk),
            //   `-shortest` sẽ truncate. Để ffmpeg tự chọn duration max.
            //
            // Force MP4 muxer — nếu temp filename không có `.mp4` ext (vd
            //   `.tmp` suffix), ffmpeg sẽ AVERROR(EINVAL) -22.
            .args(["-f", "mp4"])
            .arg(&out);

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("spawn ffmpeg: {e}"))?;
        // Capture cả `Error` events lẫn `Log(Error|Fatal, ...)`. Nhiều lỗi
        // ffmpeg (filter invalid, codec missing, file unreadable) chỉ xuất hiện
        // dưới dạng Log lines, không trigger FfmpegEvent::Error.
        let mut err_log: Vec<String> = Vec::new();
        let iter = child
            .iter()
            .map_err(|e| format!("ffmpeg iter: {e}"))?;
        for event in iter {
            match event {
                FfmpegEvent::Progress(p) => {
                    let secs = parse_ffmpeg_time(&p.time);
                    let pct = (secs / duration * 100.0).clamp(0.0, 99.5);
                    let _ = app_clone.emit(
                        "watermark-progress",
                        ProgressEvent {
                            watermark_id: id.clone(),
                            percent: pct,
                        },
                    );
                }
                FfmpegEvent::Error(e) => {
                    err_log.push(e);
                }
                FfmpegEvent::Log(level, msg) => {
                    if matches!(level, LogLevel::Error | LogLevel::Fatal) {
                        err_log.push(msg);
                    }
                }
                _ => {}
            }
        }
        let status = child.wait().map_err(|e| format!("ffmpeg wait: {e}"))?;
        if !status.success() {
            let _ = std::fs::remove_file(&out);
            let detail = if err_log.is_empty() {
                "không có log lỗi".to_string()
            } else {
                // Lấy tối đa 3 dòng cuối cùng — message ý nghĩa nhất ở cuối.
                let tail = err_log
                    .iter()
                    .rev()
                    .take(3)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" | ");
                tail
            };
            return Err(format!(
                "ffmpeg exit code {:?}: {detail}",
                status.code()
            ));
        }
        if !out.exists() {
            return Err("ffmpeg success nhưng không có file output".to_string());
        }
        Ok(())
    })
    .await
    .map_err(|e| CmdError::msg(format!("encode task panicked: {e}")))?
    .map_err(CmdError::msg)
}

/// Apply watermark lên video. Ghi đè file gốc khi xong (atomic rename).
#[tauri::command]
pub async fn apply_video_watermark(
    app: AppHandle,
    fb_db: State<'_, FbReelsDbState>,
    video_path: String,
    page_id: String,
    options: WatermarkOptions,
    watermark_id: String,
) -> CmdResult<()> {
    let video = PathBuf::from(&video_path);
    if !video.exists() {
        return Err(CmdError::msg(format!(
            "file video không tồn tại: {}",
            video.display()
        )));
    }
    if !(1.0..=50.0).contains(&options.size_pct) {
        return Err(CmdError::msg("size_pct ngoài khoảng 1..=50"));
    }
    if !(0.0..=1.0).contains(&options.opacity) {
        return Err(CmdError::msg("opacity ngoài khoảng 0..=1"));
    }
    if !(0.0..=20.0).contains(&options.padding_pct) {
        return Err(CmdError::msg("padding_pct ngoài khoảng 0..=20"));
    }

    // Guard: page_id phải tồn tại trong DB local + fetch access_token để
    // gọi Graph API picture endpoint. Không có token → FB trả default
    // placeholder ("?" silhouette) thay vì avatar thật.
    let page_token: Option<String> = {
        let conn = fb_db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        match conn.query_row(
            "SELECT access_token FROM fb_pages WHERE page_id = ?1",
            rusqlite::params![page_id],
            |r| r.get::<_, String>(0),
        ) {
            Ok(t) => Some(t),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                return Err(CmdError::msg(format!(
                    "Page {page_id} chưa được lưu trong app — thêm Page trước"
                )));
            }
            Err(e) => return Err(CmdError::Db(e)),
        }
    };

    emit_stage(&app, &watermark_id, "preparing", "Đang chuẩn bị ffmpeg...");
    let ffmpeg_bin = ensure_ffmpeg(&app, &watermark_id).await?;

    emit_stage(&app, &watermark_id, "fetching_logo", "Đang tải logo Page...");
    let logo_path =
        ensure_page_logo_cached(&app, &page_id, page_token.as_deref()).await?;

    emit_stage(&app, &watermark_id, "probing", "Đang đọc kích thước video...");
    let probe = probe_video(&ffmpeg_bin, &video).await?;
    let logo_w_px =
        (((probe.width as f32) * options.size_pct / 100.0).round() as u32).max(8);
    let pad_px =
        ((probe.width as f32) * options.padding_pct / 100.0).round() as u32;

    let ext = video
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("mp4");
    // Tmp file ở `app_data_dir/tmp_watermark/wm_{uuid}.{ext}` — KHÔNG nằm
    // trong Downloads folder. Định danh UUID hoàn toàn random nên không trùng
    // giữa N task song song và không match heuristic pattern nào của Defender.
    // Extension thật ({ext}) giữ cho ffmpeg suy được container.
    let tmp_dir = watermark_tmp_dir(&app)?;
    let tmp_out = tmp_dir.join(format!("wm_{}.{ext}", uuid::Uuid::new_v4()));

    emit_stage(&app, &watermark_id, "encoding", "Đang gắn logo...");
    emit_progress(&app, &watermark_id, 0.0);

    let encode_result = run_overlay(
        &app,
        &ffmpeg_bin,
        &video,
        &logo_path,
        &tmp_out,
        logo_w_px,
        pad_px,
        options.opacity,
        probe.duration_s,
        probe.fps,
        probe.has_audio,
        options.anti_theft,
        &watermark_id,
    )
    .await;

    // Nếu encode fail → cleanup tmp + return error. Đảm bảo `tmp_watermark/`
    // không tích tụ rác từ các lần fail.
    if let Err(e) = encode_result {
        let _ = std::fs::remove_file(&tmp_out);
        return Err(e);
    }

    // Replace file gốc bằng tmp. Dùng `std::fs::copy` (CopyFileExW) thay vì
    // rename — copy overwrite atomic trong 1 syscall, không có race window
    // remove-then-rename. Vì tmp ở app_data_dir, không bị Defender quarantine
    // giữa ffmpeg exit và copy.
    replace_file_with_retry(&tmp_out, &video).await?;

    emit_progress(&app, &watermark_id, 100.0);
    emit_stage(&app, &watermark_id, "done", "Xong");
    Ok(())
}

/// Thay thế `dst` bằng nội dung của `src` qua atomic copy + delete tmp.
///
/// Strategy = `std::fs::copy(src, dst)` thay vì remove+rename:
/// - CopyFileExW (Windows backend của `fs::copy`) overwrite dst trong 1 syscall,
///   không có race window giữa "remove dst" và "put src in place".
/// - Khi AV/Defender đang scan dst, CopyFileExW vẫn mở dst với
///   FILE_SHARE_READ → ít gặp ERROR_SHARING_VIOLATION hơn rename.
/// - Khi AV/Defender đã quarantine src (NotFound), retry vô nghĩa → bail nhanh
///   với message rõ ràng để user biết cần thêm app vào exclusion.
///
/// Sau copy thành công, xóa src (best-effort) → giữ `tmp_watermark/` sạch.
async fn replace_file_with_retry(src: &Path, dst: &Path) -> CmdResult<()> {
    use std::io::ErrorKind;

    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    let mut delay = Duration::from_millis(50);
    let mut attempts = 0u32;

    loop {
        attempts += 1;
        match std::fs::copy(src, dst) {
            Ok(_) => {
                // Cleanup tmp file — best effort. Nếu AV vẫn giữ src, nó sẽ
                // được xóa sau khi AV release; trong lúc đó chỉ là junk trong
                // app_data_dir/tmp_watermark/, không ảnh hưởng user.
                let _ = std::fs::remove_file(src);
                return Ok(());
            }
            Err(e) => {
                // NotFound = src biến mất → Defender quarantine hoặc app khác
                // xóa. Retry vô nghĩa.
                if e.kind() == ErrorKind::NotFound {
                    return Err(CmdError::msg(format!(
                        "file watermark biến mất giữa chừng (Windows Defender \
                         quarantine?) — thử thêm thư mục \"{}\" vào exclusion \
                         của Defender. Path: {}",
                        src.parent()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "<unknown>".to_string()),
                        src.display()
                    )));
                }

                // Sharing violation / permission denied → AV/Explorer giữ dst.
                // Retry với backoff.
                let retriable = matches!(e.raw_os_error(), Some(32) | Some(33))
                    || e.kind() == ErrorKind::PermissionDenied;
                if !retriable || std::time::Instant::now() >= deadline {
                    return Err(CmdError::msg(format!(
                        "copy {} → {}: {e} (sau {attempts} lần thử trong ~8s)",
                        src.display(),
                        dst.display()
                    )));
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_millis(500));
            }
        }
    }
}

/// Xóa cache logo cho 1 page — UI có nút "Tải lại logo" cho user khi Page đổi
/// avatar. Idempotent — không có file thì cũng OK.
#[tauri::command]
pub fn clear_page_logo_cache(app: AppHandle, page_id: String) -> CmdResult<()> {
    if page_id.is_empty() || page_id.chars().any(|c| !c.is_ascii_alphanumeric()) {
        return Err(CmdError::msg("page_id không hợp lệ"));
    }
    let dir = page_logo_dir(&app)?;
    let path = dir.join(format!("{page_id}.png"));
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(())
}

/// Parse "HH:MM:SS.mmm" → giây (f32). Format chuẩn của ffmpeg progress.
fn parse_ffmpeg_time(s: &str) -> f32 {
    let s = s.trim();
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 {
        return 0.0;
    }
    let h: f32 = parts[0].parse().unwrap_or(0.0);
    let m: f32 = parts[1].parse().unwrap_or(0.0);
    let s: f32 = parts[2].parse().unwrap_or(0.0);
    h * 3600.0 + m * 60.0 + s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_time_basic() {
        assert!((parse_ffmpeg_time("00:00:01.23") - 1.23).abs() < 0.001);
        assert!((parse_ffmpeg_time("01:02:03.50") - 3723.50).abs() < 0.001);
        assert_eq!(parse_ffmpeg_time("garbage"), 0.0);
        assert_eq!(parse_ffmpeg_time(""), 0.0);
    }

    #[test]
    fn image_magic_bytes() {
        let png = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0];
        let jpeg = [0xFF, 0xD8, 0xFF, 0xE0, 0, 0, 0, 0, 0, 0, 0, 0];
        let webp = *b"RIFF\x00\x00\x00\x00WEBP";
        let gif = *b"GIF89a\x00\x00\x00\x00\x00\x00";
        let json = *b"{\"error\":{\"";
        assert!(is_supported_image(&png));
        assert!(is_supported_image(&jpeg));
        assert!(is_supported_image(&webp));
        assert!(is_supported_image(&gif));
        assert!(!is_supported_image(&json));
        assert!(!is_supported_image(b""));
        assert!(!is_supported_image(b"short"));
    }

    fn probe(line: &str) -> VideoProbe {
        let mut p = VideoProbe::default();
        parse_stream_line_fallback(line, &mut p);
        p
    }

    #[test]
    fn fallback_parse_standard_tiktok_h264() {
        // Format thường gặp nhất từ TikTok export.
        let line = "[info]   Stream #0:0[0x1](und): Video: h264 (High) (avc1 / 0x31637661), yuv420p(tv, bt709, progressive), 720x1280 [SAR 1:1 DAR 9:16], 1797 kb/s, 30 fps, 30 tbr, 15360 tbn (default)";
        let p = probe(line);
        assert_eq!(p.width, 720);
        assert_eq!(p.height, 1280);
        assert!((p.fps - 30.0).abs() < 0.01);
    }

    #[test]
    fn fallback_parse_missing_fps_only_tbr() {
        // TikTok đôi khi omit `fps` khi == tbr → ffmpeg_sidecar bail trong
        // try_parse_video_stream vì có `?` sau fps parse. Đây là root cause.
        // Fallback PHẢI lấy được WxH dù fps không có.
        let line = "[info]   Stream #0:0[0x1](und): Video: h264 (High) (avc1 / 0x31637661), yuv420p, 540x960, 1234 kb/s, 30 tbr, 90k tbn (default)";
        let p = probe(line);
        assert_eq!(p.width, 540);
        assert_eq!(p.height, 960);
    }

    #[test]
    fn fallback_parse_hevc_iphone_hdr() {
        // iPhone xuất HEVC + side data HDR. ffmpeg in stream với 10-bit + bt2020.
        let line = "[info]   Stream #0:0[0x1](und): Video: hevc (Main 10) (hvc1 / 0x31637668), yuv420p10le(tv, bt2020nc/bt2020/smpte2084), 1080x1920 [SAR 1:1 DAR 9:16], 4500 kb/s, 30 fps, 30 tbr, 600 tbn (default)";
        let p = probe(line);
        assert_eq!(p.width, 1080);
        assert_eq!(p.height, 1920);
        assert!((p.fps - 30.0).abs() < 0.01);
    }

    #[test]
    fn fallback_parse_av1_fractional_fps() {
        let line = "[info]   Stream #0:0(eng): Video: av1 (Main) (av01 / 0x31307661), yuv420p, 1920x1080, 29.97 fps, 30 tbr, 12800 tbn";
        let p = probe(line);
        assert_eq!(p.width, 1920);
        assert_eq!(p.height, 1080);
        assert!((p.fps - 29.97).abs() < 0.01);
    }

    #[test]
    fn fallback_parse_vp9_webm() {
        let line = "[info]   Stream #0:0: Video: vp9 (Profile 0), yuv420p(tv, bt709), 1280x720, SAR 1:1 DAR 16:9, 60 fps, 60 tbr, 1k tbn (default)";
        let p = probe(line);
        assert_eq!(p.width, 1280);
        assert_eq!(p.height, 720);
        assert!((p.fps - 60.0).abs() < 0.01);
    }

    #[test]
    fn fallback_no_match_audio_line() {
        // Audio line không có "Video:" → KHÔNG được set width/height.
        let line = "[info]   Stream #0:1(eng): Audio: aac (LC) (mp4a / 0x6134706D), 48000 Hz, stereo, fltp, 128 kb/s (default)";
        let p = probe(line);
        assert_eq!(p.width, 0);
        assert_eq!(p.height, 0);
    }

    #[test]
    fn fallback_no_match_subtitle_line() {
        let line = "[info]   Stream #0:13(dut): Subtitle: hdmv_pgs_subtitle, 1920x1080";
        let p = probe(line);
        // Subtitle line có WxH nhưng KHÔNG có ": Video:" → guard reject.
        assert_eq!(p.width, 0);
    }

    #[test]
    fn fallback_codec_tag_not_match() {
        // Codec tag dạng `0x31637661` không có 'x' giữa 2 cụm digit ≥2 → không
        // bị match nhầm là WxH.
        let line = "[info]   Stream #0:0: Video: h264 (avc1 / 0x31637661), yuv420p";
        let p = probe(line);
        assert_eq!(p.width, 0);
    }

    #[test]
    fn fallback_picks_resolution_not_aspect_ratio() {
        // DAR có thể là 9x16 (1 digit mỗi vế) — bị min 2 digit của regex loại.
        // 720x1280 phải được pick, không phải 1x1 (SAR) hay 9x16 (DAR).
        let line = "[info]   Stream #0:0: Video: h264, yuv420p, 720x1280 [SAR 1:1 DAR 9:16], 30 fps";
        let p = probe(line);
        assert_eq!(p.width, 720);
        assert_eq!(p.height, 1280);
    }

    #[test]
    fn fallback_parse_duration_standard() {
        let line = "[info]   Duration: 00:00:27.00, start: 0.000000, bitrate: 1797 kb/s";
        assert!((parse_duration_line_fallback(line).unwrap() - 27.0).abs() < 0.01);
    }

    #[test]
    fn fallback_parse_duration_with_minutes() {
        let line = "  Duration: 00:01:23.45, start: 0.000000, bitrate: 1234 kb/s";
        assert!((parse_duration_line_fallback(line).unwrap() - 83.45).abs() < 0.01);
    }

    #[test]
    fn fallback_parse_duration_na_returns_none() {
        // Live stream / corrupt header: ffmpeg in "Duration: N/A".
        let line = "[info]   Duration: N/A, start: 0.000000, bitrate: N/A";
        assert!(parse_duration_line_fallback(line).is_none());
    }

    #[test]
    fn fallback_parse_duration_ignores_non_duration_lines() {
        let line = "[info]   Stream #0:0: Video: h264, 720x1280, 30 fps";
        assert!(parse_duration_line_fallback(line).is_none());
    }
}
