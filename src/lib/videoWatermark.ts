import { invoke } from "./tauri";

/// Tham số watermark — phải khớp `WatermarkOptions` ở `video_watermark.rs`.
export interface VideoWatermarkOptions {
  sizePct: number;
  opacity: number;
  paddingPct: number;
  /// Anti-theft: logo nhảy 4 góc mỗi 5s. Default false.
  antiTheft: boolean;
}

/// Stage event payload — UI hiển thị message thân thiện theo stage.
/// Stages:
///   "preparing" | "downloading_ffmpeg" | "fetching_logo"
///   | "probing" | "encoding" | "done"
export interface WatermarkStageEvent {
  watermarkId: string;
  stage:
    | "preparing"
    | "downloading_ffmpeg"
    | "fetching_logo"
    | "probing"
    | "encoding"
    | "done";
  message: string;
}

export interface WatermarkProgressEvent {
  watermarkId: string;
  /// 0..=100
  percent: number;
}

/// Apply logo Page lên video, ghi đè file gốc. Caller chịu trách nhiệm:
///   - Truyền `watermarkId` unique để match progress events.
///   - Subscribe `watermark-progress` + `watermark-stage` qua `listen()`.
///   - Catch lỗi: ffmpeg không tải được, Page chưa lưu, file lock, v.v.
export function applyVideoWatermark(
  videoPath: string,
  pageId: string,
  options: VideoWatermarkOptions,
  watermarkId: string,
): Promise<void> {
  return invoke<void>("apply_video_watermark", {
    videoPath,
    pageId,
    options,
    watermarkId,
  });
}

/// Xóa cache logo cho 1 page — gọi khi user đổi avatar Page và muốn refresh.
export function clearPageLogoCache(pageId: string): Promise<void> {
  return invoke<void>("clear_page_logo_cache", { pageId });
}
