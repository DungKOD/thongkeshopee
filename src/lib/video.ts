import { invoke } from "./tauri";

/// Row từ local `video_logs.db` — user xem history của chính mình.
export interface VideoDownloadLog {
  id: number;
  url: string;
  downloaded_at_ms: number;
  status: string;
}

export function listVideoDownloads(
  limit: number,
  offset: number,
): Promise<VideoDownloadLog[]> {
  return invoke<VideoDownloadLog[]>("list_video_downloads", {
    limit,
    offset,
  });
}

/**
 * Log 1 lần download video vào local SQLite `video_logs.db` (qua Tauri command).
 * UPSERT ON CONFLICT(url) — cùng 1 video tải lại nhiều lần chỉ có 1 row,
 * giữ status + timestamp mới nhất. Xem `commands/video.rs::log_video_download`.
 */
export function logVideoDownload(
  videoUrl: string,
  status: "success" | "failed",
): Promise<void> {
  return invoke<void>("log_video_download", {
    url: videoUrl,
    status,
  });
}
