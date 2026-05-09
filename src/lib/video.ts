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

/// Log 1 lần download video — chỉ ghi local DB.
export function logVideoDownload(
  videoUrl: string,
  status: "success" | "failed",
): Promise<void> {
  return invoke<void>("log_video_download", {
    url: videoUrl,
    status,
  });
}
