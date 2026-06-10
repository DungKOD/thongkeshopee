import { invoke } from "./tauri";
import { callAppsScript, formatVnTimestamp } from "./appsScript";

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
 * Log 1 lần download video — ghi 2 nơi:
 * 1. Local SQLite `video_logs.db` (qua Tauri command) — bắt buộc thành công,
 *    nếu fail thì throw để caller biết.
 * 2. Google Sheet (qua Apps Script `logVideoDownload` action) — best-effort,
 *    sync theo tab tên = email local-part. Lỗi mạng/auth chỉ log warning,
 *    KHÔNG throw — để 1 lần lỗi Sheet không khiến user thấy "tải fail" trong
 *    khi file đã có trên đĩa.
 *
 * Apps Script upsert theo URL: cùng 1 video tải lại nhiều lần chỉ có 1 row
 * (giữ status + timestamp mới nhất). Đồng nhất với behavior local DB
 * (UPSERT ON CONFLICT(url) — xem `commands/video.rs::log_video_download`).
 */
export async function logVideoDownload(
  videoUrl: string,
  status: "success" | "failed",
): Promise<void> {
  await invoke<void>("log_video_download", {
    url: videoUrl,
    status,
  });
  try {
    await callAppsScript("logVideoDownload", {
      videoUrl,
      videoStatus: status,
      videoTimestamp: formatVnTimestamp(new Date()),
    });
  } catch (e) {
    console.warn("[video] sync log lên Google Sheet thất bại:", e);
  }
}
