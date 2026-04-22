import { invoke } from "./tauri";

// ==========================================================================
// Video log wrappers — Google Sheet qua Apps Script.
//
// DB sync đã migrate sang Worker/R2 (`lib/sync.ts`). Video giữ Apps Script
// vì không vướng giới hạn 50MB (mỗi row ~100 bytes, user không đạt tới trần).
// ==========================================================================

function appsScriptUrl(): string {
  const u = import.meta.env.VITE_APPS_SCRIPT_URL;
  if (!u) throw new Error("VITE_APPS_SCRIPT_URL chưa cấu hình trong .env.local");
  return u;
}

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

/// Row từ Google Sheet — admin xem log của user khác.
/// `timestamp` đã format sẵn ở BE theo local time (HH:MM:SS DD/MM/YYYY).
/// `status` đã tiếng Việt từ Apps Script: "thành công" | "thất bại".
export interface VideoLogRow {
  timestamp: string;
  url: string;
  status: string;
}

/// Log 1 lần download video: BE ghi local DB + best-effort post Sheet.
export function logVideoDownload(
  idToken: string,
  videoUrl: string,
  status: "success" | "failed",
): Promise<void> {
  return invoke<void>("log_video_download", {
    appsScriptUrl: appsScriptUrl(),
    idToken,
    url: videoUrl,
    status,
  });
}

/// Admin-only: fetch TOÀN BỘ sheet của target → replace cache DB local.
/// Trả về số row đã cache. Dùng khi "Tải lại" hoặc lần đầu xem user.
export function adminFetchUserLogSheet(
  idToken: string,
  targetLocalPart: string,
): Promise<number> {
  return invoke<number>("admin_fetch_user_log_sheet", {
    appsScriptUrl: appsScriptUrl(),
    idToken,
    targetLocalPart,
  });
}

/// Đọc cache rows (DB local) cho 1 user. Infinite scroll paginate qua hàm này.
export function adminReadUserLogCache(
  targetLocalPart: string,
  limit: number,
  offset: number,
): Promise<VideoLogRow[]> {
  return invoke<VideoLogRow[]>("admin_read_user_log_cache", {
    targetLocalPart,
    limit,
    offset,
  });
}

export interface AdminFetchMeta {
  fetched_at_ms: number;
  row_count: number;
}

/// Metadata fetch gần nhất (null = chưa fetch bao giờ).
export function adminUserLogFetchMeta(
  targetLocalPart: string,
): Promise<AdminFetchMeta | null> {
  return invoke<AdminFetchMeta | null>("admin_user_log_fetch_meta", {
    targetLocalPart,
  });
}

/// Admin xóa 1 row ở Sheet + local cache. Match qua tuple (timestamp, url, status).
export function adminDeleteUserLogRow(
  idToken: string,
  targetLocalPart: string,
  timestamp: string,
  videoUrl: string,
  status: string,
): Promise<void> {
  return invoke<void>("admin_delete_user_log_row", {
    appsScriptUrl: appsScriptUrl(),
    idToken,
    targetLocalPart,
    timestamp,
    url: videoUrl,
    status,
  });
}

/// Admin xóa toàn bộ sheet tab + clear cache.
export function adminDeleteUserLogSheet(
  idToken: string,
  targetLocalPart: string,
): Promise<void> {
  return invoke<void>("admin_delete_user_log_sheet", {
    appsScriptUrl: appsScriptUrl(),
    idToken,
    targetLocalPart,
  });
}
