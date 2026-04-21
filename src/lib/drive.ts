import { invoke } from "./tauri";

export interface DriveCheckResult {
  existed: boolean;
  file_id: string;
  size_bytes: number;
  last_modified_ms: number;
  fingerprint: string | null;
}

export interface DriveMetadataResult {
  exists: boolean;
  file_id?: string;
  size_bytes?: number;
  last_modified_ms?: number;
  fingerprint?: string | null;
}

export interface DriveUploadResult {
  file_id: string;
  size_bytes: number;
  last_modified_ms: number;
  fingerprint: string;
}

export function machineFingerprint(): Promise<string> {
  return invoke<string>("machine_fingerprint");
}

export interface DriveDownloadResult {
  target_path: string;
  size_bytes: number;
  last_modified_ms: number;
}

function url(): string {
  const u = import.meta.env.VITE_APPS_SCRIPT_URL;
  if (!u) throw new Error("VITE_APPS_SCRIPT_URL chưa cấu hình trong .env.local");
  return u;
}

export function driveCheckOrCreate(idToken: string): Promise<DriveCheckResult> {
  return invoke<DriveCheckResult>("drive_check_or_create", {
    appsScriptUrl: url(),
    idToken,
  });
}

export function driveMetadata(idToken: string): Promise<DriveMetadataResult> {
  return invoke<DriveMetadataResult>("drive_metadata", {
    appsScriptUrl: url(),
    idToken,
  });
}

export function driveUploadDb(idToken: string): Promise<DriveUploadResult> {
  return invoke<DriveUploadResult>("drive_upload_db", {
    appsScriptUrl: url(),
    idToken,
  });
}

/// Pull remote DB → merge local-win + apply tombstones → push snapshot lên Drive.
/// Thay thế `driveUploadDb` cho flow sync v2 (cross-device safe).
export function drivePullMergePush(
  idToken: string,
): Promise<DriveUploadResult> {
  return invoke<DriveUploadResult>("drive_pull_merge_push", {
    appsScriptUrl: url(),
    idToken,
  });
}

export function driveDownloadDb(idToken: string): Promise<DriveDownloadResult> {
  return invoke<DriveDownloadResult>("drive_download_db", {
    appsScriptUrl: url(),
    idToken,
  });
}

export function driveApplyPending(): Promise<boolean> {
  return invoke<boolean>("drive_apply_pending");
}

export interface UserListFileMeta {
  fileId: string;
  sizeBytes: number;
  lastModified: number;
}

export interface UserListEntry {
  uid: string;
  email: string | null;
  localPart: string | null;
  premium: boolean;
  admin: boolean;
  expiredAt: string | null;
  createdAt: string | null;
  file: UserListFileMeta | null;
}

export function driveListUsers(idToken: string): Promise<UserListEntry[]> {
  return invoke<UserListEntry[]>("drive_list_users", {
    appsScriptUrl: url(),
    idToken,
  });
}

/// Cache singleton — user list JSON blob. FE tự parse qua `JSON.parse(users_json)`.
export interface AdminUserListCache {
  users_json: string;
  fetched_at_ms: number;
}

/// Đọc user list cache (DB local). null = chưa fetch bao giờ → FE fallback rỗng.
export function adminReadUserListCache(): Promise<AdminUserListCache | null> {
  return invoke<AdminUserListCache | null>("admin_read_user_list_cache");
}

/// Fetch user list từ AS → replace cache → trả về. Background revalidate.
export function adminFetchUserList(
  idToken: string,
): Promise<AdminUserListCache> {
  return invoke<AdminUserListCache>("admin_fetch_user_list", {
    appsScriptUrl: url(),
    idToken,
  });
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
    appsScriptUrl: url(),
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
    appsScriptUrl: url(),
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
    appsScriptUrl: url(),
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
    appsScriptUrl: url(),
    idToken,
    targetLocalPart,
  });
}

/// Snapshot info của DB admin đang xem (null = chế độ bình thường).
export interface AdminViewInfo {
  uid: string;
  email: string | null;
  local_part: string;
  db_path: string;
  size_bytes: number;
  last_modified_ms: number;
  entered_at_ms: number;
}

/// Admin-only: download DB của user target + swap connection sang read-only.
/// Mutation command sau đó sẽ fail với SQLITE_READONLY (safety net).
export function adminViewUserDb(
  idToken: string,
  targetUid: string,
  targetLocalPart: string,
  targetEmail: string | null,
): Promise<AdminViewInfo> {
  return invoke<AdminViewInfo>("admin_view_user_db", {
    appsScriptUrl: url(),
    idToken,
    targetUid,
    targetLocalPart,
    targetEmail,
  });
}

/// Exit admin-view mode — reopen DB gốc của admin (RW + PRAGMA).
export function adminExitViewUserDb(): Promise<void> {
  return invoke<void>("admin_exit_view_user_db");
}

/// Query state hiện tại (chủ yếu cho debug — FE giữ state trong context).
export function adminViewStateGet(): Promise<AdminViewInfo | null> {
  return invoke<AdminViewInfo | null>("admin_view_state_get");
}
