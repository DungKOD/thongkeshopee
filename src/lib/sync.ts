import { invoke } from "./tauri";

// ==========================================================================
// DB sync wrappers — gọi Cloudflare Worker (`/metadata`, `/upload`,
// `/download`) qua Rust client `commands::sync_client`.
//
// Video log Google Sheet vẫn dùng `lib/video.ts` (Apps Script) — 2 world tách
// bạch để rollback independent.
// ==========================================================================

export interface SyncMetadataResult {
  exists: boolean;
  file_id?: string | null;
  size_bytes?: number | null;
  last_modified_ms?: number | null;
  fingerprint?: string | null;
}

export interface SyncUploadResult {
  file_id: string;
  size_bytes: number;
  last_modified_ms: number;
  fingerprint: string;
}

export interface SyncDownloadResult {
  target_path: string;
  size_bytes: number;
  last_modified_ms: number;
}

function syncApiUrl(): string {
  const u = import.meta.env.VITE_SYNC_API_URL;
  if (!u) throw new Error("VITE_SYNC_API_URL chưa cấu hình trong .env.local");
  return u;
}

export function machineFingerprint(): Promise<string> {
  return invoke<string>("machine_fingerprint");
}

export function syncMetadata(idToken: string): Promise<SyncMetadataResult> {
  return invoke<SyncMetadataResult>("sync_metadata", {
    syncApiUrl: syncApiUrl(),
    idToken,
  });
}

/** Upload local DB lên R2. `remoteExists` (từ syncMetadata) dùng cho guard
 *  server-side: reject nếu local fresh (change_id=0) + remote đã có data →
 *  tránh đè mất backup cũ khi reinstall. */
export function syncUploadDb(
  idToken: string,
  remoteExists: boolean,
): Promise<SyncUploadResult> {
  return invoke<SyncUploadResult>("sync_upload_db", {
    syncApiUrl: syncApiUrl(),
    idToken,
    remoteExists,
  });
}

/** Pull remote DB → merge local-win + apply tombstones → push snapshot lên R2.
 *  Cross-device safe — thay cho syncUploadDb khi remote có mutation từ máy khác. */
export function syncPullMergePush(idToken: string): Promise<SyncUploadResult> {
  return invoke<SyncUploadResult>("sync_pull_merge_push", {
    syncApiUrl: syncApiUrl(),
    idToken,
  });
}

export function syncDownloadDb(idToken: string): Promise<SyncDownloadResult> {
  return invoke<SyncDownloadResult>("sync_download_db", {
    syncApiUrl: syncApiUrl(),
    idToken,
  });
}

export function syncApplyPending(): Promise<boolean> {
  return invoke<boolean>("sync_apply_pending");
}

// ==========================================================================
// Admin — user list + view DB user khác.
// ==========================================================================

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

/** Direct one-shot call — admin list users from Worker `/admin/users`. */
export function adminListUsers(idToken: string): Promise<UserListEntry[]> {
  return invoke<UserListEntry[]>("admin_list_users", {
    syncApiUrl: syncApiUrl(),
    idToken,
  });
}

/// Worker xóa R2 orphan files (UIDs không có Firestore doc). Return list UIDs
/// đã xóa. Use case: đổi Firebase project → dọn file cũ khỏi bucket.
export function adminCleanupOrphans(idToken: string): Promise<string[]> {
  return invoke<string[]>("admin_cleanup_orphans", {
    syncApiUrl: syncApiUrl(),
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

/// Fetch user list từ Worker → replace cache → trả về. Background revalidate.
export function adminFetchUserList(
  idToken: string,
): Promise<AdminUserListCache> {
  return invoke<AdminUserListCache>("admin_fetch_user_list", {
    syncApiUrl: syncApiUrl(),
    idToken,
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
    syncApiUrl: syncApiUrl(),
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
