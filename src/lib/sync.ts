import { invoke } from "./tauri";
import { timed } from "./net_log";

// ==========================================================================
// Admin wrappers — user list + admin-view của user khác.
//
// Sync DB chính đã chuyển sang v9 (`lib/sync_v9.ts`). File này giữ admin
// helpers gọi Tauri commands ở `commands::app_util` (moved from v8 sync.rs
// trong P8b) và `commands::admin_view`.
// ==========================================================================

function syncApiUrl(): string {
  const u = import.meta.env.VITE_SYNC_API_URL;
  if (!u) throw new Error("VITE_SYNC_API_URL chưa cấu hình trong .env.local");
  return u;
}

/// Sync metadata cho 1 user trên R2, derive từ manifest.json + snapshots/.
/// null khi user chưa sync lần nào.
export interface UserListSyncMeta {
  hasManifest: boolean;
  hasSnapshot: boolean;
  lastModifiedMs: number | null;
}

export interface UserListEntry {
  uid: string;
  email: string | null;
  localPart: string | null;
  premium: boolean;
  admin: boolean;
  expiredAt: string | null;
  createdAt: string | null;
  sync: UserListSyncMeta | null;
}

/** Admin list users (v9) — Worker `/v9/admin/users`. */
export function adminListUsers(idToken: string): Promise<UserListEntry[]> {
  return timed("tauri_admin", "admin_list_users", () =>
    invoke<UserListEntry[]>("admin_list_users", {
      syncApiUrl: syncApiUrl(),
      idToken,
    }),
  );
}

/// Cache singleton — user list JSON blob. FE tự parse qua `JSON.parse(users_json)`.
export interface AdminUserListCache {
  users_json: string;
  fetched_at_ms: number;
}

/// Đọc user list cache (DB local). null = chưa fetch bao giờ → FE fallback rỗng.
/// Require admin claim (defense-in-depth; primary auth ở Worker khi fetch).
export function adminReadUserListCache(
  idToken: string,
): Promise<AdminUserListCache | null> {
  return invoke<AdminUserListCache | null>("admin_read_user_list_cache", {
    idToken,
  });
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
