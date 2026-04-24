import { invoke } from "./tauri";
import { timed } from "./net_log";

// ==========================================================================
// Sync v9 wrappers — per-table delta sync. Gọi 5 Tauri commands đã register
// ở `commands/sync_v9_cmds.rs`.
//
// Backend tự giữ cursor + manifest state trong SQLite. FE chỉ pass base URL
// + Firebase ID token. Result DTO đã camelCase hóa (Rust serde
// rename_all = "camelCase").
// ==========================================================================

export interface SyncV9State {
  /// True = máy này đang giữa bootstrap (fresh install + snapshot restore
  /// chưa xong). Push sẽ bị guard bypass ở Rust. UI hiển thị BootstrapSplash.
  freshInstallPending: boolean;
  /// Clock ms của manifest đã pull cuối cùng. 0 = chưa pull gì.
  lastPulledManifestClockMs: number;
  /// R2 key snapshot mới nhất đã biết (từ manifest). Null khi chưa có snapshot.
  lastSnapshotKey: string | null;
  /// Clock ms tại thời điểm tạo snapshot.
  lastSnapshotClockMs: number;
  /// List table có row chưa push (rowid/cursor > last_uploaded_cursor).
  /// Empty = DB clean, không có gì để sync.
  pendingPushTables: string[];
  /// Số sync_event_log entries chưa flush lên R2 (observability log, non-critical).
  /// FE dùng quyết định trigger flush nếu > threshold.
  pendingLogCount: number;
}

export interface PushReport {
  uploadedCount: number;
  skippedIdentical: number;
  totalBytes: number;
  casRetries: number;
}

export interface PullReport {
  appliedDeltas: number;
  totalEvents: number;
  skipped: number;
  skippedByHlc: number;
  tombstonesApplied: number;
  rowsDeleted: number;
  /// Tổng bytes fetched từ R2 (compressed). UI hiển thị download volume.
  totalBytes: number;
}

export interface SyncReport {
  pull: PullReport;
  push: PushReport;
}

export interface CompactionReport {
  triggered: boolean;
  snapshotKey: string | null;
  snapshotSizeBytes: number;
  deltasCleared: number;
  casRetries: number;
}

function syncApiUrl(): string {
  const u = import.meta.env.VITE_SYNC_API_URL;
  if (!u) throw new Error("VITE_SYNC_API_URL chưa cấu hình trong .env.local");
  return u;
}

/// Guard cho mọi lệnh network. Check `navigator.onLine` trước khi invoke.
/// Offline → throw Error với message rõ ràng (tiếng Việt) để UI hiển thị.
/// `timed()` wrapper log lại failure vào sync_event_log local cho user trace.
function requireOnline(opName: string): void {
  if (!navigator.onLine) {
    throw new Error(`${opName}: không có kết nối mạng — vui lòng kiểm tra internet`);
  }
}

/// Snapshot UI state. Cheap — chỉ read local DB, không HTTP.
export function syncV9GetState(): Promise<SyncV9State> {
  return invoke<SyncV9State>("sync_v9_get_state");
}

/// Upload pending deltas + update manifest. CAS retry max 3 trong Rust.
/// Skip-identical hash check bỏ qua upload khi table content không đổi.
export function syncV9PushAll(idToken: string): Promise<PushReport> {
  return timed("tauri_sync_push", "sync_v9_push_all", async () => {
    requireOnline("Đẩy R2");
    const r = await invoke<PushReport>("sync_v9_push_all", {
      baseUrl: syncApiUrl(),
      idToken,
    });
    return r;
  });
}

/// Fetch manifest → diff → apply deltas chưa pull. Mỗi file 1 TX (rollback
/// on error). HLC absorb từ max event clock → HLC monotonic cross-machine.
export function syncV9PullAll(idToken: string): Promise<PullReport> {
  return timed("tauri_sync_pull", "sync_v9_pull_all", async () => {
    requireOnline("Kéo R2");
    const r = await invoke<PullReport>("sync_v9_pull_all", {
      baseUrl: syncApiUrl(),
      idToken,
    });
    return r;
  });
}

/// Pull rồi push. Thứ tự này giảm CAS conflict khi 2 máy cùng sync.
export function syncV9SyncAll(idToken: string): Promise<SyncReport> {
  return timed("tauri_sync_all", "sync_v9_sync_all", async () => {
    requireOnline("Đồng bộ R2");
    const r = await invoke<SyncReport>("sync_v9_sync_all", {
      baseUrl: syncApiUrl(),
      idToken,
    });
    return r;
  });
}

/// Flush pending events trong `sync_event_log` lên R2. Group by date,
/// zstd + NDJSON. Batch max 500 events/call. Return số events đã upload.
export function syncV9LogFlush(idToken: string): Promise<number> {
  return timed("tauri_sync_log_flush", "sync_v9_log_flush", async () => {
    requireOnline("Flush log");
    const r = await invoke<number>("sync_v9_log_flush", {
      baseUrl: syncApiUrl(),
      idToken,
    });
    return r;
  });
}

/// P10 compaction — snapshot local DB + upload + clear manifest.deltas.
/// Auto-trigger khi manifest.deltas > threshold. No-op nếu chưa cần.
export function syncV9CompactIfNeeded(idToken: string): Promise<CompactionReport> {
  return timed("tauri_sync_compact", "sync_v9_compact_if_needed", async () => {
    requireOnline("Compact R2");
    const r = await invoke<CompactionReport>("sync_v9_compact_if_needed", {
      baseUrl: syncApiUrl(),
      idToken,
    });
    return r;
  });
}

// ==========================================================================
// LOCAL sync log (user viewer — đọc DB local, không HTTP)
// ==========================================================================

/// Đọc local `sync_event_log`. Events trả DESC theo event_id (mới → cũ).
/// `kindFilter` exact match (vd `push_upload`) hoặc bỏ = all kinds.
export function syncV9LogListLocal(
  limit: number,
  kindFilter?: string | null,
): Promise<AdminSyncLogEvent[]> {
  return invoke<AdminSyncLogEvent[]>("sync_v9_log_list_local", {
    limit,
    kindFilter: kindFilter ?? null,
  });
}

// ==========================================================================
// ADMIN — sync log viewer (admin-only)
// ==========================================================================

export interface AdminSyncLogFile {
  key: string;
  date: string;
  sizeBytes: number;
  uploadedAt: string;
}

export interface AdminSyncLogList {
  files: AdminSyncLogFile[];
  /// True khi Worker hit MAX_EVENTS (500) → FE phải narrow date range
  /// hoặc paginate bằng date chunks.
  truncated: boolean;
}

export interface AdminSyncLogEvent {
  eventId: number;
  ts: string;
  fingerprint: string;
  kind: string;
  /// Raw JSON context theo variant. FE render tùy kind (ví dụ: push_upload
  /// hiển thị table/cursor/bytes, pull_fetch hiển thị file key).
  ctx: unknown;
  uploadedAt: string | null;
}

/// List file metadata sync log của target user trong date range.
/// `fromDate`/`toDate`: `YYYY-MM-DD`. Worker cap 500 files/response.
export function adminV9SyncLogList(
  idToken: string,
  targetUid: string,
  fromDate: string,
  toDate: string,
): Promise<AdminSyncLogList> {
  return invoke<AdminSyncLogList>("admin_v9_sync_log_list", {
    baseUrl: syncApiUrl(),
    idToken,
    targetUid,
    fromDate,
    toDate,
  });
}

/// Fetch + decompress 1 file NDJSON + parse events. `key` phải khớp
/// `users/{uid}/sync_logs/...` (Worker validate).
export function adminV9SyncLogFetchEvents(
  idToken: string,
  key: string,
): Promise<AdminSyncLogEvent[]> {
  return invoke<AdminSyncLogEvent[]>("admin_v9_sync_log_fetch_events", {
    baseUrl: syncApiUrl(),
    idToken,
    key,
  });
}
