/// In-memory ring buffer log mọi request mà FE phát ra (Firebase Auth,
/// Tauri invoke hit network, Apps Script, v.v.). Non-persistent in-memory,
/// xoá khi reload app. Dùng debug network activity + UI Sync Log dialog.
///
/// **Persistent log** parallel: mỗi entry cũng được fire-and-forget gửi
/// xuống Rust qua `app_log_request` Tauri command → append vào file daily
/// `{app_data}/net_log/YYYY-MM-DD.log`. User mở folder qua Settings để
/// xem/grep/phân tích sau (không bị xoá khi reload app).
///
/// Event từ Rust sync layer (push_upload, pull_fetch) KHÔNG log ở đây vì
/// đã persist trong `sync_event_log` DB. net_log bổ sung các call không đi
/// qua Rust (Firebase token refresh, FE-triggered HTTP).

export type NetKind =
  | "firebase_token"
  | "firebase_signin"
  | "firebase_signout"
  | "firebase_other"
  | "tauri_sync_push"
  | "tauri_sync_pull"
  | "tauri_sync_all"
  | "tauri_sync_log_flush"
  | "tauri_sync_compact"
  | "tauri_admin"
  | "apps_script"
  | "other";

export interface NetLogEntry {
  id: number;
  /// ISO timestamp khi bắt đầu request.
  tsStart: string;
  /// ms từ start đến end.
  durationMs: number;
  kind: NetKind;
  /// Tên request (endpoint/command/function). Vd "sync_v9_push_all",
  /// "getIdToken", "admin_fetch_user_list".
  label: string;
  /// true = thành công. false = error/timeout.
  ok: boolean;
  /// Error message nếu ok=false.
  error?: string;
  /// Bytes uploaded/downloaded nếu caller biết (optional).
  bytes?: number;
  /// Extra context — string key/value (kept small, não-PII).
  meta?: Record<string, string | number>;
}

const MAX_ENTRIES = 500;
let entries: NetLogEntry[] = [];
let nextId = 1;
const subscribers = new Set<() => void>();

function notify(): void {
  for (const fn of subscribers) {
    try {
      fn();
    } catch {
      // ignore
    }
  }
}

export function logRequest(
  entry: Omit<NetLogEntry, "id">,
): NetLogEntry {
  const full: NetLogEntry = { id: nextId++, ...entry };
  entries.unshift(full); // newest first
  if (entries.length > MAX_ENTRIES) entries.length = MAX_ENTRIES;
  notify();
  // Fire-and-forget persist xuống file. Không await — không slow user flow.
  // Lỗi I/O chỉ console.warn, không break.
  void persistToFile(full);
  return full;
}

/// Lazy-load Tauri invoke để tránh circular import + cho phép unit test
/// trong môi trường non-Tauri.
async function persistToFile(entry: NetLogEntry): Promise<void> {
  try {
    const { invoke } = await import("./tauri");
    await invoke("app_log_request", {
      payload: {
        tsStart: entry.tsStart,
        durationMs: entry.durationMs,
        kind: entry.kind,
        label: entry.label,
        ok: entry.ok,
        error: entry.error ?? null,
        bytes: entry.bytes ?? null,
        meta: entry.meta ?? null,
      },
    });
  } catch (e) {
    // Chỉ console — không break UX nếu file write fail (disk full / perm).
    console.warn("[net_log] persist failed:", e);
  }
}

export function getEntries(): NetLogEntry[] {
  return entries;
}

export function clearEntries(): void {
  entries = [];
  notify();
}

export function subscribe(fn: () => void): () => void {
  subscribers.add(fn);
  return () => subscribers.delete(fn);
}

/// Helper wrap async fn — tự time + log success/error.
export async function timed<T>(
  kind: NetKind,
  label: string,
  fn: () => Promise<T>,
  meta?: Record<string, string | number>,
): Promise<T> {
  const start = performance.now();
  const tsStart = new Date().toISOString();
  try {
    const result = await fn();
    logRequest({
      tsStart,
      durationMs: Math.round(performance.now() - start),
      kind,
      label,
      ok: true,
      meta,
    });
    return result;
  } catch (e) {
    logRequest({
      tsStart,
      durationMs: Math.round(performance.now() - start),
      kind,
      label,
      ok: false,
      error: (e as Error).message ?? String(e),
      meta,
    });
    throw e;
  }
}
