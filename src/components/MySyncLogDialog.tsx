import { useCallback, useEffect, useMemo, useState } from "react";
import {
  syncV9LogListLocal,
  type AdminSyncLogEvent,
} from "../lib/sync_v9";
import { SyncEventCard } from "./SyncEventCard";
import {
  clearEntries,
  getEntries,
  subscribe,
  type NetLogEntry,
} from "../lib/net_log";
import { fmtBytes } from "../formulas";

interface MySyncLogDialogProps {
  isOpen: boolean;
  onClose: () => void;
}

type TabKey = "events" | "cycles" | "requests";
type UploadedFilter = "all" | "uploaded" | "pending";

const DEFAULT_LIMIT = 200;
/// Gap giữa 2 event để coi là khác cycle (milliseconds). Sync cycle thường
/// < 30s, nên 60s là buffer an toàn.
const CYCLE_GAP_MS = 60_000;

/// User variant sync log viewer — 3 tabs:
/// 1. Events: stream raw events (cards)
/// 2. Cycles: group events per sync cycle, show request count + bytes
/// 3. Requests: FE network call log (Firebase token, Tauri invokes)
export function MySyncLogDialog({ isOpen, onClose }: MySyncLogDialogProps) {
  const [tab, setTab] = useState<TabKey>("events");
  const [events, setEvents] = useState<AdminSyncLogEvent[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [kindFilter, setKindFilter] = useState<string>("all");
  const [uploadedFilter, setUploadedFilter] = useState<UploadedFilter>("all");
  const [netLog, setNetLog] = useState<NetLogEntry[]>(() => getEntries());

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const list = await syncV9LogListLocal(DEFAULT_LIMIT);
      setEvents(list);
    } catch (e) {
      setError((e as Error).message);
      setEvents(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!isOpen) return;
    void load();
  }, [isOpen, load]);

  // Subscribe net_log — realtime update khi có request mới.
  useEffect(() => {
    if (!isOpen) return;
    setNetLog([...getEntries()]);
    return subscribe(() => setNetLog([...getEntries()]));
  }, [isOpen]);

  const allKinds = useMemo(() => {
    if (!events) return [] as string[];
    return Array.from(new Set(events.map((e) => e.kind))).sort();
  }, [events]);

  const filtered = useMemo(() => {
    if (!events) return [];
    return events.filter((e) => {
      if (kindFilter !== "all" && e.kind !== kindFilter) return false;
      if (uploadedFilter === "uploaded" && !e.uploadedAt) return false;
      if (uploadedFilter === "pending" && e.uploadedAt) return false;
      return true;
    });
  }, [events, kindFilter, uploadedFilter]);

  const cycles = useMemo(() => groupIntoCycles(events ?? []), [events]);

  if (!isOpen) return null;

  const pendingCount = (events ?? []).filter((e) => !e.uploadedAt).length;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
      onClick={onClose}
    >
      <div
        className="flex h-[85vh] w-full max-w-4xl flex-col overflow-hidden rounded-xl border border-surface-8 bg-surface-1 shadow-elev-16"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-surface-8 bg-surface-2 px-5 py-3">
          <div className="min-w-0">
            <h2 className="flex items-center gap-2 text-lg font-semibold text-white">
              <span className="material-symbols-rounded text-xl text-shopee-400">
                history
              </span>
              Sync log (local)
            </h2>
            <p className="text-xs text-white/60">
              Events: {events?.length ?? 0} · Cycles: {cycles.length} ·
              Requests: {netLog.length}
              {pendingCount > 0 && (
                <span
                  className="ml-2 rounded-full bg-white/10 px-2 py-0.5 text-white/60"
                  title="Log observability metadata chưa upload lên R2. Data đã sync bình thường — log chỉ là debug trail, không ảnh hưởng DB."
                >
                  {pendingCount} log chưa backup
                </span>
              )}
            </p>
          </div>
          <button
            onClick={onClose}
            className="btn-ripple flex h-9 w-9 items-center justify-center rounded-full text-white/70 hover:bg-white/10 hover:text-white"
            title="Đóng"
          >
            <span className="material-symbols-rounded">close</span>
          </button>
        </div>

        {/* Tab nav */}
        <div className="flex border-b border-surface-8 bg-surface-2 px-3">
          <TabBtn
            active={tab === "events"}
            onClick={() => setTab("events")}
            icon="list"
            label={`Events (${events?.length ?? 0})`}
          />
          <TabBtn
            active={tab === "cycles"}
            onClick={() => setTab("cycles")}
            icon="sync"
            label={`Lịch sử cycle (${cycles.length})`}
          />
          <TabBtn
            active={tab === "requests"}
            onClick={() => setTab("requests")}
            icon="swap_vert"
            label={`Requests (${netLog.length})`}
          />
          <button
            onClick={() => void load()}
            disabled={loading}
            className="btn-ripple ml-auto flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium text-white/75 hover:bg-white/10 disabled:opacity-50"
            title="Tải lại events + reload từ DB"
          >
            <span
              className={`material-symbols-rounded text-base ${loading ? "animate-spin" : ""}`}
            >
              refresh
            </span>
            Tải lại
          </button>
        </div>

        {/* Controls per-tab */}
        {tab === "events" && (
          <div className="flex flex-wrap items-center gap-3 border-b border-surface-8 bg-surface-2 px-5 py-2 text-sm">
            <label className="flex items-center gap-1.5 text-white/75">
              Kind:
              <select
                value={kindFilter}
                onChange={(e) => setKindFilter(e.currentTarget.value)}
                className="rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-white/90"
              >
                <option value="all">Tất cả</option>
                {allKinds.map((k) => (
                  <option key={k} value={k}>
                    {k}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex items-center gap-1.5 text-white/75">
              Trạng thái:
              <select
                value={uploadedFilter}
                onChange={(e) =>
                  setUploadedFilter(e.currentTarget.value as UploadedFilter)
                }
                className="rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-white/90"
              >
                <option value="all">Tất cả</option>
                <option value="pending">Chưa flush R2</option>
                <option value="uploaded">Đã flush R2</option>
              </select>
            </label>
            <span className="ml-auto text-white/45">
              {filtered.length} / {events?.length ?? 0} hiển thị
            </span>
          </div>
        )}

        {tab === "requests" && (
          <div className="flex items-center gap-3 border-b border-surface-8 bg-surface-2 px-5 py-2 text-sm">
            <span className="text-white/55">
              Log mọi HTTP request FE gọi ra (Firebase + Tauri). In-memory,
              xóa khi reload app.
            </span>
            <button
              onClick={() => clearEntries()}
              className="btn-ripple ml-auto flex items-center gap-1 rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-xs text-white/70 hover:bg-surface-4"
            >
              <span className="material-symbols-rounded text-sm">delete</span>
              Xóa log
            </button>
          </div>
        )}

        <div className="flex-1 overflow-y-auto">
          {error ? (
            <div className="m-5 rounded-lg border border-red-500/40 bg-red-900/20 p-4 text-sm text-red-200">
              {error}
            </div>
          ) : loading && !events ? (
            <div className="flex items-center justify-center py-16 text-white/50">
              <span className="material-symbols-rounded animate-spin text-3xl">
                sync
              </span>
            </div>
          ) : tab === "events" ? (
            filtered.length === 0 ? (
              <EmptyState
                text={
                  events && events.length > 0
                    ? "Không có event nào khớp filter"
                    : "Chưa có event nào — thử sync 1 lần"
                }
              />
            ) : (
              <ul className="space-y-2 p-4">
                {filtered.map((e) => (
                  <SyncEventCard key={e.eventId} event={e} />
                ))}
              </ul>
            )
          ) : tab === "cycles" ? (
            cycles.length === 0 ? (
              <EmptyState text="Chưa có sync cycle nào" />
            ) : (
              <ul className="space-y-2 p-4">
                {cycles.map((c) => (
                  <CycleCard key={c.id} cycle={c} />
                ))}
              </ul>
            )
          ) : netLog.length === 0 ? (
            <EmptyState text="Chưa có request nào trong session này" />
          ) : (
            <ul className="divide-y divide-surface-8">
              {netLog.map((r) => (
                <NetLogRow key={r.id} entry={r} />
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}

// =============================================================
// Cycle grouping
// =============================================================

interface SyncCycle {
  id: string;
  startTs: string;
  endTs: string;
  durationMs: number;
  eventCount: number;
  /// HTTP requests (push_upload + pull_fetch + compaction upload +
  /// bootstrap snapshot + push_start — mỗi event = 1 network call).
  requestCount: number;
  uploadBytes: number;
  downloadBytes: number;
  tablesTouched: Set<string>;
  hasError: boolean;
  firstEvent: AdminSyncLogEvent;
  events: AdminSyncLogEvent[];
}

/// Cluster events theo time proximity — gap > CYCLE_GAP_MS → cycle mới.
/// Events DESC (mới → cũ), ta walk và chia cluster.
function groupIntoCycles(events: AdminSyncLogEvent[]): SyncCycle[] {
  if (events.length === 0) return [];
  const sorted = [...events].sort((a, b) =>
    a.ts < b.ts ? 1 : a.ts > b.ts ? -1 : 0,
  );
  const cycles: SyncCycle[] = [];
  let current: AdminSyncLogEvent[] = [];
  let lastTime = Number.POSITIVE_INFINITY;

  for (const e of sorted) {
    const t = new Date(e.ts).getTime();
    if (lastTime - t > CYCLE_GAP_MS && current.length > 0) {
      cycles.push(buildCycle(current));
      current = [];
    }
    current.push(e);
    lastTime = t;
  }
  if (current.length > 0) cycles.push(buildCycle(current));
  return cycles;
}

function buildCycle(events: AdminSyncLogEvent[]): SyncCycle {
  // events here are DESC (newest first within cycle).
  const first = events[events.length - 1]; // oldest trong cycle = start
  const last = events[0]; // newest = end
  let uploadBytes = 0;
  let downloadBytes = 0;
  let requestCount = 0;
  const tables = new Set<string>();
  let hasError = false;

  for (const e of events) {
    const ctx = (e.ctx ?? {}) as Record<string, unknown>;
    const bytes = Number(ctx.bytes ?? 0);
    const table = typeof ctx.table === "string" ? ctx.table : null;
    if (table) tables.add(table);

    switch (e.kind) {
      case "push_upload":
        uploadBytes += bytes;
        requestCount += 1;
        break;
      case "pull_fetch":
        downloadBytes += bytes;
        requestCount += 1;
        break;
      case "bootstrap_snapshot":
      case "compaction_complete":
        requestCount += 1;
        break;
      case "error":
        hasError = true;
        break;
    }
  }
  const startMs = new Date(first.ts).getTime();
  const endMs = new Date(last.ts).getTime();
  return {
    id: `${first.eventId}-${last.eventId}`,
    startTs: first.ts,
    endTs: last.ts,
    durationMs: Math.max(0, endMs - startMs),
    eventCount: events.length,
    requestCount,
    uploadBytes,
    downloadBytes,
    tablesTouched: tables,
    hasError,
    firstEvent: first,
    events,
  };
}

function CycleCard({ cycle }: { cycle: SyncCycle }) {
  const [expanded, setExpanded] = useState(false);
  const start = new Date(cycle.startTs);
  const dur =
    cycle.durationMs < 1000
      ? `${cycle.durationMs}ms`
      : `${(cycle.durationMs / 1000).toFixed(1)}s`;

  return (
    <li
      className={`rounded-lg border border-surface-8 border-l-[4px] bg-surface-2 ${
        cycle.hasError ? "border-l-red-500" : "border-l-shopee-500"
      }`}
    >
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        className="flex w-full items-center gap-3 px-4 py-3 text-left hover:bg-surface-4/40"
      >
        <span
          className={`material-symbols-rounded text-xl transition-transform ${
            expanded ? "rotate-90" : ""
          } ${cycle.hasError ? "text-red-300" : "text-shopee-300"}`}
        >
          chevron_right
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2 text-base text-white/90">
            <span className="font-semibold">
              {start.toLocaleString("vi-VN", {
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
                day: "2-digit",
                month: "2-digit",
              })}
            </span>
            <span className="text-white/50">· {dur}</span>
            {cycle.hasError && (
              <span className="rounded bg-red-900/40 px-2 py-0.5 text-[13px] text-red-200">
                lỗi
              </span>
            )}
          </div>
          <div className="mt-1 flex flex-wrap items-center gap-x-4 gap-y-1 text-[15px]">
            <span className="flex items-center gap-1 text-blue-200">
              <span className="material-symbols-rounded text-base">
                swap_horiz
              </span>
              {cycle.requestCount} request
            </span>
            <span className="flex items-center gap-1 text-shopee-200">
              <span className="material-symbols-rounded text-base">
                arrow_upward
              </span>
              {fmtBytes(cycle.uploadBytes)}
            </span>
            <span className="flex items-center gap-1 text-blue-200">
              <span className="material-symbols-rounded text-base">
                arrow_downward
              </span>
              {fmtBytes(cycle.downloadBytes)}
            </span>
            <span className="text-white/50">
              {cycle.eventCount} events · {cycle.tablesTouched.size} table
            </span>
          </div>
        </div>
      </button>
      {expanded && (
        <div className="border-t border-surface-8 bg-surface-0/50 p-3">
          <ul className="space-y-1.5">
            {cycle.events.map((e) => (
              <SyncEventCard key={e.eventId} event={e} />
            ))}
          </ul>
        </div>
      )}
    </li>
  );
}

// =============================================================
// Net log row
// =============================================================

function NetLogRow({ entry }: { entry: NetLogEntry }) {
  const ts = new Date(entry.tsStart);
  const kindClass = netKindClass(entry.kind);

  return (
    <li className="flex items-start gap-3 bg-surface-2/40 px-4 py-2.5 text-[15px] hover:bg-surface-2">
      <span
        className={`mt-0.5 inline-flex items-center gap-1 rounded-md px-2 py-0.5 text-[13px] font-medium ${kindClass}`}
      >
        <span className="material-symbols-rounded text-sm">
          {netKindIcon(entry.kind)}
        </span>
        {netKindLabel(entry.kind)}
      </span>
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-baseline gap-2">
          <span className="font-mono text-white/90">{entry.label}</span>
          {entry.ok ? (
            <span className="text-[13px] text-green-300">✓</span>
          ) : (
            <span className="text-[13px] text-red-300">✗ {entry.error}</span>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-x-3 gap-y-0.5 text-[13px] text-white/50">
          <span>
            {ts.toLocaleTimeString("vi-VN", {
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            })}
          </span>
          <span>{entry.durationMs}ms</span>
          {entry.bytes !== undefined && <span>{fmtBytes(entry.bytes)}</span>}
          {entry.meta &&
            Object.entries(entry.meta).map(([k, v]) => (
              <span key={k} className="font-mono">
                {k}={v}
              </span>
            ))}
        </div>
      </div>
    </li>
  );
}

function netKindLabel(k: NetLogEntry["kind"]): string {
  switch (k) {
    case "firebase_token":
      return "Firebase token";
    case "firebase_signin":
      return "Firebase signin";
    case "firebase_signout":
      return "Firebase signout";
    case "firebase_other":
      return "Firebase";
    case "tauri_sync_push":
      return "Sync push";
    case "tauri_sync_pull":
      return "Sync pull";
    case "tauri_sync_all":
      return "Sync all";
    case "tauri_sync_log_flush":
      return "Log flush";
    case "tauri_sync_compact":
      return "Compact";
    case "tauri_admin":
      return "Admin API";
    case "apps_script":
      return "Apps Script";
    default:
      return "Other";
  }
}

function netKindIcon(k: NetLogEntry["kind"]): string {
  if (k.startsWith("firebase")) return "local_fire_department";
  if (k === "tauri_sync_push") return "arrow_upward";
  if (k === "tauri_sync_pull") return "arrow_downward";
  if (k === "tauri_sync_all") return "sync";
  if (k === "tauri_sync_compact") return "compress";
  if (k === "tauri_sync_log_flush") return "upload";
  if (k === "tauri_admin") return "admin_panel_settings";
  if (k === "apps_script") return "description";
  return "language";
}

function netKindClass(k: NetLogEntry["kind"]): string {
  if (k.startsWith("firebase")) return "bg-orange-900/40 text-orange-200";
  if (k === "tauri_sync_push" || k === "tauri_sync_log_flush")
    return "bg-shopee-900/40 text-shopee-200";
  if (k === "tauri_sync_pull") return "bg-blue-900/40 text-blue-200";
  if (k === "tauri_sync_all") return "bg-purple-900/40 text-purple-200";
  if (k === "tauri_sync_compact") return "bg-amber-900/40 text-amber-200";
  if (k === "tauri_admin") return "bg-red-900/30 text-red-200";
  return "bg-surface-6 text-white/75";
}

// =============================================================
// Helpers
// =============================================================

function TabBtn({
  active,
  onClick,
  icon,
  label,
}: {
  active: boolean;
  onClick: () => void;
  icon: string;
  label: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`flex items-center gap-1.5 border-b-2 px-4 py-2 text-sm font-medium transition-colors ${
        active
          ? "border-shopee-500 text-shopee-300"
          : "border-transparent text-white/60 hover:text-white/85"
      }`}
    >
      <span className="material-symbols-rounded text-base">{icon}</span>
      {label}
    </button>
  );
}

function EmptyState({ text }: { text: string }) {
  return (
    <div className="flex flex-col items-center gap-2 py-16 text-white/50">
      <span className="material-symbols-rounded text-5xl text-white/30">
        event_busy
      </span>
      <p className="text-sm">{text}</p>
    </div>
  );
}

