import { useCallback, useEffect, useMemo, useState } from "react";
import { auth } from "../lib/firebase";
import {
  adminV9SyncLogFetchEvents,
  adminV9SyncLogList,
  type AdminSyncLogEvent,
  type AdminSyncLogFile,
} from "../lib/sync_v9";
import { SyncEventCard } from "./SyncEventCard";
import { fmtBytes } from "../formulas";

interface SyncLogViewerDialogProps {
  isOpen: boolean;
  onClose: () => void;
  targetUid: string;
  targetEmail: string | null;
}

/// Default range = 7 ngày gần nhất. Worker cap 500 files/response, range
/// hẹp giúp không bị truncate.
function defaultRange(): { from: string; to: string } {
  const today = new Date();
  const to = today.toISOString().slice(0, 10);
  const d = new Date(today);
  d.setDate(d.getDate() - 6);
  const from = d.toISOString().slice(0, 10);
  return { from, to };
}

interface FileEntry extends AdminSyncLogFile {
  expanded: boolean;
  events: AdminSyncLogEvent[] | null;
  eventsLoading: boolean;
  eventsError: string | null;
}

export function SyncLogViewerDialog({
  isOpen,
  onClose,
  targetUid,
  targetEmail,
}: SyncLogViewerDialogProps) {
  const initialRange = useMemo(defaultRange, []);
  const [fromDate, setFromDate] = useState(initialRange.from);
  const [toDate, setToDate] = useState(initialRange.to);
  const [files, setFiles] = useState<FileEntry[] | null>(null);
  const [truncated, setTruncated] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [kindFilter, setKindFilter] = useState<string>("all");

  const load = useCallback(async () => {
    const current = auth.currentUser;
    if (!current) {
      setError("Chưa đăng nhập");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const idToken = await current.getIdToken(false);
      const list = await adminV9SyncLogList(idToken, targetUid, fromDate, toDate);
      // Sort desc theo date + uploadedAt (mới nhất trước).
      const sorted = [...list.files].sort((a, b) => {
        if (a.date !== b.date) return b.date.localeCompare(a.date);
        return b.uploadedAt.localeCompare(a.uploadedAt);
      });
      setFiles(
        sorted.map((f) => ({
          ...f,
          expanded: false,
          events: null,
          eventsLoading: false,
          eventsError: null,
        })),
      );
      setTruncated(list.truncated);
    } catch (e) {
      setError((e as Error).message);
      setFiles(null);
    } finally {
      setLoading(false);
    }
  }, [fromDate, toDate, targetUid]);

  useEffect(() => {
    if (!isOpen) return;
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isOpen, targetUid]);

  const toggleExpand = useCallback(
    async (key: string) => {
      setFiles((prev) => {
        if (!prev) return prev;
        return prev.map((f) =>
          f.key === key ? { ...f, expanded: !f.expanded } : f,
        );
      });
      // Lazy load events lần đầu expand.
      const current = files?.find((f) => f.key === key);
      if (!current || current.events !== null || current.eventsLoading) return;
      const user = auth.currentUser;
      if (!user) return;
      setFiles((prev) =>
        prev
          ? prev.map((f) =>
              f.key === key ? { ...f, eventsLoading: true, eventsError: null } : f,
            )
          : prev,
      );
      try {
        const idToken = await user.getIdToken(false);
        const events = await adminV9SyncLogFetchEvents(idToken, key);
        // Sort by event_id ASC (causal order trong file).
        events.sort((a, b) => a.eventId - b.eventId);
        setFiles((prev) =>
          prev
            ? prev.map((f) =>
                f.key === key
                  ? { ...f, events, eventsLoading: false, eventsError: null }
                  : f,
              )
            : prev,
        );
      } catch (e) {
        setFiles((prev) =>
          prev
            ? prev.map((f) =>
                f.key === key
                  ? {
                      ...f,
                      eventsLoading: false,
                      eventsError: (e as Error).message,
                    }
                  : f,
              )
            : prev,
        );
      }
    },
    [files],
  );

  const allKinds = useMemo(() => {
    if (!files) return [] as string[];
    const s = new Set<string>();
    for (const f of files) {
      if (f.events) for (const e of f.events) s.add(e.kind);
    }
    return Array.from(s).sort();
  }, [files]);

  if (!isOpen) return null;

  const total = files?.length ?? 0;
  const totalBytes = (files ?? []).reduce((a, f) => a + f.sizeBytes, 0);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
      onClick={onClose}
    >
      <div
        className="flex h-[85vh] w-full max-w-4xl flex-col overflow-hidden rounded-xl border border-surface-8 bg-surface-1 shadow-elev-16"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between border-b border-surface-8 bg-surface-2 px-5 py-3">
          <div className="min-w-0">
            <h2 className="flex items-center gap-2 text-lg font-semibold text-white">
              <span className="material-symbols-rounded text-xl text-shopee-400">
                history
              </span>
              Sync log (admin)
            </h2>
            <p
              className="truncate text-xs text-white/60"
              title={targetEmail ?? targetUid}
            >
              {targetEmail ?? targetUid}
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

        {/* Controls */}
        <div className="flex flex-wrap items-center gap-3 border-b border-surface-8 bg-surface-2 px-5 py-2 text-xs">
          <div className="flex items-center gap-1.5">
            <span className="text-white/60">Từ</span>
            <input
              type="date"
              value={fromDate}
              onChange={(e) => setFromDate(e.currentTarget.value)}
              className="w-[135px] rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-white/90"
            />
            <span className="text-white/40">–</span>
            <span className="text-white/60">đến</span>
            <input
              type="date"
              value={toDate}
              onChange={(e) => setToDate(e.currentTarget.value)}
              className="w-[135px] rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-white/90"
            />
          </div>
          <button
            onClick={() => void load()}
            disabled={loading}
            className="btn-ripple flex items-center gap-1.5 rounded-md bg-shopee-500 px-3 py-1.5 font-medium text-white hover:bg-shopee-600 disabled:opacity-50"
          >
            <span
              className={`material-symbols-rounded text-sm ${loading ? "animate-spin" : ""}`}
            >
              {loading ? "sync" : "refresh"}
            </span>
            {loading ? "Đang tải..." : "Tải lại"}
          </button>
          {allKinds.length > 0 && (
            <label
              className="flex items-center gap-1.5 text-white/70"
              title="Lọc events theo kind (chỉ ảnh hưởng tới file đã expand)"
            >
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
          )}
          <div className="ml-auto flex items-center gap-2 text-white/60">
            <span>
              {total} file · {fmtBytes(totalBytes)}
            </span>
            {truncated && (
              <span
                className="rounded-full bg-amber-500/20 px-2 py-0.5 text-amber-300"
                title="Worker đã cut ở 500 files — thu hẹp khoảng ngày"
              >
                truncated
              </span>
            )}
          </div>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto">
          {error ? (
            <div className="m-5 rounded-lg border border-red-500/40 bg-red-900/20 p-4 text-sm text-red-200">
              {error}
            </div>
          ) : loading && !files ? (
            <div className="flex items-center justify-center py-16 text-white/50">
              <span className="material-symbols-rounded animate-spin text-3xl">
                sync
              </span>
            </div>
          ) : files === null || files.length === 0 ? (
            <div className="flex flex-col items-center gap-2 py-16 text-white/50">
              <span className="material-symbols-rounded text-5xl text-white/30">
                event_busy
              </span>
              <p className="text-sm">Không có log file nào trong khoảng này</p>
            </div>
          ) : (
            <ul className="divide-y divide-surface-8">
              {files.map((f) => (
                <li key={f.key} className="bg-surface-1 hover:bg-surface-2">
                  <button
                    type="button"
                    onClick={() => void toggleExpand(f.key)}
                    className="flex w-full items-center gap-3 px-5 py-3 text-left"
                  >
                    <span
                      className={`material-symbols-rounded text-lg text-white/60 transition-transform ${
                        f.expanded ? "rotate-90" : ""
                      }`}
                    >
                      chevron_right
                    </span>
                    <span className="rounded bg-shopee-900/30 px-2 py-0.5 font-mono text-xs text-shopee-200">
                      {f.date}
                    </span>
                    <span className="text-sm text-white/70">
                      {fmtBytes(f.sizeBytes)}
                    </span>
                    <span className="ml-auto text-xs text-white/40">
                      {fmtUploaded(f.uploadedAt)}
                    </span>
                  </button>
                  {f.expanded && (
                    <div className="border-t border-surface-8 bg-surface-0 px-5 py-3">
                      {f.eventsLoading ? (
                        <div className="flex items-center gap-2 text-xs text-white/50">
                          <span className="material-symbols-rounded animate-spin text-sm">
                            sync
                          </span>
                          Đang fetch + decompress...
                        </div>
                      ) : f.eventsError ? (
                        <div className="text-xs text-red-300">
                          Lỗi: {f.eventsError}
                        </div>
                      ) : f.events === null ? null : (
                        <EventsList events={f.events} kindFilter={kindFilter} />
                      )}
                      <div
                        className="mt-2 truncate font-mono text-[10px] text-white/30"
                        title={f.key}
                      >
                        {f.key}
                      </div>
                    </div>
                  )}
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}

function EventsList({
  events,
  kindFilter,
}: {
  events: AdminSyncLogEvent[];
  kindFilter: string;
}) {
  const filtered =
    kindFilter === "all"
      ? events
      : events.filter((e) => e.kind === kindFilter);
  if (filtered.length === 0) {
    return (
      <div className="text-xs text-white/40">
        Không có event nào khớp filter
      </div>
    );
  }
  return (
    <ul className="space-y-2">
      {filtered.map((e) => (
        <SyncEventCard key={e.eventId} event={e} />
      ))}
    </ul>
  );
}


function fmtUploaded(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString("vi-VN", {
      hour: "2-digit",
      minute: "2-digit",
      day: "2-digit",
      month: "2-digit",
    });
  } catch {
    return iso;
  }
}
