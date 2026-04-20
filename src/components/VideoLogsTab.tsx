import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { auth } from "../lib/firebase";
import {
  adminDeleteUserLogRow,
  adminDeleteUserLogSheet,
  adminFetchUserList,
  adminFetchUserLogSheet,
  adminReadUserListCache,
  adminReadUserLogCache,
  adminUserLogFetchMeta,
  type AdminFetchMeta,
  type UserListEntry,
  type VideoLogRow,
} from "../lib/drive";
import { ConfirmDialog } from "./ConfirmDialog";

const PAGE_SIZE = 100;

interface SelectedUser {
  localPart: string;
  email: string;
}

function fmtFetchedAt(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number) => n.toString().padStart(2, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())} ${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()}`;
}

export function VideoLogsTab() {
  const [users, setUsers] = useState<UserListEntry[] | null>(null);
  const [usersLoading, setUsersLoading] = useState(false);
  const [usersError, setUsersError] = useState<string | null>(null);
  const [query, setQuery] = useState("");

  const [selected, setSelected] = useState<SelectedUser | null>(null);
  const [meta, setMeta] = useState<AdminFetchMeta | null>(null);

  // 2 loading states: initial (cache đọc lần đầu) + fetching (gọi Sheet).
  const [initialLoading, setInitialLoading] = useState(false);
  const [fetching, setFetching] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);

  const [logs, setLogs] = useState<VideoLogRow[]>([]);
  const [logsLoading, setLogsLoading] = useState(false);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [offset, setOffset] = useState(0);

  const sentinelRef = useRef<HTMLDivElement | null>(null);

  // Stale-while-revalidate: đọc cache DB trước (render ngay nếu có),
  // sau đó fetch fresh từ AS qua background → replace cache → re-render.
  const loadUsers = useCallback(async () => {
    const current = auth.currentUser;
    if (!current) {
      setUsersError("Chưa đăng nhập");
      return;
    }

    // 1. Cache first — render instant nếu đã từng fetch.
    try {
      const cached = await adminReadUserListCache();
      if (cached) {
        const list: UserListEntry[] = JSON.parse(cached.users_json);
        setUsers(list.filter((u) => u.localPart));
      }
    } catch {
      /* cache miss/parse OK — tiếp tục fetch */
    }

    // 2. Fetch fresh background → update cache + UI.
    setUsersLoading(true);
    setUsersError(null);
    try {
      const idToken = await current.getIdToken(false);
      const fresh = await adminFetchUserList(idToken);
      const list: UserListEntry[] = JSON.parse(fresh.users_json);
      setUsers(list.filter((u) => u.localPart));
    } catch (e) {
      setUsersError((e as Error).message);
    } finally {
      setUsersLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadUsers();
  }, [loadUsers]);

  const filteredUsers = useMemo(() => {
    if (!users) return [];
    const q = query.toLowerCase().trim();
    return q
      ? users.filter(
          (u) =>
            (u.email ?? "").toLowerCase().includes(q) ||
            (u.localPart ?? "").toLowerCase().includes(q),
        )
      : users;
  }, [users, query]);

  // Đọc trang đầu từ cache DB. Trả về số rows đọc được.
  const readCacheFirstPage = useCallback(async (localPart: string) => {
    const rows = await adminReadUserLogCache(localPart, PAGE_SIZE, 0);
    setLogs(rows);
    setOffset(rows.length);
    setHasMore(rows.length >= PAGE_SIZE);
    const m = await adminUserLogFetchMeta(localPart);
    setMeta(m);
    return rows.length;
  }, []);

  // Fetch Sheet → replace cache → đọc lại trang đầu.
  const fetchFromSheet = useCallback(
    async (localPart: string) => {
      const current = auth.currentUser;
      if (!current) return;
      setFetching(true);
      setFetchError(null);
      try {
        const idToken = await current.getIdToken(false);
        await adminFetchUserLogSheet(idToken, localPart);
        await readCacheFirstPage(localPart);
      } catch (e) {
        setFetchError((e as Error).message);
      } finally {
        setFetching(false);
      }
    },
    [readCacheFirstPage],
  );

  const selectUser = useCallback(
    async (u: UserListEntry) => {
      if (!u.localPart) return;
      setSelected({
        localPart: u.localPart,
        email: u.email ?? u.localPart,
      });
      setLogs([]);
      setOffset(0);
      setHasMore(false);
      setMeta(null);
      setLogsError(null);
      setFetchError(null);

      // 1. Cache DB trước — render ngay (dù có hay không).
      setInitialLoading(true);
      try {
        await readCacheFirstPage(u.localPart);
      } catch (e) {
        setLogsError((e as Error).message);
      } finally {
        setInitialLoading(false);
      }

      // 2. Luôn fetch Sheet background → update cache + re-render.
      // (Không còn phụ thuộc cache rỗng — mỗi lần select đều revalidate.)
      void fetchFromSheet(u.localPart);
    },
    [readCacheFirstPage, fetchFromSheet],
  );

  const refetch = useCallback(() => {
    if (!selected) return;
    void fetchFromSheet(selected.localPart);
  }, [selected, fetchFromSheet]);

  // Track index row đang xóa để disable button + show spinner.
  const [deletingRowIdx, setDeletingRowIdx] = useState<number | null>(null);
  const [deletingSheet, setDeletingSheet] = useState(false);

  // Confirm dialog state — share cho cả delete row + delete sheet.
  const [confirmState, setConfirmState] = useState<{
    title: string;
    message: React.ReactNode;
    confirmLabel: string;
    onConfirm: () => void | Promise<void>;
  } | null>(null);

  const doDeleteRow = useCallback(
    async (idx: number) => {
      if (!selected) return;
      const row = logs[idx];
      if (!row) return;
      const current = auth.currentUser;
      if (!current) return;

      setDeletingRowIdx(idx);
      try {
        const idToken = await current.getIdToken(false);
        await adminDeleteUserLogRow(
          idToken,
          selected.localPart,
          row.timestamp,
          row.url,
          row.status,
        );
        setLogs((prev) => prev.filter((_, i) => i !== idx));
        setOffset((prev) => Math.max(0, prev - 1));
        setMeta((prev) =>
          prev ? { ...prev, row_count: Math.max(0, prev.row_count - 1) } : prev,
        );
      } catch (e) {
        setLogsError((e as Error).message);
      } finally {
        setDeletingRowIdx(null);
      }
    },
    [selected, logs],
  );

  const deleteRow = useCallback(
    (idx: number) => {
      const row = logs[idx];
      if (!row || !selected) return;
      setConfirmState({
        title: "Xóa dòng log này?",
        confirmLabel: "Xóa dòng",
        message: (
          <div className="space-y-2">
            <p className="text-white/80">
              Sẽ xóa khỏi Google Sheet + cache local. Không hoàn tác được.
            </p>
            <div className="rounded-md border border-surface-8 bg-surface-0 px-3 py-2 font-mono text-xs text-white/60">
              <div>🕐 {row.timestamp}</div>
              <div className="truncate">🔗 {row.url}</div>
              <div>
                {row.status === "thất bại" ? "❌" : "✅"} {row.status}
              </div>
            </div>
          </div>
        ),
        onConfirm: async () => {
          setConfirmState(null);
          await doDeleteRow(idx);
        },
      });
    },
    [logs, selected, doDeleteRow],
  );

  const doDeleteSheet = useCallback(async () => {
    if (!selected) return;
    const current = auth.currentUser;
    if (!current) return;

    setDeletingSheet(true);
    setLogsError(null);
    try {
      const idToken = await current.getIdToken(false);
      await adminDeleteUserLogSheet(idToken, selected.localPart);
      setLogs([]);
      setOffset(0);
      setHasMore(false);
      setMeta(null);
    } catch (e) {
      setLogsError((e as Error).message);
    } finally {
      setDeletingSheet(false);
    }
  }, [selected]);

  const deleteSheet = useCallback(() => {
    if (!selected) return;
    setConfirmState({
      title: `Xóa tab "${selected.localPart}"?`,
      confirmLabel: "Xóa tab",
      message: (
        <div className="space-y-2">
          <p className="text-white/80">
            Xóa toàn bộ log video của{" "}
            <span className="font-semibold text-white">{selected.email}</span>.
            Không hoàn tác được.
          </p>
          <p className="text-xs text-white/50">
            (File Sheet gốc không bị ảnh hưởng — chỉ xóa tab của user này.)
          </p>
        </div>
      ),
      onConfirm: async () => {
        setConfirmState(null);
        await doDeleteSheet();
      },
    });
  }, [selected, doDeleteSheet]);

  const loadMore = useCallback(async () => {
    if (!selected || logsLoading || !hasMore) return;
    setLogsLoading(true);
    setLogsError(null);
    try {
      const more = await adminReadUserLogCache(
        selected.localPart,
        PAGE_SIZE,
        offset,
      );
      setLogs((prev) => [...prev, ...more]);
      setOffset(offset + more.length);
      setHasMore(more.length >= PAGE_SIZE);
    } catch (e) {
      setLogsError((e as Error).message);
    } finally {
      setLogsLoading(false);
    }
  }, [selected, offset, hasMore, logsLoading]);

  // Infinite scroll: observe sentinel cuối list → khi scroll tới → loadMore.
  useEffect(() => {
    const el = sentinelRef.current;
    if (!el || !hasMore || initialLoading || fetching) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting) void loadMore();
      },
      { rootMargin: "200px" },
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [loadMore, hasMore, initialLoading, fetching, logs.length]);

  const busy = initialLoading || fetching;

  return (
    <div className="flex h-[calc(100vh-180px)] gap-4">
      {/* Sidebar: user list */}
      <aside className="flex w-80 shrink-0 flex-col overflow-hidden rounded-xl border border-surface-8 bg-surface-1">
        <div className="flex items-center gap-2 border-b border-surface-8 px-3 py-2">
          <span className="material-symbols-rounded text-base text-white/50">
            search
          </span>
          <input
            value={query}
            onChange={(e) => setQuery(e.currentTarget.value)}
            placeholder="Tìm user..."
            className="flex-1 bg-transparent text-sm text-white/90 placeholder:text-white/30 focus:outline-none"
          />
          <button
            onClick={() => void loadUsers()}
            disabled={usersLoading}
            className="btn-ripple flex h-7 w-7 items-center justify-center rounded text-white/60 hover:bg-white/10 disabled:opacity-40"
            title="Tải lại danh sách user"
          >
            <span
              className={`material-symbols-rounded text-base ${usersLoading ? "animate-spin" : ""}`}
            >
              refresh
            </span>
          </button>
        </div>
        <div className="flex-1 overflow-auto">
          {usersLoading && !users ? (
            <div className="flex items-center justify-center py-8 text-white/50">
              <span className="material-symbols-rounded animate-spin">
                progress_activity
              </span>
            </div>
          ) : usersError ? (
            <div className="p-3 text-xs text-red-300">{usersError}</div>
          ) : filteredUsers.length === 0 ? (
            <div className="p-3 text-center text-xs text-white/50">
              {users === null
                ? "—"
                : users.length === 0
                  ? "Chưa có user nào"
                  : "Không tìm thấy user"}
            </div>
          ) : (
            <ul>
              {filteredUsers.map((u) => {
                const isActive = selected?.localPart === u.localPart;
                return (
                  <li key={u.uid}>
                    <button
                      onClick={() => void selectUser(u)}
                      className={`flex w-full items-center gap-2 border-b border-surface-8 px-3 py-2 text-left text-sm hover:bg-surface-2 ${
                        isActive ? "bg-shopee-900/30" : ""
                      }`}
                    >
                      <span
                        className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-white/10 text-xs font-semibold text-white"
                        aria-hidden
                      >
                        {(u.email ?? u.localPart ?? "?")[0]?.toUpperCase()}
                      </span>
                      <div className="min-w-0 flex-1">
                        <div
                          className="truncate text-white/90"
                          title={u.email ?? ""}
                        >
                          {u.email ?? u.localPart}
                        </div>
                        <div className="flex items-center gap-1 text-[10px] text-white/40">
                          {u.premium && (
                            <span className="rounded bg-green-900/40 px-1 text-green-300">
                              premium
                            </span>
                          )}
                          {u.admin && (
                            <span className="rounded bg-shopee-900/60 px-1 text-shopee-300">
                              admin
                            </span>
                          )}
                        </div>
                      </div>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      </aside>

      {/* Main: video log table */}
      <section className="flex flex-1 flex-col overflow-hidden rounded-xl border border-surface-8 bg-surface-1">
        <div className="flex items-center justify-between gap-3 border-b border-surface-8 px-4 py-3">
          <div className="min-w-0">
            <div className="text-xs text-white/50">
              {selected ? "Video log của" : "Chọn user ở danh sách bên trái"}
            </div>
            {selected && (
              <div
                className="truncate text-sm font-medium text-white/90"
                title={selected.email}
              >
                {selected.email}
              </div>
            )}
          </div>
          {selected && (
            <div className="flex items-center gap-3">
              <div className="text-right text-xs text-white/50">
                <div>
                  {meta ? `${meta.row_count} dòng` : `${logs.length} dòng`}
                  {hasMore ? " (còn nữa)" : ""}
                </div>
                {meta && (
                  <div className="text-[10px] text-white/35">
                    Cập nhật: {fmtFetchedAt(meta.fetched_at_ms)}
                  </div>
                )}
              </div>
              <button
                onClick={refetch}
                disabled={busy || deletingSheet}
                className="btn-ripple flex h-8 items-center gap-1.5 rounded-lg bg-shopee-500 px-3 text-xs font-semibold text-white shadow-elev-1 hover:bg-shopee-600 disabled:opacity-50"
                title="Fetch lại từ Google Sheet"
              >
                <span
                  className={`material-symbols-rounded text-base ${fetching ? "animate-spin" : ""}`}
                >
                  cloud_download
                </span>
                Tải lại
              </button>
              <button
                onClick={() => void deleteSheet()}
                disabled={busy || deletingSheet || logs.length === 0}
                className="btn-ripple flex h-8 items-center gap-1.5 rounded-lg bg-red-600 px-3 text-xs font-semibold text-white shadow-elev-1 hover:bg-red-700 disabled:opacity-50"
                title="Xóa tab của user này (file Sheet gốc giữ nguyên)"
              >
                <span
                  className={`material-symbols-rounded text-base ${deletingSheet ? "animate-spin" : ""}`}
                >
                  {deletingSheet ? "progress_activity" : "delete_sweep"}
                </span>
                Xóa tab
              </button>
            </div>
          )}
        </div>
        <div className="flex-1 overflow-auto">
          {initialLoading ? (
            <div className="flex flex-col items-center justify-center gap-2 py-16 text-white/60">
              <span className="material-symbols-rounded animate-spin text-3xl">
                database
              </span>
              Đang đọc cache...
            </div>
          ) : fetching && logs.length === 0 ? (
            <div className="flex flex-col items-center justify-center gap-2 py-16 text-white/60">
              <span className="material-symbols-rounded animate-spin text-3xl">
                cloud_download
              </span>
              Đang fetch log từ Google Sheet...
            </div>
          ) : fetchError ? (
            <div className="mx-auto my-10 max-w-md rounded-lg border border-red-500/40 bg-red-900/20 p-4 text-sm text-red-200">
              <div className="mb-2 font-medium">Lỗi fetch sheet</div>
              <div className="text-xs text-red-300/80">{fetchError}</div>
            </div>
          ) : !selected ? (
            <div className="flex flex-col items-center justify-center gap-2 py-16 text-white/50">
              <span className="material-symbols-rounded text-4xl">
                playlist_play
              </span>
              <div className="text-sm">
                Chọn 1 user ở sidebar để xem video log của họ
              </div>
            </div>
          ) : logs.length === 0 ? (
            <div className="flex flex-col items-center justify-center gap-2 py-16 text-white/50">
              <span className="material-symbols-rounded text-4xl">
                inbox
              </span>
              <div className="text-sm">User chưa có log video nào</div>
            </div>
          ) : (
            <>
              <table className="w-full text-sm">
                <thead className="sticky top-0 bg-surface-2">
                  <tr className="text-left text-xs uppercase tracking-wide text-white/50">
                    <th className="w-10 px-3 py-2">#</th>
                    <th className="w-44 px-3 py-2">Thời gian</th>
                    <th className="px-3 py-2">Link</th>
                    <th className="w-24 px-3 py-2">Trạng thái</th>
                    <th className="w-12 px-3 py-2"></th>
                  </tr>
                </thead>
                <tbody>
                  {logs.map((row, idx) => {
                    const deleting = deletingRowIdx === idx;
                    return (
                      <tr
                        key={`${row.timestamp}-${idx}`}
                        className="border-t border-surface-8 hover:bg-surface-2/60"
                      >
                        <td className="px-3 py-2 text-xs text-white/40">
                          {idx + 1}
                        </td>
                        <td className="px-3 py-2 text-xs tabular-nums text-white/70">
                          {row.timestamp}
                        </td>
                        <td className="max-w-[480px] px-3 py-2">
                          <a
                            href={row.url}
                            target="_blank"
                            rel="noreferrer"
                            title={row.url}
                            className="block truncate text-shopee-300 hover:underline"
                          >
                            {row.url}
                          </a>
                        </td>
                        <td className="px-3 py-2">
                          <StatusBadge status={row.status} />
                        </td>
                        <td className="px-3 py-2">
                          <button
                            onClick={() => void deleteRow(idx)}
                            disabled={
                              deletingRowIdx !== null || deletingSheet || busy
                            }
                            className="btn-ripple flex h-7 w-7 items-center justify-center rounded-full text-white/40 transition-colors hover:bg-red-900/40 hover:text-red-300 disabled:opacity-30"
                            title="Xóa dòng này"
                          >
                            <span
                              className={`material-symbols-rounded text-base ${deleting ? "animate-spin" : ""}`}
                            >
                              {deleting ? "progress_activity" : "delete"}
                            </span>
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>

              {hasMore && (
                <div
                  ref={sentinelRef}
                  className="flex items-center justify-center py-4 text-xs text-white/40"
                >
                  {logsLoading ? (
                    <span className="flex items-center gap-2">
                      <span className="material-symbols-rounded animate-spin text-sm">
                        progress_activity
                      </span>
                      Đang tải thêm...
                    </span>
                  ) : (
                    <span>Kéo để tải thêm</span>
                  )}
                </div>
              )}
              {logsError && (
                <div className="px-3 py-2 text-center text-xs text-red-300">
                  {logsError}
                </div>
              )}
            </>
          )}
        </div>
      </section>

      <ConfirmDialog
        isOpen={!!confirmState}
        title={confirmState?.title ?? ""}
        message={confirmState?.message ?? ""}
        confirmLabel={confirmState?.confirmLabel ?? "Xác nhận"}
        danger
        onConfirm={() => void confirmState?.onConfirm()}
        onClose={() => setConfirmState(null)}
      />
    </div>
  );
}

interface StatusBadgeProps {
  status: string;
}

function StatusBadge({ status }: StatusBadgeProps) {
  const ok = status === "thành công" || status === "success";
  if (ok) {
    return (
      <span className="rounded-full bg-green-900/40 px-2 py-0.5 text-xs font-medium text-green-300">
        {status || "thành công"}
      </span>
    );
  }
  return (
    <span className="rounded-full bg-red-900/40 px-2 py-0.5 text-xs font-medium text-red-300">
      {status || "thất bại"}
    </span>
  );
}
