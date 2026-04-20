import { useCallback, useEffect, useMemo, useState } from "react";
import { auth } from "../lib/firebase";
import {
  adminDownloadUserDb,
  driveListUsers,
  listVideoDownloadsFromPath,
  type UserListEntry,
  type VideoDownloadLog,
} from "../lib/drive";

const PAGE_SIZE = 100;

interface SelectedUser {
  localPart: string;
  email: string;
  dbPath: string;
}

export function VideoLogsTab() {
  const [users, setUsers] = useState<UserListEntry[] | null>(null);
  const [usersLoading, setUsersLoading] = useState(false);
  const [usersError, setUsersError] = useState<string | null>(null);
  const [query, setQuery] = useState("");

  const [selected, setSelected] = useState<SelectedUser | null>(null);
  const [loadingDb, setLoadingDb] = useState(false);
  const [dbError, setDbError] = useState<string | null>(null);

  const [logs, setLogs] = useState<VideoDownloadLog[]>([]);
  const [logsLoading, setLogsLoading] = useState(false);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [offset, setOffset] = useState(0);

  const loadUsers = useCallback(async () => {
    const current = auth.currentUser;
    if (!current) {
      setUsersError("Chưa đăng nhập");
      return;
    }
    setUsersLoading(true);
    setUsersError(null);
    try {
      const idToken = await current.getIdToken(false);
      const list = await driveListUsers(idToken);
      const withFile = list.filter((u) => u.file !== null && u.localPart);
      setUsers(withFile);
    } catch (e) {
      setUsersError((e as Error).message);
      setUsers(null);
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
      ? users.filter((u) => (u.email ?? "").toLowerCase().includes(q))
      : users;
  }, [users, query]);

  const selectUser = useCallback(async (u: UserListEntry) => {
    if (!u.localPart) return;
    const current = auth.currentUser;
    if (!current) return;

    setLoadingDb(true);
    setDbError(null);
    setLogs([]);
    setHasMore(false);
    setOffset(0);
    setLogsError(null);

    try {
      const idToken = await current.getIdToken(false);
      const dbPath = await adminDownloadUserDb(idToken, u.localPart);
      setSelected({
        localPart: u.localPart,
        email: u.email ?? u.localPart,
        dbPath,
      });
      // Load page đầu.
      const first = await listVideoDownloadsFromPath(dbPath, PAGE_SIZE, 0);
      setLogs(first);
      setOffset(first.length);
      setHasMore(first.length >= PAGE_SIZE);
    } catch (e) {
      setDbError((e as Error).message);
    } finally {
      setLoadingDb(false);
    }
  }, []);

  const loadMore = useCallback(async () => {
    if (!selected) return;
    setLogsLoading(true);
    setLogsError(null);
    try {
      const more = await listVideoDownloadsFromPath(
        selected.dbPath,
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
  }, [selected, offset]);

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
            title="Tải lại"
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
                  ? "Chưa có user nào có DB trên Drive"
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
        <div className="flex items-center justify-between border-b border-surface-8 px-4 py-3">
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
            <div className="text-xs text-white/50">
              {logs.length} dòng {hasMore ? "(còn nữa)" : ""}
            </div>
          )}
        </div>
        <div className="flex-1 overflow-auto">
          {loadingDb ? (
            <div className="flex flex-col items-center justify-center gap-2 py-16 text-white/60">
              <span className="material-symbols-rounded animate-spin text-3xl">
                cloud_download
              </span>
              Đang tải DB của user...
            </div>
          ) : dbError ? (
            <div className="mx-auto my-10 max-w-md rounded-lg border border-red-500/40 bg-red-900/20 p-4 text-sm text-red-200">
              <div className="mb-2 font-medium">Lỗi tải DB</div>
              <div className="text-xs text-red-300/80">{dbError}</div>
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
              <div className="text-sm">User chưa download video nào</div>
            </div>
          ) : (
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-surface-2">
                <tr className="text-left text-xs uppercase tracking-wide text-white/50">
                  <th className="w-10 px-3 py-2">#</th>
                  <th className="px-3 py-2">URL</th>
                  <th className="w-44 px-3 py-2">Thời gian</th>
                  <th className="w-24 px-3 py-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {logs.map((row, idx) => (
                  <tr
                    key={row.id}
                    className="border-t border-surface-8 hover:bg-surface-2/60"
                  >
                    <td className="px-3 py-2 text-xs text-white/40">
                      {idx + 1}
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
                    <td className="px-3 py-2 text-xs text-white/70">
                      {new Date(row.downloaded_at_ms).toLocaleString("vi-VN")}
                    </td>
                    <td className="px-3 py-2">
                      <StatusBadge status={row.status} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        {selected && hasMore && (
          <div className="border-t border-surface-8 p-3 text-center">
            <button
              onClick={() => void loadMore()}
              disabled={logsLoading}
              className="btn-ripple rounded-lg bg-shopee-500 px-4 py-1.5 text-sm font-medium text-white hover:bg-shopee-600 disabled:opacity-50"
            >
              {logsLoading ? "Đang tải..." : `Tải thêm 100`}
            </button>
            {logsError && (
              <div className="mt-2 text-xs text-red-300">{logsError}</div>
            )}
          </div>
        )}
      </section>
    </div>
  );
}

interface StatusBadgeProps {
  status: string;
}

function StatusBadge({ status }: StatusBadgeProps) {
  if (status === "success") {
    return (
      <span className="rounded-full bg-green-900/40 px-2 py-0.5 text-xs font-medium text-green-300">
        success
      </span>
    );
  }
  return (
    <span className="rounded-full bg-red-900/40 px-2 py-0.5 text-xs font-medium text-red-300">
      failed
    </span>
  );
}
