import { useCallback, useEffect, useMemo, useState } from "react";
import { auth } from "../lib/firebase";
import { driveListUsers, type UserListEntry } from "../lib/drive";

interface UserListDialogProps {
  isOpen: boolean;
  onClose: () => void;
}

type SortKey = "email" | "premium" | "expiredAt" | "createdAt";

export function UserListDialog({ isOpen, onClose }: UserListDialogProps) {
  const [users, setUsers] = useState<UserListEntry[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("createdAt");
  const [sortDesc, setSortDesc] = useState(true);

  const load = useCallback(async () => {
    const currentUser = auth.currentUser;
    if (!currentUser) {
      setError("Chưa đăng nhập");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const idToken = await currentUser.getIdToken(false);
      const list = await driveListUsers(idToken);
      setUsers(list);
    } catch (e) {
      setError((e as Error).message);
      setUsers(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!isOpen) return;
    void load();
  }, [isOpen, load]);

  const filtered = useMemo(() => {
    if (!users) return [];
    const q = query.toLowerCase().trim();
    const matched = q
      ? users.filter((u) => (u.email ?? "").toLowerCase().includes(q))
      : users.slice();
    matched.sort((a, b) => {
      const dir = sortDesc ? -1 : 1;
      switch (sortKey) {
        case "email":
          return dir * (a.email ?? "").localeCompare(b.email ?? "");
        case "premium":
          return dir * ((a.premium ? 1 : 0) - (b.premium ? 1 : 0));
        case "expiredAt":
          return dir * tsCompare(a.expiredAt, b.expiredAt);
        case "createdAt":
          return dir * tsCompare(a.createdAt, b.createdAt);
      }
    });
    return matched;
  }, [users, query, sortKey, sortDesc]);

  const summary = useMemo(() => {
    if (!users) return null;
    const total = users.length;
    const premium = users.filter((u) => u.premium).length;
    const admin = users.filter((u) => u.admin).length;
    const withFile = users.filter((u) => u.file !== null).length;
    return { total, premium, admin, withFile };
  }, [users]);

  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 z-40 flex items-center justify-center bg-black/60 p-4 backdrop-blur-sm"
      onClick={onClose}
    >
      <div
        className="flex w-full max-w-[95vw] flex-col overflow-hidden rounded-2xl border border-surface-8 bg-surface-1 shadow-elev-16 xl:max-w-[88vw] 2xl:max-w-[1600px]"
        style={{ maxHeight: "92vh" }}
        onClick={(e) => e.stopPropagation()}
      >
        <header className="flex items-center justify-between border-b border-surface-8 px-6 py-4">
          <div className="flex items-center gap-3">
            <span className="material-symbols-rounded text-2xl text-shopee-400">
              admin_panel_settings
            </span>
            <div>
              <h2 className="text-lg font-semibold text-white/90">
                Danh sách user
              </h2>
              {summary && (
                <p className="text-xs text-white/50">
                  {summary.total} user · {summary.premium} premium ·{" "}
                  {summary.admin} admin · {summary.withFile} có DB
                </p>
              )}
            </div>
          </div>
          <button
            onClick={onClose}
            className="btn-ripple flex h-9 w-9 items-center justify-center rounded-full text-white/70 hover:bg-white/10 hover:text-white"
            aria-label="Đóng"
          >
            <span className="material-symbols-rounded">close</span>
          </button>
        </header>

        <div className="flex items-center gap-3 border-b border-surface-8 bg-surface-2/50 px-6 py-3">
          <div className="relative flex-1">
            <span className="material-symbols-rounded pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-white/40">
              search
            </span>
            <input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.currentTarget.value)}
              placeholder="Tìm theo email..."
              className="w-full rounded-lg border border-surface-8 bg-surface-1 py-2 pl-10 pr-3 text-sm text-white/90 placeholder:text-white/30 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500"
            />
          </div>
          <button
            onClick={() => void load()}
            disabled={loading}
            className="btn-ripple flex items-center gap-1 rounded-lg border border-surface-8 bg-surface-1 px-3 py-2 text-sm text-white/80 hover:bg-surface-4 disabled:opacity-50"
          >
            <span
              className={`material-symbols-rounded text-base ${loading ? "animate-spin" : ""}`}
            >
              refresh
            </span>
            Tải lại
          </button>
        </div>

        <div className="flex-1 overflow-auto">
          {loading && !users ? (
            <LoadingState />
          ) : error ? (
            <ErrorState message={error} onRetry={load} />
          ) : !users || users.length === 0 ? (
            <EmptyState />
          ) : filtered.length === 0 ? (
            <NoMatchState query={query} />
          ) : (
            <table className="w-full text-sm">
              <thead className="sticky top-0 z-10 bg-surface-2">
                <tr className="text-left text-xs uppercase tracking-wide text-white/50">
                  <Th
                    label="Email"
                    active={sortKey === "email"}
                    desc={sortDesc}
                    onClick={() => toggleSort("email")}
                  />
                  <Th
                    label="Premium"
                    active={sortKey === "premium"}
                    desc={sortDesc}
                    onClick={() => toggleSort("premium")}
                  />
                  <Th
                    label="Hết hạn"
                    active={sortKey === "expiredAt"}
                    desc={sortDesc}
                    onClick={() => toggleSort("expiredAt")}
                  />
                  <Th
                    label="Ngày tạo"
                    active={sortKey === "createdAt"}
                    desc={sortDesc}
                    onClick={() => toggleSort("createdAt")}
                  />
                  <th className="px-6 py-3 text-right">Action</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((u) => (
                  <UserRow key={u.uid} user={u} />
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDesc((v) => !v);
    else {
      setSortKey(key);
      setSortDesc(true);
    }
  }
}

interface ThProps {
  label: string;
  active: boolean;
  desc: boolean;
  onClick: () => void;
}

function Th({ label, active, desc, onClick }: ThProps) {
  return (
    <th className="px-6 py-3">
      <button
        type="button"
        onClick={onClick}
        className={`flex items-center gap-1 hover:text-white ${
          active ? "text-shopee-300" : ""
        }`}
      >
        {label}
        {active && (
          <span className="material-symbols-rounded text-sm">
            {desc ? "arrow_downward" : "arrow_upward"}
          </span>
        )}
      </button>
    </th>
  );
}

interface UserRowProps {
  user: UserListEntry;
}

function UserRow({ user }: UserRowProps) {
  const expiredAt = parseTs(user.expiredAt);
  const createdAt = parseTs(user.createdAt);
  const isExpired =
    user.premium && expiredAt && expiredAt.getTime() < Date.now();
  const hasFile = user.file !== null;

  return (
    <tr
      className="border-t border-surface-8 hover:bg-surface-2/60"
      title={`UID: ${user.uid}`}
    >
      <td className="min-w-[280px] max-w-[420px] px-6 py-3">
        <div
          className="truncate text-sm text-white/90"
          title={user.email ?? user.uid}
        >
          {user.email ?? "—"}
        </div>
        <div className="truncate text-xs text-white/40" title={user.uid}>
          {user.admin && (
            <span className="mr-1 rounded bg-shopee-900/60 px-1 text-[10px] font-semibold text-shopee-300">
              ADMIN
            </span>
          )}
          {user.localPart ? `${user.localPart}.db` : user.uid.slice(0, 8)}
        </div>
      </td>
      <td className="px-6 py-3">
        {isExpired ? (
          <Badge color="amber">Hết hạn</Badge>
        ) : user.premium ? (
          <Badge color="green">Premium</Badge>
        ) : (
          <Badge color="gray">Free</Badge>
        )}
      </td>
      <td className="px-4 py-2.5 text-white/80">
        {expiredAt ? expiredAt.toLocaleDateString("vi-VN") : "—"}
      </td>
      <td className="px-4 py-2.5 text-white/60">
        {createdAt ? createdAt.toLocaleDateString("vi-VN") : "—"}
      </td>
      <td className="px-6 py-3 text-right">
        <button
          type="button"
          disabled
          className="btn-ripple inline-flex items-center gap-1 rounded-lg border border-surface-8 bg-surface-2 px-3 py-1 text-xs text-white/40"
          title={
            hasFile
              ? "Xem DB (Phase B — đang triển khai)"
              : "User chưa backup DB"
          }
        >
          <span className="material-symbols-rounded text-sm">visibility</span>
          Xem
        </button>
      </td>
    </tr>
  );
}

interface BadgeProps {
  color: "green" | "gray" | "amber";
  children: React.ReactNode;
}

function Badge({ color, children }: BadgeProps) {
  const map = {
    green: "bg-green-900/40 text-green-300",
    gray: "bg-surface-4 text-white/60",
    amber: "bg-amber-900/40 text-amber-300",
  } as const;
  return (
    <span
      className={`rounded-full px-2 py-0.5 text-xs font-medium ${map[color]}`}
    >
      {children}
    </span>
  );
}

function LoadingState() {
  return (
    <div className="flex items-center justify-center gap-2 py-16 text-white/50">
      <span className="material-symbols-rounded animate-spin">
        progress_activity
      </span>
      Đang tải danh sách...
    </div>
  );
}

interface ErrorStateProps {
  message: string;
  onRetry: () => void;
}

function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="mx-auto my-10 max-w-md rounded-lg border border-red-500/40 bg-red-900/20 p-4 text-sm text-red-200">
      <div className="mb-3 font-medium">Lỗi tải danh sách user</div>
      <div className="mb-3 whitespace-pre-wrap break-words text-xs text-red-300/80">
        {message}
      </div>
      <button
        onClick={onRetry}
        className="btn-ripple rounded-md bg-red-500/30 px-3 py-1 text-sm hover:bg-red-500/50"
      >
        Thử lại
      </button>
    </div>
  );
}

function EmptyState() {
  return (
    <div className="flex flex-col items-center gap-2 py-16 text-white/50">
      <span className="material-symbols-rounded text-4xl">group_off</span>
      <div>Chưa có user nào trong hệ thống</div>
    </div>
  );
}

interface NoMatchStateProps {
  query: string;
}

function NoMatchState({ query }: NoMatchStateProps) {
  return (
    <div className="flex flex-col items-center gap-2 py-16 text-white/50">
      <span className="material-symbols-rounded text-4xl">search_off</span>
      <div>Không có user nào khớp "{query}"</div>
    </div>
  );
}

function parseTs(ts: string | null): Date | null {
  if (!ts) return null;
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? null : d;
}

function tsCompare(a: string | null, b: string | null): number {
  if (!a && !b) return 0;
  if (!a) return -1;
  if (!b) return 1;
  return a.localeCompare(b);
}
