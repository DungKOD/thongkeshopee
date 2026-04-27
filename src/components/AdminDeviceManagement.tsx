import { useEffect, useMemo, useState } from "react";
import { getAuthToken } from "../lib/firebase";
import { adminListUsers, type UserListEntry } from "../lib/sync";
import {
  DEFAULT_DEVICE_LIMIT,
  removeDevice,
  setDeviceLimit,
  subscribeAllDevices,
  subscribeAllLimits,
  type AllDevicesMap,
  type AllLimitsMap,
  type DeviceEntry,
} from "../lib/userDevices";

interface RowData {
  uid: string;
  email: string;
  isAdmin: boolean;
  limit: number;
  devices: Array<{ fingerprint: string; entry: DeviceEntry }>;
}

export function AdminDeviceManagement() {
  const [allDevices, setAllDevices] = useState<AllDevicesMap>({});
  const [allLimits, setAllLimits] = useState<AllLimitsMap>({});
  const [users, setUsers] = useState<UserListEntry[]>([]);
  const [loadingUsers, setLoadingUsers] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  // Subscribe RTDB realtime — auto refresh khi admin xóa device hoặc limit đổi.
  useEffect(() => {
    const u1 = subscribeAllDevices(setAllDevices);
    const u2 = subscribeAllLimits(setAllLimits);
    return () => {
      u1();
      u2();
    };
  }, []);

  // Load user list 1 lần (uid → email mapping).
  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const token = await getAuthToken(true);
        const list = await adminListUsers(token);
        if (!cancelled) {
          setUsers(list);
          setLoadingUsers(false);
        }
      } catch (e) {
        if (!cancelled) {
          setError((e as Error).message ?? String(e));
          setLoadingUsers(false);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  /// Gộp 3 nguồn (RTDB devices + RTDB limits + Firestore user profiles) thành
  /// rows hiển thị. Sort: admin đẩy xuống cuối, còn lại theo email asc.
  const rows = useMemo<RowData[]>(() => {
    const userByUid = new Map(users.map((u) => [u.uid, u]));
    const uids = new Set([
      ...Object.keys(allDevices),
      ...Object.keys(allLimits),
    ]);
    const out: RowData[] = [];
    for (const uid of uids) {
      const u = userByUid.get(uid);
      const devicesObj = allDevices[uid] ?? {};
      const devices = Object.entries(devicesObj)
        .map(([fingerprint, entry]) => ({ fingerprint, entry }))
        .sort((a, b) => (b.entry.lastSeen ?? 0) - (a.entry.lastSeen ?? 0));
      out.push({
        uid,
        email: u?.email ?? `(uid: ${uid.slice(0, 12)}…)`,
        isAdmin: u?.admin === true,
        limit: allLimits[uid] ?? DEFAULT_DEVICE_LIMIT,
        devices,
      });
    }
    out.sort((a, b) => {
      if (a.isAdmin !== b.isAdmin) return a.isAdmin ? 1 : -1;
      return a.email.localeCompare(b.email);
    });
    return out;
  }, [allDevices, allLimits, users]);

  const toggleExpand = (uid: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(uid)) next.delete(uid);
      else next.add(uid);
      return next;
    });
  };

  return (
    <section>
      <h3 className="mb-1 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
        <span className="material-symbols-rounded text-base">devices</span>
        Quản lý thiết bị user
        <span className="rounded-full bg-amber-500/20 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-amber-300">
          Admin
        </span>
      </h3>
      <p className="mb-3 text-xs text-white/50">
        User thường mặc định login 1 thiết bị. Đổi máy → user liên hệ admin →
        admin xóa entry máy cũ. Đặt limit cao hơn cho user cần nhiều máy.
      </p>

      {error && (
        <div className="mb-2 rounded-lg border border-red-500/40 bg-red-900/20 p-2 text-xs text-red-200">
          Lỗi tải user list: {error}
        </div>
      )}

      {loadingUsers && rows.length === 0 ? (
        <div className="rounded-xl border border-dashed border-surface-12 bg-surface-2 px-4 py-4 text-sm text-white/50">
          Đang tải...
        </div>
      ) : rows.length === 0 ? (
        <div className="rounded-xl border border-dashed border-surface-12 bg-surface-2 px-4 py-4 text-sm text-white/50">
          Chưa có user nào register thiết bị.
        </div>
      ) : (
        <ul className="divide-y divide-surface-8 overflow-hidden rounded-xl bg-surface-6">
          {rows.map((row) => (
            <UserRow
              key={row.uid}
              row={row}
              expanded={expanded.has(row.uid)}
              onToggle={() => toggleExpand(row.uid)}
            />
          ))}
        </ul>
      )}
    </section>
  );
}

interface UserRowProps {
  row: RowData;
  expanded: boolean;
  onToggle: () => void;
}

function UserRow({ row, expanded, onToggle }: UserRowProps) {
  const [editingLimit, setEditingLimit] = useState(false);
  const [limitDraft, setLimitDraft] = useState(String(row.limit));
  const [saving, setSaving] = useState(false);
  const [opError, setOpError] = useState<string | null>(null);

  useEffect(() => {
    setLimitDraft(String(row.limit));
  }, [row.limit]);

  const handleSaveLimit = async () => {
    const n = Number(limitDraft);
    if (!Number.isInteger(n) || n < 1 || n > 99) {
      setOpError("Limit phải là số nguyên 1-99");
      return;
    }
    setSaving(true);
    setOpError(null);
    try {
      await setDeviceLimit(row.uid, n);
      setEditingLimit(false);
    } catch (e) {
      setOpError((e as Error).message ?? String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (fingerprint: string, hostname: string) => {
    const ok = window.confirm(
      `Xóa thiết bị "${hostname}" của ${row.email}? User trên máy đó sẽ bị đăng xuất.`,
    );
    if (!ok) return;
    setOpError(null);
    try {
      await removeDevice(row.uid, fingerprint);
    } catch (e) {
      setOpError((e as Error).message ?? String(e));
    }
  };

  return (
    <li>
      <div
        className="flex cursor-pointer items-center gap-3 px-4 py-3 text-sm text-white/85 transition-colors hover:bg-white/5"
        onClick={onToggle}
      >
        <span className="material-symbols-rounded text-base text-white/50">
          {expanded ? "expand_more" : "chevron_right"}
        </span>
        <span
          className="flex-1 truncate"
          title={`${row.email} (${row.uid})`}
        >
          {row.email}
        </span>
        {row.isAdmin && (
          <span className="rounded-full bg-shopee-500/20 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-shopee-300">
            Admin
          </span>
        )}
        <span className="text-xs tabular-nums text-white/50">
          {row.devices.length} / {row.isAdmin ? "∞" : row.limit} máy
        </span>
      </div>
      {expanded && (
        <div className="space-y-2 border-t border-surface-8 bg-surface-2 px-4 py-3">
          {/* Limit editor — chỉ ý nghĩa với non-admin (admin unlimited). */}
          {!row.isAdmin && (
            <div className="flex items-center gap-2 text-xs text-white/70">
              <span>Giới hạn thiết bị:</span>
              {editingLimit ? (
                <>
                  <input
                    type="number"
                    min={1}
                    max={99}
                    value={limitDraft}
                    disabled={saving}
                    onChange={(e) => setLimitDraft(e.currentTarget.value)}
                    onWheel={(e) => (e.target as HTMLInputElement).blur()}
                    className="w-16 rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-right tabular-nums text-shopee-300 focus:border-shopee-500 focus:outline-none"
                  />
                  <button
                    type="button"
                    onClick={handleSaveLimit}
                    disabled={saving}
                    className="btn-ripple rounded-md bg-shopee-500/30 px-2 py-1 text-[11px] font-medium text-shopee-200 hover:bg-shopee-500/40 disabled:opacity-50"
                  >
                    {saving ? "..." : "Lưu"}
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setEditingLimit(false);
                      setLimitDraft(String(row.limit));
                    }}
                    disabled={saving}
                    className="btn-ripple rounded-md bg-surface-1 px-2 py-1 text-[11px] font-medium text-white/70 hover:bg-surface-4"
                  >
                    Hủy
                  </button>
                </>
              ) : (
                <>
                  <span className="rounded-md bg-surface-1 px-2 py-0.5 font-mono tabular-nums text-shopee-300">
                    {row.limit}
                  </span>
                  <button
                    type="button"
                    onClick={() => setEditingLimit(true)}
                    className="btn-ripple flex items-center gap-1 rounded-md bg-surface-1 px-2 py-0.5 text-[11px] font-medium text-white/70 hover:bg-surface-4"
                    title="Sửa limit"
                  >
                    <span className="material-symbols-rounded text-sm">
                      edit
                    </span>
                    Sửa
                  </button>
                </>
              )}
            </div>
          )}

          {opError && (
            <div className="rounded-lg border border-red-500/40 bg-red-900/20 p-2 text-xs text-red-200">
              {opError}
            </div>
          )}

          {row.devices.length === 0 ? (
            <div className="rounded-lg border border-dashed border-surface-12 px-3 py-2 text-xs text-white/50">
              Chưa có thiết bị nào.
            </div>
          ) : (
            <ul className="space-y-1">
              {row.devices.map(({ fingerprint, entry }) => (
                <li
                  key={fingerprint}
                  className="flex items-center gap-3 rounded-lg bg-surface-6 px-3 py-2 text-xs"
                >
                  <span className="material-symbols-rounded text-base text-white/50">
                    {entry.os === "macos"
                      ? "laptop_mac"
                      : entry.os === "windows"
                        ? "desktop_windows"
                        : "computer"}
                  </span>
                  <div className="min-w-0 flex-1">
                    <div
                      className="truncate font-medium text-white/85"
                      title={entry.hostname}
                    >
                      {entry.hostname}{" "}
                      <span className="text-white/40">({entry.os})</span>
                    </div>
                    <div className="mt-0.5 flex gap-3 text-[11px] text-white/45">
                      <span title="Lần login/heartbeat gần nhất">
                        Last:{" "}
                        {entry.lastSeen
                          ? new Date(entry.lastSeen).toLocaleString("vi-VN")
                          : "—"}
                      </span>
                      <span
                        className="truncate font-mono"
                        title={fingerprint}
                      >
                        FP: {fingerprint.slice(0, 12)}…
                      </span>
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => handleDelete(fingerprint, entry.hostname)}
                    className="btn-ripple flex shrink-0 items-center gap-1 rounded-md bg-red-500/20 px-2 py-1 text-[11px] font-medium text-red-200 hover:bg-red-500/30"
                    title="Xóa thiết bị (user sẽ bị đăng xuất)"
                  >
                    <span className="material-symbols-rounded text-sm">
                      delete
                    </span>
                    Xóa
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </li>
  );
}
