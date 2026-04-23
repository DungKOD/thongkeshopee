import { useCallback, useState } from "react";
import type { Dispatch, SetStateAction } from "react";

/**
 * Filter mode:
 * - `recent` có `canExpand`:
 *   - `canExpand=true`  → mặc định paginated 7, scroll xuống load more 7 ngày.
 *   - `canExpand=false` → shortcut khóa cứng N ngày, scroll không load thêm.
 * - `range`: khoảng ngày explicit (date picker hoặc shortcut "Tháng trước").
 * - `all`: hiện toàn bộ data ("Từ trước đến nay").
 */
export type FilterMode =
  | { type: "recent"; count: number; canExpand: boolean }
  | { type: "range"; from: string; to: string }
  | { type: "all" };

export type FilterScope = "stats" | "overview";

export const LOAD_MORE_STEP = 7;
/** Mặc định BOTH scopes (stats + overview) = 1 ngày gần nhất — khớp với
 *  shortcut "Ngày gần nhất" (canExpand=false, count=1) để UI highlight
 *  đúng. User click shortcut khác (7/14/30 ngày / range) để mở rộng. */
export const DEFAULT_MODE: FilterMode = {
  type: "recent",
  count: 1,
  canExpand: false,
};

/// Cleanup legacy localStorage keys từ versions trước (khi filter có persist
/// qua localStorage). Giờ state in-memory only → keys cũ orphan, xóa 1 lần
/// cho gọn. Flag persistent để không wipe mỗi startup.
const CLEANUP_FLAG = "thongkeshopee.filter.memory-only-cleanup.v1";
const LEGACY_KEYS = [
  "thongkeshopee.filter.v2",
  "thongkeshopee.filter.v2.stats",
  "thongkeshopee.filter.v2.overview",
  "thongkeshopee.filter.default-migration.v1",
];
function cleanupLegacyStorageOnce(): void {
  try {
    if (localStorage.getItem(CLEANUP_FLAG)) return;
    for (const k of LEGACY_KEYS) localStorage.removeItem(k);
    localStorage.setItem(CLEANUP_FLAG, "1");
  } catch {
    // ignore — quota/privacy mode
  }
}
cleanupLegacyStorageOnce();

/** Đầu + cuối của tháng trước so với today local. Trả YYYY-MM-DD. */
export function prevMonthRange(): { from: string; to: string } {
  const now = new Date();
  // Day 0 của month hiện tại = last day của previous month.
  const end = new Date(now.getFullYear(), now.getMonth(), 0);
  const start = new Date(end.getFullYear(), end.getMonth(), 1);
  const fmt = (d: Date) => {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
  };
  return { from: fmt(start), to: fmt(end) };
}

/**
 * State + handlers của filter bar cho 1 scope (tab). Mọi tab dùng **cùng
 * logic** — chỉ khác state instance. Callers gọi hook cho mỗi scope, route
 * theo `activeTab`.
 *
 * **Persistence = in-memory only.** Reload app / logout → state mất → default
 * "1 ngày gần nhất". Chuyển tab stats ↔ overview: giữ nguyên vì 2 instance
 * state độc lập survive qua render. Logout-login: AppInner remount qua
 * `<SettingsProvider key={user.uid}>` trong AuthGate → fresh state.
 *
 * Returns object (theo rule react-frontend.md).
 */
export interface UseFilterModeResult {
  mode: FilterMode;
  setMode: Dispatch<SetStateAction<FilterMode>>;
  /** Shortcut: N ngày gần nhất (không expand). */
  setRecent: (n: number) => void;
  /** Shortcut: tháng trước (range). */
  setPrevMonth: () => void;
  /** Shortcut: từ trước đến nay (all). */
  setAllTime: () => void;
  /** Về default (1 ngày gần nhất). */
  clear: () => void;
  /** Date picker "Từ ngày" → chuyển sang range mode. */
  setDateFrom: (v: string) => void;
  /** Date picker "Đến ngày" → chuyển sang range mode. */
  setDateTo: (v: string) => void;
}

export function useFilterMode(_scope: FilterScope): UseFilterModeResult {
  const [mode, setMode] = useState<FilterMode>(DEFAULT_MODE);

  const setRecent = useCallback(
    (n: number) => setMode({ type: "recent", count: n, canExpand: false }),
    [],
  );
  const setPrevMonth = useCallback(() => {
    const r = prevMonthRange();
    setMode({ type: "range", from: r.from, to: r.to });
  }, []);
  const setAllTime = useCallback(() => setMode({ type: "all" }), []);
  const clear = useCallback(() => setMode(DEFAULT_MODE), []);
  const setDateFrom = useCallback((v: string) => {
    setMode((m) => ({
      type: "range",
      from: v,
      to: m.type === "range" ? m.to : "",
    }));
  }, []);
  const setDateTo = useCallback((v: string) => {
    setMode((m) => ({
      type: "range",
      from: m.type === "range" ? m.from : "",
      to: v,
    }));
  }, []);

  return {
    mode,
    setMode,
    setRecent,
    setPrevMonth,
    setAllTime,
    clear,
    setDateFrom,
    setDateTo,
  };
}
