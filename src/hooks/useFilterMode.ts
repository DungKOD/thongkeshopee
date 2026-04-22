import { useCallback, useEffect, useState } from "react";
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

export const DEFAULT_RECENT = 7;
export const LOAD_MORE_STEP = 7;
export const DEFAULT_MODE: FilterMode = {
  type: "recent",
  count: DEFAULT_RECENT,
  canExpand: true,
};
/** Overview scope mặc định 1 ngày gần nhất — khớp với shortcut "Ngày gần
 *  nhất" (canExpand=false, count=1) để UI highlight đúng. User click
 *  shortcut khác để mở rộng range. */
const OVERVIEW_DEFAULT_MODE: FilterMode = {
  type: "recent",
  count: 1,
  canExpand: false,
};
const defaultModeFor = (scope: FilterScope): FilterMode =>
  scope === "overview" ? OVERVIEW_DEFAULT_MODE : DEFAULT_MODE;

/** Prefix + scope → key localStorage. 2 tab persist độc lập. */
const STORAGE_PREFIX = "thongkeshopee.filter.v2";
const storageKey = (scope: FilterScope) => `${STORAGE_PREFIX}.${scope}`;

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

function loadFilterMode(scope: FilterScope): FilterMode {
  const fallback = defaultModeFor(scope);
  try {
    // Migrate: legacy key (trước khi tách scope) dùng cho tab "stats".
    const raw =
      localStorage.getItem(storageKey(scope)) ??
      (scope === "stats" ? localStorage.getItem(STORAGE_PREFIX) : null);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    if (parsed?.type === "recent" && Number.isFinite(parsed.count)) {
      return {
        type: "recent",
        count: Math.max(1, Math.floor(parsed.count)),
        canExpand: !!parsed.canExpand,
      };
    }
    if (parsed?.type === "range" && typeof parsed.from === "string") {
      return {
        type: "range",
        from: parsed.from,
        to: typeof parsed.to === "string" ? parsed.to : "",
      };
    }
    if (parsed?.type === "all") return { type: "all" };
  } catch {
    // ignore
  }
  return fallback;
}

function saveFilterMode(scope: FilterScope, m: FilterMode) {
  try {
    localStorage.setItem(storageKey(scope), JSON.stringify(m));
  } catch {
    // ignore
  }
}

/**
 * State + handlers của filter bar cho 1 scope (tab). Mọi tab dùng **cùng
 * logic** — chỉ khác state instance + localStorage key. Callers gọi hook
 * cho mỗi scope, route theo `activeTab`.
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
  /** Về default (recent 7, canExpand). */
  clear: () => void;
  /** Date picker "Từ ngày" → chuyển sang range mode. */
  setDateFrom: (v: string) => void;
  /** Date picker "Đến ngày" → chuyển sang range mode. */
  setDateTo: (v: string) => void;
}

export function useFilterMode(scope: FilterScope): UseFilterModeResult {
  const [mode, setMode] = useState<FilterMode>(() => loadFilterMode(scope));

  useEffect(() => {
    saveFilterMode(scope, mode);
  }, [scope, mode]);

  const setRecent = useCallback(
    (n: number) => setMode({ type: "recent", count: n, canExpand: false }),
    [],
  );
  const setPrevMonth = useCallback(() => {
    const r = prevMonthRange();
    setMode({ type: "range", from: r.from, to: r.to });
  }, []);
  const setAllTime = useCallback(() => setMode({ type: "all" }), []);
  const clear = useCallback(() => setMode(defaultModeFor(scope)), [scope]);
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
