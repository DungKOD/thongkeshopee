import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { invoke } from "../lib/tauri";
import type {
  ManualEntryInput,
  ManualRowKey,
  SubIds,
  UiDay,
} from "../types";
import { uiRowKey } from "../formulas";

export const todayIso = () => new Date().toISOString().slice(0, 10);

/** Tagged union — account filter theo mode. Trùng shape Rust
 *  `AccountFilterMode` (tag=kind, camelCase).
 *  `id` là string vì content_id hash > 2^53 — Rust deser flexible accepts
 *  cả string và number, nhưng FE gửi string để preserve precision. */
export type AccountFilterMode =
  | { kind: "all" }
  | { kind: "account"; id: string };

/** Filter args gửi xuống Rust `list_days_with_rows`. Mọi field optional. */
export interface DaysFilter {
  fromDate?: string;
  toDate?: string;
  limit?: number;
  subIdFilter?: string | null;
  /// Account filter. Omit hoặc {kind:"all"} = không filter (backward compat).
  accountFilter?: AccountFilterMode;
}

/** Snapshot toàn DB từ Rust `load_overview`. Gọi 1 lần/mutation, không filter. */
export interface Overview {
  allSubIds: string[];
  totalDaysCount: number;
  totalRowsCount: number;
  oldestDate: string | null;
  newestDate: string | null;
}

const EMPTY_OVERVIEW: Overview = {
  allSubIds: [],
  totalDaysCount: 0,
  totalRowsCount: 0,
  oldestDate: null,
  newestDate: null,
};

interface UseDbStatsOptions {
  filter: DaysFilter;
}

/** Tiến độ load data — 1 step / 1 invoke command. */
export interface LoadProgress {
  done: number;
  total: number;
  label: string;
}

const PROGRESS_LABEL_INIT = "Đang khởi tạo...";
const PROGRESS_LABEL_DAYS = "Đang tải dữ liệu ngày...";
const PROGRESS_LABEL_OVERVIEW = "Đang tải tổng quan...";
const PROGRESS_LABEL_REFERRERS = "Đang tải nguồn click...";
const PROGRESS_LABEL_DONE = "Hoàn tất";

// LRU cap cho list_days_with_rows. User switch filter/account/sub_id qua lại
// nhiều combo → cache đủ rộng để giữ kết quả gần đây.
const DAYS_CACHE_MAX = 32;

/**
 * State + mutations cho data đọc từ SQLite. DB là source of truth;
 * state chỉ là cache để render, invalidate sau mỗi mutation.
 *
 * 2 nguồn fetch:
 * - `list_days_with_rows(filter)`: slice days theo filter (recent/range + sub_id).
 *   Refetch mỗi khi `filter` đổi.
 * - `load_overview()`: suggestions + counters + date bounds cho toàn DB.
 *   Chỉ refetch sau mutation hoặc swap DB (admin view). KHÔNG phụ thuộc filter.
 *
 * Staged delete UX:
 * - User click xóa dòng/ngày → toggle vào `pendingRowDeletes` (Map) / `pendingDayDeletes` (Set).
 * - UI apply strikethrough cho row/day có trong pending.
 * - User click "Lưu thay đổi" → `commitPending()` → batch_commit → refetch.
 * - User click "Hủy thay đổi" → `clearPending()` → reset, không gọi DB.
 * - Pending row lưu cả `{dayDate, subIds}` trong Map value → commit KHÔNG cần
 *   scan `days` cache. An toàn cả khi row pending ngoài slice hiện tại.
 */
/** Compute filterKey từ DaysFilter — share giữa hook và consumer cần check
 *  days đã thuộc về filter nào (vd Overview tab cần biết days đang load đúng
 *  filter của mình hay đang là filter của Stats tab). Đồng bộ format với
 *  filterKey nội bộ ở useDbStats để comparison chính xác. */
export function makeFilterKey(filter: DaysFilter): string {
  return JSON.stringify({
    fromDate: filter.fromDate ?? null,
    toDate: filter.toDate ?? null,
    limit: filter.limit ?? null,
    subIdFilter: filter.subIdFilter ?? null,
    accountFilter: filter.accountFilter ?? null,
  });
}

export function useDbStats({ filter }: UseDbStatsOptions) {
  const [days, setDays] = useState<UiDay[]>([]);
  /** filterKey của data hiện đang trong `days` state. Khi !== expected key
   *  của 1 tab cụ thể → tab đó biết days đang stale, nên giữ cache cũ. */
  const [daysFilterKey, setDaysFilterKey] = useState<string>("");
  const [overview, setOverview] = useState<Overview>(EMPTY_OVERVIEW);
  const [referrers, setReferrers] = useState<string[]>([]);
  // `loading` = chưa bao giờ có data → blank screen + progress lớn.
  // `refreshing` = đã có data + đang fetch (SWR) → giữ content cũ + indicator nhỏ.
  // Phân tách 2 state để filter switch / mutation KHÔNG flash blank UI.
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<LoadProgress>(() => ({
    done: 0,
    total: 0,
    label: PROGRESS_LABEL_INIT,
  }));

  // Mỗi step xong → tăng done, đổi label theo step kế tiếp (label giữ là
  // step VỪA xong để user thấy đã hoàn tất phần nào).
  const advanceProgress = useCallback((label: string) => {
    setProgress((p) => ({
      done: Math.min(p.done + 1, p.total),
      total: p.total,
      label,
    }));
  }, []);

  // Pending state cho staged delete.
  // Row: Map<key, ManualRowKey> để commit reconstruct payload KHÔNG qua scan `days`.
  const [pendingRowDeletes, setPendingRowDeletes] = useState<
    Map<string, ManualRowKey>
  >(() => new Map());
  const [pendingDayDeletes, setPendingDayDeletes] = useState<Set<string>>(
    () => new Set(),
  );

  // Memoize filter object để args ổn định — tránh refetch loop khi caller
  // tạo object mới mỗi render. Key serialize theo field tuần tự — dùng cùng
  // helper `makeFilterKey` để consumer có thể compare đúng định dạng.
  const filterKey = useMemo(
    () => makeFilterKey(filter),
    [
      filter.fromDate,
      filter.toDate,
      filter.limit,
      filter.subIdFilter,
      filter.accountFilter,
    ],
  );

  // Cache list_days_with_rows theo filterKey: user switch stats↔overview tab
  // (2 filter khác nhau) lặp lại sẽ hit cache → instant, không re-fetch.
  // Invalidate trong refetch() sau mutation. LRU cap 8 entries để không leak.
  const daysCacheRef = useRef<Map<string, UiDay[]>>(new Map());
  // Ref tracking filterKey hiện tại — guard chống race khi user đổi filter
  // giữa lúc invoke đang pending. Stale response phải KHÔNG được setDays /
  // advanceProgress vì sẽ overwrite kết quả mới hơn (worst case: invoke cũ
  // resolve sau invoke mới → UI kẹt vĩnh viễn ở data filter cũ).
  const latestFilterKeyRef = useRef(filterKey);
  latestFilterKeyRef.current = filterKey;
  const refetchDays = useCallback(async () => {
    const cached = daysCacheRef.current.get(filterKey);
    if (cached) {
      // Cache hit: sync setState (KHÔNG startTransition) → React batch với
      // render hiện tại → user thấy data mới ngay frame kế. LazyDayBlock đã
      // tách mount cost nên reconcile rẻ. startTransition đây làm delay 1-2
      // frame → cảm giác "lag" khi switch tab/filter.
      setDays(cached);
      setDaysFilterKey(filterKey);
      advanceProgress(PROGRESS_LABEL_DAYS);
      return;
    }
    const payload: DaysFilter = {
      fromDate: filter.fromDate,
      toDate: filter.toDate,
      limit: filter.limit,
      subIdFilter: filter.subIdFilter ?? undefined,
      accountFilter: filter.accountFilter,
    };
    const data = await invoke<UiDay[]>("list_days_with_rows", {
      filter: payload,
    });
    // Cache populate luôn (key + data khớp với invoke vừa chạy) — kể cả khi
    // stale: user click lại filter này sau đó sẽ hit cache instant.
    const cache = daysCacheRef.current;
    if (cache.size >= DAYS_CACHE_MAX) {
      const oldestKey = cache.keys().next().value;
      if (oldestKey !== undefined) cache.delete(oldestKey);
    }
    cache.set(filterKey, data);
    // Guard stale: nếu user đã đổi filter trong lúc invoke chạy, KHÔNG apply
    // state — để invoke mới hơn nắm quyền. Check qua ref vì closure capture
    // filterKey lúc tạo refetchDays, còn ref luôn trỏ tới filterKey hiện tại.
    if (latestFilterKeyRef.current !== filterKey) return;
    advanceProgress(PROGRESS_LABEL_DAYS);
    // Sync setState (KHÔNG startTransition): bọc transition khiến setDays
    // queue low-priority, trong khi setRefreshing(false) sau Promise.all
    // resolve là urgent → React commit refreshing=false + days CŨ, transition
    // update "kẹt" cho đến khi user tương tác (vd switch tab) trigger commit.
    // useDeferredValue ở consumer vẫn defer được render cost cho list dài.
    setDays(data);
    setDaysFilterKey(filterKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterKey, advanceProgress]);

  const refetchOverviewOnly = useCallback(async () => {
    const ov = await invoke<Overview>("load_overview");
    // Overview chứa allSubIds (có thể 10k+) → dropdown filter re-render nặng,
    // nhưng vẫn cần urgent để consumer (KPI counters, badge "N/M ngày") đồng
    // bộ với days. Consumer tự defer nếu cần (vd OverviewTab dùng useDeferredValue).
    advanceProgress(PROGRESS_LABEL_OVERVIEW);
    setOverview(ov);
  }, [advanceProgress]);

  const refetchReferrers = useCallback(async () => {
    const refs = await invoke<string[]>("list_click_referrers");
    advanceProgress(PROGRESS_LABEL_REFERRERS);
    setReferrers(refs);
  }, [advanceProgress]);

  const refetch = useCallback(async () => {
    // Mutation/manual refresh → invalidate cache để fetch lại từ DB.
    // SWR: dùng `refreshing` (không clear days) → UI giữ data cũ cho đến khi
    // fetch xong, tránh blank flash sau khi user lưu/import.
    daysCacheRef.current.clear();
    setRefreshing(true);
    setError(null);
    setProgress({ done: 0, total: 3, label: PROGRESS_LABEL_DAYS });
    try {
      await Promise.all([
        refetchDays(),
        refetchOverviewOnly(),
        refetchReferrers(),
      ]);
      setProgress((p) => ({ ...p, label: PROGRESS_LABEL_DONE }));
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setRefreshing(false);
    }
  }, [refetchDays, refetchOverviewOnly, refetchReferrers]);

  // Initial mount: fetch cả days + overview song song. Subsequent filter
  // changes: chỉ refetch days (overview không phụ thuộc filter).
  // `mountedRef` đánh dấu "first-mount effect ĐÃ BẮT ĐẦU" (set sync trong
  // body), KHÔNG phải "first-mount đã hoàn tất". Trước đây set sau Promise.all
  // resolve → khi filter đổi giữa initial mount (vd AccountContext refresh
  // validate filter), Effect-B body thấy mountedRef=false → setLoading(true)
  // lại + reset progress → splash hiển thị lại + phải đợi Promise.all-B mới
  // thoát (label kẹt ở "Đang tải nguồn click..." 100% khi B chậm/hang).
  // Sync ngay khi body chạy: Effect-B luôn đi refresh path → giữ splash
  // hiện tại, KHÔNG re-set loading=true. Tradeoff: nếu Effect-A's invokes
  // fail hết, overview/referrers có thể trống → user phải click "Tải lại".
  const mountedRef = useRef(false);
  useEffect(() => {
    let cancelled = false;
    const isFirstMount = !mountedRef.current;
    // Set SYNC trong body: subsequent effect runs (filter change, StrictMode
    // re-mount) sẽ thấy mountedRef=true → isFirstMount=false → đi refresh
    // path, KHÔNG setLoading(true) lại.
    if (isFirstMount) mountedRef.current = true;

    // SWR fast path: filter switch → cache hit → swap days đồng bộ.
    // Sync setState (KHÔNG startTransition) để tab switch / filter switch
    // thấy data mới ngay frame kế, không delay 1-2 frame như non-urgent.
    if (!isFirstMount && daysCacheRef.current.has(filterKey)) {
      const cached = daysCacheRef.current.get(filterKey)!;
      setDays(cached);
      setDaysFilterKey(filterKey);
      setProgress({ done: 1, total: 1, label: PROGRESS_LABEL_DONE });
      // PHẢI reset loading flags: nếu effect trước đó đã setRefreshing(true)
      // rồi bị cancel (user spam-click filter), iife của effect đó skip
      // finally do `cancelled=true` → refreshing kẹt true vĩnh viễn. Cache-hit
      // path KHÔNG fetch nên data đã ready ngay → an toàn reset cả 2 flag.
      setRefreshing(false);
      setLoading(false);
      return;
    }

    setError(null);
    if (isFirstMount) {
      setLoading(true);
      setProgress({ done: 0, total: 3, label: PROGRESS_LABEL_DAYS });
    } else {
      // Subsequent fetch + cache miss → SWR: giữ data hiện tại, show
      // indicator nhỏ, swap khi fetch xong.
      setRefreshing(true);
      setProgress({ done: 0, total: 1, label: PROGRESS_LABEL_DAYS });
    }
    (async () => {
      try {
        if (isFirstMount) {
          await Promise.all([
            refetchDays(),
            refetchOverviewOnly(),
            refetchReferrers(),
          ]);
        } else {
          await refetchDays();
        }
        if (!cancelled) {
          setProgress((p) => ({ ...p, label: PROGRESS_LABEL_DONE }));
        }
      } catch (e) {
        if (!cancelled) setError((e as Error).message ?? String(e));
      } finally {
        // KHÔNG check `cancelled`: finally luôn clear flags để effect cũ bị
        // cancel không kẹt loading/refreshing true vĩnh viễn.
        setLoading(false);
        setRefreshing(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [refetchDays, refetchOverviewOnly, refetchReferrers, filterKey]);

  // Watchdog: progress đạt 100% mà loading/refreshing vẫn true sau 1.5s →
  // force clear. Safety net cho edge case BE hang (vd 1 invoke trong
  // Promise.all không bao giờ resolve do DB lock contention / Tauri IPC
  // backlog) → user vẫn thấy được content thay vì kẹt splash vĩnh viễn.
  // 1.5s đủ dài để không trigger trong flow bình thường (Promise.all resolve
  // → setProgress(DONE) → finally clear flags, gap < 16ms typically).
  useEffect(() => {
    if (!loading && !refreshing) return;
    if (progress.total === 0 || progress.done < progress.total) return;
    const timer = setTimeout(() => {
      setLoading(false);
      setRefreshing(false);
    }, 1500);
    return () => clearTimeout(timer);
  }, [loading, refreshing, progress.done, progress.total]);

  const saveManualEntry = useCallback(
    async (input: ManualEntryInput) => {
      await invoke<boolean>("save_manual_entry", { input });
      await refetch();
    },
    [refetch],
  );

  const toggleRowPending = useCallback(
    (dayDate: string, subIds: SubIds, accountId: string | null) => {
      const key = uiRowKey(dayDate, subIds, accountId);
      setPendingRowDeletes((prev) => {
        const next = new Map(prev);
        if (next.has(key)) next.delete(key);
        else next.set(key, { dayDate, subIds, accountId });
        return next;
      });
    },
    [],
  );

  const toggleDayPending = useCallback((date: string) => {
    setPendingDayDeletes((prev) => {
      const next = new Set(prev);
      if (next.has(date)) next.delete(date);
      else next.add(date);
      return next;
    });
  }, []);

  const clearPending = useCallback(() => {
    setPendingRowDeletes(new Map());
    setPendingDayDeletes(new Set());
  }, []);

  const commitPending = useCallback(async () => {
    // Build payload trực tiếp từ Map values — không cần `days` cache.
    // Skip row nếu cả ngày đã pending (redundant, BE CASCADE khi xóa day).
    const rowKeys: ManualRowKey[] = Array.from(pendingRowDeletes.values()).filter(
      (k) => !pendingDayDeletes.has(k.dayDate),
    );

    // BE scope DELETE theo `accountId` của từng row (manual + raw_shopee_*),
    // FB ads chỉ wipe khi accountId=null ("FB chung"). Không cần guard cross-
    // account ở FE nữa — DB đảm bảo isolate.
    await invoke<{ daysDeleted: number; rowsDeleted: number }>(
      "batch_commit_deletes",
      {
        payload: {
          days: Array.from(pendingDayDeletes),
          manualRows: rowKeys,
        },
      },
    );
    clearPending();
    await refetch();
  }, [
    pendingDayDeletes,
    pendingRowDeletes,
    clearPending,
    refetch,
  ]);

  const pendingCount = pendingRowDeletes.size + pendingDayDeletes.size;

  return {
    days,
    daysFilterKey,
    overview,
    referrers,
    loading,
    refreshing,
    progress,
    error,
    refetch,
    saveManualEntry,
    pendingRowDeletes,
    pendingDayDeletes,
    toggleRowPending,
    toggleDayPending,
    clearPending,
    commitPending,
    pendingCount,
  };
}
