import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
export function useDbStats({ filter }: UseDbStatsOptions) {
  const [days, setDays] = useState<UiDay[]>([]);
  const [overview, setOverview] = useState<Overview>(EMPTY_OVERVIEW);
  const [referrers, setReferrers] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Counter bump mỗi khi DB được ghi thành công. Dùng cho auto-sync R2.
  // KHÔNG bump khi chỉ load (refetch) — chỉ bump sau mutation thực sự.
  const [mutationVersion, setMutationVersion] = useState(0);
  const markMutation = useCallback(() => {
    setMutationVersion((v) => v + 1);
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
  // tạo object mới mỗi render. Key serialize theo field tuần tự.
  const filterKey = useMemo(
    () =>
      JSON.stringify({
        fromDate: filter.fromDate ?? null,
        toDate: filter.toDate ?? null,
        limit: filter.limit ?? null,
        subIdFilter: filter.subIdFilter ?? null,
        accountFilter: filter.accountFilter ?? null,
      }),
    [
      filter.fromDate,
      filter.toDate,
      filter.limit,
      filter.subIdFilter,
      filter.accountFilter,
    ],
  );

  const refetchDays = useCallback(async () => {
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
    setDays(data);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterKey]);

  const refetchOverview = useCallback(async () => {
    const [ov, refs] = await Promise.all([
      invoke<Overview>("load_overview"),
      invoke<string[]>("list_click_referrers"),
    ]);
    setOverview(ov);
    setReferrers(refs);
  }, []);

  const refetch = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      await Promise.all([refetchDays(), refetchOverview()]);
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setLoading(false);
    }
  }, [refetchDays, refetchOverview]);

  // Initial mount: fetch cả days + overview song song. Subsequent filter
  // changes: chỉ refetch days (overview không phụ thuộc filter).
  // `mountedRef` phân biệt lần đầu (cần overview) với các lần sau.
  const mountedRef = useRef(false);
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    (async () => {
      try {
        if (!mountedRef.current) {
          await Promise.all([refetchDays(), refetchOverview()]);
          mountedRef.current = true;
        } else {
          await refetchDays();
        }
      } catch (e) {
        if (!cancelled) setError((e as Error).message ?? String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [refetchDays, refetchOverview]);

  const saveManualEntry = useCallback(
    async (input: ManualEntryInput) => {
      await invoke<void>("save_manual_entry", { input });
      markMutation();
      await refetch();
    },
    [refetch, markMutation],
  );

  const toggleRowPending = useCallback(
    (dayDate: string, subIds: SubIds, accountId: string | null) => {
      const key = uiRowKey(dayDate, subIds, accountId);
      setPendingRowDeletes((prev) => {
        const next = new Map(prev);
        if (next.has(key)) next.delete(key);
        else next.set(key, { dayDate, subIds });
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

    // Guard multi-account: BE delete_prefix_compatible KHÔNG filter
    // shopee_account_id → wipe data của TẤT CẢ TK trên tuple đó. Khi user
    // pending row ở filter=Account(A) rồi switch filter=All, có thể hiện ra
    // tuple cùng có data ở TK khác. Chặn commit + báo user trở lại filter
    // 1 TK rồi review/clear pending. Quét `days` slice hiện tại để check.
    const conflictTuples: string[] = [];
    for (const k of rowKeys) {
      const day = days.find((d) => d.date === k.dayDate);
      if (!day) continue;
      const matchedAccs = new Set<string | null>();
      const tupleKey = k.subIds.join("\x1f");
      for (const r of day.rows) {
        if (r.subIds.join("\x1f") === tupleKey) matchedAccs.add(r.accountId);
      }
      if (matchedAccs.size >= 2) {
        const label = k.subIds.filter(Boolean).join("-") || "(empty)";
        conflictTuples.push(`${k.dayDate} – ${label}`);
      }
    }
    if (conflictTuples.length > 0) {
      throw new Error(
        `Có ${conflictTuples.length} dòng pending thuộc sub_id chia sẻ giữa nhiều TK ` +
          `(${conflictTuples.slice(0, 3).join("; ")}${conflictTuples.length > 3 ? "; ..." : ""}). ` +
          `Commit sẽ wipe data của tất cả TK trên các tuple này. ` +
          `Hãy chuyển dropdown TK sang TK cụ thể, "Hủy" pending hiện tại, rồi tạo lại.`,
      );
    }

    await invoke<{ daysDeleted: number; rowsDeleted: number }>(
      "batch_commit_deletes",
      {
        payload: {
          days: Array.from(pendingDayDeletes),
          manualRows: rowKeys,
        },
      },
    );
    markMutation();
    clearPending();
    await refetch();
  }, [
    pendingDayDeletes,
    pendingRowDeletes,
    clearPending,
    refetch,
    markMutation,
    days,
  ]);

  const pendingCount = pendingRowDeletes.size + pendingDayDeletes.size;

  return {
    days,
    overview,
    referrers,
    loading,
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
    mutationVersion,
    markMutation,
  };
}
