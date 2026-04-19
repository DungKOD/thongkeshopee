import { useCallback, useEffect, useState } from "react";
import { invoke } from "../lib/tauri";
import type {
  ManualEntryInput,
  ManualRowKey,
  SubIds,
  UiDay,
} from "../types";
import { uiRowKey } from "../formulas";

export const todayIso = () => new Date().toISOString().slice(0, 10);

/**
 * State + mutations cho data đọc từ SQLite. DB là source of truth;
 * state chỉ là cache để render, invalidate sau mỗi mutation.
 *
 * Staged delete UX:
 * - User click xóa dòng/ngày → toggle vào `pendingRowDeletes` / `pendingDayDeletes`.
 * - UI apply strikethrough cho row/day có trong pending set.
 * - User click "Lưu thay đổi" → gọi `commitPending()` → Tauri batch_commit → refetch.
 * - User click "Hủy thay đổi" → `clearPending()` → reset set, không gọi DB.
 * - Reload app trong khi có pending → pending mất (in-memory), data DB còn nguyên.
 */
export function useDbStats() {
  const [days, setDays] = useState<UiDay[]>([]);
  const [referrers, setReferrers] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Counter bump mỗi khi DB được ghi thành công. Dùng cho auto-sync Drive.
  // KHÔNG bump khi chỉ load (refetch) — chỉ bump sau mutation thực sự.
  const [mutationVersion, setMutationVersion] = useState(0);
  const markMutation = useCallback(() => {
    setMutationVersion((v) => v + 1);
  }, []);

  // In-memory pending state cho staged delete.
  const [pendingRowDeletes, setPendingRowDeletes] = useState<Set<string>>(
    () => new Set(),
  );
  const [pendingDayDeletes, setPendingDayDeletes] = useState<Set<string>>(
    () => new Set(),
  );

  const refetch = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [data, refs] = await Promise.all([
        invoke<UiDay[]>("list_days_with_rows"),
        invoke<string[]>("list_click_referrers"),
      ]);
      setDays(data);
      setReferrers(refs);
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refetch();
  }, [refetch]);

  const saveManualEntry = useCallback(
    async (input: ManualEntryInput) => {
      await invoke<void>("save_manual_entry", { input });
      markMutation();
      await refetch();
    },
    [refetch, markMutation],
  );

  const toggleRowPending = useCallback(
    (dayDate: string, subIds: SubIds) => {
      const key = uiRowKey(dayDate, subIds);
      setPendingRowDeletes((prev) => {
        const next = new Set(prev);
        if (next.has(key)) next.delete(key);
        else next.add(key);
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
    setPendingRowDeletes(new Set());
    setPendingDayDeletes(new Set());
  }, []);

  const commitPending = useCallback(async () => {
    const rowKeys: ManualRowKey[] = [];
    for (const day of days) {
      for (const row of day.rows) {
        const key = uiRowKey(row.dayDate, row.subIds);
        if (
          pendingRowDeletes.has(key) &&
          !pendingDayDeletes.has(row.dayDate) // skip nếu cả ngày đã pending (redundant)
        ) {
          rowKeys.push({ dayDate: row.dayDate, subIds: row.subIds });
        }
      }
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
  }, [days, pendingDayDeletes, pendingRowDeletes, clearPending, refetch, markMutation]);

  const pendingCount = pendingRowDeletes.size + pendingDayDeletes.size;

  return {
    days,
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
