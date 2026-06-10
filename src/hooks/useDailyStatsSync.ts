import { useEffect, useRef } from "react";
import { computeUiDayTotals } from "../formulas";
import { syncDailyStats, type DailyStatsRow } from "../lib/appsScript";
import type { UiDay } from "../types";
import type { ProfitFees } from "./useSettings";

const DEBOUNCE_MS = 2000;

/**
 * Tự động sync `{ngày, tiền ads, hoa hồng, lãi}` của tất cả `days` hiện tại
 * lên Google Sheet (tab `{localPart}_stats`) sau mỗi mutation.
 *
 * Strategy:
 * 1. Sau mỗi `days` change, debounce 2s rồi compute row cho mỗi ngày bằng
 *    `computeUiDayTotals` (đảm bảo công thức khớp UI: profit = netCommission
 *    - spend, netCommission đã trừ tax + reserve).
 * 2. Diff với snapshot lần sync trước (cũng key theo date) — chỉ những ngày
 *    có row đổi (spend / commission / profit khác giá trị cũ) mới được gửi.
 * 3. POST batch 1 lần. Lỗi mạng/auth chỉ console.warn — không block user.
 *
 * Note về phạm vi: hook chỉ thấy `days` thuộc slice hiện tại (theo filter
 * `useDbStats`). Ngày ngoài slice không được sync trong session đó. Khi user
 * đổi filter về toàn thời gian (vd Overview tab) → days bao gồm tất cả →
 * sync tự catch-up phần còn thiếu.
 *
 * Disable bằng `enabled=false` khi user chưa sign-in hoặc chưa hydrate
 * settings (fees chưa ổn định → profit số sẽ sai lệch).
 */
export function useDailyStatsSync(
  days: UiDay[],
  fees: ProfitFees,
  enabled: boolean,
) {
  const snapshotRef = useRef<Map<string, DailyStatsRow>>(new Map());
  const timerRef = useRef<number | null>(null);

  useEffect(() => {
    if (!enabled || days.length === 0) return;

    if (timerRef.current !== null) {
      window.clearTimeout(timerRef.current);
    }

    timerRef.current = window.setTimeout(() => {
      // clickSources không ảnh hưởng spend/commission/profit (chỉ
      // shopeeClicks). Truyền {} là an toàn.
      const clickSources: Record<string, boolean> = {};
      const diff: DailyStatsRow[] = [];

      for (const day of days) {
        const t = computeUiDayTotals(day, clickSources, fees);
        const row: DailyStatsRow = {
          date: day.date,
          spend: Math.round(t.totalSpend),
          commission: Math.round(t.commission),
          profit: Math.round(t.profit),
        };
        const prev = snapshotRef.current.get(day.date);
        if (
          !prev ||
          prev.spend !== row.spend ||
          prev.commission !== row.commission ||
          prev.profit !== row.profit
        ) {
          diff.push(row);
          snapshotRef.current.set(day.date, row);
        }
      }

      if (diff.length === 0) return;

      syncDailyStats(diff).catch((e) => {
        console.warn("[dailyStats] sync Sheet thất bại:", e);
      });
    }, DEBOUNCE_MS);

    return () => {
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current);
      }
    };
  }, [days, fees, enabled]);
}
