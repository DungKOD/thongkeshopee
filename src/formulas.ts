import type {
  Day,
  DayTotals,
  OrderDetail,
  Video,
  VideoComputed,
} from "./types";
import {
  netCommissionRatio,
  sumFiltered,
  type ProfitFees,
} from "./hooks/useSettings";

export interface OrderStats {
  total: number;
  cancelled: number;
  zeroValue: number;
  averageValue: number;
  totalGmv: number;
}

/** Tính chi tiết đơn từ orderDetails. Null nếu không có dữ liệu chi tiết. */
export function computeOrderStats(
  details: OrderDetail[] | undefined,
): OrderStats | null {
  if (!details || details.length === 0) return null;
  const total = details.length;
  const cancelled = details.filter((d) => /hủy|cancel/i.test(d.status)).length;
  const zeroValue = details.filter((d) => d.commission === 0).length;
  const totalGmv = details.reduce((a, d) => a + d.grossValue, 0);
  const averageValue = total === 0 ? 0 : totalGmv / total;
  return { total, cancelled, zeroValue, averageValue, totalGmv };
}

const safeDiv = (a: number, b: number) => (b === 0 ? 0 : a / b);

export function computeVideo(v: Video, fees: ProfitFees): VideoComputed {
  // Ưu tiên đọc từ file (v.cpc), chỉ tính nếu không có
  const cpc = v.cpc ?? safeDiv(v.totalSpend, v.clicks);
  const conversionRate = safeDiv(v.orders, v.clicks) * 100;
  const orderValue = safeDiv(v.commission, v.orders);
  const netCommission = v.commission * netCommissionRatio(fees);
  const profit = netCommission - v.totalSpend;
  const profitMargin = safeDiv(profit, v.totalSpend) * 100;
  return { cpc, conversionRate, orderValue, netCommission, profit, profitMargin };
}

export function computeDayTotals(
  day: Day,
  clickSources: Record<string, boolean>,
  fees: ProfitFees,
): DayTotals {
  const ratio = netCommissionRatio(fees);
  return day.videos.reduce<DayTotals>(
    (acc, v) => {
      acc.clicks += v.clicks;
      acc.shopeeClicks += sumFiltered(v.shopeeClicksByReferrer, clickSources);
      acc.totalSpend += v.totalSpend;
      acc.commission += v.commission;
      acc.profit += v.commission * ratio - v.totalSpend;
      return acc;
    },
    { clicks: 0, shopeeClicks: 0, totalSpend: 0, commission: 0, profit: 0 },
  );
}

export const fmtVnd = (n: number) =>
  new Intl.NumberFormat("vi-VN", { maximumFractionDigits: 0 }).format(
    Math.round(n),
  ) + " đ";

export const fmtPct = (n: number) =>
  new Intl.NumberFormat("vi-VN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(n) + "%";

export const fmtInt = (n: number) =>
  new Intl.NumberFormat("vi-VN").format(Math.round(n));

export const fmtDate = (iso: string) => {
  if (!iso) return "";
  const [y, m, d] = iso.split("-");
  if (!y || !m || !d) return iso;
  return `${d}/${m}/${y}`;
};

/**
 * Khoảng thời gian giữa 2 mốc (ISO datetime). Trả về chuỗi rút gọn.
 * `"2026-04-15 17:17:09"` → `"2026-04-17 23:49:26"` ≈ `"2 ngày 6g"`.
 * Trả về "" nếu thiếu 1 trong 2 hoặc parse lỗi.
 */
export function fmtDuration(fromIso?: string, toIso?: string): string {
  if (!fromIso || !toIso) return "";
  const a = new Date(fromIso.replace(" ", "T")).getTime();
  const b = new Date(toIso.replace(" ", "T")).getTime();
  if (!Number.isFinite(a) || !Number.isFinite(b)) return "";
  let ms = b - a;
  if (ms < 0) return "";
  const sec = Math.floor(ms / 1000);
  const days = Math.floor(sec / 86400);
  const hours = Math.floor((sec % 86400) / 3600);
  const mins = Math.floor((sec % 3600) / 60);
  if (days > 0) return hours > 0 ? `${days} ngày ${hours}g` : `${days} ngày`;
  if (hours > 0) return mins > 0 ? `${hours}g ${mins}p` : `${hours}g`;
  if (mins > 0) return `${mins}p`;
  return `${sec}s`;
}
