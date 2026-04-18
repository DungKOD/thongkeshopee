import type { DayTotals, UiDay, UiRow, VideoComputed } from "./types";
import {
  netCommissionRatio,
  sumFiltered,
  type ProfitFees,
} from "./hooks/useSettings";

const safeDiv = (a: number, b: number) => (b === 0 ? 0 : a / b);

/** Row identity dạng string để dùng làm key trong Set/Map. */
export function uiRowKey(dayDate: string, subIds: readonly string[]): string {
  return `${dayDate}|${subIds.join("\x1f")}`;
}

/**
 * Derived values cho 1 UiRow. Rule: **field nào có trong DB thì lấy thẳng,
 * không có mới tính fallback**.
 *
 * - `cpc`: ưu tiên `row.cpc` (từ raw_fb_ad_groups.link_cpc weighted), fallback `spend/clicks`
 * - `orderValue`: ưu tiên `orderValueTotal/ordersCount` (GMV/đơn)
 * - `conversionRate`: **tính theo click Shopee** (gần với thực tế hơn FB click — user vào Shopee
 *   mới có khả năng mua). Tham số `shopeeClicks` truyền từ caller (đã filter theo settings).
 * - `netCommission`, `profit`, `profitMargin`: luôn tính (không có field DB sẵn)
 */
export function computeUiRow(
  row: UiRow,
  fees: ProfitFees,
  shopeeClicks: number = 0,
): VideoComputed {
  const clicks = row.adsClicks ?? 0;
  const spend = row.totalSpend ?? 0;
  const cpc = row.cpc ?? safeDiv(spend, clicks);
  const orders = row.ordersCount;
  const commission = row.commissionTotal;
  const conversionRate = safeDiv(orders, shopeeClicks) * 100;
  const orderValue = safeDiv(row.orderValueTotal, orders);
  const netCommission = commission * netCommissionRatio(fees);
  const profit = netCommission - spend;
  const profitMargin = safeDiv(profit, spend) * 100;
  return { cpc, conversionRate, orderValue, netCommission, profit, profitMargin };
}

export function computeUiDayTotals(
  day: UiDay,
  clickSources: Record<string, boolean>,
  fees: ProfitFees,
): DayTotals {
  const ratio = netCommissionRatio(fees);
  return day.rows.reduce<DayTotals>(
    (acc, r) => {
      acc.clicks += r.adsClicks ?? 0;
      acc.shopeeClicks += sumFiltered(r.shopeeClicksByReferrer, clickSources);
      acc.totalSpend += r.totalSpend ?? 0;
      acc.orders += r.ordersCount;
      acc.commission += r.commissionTotal;
      acc.profit += r.commissionTotal * ratio - (r.totalSpend ?? 0);
      return acc;
    },
    {
      clicks: 0,
      shopeeClicks: 0,
      totalSpend: 0,
      orders: 0,
      commission: 0,
      profit: 0,
    },
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
 * Khoảng cách giữa 2 mốc datetime (format ISO hoặc "YYYY-MM-DD HH:MM:SS").
 * Trả chuỗi rút gọn, "" nếu thiếu hoặc parse lỗi.
 */
export function fmtDuration(fromIso?: string | null, toIso?: string | null): string {
  if (!fromIso || !toIso) return "";
  const a = new Date(fromIso.replace(" ", "T")).getTime();
  const b = new Date(toIso.replace(" ", "T")).getTime();
  if (!Number.isFinite(a) || !Number.isFinite(b)) return "";
  const ms = b - a;
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
