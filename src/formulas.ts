import type { DayTotals, UiDay, UiRow, VideoComputed } from "./types";
import {
  netCommissionRatio,
  sumFiltered,
  type ProfitFees,
} from "./hooks/useSettings";

const safeDiv = (a: number, b: number) => (b === 0 ? 0 : a / b);

/**
 * Nguồn data muốn aggregate:
 * - `all`: tất cả row (FB + Shopee)
 * - `shopee_only`: chỉ row có Shopee data (hasShopeeOrders || hasShopeeClicks)
 *
 * Công thức profit KHÔNG đổi theo source — chỉ filter rows vào aggregate.
 */
export type SourceFilter = "all" | "shopee_only";

/** Predicate: row có thuộc source filter không. */
export function rowMatchesSource(row: UiRow, source: SourceFilter): boolean {
  if (source === "all") return true;
  if (source === "shopee_only") return row.hasShopeeOrders || row.hasShopeeClicks;
  return true;
}

/** Aggregate tổng cho nhiều ngày — mở rộng DayTotals thêm GMV + counters. */
export type OverviewTotals = DayTotals & {
  orderValueTotal: number;
  netCommission: number;
  daysCount: number;
  rowsCount: number;
};

/** 1 sản phẩm sau khi aggregate qua nhiều ngày (key = sub_id tuple). */
export type AggregatedProductRow = {
  subIds: readonly string[];
  displayName: string;
  adsClicks: number;
  shopeeClicks: number;
  totalSpend: number;
  ordersCount: number;
  commissionTotal: number;
  orderValueTotal: number;
  netCommission: number;
  profit: number;
  /** Số ngày sản phẩm có data trong khoảng. */
  daysActive: number;
};

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

/**
 * DayTotals cho 1 ngày = wrapper của `computeOverviewTotals` với input 1 day.
 * Giữ signature cũ để không đụng callsite DayBlock / VideoRow, nhưng logic
 * thực chạy qua Overview — sửa công thức 1 chỗ đúng cho tất cả.
 */
export function computeUiDayTotals(
  day: UiDay,
  clickSources: Record<string, boolean>,
  fees: ProfitFees,
): DayTotals {
  const t = computeOverviewTotals([day], clickSources, fees, "all");
  return {
    clicks: t.clicks,
    shopeeClicks: t.shopeeClicks,
    totalSpend: t.totalSpend,
    orders: t.orders,
    commission: t.commission,
    profit: t.profit,
  };
}

/**
 * Aggregate tổng cho nhiều UiDay. Cùng công thức với `computeUiDayTotals`
 * khi source='all' (đảm bảo Overview = Σ DayBlock totals).
 *
 * Source filter:
 * - `all`: dùng cả ads (spend, clicks) + shopee → profit = net_commission - spend
 * - `shopee_only`: chỉ row có Shopee data; **bỏ hoàn toàn ads** (spend=0,
 *   adsClicks=0) → profit = net_commission (thuần hoa hồng affiliate)
 *
 * Dùng CHUNG cho: Overview tab, DayBlock totals (1-day wrapper), test validator.
 */
export function computeOverviewTotals(
  days: readonly UiDay[],
  clickSources: Record<string, boolean>,
  fees: ProfitFees,
  source: SourceFilter = "all",
): OverviewTotals {
  const ratio = netCommissionRatio(fees);
  const includeAds = source === "all";
  const acc: OverviewTotals = {
    clicks: 0,
    shopeeClicks: 0,
    totalSpend: 0,
    orders: 0,
    commission: 0,
    profit: 0,
    orderValueTotal: 0,
    netCommission: 0,
    daysCount: 0,
    rowsCount: 0,
  };
  for (const day of days) {
    // Source filter 'shopee_only': totals SUM vẫn dùng day.totals (chính xác
    // pre-filter), nhưng bỏ ads spend/clicks → lấy net commission thuần.
    // Source 'all': dùng toàn bộ day.totals.
    //
    // KPI PHẢI lấy từ day.totals (pre row-0 filter) — nếu sum từ day.rows sẽ
    // miss tuple chỉ có click/order nhưng spend=0 & commission=0 (row-0 drop).
    const t = day.totals;
    const spend = includeAds ? t.totalSpend : 0;
    const adsClicks = includeAds ? t.adsClicks : 0;
    const net = t.commissionTotal * ratio;
    acc.clicks += adsClicks;
    acc.shopeeClicks += sumFiltered(t.shopeeClicksByReferrer, clickSources);
    acc.totalSpend += spend;
    acc.orders += t.ordersCount;
    acc.commission += t.commissionTotal;
    acc.netCommission += net;
    acc.profit += net - spend;
    acc.orderValueTotal += t.orderValueTotal;

    // daysCount/rowsCount vẫn dựa vào row-level vì là metrics hiển thị UI.
    let dayContributed = false;
    for (const r of day.rows) {
      if (!rowMatchesSource(r, source)) continue;
      acc.rowsCount += 1;
      dayContributed = true;
    }
    // Nếu day có data từ raw (totals != 0) nhưng mọi row bị filter → vẫn count.
    if (!dayContributed) {
      const hasAnyData =
        t.adsClicks > 0 ||
        t.shopeeClicksTotal > 0 ||
        t.ordersCount > 0 ||
        t.commissionTotal !== 0 ||
        t.totalSpend !== 0;
      if (hasAnyData) dayContributed = true;
    }
    if (dayContributed) acc.daysCount += 1;
  }
  return acc;
}

/**
 * Aggregate rows theo sub_id tuple qua nhiều ngày. Key = sub_ids joined.
 * Source filter giống `computeOverviewTotals`: 'shopee_only' → bỏ ads
 * (spend=0, adsClicks=0) → profit = net_commission.
 */
export function aggregateProductRows(
  days: readonly UiDay[],
  clickSources: Record<string, boolean>,
  fees: ProfitFees,
  source: SourceFilter = "all",
): AggregatedProductRow[] {
  const ratio = netCommissionRatio(fees);
  const includeAds = source === "all";
  const map = new Map<string, AggregatedProductRow>();
  for (const day of days) {
    for (const r of day.rows) {
      if (!rowMatchesSource(r, source)) continue;
      const key = r.subIds.join("\x1f");
      let agg = map.get(key);
      if (!agg) {
        agg = {
          subIds: r.subIds,
          displayName: r.displayName,
          adsClicks: 0,
          shopeeClicks: 0,
          totalSpend: 0,
          ordersCount: 0,
          commissionTotal: 0,
          orderValueTotal: 0,
          netCommission: 0,
          profit: 0,
          daysActive: 0,
        };
        map.set(key, agg);
      }
      if (!agg.displayName && r.displayName) agg.displayName = r.displayName;
      const spend = includeAds ? r.totalSpend ?? 0 : 0;
      const adsClicks = includeAds ? r.adsClicks ?? 0 : 0;
      const net = r.commissionTotal * ratio;
      agg.adsClicks += adsClicks;
      agg.shopeeClicks += sumFiltered(r.shopeeClicksByReferrer, clickSources);
      agg.totalSpend += spend;
      agg.ordersCount += r.ordersCount;
      agg.commissionTotal += r.commissionTotal;
      agg.orderValueTotal += r.orderValueTotal;
      agg.netCommission += net;
      agg.profit += net - spend;
      agg.daysActive += 1;
    }
  }
  return Array.from(map.values());
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

/// TSV (tab-separated) string cho 1 UiDay — user copy + paste vào Google
/// Sheet / Excel. Số hiển thị raw (Number, không có " đ" hay "%") để spreadsheet
/// format được. Format decimal với DOT (en-US) để Google Sheets parse đúng —
/// dấu phẩy Việt dễ bị hiểu nhầm là thousand separator.
///
/// Shape: header row + data rows + totals row.
/// Columns: Sản phẩm | Click ADS | Click Shopee | CPC | Spend | Đơn | CR |
///          GMV | Hoa hồng | Lợi nhuận | ROI
export function buildDayTsv(
  day: UiDay,
  clickSources: Record<string, boolean>,
  fees: ProfitFees,
): string {
  const round0 = (n: number) => Math.round(n).toString();
  const round2 = (n: number) => n.toFixed(2);
  const empty = "";

  const header = [
    "Sản phẩm",
    "Click ADS",
    "Click Shopee",
    "CPC",
    "Tổng tiền chạy",
    "Số đơn",
    "CR (%)",
    "GMV TB",
    "Hoa hồng",
    "Lợi nhuận",
    "ROI (%)",
  ];

  const rowLines = day.rows.map((r) => {
    const shopeeClicks = sumFiltered(r.shopeeClicksByReferrer, clickSources);
    const c = computeUiRow(r, fees, shopeeClicks);
    return [
      r.displayName || "(chưa đặt tên)",
      r.adsClicks != null ? round0(r.adsClicks) : empty,
      round0(shopeeClicks),
      c.cpc > 0 ? round0(c.cpc) : empty,
      r.totalSpend != null ? round0(r.totalSpend) : empty,
      round0(r.ordersCount),
      shopeeClicks > 0 ? round2(c.conversionRate) : empty,
      r.ordersCount > 0 ? round0(c.orderValue) : empty,
      round0(r.commissionTotal),
      round0(c.profit),
      r.totalSpend && r.totalSpend > 0 ? round2(c.profitMargin) : empty,
    ].join("\t");
  });

  const totals = computeUiDayTotals(day, clickSources, fees);
  const totalLine = [
    "Tổng",
    round0(totals.clicks),
    round0(totals.shopeeClicks),
    empty,
    round0(totals.totalSpend),
    round0(totals.orders),
    empty,
    empty,
    round0(totals.commission),
    round0(totals.profit),
    empty,
  ].join("\t");

  return [header.join("\t"), ...rowLines, totalLine].join("\n");
}

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
