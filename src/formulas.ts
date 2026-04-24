import type { DayTotals, UiDay, UiRow, VideoComputed } from "./types";
import {
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
  /** Tổng phí quản lý MCN Shopee đã cắt (đã trừ sẵn trong commission —
   *  chỉ dùng hiển thị minh bạch, không tính lại profit). */
  mcnFeeTotal: number;
  /** Tổng hoa hồng từ đơn rủi ro huỷ (Đang chờ xử lý + Chưa thanh toán).
   *  Đã tính sẵn vào `commission` — hiển thị riêng để admin biết rủi ro. */
  commissionPending: number;
  /** Tổng impressions FB ads — dùng tính CPM + impression→click rate. */
  impressions: number;
  /** Breakdown click Shopee theo referrer (đã filter settings). */
  clicksByReferrer: Record<string, number>;
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
/// SINGLE SOURCE OF TRUTH cho công thức net commission.
/// - `commission`: commission từ DB column `net_commission` — thực chất là
///   "Hoa hồng ròng tiếp thị liên kết" từ CSV col 37, Shopee ĐÃ TRỪ phí MCN
///   trong số này. Tên `commission` giữ lại vì legacy, không rename để tránh
///   đụng callsite. `UiDayTotals.mcnFeeTotal` lưu phí MCN đã bị cắt để UI hiển thị.
/// - `commissionPending`: subset commission từ đơn rủi ro huỷ: "Đang chờ xử lý"
///   + "Chưa thanh toán" (raw Shopee orders only — manual override có pending=0).
///
/// Logic:
///   net = commission × (1 - taxRate) - commissionPending × reserveRate
///
/// Giải thích:
/// - `taxRate` (thuế & phí sàn): áp cho MỌI commission (đơn nào cũng bị Shopee
///   trừ trước khi payout).
/// - `reserveRate` (dự phòng hoàn huỷ): CHỈ trừ từ pending — đơn chưa chắc
///   ăn, có rủi ro bị huỷ/hoàn. Completed order đã chắc → không cần reserve.
/// - Phí MCN: KHÔNG trừ ở đây vì đã bị Shopee cắt sẵn trong `commission`.
///   Trừ lần nữa = double-count.
///
/// MỌI nơi tính net commission PHẢI gọi hàm này. Đổi công thức → sửa 1 chỗ.
export function computeNetCommission(
  commission: number,
  commissionPending: number,
  fees: ProfitFees,
): number {
  const tax = fees.taxAndPlatformRate / 100;
  const reserve = fees.returnReserveRate / 100;
  return commission * (1 - tax) - commissionPending * reserve;
}

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
  const netCommission = computeNetCommission(
    commission,
    row.commissionPending,
    fees,
  );
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
    mcnFeeTotal: 0,
    commissionPending: 0,
    impressions: 0,
    clicksByReferrer: {},
    daysCount: 0,
    rowsCount: 0,
  };
  for (const day of days) {
    const t = day.totals;
    const spend = includeAds ? t.totalSpend : 0;
    const adsClicks = includeAds ? t.adsClicks : 0;
    const impr = includeAds ? t.impressions : 0;
    // Dùng computeNetCommission trên day.totals (pre row-0 filter) — KPI
    // accurate kể cả khi có tuple click-only bị filter.
    const net = computeNetCommission(
      t.commissionTotal,
      t.commissionPending,
      fees,
    );
    acc.clicks += adsClicks;
    acc.shopeeClicks += sumFiltered(t.shopeeClicksByReferrer, clickSources);
    acc.totalSpend += spend;
    acc.impressions += impr;
    acc.orders += t.ordersCount;
    acc.commission += t.commissionTotal;
    acc.netCommission += net;
    acc.profit += net - spend;
    acc.orderValueTotal += t.orderValueTotal;
    acc.mcnFeeTotal += t.mcnFeeTotal;
    acc.commissionPending += t.commissionPending;
    // Merge clicks by referrer (chỉ referrer enabled qua settings).
    for (const [ref, n] of Object.entries(t.shopeeClicksByReferrer)) {
      if (clickSources[ref] === false) continue;
      acc.clicksByReferrer[ref] = (acc.clicksByReferrer[ref] ?? 0) + n;
    }

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
      const net = computeNetCommission(r.commissionTotal, r.commissionPending, fees);
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

// =========================================================
// ADS ANALYTICS — metrics actionable cho quyết định chạy ads
// =========================================================

/// Data point cho trend chart: 1 ngày = 1 point (granularity day) hoặc
/// 1 tuần / 1 tháng (sau aggregateTrend).
/// profit, spend, netCommission, roi chạy cùng X-axis (date).
export type DailyTrendPoint = {
  date: string;
  /// Label hiển thị trên trục X: DD/MM.
  dateShort: string;
  spend: number;
  netCommission: number;
  profit: number;
  /// ROI % = profit/spend × 100. null nếu spend=0 (chưa chạy ads ngày đó).
  roi: number | null;
  orders: number;
  shopeeClicks: number;
  /// Số ngày data thực tế trong bucket (1 cho granularity=day, 1-7 cho week,
  /// 1-31 cho month). UI tooltip hiển thị cho user verify "tháng này gồm
  /// bao nhiêu ngày data" (filter cắt giữa tháng / Feb 28 ngày / etc.).
  dayCount: number;
};

/// Trend theo ngày cho chart. Days ASC by date để biểu đồ vẽ từ trái qua phải.
export function computeDailyTrend(
  days: readonly UiDay[],
  clickSources: Record<string, boolean>,
  fees: ProfitFees,
  source: SourceFilter,
): DailyTrendPoint[] {
  const includeAds = source === "all";
  const points: DailyTrendPoint[] = [];
  for (const day of days) {
    const t = day.totals;
    const spend = includeAds ? t.totalSpend : 0;
    const net = computeNetCommission(t.commissionTotal, t.commissionPending, fees);
    const profit = net - spend;
    const roi = spend > 0 ? (profit / spend) * 100 : null;
    const [y, m, d] = day.date.split("-");
    points.push({
      date: day.date,
      dateShort: `${d}/${m}`,
      spend,
      netCommission: net,
      profit,
      roi,
      orders: t.ordersCount,
      shopeeClicks: sumFiltered(t.shopeeClicksByReferrer, clickSources),
      dayCount: 1,
    });
    // Suppress unused var warning for `y`.
    void y;
  }
  // Sort ASC by date (days đầu vào thường DESC theo UI).
  points.sort((a, b) => a.date.localeCompare(b.date));
  return points;
}

/// Top N winners: products có profit > 0, sắp xếp theo ROI giảm dần.
/// `minSpend` = ngưỡng tối thiểu để loại noise (product spend quá ít, ROI cao
/// không có ý nghĩa thống kê). Default = tổng spend / số product × 0.1.
export function computeWinners(
  products: readonly AggregatedProductRow[],
  minSpend: number,
  limit = 5,
): AggregatedProductRow[] {
  return products
    .filter((p) => p.profit > 0 && p.totalSpend >= minSpend)
    .sort((a, b) => {
      const roiA = a.totalSpend > 0 ? a.profit / a.totalSpend : 0;
      const roiB = b.totalSpend > 0 ? b.profit / b.totalSpend : 0;
      return roiB - roiA;
    })
    .slice(0, limit);
}

/// Top N losers: products có profit < 0, sắp xếp theo số tiền lỗ giảm dần
/// (profit âm nhất trước). Loss leaders — gợi ý cut để tiết kiệm ads budget.
export function computeLosers(
  products: readonly AggregatedProductRow[],
  minSpend: number,
  limit = 5,
): AggregatedProductRow[] {
  return products
    .filter((p) => p.profit < 0 && p.totalSpend >= minSpend)
    .sort((a, b) => a.profit - b.profit) // most negative first
    .slice(0, limit);
}

/// Ngưỡng min spend để lọc noise khỏi winners/losers. Dùng median spend của
/// các product có spend > 0 — robust với outliers so với mean.
export function computeMinSpendThreshold(
  products: readonly AggregatedProductRow[],
): number {
  const spends = products
    .map((p) => p.totalSpend)
    .filter((s) => s > 0)
    .sort((a, b) => a - b);
  if (spends.length === 0) return 0;
  const mid = Math.floor(spends.length / 2);
  return spends.length % 2 === 0
    ? (spends[mid - 1] + spends[mid]) / 2
    : spends[mid];
}

/// Breakeven CR (%) = CR tối thiểu để lợi nhuận = 0 với hiện trạng.
/// Công thức: profit = 0 ⇔ net_commission = spend
///          ⇔ orders × (avgCommissionPerOrder × (1-tax) - ...) = spend
/// Simplification: dùng commission/order TB hiện tại × (1 - taxRate)
/// làm revenue per order, ignore pending reserve (simplification — reserve
/// rate nhỏ, không đổi dramatically kết quả).
/// breakevenCR = spend / (shopeeClicks × netPerOrder) × 100
export type BreakevenAnalysis = {
  /// CR% tối thiểu để hòa vốn với click + spend hiện tại.
  breakevenCr: number | null;
  /// CR% thực tế hiện tại.
  currentCr: number | null;
  /// Gap = currentCr - breakevenCr. Dương = đang lãi, âm = đang lỗ.
  gap: number | null;
  /// Revenue trung bình per order (commission net).
  netPerOrder: number | null;
};

export function computeBreakeven(
  totals: OverviewTotals,
  fees: ProfitFees,
): BreakevenAnalysis {
  const tax = fees.taxAndPlatformRate / 100;
  const netPerOrder =
    totals.orders > 0
      ? (totals.commission / totals.orders) * (1 - tax)
      : null;
  const currentCr =
    totals.shopeeClicks > 0 ? (totals.orders / totals.shopeeClicks) * 100 : null;
  let breakevenCr: number | null = null;
  if (
    netPerOrder !== null &&
    netPerOrder > 0 &&
    totals.totalSpend > 0 &&
    totals.shopeeClicks > 0
  ) {
    breakevenCr = (totals.totalSpend / (totals.shopeeClicks * netPerOrder)) * 100;
  }
  const gap =
    currentCr !== null && breakevenCr !== null ? currentCr - breakevenCr : null;
  return { breakevenCr, currentCr, gap, netPerOrder };
}

/// Funnel metrics: Impression → Click ADS → Click Shopee → Số đơn.
/// - CTR (FB): Click ADS / Impression — ad creative/target performance
/// - Click-through Shopee: Click Shopee / Click ADS — ad→landing relevance
/// - CR: Đơn / Click Shopee — product conversion
/// Bottleneck analysis: step nào drop nhiều nhất → fix đó.
export type FunnelMetrics = {
  impressions: number;
  adsClicks: number;
  shopeeClicks: number;
  orders: number;
  /// Impression → Click ADS %.
  ctrFb: number | null;
  /// Click ADS → Click Shopee %.
  ctrShopee: number | null;
  /// Click Shopee → Order %.
  cr: number | null;
};

export function computeFunnel(totals: OverviewTotals): FunnelMetrics {
  const ctrFb =
    totals.impressions > 0 ? (totals.clicks / totals.impressions) * 100 : null;
  const ctrShopee =
    totals.clicks > 0 ? (totals.shopeeClicks / totals.clicks) * 100 : null;
  const cr =
    totals.shopeeClicks > 0 ? (totals.orders / totals.shopeeClicks) * 100 : null;
  return {
    impressions: totals.impressions,
    adsClicks: totals.clicks,
    shopeeClicks: totals.shopeeClicks,
    orders: totals.orders,
    ctrFb,
    ctrShopee,
    cr,
  };
}

/// Efficiency metrics cho ads spend — ngoài ROI đã có.
/// - ROAS: Revenue (hoa hồng ròng) / Spend. ROAS=1 = hòa vốn, >1 = lãi.
/// - EPC: Commission ròng / Click Shopee. Earnings Per Click — affiliate KPI.
/// - CPM: Spend / Impressions × 1000. Giá mỗi 1000 impression.
/// - AOV: Order Value / Orders. Đã có trong SecondaryKpiRow nhưng surface lại.
/// - Avg daily spend: spend / daysCount — biết đang chạy ngân sách bao nhiêu/ngày.
export type AdsEfficiencyMetrics = {
  roas: number | null;
  epc: number | null;
  cpm: number | null;
  aov: number | null;
  avgDailySpend: number | null;
};

export function computeAdsEfficiency(
  totals: OverviewTotals,
): AdsEfficiencyMetrics {
  return {
    roas:
      totals.totalSpend > 0 ? totals.netCommission / totals.totalSpend : null,
    epc:
      totals.shopeeClicks > 0
        ? totals.netCommission / totals.shopeeClicks
        : null,
    cpm:
      totals.impressions > 0
        ? (totals.totalSpend / totals.impressions) * 1000
        : null,
    aov: totals.orders > 0 ? totals.orderValueTotal / totals.orders : null,
    avgDailySpend:
      totals.daysCount > 0 ? totals.totalSpend / totals.daysCount : null,
  };
}

// =========================================================
// TREND AGGREGATION — gom daily points → weekly/monthly cho chart density
// =========================================================

/// Granularity time-bucket cho trend chart.
export type TrendGranularity = "day" | "week" | "month";

/// Default granularity dựa vào số ngày data có. Chart 100+ ngày bar/label
/// XAxis chồng không đọc được → auto-aggregate.
export function defaultTrendGranularity(dayCount: number): TrendGranularity {
  if (dayCount <= 31) return "day";
  if (dayCount <= 180) return "week";
  return "month";
}

/// Granularity options hợp lệ với data range. Cho user toggle nhỏ hơn default
/// (vd 200 ngày: tháng default, options Tuần/Tháng — không cho Ngày vì 200
/// bar chồng không xem được). Lớn hơn default thì OK (zoom out).
export function availableGranularities(dayCount: number): TrendGranularity[] {
  if (dayCount <= 31) return ["day"];
  if (dayCount <= 180) return ["day", "week"];
  if (dayCount <= 365) return ["week", "month"];
  return ["month"]; // > 1 năm: tuần cũng quá nhiều → chỉ tháng
}

/// Anchor date của tuần chứa date — Monday đầu tuần (ISO week, UTC).
function weekAnchor(dateYmd: string): string {
  const d = new Date(`${dateYmd}T00:00:00Z`);
  const dow = d.getUTCDay(); // 0=Sun, 1=Mon … 6=Sat
  const daysBack = (dow + 6) % 7; // about-face từ thứ hiện tại lùi về Mon
  d.setUTCDate(d.getUTCDate() - daysBack);
  return d.toISOString().slice(0, 10);
}

/// Anchor date của tháng chứa date — ngày 01 của tháng đó.
function monthAnchor(dateYmd: string): string {
  return `${dateYmd.slice(0, 7)}-01`;
}

/// Gom DailyTrendPoint theo granularity. Sum tất cả monetary fields, ROI
/// recompute weighted = sum(profit)/sum(spend) (chính xác hơn avg ROI ngày).
/// Date của point output = anchor date (Mon của tuần / mùng 1 của tháng).
/// dateShort định dạng tuỳ granularity:
/// - day: DD/MM (giữ nguyên)
/// - week: T DD/MM (T = "Tuần", date = Mon)
/// - month: MM/YY
export function aggregateTrend(
  points: readonly DailyTrendPoint[],
  granularity: TrendGranularity,
): DailyTrendPoint[] {
  if (granularity === "day") return [...points];

  const anchorFn = granularity === "week" ? weekAnchor : monthAnchor;
  const groups = new Map<string, DailyTrendPoint[]>();
  for (const p of points) {
    const a = anchorFn(p.date);
    let g = groups.get(a);
    if (!g) {
      g = [];
      groups.set(a, g);
    }
    g.push(p);
  }

  const result: DailyTrendPoint[] = [];
  for (const [anchor, group] of groups) {
    const spend = group.reduce((s, p) => s + p.spend, 0);
    const netCommission = group.reduce((s, p) => s + p.netCommission, 0);
    const profit = group.reduce((s, p) => s + p.profit, 0);
    const orders = group.reduce((s, p) => s + p.orders, 0);
    const shopeeClicks = group.reduce((s, p) => s + p.shopeeClicks, 0);
    const roi = spend > 0 ? (profit / spend) * 100 : null;

    let dateShort: string;
    if (granularity === "week") {
      const [, m, d] = anchor.split("-");
      dateShort = `T ${d}/${m}`;
    } else {
      const [y, m] = anchor.split("-");
      dateShort = `${m}/${y.slice(2)}`;
    }

    result.push({
      date: anchor,
      dateShort,
      spend,
      netCommission,
      profit,
      roi,
      orders,
      shopeeClicks,
      // Số ngày THỰC TẾ có data trong bucket — không phải số ngày calendar.
      // Vd Feb 2026 có 28 ngày calendar, nhưng nếu user chỉ có 20 ngày data
      // trong tháng đó thì dayCount = 20.
      dayCount: group.length,
    });
  }
  result.sort((a, b) => a.date.localeCompare(b.date));
  return result;
}

/// Cumulative profit trend: mỗi ngày = profit累mulative từ ngày đầu đến đó.
/// Thấy được đường tăng trưởng (line luôn tăng nếu profit dương mọi ngày,
/// hoặc có đoạn giảm nếu ngày lỗ).
export type CumulativePoint = {
  date: string;
  dateShort: string;
  cumulativeProfit: number;
  cumulativeSpend: number;
  cumulativeRevenue: number;
  /// Carry từ DailyTrendPoint.dayCount — UI tooltip dùng cho biết bucket
  /// gồm bao nhiêu ngày data (week/month aggregation).
  dayCount: number;
};

export function computeCumulativeTrend(
  trend: readonly DailyTrendPoint[],
): CumulativePoint[] {
  let cp = 0;
  let cs = 0;
  let cr = 0;
  const out: CumulativePoint[] = [];
  for (const p of trend) {
    cp += p.profit;
    cs += p.spend;
    cr += p.netCommission;
    out.push({
      date: p.date,
      dateShort: p.dateShort,
      cumulativeProfit: cp,
      cumulativeSpend: cs,
      cumulativeRevenue: cr,
      dayCount: p.dayCount,
    });
  }
  return out;
}

/// Best & worst day trong khoảng: top theo profit (dương nhất), bottom theo
/// profit (âm nhất). Gợi ý user pattern ngày tốt/xấu để investigate.
export type ExtremumDay = {
  date: string;
  profit: number;
  spend: number;
  netCommission: number;
  roi: number | null;
  orders: number;
};

export function computeExtremumDays(
  trend: readonly DailyTrendPoint[],
): { best: ExtremumDay | null; worst: ExtremumDay | null } {
  // Chỉ xét ngày có spend > 0 (ngày chưa chạy ads → profit = net commission,
  // so sánh không fair cho ads analysis).
  const withSpend = trend.filter((p) => p.spend > 0);
  if (withSpend.length === 0) return { best: null, worst: null };

  // Best = ngày THỰC SỰ LÃI (profit > 0) có profit cao nhất. Nếu không có
  // ngày lãi → null (UI show empty state, không gọi ngày lỗ nhẹ nhất là
  // "ngày tốt nhất" — misleading).
  const profitable = withSpend.filter((p) => p.profit > 0);
  const best =
    profitable.length > 0
      ? profitable.reduce((a, b) => (a.profit >= b.profit ? a : b))
      : null;

  // Worst = ngày THỰC SỰ LỖ (profit < 0) có profit thấp nhất. Nếu không có
  // ngày lỗ → null (show empty state "không có ngày lỗ").
  const losing = withSpend.filter((p) => p.profit < 0);
  const worst =
    losing.length > 0
      ? losing.reduce((a, b) => (a.profit <= b.profit ? a : b))
      : null;

  const toExtremum = (p: DailyTrendPoint): ExtremumDay => ({
    date: p.date,
    profit: p.profit,
    spend: p.spend,
    netCommission: p.netCommission,
    roi: p.roi,
    orders: p.orders,
  });
  return {
    best: best ? toExtremum(best) : null,
    worst: worst ? toExtremum(worst) : null,
  };
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

/** Rút gọn số thành K/M/B cho trục Y chart (giữ dấu âm). */
export function fmtMoneyShort(n: number): string {
  if (n === 0) return "0";
  const abs = Math.abs(n);
  if (abs >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(n / 1_000).toFixed(0)}K`;
  return String(Math.round(n));
}

/** Bytes → human readable (B / KB / MB / GB) cho progress bar download. */
export function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(2)} MB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

/** Epoch ms → "HH:MM DD/MM/YYYY" (local time) cho lịch sử download video. */
export function fmtHistoryTime(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number) => n.toString().padStart(2, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())} ${pad(d.getDate())}/${pad(
    d.getMonth() + 1,
  )}/${d.getFullYear()}`;
}

/** Tone semantic cho UI — unify style mapping khắp components (chart, card, KPI). */
export type Tone = "positive" | "negative" | "neutral" | "muted";

/** Text color class theo tone — tailwind classes. */
export function toneTextClass(tone: Tone): string {
  switch (tone) {
    case "positive":
      return "text-green-400";
    case "negative":
      return "text-red-400";
    case "muted":
      return "text-white/70";
    case "neutral":
      return "text-white";
  }
}

/** Tone derived từ profit value. Zero = neutral. */
export function profitTone(value: number): Tone {
  if (value > 0) return "positive";
  if (value < 0) return "negative";
  return "neutral";
}

/** Tone derived từ ROI % (null = muted = chưa có spend để tính). */
export function roiTone(roi: number | null): Tone {
  if (roi === null) return "muted";
  if (roi > 0) return "positive";
  if (roi < 0) return "negative";
  return "neutral";
}

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

/// Format "X phút trước" / "X giờ trước" / "X ngày trước" cho presence UI.
/// Dùng cho "lần cuối online" timestamp. Tuần+ → hiện date tuyệt đối.
export function fmtTimeAgo(ms: number | null | undefined): string {
  if (!ms) return "—";
  const diff = Date.now() - ms;
  if (diff < 0) return "vừa xong";
  const sec = Math.floor(diff / 1000);
  if (sec < 60) return "vừa xong";
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min} phút trước`;
  const hour = Math.floor(min / 60);
  if (hour < 24) return `${hour} giờ trước`;
  const day = Math.floor(hour / 24);
  if (day < 7) return `${day} ngày trước`;
  // > 1 tuần → date tuyệt đối.
  return new Date(ms).toLocaleDateString("vi-VN");
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
