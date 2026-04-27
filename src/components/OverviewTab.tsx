import { useEffect, useMemo, useState } from "react";
import type { UiDay } from "../types";
import { invoke } from "../lib/tauri";
import type { AccountFilterMode, DaysFilter } from "../hooks/useDbStats";
import {
  aggregateProductRows,
  computeAdsEfficiency,
  computeBreakeven,
  computeCumulativeTrend,
  computeDailyTrend,
  computeExtremumDays,
  computeFunnel,
  computeLosers,
  computeMinSpendThreshold,
  computeOverviewTotals,
  computeWinners,
  fmtDate,
  fmtInt,
  fmtPct,
  fmtVnd,
  profitTone,
  roiTone,
  toneIconClass,
  toneTextClass,
  type AggregatedProductRow,
  type OverviewTotals,
  type SourceFilter,
  type Tone,
} from "../formulas";
import { useSettings } from "../hooks/useSettings";
import { AggregateProductDialog } from "./AggregateProductDialog";
import { OverviewTrendChart } from "./OverviewTrendChart";
import { OverviewAdsInsights } from "./OverviewAdsInsights";
import { HourlyChart, type HourlyBucket } from "./HourlyChart";
import {
  ClickDelayChart,
  ReferrerEfficiencyTable,
  type DelayBucket,
  type ReferrerEfficiency,
} from "./OverviewClickInsights";

interface OverviewTabProps {
  days: UiDay[];
  /** Hiển thị trong header: "01/04/2026 – 20/04/2026". Bỏ trống nếu không biết. */
  dateFrom: string;
  dateTo: string;
  /** Tổng ngày trong DB — so sánh với daysCount thực tế để user biết đang xem bao nhiêu. */
  totalDaysInDb: number;
  /** Filter hiện tại truyền từ AppInner — dùng cho `load_hourly_orders` BE
   *  aggregate theo đúng khoảng + account mà user đang xem. */
  currentFilter: DaysFilter;
  accountFilter: AccountFilterMode;
}

interface HourlyOrderBucketDto {
  hour: number;
  orders: number;
  orderValue: number;
  commission: number;
}

interface HourlyClickBucketDto {
  hour: number;
  clicks: number;
}

const SOURCE_OPTIONS: Array<{ id: SourceFilter; label: string; icon: string; desc: string }> = [
  { id: "all", label: "Tất cả", icon: "all_inclusive", desc: "FB Ads + Shopee" },
  { id: "shopee_only", label: "Chỉ Shopee", icon: "shopping_cart", desc: "Chỉ rows có Shopee data" },
];

const ROI_TOOLTIP =
  "ROI = (Hoa hồng sau phí − Tiền ads) / Tiền ads × 100%\n" +
  "• 0% = hòa vốn\n• > 0% = có lãi\n• < 0% = đang lỗ";

/**
 * Header cho bảng sản phẩm aggregate. `adsOnly=true` → chỉ show khi source=all.
 * `showAdsLabel` + `shopeeLabel` → label thay đổi theo source (vd Lợi nhuận ↔ Hoa hồng ròng).
 */
const PRODUCT_HEADERS: Array<{
  label: string;
  tooltip?: string;
  align: "narrow" | "left" | "center";
  adsOnly?: boolean;
  showAdsLabel?: boolean;
  shopeeLabel?: string;
}> = [
  { label: "#", align: "narrow" },
  { label: "Sản phẩm", align: "left" },
  { label: "Ngày", tooltip: "Số ngày sản phẩm này có data trong khoảng", align: "center" },
  {
    label: "Click ADS",
    tooltip: "Tổng click quảng cáo FB (link_clicks) — sum qua tất cả ngày",
    align: "center",
    adsOnly: true,
  },
  {
    label: "Click Shopee",
    tooltip: "Tổng click affiliate về Shopee (lọc theo nguồn trong Cài đặt) — sum qua tất cả ngày",
    align: "center",
  },
  {
    label: "Tổng tiền chạy",
    tooltip: "Spend FB (đã trừ ngân sách chưa tiêu) — sum qua tất cả ngày",
    align: "center",
    adsOnly: true,
  },
  {
    label: "Số đơn",
    tooltip: "Tổng số đơn (COUNT DISTINCT order_id) — sum qua tất cả ngày",
    align: "center",
  },
  {
    label: "CR",
    tooltip: "CR = Tổng số đơn / Tổng Click Shopee × 100%",
    align: "center",
  },
  {
    label: "GMV",
    tooltip: "Tổng Giá trị đơn hàng — sum qua tất cả ngày",
    align: "center",
  },
  {
    label: "Hoa hồng",
    tooltip: "Tổng hoa hồng gross (chưa trừ phí sàn/thuế) — sum qua tất cả ngày",
    align: "center",
  },
  {
    label: "Lợi nhuận",
    shopeeLabel: "Hoa hồng ròng",
    showAdsLabel: true,
    tooltip:
      "Lợi nhuận = Hoa hồng × (1 − thuế − dự phòng) − Tiền ads.\nShopee-only: không trừ ads → bằng Hoa hồng ròng.",
    align: "center",
  },
  { label: "ROI", tooltip: ROI_TOOLTIP, align: "center", adsOnly: true },
];

export function OverviewTab({
  days,
  dateFrom,
  dateTo,
  totalDaysInDb,
  currentFilter,
  accountFilter,
}: OverviewTabProps) {
  const { settings } = useSettings();
  const [source, setSource] = useState<SourceFilter>("all");
  const [selectedProduct, setSelectedProduct] =
    useState<AggregatedProductRow | null>(null);

  // Single useMemo cho toàn bộ analytics — tất cả đều re-compute cùng lúc
  // khi filter đổi. Gộp để tránh 11 cascading useMemo với deps trùng nhau
  // (mỗi lần days/settings/source đổi sẽ fire 11 separate memo invalidations).
  const analytics = useMemo(() => {
    const totals = computeOverviewTotals(
      days,
      settings.clickSources,
      settings.profitFees,
      source,
    );
    const products = aggregateProductRows(
      days,
      settings.clickSources,
      settings.profitFees,
      source,
    );
    products.sort((a, b) => b.profit - a.profit);

    const trendData = computeDailyTrend(
      days,
      settings.clickSources,
      settings.profitFees,
      source,
    );
    const cumulativeData = computeCumulativeTrend(trendData);
    const breakeven = computeBreakeven(totals, settings.profitFees);
    const funnel = computeFunnel(totals);
    const efficiency = computeAdsEfficiency(totals);
    const { best: bestDay, worst: worstDay } = computeExtremumDays(trendData);
    const minSpend = computeMinSpendThreshold(products);
    const winners = computeWinners(products, minSpend, 5);
    const losers = computeLosers(products, minSpend, 5);

    return {
      totals,
      products,
      trendData,
      cumulativeData,
      breakeven,
      funnel,
      efficiency,
      bestDay,
      worstDay,
      winners,
      losers,
    };
  }, [days, settings.clickSources, settings.profitFees, source]);
  const {
    totals,
    products,
    trendData,
    cumulativeData,
    breakeven,
    funnel,
    efficiency,
    bestDay,
    worstDay,
    winners,
    losers,
  } = analytics;

  // 4 BE aggregates dưới đây fetch song song khi filter đổi (date range /
  // account). KHÔNG pass subIdFilter — Overview scope là all products.
  // Overlay clicks lên HourlyBucket thay vì giữ 2 state riêng — HourlyChart
  // nhận cả 2 dataset với `metric` khác nhau.
  const [hourlyOrders, setHourlyOrders] = useState<HourlyBucket[]>([]);
  const [hourlyClicks, setHourlyClicks] = useState<HourlyBucket[]>([]);
  const [referrerEff, setReferrerEff] = useState<ReferrerEfficiency[]>([]);
  const [clickDelays, setClickDelays] = useState<DelayBucket[]>([]);
  const [clickInsightsLoading, setClickInsightsLoading] = useState(false);
  useEffect(() => {
    let cancelled = false;
    setClickInsightsLoading(true);
    const beFilter: DaysFilter = {
      fromDate: currentFilter.fromDate,
      toDate: currentFilter.toDate,
      limit: currentFilter.limit,
      accountFilter,
    };
    Promise.all([
      invoke<HourlyOrderBucketDto[]>("load_hourly_orders", { filter: beFilter }),
      invoke<HourlyClickBucketDto[]>("load_hourly_clicks", { filter: beFilter }),
      invoke<ReferrerEfficiency[]>("load_referrer_efficiency", { filter: beFilter }),
      invoke<DelayBucket[]>("load_click_order_delays", { filter: beFilter }),
    ])
      .then(([orders, clicks, referrers, delays]) => {
        if (cancelled) return;
        setHourlyOrders(
          orders.map((b) => ({
            hour: b.hour,
            orders: b.orders,
            orderValue: b.orderValue,
            commission: b.commission,
            clicks: 0,
          })),
        );
        setHourlyClicks(
          clicks.map((b) => ({
            hour: b.hour,
            orders: 0,
            orderValue: 0,
            commission: 0,
            clicks: b.clicks,
          })),
        );
        setReferrerEff(referrers);
        setClickDelays(delays);
      })
      .catch((e) => {
        console.error("[overview click insights] load failed:", e);
        if (!cancelled) {
          setHourlyOrders([]);
          setHourlyClicks([]);
          setReferrerEff([]);
          setClickDelays([]);
        }
      })
      .finally(() => {
        if (!cancelled) setClickInsightsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [currentFilter.fromDate, currentFilter.toDate, currentFilter.limit, accountFilter]);

  return (
    <div className="mx-auto max-w-[1400px] space-y-6">
      {/* ============ Source filter ============ */}
      <section className="flex flex-wrap items-center gap-3 rounded-xl border border-surface-8 bg-surface-2 px-4 py-3 shadow-elev-1">
        <div className="flex items-center gap-2">
          <span className="material-symbols-rounded text-shopee-400">filter_alt</span>
          <span className="text-xs font-semibold uppercase tracking-wider text-white/60">
            Nguồn data
          </span>
        </div>
        <div className="flex flex-wrap gap-1.5">
          {SOURCE_OPTIONS.map((opt) => (
            <button
              key={opt.id}
              type="button"
              onClick={() => setSource(opt.id)}
              title={opt.desc}
              className={`btn-ripple flex items-center gap-1.5 rounded-full px-3 py-1.5 text-sm font-medium transition-colors ${
                source === opt.id
                  ? "bg-shopee-500 text-white shadow-elev-2"
                  : "bg-surface-4 text-white/70 hover:bg-surface-6"
              }`}
            >
              <span className="material-symbols-rounded text-base">{opt.icon}</span>
              {opt.label}
            </button>
          ))}
        </div>

        <span className="mx-1 hidden h-6 w-px bg-surface-8 md:inline-block" />

        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-white/70">
          <span className="inline-flex items-center gap-1">
            <span className="material-symbols-rounded text-sm text-white/40">event</span>
            {dateFrom && dateTo
              ? `${fmtDate(dateFrom)} – ${fmtDate(dateTo)}`
              : "Toàn thời gian"}
          </span>
          <span className="text-white/30">·</span>
          <span>
            <b className="tabular-nums text-white/90">{fmtInt(totals.daysCount)}</b>
            <span className="text-white/50"> / {fmtInt(totalDaysInDb)} ngày</span>
          </span>
          <span className="text-white/30">·</span>
          <span>
            <b className="tabular-nums text-white/90">{fmtInt(totals.rowsCount)}</b>
            <span className="text-white/50"> dòng</span>
          </span>
        </div>
      </section>

      {/* ============ KPI primary (lớn) ============ */}
      <PrimaryKpiRow totals={totals} source={source} />

      {/* ============ MCN fee banner (chỉ show khi có) ============ */}
      {totals.mcnFeeTotal > 0 && (
        <section
          className="flex items-center gap-3 rounded-xl border border-amber-500/30 bg-amber-500/5 px-4 py-3 text-sm text-amber-100/90"
          title="Shopee đã cắt phí MCN trước khi payout. Hoa hồng hiển thị là NET (đã trừ phí MCN). Số này chỉ minh bạch, KHÔNG bị trừ lần nữa vào lợi nhuận."
        >
          <span className="material-symbols-rounded text-amber-300">info</span>
          <div className="flex flex-col">
            <span className="text-[11px] font-semibold uppercase tracking-wider text-amber-300/80">
              Phí quản lý MCN đã bị cắt
            </span>
            <span className="mt-0.5 text-lg font-bold tabular-nums text-amber-100">
              {fmtVnd(totals.mcnFeeTotal)}
            </span>
          </div>
          <span className="ml-auto text-xs text-amber-200/60">
            đã trừ sẵn trong Hoa hồng gross
          </span>
        </section>
      )}

      {/* ============ KPI secondary (nhỏ) ============ */}
      <SecondaryKpiRow totals={totals} source={source} />

      {/* ============ Trend chart (3 modes: finance / ROI / cumulative) ============ */}
      <OverviewTrendChart
        data={trendData}
        cumulative={cumulativeData}
        showAds={source === "all"}
      />

      {/* ============ Hourly clicks + orders (side-by-side) ============ */}
      <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <HourlyChart
          data={hourlyClicks}
          title="Giờ click Shopee nhiều nhất"
          metric="clicks"
          icon="mouse"
          loading={clickInsightsLoading}
        />
        <HourlyChart
          data={hourlyOrders}
          title="Giờ mua hàng nhiều nhất"
          metric="orders"
          icon="schedule"
          loading={clickInsightsLoading}
        />
      </section>

      {/* ============ Click-to-order delay histogram ============ */}
      <ClickDelayChart data={clickDelays} />

      {/* ============ Referrer efficiency leaderboard ============ */}
      <ReferrerEfficiencyTable rows={referrerEff} />

      {/* ============ Efficiency + Breakeven + Funnel + Best/Worst day +
           Referrers + Winners/Losers ============ */}
      <OverviewAdsInsights
        winners={winners}
        losers={losers}
        breakeven={breakeven}
        funnel={funnel}
        efficiency={efficiency}
        bestDay={bestDay}
        worstDay={worstDay}
        showAds={source === "all"}
        onSelectProduct={setSelectedProduct}
      />

      {/* ============ Bảng sản phẩm ============ */}
      <ProductsTable
        rows={products}
        source={source}
        onSelectProduct={setSelectedProduct}
      />

      <AggregateProductDialog
        isOpen={!!selectedProduct}
        product={selectedProduct}
        days={days}
        source={source}
        accountFilter={accountFilter}
        onClose={() => setSelectedProduct(null)}
      />
    </div>
  );
}

// =========================================================
// KPI ROWS
// =========================================================

function PrimaryKpiRow({
  totals,
  source,
}: {
  totals: OverviewTotals;
  source: SourceFilter;
}) {
  const roi =
    totals.totalSpend > 0 ? (totals.profit / totals.totalSpend) * 100 : null;
  const profitToneValue = profitTone(totals.profit);
  const roiToneValue = roiTone(roi);
  const showAds = source === "all";

  // Shopee-only → 3 cards (không spend, không ROI). All → 4 cards.
  const gridCols = showAds ? "md:grid-cols-4" : "md:grid-cols-3";
  return (
    <section className={`grid grid-cols-1 gap-4 ${gridCols}`}>
      <BigKpi
        icon="trending_up"
        label={showAds ? "Lợi nhuận" : "Hoa hồng ròng"}
        value={fmtVnd(totals.profit)}
        tone={profitToneValue}
        sub={
          showAds
            ? `Hoa hồng ròng ${fmtVnd(totals.netCommission)}`
            : `Gross ${fmtVnd(totals.commission)}`
        }
      />
      {showAds && (
        <BigKpi
          icon="percent"
          label="ROI"
          value={roi !== null ? fmtPct(roi) : "—"}
          tone={roiToneValue}
          sub={
            roi === null
              ? "Chưa có spend"
              : roi > 0
              ? "Có lãi"
              : roi < 0
              ? "Đang lỗ"
              : "Hòa vốn"
          }
        />
      )}
      <BigKpi
        icon="payments"
        label="Hoa hồng gross"
        value={fmtVnd(totals.commission)}
        tone="commission"
        sub={`GMV ${fmtVnd(totals.orderValueTotal)}`}
      />
      {showAds ? (
        <BigKpi
          icon="shopping_bag"
          label="Tổng tiền chạy"
          value={fmtVnd(totals.totalSpend)}
          tone="spend"
          sub={`${fmtInt(totals.clicks)} click ADS`}
        />
      ) : (
        <BigKpi
          icon="shopping_cart"
          label="Số đơn"
          value={fmtInt(totals.orders)}
          tone="neutral"
          sub={`${fmtInt(totals.shopeeClicks)} click Shopee`}
        />
      )}
    </section>
  );
}

function SecondaryKpiRow({
  totals,
  source,
}: {
  totals: OverviewTotals;
  source: SourceFilter;
}) {
  const showAds = source === "all";
  const cpc = totals.clicks > 0 ? totals.totalSpend / totals.clicks : null;
  const cpcShopee =
    totals.shopeeClicks > 0 ? totals.totalSpend / totals.shopeeClicks : null;
  const cr =
    totals.shopeeClicks > 0 ? (totals.orders / totals.shopeeClicks) * 100 : null;
  const avgOrder =
    totals.orders > 0 ? totals.orderValueTotal / totals.orders : null;
  const avgCommission =
    totals.orders > 0 ? totals.commission / totals.orders : null;

  const gridCols = showAds
    ? "md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-7"
    : "md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5";
  return (
    <section className={`grid grid-cols-2 gap-3 ${gridCols}`}>
      {showAds && (
        <SmallKpi
          label="Click ADS"
          value={fmtInt(totals.clicks)}
          icon="ads_click"
        />
      )}
      <SmallKpi
        label="Click Shopee"
        value={fmtInt(totals.shopeeClicks)}
        icon="mouse"
      />
      {showAds && (
        <SmallKpi
          label="CPC (FB)"
          value={cpc !== null ? fmtVnd(cpc) : "—"}
          icon="paid"
          tone="spend"
          tooltip="CPC = Tổng tiền chạy / Click ADS"
        />
      )}
      {showAds && (
        <SmallKpi
          label="CPC thực tế"
          value={cpcShopee !== null ? fmtVnd(cpcShopee) : "—"}
          icon="request_quote"
          tone="spend"
          tooltip="CPC thực tế = Tổng tiền chạy / Click Shopee"
        />
      )}
      <SmallKpi
        label="Số đơn"
        value={fmtInt(totals.orders)}
        icon="shopping_cart"
        sub={
          avgCommission !== null
            ? `HH TB ${fmtVnd(avgCommission)}`
            : undefined
        }
      />
      <SmallKpi
        label="Tỷ lệ chuyển đổi"
        value={cr !== null ? fmtPct(cr) : "—"}
        icon="trending_up"
        tooltip="CR = Số đơn / Click Shopee × 100%"
      />
      <SmallKpi
        label="GMV"
        value={fmtVnd(totals.orderValueTotal)}
        icon="payments"
        tone="commission"
        tooltip="Tổng Giá trị đơn hàng"
      />
      <SmallKpi
        label="Giá trị đơn hàng TB"
        value={avgOrder !== null ? fmtVnd(avgOrder) : "—"}
        icon="receipt_long"
        tone="commission"
        tooltip="AOV = GMV / Số đơn — giá trị trung bình mỗi đơn hàng"
      />
    </section>
  );
}

function BigKpi({
  icon,
  label,
  value,
  sub,
  tone,
}: {
  icon: string;
  label: string;
  value: string;
  sub?: string;
  tone: Tone;
}) {
  return (
    <div className="rounded-xl bg-surface-4 p-5 shadow-elev-2 transition-shadow hover:shadow-elev-4">
      <div className="flex items-center gap-2">
        <span className={`material-symbols-rounded text-lg ${toneIconClass(tone)}`}>
          {icon}
        </span>
        <p className="text-xs font-semibold uppercase tracking-wider text-white/55">
          {label}
        </p>
      </div>
      <p
        className={`num-glow mt-2 truncate text-3xl font-bold tabular-nums ${toneTextClass(tone)}`}
        title={value}
      >
        {value}
      </p>
      {sub && (
        <p className="mt-1 truncate text-xs text-white/50" title={sub}>
          {sub}
        </p>
      )}
    </div>
  );
}

function SmallKpi({
  label,
  value,
  icon,
  sub,
  tooltip,
  tone = "neutral",
}: {
  label: string;
  value: string;
  icon: string;
  sub?: string;
  tooltip?: string;
  /** Brand tone — spend/commission color icon. Default neutral (white/40). */
  tone?: Tone;
}) {
  const iconCls =
    tone === "neutral" ? "text-white/35" : toneIconClass(tone);
  return (
    <div
      className="rounded-lg bg-surface-2 px-4 py-3 shadow-elev-1"
      title={tooltip}
    >
      <div className="flex items-center gap-1.5">
        <span className={`material-symbols-rounded text-sm ${iconCls}`}>{icon}</span>
        <p className={`text-[11px] font-medium uppercase tracking-wider text-white/55 ${tooltip ? "cursor-help" : ""}`}>
          {label}
        </p>
      </div>
      <p
        className="num-glow mt-1 truncate text-xl font-bold tabular-nums text-white/95"
        title={value}
      >
        {value}
      </p>
      {sub && (
        <p className="mt-0.5 truncate text-[11px] text-white/45" title={sub}>
          {sub}
        </p>
      )}
    </div>
  );
}

// =========================================================
// PRODUCTS TABLE
// =========================================================

function ProductsTable({
  rows,
  source,
  onSelectProduct,
}: {
  rows: AggregatedProductRow[];
  source: SourceFilter;
  onSelectProduct: (row: AggregatedProductRow) => void;
}) {
  const showAds = source === "all";
  if (rows.length === 0) {
    return (
      <section className="rounded-xl border border-dashed border-surface-8 bg-surface-1 px-6 py-10 text-center text-white/60">
        <span className="material-symbols-rounded mb-2 block text-4xl text-shopee-400">
          inventory_2
        </span>
        Không có sản phẩm nào trong khoảng + filter đã chọn
      </section>
    );
  }

  return (
    <section className="overflow-hidden rounded-xl bg-surface-2 shadow-elev-2">
      <header className="flex items-center justify-between border-b border-surface-8 px-5 py-3">
        <div className="flex items-center gap-2">
          <span className="material-symbols-rounded text-shopee-400">inventory_2</span>
          <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
            Sản phẩm (tổng hợp)
          </h3>
          <span className="rounded-full bg-shopee-900/40 px-2 py-0.5 text-xs text-shopee-300">
            {fmtInt(rows.length)}
          </span>
        </div>
        <span className="text-xs text-white/50">
          Sắp xếp theo {showAds ? "Lợi nhuận" : "Hoa hồng ròng"} ↓
        </span>
      </header>

      <div>
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr className="border-b-2 border-shopee-500/50 bg-gradient-to-b from-shopee-900/35 to-shopee-900/15 text-shopee-100">
              {PRODUCT_HEADERS.filter((h) => showAds || !h.adsOnly).map((h) => (
                <th
                  key={h.label}
                  title={h.tooltip}
                  className={`py-3.5 text-xs font-bold uppercase tracking-wider whitespace-nowrap ${
                    h.align === "left"
                      ? "min-w-[220px] px-4 text-left"
                      : h.align === "narrow"
                      ? "w-12 px-2 text-center"
                      : "px-3 text-center"
                  } ${h.tooltip ? "cursor-help" : ""}`}
                >
                  <span className="inline-flex items-center gap-1">
                    <span>{h.showAdsLabel && !showAds ? h.shopeeLabel : h.label}</span>
                    {h.tooltip && (
                      <span className="text-[11px] leading-none text-shopee-300/60">ⓘ</span>
                    )}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <ProductRow
                key={r.subIds.join("\x1f")}
                row={r}
                index={i + 1}
                showAds={showAds}
                onClick={() => onSelectProduct(r)}
              />
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function ProductRow({
  row,
  index,
  showAds,
  onClick,
}: {
  row: AggregatedProductRow;
  index: number;
  showAds: boolean;
  onClick: () => void;
}) {
  const cr =
    row.shopeeClicks > 0 ? (row.ordersCount / row.shopeeClicks) * 100 : null;
  const roi = row.totalSpend > 0 ? (row.profit / row.totalSpend) * 100 : null;
  const profitCls =
    row.profit > 0
      ? "text-green-400"
      : row.profit < 0
      ? "text-red-400"
      : "text-gray-400";
  const cellCls = "px-3 py-2.5 text-center";
  const naCls = "text-white/30";
  return (
    <tr
      onClick={() => {
        // Không trigger dialog nếu user đang bôi đen text trong row.
        const selection = window.getSelection();
        if (selection && selection.toString().length > 0) return;
        onClick();
      }}
      className="h-[52px] cursor-pointer border-b border-surface-8 text-white/80 transition-colors hover:bg-shopee-500/15 hover:shadow-[inset_3px_0_0_0] hover:shadow-shopee-500"
      title={row.displayName}
    >
      <td className="w-12 px-2 py-2.5 text-center text-sm tabular-nums text-white/50">
        {index}
      </td>
      <td
        className="max-w-[280px] truncate px-4 py-2.5 text-left text-sm font-semibold text-white"
        title={row.displayName}
      >
        {row.displayName || (
          <span className="italic font-normal text-gray-500">(chưa đặt tên)</span>
        )}
      </td>
      <td className={`${cellCls} tabular-nums text-white/60`}>{row.daysActive}</td>
      {showAds && (
        <td className={`${cellCls} tabular-nums ${row.adsClicks === 0 ? naCls : ""}`}>
          {row.adsClicks > 0 ? fmtInt(row.adsClicks) : "—"}
        </td>
      )}
      <td className={`${cellCls} tabular-nums`}>{fmtInt(row.shopeeClicks)}</td>
      {showAds && (
        <td className={`${cellCls} tabular-nums ${row.totalSpend === 0 ? naCls : ""}`}>
          {row.totalSpend > 0 ? fmtVnd(row.totalSpend) : "—"}
        </td>
      )}
      <td className={`${cellCls} tabular-nums`}>{fmtInt(row.ordersCount)}</td>
      <td className={`${cellCls} tabular-nums text-gray-400 ${cr === null ? naCls : ""}`}>
        {cr !== null ? fmtPct(cr) : "—"}
      </td>
      <td className={`${cellCls} tabular-nums text-gray-400 ${row.ordersCount === 0 ? naCls : ""}`}>
        {row.ordersCount > 0 ? fmtVnd(row.orderValueTotal) : "—"}
      </td>
      <td className={`${cellCls} tabular-nums`}>{fmtVnd(row.commissionTotal)}</td>
      <td className={`${cellCls} tabular-nums font-medium ${profitCls}`}>
        {fmtVnd(row.profit)}
      </td>
      {showAds && (
        <td className={`${cellCls} tabular-nums ${roi === null ? naCls : profitCls}`}>
          {roi !== null ? fmtPct(roi) : "—"}
        </td>
      )}
    </tr>
  );
}
