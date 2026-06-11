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
  fmtVnd,
  type AggregatedProductRow,
  type SourceFilter,
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
import {
  CancellationRateChart,
  type CancellationByDayBucket,
} from "./CancellationRateChart";
import { PrimaryKpiRow, SecondaryKpiRow } from "./OverviewKpiSection";
import { ProductsTable } from "./OverviewProductsTable";

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

/** Debounce ms cho 5-BE-query effect khi user đổi filter. */
const INSIGHTS_DEBOUNCE_MS = 400;


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
  const [cancellationByDay, setCancellationByDay] = useState<
    CancellationByDayBucket[]
  >([]);
  const [clickInsightsLoading, setClickInsightsLoading] = useState(false);
  useEffect(() => {
    let cancelled = false;

    // Guard: days rỗng (chưa import / filter trả empty) → clear hết, tránh
    // gọi BE với date range null (BE sẽ aggregate all-time).
    if (days.length === 0) {
      setHourlyOrders([]);
      setHourlyClicks([]);
      setReferrerEff([]);
      setClickDelays([]);
      setCancellationByDay([]);
      setClickInsightsLoading(false);
      return;
    }

    setClickInsightsLoading(true);

    // Debounce: user đổi filter liên tục (gõ ngày, chuyển account) sẽ dồn
    // về 1 lần query thay vì spam 5 BE command mỗi keystroke.
    const timer = setTimeout(() => {
      if (cancelled) return;

      // Khi user pick mode "Ngày gần nhất" (limit=N) thay vì range, currentFilter
      // chỉ có `limit` — BE queries hourly/referrer/delay/cancellation KHÔNG hiểu
      // `limit` (chỉ list_days_with_rows hiểu) → query trả all-time. Derive
      // date range từ `days[]` (đã được FE filter đúng) để pass xuống BE.
      let beFromDate = currentFilter.fromDate;
      let beToDate = currentFilter.toDate;
      if (!beFromDate || !beToDate) {
        const sortedDates = days.map((d) => d.date).sort();
        beFromDate = beFromDate ?? sortedDates[0];
        beToDate = beToDate ?? sortedDates[sortedDates.length - 1];
      }

      const beFilter: DaysFilter = {
        fromDate: beFromDate,
        toDate: beToDate,
        accountFilter,
      };
      Promise.all([
        invoke<HourlyOrderBucketDto[]>("load_hourly_orders", { filter: beFilter }),
        invoke<HourlyClickBucketDto[]>("load_hourly_clicks", { filter: beFilter }),
        invoke<ReferrerEfficiency[]>("load_referrer_efficiency", { filter: beFilter }),
        invoke<DelayBucket[]>("load_click_order_delays", { filter: beFilter }),
        invoke<CancellationByDayBucket[]>("load_cancellation_by_subid", {
          filter: beFilter,
        }),
      ])
        .then(([orders, clicks, referrers, delays, cancellations]) => {
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
          setCancellationByDay(cancellations);
        })
        .catch((e) => {
          console.error("[overview click insights] load failed:", e);
          if (!cancelled) {
            setHourlyOrders([]);
            setHourlyClicks([]);
            setReferrerEff([]);
            setClickDelays([]);
            setCancellationByDay([]);
          }
        })
        .finally(() => {
          if (!cancelled) setClickInsightsLoading(false);
        });
    }, INSIGHTS_DEBOUNCE_MS);

    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [
    currentFilter.fromDate,
    currentFilter.toDate,
    currentFilter.limit,
    accountFilter,
    days,
  ]);

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

      {/* ============ Tỉ lệ hoàn hủy top sản phẩm — sort DESC theo % hủy ============ */}
      <CancellationRateChart
        data={cancellationByDay}
        onSelectSubId={(subIds) => {
          // Lookup product trong aggregate list theo sub_id key (join \x1f).
          // Match found → mở dialog detail; không → silent (SP ngoài source filter).
          const key = subIds.join("\x1f");
          const match = products.find((p) => p.subIds.join("\x1f") === key);
          if (match) setSelectedProduct(match);
        }}
      />

      {/* ============ Click-to-order delay histogram ============ */}
      <ClickDelayChart data={clickDelays} />

      {/* ============ Referrer efficiency leaderboard ============ */}
      <ReferrerEfficiencyTable rows={referrerEff} fees={settings.profitFees} />

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

