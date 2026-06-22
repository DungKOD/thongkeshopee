import { memo, startTransition, useDeferredValue, useEffect, useState } from "react";
import { useProgressiveCount } from "../hooks/useProgressiveCount";
import type { UiDay } from "../types";
import { invoke } from "../lib/tauri";
import type { AccountFilterMode, DaysFilter } from "../hooks/useDbStats";
import {
  fmtDate,
  fmtInt,
  fmtVnd,
  type AggregatedProductRow,
  type SourceFilter,
} from "../formulas";
import { useSettings } from "../hooks/useSettings";
import { useOverviewAnalytics } from "../hooks/useOverviewAnalytics";
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

// Progressive mount: từ section 0 đến SECTION_COUNT. Mỗi section nặng
// (Recharts SVG, table large) mount vào 1 idle frame riêng → main thread
// không bao giờ chiếm > 1 frame cho 1 chart. User thấy UI hình thành dần
// từ trên xuống.
const SECTION_COUNT = 8;
// Section index map (xem JSX bên dưới):
// 0 = Source filter (luôn render — light)
// 1 = PrimaryKpiRow + MCN banner + SecondaryKpiRow
// 2 = OverviewTrendChart (heavy: ComposedChart)
// 3 = HourlyChart × 2 (BarChart)
// 4 = CancellationRateChart
// 5 = ClickDelayChart
// 6 = ReferrerEfficiencyTable
// 7 = OverviewAdsInsights + ProductsTable
const INITIAL_SECTIONS = 2; // filter + KPI ngay frame đầu
const SECTIONS_PER_FRAME = 1;

// =============================================================
// Module-level cache cho 5 BE insights — survives OverviewTab unmount/remount.
// Khi user chuyển sang tab khác và quay lại với cùng filter → instant hit,
// không phải đợi 5 BE query chạy lại + Recharts không "đơ" lúc swap data.
// AppInner gọi `clearOverviewInsightsCache()` sau mỗi mutation để invalidate.
// LRU cap 16 entry để user adjust filter qua-lại không leak.
// =============================================================
type InsightsCacheEntry = {
  hourlyOrders: HourlyBucket[];
  hourlyClicks: HourlyBucket[];
  referrerEff: ReferrerEfficiency[];
  clickDelays: DelayBucket[];
  cancellationByDay: CancellationByDayBucket[];
};
const INSIGHTS_CACHE_MAX = 16;
const insightsCache = new Map<string, InsightsCacheEntry>();

/** Xóa toàn bộ insights cache. Gọi sau mutation (save/import/revert) hoặc
 *  khi user click "Tải lại" để force refetch. */
export function clearOverviewInsightsCache(): void {
  insightsCache.clear();
}

function insightsCacheKey(
  beFromDate: string,
  beToDate: string,
  accountFilter: AccountFilterMode,
): string {
  return JSON.stringify({
    from: beFromDate,
    to: beToDate,
    account: accountFilter,
  });
}


/// Memo wrapper — AppInner re-render mỗi keystroke trong sub_id input,
/// nhưng OverviewTab props (days, currentFilter, accountFilter) thường stable
/// theo reference (useMemo + useDeferredValue ở parent). Bail out re-render
/// giúp gõ search không trigger re-paint 5 chart.
export const OverviewTab = memo(OverviewTabImpl);

function OverviewTabImpl({
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

  // useDeferredValue: input cho analytics worker — khi days đổi (filter
  // switch), defer giữ giá trị cũ trong khi background-priority re-render,
  // tránh flash UI giữa các lần worker chưa kịp trả kết quả mới.
  const deferredDays = useDeferredValue(days);
  const deferredSource = useDeferredValue(source);

  // Compute analytics OFF main thread qua Web Worker. Hook giữ analytics cũ
  // trong khi worker đang tính → UI không flash, không freeze 50-200ms như
  // trước. Lần đầu (sync trên main thread) chỉ tốn O(0) vì days thường rỗng
  // lúc mount, sau đó mọi compute đều qua worker.
  const { analytics } = useOverviewAnalytics(
    deferredDays,
    settings.clickSources,
    settings.profitFees,
    deferredSource,
  );
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

  // Progressive section mount: render section 0+1 (filter + KPI) ngay frame
  // đầu → user thấy số liệu tức thì. Sections nặng (Recharts, OverviewAds
  // Insights, ProductsTable) stream dần qua idle frames → main thread không
  // bao giờ block.
  //
  // resetKey = null (constant): KHÔNG reset khi data/filter đổi. Sections
  // chỉ progressive mount 1 lần lúc OverviewTab mount đầu tiên. Sau đó
  // filter switch chỉ re-render với data mới, không unmount → tránh
  // skeleton flash khi user adjust khoảng ngày.
  //
  // Khi user rời tab Overview → OverviewTab unmount → lần sau quay lại
  // mount lại từ đầu → progressive lại từ INITIAL_SECTIONS.
  const mountedSections = useProgressiveCount(
    SECTION_COUNT,
    null,
    INITIAL_SECTIONS,
    SECTIONS_PER_FRAME,
  );

  // 5 BE aggregates dưới đây fetch song song khi filter đổi (date range /
  // account). KHÔNG pass subIdFilter — Overview scope là all products.
  // Overlay clicks lên HourlyBucket thay vì giữ 2 state riêng — HourlyChart
  // nhận cả 2 dataset với `metric` khác nhau.
  //
  // **Streaming**: KHÔNG dùng Promise.all (trước đây phải chờ slowest query
  // — referrer_efficiency — xong mới render đc cái gì). Giờ mỗi invoke có
  // .then() riêng → chart nào BE xong trước, hiển thị ngay. User thấy
  // Hourly clicks (~50ms) trước cancellation (~300ms) → cảm giác mượt.
  // setState bọc startTransition → React 19 schedule background priority,
  // không chặn input/scroll giữa các lần state update.
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

    // Derive BE date range NGOÀI setTimeout để check cache đồng bộ trước khi
    // debounce. Cache hit → setState ngay frame này, user thấy data tức thì
    // khi quay lại tab Overview với filter cũ.
    let beFromDate = currentFilter.fromDate;
    let beToDate = currentFilter.toDate;
    if (!beFromDate || !beToDate) {
      const sortedDates = days.map((d) => d.date).sort();
      beFromDate = beFromDate ?? sortedDates[0];
      beToDate = beToDate ?? sortedDates[sortedDates.length - 1];
    }
    const cacheKey = insightsCacheKey(beFromDate!, beToDate!, accountFilter);
    const cached = insightsCache.get(cacheKey);
    if (cached) {
      // Cache hit: BATCH cả 5 setState trong 1 startTransition → 1 commit duy
      // nhất, charts update đồng loạt. Trước đây stagger qua rAF → 5 frames
      // chart loading lẻ tẻ ("nhì nhằng" — user complaint). Recharts SVG nặng
      // nhưng React 19 + concurrent rendering xử OK trong 1 frame.
      // LRU touch: re-insert để bump lên cuối (giữ entry thường dùng).
      insightsCache.delete(cacheKey);
      insightsCache.set(cacheKey, cached);
      startTransition(() => {
        setHourlyClicks(cached.hourlyClicks);
        setHourlyOrders(cached.hourlyOrders);
        setCancellationByDay(cached.cancellationByDay);
        setClickDelays(cached.clickDelays);
        setReferrerEff(cached.referrerEff);
        setClickInsightsLoading(false);
      });
      return;
    }

    // KHÔNG setClickInsightsLoading(true) ở cache-miss → giữ chart show data
    // cũ (hoặc "no data" nếu empty) trong khi fetch background. Tránh flash
    // skeleton → data làm chart re-paint 2 lần. Khi tickDone batch xong sẽ
    // swap 1 lần duy nhất từ stale/empty sang final data.

    // Debounce: user đổi filter liên tục (gõ ngày, chuyển account) sẽ dồn
    // về 1 lần query thay vì spam 5 BE command mỗi keystroke.
    const timer = setTimeout(() => {
      if (cancelled) return;

      const beFilter: DaysFilter = {
        fromDate: beFromDate,
        toDate: beToDate,
        accountFilter,
      };

      // Track 5 promise riêng → ghi vào `pending` khi resolve. Khi cái cuối
      // resolve mới BATCH 1 setState duy nhất → user thấy charts update 1 lần,
      // không "nhì nhằng" như trước (5 setState độc lập = 5 re-render). Lỗi
      // 1 query KHÔNG kill cả 4 còn lại — chart đó sẽ giữ data cũ.
      let remaining = 5;
      const pending: Partial<InsightsCacheEntry> = {};
      const tickDone = () => {
        if (cancelled) return;
        remaining -= 1;
        if (remaining > 0) return;
        // Tất cả 5 xong (kể cả lỗi) → batch commit + cache write.
        startTransition(() => {
          if (pending.hourlyOrders) setHourlyOrders(pending.hourlyOrders);
          if (pending.hourlyClicks) setHourlyClicks(pending.hourlyClicks);
          if (pending.referrerEff) setReferrerEff(pending.referrerEff);
          if (pending.clickDelays) setClickDelays(pending.clickDelays);
          if (pending.cancellationByDay) setCancellationByDay(pending.cancellationByDay);
          setClickInsightsLoading(false);
        });
        // Chỉ cache khi đủ cả 5 thành công (không cache partial — query lỗi
        // → giữ trạng thái mặc định, lần sau retry).
        if (
          pending.hourlyOrders &&
          pending.hourlyClicks &&
          pending.referrerEff &&
          pending.clickDelays &&
          pending.cancellationByDay
        ) {
          // LRU evict: drop oldest nếu vượt cap.
          if (insightsCache.size >= INSIGHTS_CACHE_MAX) {
            const oldest = insightsCache.keys().next().value;
            if (oldest !== undefined) insightsCache.delete(oldest);
          }
          insightsCache.set(cacheKey, pending as InsightsCacheEntry);
        }
      };

      invoke<HourlyOrderBucketDto[]>("load_hourly_orders", { filter: beFilter })
        .then((orders) => {
          pending.hourlyOrders = orders.map((b) => ({
            hour: b.hour,
            orders: b.orders,
            orderValue: b.orderValue,
            commission: b.commission,
            clicks: 0,
          }));
        })
        .catch((e) => console.error("[load_hourly_orders] failed:", e))
        .finally(tickDone);

      invoke<HourlyClickBucketDto[]>("load_hourly_clicks", { filter: beFilter })
        .then((clicks) => {
          pending.hourlyClicks = clicks.map((b) => ({
            hour: b.hour,
            orders: 0,
            orderValue: 0,
            commission: 0,
            clicks: b.clicks,
          }));
        })
        .catch((e) => console.error("[load_hourly_clicks] failed:", e))
        .finally(tickDone);

      invoke<ReferrerEfficiency[]>("load_referrer_efficiency", { filter: beFilter })
        .then((referrers) => {
          pending.referrerEff = referrers;
        })
        .catch((e) => console.error("[load_referrer_efficiency] failed:", e))
        .finally(tickDone);

      invoke<DelayBucket[]>("load_click_order_delays", { filter: beFilter })
        .then((delays) => {
          pending.clickDelays = delays;
        })
        .catch((e) => console.error("[load_click_order_delays] failed:", e))
        .finally(tickDone);

      invoke<CancellationByDayBucket[]>("load_cancellation_by_subid", {
        filter: beFilter,
      })
        .then((cancellations) => {
          pending.cancellationByDay = cancellations;
        })
        .catch((e) => console.error("[load_cancellation_by_subid] failed:", e))
        .finally(tickDone);
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

      {/* Section 1: KPI block — luôn ưu tiên render đầu tiên. content-
          visibility:auto giúp browser skip layout/paint khi scroll off-screen. */}
      {mountedSections >= 1 && (
        <DeferredSection>
          {/* ============ KPI primary (lớn) ============ */}
          <PrimaryKpiRow totals={totals} source={source} />

          {/* ============ MCN fee banner (chỉ show khi có) ============ */}
          {totals.mcnFeeTotal > 0 && (
            <section
              className="mt-6 flex items-center gap-3 rounded-xl border border-amber-500/30 bg-amber-500/5 px-4 py-3 text-sm text-amber-100/90"
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
          <div className="mt-6">
            <SecondaryKpiRow totals={totals} source={source} />
          </div>
        </DeferredSection>
      )}

      {/* Section 2: Trend chart — Recharts ComposedChart nặng nhất (50-100ms
          SVG paint). Mount ở idle frame riêng. */}
      {mountedSections >= 2 ? (
        <DeferredSection>
          <OverviewTrendChart
            data={trendData}
            cumulative={cumulativeData}
            showAds={source === "all"}
          />
        </DeferredSection>
      ) : (
        <SectionSkeleton label="Biểu đồ trend" minHeight={420} />
      )}

      {/* Section 3: Hourly charts (BarChart × 2). */}
      {mountedSections >= 3 ? (
        <DeferredSection>
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
        </DeferredSection>
      ) : (
        <SectionSkeleton label="Biểu đồ giờ" minHeight={320} />
      )}

      {/* Section 4: Cancellation rate (BarChart). */}
      {mountedSections >= 4 ? (
        <DeferredSection>
          <CancellationRateChart
            data={cancellationByDay}
            onSelectSubId={(subIds) => {
              // Lookup product trong aggregate list theo sub_id key (join \x1f).
              // Match found → mở dialog detail; không → silent.
              const key = subIds.join("\x1f");
              const match = products.find((p) => p.subIds.join("\x1f") === key);
              if (match) setSelectedProduct(match);
            }}
          />
        </DeferredSection>
      ) : (
        <SectionSkeleton label="Tỉ lệ hoàn hủy" minHeight={320} />
      )}

      {/* Section 5: Click-to-order delay histogram. */}
      {mountedSections >= 5 ? (
        <DeferredSection>
          <ClickDelayChart data={clickDelays} />
        </DeferredSection>
      ) : (
        <SectionSkeleton label="Click delay" minHeight={280} />
      )}

      {/* Section 6: Referrer efficiency table. */}
      {mountedSections >= 6 ? (
        <DeferredSection>
          <ReferrerEfficiencyTable rows={referrerEff} fees={settings.profitFees} />
        </DeferredSection>
      ) : (
        <SectionSkeleton label="Referrer efficiency" minHeight={240} />
      )}

      {/* Section 7: Efficiency + Breakeven + Funnel + Best/Worst day. */}
      {mountedSections >= 7 ? (
        <DeferredSection>
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
          <div className="mt-6">
            <ProductsTable
              rows={products}
              source={source}
              onSelectProduct={setSelectedProduct}
            />
          </div>
        </DeferredSection>
      ) : (
        <SectionSkeleton label="Bảng sản phẩm" minHeight={400} />
      )}

      <AggregateProductDialog
        isOpen={!!selectedProduct}
        product={selectedProduct}
        days={deferredDays}
        source={deferredSource}
        accountFilter={accountFilter}
        onClose={() => setSelectedProduct(null)}
      />

      {/* Indicator nhỏ khi còn section chưa mount. */}
      {mountedSections < SECTION_COUNT && (
        <div className="mx-auto flex max-w-xs items-center justify-center gap-2 py-4 text-xs text-white/40">
          <span className="material-symbols-rounded animate-spin text-base">
            sync
          </span>
          Đang dựng {SECTION_COUNT - mountedSections} phần còn lại...
        </div>
      )}
    </div>
  );
}

/**
 * Wrapper với `content-visibility: auto`: browser skip layout/paint của
 * children khi off-screen → render section nặng (Recharts SVG) không tiêu
 * paint budget của initial frame.
 *
 * `contain-intrinsic-size: auto 400px` cho placeholder height ổn định
 * → tránh layout shift khi scroll qua khu vực chưa paint.
 */
function DeferredSection({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        contentVisibility: "auto",
        containIntrinsicSize: "auto 400px",
      }}
    >
      {children}
    </div>
  );
}

/**
 * Skeleton placeholder cho section chưa mount. Giữ layout height ổn định
 * → user scroll không bị shift khi section thật mount vào.
 */
function SectionSkeleton({
  label,
  minHeight,
}: {
  label: string;
  minHeight: number;
}) {
  return (
    <section
      className="flex animate-pulse items-center justify-center rounded-xl border border-surface-8 bg-surface-1/40 text-xs text-white/30"
      style={{ minHeight }}
      aria-busy="true"
      aria-label={`Đang chuẩn bị ${label}`}
    >
      <span className="material-symbols-rounded mr-2 animate-spin text-base text-white/30">
        sync
      </span>
      {label} sẽ hiển thị sau...
    </section>
  );
}

