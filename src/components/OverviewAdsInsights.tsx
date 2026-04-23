import type {
  AdsEfficiencyMetrics,
  AggregatedProductRow,
  BreakevenAnalysis,
  ExtremumDay,
  FunnelMetrics,
  Tone,
} from "../formulas";
import { fmtDate, fmtInt, fmtPct, fmtVnd, toneTextClass } from "../formulas";

interface InsightsProps {
  winners: AggregatedProductRow[];
  losers: AggregatedProductRow[];
  breakeven: BreakevenAnalysis;
  funnel: FunnelMetrics;
  efficiency: AdsEfficiencyMetrics;
  bestDay: ExtremumDay | null;
  worstDay: ExtremumDay | null;
  showAds: boolean;
  onSelectProduct: (product: AggregatedProductRow) => void;
}

export function OverviewAdsInsights({
  winners,
  losers,
  breakeven,
  funnel,
  efficiency,
  bestDay,
  worstDay,
  showAds,
  onSelectProduct,
}: InsightsProps) {
  return (
    <div className="space-y-4">
      {showAds && <EfficiencyRow efficiency={efficiency} />}
      {showAds && <BreakevenCard breakeven={breakeven} />}
      <FunnelCard funnel={funnel} showAds={showAds} />
      {showAds && (bestDay || worstDay) && (
        <ExtremumDaysCard best={bestDay} worst={worstDay} />
      )}
      {showAds && (
        <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <WinnersLosersTable
            title="Top 5 nên SCALE"
            icon="rocket_launch"
            iconColor="text-green-400"
            subtitle="ROI cao + spend đủ lớn → nhân ngân sách"
            rows={winners}
            tone="positive"
            onSelectProduct={onSelectProduct}
            empty="Chưa có sản phẩm nào đạt ROI dương với spend đủ lớn"
          />
          <WinnersLosersTable
            title="Top 5 nên CUT"
            icon="trending_down"
            iconColor="text-red-400"
            subtitle="Lỗ nhiều nhất → tắt ads hoặc tối ưu creative"
            rows={losers}
            tone="negative"
            onSelectProduct={onSelectProduct}
            empty="Không có sản phẩm lỗ đáng kể 👏"
          />
        </section>
      )}
    </div>
  );
}

// =========================================================
// EFFICIENCY METRICS (ROAS / EPC / CPM / AOV / daily spend / pending %)
// =========================================================

function EfficiencyRow({ efficiency }: { efficiency: AdsEfficiencyMetrics }) {
  const { roas, epc, cpm, aov, avgDailySpend } = efficiency;
  return (
    <section className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-2">
      <header className="mb-3 flex items-center gap-2">
        <span className="material-symbols-rounded text-shopee-400">
          speed
        </span>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
          Chỉ số hiệu quả ads
        </h3>
      </header>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-5">
        <MicroStat
          label="ROAS"
          value={roas !== null ? `${roas.toFixed(2)}×` : "—"}
          tone={roas === null ? "muted" : roas >= 1 ? "positive" : "negative"}
          tooltip={
            "ROAS = Hoa hồng ròng / Spend.\n" +
            "• ≥ 1× = revenue đủ bù ads (chưa tính thuế khác)\n" +
            "• < 1× = đốt tiền, đang lỗ"
          }
        />
        <MicroStat
          label="EPC"
          value={epc !== null ? fmtVnd(epc) : "—"}
          tone="neutral"
          tooltip="EPC (Earnings per Click) = Hoa hồng ròng / Click Shopee"
        />
        <MicroStat
          label="CPM"
          value={cpm !== null ? fmtVnd(cpm) : "—"}
          tone="muted"
          tooltip="CPM = Spend / Impressions × 1000. Giá cho 1000 lượt hiển thị"
        />
        <MicroStat
          label="AOV"
          value={aov !== null ? fmtVnd(aov) : "—"}
          tone="neutral"
          tooltip="Average Order Value = GMV / Số đơn"
        />
        <MicroStat
          label="Spend TB/ngày"
          value={avgDailySpend !== null ? fmtVnd(avgDailySpend) : "—"}
          tone="muted"
          tooltip="Spend / số ngày có data — biết ngân sách bạn đang chạy"
        />
      </div>
    </section>
  );
}

function MicroStat({
  label,
  value,
  tone,
  tooltip,
}: {
  label: string;
  value: string;
  tone: Tone;
  tooltip?: string;
}) {
  return (
    <div
      className="rounded-lg bg-surface-4 px-3 py-2.5"
      title={tooltip}
    >
      <p
        className={`text-[10px] font-semibold uppercase tracking-wider text-white/55 ${
          tooltip ? "cursor-help" : ""
        }`}
      >
        {label}
      </p>
      <p
        className={`mt-1 truncate text-xl font-bold tabular-nums ${toneTextClass(tone)}`}
      >
        {value}
      </p>
    </div>
  );
}

// =========================================================
// BREAKEVEN
// =========================================================

function BreakevenCard({ breakeven }: { breakeven: BreakevenAnalysis }) {
  const { breakevenCr, currentCr, gap, netPerOrder } = breakeven;
  const insufficient =
    breakevenCr === null || currentCr === null || netPerOrder === null;
  const positive = gap !== null && gap >= 0;

  return (
    <section className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-2">
      <header className="mb-3 flex items-center gap-2">
        <span className="material-symbols-rounded text-shopee-400">flag</span>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
          Điểm hòa vốn CR
        </h3>
        <span
          className="material-symbols-rounded cursor-help text-sm text-white/40"
          title={
            "CR breakeven = spend / (click Shopee × hoa hồng ròng TB / đơn) × 100%.\n" +
            "Là CR tối thiểu để lợi nhuận bằng 0 với mức spend + traffic hiện tại.\n" +
            "Gap = CR hiện tại - CR breakeven. Dương = đang lãi, âm = đang lỗ."
          }
        >
          help
        </span>
      </header>

      {insufficient ? (
        <div className="text-sm text-white/50">
          Cần có đủ spend + click Shopee + đơn để tính điểm hòa vốn.
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
          <BreakevenStat
            label="CR hiện tại"
            value={fmtPct(currentCr!)}
            tone={positive ? "positive" : "negative"}
            sub={`${fmtVnd(netPerOrder!)} / đơn (ròng)`}
          />
          <BreakevenStat
            label="CR cần đạt (hòa vốn)"
            value={fmtPct(breakevenCr!)}
            tone="muted"
            sub="Tối thiểu để profit = 0"
          />
          <BreakevenStat
            label={positive ? "Biên an toàn" : "Cần cải thiện"}
            value={gap !== null ? fmtPct(Math.abs(gap)) : "—"}
            tone={positive ? "positive" : "negative"}
            sub={
              positive
                ? `CR hiện cao hơn breakeven ${fmtPct(gap!)}`
                : `Cần tăng CR thêm ${fmtPct(Math.abs(gap!))}`
            }
          />
        </div>
      )}
    </section>
  );
}

function BreakevenStat({
  label,
  value,
  sub,
  tone,
}: {
  label: string;
  value: string;
  sub?: string;
  tone: Tone;
}) {
  return (
    <div className="rounded-lg bg-surface-4 px-4 py-3">
      <p className="text-[11px] font-medium uppercase tracking-wider text-white/55">
        {label}
      </p>
      <p
        className={`mt-1 truncate text-2xl font-bold tabular-nums ${toneTextClass(tone)}`}
      >
        {value}
      </p>
      {sub && (
        <p className="mt-1 truncate text-[11px] text-white/45" title={sub}>
          {sub}
        </p>
      )}
    </div>
  );
}

// =========================================================
// FUNNEL
// =========================================================

function FunnelCard({
  funnel,
  showAds,
}: {
  funnel: FunnelMetrics;
  showAds: boolean;
}) {
  // Bar width scale theo step lớn nhất (thường impressions khi có FB data).
  const max = Math.max(
    funnel.impressions,
    funnel.adsClicks,
    funnel.shopeeClicks,
    funnel.orders,
    1,
  );
  const widthOf = (n: number) =>
    Math.max(n > 0 ? (n / max) * 100 : 0, n > 0 ? 4 : 0);

  const hasImpressions = showAds && funnel.impressions > 0;

  return (
    <section className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-2">
      <header className="mb-3 flex items-center gap-2">
        <span className="material-symbols-rounded text-shopee-400">
          filter_alt
        </span>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
          Phễu chuyển đổi
        </h3>
        <span
          className="material-symbols-rounded cursor-help text-sm text-white/40"
          title={
            "CTR (FB)   = Click ADS / Impressions — ad creative có bắt mắt không\n" +
            "CTR Shopee = Click Shopee / Click ADS — ad→landing có relevance không\n" +
            "CR         = Đơn / Click Shopee — sản phẩm có thuyết phục không\n" +
            "Step drop nhiều nhất = bottleneck cần fix."
          }
        >
          help
        </span>
      </header>

      <div className="space-y-3">
        {hasImpressions && (
          <FunnelRow
            label="Impressions"
            value={funnel.impressions}
            widthPct={widthOf(funnel.impressions)}
            color="from-violet-700 to-violet-400"
            icon="visibility"
          />
        )}
        {hasImpressions && (
          <StepArrow
            label="CTR (FB)"
            pct={funnel.ctrFb}
            tooltip="Click ADS / Impressions — ad có thu hút không"
          />
        )}
        {showAds && (
          <FunnelRow
            label="Click ADS"
            value={funnel.adsClicks}
            widthPct={widthOf(funnel.adsClicks)}
            color="from-orange-600 to-orange-400"
            icon="ads_click"
          />
        )}
        {showAds && (
          <StepArrow
            label="CTR Shopee"
            pct={funnel.ctrShopee}
            tooltip="Click Shopee / Click ADS — ad→landing relevance"
          />
        )}
        <FunnelRow
          label="Click Shopee"
          value={funnel.shopeeClicks}
          widthPct={widthOf(funnel.shopeeClicks)}
          color="from-shopee-700 to-shopee-400"
          icon="mouse"
        />
        <StepArrow
          label="CR"
          pct={funnel.cr}
          tooltip="Đơn / Click Shopee — product conversion"
        />
        <FunnelRow
          label="Số đơn"
          value={funnel.orders}
          widthPct={widthOf(funnel.orders)}
          color="from-green-700 to-green-400"
          icon="shopping_cart"
        />
      </div>
    </section>
  );
}

// =========================================================
// BEST / WORST DAY + REFERRERS
// =========================================================

function ExtremumDaysCard({
  best,
  worst,
}: {
  best: ExtremumDay | null;
  worst: ExtremumDay | null;
}) {
  return (
    <section className="grid grid-cols-1 gap-4 md:grid-cols-2">
      <DayCard
        day={best}
        label="Ngày lãi cao nhất"
        icon="stars"
        tone="positive"
        emptyText="Chưa có ngày nào có lãi trong khoảng này"
        emptyIcon="sentiment_dissatisfied"
      />
      <DayCard
        day={worst}
        label="Ngày lỗ nặng nhất"
        icon="warning"
        tone="negative"
        emptyText="Không có ngày nào lỗ 🎉"
        emptyIcon="celebration"
      />
    </section>
  );
}

function DayCard({
  day,
  label,
  icon,
  tone,
  emptyText,
  emptyIcon,
}: {
  day: ExtremumDay | null;
  label: string;
  icon: string;
  tone: "positive" | "negative";
  emptyText: string;
  emptyIcon: string;
}) {
  const accent = toneTextClass(tone);
  if (!day) {
    return (
      <div className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-2">
        <div className="flex items-center gap-2">
          <span className="material-symbols-rounded text-white/30">
            {icon}
          </span>
          <p className="text-xs font-semibold uppercase tracking-wider text-white/50">
            {label}
          </p>
        </div>
        <div className="mt-3 flex items-center gap-2 text-sm text-white/50">
          <span className="material-symbols-rounded text-base text-white/40">
            {emptyIcon}
          </span>
          {emptyText}
        </div>
      </div>
    );
  }
  return (
    <div className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-2">
      <div className="flex items-center gap-2">
        <span className={`material-symbols-rounded ${accent}`}>{icon}</span>
        <p className="text-xs font-semibold uppercase tracking-wider text-white/60">
          {label}
        </p>
        <span className="ml-auto text-sm font-medium text-white/80">
          {fmtDate(day.date)}
        </span>
      </div>
      <p
        className={`mt-2 truncate text-2xl font-bold tabular-nums ${accent}`}
        title={fmtVnd(day.profit)}
      >
        {fmtVnd(day.profit)}
      </p>
      <div className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-[11px] text-white/55">
        <span>
          Spend <b className="text-white/75">{fmtVnd(day.spend)}</b>
        </span>
        <span>
          ROI <b className={accent}>{day.roi !== null ? fmtPct(day.roi) : "—"}</b>
        </span>
        <span>{fmtInt(day.orders)} đơn</span>
      </div>
    </div>
  );
}

function FunnelRow({
  label,
  value,
  widthPct,
  color,
  icon,
}: {
  label: string;
  value: number;
  widthPct: number;
  color: string;
  icon: string;
}) {
  return (
    <div className="flex items-center gap-3">
      <div className="flex w-[140px] shrink-0 items-center gap-1.5 text-sm text-white/70">
        <span className="material-symbols-rounded text-sm text-white/40">
          {icon}
        </span>
        {label}
      </div>
      <div className="relative h-7 flex-1 overflow-hidden rounded-md bg-surface-4">
        <div
          className={`h-full rounded-md bg-gradient-to-r ${color} transition-[width] duration-300`}
          style={{ width: `${widthPct}%` }}
        />
      </div>
      <div className="w-[90px] shrink-0 text-right text-sm font-semibold tabular-nums text-white/90">
        {fmtInt(value)}
      </div>
    </div>
  );
}

function StepArrow({
  label,
  pct,
  tooltip,
}: {
  label: string;
  pct: number | null;
  tooltip: string;
}) {
  return (
    <div className="flex items-center gap-2 pl-[140px] text-xs text-white/50">
      <span className="material-symbols-rounded text-base text-white/30">
        south
      </span>
      <span title={tooltip} className="cursor-help">
        <b className="tabular-nums text-shopee-300">
          {pct !== null ? fmtPct(pct) : "—"}
        </b>{" "}
        {label}
      </span>
    </div>
  );
}

// =========================================================
// WINNERS / LOSERS
// =========================================================

function WinnersLosersTable({
  title,
  icon,
  iconColor,
  subtitle,
  rows,
  tone,
  empty,
  onSelectProduct,
}: {
  title: string;
  icon: string;
  iconColor: string;
  subtitle: string;
  rows: AggregatedProductRow[];
  tone: "positive" | "negative";
  empty: string;
  onSelectProduct: (row: AggregatedProductRow) => void;
}) {
  const profitClass = toneTextClass(tone);

  return (
    <section className="overflow-hidden rounded-xl bg-surface-2 shadow-elev-2">
      <header className="flex items-center gap-2 border-b border-surface-8 px-5 py-3">
        <span className={`material-symbols-rounded ${iconColor}`}>{icon}</span>
        <div className="min-w-0 flex-1">
          <h3 className="text-sm font-semibold text-white/90">{title}</h3>
          <p className="text-[11px] text-white/50">{subtitle}</p>
        </div>
        <span className="rounded-full bg-surface-4 px-2 py-0.5 text-xs text-white/60">
          {rows.length}
        </span>
      </header>
      {rows.length === 0 ? (
        <div className="px-5 py-8 text-center text-sm text-white/50">{empty}</div>
      ) : (
        <ul className="divide-y divide-surface-8">
          {rows.map((r) => {
            const roi = r.totalSpend > 0 ? (r.profit / r.totalSpend) * 100 : null;
            return (
              <li key={r.subIds.join("\x1f")}>
                <button
                  type="button"
                  onClick={() => onSelectProduct(r)}
                  className="flex w-full items-center gap-3 px-5 py-2.5 text-left hover:bg-shopee-500/10"
                  title={r.displayName}
                >
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-semibold text-white/95">
                      {r.displayName || (
                        <span className="italic font-normal text-white/40">
                          (chưa đặt tên)
                        </span>
                      )}
                    </p>
                    <p className="truncate text-[11px] text-white/50">
                      Chi tiêu {fmtVnd(r.totalSpend)} · {fmtInt(r.ordersCount)} đơn ·{" "}
                      {r.daysActive} ngày
                    </p>
                  </div>
                  <div className="shrink-0 text-right">
                    <p
                      className={`text-sm font-bold tabular-nums ${profitClass}`}
                    >
                      {fmtVnd(r.profit)}
                    </p>
                    <p className={`text-[11px] tabular-nums ${profitClass}`}>
                      ROI {roi !== null ? fmtPct(roi) : "—"}
                    </p>
                  </div>
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
