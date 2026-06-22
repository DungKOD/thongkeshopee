import { memo, useState } from "react";
import {
  fmtInt,
  fmtPct,
  fmtVnd,
  fmtVndCompactVi,
  profitTone,
  roiTone,
  toneIconClass,
  toneTextClass,
  type OverviewTotals,
  type SourceFilter,
  type Tone,
} from "../formulas";

export const ROI_TOOLTIP =
  "ROI = (Hoa hồng sau phí − Tiền ads) / Tiền ads × 100%\n" +
  "• 0% = hòa vốn\n• > 0% = có lãi\n• < 0% = đang lỗ";

function PrimaryKpiRowImpl({
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
  const compactNet = fmtVndCompactVi(totals.netCommission);
  const compactCommission = fmtVndCompactVi(totals.commission);
  const compactGmv = fmtVndCompactVi(totals.orderValueTotal);
  const compactSpend = fmtVndCompactVi(totals.totalSpend);
  return (
    <section className={`grid grid-cols-1 gap-4 ${gridCols}`}>
      <BigKpi
        icon="trending_up"
        label={showAds ? "Lợi nhuận" : "Hoa hồng ròng"}
        labelHint={showAds ? undefined : "(sau thuế & hoàn hủy)"}
        labelTooltip={
          showAds ? undefined : "Hoa hồng ròng = sau thuế & hoàn hủy"
        }
        value={fmtVnd(totals.profit)}
        compact={fmtVndCompactVi(totals.profit)}
        tone={profitToneValue}
        sub={
          showAds
            ? `HH ròng • ${compactNet ?? fmtVnd(totals.netCommission)}`
            : `Gross • ${compactCommission ?? fmtVnd(totals.commission)}`
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
        compact={compactCommission}
        tone="commission"
        sub={`GMV • ${compactGmv ?? fmtVnd(totals.orderValueTotal)}`}
      />
      {showAds ? (
        <BigKpi
          icon="shopping_bag"
          label="Tổng tiền chạy"
          value={fmtVnd(totals.totalSpend)}
          compact={compactSpend}
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

function SecondaryKpiRowImpl({
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
        label="CR"
        value={cr !== null ? fmtPct(cr) : "—"}
        icon="trending_up"
        tooltip="Tỷ lệ chuyển đổi (CR) = Số đơn / Click Shopee × 100%"
      />
      <SmallKpi
        label="GMV"
        value={fmtVnd(totals.orderValueTotal)}
        compact={fmtVndCompactVi(totals.orderValueTotal)}
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
  labelHint,
  labelTooltip,
  value,
  compact,
  sub,
  tone,
}: {
  icon: string;
  label: string;
  /** Hint hiển thị inline nhỏ sau label (vd "(sau thuế & hoàn hủy)"). */
  labelHint?: string;
  /** Tooltip cho cả label + hint khi hover. */
  labelTooltip?: string;
  value: string;
  /** Compact VN-friendly (vd "3,94 tỷ") — show prominently phía trên số đầy đủ. */
  compact?: string | null;
  sub?: string;
  tone: Tone;
}) {
  const [hidden, setHidden] = useState(false);
  return (
    <div className="rounded-xl bg-surface-4 p-5 shadow-elev-2 transition-shadow hover:shadow-elev-4">
      <div className="flex items-center gap-2">
        <span className={`material-symbols-rounded text-lg ${toneIconClass(tone)}`}>
          {icon}
        </span>
        <p
          className={`flex-1 text-xs font-semibold uppercase tracking-wider text-white/55 ${labelTooltip ? "cursor-help" : ""}`}
          title={labelTooltip}
        >
          {label}
          {labelHint && (
            <span className="ml-1 normal-case tracking-normal text-white/35">
              {labelHint}
            </span>
          )}
        </p>
        <button
          onClick={() => setHidden((h) => !h)}
          className="flex h-5 w-5 items-center justify-center rounded text-white/25 hover:text-white/60"
          title={hidden ? "Hiện" : "Ẩn"}
          aria-label={hidden ? "Hiện" : "Ẩn"}
        >
          <span className="material-symbols-rounded text-[14px]">
            {hidden ? "visibility_off" : "visibility"}
          </span>
        </button>
      </div>
      {compact && !hidden && (
        <p
          className="mt-2 flex items-baseline gap-1 text-2xl font-extrabold tabular-nums"
          title={value}
        >
          <span className="bg-gradient-to-r from-amber-300 via-shopee-300 to-amber-200 bg-clip-text text-transparent drop-shadow-[0_0_18px_rgba(238,77,45,0.35)]">
            {compact}
          </span>
        </p>
      )}
      <p
        className={`num-glow ${compact && !hidden ? "mt-0.5 text-base" : "mt-2 text-3xl"} truncate font-bold tabular-nums ${toneTextClass(tone)}`}
        title={hidden ? undefined : value}
      >
        {hidden ? (
          <span className="select-none text-3xl tracking-widest text-white/20">••••</span>
        ) : (
          value
        )}
      </p>
      {sub && (
        <p
          className="mt-1.5 inline-flex max-w-full items-center gap-1 truncate rounded-full bg-white/5 px-2 py-0.5 text-[11px] font-medium text-white/65"
          title={hidden ? undefined : sub}
        >
          {hidden ? (
            <span className="select-none tracking-widest text-white/20">••</span>
          ) : (
            sub
          )}
        </p>
      )}
    </div>
  );
}

function SmallKpi({
  label,
  value,
  compact,
  icon,
  sub,
  tooltip,
  tone = "neutral",
}: {
  label: string;
  value: string;
  /** Compact VN-friendly (vd "3,94 tỷ") — show ngay trên value số đầy đủ. */
  compact?: string | null;
  icon: string;
  sub?: string;
  tooltip?: string;
  /** Brand tone — spend/commission color icon. Default neutral (white/40). */
  tone?: Tone;
}) {
  const [hidden, setHidden] = useState(false);
  const iconCls =
    tone === "neutral" ? "text-white/35" : toneIconClass(tone);
  return (
    <div className="rounded-lg bg-surface-2 px-4 py-3 shadow-elev-1">
      <div className="flex items-center gap-1.5">
        <span className={`material-symbols-rounded text-sm ${iconCls}`}>{icon}</span>
        <p
          className={`flex-1 text-[11px] font-medium uppercase tracking-wider text-white/55 ${tooltip ? "cursor-help" : ""}`}
          title={tooltip}
        >
          {label}
        </p>
        <button
          onClick={() => setHidden((h) => !h)}
          className="flex h-5 w-5 items-center justify-center rounded text-white/25 hover:text-white/60"
          title={hidden ? "Hiện" : "Ẩn"}
          aria-label={hidden ? "Hiện" : "Ẩn"}
        >
          <span className="material-symbols-rounded text-[13px]">
            {hidden ? "visibility_off" : "visibility"}
          </span>
        </button>
      </div>
      {compact && !hidden && (
        <p
          className="mt-1 truncate text-base font-extrabold tabular-nums"
          title={value}
        >
          <span className="bg-gradient-to-r from-amber-300 via-shopee-300 to-amber-200 bg-clip-text text-transparent">
            {compact}
          </span>
        </p>
      )}
      <p
        className={`num-glow ${compact && !hidden ? "mt-0 text-sm" : "mt-1 text-xl"} truncate font-bold tabular-nums text-white/95`}
        title={hidden ? undefined : value}
      >
        {hidden ? (
          <span className="select-none text-xl tracking-widest text-white/20">••••</span>
        ) : (
          value
        )}
      </p>
      {sub && (
        <p className="mt-0.5 truncate text-[11px] text-white/45" title={hidden ? undefined : sub}>
          {hidden ? (
            <span className="select-none tracking-widest text-white/20">••</span>
          ) : (
            sub
          )}
        </p>
      )}
    </div>
  );
}

// memo: totals ref ổn định khi deferredDays giữ nguyên (cache hit) → KPI
// rows skip re-render khi parent (OverviewTab) update vì state khác (vd:
// click insights state).
export const PrimaryKpiRow = memo(PrimaryKpiRowImpl);
export const SecondaryKpiRow = memo(SecondaryKpiRowImpl);
