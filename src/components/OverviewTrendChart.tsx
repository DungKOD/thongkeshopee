import { useMemo, useState } from "react";
import {
  Area,
  AreaChart,
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type {
  CumulativePoint,
  DailyTrendPoint,
  TrendGranularity,
} from "../formulas";
import {
  aggregateTrend,
  availableGranularities,
  computeCumulativeTrend,
  defaultTrendGranularity,
  fmtDate,
  fmtMoneyShort,
  fmtPct,
  fmtVnd,
} from "../formulas";

interface Props {
  /// Data points ASC theo ngày (compute bởi `computeDailyTrend`).
  /// Reflect filter hiện tại (date range / sub_id / account) vì `days`
  /// prop truyền xuống Overview đã filter ở BE.
  data: DailyTrendPoint[];
  cumulative: CumulativePoint[];
  showAds: boolean;
}

type ChartMode = "finance" | "roi" | "cumulative";

const GRANULARITY_LABEL: Record<TrendGranularity, string> = {
  day: "Ngày",
  week: "Tuần",
  month: "Tháng",
};

/**
 * Trend chart ngày theo ngày:
 * - Mode "finance": cột spend (đỏ cam) + cột netCommission (xanh dương) + đường profit (xanh lá / đỏ theo tone).
 * - Mode "roi": đường ROI % với reference line 0% (breakeven). Tô area xanh nếu >0, đỏ nếu <0 qua coloring stroke.
 */
export function OverviewTrendChart({ data, cumulative, showAds }: Props) {
  const [mode, setMode] = useState<ChartMode>("finance");

  // Granularity: data nhiều ngày (>31) → auto gom tuần/tháng. User vẫn có
  // thể override qua toggle. Default re-compute khi data length đổi
  // (ví dụ filter range thay đổi).
  const granularityOptions = useMemo(
    () => availableGranularities(data.length),
    [data.length],
  );
  const defaultGran = useMemo(
    () => defaultTrendGranularity(data.length),
    [data.length],
  );
  const [granularityOverride, setGranularityOverride] =
    useState<TrendGranularity | null>(null);
  // Override invalid (vd user chọn "day" nhưng data 500 ngày) → fallback default.
  const granularity: TrendGranularity =
    granularityOverride && granularityOptions.includes(granularityOverride)
      ? granularityOverride
      : defaultGran;

  // Aggregate data + recompute cumulative theo granularity.
  // day → no-op (return clone); week/month → group + sum + weighted ROI.
  const aggregated = useMemo(
    () => aggregateTrend(data, granularity),
    [data, granularity],
  );
  const aggregatedCumulative = useMemo(
    () =>
      granularity === "day" ? cumulative : computeCumulativeTrend(aggregated),
    [granularity, cumulative, aggregated],
  );

  // Shopee-only: không hiển thị mode ROI (không có spend → không có ROI nghĩa).
  const effectiveMode: ChartMode = showAds ? mode : "finance";

  const empty = aggregated.length === 0;

  return (
    <section className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-2">
      <header className="mb-3 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <span className="material-symbols-rounded text-shopee-400">
            monitoring
          </span>
          <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
            Xu hướng theo ngày
          </h3>
          <span className="rounded-full bg-shopee-900/40 px-2 py-0.5 text-xs text-shopee-300">
            {data.length} ngày
            {granularity !== "day"
              ? ` → ${aggregated.length} ${GRANULARITY_LABEL[granularity].toLowerCase()}`
              : ""}
          </span>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {/* Granularity toggle — chỉ hiển thị khi có ≥ 2 option (vd > 31 ngày). */}
          {granularityOptions.length > 1 && (
            <div className="flex rounded-full bg-surface-4 p-0.5">
              {granularityOptions.map((g) => (
                <ModeButton
                  key={g}
                  active={granularity === g}
                  onClick={() => setGranularityOverride(g)}
                >
                  {GRANULARITY_LABEL[g]}
                </ModeButton>
              ))}
            </div>
          )}
          {showAds && (
            <div className="flex rounded-full bg-surface-4 p-0.5">
              <ModeButton
                active={effectiveMode === "finance"}
                onClick={() => setMode("finance")}
              >
                Chi tiêu / Lợi nhuận
              </ModeButton>
              <ModeButton
                active={effectiveMode === "roi"}
                onClick={() => setMode("roi")}
              >
                ROI %
              </ModeButton>
              <ModeButton
                active={effectiveMode === "cumulative"}
                onClick={() => setMode("cumulative")}
              >
                Lãi tích luỹ
              </ModeButton>
            </div>
          )}
        </div>
      </header>

      {empty ? (
        <div className="flex h-[280px] items-center justify-center text-sm text-white/40">
          Không có dữ liệu để vẽ biểu đồ
        </div>
      ) : (
        <div className="h-[280px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            {effectiveMode === "cumulative" ? (
              <AreaChart
                data={aggregatedCumulative}
                margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
              >
                <defs>
                  <linearGradient id="grad-cum-profit" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#22c55e" stopOpacity={0.6} />
                    <stop offset="95%" stopColor="#22c55e" stopOpacity={0.05} />
                  </linearGradient>
                  <linearGradient id="grad-cum-spend" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#f97316" stopOpacity={0.4} />
                    <stop offset="95%" stopColor="#f97316" stopOpacity={0.02} />
                  </linearGradient>
                </defs>
                <CartesianGrid stroke="#ffffff10" vertical={false} />
                <XAxis
                  dataKey="dateShort"
                  tick={{ fill: "#ffffff70", fontSize: 11 }}
                  axisLine={{ stroke: "#ffffff20" }}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fill: "#ffffff70", fontSize: 11 }}
                  axisLine={{ stroke: "#ffffff20" }}
                  tickLine={false}
                  tickFormatter={fmtMoneyShort}
                  width={55}
                />
                <Tooltip
                  content={(props: unknown) => (
                    <CumulativeTooltip
                      {...(props as TrendTooltipProps)}
                    />
                  )}
                  cursor={{ stroke: "#ffffff20" }}
                />
                <Legend
                  wrapperStyle={{ fontSize: 12, paddingTop: 8 }}
                  iconType="circle"
                />
                <ReferenceLine
                  y={0}
                  stroke="#ffffff30"
                  strokeDasharray="3 3"
                />
                <Area
                  type="monotone"
                  dataKey="cumulativeSpend"
                  name="Chi tiêu tích luỹ"
                  stroke="#f97316"
                  fill="url(#grad-cum-spend)"
                  strokeWidth={2}
                />
                <Area
                  type="monotone"
                  dataKey="cumulativeProfit"
                  name="Lãi tích luỹ"
                  stroke="#22c55e"
                  fill="url(#grad-cum-profit)"
                  strokeWidth={2.5}
                />
              </AreaChart>
            ) : effectiveMode === "finance" ? (
              <ComposedChart
                data={aggregated}
                margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
              >
                <CartesianGrid stroke="#ffffff10" vertical={false} />
                <XAxis
                  dataKey="dateShort"
                  tick={{ fill: "#ffffff70", fontSize: 11 }}
                  axisLine={{ stroke: "#ffffff20" }}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fill: "#ffffff70", fontSize: 11 }}
                  axisLine={{ stroke: "#ffffff20" }}
                  tickLine={false}
                  tickFormatter={fmtMoneyShort}
                  width={55}
                />
                <Tooltip
                  content={(props: unknown) => (
                    <TrendTooltip
                      {...(props as TrendTooltipProps)}
                      mode="finance"
                    />
                  )}
                  cursor={{ fill: "#ffffff08" }}
                />
                <Legend
                  wrapperStyle={{ fontSize: 12, paddingTop: 8 }}
                  iconType="circle"
                />
                {showAds && (
                  <Bar
                    dataKey="spend"
                    name="Chi tiêu ADS"
                    fill="#f97316"
                    radius={[4, 4, 0, 0]}
                    barSize={14}
                  />
                )}
                <Bar
                  dataKey="netCommission"
                  name="Hoa hồng ròng"
                  fill="#3b82f6"
                  radius={[4, 4, 0, 0]}
                  barSize={14}
                />
                <Line
                  type="monotone"
                  dataKey="profit"
                  name="Lợi nhuận"
                  stroke="#22c55e"
                  strokeWidth={2.5}
                  dot={{ r: 3, fill: "#22c55e" }}
                  activeDot={{ r: 5 }}
                />
                <ReferenceLine
                  y={0}
                  stroke="#ffffff30"
                  strokeDasharray="3 3"
                />
              </ComposedChart>
            ) : (
              <ComposedChart
                data={aggregated}
                margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
              >
                <CartesianGrid stroke="#ffffff10" vertical={false} />
                <XAxis
                  dataKey="dateShort"
                  tick={{ fill: "#ffffff70", fontSize: 11 }}
                  axisLine={{ stroke: "#ffffff20" }}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fill: "#ffffff70", fontSize: 11 }}
                  axisLine={{ stroke: "#ffffff20" }}
                  tickLine={false}
                  tickFormatter={(v) => `${v}%`}
                  width={55}
                />
                <Tooltip
                  content={(props: unknown) => (
                    <TrendTooltip
                      {...(props as TrendTooltipProps)}
                      mode="roi"
                    />
                  )}
                  cursor={{ stroke: "#ffffff20" }}
                />
                <ReferenceLine
                  y={0}
                  stroke="#ffffff50"
                  strokeDasharray="4 4"
                  label={{
                    value: "Hòa vốn",
                    fill: "#ffffff60",
                    fontSize: 10,
                    position: "insideTopRight",
                  }}
                />
                <Line
                  type="monotone"
                  dataKey="roi"
                  name="ROI %"
                  stroke="#22c55e"
                  strokeWidth={2.5}
                  dot={(props: {
                    cx?: number;
                    cy?: number;
                    payload?: DailyTrendPoint;
                    index?: number;
                  }) => {
                    const { cx, cy, payload, index } = props;
                    if (cx === undefined || cy === undefined || !payload) {
                      return <g key={`dot-${index}`} />;
                    }
                    const color =
                      payload.roi === null
                        ? "#ffffff40"
                        : payload.roi >= 0
                        ? "#22c55e"
                        : "#ef4444";
                    return (
                      <circle
                        key={`dot-${index ?? payload.date}`}
                        cx={cx}
                        cy={cy}
                        r={4}
                        fill={color}
                        stroke="#0a0a0a"
                        strokeWidth={1.5}
                      />
                    );
                  }}
                  connectNulls={false}
                />
              </ComposedChart>
            )}
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}

function ModeButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`btn-ripple rounded-full px-3 py-1 text-xs font-medium transition-colors ${
        active
          ? "bg-shopee-500 text-white shadow-elev-1"
          : "text-white/60 hover:text-white/90"
      }`}
    >
      {children}
    </button>
  );
}

/// Recharts Tooltip `content` callback type — loose signature vì typing chính
/// thức của lib đang conflict với generic. Chỉ cần shape tối thiểu này.
interface TrendTooltipProps {
  active?: boolean;
  payload?: Array<{ payload: DailyTrendPoint & Partial<CumulativePoint> }>;
  label?: string;
}

function CumulativeTooltip({ active, payload }: TrendTooltipProps) {
  if (!active || !payload || payload.length === 0) return null;
  const point = payload[0].payload as CumulativePoint & { dayCount?: number };
  const isAggregated = (point.dayCount ?? 1) > 1;
  return (
    <div className="rounded-lg border border-surface-8 bg-surface-0/95 px-3 py-2 shadow-elev-8 backdrop-blur">
      <div className="mb-1 text-xs font-semibold text-white/90">
        {fmtDate(point.date)}
        {isAggregated && (
          <span className="ml-1.5 rounded bg-white/10 px-1.5 py-0.5 text-[10px] font-normal text-white/70">
            {point.dayCount} ngày
          </span>
        )}
      </div>
      <dl className="space-y-0.5 text-xs">
        <TooltipRow
          color="#22c55e"
          label="Lãi tích luỹ"
          value={fmtVnd(point.cumulativeProfit)}
          valueClass={
            point.cumulativeProfit >= 0 ? "text-green-300" : "text-red-300"
          }
        />
        <TooltipRow
          color="#f97316"
          label="Chi tiêu tích luỹ"
          value={fmtVnd(point.cumulativeSpend)}
        />
        <TooltipRow
          color="#3b82f6"
          label="Hoa hồng tích luỹ"
          value={fmtVnd(point.cumulativeRevenue)}
        />
      </dl>
    </div>
  );
}

/// Custom tooltip: header = ngày đầy đủ (DD/MM/YYYY), body = từng metric.
/// dayCount > 1 → bucket tuần/tháng → hiển thị range "X ngày" để user biết
/// gom bao nhiêu ngày data thực sự (vd Feb 28 ngày, hoặc filter cắt giữa tháng).
function TrendTooltip({
  active,
  payload,
  label,
  mode,
}: TrendTooltipProps & { mode: ChartMode }) {
  if (!active || !payload || payload.length === 0) return null;
  const point = payload[0].payload;
  const isAggregated = (point.dayCount ?? 1) > 1;
  return (
    <div className="rounded-lg border border-surface-8 bg-surface-0/95 px-3 py-2 shadow-elev-8 backdrop-blur">
      <div className="mb-1 text-xs font-semibold text-white/90">
        {fmtDate(point.date)}
        {isAggregated && (
          <span className="ml-1.5 rounded bg-white/10 px-1.5 py-0.5 text-[10px] font-normal text-white/70">
            {point.dayCount} ngày
          </span>
        )}
        {label ? <span className="ml-1 text-white/40">({label})</span> : null}
      </div>
      {mode === "finance" ? (
        <dl className="space-y-0.5 text-xs">
          {point.spend > 0 && (
            <TooltipRow color="#f97316" label="Chi tiêu ADS" value={fmtVnd(point.spend)} />
          )}
          <TooltipRow
            color="#3b82f6"
            label="Hoa hồng ròng"
            value={fmtVnd(point.netCommission)}
          />
          <TooltipRow
            color="#22c55e"
            label="Lợi nhuận"
            value={fmtVnd(point.profit)}
            valueClass={
              point.profit > 0
                ? "text-green-300"
                : point.profit < 0
                ? "text-red-300"
                : "text-white/70"
            }
          />
          <TooltipRow
            color="#ffffff30"
            label="Đơn / Click Shopee"
            value={`${point.orders} / ${point.shopeeClicks}`}
            valueClass="text-white/60"
          />
        </dl>
      ) : (
        <dl className="space-y-0.5 text-xs">
          <TooltipRow
            color={
              point.roi === null
                ? "#ffffff40"
                : point.roi >= 0
                ? "#22c55e"
                : "#ef4444"
            }
            label="ROI"
            value={point.roi === null ? "—" : fmtPct(point.roi)}
            valueClass={
              point.roi === null
                ? "text-white/40"
                : point.roi >= 0
                ? "text-green-300"
                : "text-red-300"
            }
          />
          <TooltipRow color="#f97316" label="Chi tiêu" value={fmtVnd(point.spend)} />
          <TooltipRow
            color="#22c55e"
            label="Lợi nhuận"
            value={fmtVnd(point.profit)}
          />
        </dl>
      )}
    </div>
  );
}

function TooltipRow({
  color,
  label,
  value,
  valueClass,
}: {
  color: string;
  label: string;
  value: string;
  valueClass?: string;
}) {
  return (
    <div className="flex items-center gap-2">
      <span
        className="inline-block h-2 w-2 shrink-0 rounded-full"
        style={{ background: color }}
      />
      <span className="text-white/60">{label}</span>
      <span className={`ml-auto tabular-nums font-medium ${valueClass ?? "text-white/90"}`}>
        {value}
      </span>
    </div>
  );
}
