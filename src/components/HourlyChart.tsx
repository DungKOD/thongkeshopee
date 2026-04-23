import { useMemo } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fmtInt, fmtMoneyShort, fmtVnd } from "../formulas";

/// 1 bucket = 1 giờ trong ngày (0-23). Shape khớp BE `HourlyOrderBucket` +
/// extend thêm `clicks` (cho hourly click chart). Bucket cho order-based data
/// set `clicks=0`, click-based data set `orders/orderValue/commission=0`.
export type HourlyBucket = {
  hour: number;
  orders: number;
  orderValue: number;
  commission: number;
  clicks: number;
};

export type HourlyMetric = "orders" | "orderValue" | "commission" | "clicks";

interface Props {
  data: HourlyBucket[];
  /// Title section hiển thị trên chart.
  title?: string;
  /// Metric nào là tâm điểm — orders là default.
  metric?: HourlyMetric;
  /// Icon material symbol.
  icon?: string;
  /// Loading state (optional).
  loading?: boolean;
}

/**
 * Bar chart 24 cột, x-axis = giờ (0-23), y-axis = metric chọn (orders/GMV/HH).
 * Highlight peak hours (top 3 bucket) bằng màu shopee; còn lại màu xám.
 * Giúp user nhanh thấy khung giờ user mua nhiều nhất → tối ưu đăng bài.
 */
export function HourlyChart({
  data,
  title = "Giờ mua hàng",
  metric = "orders",
  icon = "schedule",
  loading = false,
}: Props) {
  const { peakHours, totalMetric, peakHour } = useMemo(() => {
    const getVal = (b: HourlyBucket) => {
      switch (metric) {
        case "orders":
          return b.orders;
        case "orderValue":
          return b.orderValue;
        case "commission":
          return b.commission;
        case "clicks":
          return b.clicks;
      }
    };

    const total = data.reduce((sum, b) => sum + getVal(b), 0);

    // Top 3 giờ có value cao nhất (chỉ tính > 0 để skip ngày rỗng).
    const indexed = data
      .map((b, i) => ({ idx: i, val: getVal(b) }))
      .filter((x) => x.val > 0)
      .sort((a, b) => b.val - a.val);
    const peak = indexed.length > 0 ? indexed[0].idx : null;
    const topSet = new Set(indexed.slice(0, 3).map((x) => x.idx));
    return { peakHours: topSet, totalMetric: total, peakHour: peak };
  }, [data, metric]);

  const metricLabel: string = {
    orders: "đơn",
    orderValue: "GMV",
    commission: "hoa hồng",
    clicks: "click",
  }[metric];

  const chartData = useMemo(
    () =>
      data.map((b, i) => ({
        hourLabel: `${b.hour}h`,
        hour: b.hour,
        isPeak: peakHours.has(i),
        orders: b.orders,
        orderValue: b.orderValue,
        commission: b.commission,
        clicks: b.clicks,
      })),
    [data, peakHours],
  );

  const formatY = (v: number) =>
    metric === "orders" || metric === "clicks" ? fmtInt(v) : fmtMoneyShort(v);

  return (
    <section className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-2">
      <header className="mb-3 flex items-center gap-2">
        <span className="material-symbols-rounded text-shopee-400">{icon}</span>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
          {title}
        </h3>
        {peakHour !== null && totalMetric > 0 && (
          <span className="ml-auto rounded-full bg-shopee-900/40 px-2.5 py-0.5 text-xs text-shopee-300">
            Peak: <b>{peakHour}h–{peakHour + 1}h</b>
          </span>
        )}
      </header>

      {loading ? (
        <div className="flex h-[220px] items-center justify-center text-sm text-white/40">
          <span className="material-symbols-rounded mr-2 animate-spin">
            sync
          </span>
          Đang tải...
        </div>
      ) : totalMetric === 0 ? (
        <div className="flex h-[220px] items-center justify-center text-sm text-white/40">
          Chưa có dữ liệu đơn hàng để phân tích
        </div>
      ) : (
        <div className="h-[220px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={chartData}
              margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
            >
              <CartesianGrid stroke="#ffffff10" vertical={false} />
              <XAxis
                dataKey="hourLabel"
                tick={{ fill: "#ffffff70", fontSize: 10 }}
                axisLine={{ stroke: "#ffffff20" }}
                tickLine={false}
                interval={1}
              />
              <YAxis
                tick={{ fill: "#ffffff70", fontSize: 11 }}
                axisLine={{ stroke: "#ffffff20" }}
                tickLine={false}
                tickFormatter={formatY}
                width={45}
              />
              <Tooltip
                content={(props: unknown) => (
                  <HourlyTooltip
                    {...(props as HourlyTooltipProps)}
                    metric={metric}
                    metricLabel={metricLabel}
                  />
                )}
                cursor={{ fill: "#ffffff08" }}
              />
              <Bar
                dataKey={metric}
                radius={[4, 4, 0, 0]}
                maxBarSize={26}
              >
                {chartData.map((d, i) => (
                  <Cell
                    key={`cell-${i}`}
                    fill={d.isPeak ? "#ee4d2d" : "#475569"}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}

interface HourlyTooltipProps {
  active?: boolean;
  payload?: Array<{
    payload: {
      hour: number;
      orders: number;
      orderValue: number;
      commission: number;
      clicks: number;
    };
  }>;
}

function HourlyTooltip({
  active,
  payload,
  metric,
  metricLabel,
}: HourlyTooltipProps & { metric: HourlyMetric; metricLabel: string }) {
  if (!active || !payload || payload.length === 0) return null;
  const p = payload[0].payload;
  // Click-based chart: hiển thị clicks only (không có order context cùng giờ).
  // Order-based chart: hiển thị cả 3 metric vì user có thể muốn thấy GMV/commission
  // của giờ đó bên cạnh số đơn.
  const clickOnly = metric === "clicks";
  return (
    <div className="rounded-lg border border-surface-8 bg-surface-0/95 px-3 py-2 shadow-elev-8 backdrop-blur">
      <div className="mb-1 text-xs font-semibold text-white/90">
        {p.hour}h – {p.hour + 1}h
      </div>
      <dl className="space-y-0.5 text-xs">
        {clickOnly ? (
          <Row label="Click Shopee" value={fmtInt(p.clicks)} />
        ) : (
          <>
            <Row label="Số đơn" value={fmtInt(p.orders)} />
            <Row label="GMV" value={fmtVnd(p.orderValue)} />
            <Row label="Hoa hồng" value={fmtVnd(p.commission)} />
          </>
        )}
      </dl>
      <p className="mt-1 text-[10px] text-white/40">
        Trục Y: {metricLabel}
      </p>
    </div>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center gap-3">
      <span className="text-white/55">{label}</span>
      <span className="ml-auto tabular-nums font-medium text-white/90">
        {value}
      </span>
    </div>
  );
}
