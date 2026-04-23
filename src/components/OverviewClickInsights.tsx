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
import { fmtInt, fmtPct, fmtVnd } from "../formulas";

/// Shape BE `load_referrer_efficiency` trả về.
export interface ReferrerEfficiency {
  referrer: string;
  clicks: number;
  orders: number;
  commission: number;
  cr: number | null;
}

/// Bảng referrer leaderboard sắp theo CR (quality signal cho ads).
/// Khác "top referrers" chỉ count clicks — cái này show orders + commission
/// + CR để xác định referrer nào bring revenue thực sự.
export function ReferrerEfficiencyTable({
  rows,
}: {
  rows: ReferrerEfficiency[];
}) {
  if (rows.length === 0) {
    return (
      <section className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-2">
        <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
          Hiệu quả nguồn traffic
        </h3>
        <p className="mt-3 text-sm text-white/50">
          Chưa có dữ liệu click + đơn để phân tích
        </p>
      </section>
    );
  }

  return (
    <section className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-2">
      <header className="mb-3 flex items-center gap-2">
        <span className="material-symbols-rounded text-shopee-400">hub</span>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
          Hiệu quả nguồn traffic
        </h3>
        <span
          className="material-symbols-rounded cursor-help text-sm text-white/40"
          title={
            "Sắp theo CR (Conversion Rate). Orders được phân bổ theo tỉ lệ " +
            "click giữa các referrer cùng sub_ids trong cùng ngày (approximate " +
            "vì Shopee không gắn referrer cho order)."
          }
        >
          help
        </span>
      </header>
      <div className="overflow-hidden rounded-lg border border-surface-8">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-surface-4 text-[11px] font-semibold uppercase tracking-wide text-shopee-200">
              <th className="px-3 py-2 text-left">Nguồn</th>
              <th className="px-3 py-2 text-right">Click</th>
              <th className="px-3 py-2 text-right">Đơn</th>
              <th className="px-3 py-2 text-right">CR</th>
              <th className="px-3 py-2 text-right">Hoa hồng</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const crClass =
                r.cr === null
                  ? "text-white/30"
                  : r.cr >= 2
                  ? "text-green-400"
                  : r.cr >= 1
                  ? "text-amber-300"
                  : "text-red-400";
              return (
                <tr
                  key={r.referrer || "(unknown)"}
                  className="border-t border-surface-8 hover:bg-surface-1/50"
                >
                  <td
                    className="px-3 py-2 text-white/90 max-w-[200px] truncate"
                    title={r.referrer || "(không xác định)"}
                  >
                    {r.referrer || "(không xác định)"}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-white/85">
                    {fmtInt(r.clicks)}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-white/85">
                    {fmtInt(Math.round(r.orders))}
                  </td>
                  <td
                    className={`px-3 py-2 text-right tabular-nums font-semibold ${crClass}`}
                  >
                    {r.cr !== null ? fmtPct(r.cr) : "—"}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-white/85">
                    {fmtVnd(r.commission)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}

/// 1 bucket trong histogram click-to-order delay. Thứ tự fixed theo BE trả về:
/// <1h, 1-6h, 6-24h, 1-3d, >3d, no_click.
export interface DelayBucket {
  bucket: string;
  orders: number;
}

const BUCKET_LABELS: Record<string, string> = {
  "<1h": "< 1 giờ",
  "1-6h": "1 – 6 giờ",
  "6-24h": "6 – 24 giờ",
  "1-3d": "1 – 3 ngày",
  ">3d": "> 3 ngày",
  no_click: "Không có click",
};

/** Bucket nào là "impulse buy" (tô xanh) vs "consider" vs "no_click". */
const BUCKET_COLOR: Record<string, string> = {
  "<1h": "#22c55e",
  "1-6h": "#84cc16",
  "6-24h": "#eab308",
  "1-3d": "#f97316",
  ">3d": "#ef4444",
  no_click: "#64748b",
};

/// Histogram thời gian từ click → đặt hàng. Biểu đồ phân bố để hiểu user
/// behavior: impulse (<1h) vs consider (>24h) → tune retargeting window.
export function ClickDelayChart({ data }: { data: DelayBucket[] }) {
  const total = data.reduce((s, b) => s + b.orders, 0);
  if (total === 0) {
    return (
      <section className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-2">
        <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
          Thời gian click → đặt hàng
        </h3>
        <p className="mt-3 text-sm text-white/50">Chưa có đơn để phân tích</p>
      </section>
    );
  }

  const chartData = data.map((b) => ({
    label: BUCKET_LABELS[b.bucket] ?? b.bucket,
    key: b.bucket,
    orders: b.orders,
    pct: (b.orders / total) * 100,
  }));

  return (
    <section className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-2">
      <header className="mb-3 flex items-center gap-2">
        <span className="material-symbols-rounded text-shopee-400">timer</span>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
          Thời gian click → đặt hàng
        </h3>
        <span
          className="material-symbols-rounded cursor-help text-sm text-white/40"
          title={
            "Histogram phân bố delay từ lúc user click Shopee → đặt hàng.\n" +
            "• < 1h: impulse buy — ads hiệu quả ngay\n" +
            "• 1-24h: consider — cần retargeting cùng ngày\n" +
            "• > 1 ngày: long consider — retargeting 3-7 ngày\n" +
            "• Không click: đơn không có click_time (legacy hoặc re-order)"
          }
        >
          help
        </span>
        <span className="ml-auto text-xs text-white/50">
          {fmtInt(total)} đơn
        </span>
      </header>
      <div className="h-[220px] w-full">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
          >
            <CartesianGrid stroke="#ffffff10" vertical={false} />
            <XAxis
              dataKey="label"
              tick={{ fill: "#ffffff70", fontSize: 10 }}
              axisLine={{ stroke: "#ffffff20" }}
              tickLine={false}
            />
            <YAxis
              tick={{ fill: "#ffffff70", fontSize: 11 }}
              axisLine={{ stroke: "#ffffff20" }}
              tickLine={false}
              tickFormatter={fmtInt}
              width={45}
            />
            <Tooltip
              content={(props: unknown) => (
                <DelayTooltip {...(props as DelayTooltipProps)} />
              )}
              cursor={{ fill: "#ffffff08" }}
            />
            <Bar dataKey="orders" radius={[4, 4, 0, 0]} maxBarSize={60}>
              {chartData.map((d, i) => (
                <Cell key={`cell-${i}`} fill={BUCKET_COLOR[d.key] ?? "#64748b"} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}

interface DelayTooltipProps {
  active?: boolean;
  payload?: Array<{
    payload: { label: string; orders: number; pct: number };
  }>;
}

function DelayTooltip({ active, payload }: DelayTooltipProps) {
  if (!active || !payload || payload.length === 0) return null;
  const p = payload[0].payload;
  return (
    <div className="rounded-lg border border-surface-8 bg-surface-0/95 px-3 py-2 shadow-elev-8 backdrop-blur">
      <div className="text-xs font-semibold text-white/90">{p.label}</div>
      <div className="mt-1 text-xs text-white/70">
        <b className="tabular-nums text-white/95">{fmtInt(p.orders)}</b> đơn ·{" "}
        <b className="text-shopee-300">{fmtPct(p.pct)}</b>
      </div>
    </div>
  );
}
