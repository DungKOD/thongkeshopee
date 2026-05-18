import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fmtInt } from "../formulas";
import type { AggregatedProductRow } from "../formulas";

/// DTO BE: bucket per (sub_id tuple, day). FE aggregate qua sub_id để tính
/// % hủy + sparkline. Sort DESC theo % hủy, top N.
export type CancellationByDayBucket = {
  dayDate: string;
  subId1: string;
  subId2: string;
  subId3: string;
  subId4: string;
  subId5: string;
  totalOrders: number;
  cancelledOrders: number;
  zeroHhOrders: number;
};

/// Aggregated per-subid summary cho chart row.
type SubIdSummary = {
  subIds: [string, string, string, string, string];
  /// Key duy nhất từ sub_id tuple (join \x1f) — dùng làm map key + React key.
  key: string;
  /// Display name = sub_id đầu tiên non-empty, fallback "(chưa đặt)".
  displayName: string;
  totalOrders: number;
  cancelledOrders: number;
  zeroHhOrders: number;
  /// Cancel rate % = cancelledOrders / totalOrders × 100.
  cancelRate: number;
  /// Bad rate % = (cancelled + zeroHH non-cancelled) / total × 100.
  /// zeroHH includes cancelled, nên trừ ra để không double-count.
  badRate: number;
  /// Daily breakdown — sort theo dayDate ASC. Dùng cho tooltip sparkline.
  daily: { dayDate: string; total: number; cancelled: number; zeroHh: number }[];
};

interface Props {
  data: CancellationByDayBucket[];
  /// Min total orders để 1 sub_id xuất hiện trong chart. Default 5 — tránh
  /// noise từ SP chỉ có 1 đơn hủy (100% nhưng vô nghĩa).
  minOrdersDefault?: number;
  /// Top N — default 15. User có thể đổi qua dropdown.
  topNDefault?: number;
  /// Click vào bar → mở dialog detail (tuỳ chọn, parent quyết định).
  onSelectSubId?: (subIds: AggregatedProductRow["subIds"]) => void;
}

const COLOR_CANCELLED = "#ef4444";
const COLOR_DANGER_HIGH = "#dc2626";
const COLOR_WARNING = "#f59e0b";

export function CancellationRateChart({
  data,
  minOrdersDefault = 5,
  topNDefault = 15,
  onSelectSubId,
}: Props) {
  const [minOrders, setMinOrders] = useState(minOrdersDefault);
  const [topN, setTopN] = useState(topNDefault);

  // Aggregate per sub_id.
  const summaries = useMemo<SubIdSummary[]>(() => {
    const map = new Map<string, SubIdSummary>();
    for (const b of data) {
      const tuple: [string, string, string, string, string] = [
        b.subId1,
        b.subId2,
        b.subId3,
        b.subId4,
        b.subId5,
      ];
      const key = tuple.join("\x1f");
      let s = map.get(key);
      if (!s) {
        const displayName = tuple.find((x) => x.trim() !== "") ?? "(chưa đặt)";
        s = {
          subIds: tuple,
          key,
          displayName,
          totalOrders: 0,
          cancelledOrders: 0,
          zeroHhOrders: 0,
          cancelRate: 0,
          badRate: 0,
          daily: [],
        };
        map.set(key, s);
      }
      s.totalOrders += b.totalOrders;
      s.cancelledOrders += b.cancelledOrders;
      s.zeroHhOrders += b.zeroHhOrders;
      s.daily.push({
        dayDate: b.dayDate,
        total: b.totalOrders,
        cancelled: b.cancelledOrders,
        zeroHh: b.zeroHhOrders,
      });
    }
    const out: SubIdSummary[] = [];
    for (const s of map.values()) {
      if (s.totalOrders > 0) {
        s.cancelRate = (s.cancelledOrders / s.totalOrders) * 100;
        s.badRate = (s.zeroHhOrders / s.totalOrders) * 100;
      }
      s.daily.sort((a, b) => a.dayDate.localeCompare(b.dayDate));
      out.push(s);
    }
    return out;
  }, [data]);

  // Filter + sort DESC theo cancelRate, top N.
  const ranked = useMemo(() => {
    return summaries
      .filter((s) => s.totalOrders >= minOrders)
      .sort((a, b) => b.cancelRate - a.cancelRate)
      .slice(0, topN);
  }, [summaries, minOrders, topN]);

  // Overall TB cancel rate cho reference (chỉ tính từ subset >= minOrders để
  // khớp với scope chart, tránh weighted bias từ SP đơn-lẻ).
  const avgCancelRate = useMemo(() => {
    const filtered = summaries.filter((s) => s.totalOrders >= minOrders);
    if (filtered.length === 0) return 0;
    let totalOrders = 0;
    let totalCancelled = 0;
    for (const s of filtered) {
      totalOrders += s.totalOrders;
      totalCancelled += s.cancelledOrders;
    }
    return totalOrders > 0 ? (totalCancelled / totalOrders) * 100 : 0;
  }, [summaries, minOrders]);

  const filteredCount = useMemo(
    () => summaries.filter((s) => s.totalOrders >= minOrders).length,
    [summaries, minOrders],
  );

  if (data.length === 0) return null;

  // Chart data — cao bar phụ thuộc số row (mỗi row ~32px).
  const chartHeight = Math.max(220, ranked.length * 34 + 50);

  return (
    <section className="rounded-xl bg-surface-2 px-4 py-4 shadow-elev-1">
      {/* ============ Header + controls ============ */}
      <div className="mb-3 flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-2">
          <span className="material-symbols-rounded text-base text-red-400">
            cancel
          </span>
          <h3 className="text-xs font-semibold uppercase tracking-[0.1em] text-white/70">
            Tỉ lệ hoàn hủy theo sản phẩm
          </h3>
          <span className="text-[11px] text-white/45">
            ({ranked.length}/{filteredCount} hiển thị)
          </span>
        </div>
        <span className="text-[11px] text-white/55">
          TB chung:{" "}
          <span className="font-semibold tabular-nums text-white/85">
            {avgCancelRate.toFixed(1)}%
          </span>
        </span>
        <div className="ml-auto flex items-center gap-3 text-[11px]">
          <label className="flex items-center gap-1.5 text-white/60">
            Min đơn:
            <select
              value={minOrders}
              onChange={(e) => setMinOrders(Number(e.target.value))}
              className="rounded border border-surface-8 bg-surface-4 px-1.5 py-0.5 text-white/85 outline-none focus:border-shopee-500"
            >
              {[1, 3, 5, 10, 20, 50].map((n) => (
                <option key={n} value={n}>
                  ≥ {n}
                </option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-1.5 text-white/60">
            Top:
            <select
              value={topN}
              onChange={(e) => setTopN(Number(e.target.value))}
              className="rounded border border-surface-8 bg-surface-4 px-1.5 py-0.5 text-white/85 outline-none focus:border-shopee-500"
            >
              {[10, 15, 20, 30, 50].map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>
        </div>
      </div>

      {ranked.length === 0 ? (
        <div className="rounded-lg border border-dashed border-surface-8 bg-surface-1 px-4 py-6 text-center text-sm text-white/55">
          Không có sản phẩm nào có ≥ {minOrders} đơn trong khoảng đã chọn.
        </div>
      ) : (
        <div style={{ height: chartHeight }} className="w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={ranked}
              layout="vertical"
              margin={{ top: 8, right: 50, left: 8, bottom: 8 }}
            >
              <CartesianGrid stroke="#ffffff10" horizontal={false} />
              <XAxis
                type="number"
                domain={[0, (max: number) => Math.min(100, Math.ceil(max + 5))]}
                tick={{ fill: "#ffffff70", fontSize: 11 }}
                axisLine={{ stroke: "#ffffff20" }}
                tickLine={false}
                tickFormatter={(v) => `${v}%`}
              />
              <YAxis
                type="category"
                dataKey="displayName"
                tick={{ fill: "#ffffff85", fontSize: 11 }}
                axisLine={{ stroke: "#ffffff20" }}
                tickLine={false}
                width={140}
                interval={0}
              />
              <Tooltip cursor={{ fill: "#ffffff08" }} content={<RowTooltip />} />
              <Bar
                dataKey="cancelRate"
                radius={[0, 4, 4, 0]}
                onClick={(d) => {
                  if (!onSelectSubId) return;
                  const subIds = (d as unknown as { payload: SubIdSummary })
                    ?.payload?.subIds;
                  if (subIds) onSelectSubId(subIds);
                }}
              >
                {ranked.map((entry) => (
                  <Cell
                    key={entry.key}
                    fill={
                      entry.cancelRate >= 20
                        ? COLOR_DANGER_HIGH
                        : entry.cancelRate >= 10
                        ? COLOR_CANCELLED
                        : COLOR_WARNING
                    }
                    cursor={onSelectSubId ? "pointer" : "default"}
                  />
                ))}
                <LabelList
                  dataKey="cancelRate"
                  position="right"
                  formatter={(v: unknown) => {
                    const n = Number(v);
                    return n > 0 ? `${n.toFixed(1)}%` : "";
                  }}
                  style={{ fill: "#ffffffb0", fontSize: 11, fontWeight: 600 }}
                />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      <p className="mt-2 text-[10px] text-white/40">
        Bar màu = mức độ: <span className="text-amber-400">vàng &lt; 10%</span> ·{" "}
        <span className="text-red-400">đỏ 10–20%</span> ·{" "}
        <span className="text-red-600">đậm ≥ 20%</span>. Hover bar để xem chi tiết
        theo ngày.
        {onSelectSubId && " Click bar để mở chi tiết sản phẩm."}
      </p>
    </section>
  );
}

// =========================================================
// Tooltip — hiển thị total/cancelled/zeroHH + per-day breakdown
// =========================================================

function RowTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: Array<{ payload?: SubIdSummary }>;
}) {
  if (!active || !payload || payload.length === 0) return null;
  const s = payload[0]?.payload;
  if (!s) return null;

  return (
    <div className="max-w-[320px] rounded-lg border border-white/20 bg-[#1f1f23] px-3 py-2 text-xs shadow-lg">
      <div
        className="mb-1.5 truncate font-semibold text-white/95"
        title={s.displayName}
      >
        {s.displayName}
      </div>
      <div className="space-y-0.5">
        <Row label="Tổng đơn" value={fmtInt(s.totalOrders)} />
        <Row
          label="Đã hủy"
          value={`${fmtInt(s.cancelledOrders)} (${s.cancelRate.toFixed(1)}%)`}
          tone="danger"
        />
        <Row
          label="HH = 0đ"
          value={`${fmtInt(s.zeroHhOrders)} (${s.badRate.toFixed(1)}%)`}
          tone="warning"
        />
      </div>
      {s.daily.length > 0 && (
        <>
          <div className="mt-2 mb-1 text-[10px] uppercase tracking-wider text-white/45">
            Theo ngày ({s.daily.length} ngày)
          </div>
          <div className="max-h-[160px] space-y-0.5 overflow-y-auto pr-1">
            {s.daily.map((d) => {
              const rate = d.total > 0 ? (d.cancelled / d.total) * 100 : 0;
              const parts = d.dayDate.split("-");
              const short =
                parts.length === 3 ? `${parts[2]}/${parts[1]}` : d.dayDate;
              return (
                <div
                  key={d.dayDate}
                  className="flex items-center gap-2 text-[11px]"
                >
                  <span className="w-10 tabular-nums text-white/55">
                    {short}
                  </span>
                  <span className="flex-1 tabular-nums text-white/70">
                    {fmtInt(d.total)} đơn · {fmtInt(d.cancelled)} hủy
                  </span>
                  <span
                    className={`w-12 text-right tabular-nums ${
                      rate >= 20
                        ? "text-red-400"
                        : rate >= 10
                        ? "text-red-300"
                        : rate > 0
                        ? "text-amber-300"
                        : "text-white/40"
                    }`}
                  >
                    {rate.toFixed(1)}%
                  </span>
                </div>
              );
            })}
          </div>
        </>
      )}
    </div>
  );
}

function Row({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: "danger" | "warning";
}) {
  const cls =
    tone === "danger"
      ? "text-red-300"
      : tone === "warning"
      ? "text-amber-300"
      : "text-white/95";
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-white/60">{label}</span>
      <span className={`font-semibold tabular-nums ${cls}`}>{value}</span>
    </div>
  );
}
