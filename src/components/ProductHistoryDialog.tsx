import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { UiDay, UiRow } from "../types";
import {
  computeUiRow,
  fmtDate,
  fmtInt,
  fmtPct,
  fmtVnd,
} from "../formulas";
import { sumFiltered, useSettings } from "../hooks/useSettings";
import { invoke } from "../lib/tauri";
import type { AccountFilterMode } from "../hooks/useDbStats";

// Ngưỡng ROI: ≥ 50% = an toàn (xanh), 0–50% = cảnh báo lỗ (vàng),
// < 0% = lỗ thực sự — cảnh báo cao nhất (đỏ).
const ROI_SAFE_THRESHOLD = 50;
const ROI_SAFE_COLOR = "#22c55e";
const ROI_WARN_COLOR = "#f59e0b";
const ROI_DANGER_COLOR = "#ef4444";

function roiColor(roi: number): string {
  if (roi < 0) return ROI_DANGER_COLOR;
  if (roi < ROI_SAFE_THRESHOLD) return ROI_WARN_COLOR;
  return ROI_SAFE_COLOR;
}

interface ProductHistoryDialogProps {
  isOpen: boolean;
  row: UiRow | null;
  accountFilter?: AccountFilterMode;
  onClose: () => void;
}

export function ProductHistoryDialog({
  isOpen,
  row,
  accountFilter,
  onClose,
}: ProductHistoryDialogProps) {
  const { settings } = useSettings();
  const [allDays, setAllDays] = useState<UiDay[]>([]);
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!isOpen || !row) {
      setAllDays([]);
      return;
    }
    let cancelled = false;
    setLoading(true);
    invoke<UiDay[]>("list_days_with_rows", {
      filter: {
        subIdFilter: row.displayName || undefined,
        accountFilter,
      },
    })
      .then((data) => {
        if (!cancelled) setAllDays(data ?? []);
      })
      .catch(() => {
        if (!cancelled) setAllDays([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [isOpen, row, accountFilter]);

  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  const subIdsKey = row ? row.subIds.join("\x1f") : "";

  // Lọc exact subIds match, 1 entry per (day, accountId).
  const historyRows = useMemo(() => {
    if (!row) return [];
    const result: UiRow[] = [];
    for (const day of allDays) {
      for (const r of day.rows) {
        if (r.subIds.join("\x1f") === subIdsKey) {
          result.push(r);
        }
      }
    }
    return result.sort((a, b) => b.dayDate.localeCompare(a.dayDate));
  }, [allDays, subIdsKey, row]);

  const showAccount =
    !accountFilter || accountFilter.kind === "all";

  // Trend ROI theo ngày: group multiple TK Shopee cùng ngày → aggregate
  // spend/profit, ROI = profit / spend × 100% (null khi không có spend).
  const trendData = useMemo(() => {
    const map = new Map<
      string,
      { date: string; spend: number; profit: number; commission: number }
    >();
    for (const r of historyRows) {
      const shopee = sumFiltered(r.shopeeClicksByReferrer, settings.clickSources);
      const c = computeUiRow(r, settings.profitFees, shopee);
      const e = map.get(r.dayDate) ?? {
        date: r.dayDate,
        spend: 0,
        profit: 0,
        commission: 0,
      };
      e.spend += r.totalSpend ?? 0;
      e.profit += c.profit;
      e.commission += r.commissionTotal;
      map.set(r.dayDate, e);
    }
    return Array.from(map.values())
      .sort((a, b) => a.date.localeCompare(b.date))
      .map((e) => {
        const full = fmtDate(e.date);
        return {
          ...e,
          dateShort: full.length >= 5 ? full.slice(0, 5) : full,
          roi: e.spend > 0 ? (e.profit / e.spend) * 100 : null,
        };
      });
  }, [historyRows, settings.clickSources, settings.profitFees]);

  const totals = useMemo(() => {
    let spend = 0;
    let orders = 0;
    let commission = 0;
    let profit = 0;
    let profitable = 0;
    let lossy = 0;
    for (const r of historyRows) {
      const shopee = sumFiltered(r.shopeeClicksByReferrer, settings.clickSources);
      const c = computeUiRow(r, settings.profitFees, shopee);
      spend += r.totalSpend ?? 0;
      orders += r.ordersCount;
      commission += r.commissionTotal;
      profit += c.profit;
      if (c.profit > 0) profitable++;
      else if (c.profit < 0) lossy++;
    }
    const distinctDays = new Set(historyRows.map((r) => r.dayDate)).size;
    return { spend, orders, commission, profit, profitable, lossy, distinctDays };
  }, [historyRows, settings.clickSources, settings.profitFees]);

  if (!isOpen || !row) return null;

  const profitCls =
    totals.profit > 0
      ? "text-green-400"
      : totals.profit < 0
      ? "text-red-400"
      : "text-gray-300";

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        className="relative flex max-h-[calc(100vh-2rem)] w-full max-w-4xl flex-col overflow-hidden rounded-2xl bg-surface-2 shadow-elev-24"
        role="dialog"
        aria-modal="true"
        aria-labelledby="product-history-dialog-title"
      >
        {/* Header */}
        <header className="flex shrink-0 items-start gap-4 bg-gradient-to-r from-shopee-700/90 to-shopee-600/70 px-6 py-4">
          <span className="mt-1 flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-white/15 text-white">
            <span className="material-symbols-rounded text-xl">timeline</span>
          </span>
          <div className="min-w-0 flex-1">
            <p className="text-[11px] font-medium uppercase tracking-[0.12em] text-white/70">
              Lịch sử theo ngày
            </p>
            <div className="mt-0.5 flex min-w-0 items-center gap-1.5">
              <h2
                id="product-history-dialog-title"
                className="truncate text-xl font-bold text-white"
                title={row.displayName}
              >
                {row.displayName || "(chưa đặt tên)"}
              </h2>
              {row.displayName && (
                <button
                  type="button"
                  onClick={() => {
                    navigator.clipboard.writeText(row.displayName).then(() => {
                      setCopied(true);
                      setTimeout(() => setCopied(false), 1500);
                    });
                  }}
                  title={copied ? "Đã copy!" : "Copy tên sản phẩm"}
                  aria-label="Copy tên sản phẩm"
                  className={`btn-ripple flex-none flex h-7 w-7 items-center justify-center rounded-full transition-colors ${
                    copied
                      ? "text-green-300 bg-white/10"
                      : "text-white/70 hover:bg-white/15 hover:text-white"
                  }`}
                >
                  <span className="material-symbols-rounded text-base">
                    {copied ? "check" : "content_copy"}
                  </span>
                </button>
              )}
            </div>
            {!loading && historyRows.length > 0 && (
              <div className="mt-1.5 flex flex-wrap items-center gap-3 text-xs text-white/80">
                <span className="inline-flex items-center gap-1">
                  <span className="material-symbols-rounded text-sm">event</span>
                  {totals.distinctDays} ngày có data
                </span>
                <span className="text-white/40">·</span>
                <span className="inline-flex items-center gap-1 text-green-300">
                  <span className="material-symbols-rounded text-sm">trending_up</span>
                  {totals.profitable} ngày lãi
                </span>
                <span className="text-white/40">·</span>
                <span className="inline-flex items-center gap-1 text-red-300">
                  <span className="material-symbols-rounded text-sm">trending_down</span>
                  {totals.lossy} ngày lỗ
                </span>
              </div>
            )}
          </div>
          <button
            type="button"
            onClick={onClose}
            className="btn-ripple flex h-10 w-10 shrink-0 items-center justify-center rounded-full text-white/80 hover:bg-white/15"
            title="Đóng (Esc)"
            aria-label="Đóng"
          >
            <span className="material-symbols-rounded">close</span>
          </button>
        </header>

        {/* Body */}
        <div className="min-h-0 flex-1 overflow-y-auto bg-surface-0">
          {loading ? (
            <div className="flex items-center justify-center px-6 py-12 text-sm text-white/50">
              <span className="material-symbols-rounded mr-2 animate-spin text-base">sync</span>
              Đang tải lịch sử...
            </div>
          ) : historyRows.length === 0 ? (
            <div className="flex flex-col items-center gap-2 px-6 py-12 text-sm text-white/50">
              <span className="material-symbols-rounded text-3xl text-white/20">
                history
              </span>
              <span>Không tìm thấy lịch sử cho sản phẩm này.</span>
            </div>
          ) : (
            <>
              <RoiTrendSection data={trendData} />
              <div className="overflow-x-auto">
                <table className="w-full border-collapse text-sm">
                <thead>
                  <tr className="border-b-2 border-shopee-500/50 bg-gradient-to-b from-shopee-900/35 to-shopee-900/15 text-shopee-100">
                    <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                      Ngày
                    </th>
                    {showAccount && (
                      <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                        TK Shopee
                      </th>
                    )}
                    <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                      Tiền ads
                    </th>
                    <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                      Số đơn
                    </th>
                    <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                      Hoa hồng
                    </th>
                    <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                      Lợi nhuận
                    </th>
                    <th
                      className="cursor-help whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider"
                      title="ROI = (Hoa hồng sau phí − Tiền ads) / Tiền ads × 100%"
                    >
                      ROI <span className="text-shopee-300/60 text-[11px]">ⓘ</span>
                    </th>
                  </tr>
                </thead>
                <tbody>
                  <tr className="border-b-2 border-shopee-500 bg-shopee-900/25 text-base font-bold text-white">
                    <td className="sticky top-0 z-10 bg-shopee-900/40 px-3 py-4 text-center text-sm uppercase tracking-wider text-shopee-300">
                      Tổng
                    </td>
                    {showAccount && <td className="bg-shopee-900/25" />}
                    <td className="px-3 py-4 text-center tabular-nums text-blue-400">
                      {fmtVnd(totals.spend)}
                    </td>
                    <td className="px-3 py-4 text-center tabular-nums">
                      {fmtInt(totals.orders)}
                    </td>
                    <td className="px-3 py-4 text-center tabular-nums text-shopee-400">
                      {fmtVnd(totals.commission)}
                    </td>
                    <td className={`px-3 py-4 text-center tabular-nums ${profitCls}`}>
                      {fmtVnd(totals.profit)}
                    </td>
                    <td className={`px-3 py-4 text-center tabular-nums ${totals.spend > 0 ? profitCls : "text-white/30"}`}>
                      {totals.spend > 0
                        ? fmtPct((totals.profit / totals.spend) * 100)
                        : "—"}
                    </td>
                  </tr>
                  {historyRows.map((r) => (
                    <HistoryRow
                      key={`${r.dayDate}|${r.accountId ?? ""}`}
                      row={r}
                      showAccount={showAccount}
                    />
                  ))}
                </tbody>
              </table>
              </div>
            </>
          )}
        </div>
      </div>
    </div>,
    document.body,
  );
}

// =========================================================
// ROI trend chart
// =========================================================

interface TrendPoint {
  date: string;
  dateShort: string;
  spend: number;
  profit: number;
  commission: number;
  roi: number | null;
}

function RoiTrendSection({ data }: { data: TrendPoint[] }) {
  const validRoiPoints = data.filter((d) => d.roi !== null).length;
  const noAdsCount = data.filter((d) => d.spend === 0).length;
  const dangerCount = data.filter((d) => d.roi !== null && d.roi < 0).length;
  const warnCount = data.filter(
    (d) => d.roi !== null && d.roi >= 0 && d.roi < ROI_SAFE_THRESHOLD,
  ).length;

  // Tính min/max ROI để mở rộng danger zone xuống đáy chart.
  const { yMin, yMax } = useMemo(() => {
    const vals = data
      .map((d) => d.roi)
      .filter((v): v is number => v !== null);
    if (vals.length === 0) return { yMin: -10, yMax: 100 };
    const min = Math.min(...vals, 0);
    const max = Math.max(...vals, ROI_SAFE_THRESHOLD);
    const pad = Math.max(10, (max - min) * 0.1);
    return { yMin: min - pad, yMax: max + pad };
  }, [data]);

  // Group consecutive days với spend === 0 → vùng "đã tắt camp" để highlight band.
  const noAdsSegments = useMemo(() => {
    const segs: Array<{ x1: string; x2: string; days: number }> = [];
    let cur: { x1: string; x2: string; days: number } | null = null;
    for (const p of data) {
      if (p.spend === 0) {
        if (cur) {
          cur.x2 = p.dateShort;
          cur.days += 1;
        } else {
          cur = { x1: p.dateShort, x2: p.dateShort, days: 1 };
        }
      } else if (cur) {
        segs.push(cur);
        cur = null;
      }
    }
    if (cur) segs.push(cur);
    return segs;
  }, [data]);

  return (
    <section className="border-b border-surface-8 bg-surface-1 px-5 py-4">
      <header className="mb-2 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <span className="material-symbols-rounded text-shopee-400">monitoring</span>
          <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
            Xu hướng ROI theo ngày
          </h3>
          <span className="rounded-full bg-shopee-900/40 px-2 py-0.5 text-xs text-shopee-300">
            {data.length} ngày
          </span>
          {dangerCount > 0 && (
            <span
              className="inline-flex items-center gap-1 rounded-full bg-red-500/15 px-2 py-0.5 text-xs font-bold text-red-300"
              title={`ROI < 0% → đang lỗ (chạy ads tốn hơn hoa hồng thu về). ${dangerCount} ngày.`}
            >
              <span className="material-symbols-rounded text-[13px]">error</span>
              {dangerCount} ngày LỖ
            </span>
          )}
          {warnCount > 0 && (
            <span
              className="inline-flex items-center gap-1 rounded-full bg-amber-500/15 px-2 py-0.5 text-xs font-medium text-amber-300"
              title={`ROI 0–${ROI_SAFE_THRESHOLD}% → biên lãi mỏng, dễ tụt về lỗ khi chi phí biến động. ${warnCount} ngày.`}
            >
              <span className="material-symbols-rounded text-[13px]">warning</span>
              {warnCount} ngày cảnh báo
            </span>
          )}
          {noAdsCount > 0 && (
            <span
              className="inline-flex items-center gap-1 rounded-full bg-white/10 px-2 py-0.5 text-xs font-medium text-white/70"
              title="Ngày không có chi phí ads → camp đã tắt (hoặc chưa bật)"
            >
              <span className="material-symbols-rounded text-[13px]">power_off</span>
              {noAdsCount} ngày tắt camp
            </span>
          )}
        </div>
      </header>
      {validRoiPoints === 0 ? (
        <div className="flex h-[200px] items-center justify-center text-sm text-white/40">
          Không có ngày nào có chi phí ads → không tính được ROI
        </div>
      ) : (
        <div className="h-[220px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <CartesianGrid stroke="#ffffff10" vertical={false} />
              <XAxis
                dataKey="dateShort"
                tick={{ fill: "#ffffff70", fontSize: 11 }}
                axisLine={{ stroke: "#ffffff20" }}
                tickLine={false}
              />
              <YAxis
                domain={[yMin, yMax]}
                tick={{ fill: "#ffffff70", fontSize: 11 }}
                axisLine={{ stroke: "#ffffff20" }}
                tickLine={false}
                tickFormatter={(v) => `${v}%`}
                width={55}
              />
              <Tooltip
                content={(props: unknown) => (
                  <RoiTooltip {...(props as RoiTooltipProps)} />
                )}
                cursor={{ stroke: "#ffffff20" }}
              />
              {/* Danger zone: ROI < 0 → cảnh báo cao nhất (lỗ thực sự). */}
              <ReferenceArea
                y1={yMin}
                y2={0}
                fill={ROI_DANGER_COLOR}
                fillOpacity={0.14}
                ifOverflow="visible"
              />
              {/* Warning zone: 0 → 50% → biên lãi mỏng, cảnh báo. */}
              <ReferenceArea
                y1={0}
                y2={ROI_SAFE_THRESHOLD}
                fill={ROI_WARN_COLOR}
                fillOpacity={0.08}
                ifOverflow="visible"
              />
              {noAdsSegments.map((s, i) => (
                <ReferenceArea
                  key={`no-ads-${i}`}
                  x1={s.x1}
                  x2={s.x2}
                  fill="#ffffff"
                  fillOpacity={0.05}
                  stroke="#ffffff"
                  strokeOpacity={0.25}
                  strokeDasharray="3 3"
                  ifOverflow="extendDomain"
                  label={{
                    value: s.days === 1 ? "Tắt" : `Tắt ${s.days}d`,
                    fill: "#ffffff90",
                    fontSize: 10,
                    fontWeight: 600,
                    position: "insideTop",
                  }}
                />
              ))}
              <ReferenceLine
                y={0}
                stroke="#ffffff"
                strokeWidth={2.5}
                ifOverflow="extendDomain"
                label={(props: {
                  viewBox?: { x?: number; y?: number; width?: number };
                }) => {
                  const vb = props.viewBox ?? {};
                  const x = (vb.x ?? 0) + (vb.width ?? 0) - 8;
                  const y = vb.y ?? 0;
                  const w = 138;
                  const h = 22;
                  return (
                    <g>
                      <rect
                        x={x - w}
                        y={y - h / 2}
                        width={w}
                        height={h}
                        rx={4}
                        fill="#0a0a0a"
                        stroke="#ffffff"
                        strokeWidth={1.5}
                      />
                      <text
                        x={x - w / 2}
                        y={y + 1}
                        textAnchor="middle"
                        dominantBaseline="middle"
                        fontSize={11}
                        fontWeight={800}
                        fill="#ffffff"
                        style={{ letterSpacing: "0.06em" }}
                      >
                        ◆ HÒA VỐN · ROI 0%
                      </text>
                    </g>
                  );
                }}
              />
              <ReferenceLine
                y={ROI_SAFE_THRESHOLD}
                stroke={ROI_SAFE_COLOR}
                strokeWidth={1.5}
                strokeDasharray="4 4"
                label={{
                  value: `An toàn ≥ ${ROI_SAFE_THRESHOLD}%`,
                  fill: ROI_SAFE_COLOR,
                  fontSize: 11,
                  fontWeight: 600,
                  position: "insideTopRight",
                }}
              />
              <Line
                type="monotone"
                dataKey="roi"
                name="ROI %"
                stroke="#ffffff60"
                strokeWidth={2.5}
                dot={(props: {
                  cx?: number;
                  cy?: number;
                  payload?: TrendPoint;
                  index?: number;
                }) => {
                  const { cx, cy, payload, index } = props;
                  if (cx === undefined || cy === undefined || !payload) {
                    return <g key={`dot-${index}`} />;
                  }
                  // Ngày không có ads → vẽ dấu × trắng để dễ scan camp tắt.
                  if (payload.roi === null) {
                    const r = 4;
                    return (
                      <g key={`dot-${index ?? payload.date}`}>
                        <line
                          x1={cx - r}
                          y1={cy - r}
                          x2={cx + r}
                          y2={cy + r}
                          stroke="#ffffff80"
                          strokeWidth={2}
                          strokeLinecap="round"
                        />
                        <line
                          x1={cx - r}
                          y1={cy + r}
                          x2={cx + r}
                          y2={cy - r}
                          stroke="#ffffff80"
                          strokeWidth={2}
                          strokeLinecap="round"
                        />
                      </g>
                    );
                  }
                  const color = roiColor(payload.roi);
                  // Dot lỗ (ROI<0) to hơn + viền pulse-like để hút mắt.
                  const isDanger = payload.roi < 0;
                  const r = isDanger ? 6 : payload.roi < ROI_SAFE_THRESHOLD ? 4.5 : 4;
                  return (
                    <g key={`dot-${index ?? payload.date}`}>
                      {isDanger && (
                        <circle
                          cx={cx}
                          cy={cy}
                          r={r + 3}
                          fill={ROI_DANGER_COLOR}
                          fillOpacity={0.25}
                        />
                      )}
                      <circle
                        cx={cx}
                        cy={cy}
                        r={r}
                        fill={color}
                        stroke="#0a0a0a"
                        strokeWidth={1.5}
                      />
                    </g>
                  );
                }}
                connectNulls={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </section>
  );
}

interface RoiTooltipProps {
  active?: boolean;
  payload?: Array<{ payload: TrendPoint }>;
}

function RoiTooltip({ active, payload }: RoiTooltipProps) {
  if (!active || !payload || payload.length === 0) return null;
  const point = payload[0].payload;
  const isOff = point.spend === 0;
  let roiCls = "text-white/40";
  let statusBadge: React.ReactNode = null;
  if (point.roi !== null) {
    if (point.roi < 0) {
      roiCls = "text-red-300";
      statusBadge = (
        <span className="inline-flex items-center gap-0.5 rounded-md bg-red-500/25 px-1.5 py-0.5 text-[10px] font-bold uppercase text-red-200">
          <span className="material-symbols-rounded text-[12px]">error</span>
          Lỗ
        </span>
      );
    } else if (point.roi < ROI_SAFE_THRESHOLD) {
      roiCls = "text-amber-300";
      statusBadge = (
        <span className="inline-flex items-center gap-0.5 rounded-md bg-amber-500/20 px-1.5 py-0.5 text-[10px] font-bold uppercase text-amber-200">
          <span className="material-symbols-rounded text-[12px]">warning</span>
          Cảnh báo
        </span>
      );
    } else {
      roiCls = "text-green-300";
      statusBadge = (
        <span className="inline-flex items-center gap-0.5 rounded-md bg-green-500/20 px-1.5 py-0.5 text-[10px] font-bold uppercase text-green-200">
          <span className="material-symbols-rounded text-[12px]">check_circle</span>
          An toàn
        </span>
      );
    }
  }
  return (
    <div className="rounded-lg border border-surface-8 bg-surface-0/95 px-3 py-2 shadow-elev-8 backdrop-blur">
      <div className="mb-1 flex items-center gap-1.5 text-xs font-semibold text-white/90">
        {fmtDate(point.date)}
        {isOff ? (
          <span className="inline-flex items-center gap-0.5 rounded-md bg-white/15 px-1.5 py-0.5 text-[10px] font-medium text-white/80">
            <span className="material-symbols-rounded text-[12px]">power_off</span>
            Tắt camp
          </span>
        ) : (
          statusBadge
        )}
      </div>
      <dl className="space-y-0.5 text-xs">
        <div className="flex items-center gap-2">
          <span className="text-white/60">ROI</span>
          <span className={`ml-auto tabular-nums font-medium ${roiCls}`}>
            {point.roi === null ? "—" : fmtPct(point.roi)}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-white/60">Chi tiêu</span>
          <span
            className={`ml-auto tabular-nums font-medium ${
              isOff ? "text-amber-300/80" : "text-blue-300"
            }`}
          >
            {isOff ? "—" : fmtVnd(point.spend)}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-white/60">Lợi nhuận</span>
          <span
            className={`ml-auto tabular-nums font-medium ${
              point.profit > 0
                ? "text-green-300"
                : point.profit < 0
                ? "text-red-300"
                : "text-white/70"
            }`}
          >
            {fmtVnd(point.profit)}
          </span>
        </div>
      </dl>
    </div>
  );
}

// =========================================================
// Row component
// =========================================================

function HistoryRow({
  row,
  showAccount,
}: {
  row: UiRow;
  showAccount: boolean;
}) {
  const { settings } = useSettings();
  const shopeeClicks = sumFiltered(
    row.shopeeClicksByReferrer,
    settings.clickSources,
  );
  const c = computeUiRow(row, settings.profitFees, shopeeClicks);
  const profitCls =
    c.profit > 0
      ? "text-green-400"
      : c.profit < 0
      ? "text-red-400"
      : "text-gray-400";

  const hasSpend = !!row.totalSpend && row.totalSpend > 0;

  return (
    <tr className="border-b border-surface-8 text-white/80 transition-colors hover:bg-shopee-500/10">
      <td className="px-3 py-2.5 text-center tabular-nums font-medium">
        {fmtDate(row.dayDate)}
      </td>
      {showAccount && (
        <td className="px-3 py-2.5 text-center">
          {row.accountName ? (
            <span
              className="inline-block max-w-[140px] truncate rounded-md bg-shopee-900/40 px-2 py-0.5 text-xs font-medium text-shopee-200"
              title={row.accountName}
            >
              {row.accountName}
            </span>
          ) : (
            <span className="inline-block rounded-md bg-amber-500/15 px-2 py-0.5 text-xs font-medium text-amber-300">
              FB chung
            </span>
          )}
        </td>
      )}
      <td className={`px-3 py-2.5 text-center tabular-nums ${row.totalSpend === null ? "text-white/30" : "text-blue-400"}`}>
        {row.totalSpend !== null ? fmtVnd(row.totalSpend) : "—"}
      </td>
      <td className="px-3 py-2.5 text-center tabular-nums">
        {fmtInt(row.ordersCount)}
      </td>
      <td className="px-3 py-2.5 text-center tabular-nums text-shopee-400">
        {fmtVnd(row.commissionTotal)}
      </td>
      <td className={`px-3 py-2.5 text-center tabular-nums font-semibold ${profitCls}`}>
        <span className="mr-1 text-xs">
          {c.profit > 0 ? "▲" : c.profit < 0 ? "▼" : ""}
        </span>
        {fmtVnd(c.profit)}
      </td>
      <td
        className={`px-3 py-2.5 text-center tabular-nums ${hasSpend ? profitCls : "text-white/30"}`}
        title={
          hasSpend
            ? `ROI = (${fmtVnd(c.netCommission)} − ${fmtVnd(row.totalSpend!)}) / ${fmtVnd(row.totalSpend!)} × 100%`
            : "Không có spend → không tính ROI"
        }
      >
        {hasSpend ? fmtPct(c.profitMargin) : "—"}
      </td>
    </tr>
  );
}
