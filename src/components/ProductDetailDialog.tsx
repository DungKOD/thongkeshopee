import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import type { OrderItemDetail, UiRow } from "../types";
import {
  computeUiRow,
  fmtDate,
  fmtDuration,
  fmtInt,
  fmtPct,
  fmtVnd,
} from "../formulas";
import { sumFiltered, useSettings } from "../hooks/useSettings";
import { invoke } from "../lib/tauri";

interface ProductDetailDialogProps {
  isOpen: boolean;
  row: UiRow | null;
  onClose: () => void;
}

export function ProductDetailDialog({
  isOpen,
  row,
  onClose,
}: ProductDetailDialogProps) {
  const { settings } = useSettings();
  const [items, setItems] = useState<OrderItemDetail[]>([]);
  const [loadingItems, setLoadingItems] = useState(false);

  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  useEffect(() => {
    if (!isOpen || !row) {
      setItems([]);
      return;
    }
    let cancelled = false;
    setLoadingItems(true);
    invoke<OrderItemDetail[]>("get_order_items_for_row", {
      dayDate: row.dayDate,
      subIds: row.subIds,
    })
      .then((data) => {
        if (!cancelled) setItems(data);
      })
      .catch(() => {
        if (!cancelled) setItems([]);
      })
      .finally(() => {
        if (!cancelled) setLoadingItems(false);
      });
    return () => {
      cancelled = true;
    };
  }, [isOpen, row]);

  if (!isOpen || !row) return null;

  const shopeeClicks = sumFiltered(
    row.shopeeClicksByReferrer,
    settings.clickSources,
  );
  const computed = computeUiRow(row, settings.profitFees, shopeeClicks);
  const feeAmount = row.commissionTotal - computed.netCommission;
  const hasAdSpend = !!row.totalSpend && row.totalSpend > 0;

  const profitTone =
    computed.profit > 0
      ? "positive"
      : computed.profit < 0
      ? "negative"
      : "neutral";

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  const sources: { icon: string; label: string; active: boolean }[] = [
    { icon: "ads_click", label: "FB Ads", active: row.hasFb },
    { icon: "ads_click", label: "Shopee Click", active: row.hasShopeeClicks },
    { icon: "shopping_cart", label: "Shopee Order", active: row.hasShopeeOrders },
    { icon: "edit_note", label: "Manual", active: row.hasManual },
  ];

  const uniqueOrderIds = new Set(items.map((i) => i.orderId)).size;
  const cancelledCount = items.filter((i) =>
    /hủy|cancel/i.test(i.orderStatus ?? ""),
  ).length;
  const zeroCommissionCount = items.filter(
    (i) => (i.netCommission ?? 0) === 0,
  ).length;

  // Chỉ số đơn hàng
  const totalItemsCount = items.length;
  const cancelRate =
    totalItemsCount > 0 ? (cancelledCount / totalItemsCount) * 100 : null;
  const avgCommission =
    row.ordersCount > 0 ? row.commissionTotal / row.ordersCount : null;
  const commissionRate =
    row.orderValueTotal > 0
      ? (row.commissionTotal / row.orderValueTotal) * 100
      : null;

  const statusColor = (s: string | null | undefined): string => {
    if (!s) return "text-white/60";
    if (/hủy|cancel/i.test(s)) return "text-red-400";
    if (/hoàn thành|completed/i.test(s)) return "text-green-400";
    if (/chờ|pending/i.test(s)) return "text-amber-400";
    return "text-white/80";
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        className="flex max-h-[calc(100vh-2rem)] w-full max-w-5xl flex-col overflow-hidden rounded-2xl bg-surface-2 shadow-elev-24"
        role="dialog"
        aria-modal="true"
        aria-labelledby="product-detail-dialog-title"
      >
        {/* ============ App bar ============ */}
        <header className="flex shrink-0 items-start gap-4 bg-gradient-to-r from-shopee-700/90 to-shopee-600/70 px-6 py-4">
          <span className="mt-1 flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-white/15 text-white">
            <span className="material-symbols-rounded text-xl">
              inventory_2
            </span>
          </span>
          <div className="min-w-0 flex-1">
            <p className="text-[11px] font-medium uppercase tracking-[0.12em] text-white/70">
              Chi tiết campaign
            </p>
            <h2
              id="product-detail-dialog-title"
              className="mt-0.5 truncate text-xl font-bold text-white"
              title={row.displayName}
            >
              {row.displayName || "(chưa đặt tên)"}
            </h2>
            <div className="mt-1.5 flex flex-wrap items-center gap-1.5 text-xs text-white/80">
              <span className="inline-flex items-center gap-1">
                <span className="material-symbols-rounded text-sm">event</span>
                {fmtDate(row.dayDate)}
              </span>
              <span className="text-white/40">•</span>
              {sources
                .filter((s) => s.active)
                .map((s) => (
                  <span
                    key={s.label}
                    className="inline-flex items-center gap-1 rounded-full bg-white/15 px-2 py-0.5 text-[11px]"
                  >
                    <span className="material-symbols-rounded text-xs">
                      {s.icon}
                    </span>
                    {s.label}
                  </span>
                ))}
              {sources.every((s) => !s.active) && (
                <span className="italic">Chưa có nguồn</span>
              )}
            </div>
          </div>
        </header>

        <div className="min-h-0 flex-1 space-y-6 overflow-y-auto bg-surface-0 px-6 py-5">
          {/* ============ KPI cards ============ */}
          <section className="grid grid-cols-2 gap-3 md:grid-cols-4">
            <KpiCard
              icon="trending_up"
              label="Lợi nhuận"
              value={fmtVnd(computed.profit)}
              tone={profitTone}
            />
            <KpiCard
              icon="percent"
              label="ROI"
              value={hasAdSpend ? fmtPct(computed.profitMargin) : "—"}
              tone={hasAdSpend ? profitTone : "muted"}
              tooltip="ROI = (Hoa hồng sau phí − Tiền ads) / Tiền ads"
            />
            <KpiCard
              icon="shopping_bag"
              label="Số đơn"
              value={fmtInt(row.ordersCount)}
              sub={fmtVnd(row.commissionTotal) + " hoa hồng"}
            />
            <KpiCard
              icon="payments"
              label="Tiền ads"
              value={hasAdSpend ? fmtVnd(row.totalSpend!) : "—"}
              sub={
                row.adsClicks
                  ? `${fmtInt(row.adsClicks)} click · CPC ${
                      row.cpc ? fmtVnd(row.cpc) : "—"
                    }`
                  : undefined
              }
              tone="muted"
            />
          </section>

          {/* ============ Chỉ số quảng cáo ============ */}
          <Section icon="ads_click" title="Chỉ số quảng cáo">
            <div className="grid grid-cols-2 gap-x-8 gap-y-2 rounded-xl bg-surface-2 px-5 py-4 shadow-elev-1 md:grid-cols-3">
              {/* Cột 1: Click ADS + Click Shopee để so sánh */}
              <MetricRow
                label="Click ADS"
                value={row.adsClicks === null ? "—" : fmtInt(row.adsClicks)}
                tooltip="Số click vào liên kết FB (link_clicks)"
              />
              <MetricRow
                label="Click Shopee"
                value={fmtInt(shopeeClicks)}
                tooltip="Số click từ FB → Shopee affiliate (cookie attribute)"
              />
              <MetricRow
                label="Tỷ lệ qua Shopee"
                value={
                  row.adsClicks && row.adsClicks > 0
                    ? fmtPct((shopeeClicks / row.adsClicks) * 100)
                    : "—"
                }
                tooltip="Click Shopee / Click ADS × 100% — mức độ chuyển từ click ads sang thực sự vào Shopee"
              />
              <MetricRow
                label="Lượt hiển thị"
                value={row.impressions === null ? "—" : fmtInt(row.impressions)}
              />
              <MetricRow
                label="CPC (FB báo)"
                value={row.cpc === null ? "—" : fmtVnd(row.cpc)}
                tooltip={
                  "CPC (FB báo) = Tổng tiền chạy / Click ADS\n" +
                  "Đơn giá mỗi click ads theo số FB đã đếm.\n" +
                  "Ưu tiên weighted avg từ FB, fallback spend ÷ clicks."
                }
              />
              <MetricRow
                label="CPC thực tế (Shopee)"
                value={
                  row.totalSpend && row.totalSpend > 0 && shopeeClicks > 0
                    ? fmtVnd(row.totalSpend / shopeeClicks)
                    : "—"
                }
                tooltip={
                  "CPC thực tế = Tổng tiền chạy / Click Shopee\n" +
                  "Đơn giá mỗi lượt THẬT SỰ vào Shopee affiliate.\n\n" +
                  "So sánh với 'CPC (FB báo)':\n" +
                  "• Nếu CPC thực tế >> CPC FB → nhiều click FB không vào được Shopee (bot, misclick, attribution lost).\n" +
                  "• Càng gần CPC FB → traffic càng chất lượng."
                }
              />
              <MetricRow
                label="Tỷ lệ chuyển đổi"
                value={
                  shopeeClicks > 0 ? fmtPct(computed.conversionRate) : "—"
                }
                tooltip="CR = Số đơn / Click Shopee × 100%"
              />
              <MetricRow
                label="Tổng tiền chạy"
                value={
                  row.totalSpend === null ? "—" : fmtVnd(row.totalSpend)
                }
              />
            </div>
          </Section>

          {/* ============ Chỉ số đơn hàng ============ */}
          <Section icon="shopping_bag" title="Chỉ số đơn hàng">
            <div className="grid grid-cols-2 gap-x-8 gap-y-2 rounded-xl bg-surface-2 px-5 py-4 shadow-elev-1 md:grid-cols-3">
              <MetricRow
                label="Số đơn"
                value={fmtInt(row.ordersCount)}
              />
              <MetricRow
                label="Tỷ lệ hoàn/hủy"
                value={
                  cancelRate !== null
                    ? fmtPct(cancelRate)
                    : loadingItems
                    ? "…"
                    : "—"
                }
                tooltip="Số item hủy / Tổng item trong đơn × 100% (tính từ Shopee Commission CSV)"
              />
              <MetricRow
                label="GMV tổng"
                value={fmtVnd(row.orderValueTotal)}
                tooltip="Tổng Giá trị đơn hàng"
              />
              <MetricRow
                label="GMV trung bình"
                value={
                  row.ordersCount > 0
                    ? fmtVnd(row.orderValueTotal / row.ordersCount)
                    : "—"
                }
                tooltip="GMV tổng / Số đơn"
              />
              <MetricRow
                label="Hoa hồng trung bình"
                value={avgCommission !== null ? fmtVnd(avgCommission) : "—"}
                tooltip="Tổng hoa hồng / Số đơn"
              />
              <MetricRow
                label="Tỷ lệ hoa hồng"
                value={
                  commissionRate !== null ? fmtPct(commissionRate) : "—"
                }
                tooltip="Hoa hồng / GMV × 100% — tỷ suất hoa hồng trên doanh thu"
              />
            </div>
          </Section>

          {/* ============ Click theo nguồn ============ */}
          {Object.entries(row.shopeeClicksByReferrer).length > 0 && (
            <Section icon="hub" title="Click theo nguồn">
              <div className="flex flex-wrap gap-2">
                {Object.entries(row.shopeeClicksByReferrer)
                  .sort((a, b) => b[1] - a[1])
                  .map(([ref, n]) => (
                    <div
                      key={ref}
                      className="flex items-baseline gap-2 rounded-full bg-surface-4 px-3 py-1.5 shadow-elev-1"
                    >
                      <span className="text-xs font-medium text-white/60">
                        {ref}
                      </span>
                      <span className="text-sm font-semibold tabular-nums text-shopee-300">
                        {fmtInt(n)}
                      </span>
                    </div>
                  ))}
              </div>
            </Section>
          )}

          {/* ============ Lợi nhuận breakdown ============ */}
          <Section icon="account_balance_wallet" title="Lợi nhuận">
            <div className="rounded-xl bg-surface-2 px-5 py-4 shadow-elev-1">
              <BreakdownRow
                label="Hoa hồng gross"
                value={fmtVnd(row.commissionTotal)}
              />
              <BreakdownRow
                label={`Phí (thuế & sàn ${settings.profitFees.taxAndPlatformRate}% + dự phòng ${settings.profitFees.returnReserveRate}%)`}
                value={`− ${fmtVnd(feeAmount)}`}
                valueClass="text-amber-400"
              />
              <div className="my-2 border-t border-dashed border-surface-8" />
              <BreakdownRow
                label="Hoa hồng sau phí"
                value={fmtVnd(computed.netCommission)}
                labelClass="text-white/80 font-medium"
              />
              <BreakdownRow
                label="Tiền ads"
                value={`− ${fmtVnd(row.totalSpend ?? 0)}`}
                valueClass="text-amber-400"
              />
              <div className="my-2 border-t-2 border-shopee-500/60" />
              <BreakdownRow
                label="Lợi nhuận"
                value={fmtVnd(computed.profit)}
                labelClass="text-base font-semibold text-white"
                valueClass={`text-base font-bold ${
                  profitTone === "positive"
                    ? "text-green-400"
                    : profitTone === "negative"
                    ? "text-red-400"
                    : "text-white/80"
                }`}
              />
              <BreakdownRow
                label="ROI"
                value={
                  hasAdSpend ? fmtPct(computed.profitMargin) : "— (chưa có spend)"
                }
                labelClass="text-white/60"
                valueClass={
                  hasAdSpend
                    ? profitTone === "positive"
                      ? "text-green-400"
                      : profitTone === "negative"
                      ? "text-red-400"
                      : "text-white/80"
                    : "text-white/30"
                }
              />
            </div>
          </Section>

          {/* ============ Sản phẩm bán được ============ */}
          <Section
            icon="shopping_cart"
            title="Sản phẩm bán được"
            count={loadingItems ? undefined : items.length}
            right={
              !loadingItems && items.length > 0 ? (
                <div className="flex gap-1.5 text-[11px]">
                  <Chip tone="default">{uniqueOrderIds} đơn</Chip>
                  {cancelledCount > 0 && (
                    <Chip tone="danger">{cancelledCount} hủy</Chip>
                  )}
                  {zeroCommissionCount > 0 && (
                    <Chip tone="warning">{zeroCommissionCount} không HH</Chip>
                  )}
                </div>
              ) : null
            }
          >
            {loadingItems ? (
              <div className="rounded-xl border border-surface-8 bg-surface-2 px-4 py-4 text-sm text-white/50">
                <span className="material-symbols-rounded mr-2 animate-spin align-middle text-base">
                  progress_activity
                </span>
                Đang tải...
              </div>
            ) : items.length === 0 ? (
              <div className="flex items-center gap-3 rounded-xl border border-dashed border-surface-8 bg-surface-2 px-4 py-4 text-sm text-white/60">
                <span className="material-symbols-rounded text-shopee-400">
                  info
                </span>
                <span>
                  Chưa có order nào cho campaign này. Import Shopee Commission
                  CSV để xem chi tiết.
                </span>
              </div>
            ) : (
              <div className="overflow-hidden rounded-xl border border-surface-8 bg-surface-2 shadow-elev-1">
                <div className="overflow-x-auto">
                  <table className="w-full border-collapse text-xs">
                    <thead>
                      <tr className="bg-surface-4 text-shopee-200">
                        <th className="border-b border-surface-8 px-3 py-2.5 text-left font-semibold uppercase tracking-wider">
                          Sản phẩm
                        </th>
                        <th className="border-b border-surface-8 px-3 py-2.5 text-center font-semibold uppercase tracking-wider">
                          SL
                        </th>
                        <th className="border-b border-surface-8 px-3 py-2.5 text-center font-semibold uppercase tracking-wider">
                          Trạng thái
                        </th>
                        <th className="border-b border-surface-8 px-3 py-2.5 text-center font-semibold uppercase tracking-wider">
                          Click → Đặt
                        </th>
                        <th className="border-b border-surface-8 px-3 py-2.5 text-right font-semibold uppercase tracking-wider">
                          GMV
                        </th>
                        <th className="border-b border-surface-8 px-3 py-2.5 text-right font-semibold uppercase tracking-wider">
                          Hoa hồng ròng
                        </th>
                        <th className="border-b border-surface-8 px-3 py-2.5 text-left font-semibold uppercase tracking-wider">
                          Kênh
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {items.map((it, idx) => {
                        const duration = fmtDuration(
                          it.clickTime,
                          it.orderTime,
                        );
                        const statusCls = statusColor(it.orderStatus);
                        const tooltipName = it.itemName ?? "";
                        const shopAndName = it.shopName
                          ? `${it.shopName} · ${tooltipName}`
                          : tooltipName;
                        return (
                          <tr
                            key={`${it.orderId}-${it.itemId}-${it.modelId}-${idx}`}
                            className="border-b border-surface-8 last:border-b-0 text-white/85 transition-colors hover:bg-shopee-500/15"
                          >
                            <td
                              className="max-w-[320px] truncate px-3 py-2"
                              title={shopAndName}
                            >
                              {it.itemName || (
                                <span className="italic text-white/40">
                                  (không có tên)
                                </span>
                              )}
                            </td>
                            <td className="px-3 py-2 text-center tabular-nums">
                              {it.quantity ?? "—"}
                            </td>
                            <td
                              className={`px-3 py-2 text-center ${statusCls}`}
                            >
                              {it.orderStatus ?? "—"}
                            </td>
                            <td
                              className="px-3 py-2 text-center tabular-nums"
                              title={
                                it.clickTime && it.orderTime
                                  ? `Click: ${it.clickTime}\nĐặt: ${it.orderTime}`
                                  : undefined
                              }
                            >
                              {duration || "—"}
                            </td>
                            <td className="px-3 py-2 text-right tabular-nums">
                              {it.orderValue !== null
                                ? fmtVnd(it.orderValue)
                                : "—"}
                            </td>
                            <td className="px-3 py-2 text-right tabular-nums">
                              {it.netCommission !== null
                                ? fmtVnd(it.netCommission)
                                : "—"}
                            </td>
                            <td className="px-3 py-2 text-left text-white/60">
                              {it.channel ?? "—"}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </Section>

          {/* ============ Sub_id identity (ẩn xuống cuối) ============ */}
          <Section icon="tag" title="Định danh Sub_id">
            <div className="grid grid-cols-2 gap-2 rounded-xl bg-surface-2 px-5 py-4 shadow-elev-1 md:grid-cols-5">
              {row.subIds.map((s, i) => (
                <div key={i} className="min-w-0">
                  <p className="text-[10px] font-medium uppercase tracking-wider text-white/45">
                    Sub_id{i + 1}
                  </p>
                  <p
                    className="mt-0.5 truncate font-mono text-xs text-white/85"
                    title={s}
                  >
                    {s || <span className="text-white/30">—</span>}
                  </p>
                </div>
              ))}
            </div>
          </Section>
        </div>

        <footer className="flex shrink-0 justify-end gap-2 border-t border-surface-8 bg-surface-1 px-6 py-3">
          <button
            type="button"
            onClick={onClose}
            className="btn-ripple rounded-lg bg-shopee-500 px-6 py-2 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4"
          >
            Đóng
          </button>
        </footer>
      </div>
    </div>,
    document.body,
  );
}

// =========================================================
// Presentational atoms
// =========================================================

function Section({
  icon,
  title,
  count,
  right,
  children,
}: {
  icon: string;
  title: string;
  count?: number;
  right?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section>
      <div className="mb-2 flex items-center gap-2">
        <span className="material-symbols-rounded text-base text-shopee-400">
          {icon}
        </span>
        <h3 className="flex-1 text-xs font-semibold uppercase tracking-[0.1em] text-white/60">
          {title}
          {count !== undefined && (
            <span className="ml-1.5 text-white/40">({count})</span>
          )}
        </h3>
        {right}
      </div>
      {children}
    </section>
  );
}

function KpiCard({
  icon,
  label,
  value,
  sub,
  tone = "neutral",
  tooltip,
}: {
  icon: string;
  label: string;
  value: string;
  sub?: string;
  tone?: "positive" | "negative" | "neutral" | "muted";
  tooltip?: string;
}) {
  const toneMap: Record<string, string> = {
    positive: "text-green-400",
    negative: "text-red-400",
    neutral: "text-white",
    muted: "text-white/70",
  };
  return (
    <div
      className="rounded-xl bg-surface-4 p-4 shadow-elev-2 transition-shadow hover:shadow-elev-4"
      title={tooltip}
    >
      <div className="flex items-center gap-1.5">
        <span className="material-symbols-rounded text-base text-white/40">
          {icon}
        </span>
        <p className="text-[11px] font-medium uppercase tracking-wider text-white/55">
          {label}
        </p>
      </div>
      <p
        className={`mt-1.5 truncate text-2xl font-bold tabular-nums ${toneMap[tone]}`}
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

function MetricRow({
  label,
  value,
  tooltip,
}: {
  label: string;
  value: string;
  tooltip?: string;
}) {
  return (
    <div className="flex items-baseline justify-between gap-2 border-b border-dashed border-surface-6 py-1 last:border-b-0">
      <span
        className={`text-sm text-white/60 ${tooltip ? "cursor-help" : ""}`}
        title={tooltip}
      >
        {label}
      </span>
      <span className="text-sm font-semibold tabular-nums text-white/95">
        {value}
      </span>
    </div>
  );
}

function BreakdownRow({
  label,
  value,
  labelClass = "text-sm text-white/65",
  valueClass = "text-sm tabular-nums text-white/90",
}: {
  label: string;
  value: string;
  labelClass?: string;
  valueClass?: string;
}) {
  return (
    <div className="flex items-baseline justify-between gap-3 py-1">
      <span className={labelClass}>{label}</span>
      <span className={valueClass}>{value}</span>
    </div>
  );
}

function Chip({
  tone,
  children,
}: {
  tone: "default" | "danger" | "warning";
  children: React.ReactNode;
}) {
  const cls =
    tone === "danger"
      ? "bg-red-500/15 text-red-300 border-red-500/30"
      : tone === "warning"
      ? "bg-amber-500/15 text-amber-300 border-amber-500/30"
      : "bg-surface-6 text-white/70 border-surface-8";
  return (
    <span
      className={`rounded-full border px-2 py-0.5 font-medium normal-case ${cls}`}
    >
      {children}
    </span>
  );
}
