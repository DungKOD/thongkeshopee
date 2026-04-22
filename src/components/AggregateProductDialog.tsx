import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { OrderItemDetail, UiDay } from "../types";
import {
  fmtDate,
  fmtDuration,
  fmtInt,
  fmtPct,
  fmtVnd,
  type AggregatedProductRow,
  type SourceFilter,
} from "../formulas";
import { useSettings } from "../hooks/useSettings";
import { invoke } from "../lib/tauri";
import {
  captureElementToBlob,
  prefetchFontEmbedCSS,
} from "../lib/screenshot";
import { DayScreenshotDialog } from "./DayScreenshotDialog";
import { ScrollToTopButton } from "./ScrollToTopButton";

interface AggregateProductDialogProps {
  isOpen: boolean;
  product: AggregatedProductRow | null;
  /** Days đã load ở tab Overview — dialog tự filter rows match subIds. */
  days: UiDay[];
  source: SourceFilter;
  onClose: () => void;
}

export function AggregateProductDialog({
  isOpen,
  product,
  days,
  source,
  onClose,
}: AggregateProductDialogProps) {
  useSettings(); // subscribe để tự re-render khi profitFees đổi (không dùng trực tiếp ở đây)
  const [capturing, setCapturing] = useState(false);
  const [screenshotBlob, setScreenshotBlob] = useState<Blob | null>(null);
  const dialogRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Body scroll lock.
  useEffect(() => {
    if (!isOpen) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = prev;
    };
  }, [isOpen]);

  // Esc — để DayScreenshotDialog handle khi nó mở.
  useEffect(() => {
    if (!isOpen) return;
    if (screenshotBlob) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose, screenshotBlob]);

  // Clear screenshot khi dialog đóng.
  useEffect(() => {
    if (!isOpen) setScreenshotBlob(null);
  }, [isOpen]);

  const perDayMatches = useMemo(() => {
    if (!product) return [];
    const key = product.subIds.join("\x1f");
    const result: string[] = [];
    for (const d of days) {
      for (const r of d.rows) {
        if (r.subIds.join("\x1f") === key) {
          result.push(d.date);
          break;
        }
      }
    }
    return result;
  }, [product, days]);

  const [items, setItems] = useState<OrderItemDetail[]>([]);
  const [loadingItems, setLoadingItems] = useState(false);

  // Fetch order items cho tất cả ngày có sản phẩm này, merge.
  useEffect(() => {
    if (!isOpen || !product || perDayMatches.length === 0) {
      setItems([]);
      return;
    }
    let cancelled = false;
    setLoadingItems(true);
    const subIds = product.subIds as unknown as [string, string, string, string, string];
    Promise.all(
      perDayMatches.map((dayDate) =>
        invoke<OrderItemDetail[]>("get_order_items_for_row", {
          dayDate,
          subIds,
        }).catch(() => [] as OrderItemDetail[]),
      ),
    )
      .then((batches) => {
        if (cancelled) return;
        const flat = batches.flat();
        // Dedupe theo (orderId, itemId, modelId) — trong thực tế không lặp qua
        // ngày nhưng thêm guard cho chắc.
        const seen = new Set<string>();
        const unique: OrderItemDetail[] = [];
        for (const it of flat) {
          const k = `${it.orderId}\x1f${it.itemId}\x1f${it.modelId}`;
          if (seen.has(k)) continue;
          seen.add(k);
          unique.push(it);
        }
        // Sort theo order_time desc (mới nhất trên cùng).
        unique.sort((a, b) => {
          const ao = a.orderTime ?? "";
          const bo = b.orderTime ?? "";
          return ao < bo ? 1 : ao > bo ? -1 : 0;
        });
        setItems(unique);
      })
      .finally(() => {
        if (!cancelled) setLoadingItems(false);
      });
    return () => {
      cancelled = true;
    };
  }, [isOpen, product, perDayMatches]);

  const uniqueOrderIds = useMemo(
    () => new Set(items.map((i) => i.orderId)).size,
    [items],
  );
  const cancelledCount = useMemo(
    () => items.filter((i) => /hủy|cancel/i.test(i.orderStatus ?? "")).length,
    [items],
  );
  const zeroCommissionCount = useMemo(
    () => items.filter((i) => (i.netCommission ?? 0) === 0).length,
    [items],
  );

  const handleScreenshot = async () => {
    if (!dialogRef.current || capturing) return;
    setCapturing(true);
    try {
      const blob = await captureElementToBlob(dialogRef.current, {
        pixelRatio: 2,
        backgroundColor: "#121212",
      });
      setScreenshotBlob(blob);
    } catch (e) {
      console.error("screenshot failed", e);
      alert(`Chụp ảnh thất bại: ${String(e)}`);
    } finally {
      setCapturing(false);
    }
  };

  if (!isOpen || !product) return null;

  const showAds = source === "all";
  const cr =
    product.shopeeClicks > 0
      ? (product.ordersCount / product.shopeeClicks) * 100
      : null;
  const roi =
    product.totalSpend > 0
      ? (product.profit / product.totalSpend) * 100
      : null;
  const avgOrderValue =
    product.ordersCount > 0 ? product.orderValueTotal / product.ordersCount : null;
  const avgCommission =
    product.ordersCount > 0 ? product.commissionTotal / product.ordersCount : null;
  const cpc =
    product.adsClicks > 0 ? product.totalSpend / product.adsClicks : null;

  const profitTone =
    product.profit > 0
      ? "positive"
      : product.profit < 0
      ? "negative"
      : "neutral";

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        ref={dialogRef}
        className={`relative flex max-h-[calc(100vh-2rem)] w-full max-w-5xl flex-col overflow-hidden rounded-2xl bg-surface-2 shadow-elev-24 ${
          capturing ? "capture-mode" : ""
        }`}
        role="dialog"
        aria-modal="true"
        aria-labelledby="aggregate-product-dialog-title"
      >
        {/* ============ App bar ============ */}
        <header className="flex shrink-0 items-start gap-4 bg-gradient-to-r from-shopee-700/90 to-shopee-600/70 px-6 py-4">
          <span className="mt-1 flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-white/15 text-white">
            <span className="material-symbols-rounded text-xl">inventory_2</span>
          </span>
          <div className="min-w-0 flex-1">
            <p className="text-[11px] font-medium uppercase tracking-[0.12em] text-white/70">
              Chi tiết sản phẩm (tổng hợp)
            </p>
            <h2
              id="aggregate-product-dialog-title"
              className="mt-0.5 truncate text-xl font-bold text-white"
              title={product.displayName}
            >
              {product.displayName || "(chưa đặt tên)"}
            </h2>
            <div className="mt-1.5 flex flex-wrap items-center gap-1.5 text-xs text-white/80">
              <span className="inline-flex items-center gap-1">
                <span className="material-symbols-rounded text-sm">event</span>
                {product.daysActive} ngày hoạt động
              </span>
              <span className="text-white/40">•</span>
              <span className="inline-flex items-center gap-1 rounded-full bg-white/15 px-2 py-0.5 text-[11px]">
                <span className="material-symbols-rounded text-xs">filter_alt</span>
                {showAds ? "Tất cả" : "Chỉ Shopee"}
              </span>
            </div>
          </div>
          <div className="capture-hide flex shrink-0 items-center">
            <button
              type="button"
              onClick={handleScreenshot}
              onMouseEnter={prefetchFontEmbedCSS}
              onFocus={prefetchFontEmbedCSS}
              disabled={capturing}
              className="btn-ripple flex h-10 w-10 items-center justify-center rounded-full text-white/80 hover:bg-white/15 disabled:cursor-not-allowed disabled:opacity-40"
              title={capturing ? "Đang chụp..." : "Chụp ảnh chi tiết"}
              aria-label="Chụp ảnh chi tiết"
            >
              <span className="material-symbols-rounded">
                {capturing ? "hourglass_empty" : "photo_camera"}
              </span>
            </button>
          </div>
        </header>

        <div
          ref={scrollRef}
          className="min-h-0 flex-1 space-y-6 overflow-y-auto bg-surface-0 px-6 py-5"
        >
          {/* ============ KPI cards ============ */}
          <section className={`grid grid-cols-2 gap-3 ${showAds ? "md:grid-cols-4" : "md:grid-cols-3"}`}>
            <KpiCard
              icon="trending_up"
              label={showAds ? "Lợi nhuận" : "Hoa hồng ròng"}
              value={fmtVnd(product.profit)}
              tone={profitTone}
            />
            {showAds && (
              <KpiCard
                icon="percent"
                label="ROI"
                value={roi !== null ? fmtPct(roi) : "—"}
                tone={roi === null ? "muted" : roi > 0 ? "positive" : roi < 0 ? "negative" : "neutral"}
              />
            )}
            <KpiCard
              icon="shopping_bag"
              label="Số đơn"
              value={fmtInt(product.ordersCount)}
              sub={fmtVnd(product.commissionTotal) + " hoa hồng"}
            />
            {showAds ? (
              <KpiCard
                icon="payments"
                label="Tiền ads"
                value={fmtVnd(product.totalSpend)}
                sub={
                  product.adsClicks > 0
                    ? `${fmtInt(product.adsClicks)} click · CPC ${cpc !== null ? fmtVnd(cpc) : "—"}`
                    : undefined
                }
                tone="muted"
              />
            ) : (
              <KpiCard
                icon="shopping_cart"
                label="Click Shopee"
                value={fmtInt(product.shopeeClicks)}
                tone="muted"
              />
            )}
          </section>

          {/* ============ Chỉ số tổng hợp ============ */}
          <Section icon="query_stats" title="Chỉ số tổng hợp">
            <div className="grid grid-cols-2 gap-x-8 gap-y-2 rounded-xl bg-surface-2 px-5 py-4 shadow-elev-1 md:grid-cols-3">
              {showAds && (
                <MetricRow
                  label="Click ADS"
                  value={fmtInt(product.adsClicks)}
                  tooltip="Tổng click quảng cáo FB qua tất cả ngày"
                />
              )}
              <MetricRow
                label="Click Shopee"
                value={fmtInt(product.shopeeClicks)}
                tooltip="Tổng click affiliate qua tất cả ngày"
              />
              {showAds && (
                <MetricRow
                  label="CPC TB"
                  value={cpc !== null ? fmtVnd(cpc) : "—"}
                  tooltip="CPC = Tổng tiền chạy / Tổng Click ADS"
                />
              )}
              <MetricRow
                label="GMV"
                value={fmtVnd(product.orderValueTotal)}
                tooltip="Tổng Giá trị đơn hàng"
              />
              <MetricRow
                label="GMV TB/đơn"
                value={avgOrderValue !== null ? fmtVnd(avgOrderValue) : "—"}
                tooltip="GMV / Số đơn"
              />
              <MetricRow
                label="Hoa hồng TB/đơn"
                value={avgCommission !== null ? fmtVnd(avgCommission) : "—"}
                tooltip="Hoa hồng / Số đơn"
              />
              <MetricRow
                label="Tỷ lệ chuyển đổi"
                value={cr !== null ? fmtPct(cr) : "—"}
                tooltip="CR = Số đơn / Click Shopee × 100%"
              />
              <MetricRow
                label="Hoa hồng gross"
                value={fmtVnd(product.commissionTotal)}
              />
              <MetricRow
                label="Hoa hồng ròng"
                value={fmtVnd(product.netCommission)}
                tooltip="Gross × (1 − thuế − dự phòng)"
              />
            </div>
          </Section>

          {/* ============ Tất cả đơn hàng ============ */}
          <Section
            icon="shopping_cart"
            title="Tất cả đơn hàng"
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
                  Chưa có order nào cho sản phẩm này trong khoảng + filter hiện tại.
                </span>
              </div>
            ) : (
              <div className="overflow-hidden rounded-xl border border-surface-8 bg-surface-2 shadow-elev-1">
                <div>
                  <table className="w-full border-collapse text-xs">
                    <thead>
                      <tr className="bg-surface-4 text-shopee-200">
                        <th className="border-b border-surface-8 px-3 py-2.5 text-center font-semibold uppercase tracking-wider">
                          Ngày
                        </th>
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
                        const duration = fmtDuration(it.clickTime, it.orderTime);
                        const statusCls = statusColor(it.orderStatus);
                        const tooltipName = it.itemName ?? "";
                        const shopAndName = it.shopName
                          ? `${it.shopName} · ${tooltipName}`
                          : tooltipName;
                        const orderDate = it.orderTime
                          ? it.orderTime.slice(0, 10)
                          : "";
                        return (
                          <tr
                            key={`${it.orderId}-${it.itemId}-${it.modelId}-${idx}`}
                            className="border-b border-surface-8 last:border-b-0 text-white/85 transition-colors hover:bg-shopee-500/15"
                          >
                            <td
                              className="whitespace-nowrap px-3 py-2 text-center tabular-nums text-white/70"
                              title={it.orderTime ?? undefined}
                            >
                              {orderDate ? fmtDate(orderDate) : "—"}
                            </td>
                            <td
                              className="max-w-[300px] truncate px-3 py-2"
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
                              className={`max-w-[140px] truncate whitespace-nowrap px-3 py-2 text-center ${statusCls}`}
                              title={it.orderStatus ?? ""}
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
                              {it.orderValue !== null ? fmtVnd(it.orderValue) : "—"}
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

          {/* ============ Sub_id identity ============ */}
          <Section icon="tag" title="Định danh Sub_id">
            <div className="grid grid-cols-2 gap-2 rounded-xl bg-surface-2 px-5 py-4 shadow-elev-1 md:grid-cols-5">
              {product.subIds.map((s, i) => (
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

        <footer className="capture-hide flex shrink-0 justify-end gap-2 border-t border-surface-8 bg-surface-1 px-6 py-3">
          <button
            type="button"
            onClick={onClose}
            className="btn-ripple rounded-lg bg-shopee-500 px-6 py-2 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4"
          >
            Đóng
          </button>
        </footer>

        <ScrollToTopButton
          targetRef={scrollRef}
          className="absolute bottom-20 right-6"
        />
      </div>

      <DayScreenshotDialog
        isOpen={!!screenshotBlob}
        blob={screenshotBlob}
        date={perDayMatches[0] ?? ""}
        dateLabel={perDayMatches[0] ? fmtDate(perDayMatches[0]) : ""}
        title={`Chi tiết tổng hợp — ${product.displayName || "(chưa đặt tên)"}`}
        defaultFileName={`thongkee-tong-hop-${product.subIds[0] || "product"}.png`}
        onClose={() => setScreenshotBlob(null)}
      />
    </div>,
    document.body,
  );
}

// =========================================================
// Atoms
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

function statusColor(s: string | null | undefined): string {
  if (!s) return "text-white/60";
  if (/hủy|cancel/i.test(s)) return "text-red-400";
  if (/hoàn thành|completed/i.test(s)) return "text-green-400";
  if (/chờ|pending/i.test(s)) return "text-amber-400";
  return "text-white/80";
}

function KpiCard({
  icon,
  label,
  value,
  sub,
  tone = "neutral",
}: {
  icon: string;
  label: string;
  value: string;
  sub?: string;
  tone?: "positive" | "negative" | "neutral" | "muted";
}) {
  const toneMap: Record<string, string> = {
    positive: "text-green-400",
    negative: "text-red-400",
    neutral: "text-white",
    muted: "text-white/70",
  };
  return (
    <div className="rounded-xl bg-surface-4 p-4 shadow-elev-2 transition-shadow hover:shadow-elev-4">
      <div className="flex items-center gap-1.5">
        <span className="material-symbols-rounded text-base text-white/40">
          {icon}
        </span>
        <p className="text-[11px] font-medium uppercase tracking-wider text-white/55">
          {label}
        </p>
      </div>
      <p
        className={`num-glow mt-1.5 truncate text-2xl font-bold tabular-nums ${toneMap[tone]}`}
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
    <div className="detail-row -mx-2 flex items-baseline justify-between gap-2 rounded-md border-b border-dashed border-surface-6 px-2 py-1 last:border-b-0">
      <span
        className={`detail-row-label text-sm text-white/60 ${tooltip ? "cursor-help" : ""}`}
        title={tooltip}
      >
        {label}
      </span>
      <span className="detail-row-value text-base font-bold tabular-nums text-white/95">
        {value}
      </span>
    </div>
  );
}

