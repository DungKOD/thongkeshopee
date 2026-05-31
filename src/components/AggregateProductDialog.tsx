import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  Bar,
  BarChart,
  CartesianGrid,
  LabelList,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { OrderItemDetail, UiDay } from "../types";
import {
  fmtDate,
  fmtDuration,
  fmtInt,
  fmtPct,
  fmtVnd,
  toneIconClass,
  toneTextClass,
  type AggregatedProductRow,
  type SourceFilter,
  type Tone,
} from "../formulas";
import { useSettings } from "../hooks/useSettings";
import { invoke } from "../lib/tauri";
import {
  captureElementToBlob,
  prefetchFontEmbedCSS,
} from "../lib/screenshot";
import { DayScreenshotDialog } from "./DayScreenshotDialog";
import { ScrollToTopButton } from "./ScrollToTopButton";
import { ProductClickInsights } from "./ProductClickInsights";
import type { AccountFilterMode } from "../hooks/useDbStats";

interface AggregateProductDialogProps {
  isOpen: boolean;
  product: AggregatedProductRow | null;
  /** Days đã load ở tab Overview — dialog tự filter rows match subIds. */
  days: UiDay[];
  source: SourceFilter;
  /** Account filter từ parent — dùng cho BE click insights queries. */
  accountFilter?: AccountFilterMode;
  onClose: () => void;
}

export function AggregateProductDialog({
  isOpen,
  product,
  days,
  source,
  accountFilter,
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

  // Aggregate stats ở MỨC ĐƠN (distinct order_id), KHÔNG phải line-item.
  // 1 đơn có nhiều line → trước đây 14 hủy / 469 không HH cộng nhiều lần per
  // line; giờ collapse về order: hủy = có ≥1 line hủy, không-HH = sum(net) = 0.
  const orderStats = useMemo(() => {
    const perOrder = new Map<string, { cancelled: boolean; netSum: number }>();
    for (const it of items) {
      const cur = perOrder.get(it.orderId) ?? { cancelled: false, netSum: 0 };
      if (/hủy|cancel/i.test(it.orderStatus ?? "")) cur.cancelled = true;
      cur.netSum += it.netCommission ?? 0;
      perOrder.set(it.orderId, cur);
    }
    let cancelledOrders = 0;
    let zeroCommissionOrders = 0;
    let attributedOrders = 0;
    for (const v of perOrder.values()) {
      if (v.cancelled) cancelledOrders += 1;
      if (v.netSum === 0) zeroCommissionOrders += 1;
      else attributedOrders += 1;
    }
    return {
      totalOrders: perOrder.size,
      cancelledOrders,
      zeroCommissionOrders,
      attributedOrders,
    };
  }, [items]);

  // Daily breakdown cho chart: mỗi bar = 1 ngày, stack {attributed, zeroHH,
  // cancelled}. Phân loại đơn theo orderTime date (không phải day_date input
  // — đơn có thể click ngày X, đặt ngày Y; chart dùng ngày ĐẶT đơn).
  // 3 nhóm exclusive: cancelled > zeroHH (non-cancelled) > attributed (HH>0).
  const dailyBreakdown = useMemo(() => {
    const perDate = new Map<
      string,
      Map<string, { cancelled: boolean; netSum: number }>
    >();
    for (const it of items) {
      const date = it.orderTime ? it.orderTime.slice(0, 10) : "";
      if (!date) continue;
      let orderMap = perDate.get(date);
      if (!orderMap) {
        orderMap = new Map();
        perDate.set(date, orderMap);
      }
      const cur = orderMap.get(it.orderId) ?? { cancelled: false, netSum: 0 };
      if (/hủy|cancel/i.test(it.orderStatus ?? "")) cur.cancelled = true;
      cur.netSum += it.netCommission ?? 0;
      orderMap.set(it.orderId, cur);
    }
    const out: {
      date: string;
      dateShort: string;
      attributed: number;
      zeroHH: number;
      cancelled: number;
    }[] = [];
    for (const [date, orderMap] of perDate) {
      let attributed = 0;
      let cancelled = 0;
      let zeroHH = 0;
      for (const v of orderMap.values()) {
        if (v.cancelled) cancelled += 1;
        else if (v.netSum === 0) zeroHH += 1;
        else attributed += 1;
      }
      const parts = date.split("-");
      const dateShort = parts.length === 3 ? `${parts[2]}/${parts[1]}` : date;
      out.push({ date, dateShort, attributed, zeroHH, cancelled });
    }
    out.sort((a, b) => a.date.localeCompare(b.date));
    return out;
  }, [items]);

  // Product insights filter — derive date range từ days prop. Days list
  // sorted DESC (newest first) → first=to, last=from. ProductClickInsights
  // fetch 4 BE aggregates (hourly clicks, hourly orders, delays, referrer)
  // scoped theo sub_ids của product.
  const insightsFilter = useMemo(() => {
    if (!product) return null;
    const fromDate = days.length > 0 ? days[days.length - 1].date : undefined;
    const toDate = days.length > 0 ? days[0].date : undefined;
    return {
      fromDate,
      toDate,
      accountFilter,
      subIds: product.subIds as unknown as [string, string, string, string, string],
    };
  }, [product, days, accountFilter]);

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
  const roasPct =
    product.totalSpend > 0
      ? (product.netCommission / product.totalSpend) * 100
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
          <div className="capture-hide flex shrink-0 items-center gap-1">
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
            <button
              type="button"
              onClick={onClose}
              className="btn-ripple flex h-10 w-10 items-center justify-center rounded-full text-white/80 hover:bg-white/15"
              title="Đóng (Esc)"
              aria-label="Đóng"
            >
              <span className="material-symbols-rounded">close</span>
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
              tone="commission"
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
                tone="spend"
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
              {showAds && (
                <MetricRow
                  label="ROI (HH/Ads)"
                  value={roasPct !== null ? fmtPct(roasPct) : "—"}
                  tooltip="ROI = Hoa hồng ròng / Tiền ads × 100%"
                />
              )}
            </div>
          </Section>

          {/* ============ Phân tích click Shopee (scope theo sub_ids) ============
              Chart "Đơn hàng theo trạng thái" được chèn qua slot beforeDelayChart
              → nằm NGAY TRÊN "Thời gian click → đặt hàng" (ClickDelayChart). */}
          {insightsFilter && (
            <ProductClickInsights
              filter={insightsFilter}
              beforeDelayChart={
                !loadingItems && dailyBreakdown.length > 0 ? (
                  <Section icon="bar_chart" title="Đơn hàng theo trạng thái">
                    <OrderStatusBreakdownChart data={dailyBreakdown} />
                  </Section>
                ) : null
              }
            />
          )}

          {/* ============ Tất cả đơn hàng ============ */}
          <Section
            icon="shopping_cart"
            title="Tất cả đơn hàng"
            count={loadingItems ? undefined : items.length}
            right={
              !loadingItems && items.length > 0 ? (
                <div className="flex gap-1.5 text-[11px]">
                  <Chip
                    tone="default"
                    title={`${fmtInt(orderStats.attributedOrders)} đơn có HH · ${fmtInt(
                      orderStats.zeroCommissionOrders,
                    )} đơn HH=0đ (gồm cả đơn hủy & đơn Shopee chưa attribute)`}
                  >
                    {orderStats.totalOrders} đơn
                  </Chip>
                  {orderStats.cancelledOrders > 0 && (
                    <Chip
                      tone="danger"
                      title="Đơn có ít nhất 1 line trạng thái 'Đã hủy' / 'Cancelled'"
                    >
                      {orderStats.cancelledOrders} hủy
                    </Chip>
                  )}
                  {orderStats.zeroCommissionOrders > 0 && (
                    <Chip
                      tone="warning"
                      title="Đơn có hoa hồng = 0đ. Nguyên nhân: đơn hủy, Shopee chưa attribute sub_id, hoặc bị phí MCN trừ hết. Bao gồm cả đơn hủy."
                    >
                      {orderStats.zeroCommissionOrders} không HH
                    </Chip>
                  )}
                </div>
              ) : null
            }
          >
            {loadingItems ? (
              <div className="rounded-xl border border-surface-8 bg-surface-2 px-4 py-4 text-sm text-white/50">
                <span className="material-symbols-rounded mr-2 animate-spin align-middle text-base">
                  sync
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
                        <th className="border-b border-surface-8 px-2 py-2.5 text-center font-semibold uppercase tracking-wider">
                          #
                        </th>
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
                        <th
                          className="cursor-help border-b border-surface-8 px-3 py-2.5 text-right font-semibold uppercase tracking-wider"
                          title="HH Shopee đã trả (raw CSV col 'Hoa hồng ròng tiếp thị liên kết', đã trừ phí MCN). Chưa trừ thuế & dự phòng — KPI 'Hoa hồng ròng' ở trên đã trừ tiếp."
                        >
                          HH Shopee trả
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
                            <td className="whitespace-nowrap px-2 py-2 text-center tabular-nums text-white/40">
                              {idx + 1}
                            </td>
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

        <ScrollToTopButton
          targetRef={scrollRef}
          className="absolute bottom-6 right-6"
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
  title,
  children,
}: {
  tone: "default" | "danger" | "warning";
  title?: string;
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
      title={title}
      className={`rounded-full border px-2 py-0.5 font-medium normal-case ${
        title ? "cursor-help" : ""
      } ${cls}`}
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
  tone?: Tone;
}) {
  const [hidden, setHidden] = useState(false);
  return (
    <div className="rounded-xl bg-surface-4 p-4 shadow-elev-2 transition-shadow hover:shadow-elev-4">
      <div className="flex items-center gap-1.5">
        <span className={`material-symbols-rounded text-base ${toneIconClass(tone)}`}>
          {icon}
        </span>
        <p className="flex-1 text-[11px] font-medium uppercase tracking-wider text-white/55">
          {label}
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
      <p
        className={`num-glow mt-1.5 truncate text-2xl font-bold tabular-nums ${toneTextClass(tone)}`}
        title={hidden ? undefined : value}
      >
        {hidden ? (
          <span className="select-none tracking-widest text-white/20">••••</span>
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

function MetricRow({
  label,
  value,
  tooltip,
}: {
  label: string;
  value: string;
  tooltip?: string;
}) {
  const [hidden, setHidden] = useState(false);
  return (
    <div className="detail-row -mx-2 flex items-center justify-between gap-2 rounded-md border-b border-dashed border-surface-6 px-2 py-1 last:border-b-0">
      <span
        className={`detail-row-label text-sm text-white/60 ${tooltip ? "cursor-help" : ""}`}
        title={tooltip}
      >
        {label}
      </span>
      <div className="flex items-center gap-1">
        <span className="detail-row-value text-base font-bold tabular-nums text-white/95">
          {hidden ? (
            <span className="select-none tracking-widest text-white/20">••••</span>
          ) : (
            value
          )}
        </span>
        <button
          onClick={() => setHidden((h) => !h)}
          className="flex h-5 w-5 shrink-0 items-center justify-center rounded text-white/25 hover:text-white/60"
          title={hidden ? "Hiện" : "Ẩn"}
          aria-label={hidden ? "Hiện" : "Ẩn"}
        >
          <span className="material-symbols-rounded text-[13px]">
            {hidden ? "visibility_off" : "visibility"}
          </span>
        </button>
      </div>
    </div>
  );
}

// =========================================================
// Chart: breakdown đơn theo trạng thái (stack theo ngày)
// =========================================================

type BreakdownPoint = {
  date: string;
  dateShort: string;
  attributed: number;
  zeroHH: number;
  cancelled: number;
};

const COLOR_ATTRIBUTED = "#22c55e"; // green — đơn có HH
const COLOR_ZERO_HH = "#f59e0b"; // amber — đơn HH=0 (chưa attribute / MCN trừ hết)
const COLOR_CANCELLED = "#ef4444"; // red — đơn hủy

type BreakdownChartPoint = BreakdownPoint & {
  total: number;
  badPct: number; // (zeroHH + cancelled) / total × 100
};

function OrderStatusBreakdownChart({ data }: { data: BreakdownPoint[] }) {
  // Augment với total + badPct cho label/tooltip.
  const points: BreakdownChartPoint[] = useMemo(
    () =>
      data.map((d) => {
        const total = d.attributed + d.zeroHH + d.cancelled;
        const bad = d.zeroHH + d.cancelled;
        const badPct = total > 0 ? (bad / total) * 100 : 0;
        return { ...d, total, badPct };
      }),
    [data],
  );

  // Totals + averages per day để hiển thị summary phía trên chart.
  // daysCount = số ngày có ít nhất 1 đơn (= data.length sau khi filter ở memo).
  const stats = useMemo(() => {
    let attributed = 0;
    let zeroHH = 0;
    let cancelled = 0;
    for (const d of data) {
      attributed += d.attributed;
      zeroHH += d.zeroHH;
      cancelled += d.cancelled;
    }
    const total = attributed + zeroHH + cancelled;
    const daysCount = data.length;
    const safeDiv = (a: number, b: number) => (b > 0 ? a / b : 0);
    return {
      attributed,
      zeroHH,
      cancelled,
      total,
      daysCount,
      avgAttributed: safeDiv(attributed, daysCount),
      avgZeroHH: safeDiv(zeroHH, daysCount),
      avgCancelled: safeDiv(cancelled, daysCount),
      avgTotal: safeDiv(total, daysCount),
    };
  }, [data]);

  const fmtPctTotal = (n: number) =>
    stats.total > 0 ? `${((n / stats.total) * 100).toFixed(1)}%` : "0%";
  const fmtAvg = (n: number) =>
    n >= 10 ? n.toFixed(0) : n.toFixed(1);

  return (
    <div className="rounded-xl bg-surface-2 px-4 py-4 shadow-elev-1">
      {/* Summary row — tổng + TB/ngày */}
      <div className="mb-3 flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px]">
        <SummaryDot
          color={COLOR_ATTRIBUTED}
          label="Có HH"
          value={stats.attributed}
          pct={fmtPctTotal(stats.attributed)}
          avg={fmtAvg(stats.avgAttributed)}
        />
        <SummaryDot
          color={COLOR_ZERO_HH}
          label="HH = 0đ"
          value={stats.zeroHH}
          pct={fmtPctTotal(stats.zeroHH)}
          avg={fmtAvg(stats.avgZeroHH)}
        />
        <SummaryDot
          color={COLOR_CANCELLED}
          label="Hủy"
          value={stats.cancelled}
          pct={fmtPctTotal(stats.cancelled)}
          avg={fmtAvg(stats.avgCancelled)}
        />
        <span className="text-white/55">
          ·{" "}
          <span className="font-semibold text-white/85">
            {fmtAvg(stats.avgTotal)} đơn/ngày
          </span>
          <span className="text-white/40"> (TB qua {stats.daysCount} ngày)</span>
        </span>
        <span className="ml-auto text-white/45">
          Label trên cột = % đơn xấu (hủy + HH=0đ)
        </span>
      </div>
      <div className="h-[240px] w-full">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={points} margin={{ top: 18, right: 10, left: 0, bottom: 0 }}>
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
              allowDecimals={false}
              width={40}
            />
            <Tooltip
              cursor={{ fill: "#ffffff08" }}
              content={<BreakdownTooltip avgTotal={stats.avgTotal} />}
            />
            <Legend
              wrapperStyle={{ fontSize: 12, paddingTop: 8 }}
              iconType="circle"
            />
            {stats.daysCount > 1 && stats.avgTotal > 0 && (
              <ReferenceLine
                y={stats.avgTotal}
                stroke="#ffffff60"
                strokeDasharray="4 4"
                strokeWidth={1.5}
                label={{
                  value: `TB ${fmtAvg(stats.avgTotal)}/ngày`,
                  position: "insideTopRight",
                  fill: "#ffffffb0",
                  fontSize: 10,
                  fontWeight: 600,
                }}
              />
            )}
            <Bar dataKey="attributed" stackId="a" name="Có HH" fill={COLOR_ATTRIBUTED} />
            <Bar dataKey="zeroHH" stackId="a" name="HH = 0đ" fill={COLOR_ZERO_HH} />
            <Bar
              dataKey="cancelled"
              stackId="a"
              name="Hủy"
              fill={COLOR_CANCELLED}
              radius={[4, 4, 0, 0]}
            >
              <LabelList
                dataKey="badPct"
                position="top"
                formatter={(v: unknown) => {
                  const n = Number(v);
                  return n > 0 ? `${n.toFixed(0)}%` : "";
                }}
                style={{ fill: "#ffffff90", fontSize: 10, fontWeight: 600 }}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

// Custom tooltip — hiển thị count + % cho mỗi segment + tổng + so sánh TB.
function BreakdownTooltip({
  active,
  payload,
  label,
  avgTotal,
}: {
  active?: boolean;
  payload?: Array<{ payload?: BreakdownChartPoint }>;
  label?: string;
  avgTotal?: number;
}) {
  if (!active || !payload || payload.length === 0) return null;
  const p = payload[0]?.payload;
  if (!p) return null;
  const total = p.total;
  const pct = (n: number) =>
    total > 0 ? `${((n / total) * 100).toFixed(1)}%` : "0%";

  // Diff vs TB — biết ngày này trên/dưới mức bình thường.
  let avgDiff: { sign: "up" | "down" | "eq"; text: string } | null = null;
  if (avgTotal !== undefined && avgTotal > 0) {
    const diffPct = ((total - avgTotal) / avgTotal) * 100;
    if (Math.abs(diffPct) < 0.5) avgDiff = { sign: "eq", text: "≈ TB" };
    else if (diffPct > 0)
      avgDiff = { sign: "up", text: `+${diffPct.toFixed(0)}% vs TB` };
    else avgDiff = { sign: "down", text: `${diffPct.toFixed(0)}% vs TB` };
  }

  return (
    <div className="rounded-lg border border-white/20 bg-[#1f1f23] px-3 py-2 text-xs shadow-lg">
      <div className="mb-1.5 font-semibold text-white/90">{label}</div>
      <div className="space-y-0.5">
        <TooltipRow color={COLOR_ATTRIBUTED} label="Có HH" value={p.attributed} pct={pct(p.attributed)} />
        <TooltipRow color={COLOR_ZERO_HH} label="HH = 0đ" value={p.zeroHH} pct={pct(p.zeroHH)} />
        <TooltipRow color={COLOR_CANCELLED} label="Hủy" value={p.cancelled} pct={pct(p.cancelled)} />
      </div>
      <div className="mt-1.5 flex items-center justify-between border-t border-white/10 pt-1.5">
        <span className="text-white/60">Tổng</span>
        <span className="flex items-center gap-2">
          <span className="font-semibold tabular-nums text-white/95">
            {fmtInt(total)} đơn
          </span>
          {avgDiff && (
            <span
              className={`text-[10px] tabular-nums ${
                avgDiff.sign === "up"
                  ? "text-green-400"
                  : avgDiff.sign === "down"
                  ? "text-amber-400"
                  : "text-white/50"
              }`}
            >
              {avgDiff.text}
            </span>
          )}
        </span>
      </div>
    </div>
  );
}

function TooltipRow({
  color,
  label,
  value,
  pct,
}: {
  color: string;
  label: string;
  value: number;
  pct: string;
}) {
  return (
    <div className="flex items-center gap-2">
      <span
        className="inline-block h-2 w-2 rounded-full"
        style={{ backgroundColor: color }}
      />
      <span className="flex-1 text-white/70">{label}</span>
      <span className="tabular-nums text-white/95">{fmtInt(value)}</span>
      <span className="w-12 text-right tabular-nums text-white/50">{pct}</span>
    </div>
  );
}

function SummaryDot({
  color,
  label,
  value,
  pct,
  avg,
}: {
  color: string;
  label: string;
  value: number;
  pct: string;
  avg?: string;
}) {
  return (
    <div className="flex items-center gap-1.5">
      <span
        className="inline-block h-2.5 w-2.5 rounded-full"
        style={{ backgroundColor: color }}
      />
      <span className="text-white/70">{label}:</span>
      <span className="font-semibold tabular-nums text-white/95">
        {fmtInt(value)}
      </span>
      <span className="text-white/45">({pct})</span>
      {avg !== undefined && (
        <span
          className="text-white/45"
          title="Trung bình mỗi ngày trong khoảng đã chọn"
        >
          · TB <span className="tabular-nums text-white/75">{avg}</span>/ngày
        </span>
      )}
    </div>
  );
}
