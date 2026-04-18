import { useEffect } from "react";
import { createPortal } from "react-dom";
import type { Video } from "../types";
import {
  computeOrderStats,
  computeVideo,
  fmtDuration,
  fmtInt,
  fmtPct,
  fmtVnd,
} from "../formulas";
import { sumFiltered, useSettings } from "../hooks/useSettings";

interface ProductDetailDialogProps {
  isOpen: boolean;
  video: Video | null;
  onClose: () => void;
}

export function ProductDetailDialog({
  isOpen,
  video,
  onClose,
}: ProductDetailDialogProps) {
  const { settings } = useSettings();

  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  if (!isOpen || !video) return null;

  const computed = computeVideo(video, settings.profitFees);
  const orderStats = computeOrderStats(video.orderDetails);
  const feeAmount = video.commission - computed.netCommission;
  const shopeeClicks = sumFiltered(
    video.shopeeClicksByReferrer,
    settings.clickSources,
  );
  const profitCls =
    computed.profit > 0
      ? "text-green-400"
      : computed.profit < 0
      ? "text-red-400"
      : "text-gray-300";

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  const validOrders = orderStats
    ? orderStats.total - orderStats.cancelled - orderStats.zeroValue
    : 0;
  const avgValidValue =
    orderStats && validOrders > 0
      ? orderStats.totalGmv / validOrders
      : 0;

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        className="w-full max-w-3xl overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
        role="dialog"
        aria-modal="true"
        aria-labelledby="product-detail-dialog-title"
      >
        <header className="flex items-center gap-3 border-b border-surface-8 px-6 py-4">
          <span className="material-symbols-rounded text-shopee-400">
            inventory_2
          </span>
          <div className="flex-1">
            <p className="text-[11px] font-medium uppercase tracking-wider text-white/50">
              Chi tiết sản phẩm
            </p>
            <h2
              id="product-detail-dialog-title"
              className="text-lg font-semibold text-white/90"
            >
              {video.name || "(chưa đặt tên)"}
            </h2>
          </div>
        </header>

        <div className="max-h-[70vh] space-y-5 overflow-y-auto px-5 py-4">
          <section>
            <h3 className="mb-2 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              Chỉ số quảng cáo
            </h3>
            <dl className="grid grid-cols-2 gap-x-4 gap-y-2 rounded-xl bg-surface-6 px-4 py-3 text-sm shadow-elev-1">
              <dt className="text-white/55">Click FB:</dt>
              <dd className="text-right tabular-nums text-white/90">
                {fmtInt(video.clicks)}
              </dd>
              <dt className="text-white/55">Click Shopee:</dt>
              <dd className="text-right tabular-nums text-white/90">
                {fmtInt(shopeeClicks)}
              </dd>
              <dt className="text-white/55">Tổng tiền chạy:</dt>
              <dd className="text-right tabular-nums text-white/90">
                {fmtVnd(video.totalSpend)}
              </dd>
              <dt className="text-white/55">Đơn giá click (CPC):</dt>
              <dd className="text-right tabular-nums text-white/90">
                {fmtVnd(computed.cpc)}
              </dd>
              <dt className="text-white/55">Tỷ lệ chuyển đổi:</dt>
              <dd className="text-right tabular-nums text-white/90">
                {fmtPct(computed.conversionRate)}
              </dd>
            </dl>
          </section>

          <section>
            <h3 className="mb-2 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              Chi tiết đơn
            </h3>
            {orderStats ? (
              <dl className="grid grid-cols-2 gap-x-4 gap-y-2 rounded-xl bg-surface-6 px-4 py-3 text-sm shadow-elev-1">
                <dt className="text-white/55">Tổng đơn:</dt>
                <dd className="text-right tabular-nums text-white/90">
                  {fmtInt(orderStats.total)}
                </dd>
                <dt className="text-white/55">Đơn hủy:</dt>
                <dd className="text-right tabular-nums text-red-400">
                  {fmtInt(orderStats.cancelled)}
                </dd>
                <dt className="text-white/55">Đơn 0đ (không hoa hồng):</dt>
                <dd className="text-right tabular-nums text-amber-400">
                  {fmtInt(orderStats.zeroValue)}
                </dd>
                <dt className="text-white/55">Tổng GMV:</dt>
                <dd className="text-right tabular-nums text-white/90">
                  {fmtVnd(orderStats.totalGmv)}
                </dd>
                <dt className="text-white/55">Giá trị đơn TB (tất cả):</dt>
                <dd className="text-right tabular-nums text-white/90">
                  {fmtVnd(orderStats.averageValue)}
                </dd>
                <dt className="text-white/55">
                  Giá trị đơn TB (loại hủy &amp; 0đ):
                </dt>
                <dd className="text-right tabular-nums text-white/90">
                  {fmtVnd(avgValidValue)}
                </dd>
              </dl>
            ) : (
              <div className="flex items-center gap-3 rounded-xl border border-dashed border-surface-12 bg-surface-2 px-4 py-4 text-sm text-white/50">
                <span className="material-symbols-rounded text-base">info</span>
                <div>
                  Sản phẩm này nhập tay, chưa có chi tiết đơn từ import.
                  <br />
                  <span className="text-xs">
                    Tổng đơn: {fmtInt(video.orders)} | Hoa hồng:{" "}
                    {fmtVnd(video.commission)}
                  </span>
                </div>
              </div>
            )}
          </section>

          <section>
            <h3 className="mb-2 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              Lợi nhuận
            </h3>
            <dl className="grid grid-cols-2 gap-x-4 gap-y-2 rounded-xl bg-surface-6 px-4 py-3 text-sm shadow-elev-1">
              <dt className="text-white/55">Hoa hồng (gross):</dt>
              <dd className="text-right tabular-nums text-white/90">
                {fmtVnd(video.commission)}
              </dd>
              <dt className="text-white/55">
                Phí (thuế&sàn {settings.profitFees.taxAndPlatformRate}% + hoàn
                hủy {settings.profitFees.returnReserveRate}%):
              </dt>
              <dd className="text-right tabular-nums text-amber-400">
                − {fmtVnd(feeAmount)}
              </dd>
              <dt className="text-white/55">Hoa hồng sau phí:</dt>
              <dd className="text-right tabular-nums text-white/90">
                {fmtVnd(computed.netCommission)}
              </dd>
              <dt className="text-white/55">Tiền ads:</dt>
              <dd className="text-right tabular-nums text-white/90">
                − {fmtVnd(video.totalSpend)}
              </dd>
              <dt className="text-white/55 font-semibold">Lợi nhuận:</dt>
              <dd className={`text-right tabular-nums font-semibold ${profitCls}`}>
                {fmtVnd(computed.profit)}
              </dd>
              <dt className="text-white/55">Tỷ suất lợi nhuận:</dt>
              <dd className={`text-right tabular-nums ${profitCls}`}>
                {fmtPct(computed.profitMargin)}
              </dd>
            </dl>
          </section>

          {video.orderDetails && video.orderDetails.length > 0 && (
            <section>
              <h3 className="mb-2 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
                Danh sách đơn ({video.orderDetails.length})
              </h3>
              <div className="overflow-x-auto rounded-xl border border-surface-8 bg-surface-2">
                <table className="w-full border-collapse text-sm">
                  <thead>
                    <tr className="bg-surface-6 text-shopee-200">
                      <th className="border-b border-surface-8 px-2 py-2 text-center text-[11px] font-semibold uppercase tracking-wider">
                        ID đơn
                      </th>
                      <th className="border-b border-surface-8 px-2 py-2 text-center text-[11px] font-semibold uppercase tracking-wider">
                        Trạng thái
                      </th>
                      <th className="border-b border-surface-8 px-2 py-2 text-center text-[11px] font-semibold uppercase tracking-wider">
                        Click → Đặt
                      </th>
                      <th className="border-b border-surface-8 px-2 py-2 text-center text-[11px] font-semibold uppercase tracking-wider">
                        GMV
                      </th>
                      <th className="border-b border-surface-8 px-2 py-2 text-center text-[11px] font-semibold uppercase tracking-wider">
                        Hoa hồng
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {video.orderDetails.map((d) => {
                      const cancelled = /hủy|cancel/i.test(d.status);
                      const zero = d.commission === 0;
                      const rowCls = cancelled
                        ? "text-red-400"
                        : zero
                        ? "text-amber-400"
                        : "text-gray-200";
                      const duration = fmtDuration(d.clickTime, d.orderTime);
                      return (
                        <tr key={d.id} className={`${rowCls} border-b border-surface-8 last:border-b-0`}>
                          <td className="px-2 py-1.5 text-center font-mono text-xs">
                            {d.id}
                          </td>
                          <td className="px-2 py-1.5 text-center text-xs">
                            {d.status}
                          </td>
                          <td
                            className="px-2 py-1.5 text-center text-xs tabular-nums"
                            title={
                              d.clickTime && d.orderTime
                                ? `Click: ${d.clickTime}\nĐặt: ${d.orderTime}`
                                : undefined
                            }
                          >
                            {duration || "—"}
                          </td>
                          <td className="px-2 py-1.5 text-center tabular-nums">
                            {fmtVnd(d.grossValue)}
                          </td>
                          <td className="px-2 py-1.5 text-center tabular-nums">
                            {fmtVnd(d.commission)}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </section>
          )}
        </div>

        <footer className="flex justify-end border-t border-surface-8 bg-surface-1 px-6 py-3">
          <button
            type="button"
            onClick={onClose}
            className="btn-ripple rounded-lg bg-shopee-500 px-5 py-2 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4"
          >
            Đóng
          </button>
        </footer>
      </div>
    </div>,
    document.body,
  );
}
