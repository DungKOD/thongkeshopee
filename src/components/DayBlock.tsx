import { useRef, useState } from "react";
import type { UiDay, UiRow } from "../types";
import { computeUiDayTotals, fmtDate, fmtInt, fmtVnd, uiRowKey } from "../formulas";
import { useSettings } from "../hooks/useSettings";
import {
  captureElementToBlob,
  prefetchFontEmbedCSS,
} from "../lib/screenshot";
import { VideoRow } from "./VideoRow";
import { ProductDetailDialog } from "./ProductDetailDialog";
import { DayScreenshotDialog } from "./DayScreenshotDialog";

interface DayBlockProps {
  day: UiDay;
  pendingDayDeletes: ReadonlySet<string>;
  pendingRowDeletes: ReadonlyMap<string, unknown>;
  onToggleDayDelete: (date: string) => void;
  onToggleRowDelete: (row: UiRow) => void;
  onEditRow: (row: UiRow) => void;
  onEditDay: (date: string) => void;
  readOnly?: boolean;
}

const ROI_TOOLTIP =
  "ROI = (Hoa hồng sau phí − Tiền ads) / Tiền ads × 100%\n" +
  "• 0% = hòa vốn\n" +
  "• > 0% = có lãi\n" +
  "• < 0% = đang lỗ\n" +
  "VD: -50% nghĩa là lỗ một nửa số tiền đã chi.";

const HEADERS: Array<{ label: string; tooltip?: string }> = [
  { label: "#" },
  { label: "Sản phẩm" },
  { label: "Click ADS", tooltip: "Tổng click quảng cáo FB (link_clicks)" },
  {
    label: "Click Shopee",
    tooltip: "Số click affiliate về Shopee (lọc theo nguồn trong Cài đặt)",
  },
  {
    label: "Đơn giá click",
    tooltip:
      "CPC (Cost Per Click) = Tổng tiền chạy / Click ADS\n" +
      "Đơn giá mỗi lần ai đó bấm vào quảng cáo.\n" +
      "Ưu tiên lấy từ FB (weighted avg). Không có thì tính từ spend/click.",
  },
  { label: "Tổng tiền chạy", tooltip: "Spend FB (đã trừ ngân sách chưa tiêu)" },
  { label: "Số lượng đơn", tooltip: "Số đơn hàng (COUNT DISTINCT order_id)" },
  {
    label: "Tỷ lệ chuyển đổi",
    tooltip: "CR = Số đơn / Click Shopee × 100% (dùng click Shopee vì user vào Shopee mới có khả năng mua)",
  },
  {
    label: "Giá trị đơn hàng",
    tooltip: "GMV trung bình = Tổng GMV / Số đơn",
  },
  { label: "Hoa hồng", tooltip: "Tổng hoa hồng ròng (net commission)" },
  {
    label: "Lợi nhuận",
    tooltip: "Lợi nhuận = Hoa hồng × (1 − thuế − dự phòng) − Tiền ads",
  },
  { label: "ROI", tooltip: ROI_TOOLTIP },
  { label: "" },
];

export function DayBlock({
  day,
  pendingDayDeletes,
  pendingRowDeletes,
  onToggleDayDelete,
  onToggleRowDelete,
  onEditRow,
  onEditDay,
  readOnly = false,
}: DayBlockProps) {
  const [detailRow, setDetailRow] = useState<UiRow | null>(null);
  const [capturing, setCapturing] = useState(false);
  const [screenshotBlob, setScreenshotBlob] = useState<Blob | null>(null);
  const sectionRef = useRef<HTMLElement>(null);
  const { settings } = useSettings();

  const handleScreenshot = async () => {
    if (!sectionRef.current || capturing) return;
    setCapturing(true);
    try {
      const blob = await captureElementToBlob(sectionRef.current, {
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

  const handleScreenshotClose = () => {
    // Clear blob ref — trình duyệt GC giải phóng bộ nhớ ngay; dialog sẽ tự
    // revoke objectURL trong cleanup effect của nó.
    setScreenshotBlob(null);
  };

  const totals = computeUiDayTotals(
    day,
    settings.clickSources,
    settings.profitFees,
  );
  const totalsProfitCls =
    totals.profit > 0
      ? "text-green-400"
      : totals.profit < 0
      ? "text-red-400"
      : "text-gray-300";

  const dayPending = pendingDayDeletes.has(day.date);
  // "All rows pending" = user đã chọn xóa từng dòng cho đến hết → visual
  // giống như xóa cả ngày (gạch toàn bộ text + mờ data, giữ nút Undo rõ).
  const allRowsPending =
    !dayPending &&
    day.rows.length > 0 &&
    day.rows.every((r) =>
      pendingRowDeletes.has(uiRowKey(r.dayDate, r.subIds)),
    );
  const effectiveDayPending = dayPending || allRowsPending;

  return (
    <section
      ref={sectionRef}
      className={`mb-6 overflow-hidden rounded-xl shadow-elev-2 transition-shadow hover:shadow-elev-4 ${
        effectiveDayPending ? "bg-surface-2/60" : "bg-surface-2"
      } ${capturing ? "capture-mode" : ""}`}
    >
      <header className="flex items-center justify-between border-b border-surface-8 px-5 py-3">
        <div className="flex items-center gap-3">
          <span className="material-symbols-rounded text-shopee-400">
            event
          </span>
          <div
            className={`flex flex-col ${
              effectiveDayPending ? "line-through" : ""
            }`}
          >
            <span className="text-[11px] font-medium uppercase tracking-wider text-white/50">
              Ngày
            </span>
            <span className="text-base font-semibold tabular-nums text-white/90">
              {fmtDate(day.date)}
            </span>
          </div>
          <div
            className={`ml-4 inline-flex items-center gap-1 rounded-full bg-shopee-900/40 px-3 py-1 text-xs font-medium text-shopee-300 ${
              effectiveDayPending ? "line-through" : ""
            }`}
          >
            <span className="material-symbols-rounded text-sm">
              inventory_2
            </span>
            {day.rows.length} dòng
          </div>
          {effectiveDayPending && (
            <span className="rounded-full bg-amber-500/20 px-2 py-0.5 text-xs font-medium text-amber-300">
              {dayPending ? "Chờ xóa cả ngày" : "Chờ xóa toàn bộ dòng"}
            </span>
          )}
        </div>
        <div className="capture-hide flex items-center gap-1">
          <button
            onClick={handleScreenshot}
            onMouseEnter={prefetchFontEmbedCSS}
            onFocus={prefetchFontEmbedCSS}
            disabled={capturing || day.rows.length === 0}
            className="btn-ripple flex h-9 w-9 items-center justify-center rounded-full text-white/60 hover:bg-shopee-500/10 hover:text-shopee-400 disabled:cursor-not-allowed disabled:opacity-40"
            title={
              day.rows.length === 0
                ? "Ngày chưa có dòng"
                : capturing
                ? "Đang chụp..."
                : "Chụp ảnh ngày"
            }
            aria-label="Chụp ảnh ngày"
          >
            <span className="material-symbols-rounded">
              {capturing ? "hourglass_empty" : "photo_camera"}
            </span>
          </button>
          {!readOnly && (
            <button
              onClick={() => onToggleDayDelete(day.date)}
              className={`btn-ripple flex h-9 w-9 items-center justify-center rounded-full ${
                dayPending
                  ? "text-amber-400 hover:bg-amber-500/10"
                  : "text-white/60 hover:bg-red-500/10 hover:text-red-400"
              }`}
              title={dayPending ? "Khôi phục ngày" : "Đánh dấu xóa ngày"}
              aria-label={dayPending ? "Khôi phục ngày" : "Đánh dấu xóa ngày"}
            >
              <span className="material-symbols-rounded">
                {dayPending ? "undo" : "delete"}
              </span>
            </button>
          )}
        </div>
      </header>

      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr className="border-b-2 border-shopee-500/50 bg-gradient-to-b from-shopee-900/35 to-shopee-900/15 text-shopee-100">
              {HEADERS.map((h, i) => {
                const isIndex = i === 0;
                const isProduct = i === 1;
                const isAction = i === HEADERS.length - 1;
                const alignCls = isIndex
                  ? "w-12 px-2 text-center"
                  : isProduct
                  ? "min-w-[220px] px-4 text-left"
                  : "px-3 text-center";
                return (
                  <th
                    key={i}
                    title={h.tooltip}
                    className={`py-3.5 text-xs font-bold uppercase tracking-wider whitespace-nowrap ${alignCls} ${
                      h.tooltip ? "cursor-help" : ""
                    } ${isAction ? "col-actions" : ""}`}
                  >
                    <span className="inline-flex items-center gap-1">
                      <span>{h.label}</span>
                      {h.tooltip && (
                        <span className="text-[11px] leading-none text-shopee-300/60">
                          ⓘ
                        </span>
                      )}
                    </span>
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {day.rows.length === 0 ? (
              <tr>
                <td
                  colSpan={HEADERS.length}
                  className="border border-gray-700 px-4 py-6 text-center text-sm text-gray-500"
                >
                  Chưa có dòng — bấm "+ Thêm dòng" hoặc import CSV
                </td>
              </tr>
            ) : (
              day.rows.map((r, i) => {
                const key = uiRowKey(r.dayDate, r.subIds);
                return (
                  <VideoRow
                    key={key}
                    index={i + 1}
                    row={r}
                    pending={effectiveDayPending || pendingRowDeletes.has(key)}
                    onEdit={() => onEditRow(r)}
                    onToggleDelete={() => {
                      // Nếu day đang pending (xóa cả ngày) mà user undo 1 row:
                      // convert sang per-row pending — bỏ day, add các row khác vào.
                      if (dayPending) {
                        onToggleDayDelete(day.date);
                        for (const other of day.rows) {
                          if (uiRowKey(other.dayDate, other.subIds) !== key) {
                            onToggleRowDelete(other);
                          }
                        }
                      } else {
                        onToggleRowDelete(r);
                      }
                    }}
                    onViewDetail={() => setDetailRow(r)}
                    readOnly={readOnly}
                  />
                );
              })
            )}
          </tbody>
          {day.rows.length > 0 && (
            <tfoot>
              <tr className="border-t-2 border-shopee-500 bg-shopee-900/25 text-base font-bold text-white">
                <td />
                <td className="px-4 py-4 text-left text-sm uppercase tracking-wider text-shopee-300">
                  Tổng
                </td>
                <td className="px-3 py-4 text-center tabular-nums">
                  {fmtInt(totals.clicks)}
                </td>
                <td className="px-3 py-4 text-center tabular-nums">
                  {fmtInt(totals.shopeeClicks)}
                </td>
                <td />
                <td className="px-3 py-4 text-center tabular-nums">
                  {fmtVnd(totals.totalSpend)}
                </td>
                <td className="px-3 py-4 text-center tabular-nums">
                  {fmtInt(totals.orders)}
                </td>
                <td />
                <td />
                <td className="px-3 py-4 text-center tabular-nums">
                  {fmtVnd(totals.commission)}
                </td>
                <td
                  className={`px-3 py-4 text-center tabular-nums ${totalsProfitCls}`}
                >
                  {fmtVnd(totals.profit)}
                </td>
                <td />
                <td className="col-actions" />
              </tr>
            </tfoot>
          )}
        </table>
      </div>

      {!readOnly && (
        <div className="capture-hide border-t border-surface-8 bg-surface-1 px-5 py-2">
          <button
            onClick={() => {
              if (!effectiveDayPending) onEditDay(day.date);
            }}
            disabled={effectiveDayPending}
            className={`btn-ripple flex items-center gap-2 rounded-lg px-3 py-2 text-sm font-medium ${
              effectiveDayPending
                ? "cursor-not-allowed text-white/20"
                : "text-shopee-400 hover:bg-shopee-500/10 active:bg-shopee-500/20"
            }`}
            title={
              effectiveDayPending ? "Ngày đã đánh dấu xóa — bỏ để thêm dòng" : ""
            }
          >
            <span className="material-symbols-rounded text-base">add</span>
            Thêm dòng
          </button>
        </div>
      )}

      <ProductDetailDialog
        isOpen={!!detailRow}
        row={detailRow}
        onClose={() => setDetailRow(null)}
      />

      <DayScreenshotDialog
        isOpen={!!screenshotBlob}
        blob={screenshotBlob}
        date={day.date}
        dateLabel={fmtDate(day.date)}
        onClose={handleScreenshotClose}
      />
    </section>
  );
}
