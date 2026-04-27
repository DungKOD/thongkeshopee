import { useEffect, useMemo, useRef, useState } from "react";
import type { UiDay, UiRow } from "../types";
import {
  buildDayTsv,
  computeUiDayTotals,
  computeUiRow,
  fmtDate,
  fmtInt,
  fmtPct,
  fmtVnd,
  uiRowKey,
} from "../formulas";
import { useSettings, sumFiltered } from "../hooks/useSettings";
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
  /** Account filter từ App — pass-through cho ProductDetailDialog. */
  accountFilter?: import("../hooks/useDbStats").AccountFilterMode;
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
  {
    label: "TK Shopee",
    tooltip:
      "Tài khoản Shopee mà dòng này thuộc về. 'FB chung' = quảng cáo FB " +
      "có ≥2 TK Shopee cùng tuple sub_id trong ngày → không quy về 1 TK duy nhất.",
  },
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
  accountFilter,
}: DayBlockProps) {
  const [detailRow, setDetailRow] = useState<UiRow | null>(null);
  const [capturing, setCapturing] = useState(false);
  const [screenshotBlob, setScreenshotBlob] = useState<Blob | null>(null);
  const sectionRef = useRef<HTMLElement>(null);
  const { settings } = useSettings();

  // Cột "TK Shopee" chỉ render khi user đang xem tất cả account (filter All).
  // Khi filter 1 acc cụ thể, mọi row đều cùng acc → cột trùng lặp, bỏ.
  const showAccount = !accountFilter || accountFilter.kind === "all";
  const headers = useMemo(
    () => (showAccount ? HEADERS : HEADERS.filter((h) => h.label !== "TK Shopee")),
    [showAccount],
  );

  // Tuple đa-account: same canonical sub_ids xuất hiện trong ≥2 row khác
  // accountId. Khi user xóa 1 row trong số này, BE batch_commit_deletes wipe
  // hết data cùng tuple/day cho mọi acc (delete_prefix_compatible không filter
  // shopee_account_id) → rủi ro mất data acc khác. Chặn nút delete + tooltip
  // hướng dẫn switch filter trước. Set chứa subIds.join key cho lookup O(1).
  const multiAccountTuples = useMemo(() => {
    const groups = new Map<string, Set<string | null>>();
    for (const r of day.rows) {
      const k = r.subIds.join("\x1f");
      let s = groups.get(k);
      if (!s) {
        s = new Set();
        groups.set(k, s);
      }
      s.add(r.accountId);
    }
    const multi = new Set<string>();
    for (const [k, s] of groups) if (s.size >= 2) multi.add(k);
    return multi;
  }, [day.rows]);
  const hasAnyMultiAccount = multiAccountTuples.size > 0;

  // Copy TSV → clipboard. Hiện icon "done" 1.5s rồi revert.
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    if (!copied) return;
    const t = setTimeout(() => setCopied(false), 1500);
    return () => clearTimeout(t);
  }, [copied]);

  const handleCopy = async () => {
    const tsv = buildDayTsv(day, settings.clickSources, settings.profitFees);
    try {
      await navigator.clipboard.writeText(tsv);
      setCopied(true);
    } catch (e) {
      console.error("clipboard write failed", e);
      alert(
        "Copy thất bại — browser có thể đã chặn clipboard. Thử refresh app.",
      );
    }
  };

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

  // Sort rows theo Lợi nhuận giảm dần (cao → thấp). Profit phụ thuộc fees
  // (tax/reserve) nên phải compute FE-side, Rust sort fallback theo tên
  // (query.rs) — ở đây override.
  const sortedRows = useMemo(() => {
    return [...day.rows].sort((a, b) => {
      const shopeeA = sumFiltered(a.shopeeClicksByReferrer, settings.clickSources);
      const shopeeB = sumFiltered(b.shopeeClicksByReferrer, settings.clickSources);
      const profitA = computeUiRow(a, settings.profitFees, shopeeA).profit;
      const profitB = computeUiRow(b, settings.profitFees, shopeeB).profit;
      return profitB - profitA;
    });
  }, [day.rows, settings.clickSources, settings.profitFees]);
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
      pendingRowDeletes.has(uiRowKey(r.dayDate, r.subIds, r.accountId)),
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
            onClick={handleCopy}
            disabled={day.rows.length === 0}
            className={`btn-ripple flex h-9 w-9 items-center justify-center rounded-full disabled:cursor-not-allowed disabled:opacity-40 ${
              copied
                ? "text-green-400"
                : "text-white/60 hover:bg-shopee-500/10 hover:text-shopee-400"
            }`}
            title={
              day.rows.length === 0
                ? "Ngày chưa có dòng"
                : copied
                ? "Đã copy (TSV — paste vào Google Sheets/Excel)"
                : "Copy bảng (TSV)"
            }
            aria-label="Copy bảng"
          >
            <span className="material-symbols-rounded">
              {copied ? "done" : "content_copy"}
            </span>
          </button>
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

      {showAccount && hasAnyMultiAccount && !readOnly && (
        <div
          className="flex items-center gap-2 border-b border-surface-8 bg-blue-500/5 px-5 py-2 text-xs text-blue-200/80"
          title="Khi 1 sub_id có data của ≥2 TK Shopee, nút xóa hàng bị khóa vì BE hiện xóa theo tuple — sẽ wipe data của tất cả TK. Chuyển dropdown sang TK cụ thể rồi mới xóa."
        >
          <span className="material-symbols-rounded text-sm text-blue-300/70">info</span>
          <span>
            Có sub_id chia sẻ giữa nhiều TK — nút xóa hàng đã khóa cho các dòng đó.
            Chuyển dropdown TK sang TK cụ thể nếu cần xóa.
          </span>
        </div>
      )}
      {day.totals.mcnFeeTotal > 0 && (
        <div
          className="flex items-center gap-2 border-b border-surface-8 bg-amber-500/5 px-5 py-2 text-xs text-amber-200/80"
          title="Shopee đã cắt phí MCN này trước khi payout. Số hoa hồng hiển thị là NET (đã trừ phí MCN)."
        >
          <span className="material-symbols-rounded text-sm text-amber-300/70">info</span>
          <span>
            Phí quản lý MCN đã trừ:{" "}
            <b className="tabular-nums text-amber-200">
              {fmtVnd(day.totals.mcnFeeTotal)}
            </b>
          </span>
        </div>
      )}
      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr className="border-b-2 border-shopee-500/50 bg-gradient-to-b from-shopee-900/35 to-shopee-900/15 text-shopee-100">
              {headers.map((h, i) => {
                const isIndex = i === 0;
                const isProduct = i === 1;
                const isAction = i === headers.length - 1;
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
                  colSpan={headers.length}
                  className="border border-gray-700 px-4 py-6 text-center text-sm text-gray-500"
                >
                  Chưa có dòng — bấm "+ Thêm dòng" hoặc import CSV
                </td>
              </tr>
            ) : (
              sortedRows.map((r, i) => {
                const key = uiRowKey(r.dayDate, r.subIds, r.accountId);
                const tupleKey = r.subIds.join("\x1f");
                const deleteBlocked =
                  showAccount && multiAccountTuples.has(tupleKey);
                return (
                  <VideoRow
                    key={key}
                    index={i + 1}
                    row={r}
                    showAccount={showAccount}
                    pending={effectiveDayPending || pendingRowDeletes.has(key)}
                    deleteBlocked={deleteBlocked}
                    onEdit={() => onEditRow(r)}
                    onToggleDelete={() => {
                      // Nếu day đang pending (xóa cả ngày) mà user undo 1 row:
                      // convert sang per-row pending — bỏ day, add các row khác vào.
                      if (dayPending) {
                        onToggleDayDelete(day.date);
                        for (const other of day.rows) {
                          if (uiRowKey(other.dayDate, other.subIds, other.accountId) !== key) {
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
                {showAccount && <td />}
                <td className="px-3 py-4 text-center tabular-nums">
                  {fmtInt(totals.clicks)}
                </td>
                <td className="px-3 py-4 text-center tabular-nums">
                  {fmtInt(totals.shopeeClicks)}
                </td>
                <td />
                <td className="px-3 py-4 text-center tabular-nums text-blue-400">
                  {fmtVnd(totals.totalSpend)}
                </td>
                <td className="px-3 py-4 text-center tabular-nums">
                  {fmtInt(totals.orders)}
                </td>
                <td
                  className="px-3 py-4 text-center tabular-nums"
                  title={
                    totals.shopeeClicks === 0
                      ? "Không có Click Shopee → không tính được CR"
                      : `CR TB = Σ Số đơn / Σ Click Shopee × 100% (${totals.orders}/${totals.shopeeClicks})`
                  }
                >
                  {totals.shopeeClicks > 0
                    ? fmtPct((totals.orders / totals.shopeeClicks) * 100)
                    : "—"}
                </td>
                <td
                  className="px-3 py-4 text-center tabular-nums"
                  title="GMV TB = Σ Giá trị đơn hàng / Σ Số đơn"
                >
                  {totals.orders > 0
                    ? fmtVnd(day.totals.orderValueTotal / totals.orders)
                    : "—"}
                </td>
                <td className="px-3 py-4 text-center tabular-nums text-shopee-400">
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
        accountFilter={accountFilter}
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
