import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
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
import { Fragment } from "react";
import { VideoRow } from "./VideoRow";
import { FbHierarchyTree } from "./FbHierarchyTree";
import { ProductDetailDialog } from "./ProductDetailDialog";
import { ProductHistoryDialog } from "./ProductHistoryDialog";
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
  const [historyRow, setHistoryRow] = useState<UiRow | null>(null);
  const [capturing, setCapturing] = useState(false);
  const [screenshotBlob, setScreenshotBlob] = useState<Blob | null>(null);
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set());
  const toggleCol = (label: string) =>
    setHiddenCols((prev) => {
      const next = new Set(prev);
      if (next.has(label)) next.delete(label); else next.add(label);
      return next;
    });
  const sectionRef = useRef<HTMLElement>(null);
  const headerRef = useRef<HTMLElement>(null);
  const tfootRef = useRef<HTMLTableSectionElement>(null);
  const [showFloating, setShowFloating] = useState(false);
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

  // Floating nav: hiện khi section nằm trong viewport nhưng header VÀ tfoot
  // đều đã cuộn ra ngoài (user đang ở giữa bảng dài).
  useEffect(() => {
    const headerEl = headerRef.current;
    const tfootEl = tfootRef.current;
    const sectionEl = sectionRef.current;
    if (!headerEl || !tfootEl || !sectionEl) return;
    const vis = new Set<Element>();
    const obs = new IntersectionObserver((entries) => {
      for (const e of entries) {
        if (e.isIntersecting) vis.add(e.target);
        else vis.delete(e.target);
      }
      setShowFloating(vis.has(sectionEl) && !vis.has(headerEl) && !vis.has(tfootEl));
    }, { threshold: 0 });
    obs.observe(headerEl);
    obs.observe(tfootEl);
    obs.observe(sectionEl);
    return () => obs.disconnect();
  }, []);

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

  const accountCounts = useMemo(() => {
    const map = new Map<string, number>();
    for (const r of day.rows) {
      const key = r.accountName ?? "FB chung";
      map.set(key, (map.get(key) ?? 0) + 1);
    }
    return map;
  }, [day.rows]);

  // Sort rows theo Lợi nhuận giảm dần (cao → thấp). Profit phụ thuộc fees
  // (tax/reserve) nên phải compute FE-side, Rust sort fallback theo tên
  // (query.rs) — ở đây override.
  const { sortedRows, totalGain, totalLoss, gainCount, lossCount } = useMemo(() => {
    const computed = day.rows.map((r) => {
      const shopee = sumFiltered(r.shopeeClicksByReferrer, settings.clickSources);
      return { row: r, profit: computeUiRow(r, settings.profitFees, shopee).profit };
    });
    computed.sort((a, b) => b.profit - a.profit);
    let gain = 0;
    let loss = 0;
    let gainCount = 0;
    let lossCount = 0;
    for (const { profit } of computed) {
      if (profit > 0) { gain += profit; gainCount++; }
      else if (profit < 0) { loss += profit; lossCount++; }
    }
    return { sortedRows: computed.map((c) => c.row), totalGain: gain, totalLoss: loss, gainCount, lossCount };
  }, [day.rows, settings.clickSources, settings.profitFees]);
  const totalsProfitCls =
    totals.profit > 0
      ? "text-green-400"
      : totals.profit < 0
      ? "text-red-400"
      : "text-gray-300";

  const MASK = (
    <span className="select-none tracking-widest text-white/20">••••</span>
  );

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
      className={`mb-6 [overflow:clip] rounded-xl shadow-elev-2 transition-shadow hover:shadow-elev-4 ${
        effectiveDayPending ? "bg-surface-2/60" : "bg-surface-2"
      } ${capturing ? "capture-mode" : ""}`}
    >
      <header ref={headerRef} className="flex items-center justify-between border-b border-surface-8 px-5 py-3">
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
        <div className="capture-hide flex items-center gap-2">
          {!effectiveDayPending && day.rows.length > 0 && (totalGain > 0 || totalLoss < 0) && (() => {
            const net = totalGain + totalLoss;
            const netPositive = net > 0;
            return (
              <div className="mr-1 flex flex-col items-end gap-1">
                {/* Dòng trên: tổng net cả ngày */}
                <div
                  className={`inline-flex items-center gap-1 rounded-full px-3 py-0.5 text-sm font-bold tabular-nums ${
                    netPositive
                      ? "bg-green-500/20 text-green-300"
                      : "bg-red-500/20 text-red-300"
                  }`}
                  title={`Tổng kết ngày = Σ lãi + Σ lỗ\n= ${fmtVnd(totalGain)} + ${fmtVnd(totalLoss)}`}
                >
                  <span className="material-symbols-rounded text-sm">
                    {netPositive ? "savings" : "money_off"}
                  </span>
                  {fmtVnd(net)}
                </div>
                {/* Dòng dưới: chip lãi + chip lỗ kèm số sản phẩm */}
                <div className="flex items-center gap-1.5">
                  {totalGain > 0 && (
                    <div
                      className="inline-flex items-center gap-1 rounded-full bg-green-500/15 px-2.5 py-0.5 text-xs font-semibold tabular-nums text-green-400"
                      title={`${gainCount} sản phẩm có lãi / ${day.rows.length} sản phẩm`}
                    >
                      <span className="material-symbols-rounded text-xs">trending_up</span>
                      {fmtVnd(totalGain)}
                      <span className="text-green-400/60">· {gainCount}/{day.rows.length}</span>
                    </div>
                  )}
                  {totalLoss < 0 && (
                    <div
                      className="inline-flex items-center gap-1 rounded-full bg-red-500/15 px-2.5 py-0.5 text-xs font-semibold tabular-nums text-red-400"
                      title={`${lossCount} sản phẩm lỗ / ${day.rows.length} sản phẩm`}
                    >
                      <span className="material-symbols-rounded text-xs">trending_down</span>
                      {fmtVnd(totalLoss)}
                      <span className="text-red-400/60">· {lossCount}/{day.rows.length}</span>
                    </div>
                  )}
                </div>
              </div>
            );
          })()}
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
      <div className="overflow-x-auto overflow-y-clip">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-20 shadow-[0_2px_8px_rgba(0,0,0,0.45)]">
            <tr className="border-b border-shopee-500/40 bg-[#1a0c10] text-shopee-100">
              {headers.map((h, i) => {
                const isIndex = i === 0;
                const isProduct = i === 1;
                const isAction = i === headers.length - 1;
                const canToggle = !isIndex && !isAction && !!h.label;
                const colHidden = canToggle && hiddenCols.has(h.label);
                const alignCls = isIndex
                  ? "w-12 px-2 text-center"
                  : isProduct
                  ? "min-w-[220px] px-4 text-left"
                  : "px-3 text-center";
                return (
                  <th
                    key={i}
                    className={`py-3.5 text-xs font-bold uppercase tracking-wider whitespace-nowrap ${alignCls} ${
                      isAction ? "col-actions" : ""
                    }`}
                  >
                    <span className="inline-flex flex-col items-center gap-0.5">
                      <span className="inline-flex items-center gap-1">
                        <span
                          className={`inline-flex items-center gap-1 ${h.tooltip ? "cursor-help" : ""} ${colHidden ? "opacity-35" : ""}`}
                          title={h.tooltip}
                        >
                          <span>{h.label}</span>
                          {h.tooltip && (
                            <span className="text-[11px] leading-none text-shopee-300/60">
                              ⓘ
                            </span>
                          )}
                        </span>
                        {canToggle && (
                          <button
                            onClick={(e) => { e.stopPropagation(); toggleCol(h.label); }}
                            className={`flex h-4 w-4 items-center justify-center rounded transition-colors ${
                              colHidden
                                ? "text-shopee-400"
                                : "text-white/20 hover:text-white/55"
                            }`}
                            title={colHidden ? "Hiện cột" : "Ẩn cột"}
                            aria-label={colHidden ? "Hiện cột" : "Ẩn cột"}
                          >
                            <span className="material-symbols-rounded text-[11px]">
                              {colHidden ? "visibility_off" : "visibility"}
                            </span>
                          </button>
                        )}
                      </span>
                      {h.label === "TK Shopee" && accountCounts.size > 0 && (
                        <span className="flex flex-wrap justify-center gap-x-2 gap-y-0.5 normal-case">
                          {Array.from(accountCounts.entries()).map(([name, count]) => (
                            <span
                              key={name}
                              className="inline-flex items-center gap-0.5 text-[10px] font-medium text-white/50"
                              title={`${name}: ${count} dòng`}
                            >
                              <span className="max-w-[80px] truncate text-white/70">{name}</span>
                              <span className="tabular-nums text-shopee-300/80">{count}</span>
                            </span>
                          ))}
                        </span>
                      )}
                    </span>
                  </th>
                );
              })}
            </tr>
            {day.rows.length > 0 && (
              <TotalsRow
                totals={totals}
                totalsProfitCls={totalsProfitCls}
                orderValueTotal={day.totals.orderValueTotal}
                showAccount={showAccount}
                hiddenCols={hiddenCols}
                MASK={MASK}
                compact
              />
            )}
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
                const hasFbBreakdown =
                  !!r.fbBreakdown && r.fbBreakdown.campaigns.length > 0;
                return (
                  <Fragment key={key}>
                    <VideoRow
                      index={i + 1}
                      row={r}
                      showAccount={showAccount}
                      pending={effectiveDayPending || pendingRowDeletes.has(key)}
                      deleteBlocked={deleteBlocked}
                      hiddenCols={hiddenCols}
                      onEdit={() => onEditRow(r)}
                      onToggleDelete={() => {
                        if (dayPending) {
                          onToggleDayDelete(day.date);
                          for (const other of day.rows) {
                            if (
                              uiRowKey(other.dayDate, other.subIds, other.accountId) !== key
                            ) {
                              onToggleRowDelete(other);
                            }
                          }
                        } else {
                          onToggleRowDelete(r);
                        }
                      }}
                      onViewDetail={() => setDetailRow(r)}
                      onViewHistory={() => setHistoryRow(r)}
                      readOnly={readOnly}
                    />
                    {hasFbBreakdown && (
                      <FbHierarchyTree
                        breakdown={r.fbBreakdown!}
                        showAccount={showAccount}
                        hiddenCols={hiddenCols}
                      />
                    )}
                  </Fragment>
                );
              })
            )}
          </tbody>
          {day.rows.length > 0 && (
            <tfoot ref={tfootRef}>
              <TotalsRow
                totals={totals}
                totalsProfitCls={totalsProfitCls}
                orderValueTotal={day.totals.orderValueTotal}
                showAccount={showAccount}
                hiddenCols={hiddenCols}
                MASK={MASK}
              />
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

      <ProductHistoryDialog
        isOpen={!!historyRow}
        row={historyRow}
        accountFilter={accountFilter}
        onClose={() => setHistoryRow(null)}
      />

      <DayScreenshotDialog
        isOpen={!!screenshotBlob}
        blob={screenshotBlob}
        date={day.date}
        dateLabel={fmtDate(day.date)}
        onClose={handleScreenshotClose}
      />

      {showFloating && createPortal(
        <div className="capture-hide fixed bottom-6 right-6 z-50 flex flex-col items-center gap-0.5 rounded-2xl border border-surface-8/60 bg-surface-2/90 px-1 py-2 shadow-elev-4 backdrop-blur-sm">
          <button
            onClick={() => sectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" })}
            className="btn-ripple flex h-9 w-9 items-center justify-center rounded-full text-white/60 hover:bg-shopee-500/10 hover:text-shopee-400"
            title="Lên đầu sell"
            aria-label="Lên đầu sell"
          >
            <span className="material-symbols-rounded">vertical_align_top</span>
          </button>
          <span className="select-none px-1 text-center text-[10px] tabular-nums leading-tight text-white/30">
            {fmtDate(day.date)}
          </span>
          <button
            onClick={() => sectionRef.current?.scrollIntoView({ behavior: "smooth", block: "end" })}
            className="btn-ripple flex h-9 w-9 items-center justify-center rounded-full text-white/60 hover:bg-shopee-500/10 hover:text-shopee-400"
            title="Xuống cuối sell"
            aria-label="Xuống cuối sell"
          >
            <span className="material-symbols-rounded">vertical_align_bottom</span>
          </button>
        </div>,
        document.body,
      )}
    </section>
  );
}

// =========================================================
// TotalsRow — dùng chung cho tfoot (bottom) và top summary
// =========================================================

type TotalsRowProps = {
  totals: ReturnType<typeof computeUiDayTotals>;
  totalsProfitCls: string;
  orderValueTotal: number;
  showAccount: boolean;
  hiddenCols: Set<string>;
  MASK: React.ReactNode;
  /** true = hàng tóm tắt trên đầu tbody (padding nhỏ hơn, bg khác). */
  compact?: boolean;
};

function TotalsRow({
  totals,
  totalsProfitCls,
  orderValueTotal,
  showAccount,
  hiddenCols,
  MASK,
  compact = false,
}: TotalsRowProps) {
  const py = compact ? "py-2" : "py-4";
  const rowCls = compact
    ? "border-b-2 border-shopee-500/50 bg-[#140810] text-sm font-bold text-white/90"
    : "border-t-2 border-shopee-500 bg-shopee-900/25 text-base font-bold text-white";

  return (
    <tr className={rowCls}>
      <td />
      <td className={`px-4 ${py} text-left text-xs uppercase tracking-wider text-shopee-300/80`}>
        {compact ? "↑ Tổng" : "Tổng"}
      </td>
      {showAccount && <td />}
      <td className={`px-3 ${py} text-center tabular-nums`}>
        {hiddenCols.has("Click ADS") ? MASK : fmtInt(totals.clicks)}
      </td>
      <td className={`px-3 ${py} text-center tabular-nums`}>
        {hiddenCols.has("Click Shopee") ? MASK : fmtInt(totals.shopeeClicks)}
      </td>
      <td
        className={`px-3 ${py} text-center tabular-nums text-gray-400`}
        title={
          totals.clicks > 0
            ? `CPC TB = Tổng tiền chạy / Tổng click ADS\n= ${fmtVnd(totals.totalSpend)} / ${fmtInt(totals.clicks)}`
            : "Không có click ADS"
        }
      >
        {hiddenCols.has("Đơn giá click")
          ? MASK
          : totals.clicks > 0
          ? fmtVnd(totals.totalSpend / totals.clicks)
          : "—"}
      </td>
      <td className={`px-3 ${py} text-center tabular-nums text-blue-400`}>
        {hiddenCols.has("Tổng tiền chạy") ? MASK : fmtVnd(totals.totalSpend)}
      </td>
      <td className={`px-3 ${py} text-center tabular-nums`}>
        {hiddenCols.has("Số lượng đơn") ? MASK : fmtInt(totals.orders)}
      </td>
      <td
        className={`px-3 ${py} text-center tabular-nums`}
        title={
          totals.shopeeClicks === 0
            ? "Không có Click Shopee → không tính được CR"
            : `CR TB = Σ Số đơn / Σ Click Shopee × 100% (${totals.orders}/${totals.shopeeClicks})`
        }
      >
        {hiddenCols.has("Tỷ lệ chuyển đổi")
          ? MASK
          : totals.shopeeClicks > 0
          ? fmtPct((totals.orders / totals.shopeeClicks) * 100)
          : "—"}
      </td>
      <td
        className={`px-3 ${py} text-center tabular-nums`}
        title="GMV TB = Σ Giá trị đơn hàng / Σ Số đơn"
      >
        {hiddenCols.has("Giá trị đơn hàng")
          ? MASK
          : totals.orders > 0
          ? fmtVnd(orderValueTotal / totals.orders)
          : "—"}
      </td>
      <td className={`px-3 ${py} text-center tabular-nums text-shopee-400`}>
        {hiddenCols.has("Hoa hồng") ? MASK : fmtVnd(totals.commission)}
      </td>
      <td className={`px-3 ${py} text-center tabular-nums ${totalsProfitCls}`}>
        {hiddenCols.has("Lợi nhuận") ? MASK : fmtVnd(totals.profit)}
      </td>
      <td
        className={`px-3 ${py} text-center tabular-nums ${
          totals.totalSpend > 0 ? totalsProfitCls : "text-white/30"
        }`}
        title={
          totals.totalSpend > 0
            ? `ROI TB = (Hoa hồng ròng − Tiền ads) / Tiền ads\n= ${fmtVnd(totals.profit)} / ${fmtVnd(totals.totalSpend)}`
            : "Không có tiền ads"
        }
      >
        {hiddenCols.has("ROI")
          ? MASK
          : totals.totalSpend > 0
          ? fmtPct((totals.profit / totals.totalSpend) * 100)
          : "—"}
      </td>
      <td className="col-actions" />
    </tr>
  );
}
