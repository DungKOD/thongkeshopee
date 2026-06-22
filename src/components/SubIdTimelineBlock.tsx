import { useMemo, useState } from "react";
import type { UiDay, UiRow } from "../types";
import {
  computeNetCommission,
  computeUiRow,
  fmtDate,
  fmtInt,
  fmtPct,
  fmtVnd,
  uiRowKey,
} from "../formulas";
import { sumFiltered, useSettings } from "../hooks/useSettings";
import { ProductDetailDialog } from "./ProductDetailDialog";
import { ProductHistoryDialog } from "./ProductHistoryDialog";

interface SubIdTimelineBlockProps {
  subId: string;
  days: UiDay[];
  pendingRowDeletes: ReadonlyMap<string, unknown>;
  onToggleRowDelete: (row: UiRow) => void;
  onEditRow: (row: UiRow) => void;
  readOnly?: boolean;
  /** Account filter từ App — pass-through cho ProductDetailDialog. */
  accountFilter?: import("../hooks/useDbStats").AccountFilterMode;
}

const HEADERS: Array<{ label: string; tooltip?: string }> = [
  { label: "Ngày" },
  {
    label: "TK Shopee",
    tooltip:
      "Tài khoản Shopee mà dòng này thuộc về. 'FB chung' = quảng cáo FB " +
      "có ≥2 TK Shopee cùng tuple sub_id trong ngày.",
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
  { label: "Tổng tiền chạy", tooltip: "Spend FB" },
  { label: "Số lượng đơn", tooltip: "Số đơn hàng (COUNT DISTINCT order_id)" },
  {
    label: "CR",
    tooltip: "Tỷ lệ chuyển đổi (CR) = Số đơn / Click Shopee × 100%",
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
  {
    label: "ROI",
    tooltip:
      "ROI = (Hoa hồng sau phí − Tiền ads) / Tiền ads × 100%\n• > 0% có lãi\n• < 0% đang lỗ\n• — khi spend = 0",
  },
  { label: "" },
];

const cellCls = "px-3 py-2.5 text-center";
const naCls = "text-white/30";

/** Set rỗng dùng làm sentinel ổn định ref khi user toggle reveal empty cols. */
const EMPTY_STR_SET: ReadonlySet<string> = new Set<string>();

export function SubIdTimelineBlock({
  subId,
  days,
  pendingRowDeletes,
  onToggleRowDelete,
  onEditRow,
  readOnly = false,
  accountFilter,
}: SubIdTimelineBlockProps) {
  const { settings } = useSettings();
  const [detailRow, setDetailRow] = useState<UiRow | null>(null);
  const [historyRow, setHistoryRow] = useState<UiRow | null>(null);

  // Flatten rows giữ thứ tự days (DESC). Sau v0.4.5+ aggregate split per-acc:
  // 1 day có thể có nhiều row cùng tuple — tab này hiện hết, mỗi row 1 acc.
  const flatRows = useMemo(() => days.flatMap((d) => d.rows), [days]);

  // Cột TK Shopee chỉ render khi filter=All (giống DayBlock).
  const showAccount = !accountFilter || accountFilter.kind === "all";

  // Auto-hide cột rỗng (cùng logic DayBlock): 1-pass qua flatRows.
  const [revealEmpty, setRevealEmpty] = useState(false);
  const autoEmptyCols = useMemo(() => {
    const empty = new Set<string>();
    if (flatRows.length === 0) return empty;
    let anyClicks = false, anyShopee = false, anyCpc = false;
    let anySpend = false, anyOrders = false, anyCommission = false;
    let anyProfit = false;
    for (const r of flatRows) {
      const shopee = sumFiltered(
        r.shopeeClicksByReferrer,
        settings.clickSources,
      );
      const c = computeUiRow(r, settings.profitFees, shopee);
      if (r.adsClicks && r.adsClicks > 0) anyClicks = true;
      if (shopee > 0) anyShopee = true;
      if (c.cpc > 0) anyCpc = true;
      if (r.totalSpend && r.totalSpend > 0) anySpend = true;
      if (r.ordersCount > 0) anyOrders = true;
      if (r.commissionTotal !== 0) anyCommission = true;
      if (c.profit !== 0) anyProfit = true;
    }
    if (!anyClicks) empty.add("Click ADS");
    if (!anyShopee) empty.add("Click Shopee");
    if (!anyCpc) empty.add("Đơn giá click");
    if (!anySpend) {
      empty.add("Tổng tiền chạy");
      empty.add("ROI");
    }
    if (!anyOrders) {
      empty.add("Số lượng đơn");
      empty.add("Giá trị đơn hàng");
    }
    if (!anyOrders || !anyShopee) empty.add("CR");
    if (!anyCommission) empty.add("Hoa hồng");
    if (!anyProfit) empty.add("Lợi nhuận");
    return empty;
  }, [flatRows, settings.clickSources, settings.profitFees]);
  const effectiveAutoHidden = revealEmpty ? EMPTY_STR_SET : autoEmptyCols;

  const headers = useMemo(() => {
    let h = HEADERS;
    if (!showAccount) h = h.filter((col) => col.label !== "TK Shopee");
    if (effectiveAutoHidden.size > 0) {
      h = h.filter((col) => !effectiveAutoHidden.has(col.label));
    }
    return h;
  }, [showAccount, effectiveAutoHidden]);

  // Đếm distinct dayDate cho label "X ngày có data" (rows.length over-count
  // khi multi-acc same day).
  const distinctDays = useMemo(
    () => new Set(flatRows.map((r) => r.dayDate)).size,
    [flatRows],
  );

  const totals = useMemo(() => {
    return flatRows.reduce(
      (acc, r) => {
        acc.clicks += r.adsClicks ?? 0;
        acc.shopeeClicks += sumFiltered(
          r.shopeeClicksByReferrer,
          settings.clickSources,
        );
        acc.totalSpend += r.totalSpend ?? 0;
        acc.orders += r.ordersCount;
        acc.orderValueTotal += r.orderValueTotal;
        acc.commission += r.commissionTotal;
        const net = computeNetCommission(
          r.commissionTotal,
          r.commissionPending,
          settings.profitFees,
        );
        acc.profit += net - (r.totalSpend ?? 0);
        return acc;
      },
      {
        clicks: 0,
        shopeeClicks: 0,
        totalSpend: 0,
        orders: 0,
        orderValueTotal: 0,
        commission: 0,
        profit: 0,
      },
    );
  }, [flatRows, settings.clickSources, settings.profitFees]);

  const profitCls =
    totals.profit > 0
      ? "text-green-400"
      : totals.profit < 0
      ? "text-red-400"
      : "text-gray-300";

  if (flatRows.length === 0) return null;

  return (
    <section className="mb-6 overflow-hidden rounded-xl bg-surface-2 shadow-elev-2">
      <header className="flex items-center justify-between border-b border-surface-8 px-5 py-3">
        <div className="flex items-center gap-3">
          <span className="material-symbols-rounded text-shopee-400">tag</span>
          <div className="flex flex-col">
            <span className="text-[11px] font-medium uppercase tracking-wider text-white/50">
              Sub_id
            </span>
            <span
              className="max-w-[480px] truncate text-base font-semibold text-white/90"
              title={subId}
            >
              {subId}
            </span>
          </div>
          <div className="ml-4 inline-flex items-center gap-1 rounded-full bg-shopee-900/40 px-3 py-1 text-xs font-medium text-shopee-300">
            <span className="material-symbols-rounded text-sm">timeline</span>
            {distinctDays} ngày có data
          </div>
        </div>
        {autoEmptyCols.size > 0 && (
          <button
            onClick={() => setRevealEmpty((v) => !v)}
            title={
              revealEmpty
                ? `Ẩn lại ${autoEmptyCols.size} cột rỗng`
                : `${autoEmptyCols.size} cột rỗng đã ẩn: ${Array.from(
                    autoEmptyCols,
                  ).join(", ")}. Bấm để hiện.`
            }
            className={`btn-ripple inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-[11px] font-medium transition-colors ${
              revealEmpty
                ? "border-shopee-500/40 bg-shopee-500/15 text-shopee-300 hover:bg-shopee-500/20"
                : "border-white/10 bg-white/5 text-white/55 hover:bg-white/10 hover:text-white/80"
            }`}
            aria-pressed={revealEmpty}
          >
            <span className="material-symbols-rounded text-sm">
              {revealEmpty ? "visibility" : "visibility_off"}
            </span>
            {autoEmptyCols.size} cột rỗng
          </button>
        )}
      </header>

      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-sm table-fixed">
          <thead>
            <tr className="border-b-2 border-shopee-500/50 bg-gradient-to-b from-shopee-900/35 to-shopee-900/15 text-shopee-100">
              {headers.map((h, i) => (
                <th
                  key={i}
                  title={h.tooltip}
                  className={`w-[120px] px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider whitespace-nowrap ${
                    h.tooltip ? "cursor-help" : ""
                  }`}
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
              ))}
            </tr>
          </thead>
          <tbody>
            {flatRows.map((r) => (
              <TimelineRow
                key={uiRowKey(r.dayDate, r.subIds, r.accountId)}
                row={r}
                showAccount={showAccount}
                autoHiddenCols={effectiveAutoHidden}
                pending={pendingRowDeletes.has(
                  uiRowKey(r.dayDate, r.subIds, r.accountId),
                )}
                onEdit={() => onEditRow(r)}
                onToggleDelete={() => onToggleRowDelete(r)}
                onViewDetail={() => setDetailRow(r)}
                onViewHistory={() => setHistoryRow(r)}
                readOnly={readOnly}
              />
            ))}
          </tbody>
          <tfoot>
            <tr className="border-t-2 border-shopee-500 bg-shopee-900/25 text-base font-bold text-white">
              <td className="px-3 py-4 text-center text-sm uppercase tracking-wider text-shopee-300">
                Tổng
              </td>
              {showAccount && <td />}
              {!effectiveAutoHidden.has("Click ADS") && (
                <td className={`px-3 py-4 text-center tabular-nums`}>
                  {fmtInt(totals.clicks)}
                </td>
              )}
              {!effectiveAutoHidden.has("Click Shopee") && (
                <td className={`px-3 py-4 text-center tabular-nums`}>
                  {fmtInt(totals.shopeeClicks)}
                </td>
              )}
              {!effectiveAutoHidden.has("Đơn giá click") && <td />}
              {!effectiveAutoHidden.has("Tổng tiền chạy") && (
                <td className={`px-3 py-4 text-center tabular-nums text-blue-400`}>
                  {fmtVnd(totals.totalSpend)}
                </td>
              )}
              {!effectiveAutoHidden.has("Số lượng đơn") && (
                <td className={`px-3 py-4 text-center tabular-nums`}>
                  {fmtInt(totals.orders)}
                </td>
              )}
              {!effectiveAutoHidden.has("CR") && (
                <td
                  className={`px-3 py-4 text-center tabular-nums`}
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
              )}
              {!effectiveAutoHidden.has("Giá trị đơn hàng") && (
                <td
                  className={`px-3 py-4 text-center tabular-nums`}
                  title="GMV TB = Σ Giá trị đơn hàng / Σ Số đơn"
                >
                  {totals.orders > 0
                    ? fmtVnd(totals.orderValueTotal / totals.orders)
                    : "—"}
                </td>
              )}
              {!effectiveAutoHidden.has("Hoa hồng") && (
                <td className={`px-3 py-4 text-center tabular-nums text-shopee-400`}>
                  {fmtVnd(totals.commission)}
                </td>
              )}
              {!effectiveAutoHidden.has("Lợi nhuận") && (
                <td
                  className={`px-3 py-4 text-center tabular-nums ${profitCls}`}
                >
                  {fmtVnd(totals.profit)}
                </td>
              )}
              {!effectiveAutoHidden.has("ROI") && <td />}
              <td />
            </tr>
          </tfoot>
        </table>
      </div>

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
    </section>
  );
}

interface TimelineRowProps {
  row: UiRow;
  pending: boolean;
  onEdit: () => void;
  onToggleDelete: () => void;
  onViewDetail: () => void;
  onViewHistory?: () => void;
  readOnly?: boolean;
  showAccount?: boolean;
  autoHiddenCols?: ReadonlySet<string>;
}

function TimelineRow({
  row,
  pending,
  onEdit,
  onToggleDelete,
  onViewDetail,
  onViewHistory,
  readOnly = false,
  showAccount = false,
  autoHiddenCols,
}: TimelineRowProps) {
  const auto = (col: string) => autoHiddenCols?.has(col) ?? false;
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

  const dataCellPending = pending ? "line-through opacity-50" : "";

  const clicksCell = row.adsClicks === null ? { text: "—", cls: naCls } : null;
  const cpcCell =
    c.cpc > 0 ? { text: fmtVnd(c.cpc), cls: "" } : { text: "—", cls: naCls };
  const spendCell =
    row.totalSpend === null ? { text: "—", cls: naCls } : null;

  const handleRowClick = () => {
    if (pending) return;
    const selection = window.getSelection();
    if (selection && selection.toString().length > 0) return;
    onViewDetail();
  };

  return (
    <tr
      onClick={handleRowClick}
      className={`border-b border-surface-8 text-white/80 transition-colors ${
        pending
          ? "bg-surface-2/50"
          : "cursor-pointer hover:bg-shopee-500/15 hover:shadow-[inset_3px_0_0_0] hover:shadow-shopee-500"
      }`}
    >
      <td className={`${cellCls} tabular-nums font-medium ${dataCellPending}`}>
        {fmtDate(row.dayDate)}
      </td>
      {showAccount && (
        <td className={`${cellCls} ${dataCellPending}`}>
          {row.accountName ? (
            <span
              className="inline-block max-w-[140px] truncate rounded-md bg-shopee-900/40 px-2 py-0.5 text-xs font-medium text-shopee-200"
              title={row.accountName}
            >
              {row.accountName}
            </span>
          ) : (
            <span
              className="inline-block rounded-md bg-amber-500/15 px-2 py-0.5 text-xs font-medium text-amber-300"
              title="FB ad có ≥2 TK Shopee cùng tuple sub_id trong ngày — không quy được duy nhất 1 TK"
            >
              FB chung
            </span>
          )}
        </td>
      )}
      {!auto("Click ADS") && (
        <td
          className={`${cellCls} tabular-nums ${dataCellPending} ${
            clicksCell?.cls ?? ""
          }`}
        >
          {clicksCell ? clicksCell.text : fmtInt(row.adsClicks ?? 0)}
        </td>
      )}
      {!auto("Click Shopee") && (
        <td className={`${cellCls} tabular-nums ${dataCellPending}`}>
          {fmtInt(shopeeClicks)}
        </td>
      )}
      {!auto("Đơn giá click") && (
        <td
          className={`${cellCls} tabular-nums text-gray-400 ${dataCellPending} ${cpcCell.cls}`}
        >
          {cpcCell.text}
        </td>
      )}
      {!auto("Tổng tiền chạy") && (
        <td
          className={`${cellCls} tabular-nums ${dataCellPending} ${
            spendCell?.cls ?? "text-blue-400"
          }`}
        >
          {spendCell ? spendCell.text : fmtVnd(row.totalSpend ?? 0)}
        </td>
      )}
      {!auto("Số lượng đơn") && (
        <td className={`${cellCls} tabular-nums ${dataCellPending}`}>
          {fmtInt(row.ordersCount)}
        </td>
      )}
      {!auto("CR") && (
        <td
          className={`${cellCls} tabular-nums text-gray-400 ${dataCellPending} ${
            shopeeClicks === 0 ? naCls : ""
          }`}
        >
          {shopeeClicks > 0 ? fmtPct(c.conversionRate) : "—"}
        </td>
      )}
      {!auto("Giá trị đơn hàng") && (
        <td
          className={`${cellCls} tabular-nums text-gray-400 ${dataCellPending} ${
            row.ordersCount === 0 ? naCls : ""
          }`}
        >
          {row.ordersCount > 0 ? fmtVnd(c.orderValue) : "—"}
        </td>
      )}
      {!auto("Hoa hồng") && (
        <td className={`${cellCls} tabular-nums text-shopee-400 ${dataCellPending}`}>
          {fmtVnd(row.commissionTotal)}
        </td>
      )}
      {!auto("Lợi nhuận") && (
        <td
          className={`${cellCls} tabular-nums font-medium ${profitCls} ${dataCellPending}`}
        >
          {fmtVnd(c.profit)}
        </td>
      )}
      {!auto("ROI") && (
        <td
          className={`${cellCls} tabular-nums ${
            row.totalSpend && row.totalSpend > 0 ? profitCls : naCls
          } ${dataCellPending}`}
        >
          {row.totalSpend && row.totalSpend > 0 ? fmtPct(c.profitMargin) : "—"}
        </td>
      )}
      <td className={cellCls}>
        <div className="flex justify-center gap-0.5">
          {onViewHistory && (
            <button
              onClick={(e) => {
                e.stopPropagation();
                onViewHistory();
              }}
              className="btn-ripple flex h-8 w-8 items-center justify-center rounded-full text-white/40 hover:bg-shopee-500/10 hover:text-shopee-300"
              title="Xem lịch sử theo ngày"
              aria-label="Lịch sử"
            >
              <span className="material-symbols-rounded text-lg">timeline</span>
            </button>
          )}
          {!readOnly && (
            <>
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  if (!pending) onEdit();
                }}
                disabled={pending}
                className={`btn-ripple flex h-8 w-8 items-center justify-center rounded-full ${
                  pending
                    ? "cursor-not-allowed text-white/20"
                    : "text-shopee-400 hover:bg-shopee-500/10"
                }`}
                title={pending ? "Đã đánh dấu xóa — bỏ để sửa" : "Sửa"}
              >
                <span className="material-symbols-rounded text-lg">edit</span>
              </button>
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  onToggleDelete();
                }}
                className={`btn-ripple flex h-8 w-8 items-center justify-center rounded-full ${
                  pending
                    ? "text-amber-400 hover:bg-amber-500/10"
                    : "text-white/60 hover:bg-red-500/10 hover:text-red-400"
                }`}
                title={pending ? "Khôi phục" : "Đánh dấu xóa"}
              >
                <span className="material-symbols-rounded text-lg">
                  {pending ? "undo" : "delete"}
                </span>
              </button>
            </>
          )}
        </div>
      </td>
    </tr>
  );
}
