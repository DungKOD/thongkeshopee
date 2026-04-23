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
    label: "Tỷ lệ chuyển đổi",
    tooltip: "CR = Số đơn / Click Shopee × 100%",
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

  // Flatten: each day có đúng 1 row match (đã filter từ App). Giữ thứ tự days (DESC).
  const flatRows = useMemo(() => days.flatMap((d) => d.rows), [days]);

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
            {flatRows.length} ngày có data
          </div>
        </div>
      </header>

      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr className="border-b-2 border-shopee-500/50 bg-gradient-to-b from-shopee-900/35 to-shopee-900/15 text-shopee-100">
              {HEADERS.map((h, i) => (
                <th
                  key={i}
                  title={h.tooltip}
                  className={`px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider whitespace-nowrap ${
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
                key={uiRowKey(r.dayDate, r.subIds)}
                row={r}
                pending={pendingRowDeletes.has(
                  uiRowKey(r.dayDate, r.subIds),
                )}
                onEdit={() => onEditRow(r)}
                onToggleDelete={() => onToggleRowDelete(r)}
                onViewDetail={() => setDetailRow(r)}
                readOnly={readOnly}
              />
            ))}
          </tbody>
          <tfoot>
            <tr className="border-t-2 border-shopee-500 bg-shopee-900/25 text-base font-bold text-white">
              <td className="px-3 py-4 text-center text-sm uppercase tracking-wider text-shopee-300">
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
                className={`px-3 py-4 text-center tabular-nums ${profitCls}`}
              >
                {fmtVnd(totals.profit)}
              </td>
              <td />
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
    </section>
  );
}

interface TimelineRowProps {
  row: UiRow;
  pending: boolean;
  onEdit: () => void;
  onToggleDelete: () => void;
  onViewDetail: () => void;
  readOnly?: boolean;
}

function TimelineRow({
  row,
  pending,
  onEdit,
  onToggleDelete,
  onViewDetail,
  readOnly = false,
}: TimelineRowProps) {
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
      <td
        className={`${cellCls} tabular-nums ${dataCellPending} ${
          clicksCell?.cls ?? ""
        }`}
      >
        {clicksCell ? clicksCell.text : fmtInt(row.adsClicks ?? 0)}
      </td>
      <td className={`${cellCls} tabular-nums ${dataCellPending}`}>
        {fmtInt(shopeeClicks)}
      </td>
      <td
        className={`${cellCls} tabular-nums text-gray-400 ${dataCellPending} ${cpcCell.cls}`}
      >
        {cpcCell.text}
      </td>
      <td
        className={`${cellCls} tabular-nums ${dataCellPending} ${
          spendCell?.cls ?? ""
        }`}
      >
        {spendCell ? spendCell.text : fmtVnd(row.totalSpend ?? 0)}
      </td>
      <td className={`${cellCls} tabular-nums ${dataCellPending}`}>
        {fmtInt(row.ordersCount)}
      </td>
      <td
        className={`${cellCls} tabular-nums text-gray-400 ${dataCellPending} ${
          shopeeClicks === 0 ? naCls : ""
        }`}
      >
        {shopeeClicks > 0 ? fmtPct(c.conversionRate) : "—"}
      </td>
      <td
        className={`${cellCls} tabular-nums text-gray-400 ${dataCellPending} ${
          row.ordersCount === 0 ? naCls : ""
        }`}
      >
        {row.ordersCount > 0 ? fmtVnd(c.orderValue) : "—"}
      </td>
      <td className={`${cellCls} tabular-nums ${dataCellPending}`}>
        {fmtVnd(row.commissionTotal)}
      </td>
      <td
        className={`${cellCls} tabular-nums font-medium ${profitCls} ${dataCellPending}`}
      >
        {fmtVnd(c.profit)}
      </td>
      <td
        className={`${cellCls} tabular-nums ${
          row.totalSpend && row.totalSpend > 0 ? profitCls : naCls
        } ${dataCellPending}`}
      >
        {row.totalSpend && row.totalSpend > 0 ? fmtPct(c.profitMargin) : "—"}
      </td>
      <td className={cellCls}>
        {!readOnly && (
          <div className="flex justify-center gap-0.5">
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
          </div>
        )}
      </td>
    </tr>
  );
}
