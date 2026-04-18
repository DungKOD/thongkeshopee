import type { UiRow } from "../types";
import { computeUiRow, fmtInt, fmtPct, fmtVnd } from "../formulas";
import { sumFiltered, useSettings } from "../hooks/useSettings";

interface CampaignRowProps {
  row: UiRow;
  pending: boolean;
  onEdit: () => void;
  onToggleDelete: () => void;
  onViewDetail: () => void;
}

const cellCls = "px-3 py-2.5 text-center";
const naCls = "text-white/30";

function fmtOrNa(
  value: number | null | undefined,
  fmt: (n: number) => string,
): { text: string; cls: string } {
  if (value === null || value === undefined)
    return { text: "—", cls: naCls };
  return { text: fmt(value), cls: "" };
}

export function VideoRow({
  row,
  pending,
  onEdit,
  onToggleDelete,
  onViewDetail,
}: CampaignRowProps) {
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

  // Strikethrough + mờ áp cho CELL data, KHÔNG áp cho cell actions để nút
  // Undo vẫn rõ và bấm được.
  const dataCellPending = pending ? "line-through opacity-50" : "";

  const spendCell = fmtOrNa(row.totalSpend, fmtVnd);
  const clicksCell = fmtOrNa(row.adsClicks, fmtInt);
  const cpcCell = fmtOrNa(c.cpc > 0 ? c.cpc : null, fmtVnd);

  // Không trigger onViewDetail khi user đang bôi đen text trong row.
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
      title={row.displayName}
    >
      <td
        className={`max-w-[280px] truncate px-4 py-2.5 text-left text-sm font-semibold text-white ${dataCellPending}`}
        title={row.displayName}
      >
        {row.displayName || (
          <span className="italic font-normal text-gray-500">
            (chưa đặt tên)
          </span>
        )}
      </td>
      <td
        className={`${cellCls} tabular-nums ${clicksCell.cls} ${dataCellPending}`}
      >
        {clicksCell.text}
      </td>
      <td className={`${cellCls} tabular-nums ${dataCellPending}`}>
        {fmtInt(shopeeClicks)}
      </td>
      <td
        className={`${cellCls} tabular-nums text-gray-400 ${cpcCell.cls} ${dataCellPending}`}
      >
        {cpcCell.text}
      </td>
      <td
        className={`${cellCls} tabular-nums ${spendCell.cls} ${dataCellPending}`}
      >
        {spendCell.text}
      </td>
      <td className={`${cellCls} tabular-nums ${dataCellPending}`}>
        {fmtInt(row.ordersCount)}
      </td>
      <td
        className={`${cellCls} tabular-nums text-gray-400 ${
          shopeeClicks === 0 ? naCls : ""
        } ${dataCellPending}`}
        title={
          shopeeClicks === 0
            ? "Không có Click Shopee → không tính được CR"
            : `CR = Số đơn / Click Shopee × 100% (${row.ordersCount}/${shopeeClicks})`
        }
      >
        {shopeeClicks > 0 ? fmtPct(c.conversionRate) : "—"}
      </td>
      <td
        className={`${cellCls} tabular-nums text-gray-400 ${
          row.ordersCount === 0 ? naCls : ""
        } ${dataCellPending}`}
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
        title={
          !row.totalSpend || row.totalSpend === 0
            ? "Không có chi phí ads → không tính được ROI (lãi tự nhiên không qua spend)"
            : `ROI = (Hoa hồng sau phí − Tiền ads) / Tiền ads\n${
                c.profitMargin > 0
                  ? "Có lãi"
                  : c.profitMargin < 0
                  ? "Đang lỗ"
                  : "Hòa vốn"
              }`
        }
      >
        {row.totalSpend && row.totalSpend > 0
          ? fmtPct(c.profitMargin)
          : "—"}
      </td>
      <td className={cellCls}>
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
            aria-label="Sửa"
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
            aria-label={pending ? "Khôi phục" : "Đánh dấu xóa"}
          >
            <span className="material-symbols-rounded text-lg">
              {pending ? "undo" : "delete"}
            </span>
          </button>
        </div>
      </td>
    </tr>
  );
}
