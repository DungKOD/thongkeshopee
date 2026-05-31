import { useState } from "react";
import type { UiRow } from "../types";
import { computeUiRow, fmtInt, fmtPct, fmtVnd } from "../formulas";
import { sumFiltered, useSettings } from "../hooks/useSettings";

interface CampaignRowProps {
  row: UiRow;
  index: number;
  pending: boolean;
  onEdit: () => void;
  onToggleDelete: () => void;
  onViewDetail: () => void;
  onViewHistory?: () => void;
  readOnly?: boolean;
  showAccount?: boolean;
  deleteBlocked?: boolean;
  hiddenCols?: Set<string>;
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
  index,
  pending,
  onEdit,
  onToggleDelete,
  onViewDetail,
  onViewHistory,
  readOnly = false,
  showAccount = false,
  deleteBlocked = false,
  hiddenCols,
}: CampaignRowProps) {
  const h = (col: string) => hiddenCols?.has(col) ?? false;
  const [copied, setCopied] = useState(false);
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
  const spendCell = fmtOrNa(row.totalSpend, fmtVnd);
  const clicksCell = fmtOrNa(row.adsClicks, fmtInt);
  const cpcCell = fmtOrNa(c.cpc > 0 ? c.cpc : null, fmtVnd);

  const MASK = (
    <span className="select-none tracking-widest text-white/20">••••</span>
  );

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
      className={`h-[52px] border-b border-surface-8 text-white/80 transition-colors ${
        pending
          ? "bg-surface-2/50"
          : "cursor-pointer hover:bg-shopee-500/15 hover:shadow-[inset_3px_0_0_0] hover:shadow-shopee-500"
      }`}
      title={row.displayName}
    >
      <td
        className={`w-12 px-2 py-2.5 text-center text-sm tabular-nums text-white/50 ${dataCellPending}`}
      >
        {index}
      </td>
      <td
        className={`max-w-[280px] px-4 py-2.5 text-left text-sm font-semibold text-white ${dataCellPending}`}
        title={h("Sản phẩm") ? undefined : row.displayName}
      >
        {h("Sản phẩm") ? MASK : (
          <div className="flex min-w-0 items-center gap-1.5">
            <span className="truncate">
              {row.displayName || (
                <span className="italic font-normal text-gray-500">
                  (chưa đặt tên)
                </span>
              )}
            </span>
            {row.displayName && (
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  navigator.clipboard.writeText(row.displayName).then(() => {
                    setCopied(true);
                    setTimeout(() => setCopied(false), 1500);
                  });
                }}
                title={copied ? "Đã copy!" : "Copy tên sản phẩm"}
                className={`flex-none flex h-5 w-5 items-center justify-center rounded transition-all ${
                  copied ? "text-green-400" : "text-white/20 hover:text-white/80"
                }`}
              >
                <span className="material-symbols-rounded text-[13px]">
                  {copied ? "check" : "content_copy"}
                </span>
              </button>
            )}
          </div>
        )}
      </td>
      {showAccount && (
        <td className={`${cellCls} ${dataCellPending}`}>
          {h("TK Shopee") ? MASK : row.accountName ? (
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
      <td className={`${cellCls} tabular-nums ${clicksCell.cls} ${dataCellPending}`}>
        {h("Click ADS") ? MASK : clicksCell.text}
      </td>
      <td className={`${cellCls} tabular-nums ${dataCellPending}`}>
        {h("Click Shopee") ? MASK : fmtInt(shopeeClicks)}
      </td>
      <td className={`${cellCls} tabular-nums text-gray-400 ${cpcCell.cls} ${dataCellPending}`}>
        {h("Đơn giá click") ? MASK : cpcCell.text}
      </td>
      <td
        className={`${cellCls} tabular-nums ${
          spendCell.cls === "" ? "text-blue-400" : spendCell.cls
        } ${dataCellPending}`}
      >
        {h("Tổng tiền chạy") ? MASK : spendCell.text}
      </td>
      <td className={`${cellCls} tabular-nums ${dataCellPending}`}>
        {h("Số lượng đơn") ? MASK : fmtInt(row.ordersCount)}
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
        {h("Tỷ lệ chuyển đổi") ? MASK : shopeeClicks > 0 ? fmtPct(c.conversionRate) : "—"}
      </td>
      <td
        className={`${cellCls} tabular-nums text-gray-400 ${
          row.ordersCount === 0 ? naCls : ""
        } ${dataCellPending}`}
      >
        {h("Giá trị đơn hàng") ? MASK : row.ordersCount > 0 ? fmtVnd(c.orderValue) : "—"}
      </td>
      <td className={`${cellCls} tabular-nums text-shopee-400 ${dataCellPending}`}>
        {h("Hoa hồng") ? MASK : fmtVnd(row.commissionTotal)}
      </td>
      <td
        className={`${cellCls} tabular-nums font-medium ${profitCls} ${dataCellPending}`}
      >
        {h("Lợi nhuận") ? MASK : fmtVnd(c.profit)}
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
        {h("ROI") ? MASK : row.totalSpend && row.totalSpend > 0 ? fmtPct(c.profitMargin) : "—"}
      </td>
      <td className={`${cellCls} col-actions`}>
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
                aria-label="Sửa"
              >
                <span className="material-symbols-rounded text-lg">edit</span>
              </button>
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  if (deleteBlocked && !pending) return;
                  onToggleDelete();
                }}
                disabled={deleteBlocked && !pending}
                className={`btn-ripple flex h-8 w-8 items-center justify-center rounded-full ${
                  deleteBlocked && !pending
                    ? "cursor-not-allowed text-white/20"
                    : pending
                    ? "text-amber-400 hover:bg-amber-500/10"
                    : "text-white/60 hover:bg-red-500/10 hover:text-red-400"
                }`}
                title={
                  deleteBlocked && !pending
                    ? "Sub_id này có data ở ≥2 TK Shopee — chuyển dropdown TK sang TK cụ thể trước khi xóa (BE hiện xóa theo tuple sẽ wipe cả TK khác)"
                    : pending
                    ? "Khôi phục"
                    : "Đánh dấu xóa"
                }
                aria-label={pending ? "Khôi phục" : "Đánh dấu xóa"}
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
