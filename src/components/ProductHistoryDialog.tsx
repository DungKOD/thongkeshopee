import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import type { UiDay, UiRow } from "../types";
import {
  computeUiRow,
  fmtDate,
  fmtInt,
  fmtPct,
  fmtVnd,
} from "../formulas";
import { sumFiltered, useSettings } from "../hooks/useSettings";
import { invoke } from "../lib/tauri";
import type { AccountFilterMode } from "../hooks/useDbStats";

interface ProductHistoryDialogProps {
  isOpen: boolean;
  row: UiRow | null;
  accountFilter?: AccountFilterMode;
  onClose: () => void;
}

export function ProductHistoryDialog({
  isOpen,
  row,
  accountFilter,
  onClose,
}: ProductHistoryDialogProps) {
  const { settings } = useSettings();
  const [allDays, setAllDays] = useState<UiDay[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!isOpen || !row) {
      setAllDays([]);
      return;
    }
    let cancelled = false;
    setLoading(true);
    invoke<UiDay[]>("list_days_with_rows", {
      filter: {
        subIdFilter: row.displayName || undefined,
        accountFilter,
      },
    })
      .then((data) => {
        if (!cancelled) setAllDays(data ?? []);
      })
      .catch(() => {
        if (!cancelled) setAllDays([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [isOpen, row, accountFilter]);

  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  const subIdsKey = row ? row.subIds.join("\x1f") : "";

  // Lọc exact subIds match, 1 entry per (day, accountId).
  const historyRows = useMemo(() => {
    if (!row) return [];
    const result: UiRow[] = [];
    for (const day of allDays) {
      for (const r of day.rows) {
        if (r.subIds.join("\x1f") === subIdsKey) {
          result.push(r);
        }
      }
    }
    return result.sort((a, b) => b.dayDate.localeCompare(a.dayDate));
  }, [allDays, subIdsKey, row]);

  const showAccount =
    !accountFilter || accountFilter.kind === "all";

  const totals = useMemo(() => {
    let spend = 0;
    let orders = 0;
    let commission = 0;
    let profit = 0;
    let profitable = 0;
    let lossy = 0;
    for (const r of historyRows) {
      const shopee = sumFiltered(r.shopeeClicksByReferrer, settings.clickSources);
      const c = computeUiRow(r, settings.profitFees, shopee);
      spend += r.totalSpend ?? 0;
      orders += r.ordersCount;
      commission += r.commissionTotal;
      profit += c.profit;
      if (c.profit > 0) profitable++;
      else if (c.profit < 0) lossy++;
    }
    const distinctDays = new Set(historyRows.map((r) => r.dayDate)).size;
    return { spend, orders, commission, profit, profitable, lossy, distinctDays };
  }, [historyRows, settings.clickSources, settings.profitFees]);

  if (!isOpen || !row) return null;

  const profitCls =
    totals.profit > 0
      ? "text-green-400"
      : totals.profit < 0
      ? "text-red-400"
      : "text-gray-300";

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        className="relative flex max-h-[calc(100vh-2rem)] w-full max-w-4xl flex-col overflow-hidden rounded-2xl bg-surface-2 shadow-elev-24"
        role="dialog"
        aria-modal="true"
        aria-labelledby="product-history-dialog-title"
      >
        {/* Header */}
        <header className="flex shrink-0 items-start gap-4 bg-gradient-to-r from-shopee-700/90 to-shopee-600/70 px-6 py-4">
          <span className="mt-1 flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-white/15 text-white">
            <span className="material-symbols-rounded text-xl">timeline</span>
          </span>
          <div className="min-w-0 flex-1">
            <p className="text-[11px] font-medium uppercase tracking-[0.12em] text-white/70">
              Lịch sử theo ngày
            </p>
            <h2
              id="product-history-dialog-title"
              className="mt-0.5 truncate text-xl font-bold text-white"
              title={row.displayName}
            >
              {row.displayName || "(chưa đặt tên)"}
            </h2>
            {!loading && historyRows.length > 0 && (
              <div className="mt-1.5 flex flex-wrap items-center gap-3 text-xs text-white/80">
                <span className="inline-flex items-center gap-1">
                  <span className="material-symbols-rounded text-sm">event</span>
                  {totals.distinctDays} ngày có data
                </span>
                <span className="text-white/40">·</span>
                <span className="inline-flex items-center gap-1 text-green-300">
                  <span className="material-symbols-rounded text-sm">trending_up</span>
                  {totals.profitable} ngày lãi
                </span>
                <span className="text-white/40">·</span>
                <span className="inline-flex items-center gap-1 text-red-300">
                  <span className="material-symbols-rounded text-sm">trending_down</span>
                  {totals.lossy} ngày lỗ
                </span>
              </div>
            )}
          </div>
          <button
            type="button"
            onClick={onClose}
            className="btn-ripple flex h-10 w-10 shrink-0 items-center justify-center rounded-full text-white/80 hover:bg-white/15"
            title="Đóng (Esc)"
            aria-label="Đóng"
          >
            <span className="material-symbols-rounded">close</span>
          </button>
        </header>

        {/* Body */}
        <div className="min-h-0 flex-1 overflow-y-auto bg-surface-0">
          {loading ? (
            <div className="flex items-center justify-center px-6 py-12 text-sm text-white/50">
              <span className="material-symbols-rounded mr-2 animate-spin text-base">sync</span>
              Đang tải lịch sử...
            </div>
          ) : historyRows.length === 0 ? (
            <div className="flex flex-col items-center gap-2 px-6 py-12 text-sm text-white/50">
              <span className="material-symbols-rounded text-3xl text-white/20">
                history
              </span>
              <span>Không tìm thấy lịch sử cho sản phẩm này.</span>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full border-collapse text-sm">
                <thead>
                  <tr className="border-b-2 border-shopee-500/50 bg-gradient-to-b from-shopee-900/35 to-shopee-900/15 text-shopee-100">
                    <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                      Ngày
                    </th>
                    {showAccount && (
                      <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                        TK Shopee
                      </th>
                    )}
                    <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                      Tiền ads
                    </th>
                    <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                      Số đơn
                    </th>
                    <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                      Hoa hồng
                    </th>
                    <th className="whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider">
                      Lợi nhuận
                    </th>
                    <th
                      className="cursor-help whitespace-nowrap px-3 py-3.5 text-center text-xs font-bold uppercase tracking-wider"
                      title="ROI = (Hoa hồng sau phí − Tiền ads) / Tiền ads × 100%"
                    >
                      ROI <span className="text-shopee-300/60 text-[11px]">ⓘ</span>
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {historyRows.map((r) => (
                    <HistoryRow
                      key={`${r.dayDate}|${r.accountId ?? ""}`}
                      row={r}
                      showAccount={showAccount}
                    />
                  ))}
                </tbody>
                <tfoot>
                  <tr className="border-t-2 border-shopee-500 bg-shopee-900/25 text-base font-bold text-white">
                    <td className="px-3 py-4 text-center text-sm uppercase tracking-wider text-shopee-300">
                      Tổng
                    </td>
                    {showAccount && <td />}
                    <td className="px-3 py-4 text-center tabular-nums text-blue-400">
                      {fmtVnd(totals.spend)}
                    </td>
                    <td className="px-3 py-4 text-center tabular-nums">
                      {fmtInt(totals.orders)}
                    </td>
                    <td className="px-3 py-4 text-center tabular-nums text-shopee-400">
                      {fmtVnd(totals.commission)}
                    </td>
                    <td className={`px-3 py-4 text-center tabular-nums ${profitCls}`}>
                      {fmtVnd(totals.profit)}
                    </td>
                    <td className={`px-3 py-4 text-center tabular-nums ${totals.spend > 0 ? profitCls : "text-white/30"}`}>
                      {totals.spend > 0
                        ? fmtPct((totals.profit / totals.spend) * 100)
                        : "—"}
                    </td>
                  </tr>
                </tfoot>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>,
    document.body,
  );
}

// =========================================================
// Row component
// =========================================================

function HistoryRow({
  row,
  showAccount,
}: {
  row: UiRow;
  showAccount: boolean;
}) {
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

  const hasSpend = !!row.totalSpend && row.totalSpend > 0;

  return (
    <tr className="border-b border-surface-8 text-white/80 transition-colors hover:bg-shopee-500/10">
      <td className="px-3 py-2.5 text-center tabular-nums font-medium">
        {fmtDate(row.dayDate)}
      </td>
      {showAccount && (
        <td className="px-3 py-2.5 text-center">
          {row.accountName ? (
            <span
              className="inline-block max-w-[140px] truncate rounded-md bg-shopee-900/40 px-2 py-0.5 text-xs font-medium text-shopee-200"
              title={row.accountName}
            >
              {row.accountName}
            </span>
          ) : (
            <span className="inline-block rounded-md bg-amber-500/15 px-2 py-0.5 text-xs font-medium text-amber-300">
              FB chung
            </span>
          )}
        </td>
      )}
      <td className={`px-3 py-2.5 text-center tabular-nums ${row.totalSpend === null ? "text-white/30" : "text-blue-400"}`}>
        {row.totalSpend !== null ? fmtVnd(row.totalSpend) : "—"}
      </td>
      <td className="px-3 py-2.5 text-center tabular-nums">
        {fmtInt(row.ordersCount)}
      </td>
      <td className="px-3 py-2.5 text-center tabular-nums text-shopee-400">
        {fmtVnd(row.commissionTotal)}
      </td>
      <td className={`px-3 py-2.5 text-center tabular-nums font-semibold ${profitCls}`}>
        <span className="mr-1 text-xs">
          {c.profit > 0 ? "▲" : c.profit < 0 ? "▼" : ""}
        </span>
        {fmtVnd(c.profit)}
      </td>
      <td
        className={`px-3 py-2.5 text-center tabular-nums ${hasSpend ? profitCls : "text-white/30"}`}
        title={
          hasSpend
            ? `ROI = (${fmtVnd(c.netCommission)} − ${fmtVnd(row.totalSpend!)}) / ${fmtVnd(row.totalSpend!)} × 100%`
            : "Không có spend → không tính ROI"
        }
      >
        {hasSpend ? fmtPct(c.profitMargin) : "—"}
      </td>
    </tr>
  );
}
