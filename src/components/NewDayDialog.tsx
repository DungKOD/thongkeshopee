import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import type { Day } from "../types";
import type { VideoInput } from "../hooks/useStats";
import { todayIso } from "../hooks/useStats";
import { sumFiltered, useSettings } from "../hooks/useSettings";
import { fmtDate } from "../formulas";

const MANUAL_KEY = "Nhập tay";

interface NewDayDialogProps {
  isOpen: boolean;
  existingDays: Day[];
  initialDay?: Day;
  initialData?: { date: string; videos: VideoInput[] };
  onSave: (
    date: string,
    videos: VideoInput[],
    replaceDayId: string | null,
  ) => void;
  onClose: () => void;
}

interface Row extends VideoInput {
  rowId: string;
}

const uid = () =>
  typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : Math.random().toString(36).slice(2);

const emptyRow = (): Row => ({
  rowId: uid(),
  name: "",
  clicks: 0,
  shopeeClicksByReferrer: {},
  totalSpend: 0,
  orders: 0,
  commission: 0,
});

const rowsFromDay = (day: Day): Row[] =>
  day.videos.map((v) => ({
    rowId: uid(),
    name: v.name,
    clicks: v.clicks,
    shopeeClicksByReferrer: { ...v.shopeeClicksByReferrer },
    totalSpend: v.totalSpend,
    orders: v.orders,
    commission: v.commission,
    cpc: v.cpc,
    orderDetails: v.orderDetails,
  }));

const cellInputCls =
  "w-full rounded-md border border-surface-8 bg-surface-1 px-2 py-1.5 text-sm text-center text-white/90 placeholder:text-white/30 transition-colors focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500";

const tableCellCls = "px-2 py-1.5 border-b border-surface-8";

const dateInputCls =
  "rounded-lg border border-surface-8 bg-surface-1 px-3 py-2 text-white/90 transition-colors focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500";

type NumField = "clicks" | "totalSpend" | "orders" | "commission";

export function NewDayDialog({
  isOpen,
  existingDays,
  initialDay,
  initialData,
  onSave,
  onClose,
}: NewDayDialogProps) {
  const [date, setDate] = useState<string>(todayIso());
  const [rows, setRows] = useState<Row[]>([emptyRow()]);
  const [loadedFromId, setLoadedFromId] = useState<string | null>(null);
  const { settings } = useSettings();

  useEffect(() => {
    if (!isOpen) return;
    if (initialDay) {
      setDate(initialDay.date);
      setRows(
        initialDay.videos.length > 0 ? rowsFromDay(initialDay) : [emptyRow()],
      );
      setLoadedFromId(initialDay.id);
    } else if (initialData) {
      setDate(initialData.date);
      setRows(
        initialData.videos.length > 0
          ? initialData.videos.map((v) => ({ rowId: uid(), ...v }))
          : [emptyRow()],
      );
      setLoadedFromId(null);
    } else {
      setDate(todayIso());
      setRows([emptyRow()]);
      setLoadedFromId(null);
    }
  }, [isOpen, initialDay, initialData]);

  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const matchingDay = existingDays.find((d) => d.date === date) ?? null;
  const isEditing = !!loadedFromId && matchingDay?.id === loadedFromId;
  const isImportMode = !!initialData && !initialDay;
  const showLoadPrompt = !!matchingDay && !isEditing && !isImportMode;
  const showOverwriteWarning = isImportMode && !!matchingDay;

  const handleDateChange = (newDate: string) => {
    setDate(newDate);
    // Nếu đã load ngày cũ nhưng đổi sang ngày khác → reset trạng thái edit
    const newMatch = existingDays.find((d) => d.date === newDate) ?? null;
    if (loadedFromId !== newMatch?.id) {
      setLoadedFromId(null);
    }
  };

  const loadExisting = () => {
    if (!matchingDay) return;
    setLoadedFromId(matchingDay.id);
    setRows(rowsFromDay(matchingDay));
    if (matchingDay.videos.length === 0) {
      setRows([emptyRow()]);
    }
  };

  const setRowName = (rowId: string, value: string) => {
    setRows((prev) =>
      prev.map((r) => (r.rowId === rowId ? { ...r, name: value } : r)),
    );
  };

  const setRowNum = (rowId: string, key: NumField, raw: string) => {
    const n = raw === "" ? 0 : Number(raw);
    const safe = Number.isFinite(n) ? n : 0;
    setRows((prev) =>
      prev.map((r) => {
        if (r.rowId !== rowId) return r;
        const updated: Row = { ...r, [key]: safe };
        // Nếu user đổi clicks hoặc spend → clear CPC cached từ file để tính lại
        if (key === "clicks" || key === "totalSpend") {
          updated.cpc = undefined;
        }
        return updated;
      }),
    );
  };

  const setRowShopeeClicks = (rowId: string, raw: string) => {
    const n = raw === "" ? 0 : Number(raw);
    const safe = Number.isFinite(n) ? n : 0;
    setRows((prev) =>
      prev.map((r) =>
        r.rowId === rowId
          ? { ...r, shopeeClicksByReferrer: { [MANUAL_KEY]: safe } }
          : r,
      ),
    );
  };

  const addRow = () => setRows((prev) => [...prev, emptyRow()]);
  const removeRow = (rowId: string) =>
    setRows((prev) => prev.filter((r) => r.rowId !== rowId));

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!date) return;
    const videos: VideoInput[] = rows.map(({ rowId: _rowId, ...rest }) => rest);
    const replaceId = loadedFromId ?? matchingDay?.id ?? null;
    onSave(date, videos, replaceId);
  };

  return createPortal(
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4">
      <div
        className="w-full max-w-4xl overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
        role="dialog"
        aria-modal="true"
        aria-labelledby="new-day-dialog-title"
      >
        <form onSubmit={handleSubmit}>
          <header className="flex items-center gap-3 border-b border-surface-8 px-6 py-4">
            <span className="material-symbols-rounded text-shopee-400">
              {isEditing ? "edit_calendar" : "add_task"}
            </span>
            <h2
              id="new-day-dialog-title"
              className="text-lg font-semibold text-white/90"
            >
              {isEditing ? "Sửa ngày" : "Thêm ngày"}
            </h2>
          </header>

          <div className="space-y-4 px-5 py-4">
            <div className="flex items-center gap-3">
              <label className="text-sm font-medium text-gray-300">
                Ngày:
              </label>
              <input
                type="date"
                value={date}
                autoFocus
                onChange={(e) => {
                  const value = e.currentTarget.value;
                  handleDateChange(value);
                }}
                className={dateInputCls}
              />
            </div>

            {showLoadPrompt && matchingDay && (
              <div className="flex items-center justify-between gap-3 rounded border border-amber-500/60 bg-amber-900/20 px-3 py-2">
                <p className="text-sm text-amber-200">
                  Ngày{" "}
                  <span className="font-semibold">{fmtDate(date)}</span> đã có{" "}
                  <span className="font-semibold">
                    {matchingDay.videos.length}
                  </span>{" "}
                  sản phẩm. Bạn có muốn sửa không?
                </p>
                <button
                  type="button"
                  onClick={loadExisting}
                  className="shrink-0 rounded-md bg-amber-500 px-3 py-1 text-sm font-semibold text-white hover:bg-amber-600"
                >
                  Tải lên để sửa
                </button>
              </div>
            )}

            {isEditing && (
              <div className="rounded border border-shopee-500/60 bg-shopee-900/30 px-3 py-2 text-sm text-shopee-200">
                Đang sửa dữ liệu có sẵn của ngày{" "}
                <span className="font-semibold">{fmtDate(date)}</span>.
              </div>
            )}

            {showOverwriteWarning && matchingDay && (
              <div className="rounded border border-red-500/60 bg-red-900/20 px-3 py-2 text-sm text-red-200">
                ⚠ Ngày <span className="font-semibold">{fmtDate(date)}</span>{" "}
                đã có{" "}
                <span className="font-semibold">
                  {matchingDay.videos.length}
                </span>{" "}
                sản phẩm. Bấm Lưu sẽ <span className="font-semibold">ghi đè</span>{" "}
                toàn bộ dữ liệu cũ.
              </div>
            )}

            <div>
              <div className="mb-2 flex items-center justify-between">
                <span className="text-sm font-medium text-gray-300">
                  Sản phẩm ({rows.length})
                </span>
              </div>

              <div className="overflow-x-auto rounded-lg border border-surface-8 bg-surface-2">
                <table className="w-full border-collapse text-sm">
                  <thead>
                    <tr className="bg-surface-6 text-shopee-200">
                      <th className="border-b border-surface-8 px-2 py-2.5 text-center text-[11px] font-semibold uppercase tracking-wider">
                        Tên SP
                      </th>
                      <th className="border-b border-surface-8 px-2 py-2.5 text-center text-[11px] font-semibold uppercase tracking-wider">
                        Click FB
                      </th>
                      <th className="border-b border-surface-8 px-2 py-2.5 text-center text-[11px] font-semibold uppercase tracking-wider">
                        Click Shopee
                      </th>
                      <th className="border-b border-surface-8 px-2 py-2.5 text-center text-[11px] font-semibold uppercase tracking-wider">
                        Tổng tiền chạy
                      </th>
                      <th className="border-b border-surface-8 px-2 py-2.5 text-center text-[11px] font-semibold uppercase tracking-wider">
                        Số đơn
                      </th>
                      <th className="border-b border-surface-8 px-2 py-2.5 text-center text-[11px] font-semibold uppercase tracking-wider">
                        Hoa hồng
                      </th>
                      <th className="w-10 border-b border-surface-8 px-2 py-2.5"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row) => {
                      const shopeeClicks = sumFiltered(
                        row.shopeeClicksByReferrer,
                        settings.clickSources,
                      );
                      return (
                        <tr key={row.rowId}>
                          <td className={tableCellCls}>
                            <input
                              type="text"
                              value={row.name}
                              placeholder="vd: sp01"
                              onChange={(e) => {
                                const value = e.currentTarget.value;
                                setRowName(row.rowId, value);
                              }}
                              className={cellInputCls}
                            />
                          </td>
                          <td className={tableCellCls}>
                            <input
                              type="number"
                              inputMode="numeric"
                              value={row.clicks === 0 ? "" : String(row.clicks)}
                              placeholder="0"
                              onChange={(e) =>
                                setRowNum(
                                  row.rowId,
                                  "clicks",
                                  e.currentTarget.value,
                                )
                              }
                              className={`${cellInputCls} tabular-nums`}
                            />
                          </td>
                          <td className={tableCellCls}>
                            <input
                              type="number"
                              inputMode="numeric"
                              value={shopeeClicks === 0 ? "" : String(shopeeClicks)}
                              placeholder="0"
                              onChange={(e) =>
                                setRowShopeeClicks(
                                  row.rowId,
                                  e.currentTarget.value,
                                )
                              }
                              className={`${cellInputCls} tabular-nums`}
                            />
                          </td>
                          {(
                            [
                              "totalSpend",
                              "orders",
                              "commission",
                            ] as NumField[]
                          ).map((key) => (
                            <td key={key} className={tableCellCls}>
                              <input
                                type="number"
                                inputMode="numeric"
                                value={row[key] === 0 ? "" : String(row[key])}
                                placeholder="0"
                                onChange={(e) =>
                                  setRowNum(
                                    row.rowId,
                                    key,
                                    e.currentTarget.value,
                                  )
                                }
                                className={`${cellInputCls} tabular-nums`}
                              />
                            </td>
                          ))}
                          <td className={`${tableCellCls} text-center`}>
                            <button
                              type="button"
                              onClick={() => removeRow(row.rowId)}
                              disabled={rows.length === 1}
                              className="btn-ripple flex h-7 w-7 items-center justify-center rounded-full text-white/50 hover:bg-red-500/10 hover:text-red-400 disabled:cursor-not-allowed disabled:opacity-30 disabled:hover:bg-transparent disabled:hover:text-white/50"
                              title="Xóa dòng"
                              aria-label="Xóa dòng"
                            >
                              <span className="material-symbols-rounded text-base">
                                close
                              </span>
                            </button>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              <button
                type="button"
                onClick={addRow}
                className="btn-ripple mt-2 flex items-center gap-2 rounded-lg px-3 py-2 text-sm font-medium text-shopee-400 hover:bg-shopee-500/10 active:bg-shopee-500/20"
              >
                <span className="material-symbols-rounded text-base">add</span>
                Thêm dòng
              </button>
            </div>
          </div>

          <footer className="flex justify-end gap-2 border-t border-surface-8 bg-surface-1 px-6 py-3">
            <button
              type="button"
              onClick={onClose}
              className="btn-ripple rounded-lg px-5 py-2 text-sm font-medium text-white/80 hover:bg-white/5"
            >
              Hủy
            </button>
            <button
              type="submit"
              disabled={!date}
              className="btn-ripple flex items-center gap-2 rounded-lg bg-shopee-500 px-5 py-2 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4 disabled:cursor-not-allowed disabled:opacity-50 disabled:shadow-none"
            >
              <span className="material-symbols-rounded text-base">save</span>
              Lưu
            </button>
          </footer>
        </form>
      </div>
    </div>,
    document.body,
  );
}
