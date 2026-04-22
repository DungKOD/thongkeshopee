import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import type { ManualEntryInput, SubIds, UiRow } from "../types";
import { parseSubIdString } from "../lib/dbImport";

interface ManualEntryDialogProps {
  isOpen: boolean;
  initialDate: string;
  initialRow?: UiRow | null;
  /// Account Shopee hiện đang active (AppInner pass từ useAccounts).
  /// Null = chưa có account nào — dialog sẽ hiện warning thay cho form.
  shopeeAccountId: number | null;
  onSave: (input: ManualEntryInput) => Promise<void>;
  onClose: () => void;
}

const inputCls =
  "w-full rounded-md border border-surface-8 bg-surface-1 px-3 py-2 text-sm text-white/90 placeholder:text-white/30 transition-colors focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500";
const labelCls = "block text-xs font-medium uppercase tracking-wider text-white/60 mb-1";

function n2s(n: number | null | undefined): string {
  if (n === null || n === undefined) return "";
  return String(n);
}

function s2n(s: string): number | null {
  const trimmed = s.trim();
  if (!trimmed) return null;
  const v = Number(trimmed.replace(/,/g, ""));
  return Number.isFinite(v) ? v : null;
}

export function ManualEntryDialog({
  isOpen,
  initialDate,
  initialRow,
  shopeeAccountId,
  onSave,
  onClose,
}: ManualEntryDialogProps) {
  const [date, setDate] = useState(initialDate);
  const [name, setName] = useState("");
  const [clicks, setClicks] = useState("");
  const [spend, setSpend] = useState("");
  const [cpc, setCpc] = useState("");
  const [orders, setOrders] = useState("");
  const [commission, setCommission] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!isOpen) return;
    setError(null);
    setSaving(false);
    setDate(initialRow?.dayDate || initialDate);
    if (initialRow) {
      const nonEmpty = initialRow.subIds.filter((s) => s).join("-");
      setName(nonEmpty || initialRow.displayName);
      setClicks(n2s(initialRow.adsClicks));
      setSpend(n2s(initialRow.totalSpend));
      setCpc(n2s(initialRow.cpc));
      setOrders(n2s(initialRow.ordersCount));
      setCommission(n2s(initialRow.commissionTotal));
    } else {
      setName("");
      setClicks("");
      setSpend("");
      setCpc("");
      setOrders("");
      setCommission("");
    }
  }, [isOpen, initialDate, initialRow]);

  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !saving) onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose, saving]);

  if (!isOpen) return null;

  const subIds: SubIds = name.trim()
    ? parseSubIdString(name.trim())
    : ["", "", "", "", ""];
  const hasSubId = subIds.some((s) => s);
  const isEdit = !!initialRow;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!date) {
      setError("Vui lòng chọn ngày");
      return;
    }
    if (!name.trim()) {
      setError("Vui lòng nhập tên (sẽ tự split thành sub_id)");
      return;
    }
    if (shopeeAccountId === null) {
      setError("Chưa có account Shopee — tạo account trước khi thêm dòng tay");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const input: ManualEntryInput = {
        dayDate: date,
        subIds,
        displayName: hasSubId ? null : name.trim(),
        overrideClicks: s2n(clicks),
        overrideSpend: s2n(spend),
        overrideCpc: s2n(cpc),
        overrideOrders: s2n(orders),
        overrideCommission: s2n(commission),
        shopeeAccountId,
      };
      await onSave(input);
    } catch (err) {
      setError((err as Error).message ?? String(err));
      setSaving(false);
    }
  };

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget && !saving) onClose();
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        className="flex max-h-[calc(100vh-2rem)] w-full max-w-2xl flex-col overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
        role="dialog"
        aria-modal="true"
        aria-labelledby="manual-entry-dialog-title"
      >
        <form onSubmit={handleSubmit} className="flex min-h-0 flex-1 flex-col">
          <header className="flex shrink-0 items-center gap-3 border-b border-surface-8 px-6 py-4">
            <span className="material-symbols-rounded text-shopee-400">
              {isEdit ? "edit" : "add_task"}
            </span>
            <h2
              id="manual-entry-dialog-title"
              className="text-lg font-semibold text-white/90"
            >
              {isEdit ? "Sửa dòng" : "Thêm dòng mới"}
            </h2>
          </header>

          <div className="min-h-0 flex-1 space-y-4 overflow-y-auto px-6 py-4">
            <div>
              <label className={labelCls}>Ngày</label>
              <input
                type="date"
                value={date}
                onChange={(e) => setDate(e.currentTarget.value)}
                className={inputCls}
                required
                disabled={isEdit}
              />
            </div>

            <div>
              <label className={labelCls}>
                Tên (sẽ split thành sub_id1-5 theo dấu "-")
              </label>
              <input
                type="text"
                value={name}
                autoFocus={!isEdit}
                onChange={(e) => setName(e.currentTarget.value)}
                placeholder="VD: sanpham0101"
                className={inputCls}
                required
                disabled={isEdit}
              />
              {hasSubId && (
                <div className="mt-1 flex flex-wrap gap-1 text-[11px] text-white/50">
                  {subIds.map((s, i) =>
                    s ? (
                      <span
                        key={i}
                        className="rounded bg-surface-6 px-1.5 py-0.5 font-mono"
                        title={s}
                      >
                        sub_id{i + 1}: {s}
                      </span>
                    ) : null,
                  )}
                </div>
              )}
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className={labelCls}>Click ADS</label>
                <input
                  type="number"
                  inputMode="numeric"
                  value={clicks}
                  onChange={(e) => setClicks(e.currentTarget.value)}
                  placeholder="—"
                  className={inputCls}
                />
              </div>
              <div>
                <label className={labelCls}>Tổng tiền chạy (VND)</label>
                <input
                  type="number"
                  inputMode="decimal"
                  value={spend}
                  onChange={(e) => setSpend(e.currentTarget.value)}
                  placeholder="—"
                  className={inputCls}
                />
              </div>
              <div>
                <label className={labelCls}>CPC (VND)</label>
                <input
                  type="number"
                  inputMode="decimal"
                  value={cpc}
                  onChange={(e) => setCpc(e.currentTarget.value)}
                  placeholder="— (tự tính từ spend/click)"
                  className={inputCls}
                />
              </div>
              <div>
                <label className={labelCls}>Số đơn</label>
                <input
                  type="number"
                  inputMode="numeric"
                  value={orders}
                  onChange={(e) => setOrders(e.currentTarget.value)}
                  placeholder="—"
                  className={inputCls}
                />
              </div>
              <div className="col-span-2">
                <label className={labelCls}>Hoa hồng (VND)</label>
                <input
                  type="number"
                  inputMode="decimal"
                  value={commission}
                  onChange={(e) => setCommission(e.currentTarget.value)}
                  placeholder="—"
                  className={inputCls}
                />
              </div>
            </div>

            {error && (
              <div className="rounded-lg border border-red-500/50 bg-red-900/30 px-3 py-2 text-sm text-red-200">
                {error}
              </div>
            )}

            {isEdit && (
              <p className="text-xs text-white/50">
                Lưu ý: dòng này có data raw từ CSV (FB/Shopee). Giá trị bạn nhập
                sẽ override dữ liệu tự động tính. Xóa field (để trống) = dùng
                lại giá trị raw.
              </p>
            )}
          </div>

          <footer className="flex shrink-0 justify-end gap-2 border-t border-surface-8 bg-surface-1 px-6 py-3">
            <button
              type="button"
              onClick={onClose}
              disabled={saving}
              className="btn-ripple rounded-lg px-5 py-2 text-sm font-medium text-white/80 hover:bg-white/5 disabled:opacity-50"
            >
              Hủy
            </button>
            <button
              type="submit"
              disabled={saving || !name.trim() || !date}
              className="btn-ripple flex items-center gap-2 rounded-lg bg-shopee-500 px-5 py-2 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <span className="material-symbols-rounded text-base">save</span>
              {saving ? "Đang lưu..." : "Lưu"}
            </button>
          </footer>
        </form>
      </div>
    </div>,
    document.body,
  );
}
