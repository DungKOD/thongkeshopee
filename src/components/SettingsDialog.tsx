import { useEffect } from "react";
import { createPortal } from "react-dom";
import type { ProfitFees, Settings } from "../hooks/useSettings";

interface SettingsDialogProps {
  isOpen: boolean;
  settings: Settings;
  daysCount: number;
  productsCount: number;
  onToggleClickSource: (source: string, enabled: boolean) => void;
  onSetProfitFee: (key: keyof ProfitFees, value: number) => void;
  onClose: () => void;
}

export function SettingsDialog({
  isOpen,
  settings,
  daysCount,
  productsCount,
  onToggleClickSource,
  onSetProfitFee,
  onClose,
}: SettingsDialogProps) {
  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const sources = Object.entries(settings.clickSources).sort((a, b) =>
    a[0].localeCompare(b[0]),
  );

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        className="w-full max-w-lg overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
        role="dialog"
        aria-modal="true"
        aria-labelledby="settings-dialog-title"
      >
        <header className="flex items-center gap-3 border-b border-surface-8 px-6 py-4">
          <span className="material-symbols-rounded text-shopee-400">
            settings
          </span>
          <h2
            id="settings-dialog-title"
            className="text-lg font-semibold text-white/90"
          >
            Cài đặt
          </h2>
        </header>

        <div className="max-h-[70vh] space-y-6 overflow-y-auto px-6 py-5">
          <section>
            <h3 className="mb-3 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base">
                insights
              </span>
              Thống kê chung
            </h3>
            <div className="grid grid-cols-2 gap-3">
              <div className="rounded-xl bg-surface-6 px-4 py-3 shadow-elev-1">
                <p className="text-xs text-white/50">Số ngày đã lưu</p>
                <p className="mt-1 text-2xl font-semibold tabular-nums text-shopee-300">
                  {daysCount}
                </p>
              </div>
              <div className="rounded-xl bg-surface-6 px-4 py-3 shadow-elev-1">
                <p className="text-xs text-white/50">Tổng sản phẩm</p>
                <p className="mt-1 text-2xl font-semibold tabular-nums text-shopee-300">
                  {productsCount}
                </p>
              </div>
            </div>
          </section>

          <section>
            <h3 className="mb-1 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base">
                payments
              </span>
              Phí khấu trừ lợi nhuận
            </h3>
            <p className="mb-3 text-xs text-white/50">
              Lợi nhuận = Hoa hồng × (1 − tổng phí) − Tiền ads
            </p>
            <div className="grid grid-cols-2 gap-3">
              <FeeInput
                label="Thuế + phí sàn"
                value={settings.profitFees.taxAndPlatformRate}
                onChange={(v) => onSetProfitFee("taxAndPlatformRate", v)}
              />
              <FeeInput
                label="Dự phòng hoàn/hủy"
                value={settings.profitFees.returnReserveRate}
                onChange={(v) => onSetProfitFee("returnReserveRate", v)}
              />
            </div>
            <div className="mt-2 rounded-lg bg-surface-2 px-3 py-2 text-xs text-white/60">
              Tổng khấu trừ:{" "}
              <span className="font-semibold text-shopee-300">
                {(
                  settings.profitFees.taxAndPlatformRate +
                  settings.profitFees.returnReserveRate
                ).toFixed(2)}
                %
              </span>
              {" → Giữ lại: "}
              <span className="font-semibold text-shopee-300">
                {(
                  100 -
                  settings.profitFees.taxAndPlatformRate -
                  settings.profitFees.returnReserveRate
                ).toFixed(2)}
                %
              </span>{" "}
              hoa hồng
            </div>
          </section>

          <section>
            <h3 className="mb-1 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base">
                filter_list
              </span>
              Nguồn Click Shopee
            </h3>
            <p className="mb-3 text-xs text-white/50">
              Chọn loại referrer (cột "Người giới thiệu") sẽ được tính vào ô
              Click Shopee. Danh sách tự cập nhật sau mỗi lần Import.
            </p>

            {sources.length === 0 ? (
              <div className="flex items-center gap-3 rounded-xl border border-dashed border-surface-12 bg-surface-2 px-4 py-4 text-sm text-white/50">
                <span className="material-symbols-rounded text-base">info</span>
                Chưa Import file click. Hãy Import để phát hiện các nguồn
                referrer.
              </div>
            ) : (
              <ul className="divide-y divide-surface-8 overflow-hidden rounded-xl bg-surface-6">
                {sources.map(([source, enabled]) => (
                  <li key={source}>
                    <label className="flex cursor-pointer items-center gap-3 px-4 py-3 text-sm text-white/85 transition-colors hover:bg-white/5">
                      <input
                        type="checkbox"
                        checked={enabled}
                        onChange={(e) =>
                          onToggleClickSource(source, e.currentTarget.checked)
                        }
                        className="h-4 w-4 accent-shopee-500"
                      />
                      <span className="flex-1">{source}</span>
                      {enabled ? (
                        <span className="material-symbols-rounded text-base text-shopee-400">
                          check_circle
                        </span>
                      ) : null}
                    </label>
                  </li>
                ))}
              </ul>
            )}
          </section>
        </div>

        <footer className="flex justify-end border-t border-surface-8 bg-surface-1 px-6 py-3">
          <button
            type="button"
            onClick={onClose}
            className="btn-ripple rounded-lg bg-shopee-500 px-5 py-2 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4"
          >
            Đóng
          </button>
        </footer>
      </div>
    </div>,
    document.body,
  );
}

interface FeeInputProps {
  label: string;
  value: number;
  onChange: (v: number) => void;
}

function FeeInput({ label, value, onChange }: FeeInputProps) {
  return (
    <label className="flex flex-col gap-1 rounded-xl bg-surface-6 px-3 py-2 shadow-elev-1">
      <span className="text-xs text-white/55">{label}</span>
      <div className="flex items-center gap-1">
        <input
          type="number"
          inputMode="decimal"
          step="0.01"
          min="0"
          max="100"
          value={value}
          onChange={(e) => {
            const raw = e.currentTarget.value;
            const n = raw === "" ? 0 : Number(raw);
            onChange(Number.isFinite(n) ? n : 0);
          }}
          className="w-full rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-right text-lg font-semibold tabular-nums text-shopee-300 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500"
        />
        <span className="text-sm text-white/60">%</span>
      </div>
    </label>
  );
}
