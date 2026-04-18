import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import type { PreviewBatch } from "../lib/dbImport";
import { kindLabel } from "../lib/dbImport";
import { fmtDate, fmtInt } from "../formulas";

interface ImportPreviewDialogProps {
  batch: PreviewBatch | null;
  onConfirm: () => Promise<void>;
  onCancel: () => void;
}

export function ImportPreviewDialog({
  batch,
  onConfirm,
  onCancel,
}: ImportPreviewDialogProps) {
  const [committing, setCommitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!batch) return;
    setError(null);
    setCommitting(false);
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !committing) onCancel();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [batch, onCancel, committing]);

  if (!batch) return null;

  const handleConfirm = async () => {
    setCommitting(true);
    setError(null);
    try {
      await onConfirm();
    } catch (e) {
      setError((e as Error).message ?? String(e));
      setCommitting(false);
    }
  };

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget && !committing) onCancel();
  };

  const totalReplace = batch.files.reduce(
    (a, f) => a + f.preview.replaceRows,
    0,
  );
  const totalNew = batch.files.reduce((a, f) => a + f.preview.newRows, 0);
  const anyDayHasData = batch.files.some((f) => f.preview.dayHasData);
  const hasReplacements = totalReplace > 0;

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        className="flex max-h-[calc(100vh-2rem)] w-full max-w-4xl flex-col overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
        role="dialog"
        aria-modal="true"
        aria-labelledby="import-preview-title"
      >
        <header className="flex shrink-0 items-center gap-3 border-b border-surface-8 px-6 py-4">
          <span className="material-symbols-rounded text-shopee-400">
            upload_file
          </span>
          <div className="flex-1">
            <p className="text-[11px] font-medium uppercase tracking-wider text-white/50">
              Xác nhận import
            </p>
            <h2
              id="import-preview-title"
              className="text-lg font-semibold text-white/90"
            >
              Ngày {fmtDate(batch.dayDate)}
            </h2>
          </div>
          <div className="shrink-0 text-right text-xs">
            <div className="text-green-400">
              + {fmtInt(totalNew)} dòng mới
            </div>
            {hasReplacements && (
              <div className="text-amber-300">
                ⟳ {fmtInt(totalReplace)} dòng replace
              </div>
            )}
          </div>
        </header>

        <div className="min-h-0 flex-1 space-y-4 overflow-y-auto px-6 py-4">
          {anyDayHasData && hasReplacements && (
            <div
              role="alert"
              className="flex items-start gap-3 rounded-lg border-2 border-amber-500 bg-amber-950/40 px-4 py-3"
            >
              <span className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-amber-500 text-base font-black text-black">
                !
              </span>
              <div className="flex-1 space-y-1">
                <p className="text-sm font-bold uppercase tracking-wide text-amber-300">
                  Ngày này đã có data — sẽ replace một số dòng
                </p>
                <p className="text-xs text-white/70">
                  Các dòng trùng identity sẽ bị ghi đè giá trị mới. Dòng không
                  trùng sẽ được thêm. Raw file gốc lưu trong app_data để rollback
                  nếu cần.
                </p>
              </div>
            </div>
          )}

          <div className="overflow-hidden rounded-xl border border-surface-8">
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr className="bg-surface-6 text-shopee-200">
                  <th className="border-b border-surface-8 px-3 py-2 text-left text-[11px] font-semibold uppercase tracking-wider">
                    Loại
                  </th>
                  <th className="border-b border-surface-8 px-3 py-2 text-left text-[11px] font-semibold uppercase tracking-wider">
                    File
                  </th>
                  <th className="border-b border-surface-8 px-3 py-2 text-right text-[11px] font-semibold uppercase tracking-wider">
                    Tổng dòng
                  </th>
                  <th className="border-b border-surface-8 px-3 py-2 text-right text-[11px] font-semibold uppercase tracking-wider">
                    Thêm mới
                  </th>
                  <th className="border-b border-surface-8 px-3 py-2 text-right text-[11px] font-semibold uppercase tracking-wider">
                    Sẽ replace
                  </th>
                </tr>
              </thead>
              <tbody>
                {batch.files.map(({ preview }, i) => (
                  <tr
                    key={i}
                    className="border-b border-surface-8 last:border-b-0 text-white/80 transition-colors hover:bg-shopee-500/15"
                  >
                    <td className="px-3 py-2">
                      <span className="inline-flex items-center gap-1.5 rounded-full bg-shopee-900/40 px-2 py-0.5 text-xs font-medium text-shopee-200">
                        {kindLabel(preview.kind)}
                      </span>
                    </td>
                    <td
                      className="max-w-[300px] truncate px-3 py-2 text-xs text-white/70"
                      title={preview.filename}
                    >
                      {preview.filename}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {fmtInt(preview.totalRows)}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums text-green-400">
                      {preview.newRows > 0 ? "+" : ""}
                      {fmtInt(preview.newRows)}
                    </td>
                    <td
                      className={`px-3 py-2 text-right tabular-nums ${
                        preview.replaceRows > 0
                          ? "text-amber-300"
                          : "text-white/30"
                      }`}
                    >
                      {preview.replaceRows > 0
                        ? `⟳ ${fmtInt(preview.replaceRows)}`
                        : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {batch.files
            .filter((f) => f.preview.sampleReplace.length > 0)
            .map(({ preview }, i) => (
              <div
                key={i}
                className="rounded-xl border border-amber-500/30 bg-amber-950/20 px-4 py-3"
              >
                <p className="mb-2 text-xs font-semibold uppercase tracking-wider text-amber-300">
                  Ví dụ dòng sẽ replace trong {kindLabel(preview.kind)} (
                  {preview.filename}):
                </p>
                <ul className="space-y-0.5 pl-4 text-xs text-white/70">
                  {preview.sampleReplace.map((s, j) => (
                    <li key={j} className="truncate font-mono" title={s}>
                      • {s}
                    </li>
                  ))}
                  {preview.replaceRows > preview.sampleReplace.length && (
                    <li className="italic text-white/40">
                      ... và {preview.replaceRows - preview.sampleReplace.length}{" "}
                      dòng khác
                    </li>
                  )}
                </ul>
              </div>
            ))}

          {error && (
            <div className="rounded-lg border border-red-500/50 bg-red-900/30 px-3 py-2 text-sm text-red-200">
              {error}
            </div>
          )}
        </div>

        <footer className="flex shrink-0 items-center justify-between gap-2 border-t border-surface-8 bg-surface-1 px-6 py-3">
          <p className="text-xs text-white/50">
            {hasReplacements
              ? "Xác nhận sẽ ghi đè các dòng trùng. Data không trùng được giữ nguyên."
              : "Tất cả dòng sẽ được thêm mới."}
          </p>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={onCancel}
              disabled={committing}
              className="btn-ripple rounded-lg px-5 py-2 text-sm font-medium text-white/80 hover:bg-white/5 disabled:opacity-50"
            >
              Hủy
            </button>
            <button
              type="button"
              onClick={handleConfirm}
              disabled={committing}
              className={`btn-ripple flex items-center gap-2 rounded-lg px-5 py-2 text-sm font-semibold text-white shadow-elev-2 hover:shadow-elev-4 disabled:cursor-not-allowed disabled:opacity-50 ${
                hasReplacements
                  ? "bg-amber-500 hover:bg-amber-600"
                  : "bg-shopee-500 hover:bg-shopee-600"
              }`}
            >
              <span className="material-symbols-rounded text-base">
                {committing ? "hourglass_top" : "save"}
              </span>
              {committing
                ? "Đang lưu..."
                : hasReplacements
                ? "Xác nhận replace & lưu"
                : "Xác nhận import"}
            </button>
          </div>
        </footer>
      </div>
    </div>,
    document.body,
  );
}
