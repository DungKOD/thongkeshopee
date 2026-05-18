import { useState } from "react";
import {
  commitFbHierarchy,
  parseFbHierarchyFile,
  previewFbHierarchy,
} from "../lib/dbImport";
import type {
  FbHierarchyParsed,
  ImportPreview,
  ImportResult,
} from "../lib/dbImport";
import { fmtDate, fmtInt } from "../formulas";

interface FbHierarchyImportDialogProps {
  isOpen: boolean;
  onClose: () => void;
  /** Gọi sau khi import xong để parent refresh data. */
  onImported: (result: ImportResult) => void;
}

interface ParsedFileItem {
  file: File;
  parsed: FbHierarchyParsed | null;
  preview: ImportPreview | null;
  reason: string | null;
  taxRate: number;
}

/**
 * Dialog import định dạng FB Ads "hierarchy" (3 cấp). Tách riêng khỏi flow
 * `previewCsvBatch`/`commitCsvBatch` của các format cũ — nút riêng trên header.
 *
 * Khi tính năng ổn định sẽ merge vào pipeline chung. Hiện tại design tối giản:
 * - Pick file (multiple ok), parse từng file qua parseFbHierarchyFile.
 * - Show summary + tax rate input.
 * - Commit từng file qua commitFbHierarchy. Lỗi 1 file không abort các file khác.
 */
export function FbHierarchyImportDialog({
  isOpen,
  onClose,
  onImported,
}: FbHierarchyImportDialogProps) {
  const [items, setItems] = useState<ParsedFileItem[]>([]);
  const [committing, setCommitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFiles = async (files: File[]) => {
    setError(null);
    const next: ParsedFileItem[] = [];
    for (const f of files) {
      const r = await parseFbHierarchyFile(f);
      if (r.ok) {
        try {
          const preview = await previewFbHierarchy(r.parsed);
          next.push({
            file: f,
            parsed: r.parsed,
            preview,
            reason: null,
            taxRate: 0,
          });
        } catch (e) {
          next.push({
            file: f,
            parsed: null,
            preview: null,
            reason: (e as Error).message ?? String(e),
            taxRate: 0,
          });
        }
      } else {
        next.push({
          file: f,
          parsed: null,
          preview: null,
          reason: r.reason,
          taxRate: 0,
        });
      }
    }
    setItems(next);
  };

  const updateTax = (idx: number, value: number) => {
    setItems((prev) =>
      prev.map((it, i) => (i === idx ? { ...it, taxRate: value } : it)),
    );
  };

  const handleCommit = async () => {
    setCommitting(true);
    setError(null);
    try {
      let lastResult: ImportResult | null = null;
      for (const it of items) {
        if (!it.parsed || !it.preview) continue;
        if (it.preview.alreadyImported) continue;
        const result = await commitFbHierarchy(it.parsed, it.taxRate);
        lastResult = result;
      }
      if (lastResult) onImported(lastResult);
      setItems([]);
      onClose();
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setCommitting(false);
    }
  };

  const handleClose = () => {
    if (committing) return;
    setItems([]);
    setError(null);
    onClose();
  };

  if (!isOpen) return null;

  const hasCommitable = items.some(
    (it) => it.parsed && it.preview && !it.preview.alreadyImported,
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
      <div className="flex max-h-[85vh] w-full max-w-3xl flex-col rounded-2xl border border-surface-8 bg-surface-1 shadow-elev-4">
        <header className="flex items-center justify-between border-b border-surface-8 px-6 py-4">
          <div className="flex items-center gap-2">
            <span className="material-symbols-rounded text-2xl text-violet-400">
              campaign
            </span>
            <div>
              <h2 className="text-lg font-semibold text-white">
                Import FB Ads
              </h2>
              <p className="text-xs text-white/50">
                Hỗ trợ CSV và Excel (.xlsx). Tự nhận diện format: cần 3 cột Tên chiến dịch · Tên nhóm quảng cáo · Tên quảng cáo.
              </p>
            </div>
          </div>
          <button
            onClick={handleClose}
            disabled={committing}
            className="flex h-9 w-9 items-center justify-center rounded-full text-white/60 hover:bg-white/10 disabled:opacity-30"
            aria-label="Đóng"
          >
            <span className="material-symbols-rounded">close</span>
          </button>
        </header>

        <div className="flex-1 overflow-auto px-6 py-4">
          {items.length === 0 ? (
            <FilePickerBox onFiles={handleFiles} />
          ) : (
            <div className="space-y-3">
              {items.map((it, idx) => (
                <FileItemRow
                  key={idx}
                  item={it}
                  onChangeTax={(v) => updateTax(idx, v)}
                />
              ))}
              <button
                onClick={() => setItems([])}
                disabled={committing}
                className="text-xs text-white/50 hover:text-white/80"
              >
                Chọn lại file
              </button>
            </div>
          )}

          {error && (
            <div className="mt-4 rounded-lg border border-red-500/50 bg-red-900/30 p-3 text-sm text-red-200">
              {error}
            </div>
          )}
        </div>

        <footer className="flex items-center justify-end gap-2 border-t border-surface-8 px-6 py-3">
          <button
            onClick={handleClose}
            disabled={committing}
            className="rounded-lg px-4 py-2 text-sm text-white/70 hover:bg-white/10 disabled:opacity-30"
          >
            Hủy
          </button>
          <button
            onClick={handleCommit}
            disabled={committing || !hasCommitable}
            className="btn-ripple rounded-lg bg-shopee-500 px-5 py-2 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 disabled:cursor-not-allowed disabled:opacity-40"
          >
            {committing ? "Đang import…" : "Import"}
          </button>
        </footer>
      </div>
    </div>
  );
}

function FilePickerBox({ onFiles }: { onFiles: (files: File[]) => void }) {
  return (
    <label className="flex cursor-pointer flex-col items-center justify-center gap-3 rounded-xl border-2 border-dashed border-surface-8 bg-surface-2/40 px-6 py-12 hover:bg-surface-2/60">
      <span className="material-symbols-rounded text-4xl text-violet-400">
        upload_file
      </span>
      <span className="text-sm font-medium text-white">
        Chọn file CSV hoặc Excel (.xlsx)
      </span>
      <span className="text-xs text-white/50">
        Cần 3 cột: Tên chiến dịch · Tên nhóm quảng cáo · Tên quảng cáo
      </span>
      <input
        type="file"
        accept=".csv,.xlsx,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        multiple
        className="hidden"
        onChange={(e) => {
          const files = Array.from(e.target.files ?? []);
          e.target.value = "";
          if (files.length > 0) onFiles(files);
        }}
      />
    </label>
  );
}

function FileItemRow({
  item,
  onChangeTax,
}: {
  item: ParsedFileItem;
  onChangeTax: (value: number) => void;
}) {
  const { file, parsed, preview, reason, taxRate } = item;
  const isError = !parsed || !preview;

  return (
    <div
      className={`rounded-lg border p-3 ${
        isError
          ? "border-red-500/40 bg-red-900/15"
          : preview!.alreadyImported
          ? "border-amber-500/40 bg-amber-900/15"
          : "border-surface-8 bg-surface-2"
      }`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div
            className="truncate text-sm font-medium text-white"
            title={file.name}
          >
            {file.name}
          </div>
          {isError ? (
            <div className="mt-1 text-xs text-red-300">{reason}</div>
          ) : (
            <div className="mt-1 text-xs text-white/60">
              Ngày {fmtDate(preview!.dayDate)} · {fmtInt(preview!.totalRows)}{" "}
              dòng
              {preview!.replaceRows > 0 &&
                ` · ${fmtInt(preview!.replaceRows)} replace`}
              {preview!.alreadyImported && " · ĐÃ IMPORT"}
            </div>
          )}
        </div>

        {!isError && !preview!.alreadyImported && (
          <div className="flex shrink-0 items-center gap-1.5">
            <label
              className="text-xs text-white/60"
              title="% thuế VAT/business tax áp cho file này"
            >
              Thuế:
            </label>
            <input
              type="number"
              min={0}
              max={100}
              step="0.01"
              value={taxRate}
              onChange={(e) => onChangeTax(Number(e.target.value) || 0)}
              className="w-16 rounded border border-surface-8 bg-surface-3 px-2 py-1 text-right text-sm text-white"
            />
            <span className="text-xs text-white/60">%</span>
          </div>
        )}
      </div>
    </div>
  );
}
