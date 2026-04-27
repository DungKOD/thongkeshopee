import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import type { PreviewBatch } from "../lib/dbImport";
import { kindLabel } from "../lib/dbImport";
import { fmtDate, fmtInt } from "../formulas";
import { useAccounts } from "../contexts/AccountContext";

interface ImportPreviewDialogProps {
  batch: PreviewBatch | null;
  /// Account id mà toàn bộ Shopee file trong batch sẽ tag về. User chọn ở
  /// ImportAccountPickerDialog trước khi pick file → dialog này chỉ hiển thị.
  shopeeAccountId: string | null;
  /// User confirm import. `fbTaxRates` map index FB file → % thuế (0..100).
  /// Shopee/rejected file index không có entry. App layer forward sang
  /// `commitCsvBatch`.
  onConfirm: (fbTaxRates: Record<number, number>) => Promise<void>;
  onCancel: () => void;
}

export function ImportPreviewDialog({
  batch,
  shopeeAccountId,
  onConfirm,
  onCancel,
}: ImportPreviewDialogProps) {
  const { accounts } = useAccounts();
  const [committing, setCommitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  /// State per-file tax rate. Key = index trong batch.files (giữ nguyên qua
  /// re-render vì batch.files không reorder). Value = % (0..100, 2 decimal).
  /// Reset mỗi lần batch đổi (useEffect dưới).
  const [taxRates, setTaxRates] = useState<Record<number, number>>({});

  useEffect(() => {
    if (!batch) return;
    setError(null);
    setCommitting(false);
    setTaxRates({});
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !committing) onCancel();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [batch, onCancel, committing]);

  const accountName = useMemo(() => {
    if (shopeeAccountId === null) return null;
    return (
      accounts?.find((a) => a.id === shopeeAccountId)?.name ??
      `TK #${shopeeAccountId}`
    );
  }, [shopeeAccountId, accounts]);

  if (!batch) return null;

  const handleConfirm = async () => {
    setCommitting(true);
    setError(null);
    try {
      await onConfirm(taxRates);
    } catch (e) {
      setError((e as Error).message ?? String(e));
      setCommitting(false);
    }
  };

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget && !committing) onCancel();
  };

  // 3 trạng thái loại trừ commit: rejected (parse-stage fail), batchDuplicate
  // (cùng hash trong batch). File hash match với DB vẫn cho commit — Rust reuse
  // entry cũ + UPSERT idempotent. Rejected ưu tiên hơn duplicate khi cả 2 cờ
  // bật (rejected file không có rawContent nên không thể là duplicate, nhưng
  // vẫn phòng hờ).
  const isRejected = (p: typeof batch.files[0]["preview"]) => p.rejected === true;
  const isDuplicate = (p: typeof batch.files[0]["preview"]) =>
    !isRejected(p) && p.batchDuplicate;
  const isInactive = (p: typeof batch.files[0]["preview"]) =>
    isRejected(p) || isDuplicate(p);

  const activeFiles = batch.files.filter((f) => !isInactive(f.preview));
  const duplicateFiles = batch.files.filter((f) => isDuplicate(f.preview));
  const rejectedFiles = batch.files.filter((f) => isRejected(f.preview));

  // Multi-day batch: user dễ scan nếu rows nhóm theo ngày. Sort active theo
  // dayDate ASC rồi filename, rejected/duplicate xuống cuối (dayDate="").
  // Map item → original index trong batch.files để state taxRates dùng key
  // ổn định, không bị shift theo sort order.
  const origIndexMap = new Map(
    batch.files.map((item, idx) => [item, idx] as const),
  );
  const sortedFiles = [...batch.files].sort((a, b) => {
    const aInactive = isInactive(a.preview);
    const bInactive = isInactive(b.preview);
    if (aInactive !== bInactive) return aInactive ? 1 : -1;
    const dateCmp = a.preview.dayDate.localeCompare(b.preview.dayDate);
    if (dateCmp !== 0) return dateCmp;
    return a.preview.filename.localeCompare(b.preview.filename);
  });

  // Detect multi-day để show hint trong footer. Expand range
  // dayDateFrom..dayDateTo cho mỗi file (Shopee có thể span nhiều ngày trong
  // 1 file) → union vào set để count chính xác. ISO date string sort + add
  // 1 day = 86400000ms hoạt động vì JS parse "YYYY-MM-DD" thành UTC midnight.
  const coveredDays = (() => {
    const set = new Set<string>();
    for (const f of activeFiles) {
      const from = f.preview.dayDateFrom;
      const to = f.preview.dayDateTo || from;
      if (!from) continue;
      const startMs = Date.parse(from);
      const endMs = Date.parse(to);
      if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
        set.add(from);
        continue;
      }
      for (let ms = startMs; ms <= endMs; ms += 86_400_000) {
        set.add(new Date(ms).toISOString().slice(0, 10));
      }
    }
    return set;
  })();
  const isMultiDay = coveredDays.size > 1;
  const hashMatchActive = activeFiles.filter((f) => f.preview.hashMatch);
  const totalReplace = activeFiles.reduce(
    (a, f) => a + f.preview.replaceRows,
    0,
  );
  const totalNew = activeFiles.reduce((a, f) => a + f.preview.newRows, 0);
  const totalSkipped = activeFiles.reduce((a, f) => a + f.preview.skipped, 0);
  const anyDayHasData = activeFiles.some((f) => f.preview.dayHasData);
  const mostlyEmptyFiles = activeFiles.filter((f) => f.preview.mostlyEmpty);
  const hasReplacements = totalReplace > 0;
  const hasAnyShopee = activeFiles.some(
    (f) =>
      f.parsed.kind === "shopee_clicks" ||
      f.parsed.kind === "shopee_commission",
  );
  // Không có gì để commit — chỉ rejected/duplicate. Disable confirm button.
  const noActiveFiles = activeFiles.length === 0;

  // Date range chỉ tính trên file active — rejected có dayDate="" sẽ làm hỏng
  // sort. Nếu mọi file inactive → header hiện "Không có file để import".
  const dateRange = (() => {
    if (activeFiles.length === 0) return null;
    const from = activeFiles
      .map((f) => f.preview.dayDateFrom)
      .sort()[0];
    const to = activeFiles
      .map((f) => f.preview.dayDateTo)
      .sort()
      .reverse()[0];
    return from === to ? fmtDate(from) : `${fmtDate(from)} → ${fmtDate(to)}`;
  })();

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
              {dateRange ?? "Không có file hợp lệ để import"}
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
          {hasAnyShopee && accountName && (
            <div className="rounded-lg border border-shopee-500/30 bg-shopee-500/10 px-4 py-2 text-sm text-shopee-200">
              <span className="material-symbols-rounded align-middle text-base">
                account_circle
              </span>{" "}
              Shopee files sẽ tag về TK: <b>{accountName}</b>
            </div>
          )}

          {rejectedFiles.length > 0 && (
            <div
              role="alert"
              className="rounded-lg border-2 border-red-500 bg-red-950/40 px-4 py-3 text-sm"
            >
              <p className="mb-2 font-semibold text-red-200">
                <span className="material-symbols-rounded align-middle text-base">
                  block
                </span>{" "}
                {rejectedFiles.length} file bị bỏ qua (không thể import):
              </p>
              <ul className="space-y-1 pl-6 text-xs text-white/80">
                {rejectedFiles.map((f, i) => (
                  <li key={i}>
                    <div className="truncate font-medium" title={f.preview.filename}>
                      • {f.preview.filename}
                    </div>
                    <div className="pl-3 text-red-300/90">
                      {f.preview.rejectReason ?? "Không rõ lý do"}
                    </div>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {duplicateFiles.length > 0 && (
            <div className="rounded-lg border border-amber-500/40 bg-amber-950/30 px-4 py-3 text-sm">
              <p className="mb-2 font-semibold text-amber-200">
                <span className="material-symbols-rounded align-middle text-base">
                  content_copy
                </span>{" "}
                {duplicateFiles.length} file sẽ bỏ qua (trùng nội dung trong batch):
              </p>
              <ul className="space-y-0.5 pl-6 text-xs text-white/70">
                {duplicateFiles.map((f, i) => (
                  <li key={i} className="truncate" title={f.preview.filename}>
                    • {f.preview.filename}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {hashMatchActive.length > 0 && (
            <div className="rounded-lg border border-blue-500/40 bg-blue-950/30 px-4 py-3 text-sm">
              <p className="mb-2 font-semibold text-blue-200">
                <span className="material-symbols-rounded align-middle text-base">
                  restart_alt
                </span>{" "}
                {hashMatchActive.length} file đã import trước đó — re-import sẽ refresh data:
              </p>
              <ul className="space-y-0.5 pl-6 text-xs text-white/70">
                {hashMatchActive.map((f, i) => (
                  <li key={i} className="truncate" title={f.preview.filename}>
                    • {f.preview.filename}
                    {f.preview.existingDayDate && (
                      <span className="text-white/40">
                        {" "}— lần đầu import ngày {fmtDate(f.preview.existingDayDate)}
                      </span>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {totalSkipped > 0 && (
            <div className="rounded-lg border border-amber-500/40 bg-amber-950/30 px-4 py-2 text-sm text-amber-200">
              <span className="material-symbols-rounded align-middle text-base">
                warning
              </span>{" "}
              {totalSkipped} dòng bị bỏ qua do không parse được ngày (check format CSV)
            </div>
          )}

          {mostlyEmptyFiles.length > 0 && (
            <div
              role="alert"
              className="rounded-lg border-2 border-orange-500 bg-orange-950/40 px-4 py-3 text-sm"
            >
              <p className="mb-2 font-semibold text-orange-200">
                <span className="material-symbols-rounded align-middle text-base">
                  warning
                </span>{" "}
                {mostlyEmptyFiles.length} file FB không có data (spend=0,
                clicks=0) cho phần lớn rows
              </p>
              <ul className="mb-2 space-y-0.5 pl-6 text-xs text-white/80">
                {mostlyEmptyFiles.map((f, i) => (
                  <li key={i} className="truncate" title={f.preview.filename}>
                    • {f.preview.filename}{" "}
                    <span className="text-white/50">
                      ({f.preview.emptyRows}/{f.preview.totalRows} rows trống)
                    </span>
                  </li>
                ))}
              </ul>
              <p className="text-xs text-orange-200/80">
                Data cũ <b>không bị đè</b> bằng 0 (DB guard). Nhưng file này
                cũng KHÔNG add value mới — kiểm tra lại xem có phải file đúng
                không trước khi import.
              </p>
            </div>
          )}

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
                  {isMultiDay
                    ? "Một số ngày trong batch đã có data — sẽ replace một số dòng"
                    : "Ngày này đã có data — sẽ replace một số dòng"}
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
                  <th className="border-b border-surface-8 px-3 py-2 text-left text-[11px] font-semibold uppercase tracking-wider">
                    Ngày
                  </th>
                  <th
                    className="border-b border-surface-8 px-2 py-2 text-center text-[11px] font-semibold uppercase tracking-wider"
                    title="Thuế % cho FB ad spend (VAT/business tax). Spend × (1 + thuế/100) = chi phí thật"
                  >
                    Thuế %
                  </th>
                  <th className="border-b border-surface-8 px-3 py-2 text-right text-[11px] font-semibold uppercase tracking-wider">
                    Tổng
                  </th>
                  <th className="border-b border-surface-8 px-3 py-2 text-right text-[11px] font-semibold uppercase tracking-wider">
                    Thêm
                  </th>
                  <th className="border-b border-surface-8 px-3 py-2 text-right text-[11px] font-semibold uppercase tracking-wider">
                    Replace
                  </th>
                </tr>
              </thead>
              <tbody>
                {sortedFiles.map((item, i) => {
                  const { parsed, preview } = item;
                  const rej = isRejected(preview);
                  const dup = isDuplicate(preview);
                  const inactive = rej || dup;
                  const isFb =
                    parsed.kind === "fb_ad_group" ||
                    parsed.kind === "fb_campaign";
                  const origIdx = origIndexMap.get(item) ?? -1;
                  const taxValue = taxRates[origIdx] ?? 0;
                  const dateCell = rej
                    ? "—"
                    : preview.dayDateFrom === preview.dayDateTo
                      ? fmtDate(preview.dayDateFrom)
                      : `${fmtDate(preview.dayDateFrom)} → ${fmtDate(preview.dayDateTo)}`;
                  const rowClass = rej
                    ? "bg-red-950/20 text-white/40 line-through"
                    : dup
                      ? "bg-amber-950/20 text-white/40 line-through"
                      : "text-white/80 hover:bg-shopee-500/15";
                  return (
                    <tr
                      key={i}
                      className={`border-b border-surface-8 last:border-b-0 transition-colors ${rowClass}`}
                    >
                      <td className="px-3 py-2">
                        {rej ? (
                          <span className="inline-flex items-center gap-1.5 rounded-full bg-red-900/40 px-2 py-0.5 text-xs font-medium text-red-200 no-underline">
                            <span className="material-symbols-rounded text-[12px]">
                              block
                            </span>
                            Lỗi
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1.5 rounded-full bg-shopee-900/40 px-2 py-0.5 text-xs font-medium text-shopee-200">
                            {kindLabel(preview.kind)}
                          </span>
                        )}
                      </td>
                      <td
                        className="max-w-[260px] truncate px-3 py-2 text-xs"
                        title={
                          rej
                            ? `${preview.filename} — ${preview.rejectReason ?? ""}`
                            : preview.filename
                        }
                      >
                        {preview.filename}
                        {!inactive && preview.hashMatch && (
                          <span className="ml-2 inline-flex items-center gap-1 rounded-full bg-blue-500/20 px-2 py-0.5 text-[10px] font-medium text-blue-300">
                            <span className="material-symbols-rounded text-[10px]">
                              restart_alt
                            </span>
                            Đã import
                          </span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-xs text-white/60 tabular-nums">
                        {dateCell}
                      </td>
                      <td className="px-1 py-1 text-center">
                        {isFb && !inactive ? (
                          <input
                            type="number"
                            min={0}
                            max={100}
                            step={0.01}
                            // Hiển thị empty khi tax=0 (default) để user gõ
                            // "10" KHÔNG ra "010". placeholder show "0" gợi ý
                            // default. Khi taxValue=0 → input rỗng → user gõ
                            // "1" thành "1" thẳng, không append vào "0".
                            value={taxValue === 0 ? "" : taxValue}
                            placeholder="0"
                            disabled={committing}
                            // Wheel scroll mặc định tăng/giảm number input —
                            // gây khó chịu khi user scroll trong dialog.
                            // Blur input khi wheel → mất focus → wheel
                            // không sửa value.
                            onWheel={(e) => (e.target as HTMLInputElement).blur()}
                            onChange={(e) => {
                              const raw = e.target.value;
                              // Empty input → reset về 0 (không phải undefined để
                              // Number(...) downstream khỏi NaN). Người dùng xóa hết
                              // thường có ý reset.
                              if (raw === "") {
                                setTaxRates((m) => ({ ...m, [origIdx]: 0 }));
                                return;
                              }
                              const n = Number(raw);
                              if (!Number.isFinite(n)) return;
                              // Clamp 0..100. Decimal precision do step=0.01 lo;
                              // không round thêm vì user gõ "8.25" → giữ nguyên f64.
                              const clamped = Math.min(100, Math.max(0, n));
                              setTaxRates((m) => ({ ...m, [origIdx]: clamped }));
                            }}
                            className="w-16 rounded border border-surface-8 bg-surface-2 px-1.5 py-0.5 text-center text-xs tabular-nums text-white/85 focus:border-shopee-400 focus:outline-none disabled:opacity-50"
                            aria-label={`Thuế % cho ${preview.filename}`}
                          />
                        ) : (
                          <span className="text-white/30">—</span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums">
                        {rej ? "—" : fmtInt(preview.totalRows)}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums text-green-400">
                        {!inactive && preview.newRows > 0 ? "+" : ""}
                        {inactive ? "—" : fmtInt(preview.newRows)}
                      </td>
                      <td
                        className={`px-3 py-2 text-right tabular-nums ${
                          !inactive && preview.replaceRows > 0
                            ? "text-amber-300"
                            : "text-white/30"
                        }`}
                      >
                        {inactive
                          ? "—"
                          : preview.replaceRows > 0
                            ? `⟳ ${fmtInt(preview.replaceRows)}`
                            : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {error && (
            <div className="rounded-lg border border-red-500/50 bg-red-900/30 px-3 py-2 text-sm text-red-200">
              {error}
            </div>
          )}
        </div>

        <footer className="flex shrink-0 items-center justify-between gap-2 border-t border-surface-8 bg-surface-1 px-6 py-3">
          <p className="text-xs text-white/50">
            {noActiveFiles
              ? "Không có file nào để commit."
              : hasReplacements
              ? "Xác nhận sẽ ghi đè các dòng trùng. Data không trùng được giữ nguyên."
              : "Tất cả dòng sẽ được thêm mới."}
            {!noActiveFiles && isMultiDay && (
              <span className="ml-2 text-white/40">
                · {activeFiles.length} file qua {coveredDays.size} ngày,
                mỗi file 1 transaction riêng.
              </span>
            )}
          </p>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={onCancel}
              disabled={committing}
              className="btn-ripple rounded-lg px-5 py-2 text-sm font-medium text-white/80 hover:bg-white/5 disabled:opacity-50"
            >
              {noActiveFiles ? "Đóng" : "Hủy"}
            </button>
            <button
              type="button"
              onClick={handleConfirm}
              disabled={
                committing ||
                noActiveFiles ||
                (hasAnyShopee && shopeeAccountId === null)
              }
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
