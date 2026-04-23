import { useCallback, useEffect, useMemo, useState } from "react";
import {
  listImportedFiles,
  revertImport,
  type ImportedFileInfo,
} from "../lib/imports";
import { ConfirmDialog } from "./ConfirmDialog";

interface ImportHistorySectionProps {
  /** Trigger reload data ngoài — bump khi parent biết import/delete mới (optional). */
  reloadKey?: number;
  /** Callback sau khi revert thành công — parent reload overview/list days. */
  onReverted?: () => void;
}

const KIND_LABEL: Record<string, string> = {
  shopee_clicks: "Click Shopee",
  shopee_commission: "Đơn hàng Shopee",
  fb_ad_group: "FB Ad Group",
  fb_campaign: "FB Campaign",
};

const KIND_COLOR: Record<string, string> = {
  shopee_clicks: "bg-blue-900/40 text-blue-200 border-blue-700/40",
  shopee_commission: "bg-emerald-900/40 text-emerald-200 border-emerald-700/40",
  fb_ad_group: "bg-violet-900/40 text-violet-200 border-violet-700/40",
  fb_campaign: "bg-fuchsia-900/40 text-fuchsia-200 border-fuchsia-700/40",
};

export function ImportHistorySection({
  reloadKey,
  onReverted,
}: ImportHistorySectionProps) {
  const [files, setFiles] = useState<ImportedFileInfo[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [confirmFile, setConfirmFile] = useState<ImportedFileInfo | null>(null);
  const [reverting, setReverting] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const rows = await listImportedFiles();
      setFiles(rows);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load, reloadKey]);

  const handleConfirmRevert = useCallback(async () => {
    if (!confirmFile) return;
    setReverting(true);
    setError(null);
    try {
      await revertImport(confirmFile.id);
      setConfirmFile(null);
      await load();
      onReverted?.();
    } catch (e) {
      setError(`Hoàn tác thất bại: ${(e as Error).message}`);
    } finally {
      setReverting(false);
    }
  }, [confirmFile, load, onReverted]);

  const { activeCount, revertedCount } = useMemo(() => {
    if (!files) return { activeCount: 0, revertedCount: 0 };
    let active = 0;
    let reverted = 0;
    for (const f of files) {
      if (f.revertedAt) reverted++;
      else active++;
    }
    return { activeCount: active, revertedCount: reverted };
  }, [files]);

  return (
    <section>
      <div className="mb-3 flex items-center justify-between gap-3">
        <h3 className="flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
          <span className="material-symbols-rounded text-base">history</span>
          Lịch sử import
        </h3>
        {files && files.length > 0 && (
          <div className="flex items-center gap-3 text-xs text-white/55">
            <span>{activeCount} active</span>
            {revertedCount > 0 && (
              <span className="text-rose-300/70">
                {revertedCount} đã hoàn tác
              </span>
            )}
            <button
              type="button"
              onClick={() => void load()}
              disabled={loading}
              className="btn-ripple flex items-center gap-1 rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-[11px] text-white/70 hover:bg-surface-4 disabled:opacity-50"
              title="Tải lại"
            >
              <span
                className={`material-symbols-rounded text-sm ${loading ? "animate-spin" : ""}`}
              >
                refresh
              </span>
              Tải lại
            </button>
          </div>
        )}
      </div>

      {error && (
        <div className="mb-3 rounded-lg border border-rose-700 bg-rose-900/40 px-3 py-2 text-xs text-rose-200">
          {error}
        </div>
      )}

      {loading && !files ? (
        <div className="rounded-xl border border-dashed border-surface-12 bg-surface-2 px-4 py-8 text-center text-sm text-white/50">
          Đang tải...
        </div>
      ) : !files || files.length === 0 ? (
        <div className="flex items-center gap-3 rounded-xl border border-dashed border-surface-12 bg-surface-2 px-4 py-4 text-sm text-white/50">
          <span className="material-symbols-rounded text-base">info</span>
          Chưa có lịch sử import.
        </div>
      ) : (
        <div className="overflow-hidden rounded-xl bg-surface-6 shadow-elev-1">
          <div className="max-h-[320px] overflow-y-auto">
            <table className="w-full text-left text-sm">
              <thead className="sticky top-0 bg-surface-4 text-[11px] uppercase tracking-wider text-white/50">
                <tr>
                  <th className="px-3 py-2 font-medium">File</th>
                  <th className="px-3 py-2 font-medium">Loại</th>
                  <th className="px-3 py-2 font-medium">Account</th>
                  <th className="px-3 py-2 font-medium text-right">Ngày</th>
                  <th className="px-3 py-2 font-medium text-right">
                    Rows active
                  </th>
                  <th className="px-3 py-2 font-medium text-right">
                    Imported at
                  </th>
                  <th className="px-3 py-2 font-medium text-center">
                    Hành động
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-surface-8">
                {files.map((f) => {
                  const reverted = !!f.revertedAt;
                  const kindLabel = KIND_LABEL[f.kind] ?? f.kind;
                  const kindColor =
                    KIND_COLOR[f.kind] ??
                    "bg-surface-2 text-white/60 border-surface-8";
                  return (
                    <tr
                      key={f.id}
                      className={
                        reverted
                          ? "bg-surface-2/30 text-white/40"
                          : "hover:bg-white/5"
                      }
                    >
                      <td
                        className="max-w-[280px] truncate px-3 py-2"
                        title={f.filename}
                      >
                        {reverted && (
                          <span
                            className="mr-1.5 inline-flex items-center rounded-md border border-rose-700/40 bg-rose-900/30 px-1.5 py-0.5 text-[10px] font-semibold text-rose-300"
                            title={`Hoàn tác lúc ${fmtDateTime(f.revertedAt ?? "")}`}
                          >
                            HOÀN TÁC
                          </span>
                        )}
                        <span
                          className={reverted ? "line-through opacity-70" : ""}
                        >
                          {f.filename}
                        </span>
                      </td>
                      <td className="px-3 py-2">
                        <span
                          className={`inline-flex items-center rounded-md border px-2 py-0.5 text-[11px] font-medium ${kindColor}`}
                        >
                          {kindLabel}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-white/70">
                        {f.accountName ?? (
                          <span className="text-white/30">—</span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums text-white/70">
                        {f.dayDate ?? "—"}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums">
                        {f.activeRows.toLocaleString("vi-VN")}
                        {f.activeRows !== f.rowCount && (
                          <span
                            className="ml-1 text-[10px] text-white/40"
                            title={`Row count ban đầu: ${f.rowCount}`}
                          >
                            /{f.rowCount}
                          </span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums text-[11px] text-white/60">
                        {fmtDateTime(f.importedAt)}
                      </td>
                      <td className="px-3 py-2 text-center">
                        {reverted ? (
                          <span className="text-[11px] text-white/40">
                            {fmtDateTime(f.revertedAt ?? "")}
                          </span>
                        ) : (
                          <button
                            type="button"
                            onClick={() => setConfirmFile(f)}
                            className="btn-ripple inline-flex items-center gap-1 rounded-md border border-rose-700/50 bg-rose-900/30 px-2 py-1 text-[11px] text-rose-200 hover:bg-rose-800/50"
                            title="Hoàn tác import file này"
                          >
                            <span className="material-symbols-rounded text-sm">
                              undo
                            </span>
                            Hoàn tác
                          </button>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <p className="mt-2 text-[11px] text-white/40">
        Hoàn tác xóa dòng chỉ có trong file này. Nếu 2 file chứa dòng trùng,
        phải hoàn tác cả 2 mới thay đổi dữ liệu. File đã hoàn tác giữ trong
        lịch sử — user có thể import lại cùng file để khôi phục.
      </p>

      <ConfirmDialog
        isOpen={confirmFile !== null}
        title={
          confirmFile
            ? `Hoàn tác import "${confirmFile.filename}"`
            : "Hoàn tác import"
        }
        message={
          confirmFile ? (
            <div className="space-y-2">
              <div className="space-y-0.5 rounded-lg border border-red-500/40 bg-red-950/40 px-3 py-2 font-mono text-xs text-red-100">
                <div>
                  <span className="text-red-300/70">File:</span>{" "}
                  {confirmFile.filename}
                </div>
                <div>
                  <span className="text-red-300/70">Loại:</span>{" "}
                  {KIND_LABEL[confirmFile.kind] ?? confirmFile.kind}
                </div>
                <div>
                  <span className="text-red-300/70">Account:</span>{" "}
                  {confirmFile.accountName ?? "—"}
                </div>
                <div>
                  <span className="text-red-300/70">Ngày:</span>{" "}
                  {confirmFile.dayDate ?? "—"}
                </div>
                <div>
                  <span className="text-red-300/70">Rows active:</span>{" "}
                  {confirmFile.activeRows.toLocaleString("vi-VN")}
                </div>
              </div>
              <p>
                <b>Chỉ xóa dòng CHỈ CÓ trong file này.</b> Nếu file khác
                cùng chứa dòng trùng → dòng đó được GIỮ LẠI (phải hoàn tác
                cả 2 file mới mất data).
              </p>
              <p className="text-white/70">
                File đã hoàn tác lưu trong lịch sử. Muốn khôi phục → import
                lại cùng file.
              </p>
            </div>
          ) : (
            ""
          )
        }
        confirmLabel={reverting ? "Đang hoàn tác..." : "Xác nhận hoàn tác"}
        cancelLabel="Quay lại"
        danger
        onConfirm={() => void handleConfirmRevert()}
        onClose={() => {
          if (!reverting) setConfirmFile(null);
        }}
      />
    </section>
  );
}

function fmtDateTime(iso: string): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString("vi-VN", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
