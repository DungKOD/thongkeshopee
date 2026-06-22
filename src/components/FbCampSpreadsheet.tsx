import { useCallback, useRef } from "react";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { emptyCampRow, type CampRow } from "../lib/fbAds";

interface FbCampSpreadsheetProps {
  rows: CampRow[];
  onChange: (rows: CampRow[]) => void;
  /** Tắt edit khi đang batch run hoặc lúc save. */
  disabled?: boolean;
}

type RowField = keyof CampRow;

const COLUMNS: { key: RowField; label: string; width: string }[] = [
  { key: "campName", label: "Campaign name", width: "w-44" },
  { key: "adsetName", label: "Ad Set name", width: "w-40" },
  { key: "adName", label: "Ad name", width: "w-40" },
  { key: "caption", label: "Caption / Content", width: "w-72" },
  { key: "videoPath", label: "Video path", width: "w-72" },
  { key: "subId", label: "Sub_id", width: "w-32" },
];

/**
 * Spreadsheet editor cho rows camp. Hỗ trợ:
 * - Edit inline mỗi cell
 * - Paste từ Excel (TSV): copy nhiều rows từ Excel → click bất kỳ cell → Ctrl+V
 *   → app split tab + newline, append vào bảng từ row đó
 * - Nút browse video file cho mỗi row
 * - Add / delete row riêng lẻ
 * - Bulk delete khi tick chọn
 */
export function FbCampSpreadsheet({
  rows,
  onChange,
  disabled = false,
}: FbCampSpreadsheetProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  const updateCell = useCallback(
    (rowIdx: number, key: RowField, value: string) => {
      const next = rows.slice();
      next[rowIdx] = { ...next[rowIdx], [key]: value };
      onChange(next);
    },
    [rows, onChange],
  );

  const addRow = useCallback(() => {
    onChange([...rows, emptyCampRow()]);
  }, [rows, onChange]);

  const deleteRow = useCallback(
    (idx: number) => {
      const next = rows.slice();
      next.splice(idx, 1);
      onChange(next.length === 0 ? [emptyCampRow()] : next);
    },
    [rows, onChange],
  );

  const handlePaste = useCallback(
    (e: React.ClipboardEvent<HTMLInputElement>, rowIdx: number, key: RowField) => {
      const text = e.clipboardData.getData("text/plain");
      // Paste single cell → để default behavior.
      if (!text.includes("\t") && !text.includes("\n")) return;

      e.preventDefault();
      const lines = text
        .replace(/\r\n/g, "\n")
        .replace(/\r/g, "\n")
        .split("\n")
        .filter((l, i, arr) => !(i === arr.length - 1 && l === ""));

      const startCol = COLUMNS.findIndex((c) => c.key === key);
      if (startCol < 0) return;

      const next = rows.slice();
      for (let li = 0; li < lines.length; li++) {
        const targetRow = rowIdx + li;
        // Grow rows nếu cần.
        while (next.length <= targetRow) next.push(emptyCampRow());

        const cells = lines[li].split("\t");
        const row = { ...next[targetRow] };
        for (let ci = 0; ci < cells.length; ci++) {
          const col = COLUMNS[startCol + ci];
          if (!col) break;
          row[col.key] = cells[ci];
        }
        next[targetRow] = row;
      }
      onChange(next);
    },
    [rows, onChange],
  );

  const handleBrowseVideo = useCallback(
    async (rowIdx: number) => {
      try {
        const picked = await openDialog({
          multiple: false,
          directory: false,
          filters: [{ name: "Video", extensions: ["mp4", "mov"] }],
        });
        if (typeof picked === "string") {
          updateCell(rowIdx, "videoPath", picked);
        }
      } catch (e) {
        console.error("browse video failed:", e);
      }
    },
    [updateCell],
  );

  const handleClearAll = useCallback(() => {
    if (!confirm("Xóa tất cả rows trong bảng?")) return;
    onChange([emptyCampRow()]);
  }, [onChange]);

  const rowsToShow = rows.length === 0 ? [emptyCampRow()] : rows;

  return (
    <section className="rounded-2xl border border-surface-8 bg-surface-1 p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div>
          <h3 className="text-sm font-semibold uppercase tracking-wider text-white/65">
            Bảng nhập camps
          </h3>
          <p className="text-[11px] text-white/45">
            {rowsToShow.length} rows · Paste từ Excel: copy nhiều rows → click
            cell bắt đầu → Ctrl+V
          </p>
        </div>
        <div className="flex gap-2">
          <button
            type="button"
            onClick={handleClearAll}
            disabled={disabled}
            className="flex items-center gap-1 rounded-md border border-surface-8 px-3 py-1.5 text-xs text-white/70 hover:bg-white/5 disabled:opacity-50"
          >
            <span className="material-symbols-rounded text-sm">clear_all</span>
            Xóa hết
          </button>
          <button
            type="button"
            onClick={addRow}
            disabled={disabled}
            className="btn-ripple flex items-center gap-1 rounded-md bg-violet-500 px-3 py-1.5 text-xs font-medium text-white hover:bg-violet-600 disabled:opacity-50"
          >
            <span className="material-symbols-rounded text-sm">add</span>
            Thêm row
          </button>
        </div>
      </div>

      <div
        ref={containerRef}
        className="overflow-x-auto rounded-lg border border-surface-8 bg-surface-2"
      >
        <table className="w-full border-collapse text-xs">
          <thead className="sticky top-0 z-10 bg-surface-4">
            <tr className="border-b border-surface-8">
              <th className="w-10 px-2 py-2 text-center font-semibold text-white/60">
                #
              </th>
              {COLUMNS.map((c) => (
                <th
                  key={c.key}
                  className={`${c.width} border-l border-surface-8 px-2 py-2 text-left font-semibold text-white/70`}
                >
                  {c.label}
                </th>
              ))}
              <th className="w-10 border-l border-surface-8 px-2 py-2"></th>
            </tr>
          </thead>
          <tbody>
            {rowsToShow.map((row, idx) => (
              <tr
                key={idx}
                className="border-b border-surface-8 last:border-b-0 hover:bg-white/[0.02]"
              >
                <td className="px-2 py-1 text-center font-mono text-[10px] text-white/40">
                  {idx + 1}
                </td>
                {COLUMNS.map((c) => (
                  <td
                    key={c.key}
                    className="border-l border-surface-8 p-0"
                  >
                    {c.key === "videoPath" ? (
                      <div className="flex items-center">
                        <input
                          type="text"
                          value={row[c.key]}
                          onChange={(e) =>
                            updateCell(idx, c.key, e.currentTarget.value)
                          }
                          onPaste={(e) => handlePaste(e, idx, c.key)}
                          disabled={disabled}
                          placeholder="C:\..."
                          className="w-full border-0 bg-transparent px-2 py-1.5 text-[12px] text-white/90 placeholder:text-white/25 focus:bg-violet-950/30 focus:outline-none"
                        />
                        <button
                          type="button"
                          onClick={() => void handleBrowseVideo(idx)}
                          disabled={disabled}
                          className="flex h-7 w-7 shrink-0 items-center justify-center text-violet-300 hover:bg-violet-500/20 disabled:opacity-50"
                          title="Browse file"
                        >
                          <span className="material-symbols-rounded text-base">
                            folder_open
                          </span>
                        </button>
                      </div>
                    ) : (
                      <input
                        type="text"
                        value={row[c.key]}
                        onChange={(e) =>
                          updateCell(idx, c.key, e.currentTarget.value)
                        }
                        onPaste={(e) => handlePaste(e, idx, c.key)}
                        disabled={disabled}
                        className="w-full border-0 bg-transparent px-2 py-1.5 text-[12px] text-white/90 focus:bg-violet-950/30 focus:outline-none"
                      />
                    )}
                  </td>
                ))}
                <td className="border-l border-surface-8 px-2 py-1 text-center">
                  <button
                    type="button"
                    onClick={() => deleteRow(idx)}
                    disabled={disabled}
                    className="flex h-6 w-6 items-center justify-center rounded text-red-300 hover:bg-red-500/20 disabled:opacity-50"
                    title="Xóa row"
                  >
                    <span className="material-symbols-rounded text-sm">
                      delete
                    </span>
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
