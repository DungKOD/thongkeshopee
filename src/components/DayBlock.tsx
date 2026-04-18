import { useState } from "react";
import type { Day, Video } from "../types";
import { computeDayTotals, fmtDate, fmtInt, fmtVnd } from "../formulas";
import { useSettings } from "../hooks/useSettings";
import { VideoRow } from "./VideoRow";
import { ConfirmDialog } from "./ConfirmDialog";
import { ProductDetailDialog } from "./ProductDetailDialog";

interface DayBlockProps {
  day: Day;
  onRemoveDay: () => void;
  onRemoveVideo: (videoId: string) => void;
  onEditDay: () => void;
}

const HEADERS = [
  "Sản phẩm",
  "Click FB",
  "Click Shopee",
  "Đơn giá click",
  "Tổng tiền chạy",
  "Số lượng đơn",
  "Tỷ lệ chuyển đổi",
  "Giá trị đơn hàng",
  "Hoa hồng",
  "Lợi nhuận",
  "Tỷ suất LN",
  "",
];

export function DayBlock({
  day,
  onRemoveDay,
  onRemoveVideo,
  onEditDay,
}: DayBlockProps) {
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);
  const [detailVideo, setDetailVideo] = useState<Video | null>(null);

  const { settings } = useSettings();
  const totals = computeDayTotals(
    day,
    settings.clickSources,
    settings.profitFees,
  );
  const totalsProfitCls =
    totals.profit > 0
      ? "text-green-400"
      : totals.profit < 0
      ? "text-red-400"
      : "text-gray-300";

  const handleDeleteDay = () => {
    if (day.videos.length === 0) {
      onRemoveDay();
    } else {
      setConfirmDeleteOpen(true);
    }
  };

  return (
    <section className="mb-6 overflow-hidden rounded-xl bg-surface-2 shadow-elev-2 transition-shadow hover:shadow-elev-4">
      <header className="flex items-center justify-between border-b border-surface-8 px-5 py-3">
        <div className="flex items-center gap-3">
          <span className="material-symbols-rounded text-shopee-400">
            event
          </span>
          <div className="flex flex-col">
            <span className="text-[11px] font-medium uppercase tracking-wider text-white/50">
              Ngày
            </span>
            <span className="text-base font-semibold tabular-nums text-white/90">
              {fmtDate(day.date)}
            </span>
          </div>
          <div className="ml-4 inline-flex items-center gap-1 rounded-full bg-shopee-900/40 px-3 py-1 text-xs font-medium text-shopee-300">
            <span className="material-symbols-rounded text-sm">
              inventory_2
            </span>
            {day.videos.length} sản phẩm
          </div>
        </div>
        <button
          onClick={handleDeleteDay}
          className="btn-ripple flex h-9 w-9 items-center justify-center rounded-full text-white/60 hover:bg-red-500/10 hover:text-red-400"
          title="Xóa ngày"
          aria-label="Xóa ngày"
        >
          <span className="material-symbols-rounded">delete</span>
        </button>
      </header>

      <div className="overflow-x-auto">
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr className="bg-surface-4 text-shopee-200">
              {HEADERS.map((h, i) => (
                <th
                  key={i}
                  className="border-b border-surface-8 px-3 py-3 text-center text-[11px] font-semibold uppercase tracking-wider whitespace-nowrap"
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {day.videos.length === 0 ? (
              <tr>
                <td
                  colSpan={HEADERS.length}
                  className="border border-gray-700 px-4 py-6 text-center text-sm text-gray-500"
                >
                  Chưa có sản phẩm — bấm "+ Thêm sản phẩm" bên dưới
                </td>
              </tr>
            ) : (
              day.videos.map((v) => (
                <VideoRow
                  key={v.id}
                  video={v}
                  onEdit={onEditDay}
                  onRemove={() => onRemoveVideo(v.id)}
                  onViewDetail={() => setDetailVideo(v)}
                />
              ))
            )}
          </tbody>
          {day.videos.length > 0 && (
            <tfoot>
              <tr className="border-t-2 border-shopee-500 bg-surface-4 font-semibold text-white/90">
                <td className="px-3 py-3 text-center">Tổng</td>
                <td className="px-3 py-3 text-center tabular-nums">
                  {fmtInt(totals.clicks)}
                </td>
                <td className="px-3 py-3 text-center tabular-nums">
                  {fmtInt(totals.shopeeClicks)}
                </td>
                <td />
                <td className="px-3 py-3 text-center tabular-nums">
                  {fmtVnd(totals.totalSpend)}
                </td>
                <td />
                <td />
                <td />
                <td className="px-3 py-3 text-center tabular-nums">
                  {fmtVnd(totals.commission)}
                </td>
                <td
                  className={`px-3 py-3 text-center tabular-nums ${totalsProfitCls}`}
                >
                  {fmtVnd(totals.profit)}
                </td>
                <td />
                <td />
              </tr>
            </tfoot>
          )}
        </table>
      </div>

      <div className="border-t border-surface-8 bg-surface-1 px-5 py-2">
        <button
          onClick={onEditDay}
          className="btn-ripple flex items-center gap-2 rounded-lg px-3 py-2 text-sm font-medium text-shopee-400 hover:bg-shopee-500/10 active:bg-shopee-500/20"
        >
          <span className="material-symbols-rounded text-base">add</span>
          Thêm sản phẩm
        </button>
      </div>

      <ConfirmDialog
        isOpen={confirmDeleteOpen}
        title="Xóa ngày"
        message={`Ngày ${fmtDate(day.date)} đang có ${day.videos.length} sản phẩm. Xóa ngày sẽ mất toàn bộ dữ liệu. Bạn có chắc muốn xóa?`}
        confirmLabel="Xóa"
        danger
        onConfirm={() => {
          setConfirmDeleteOpen(false);
          onRemoveDay();
        }}
        onClose={() => setConfirmDeleteOpen(false)}
      />

      <ProductDetailDialog
        isOpen={!!detailVideo}
        video={detailVideo}
        onClose={() => setDetailVideo(null)}
      />
    </section>
  );
}
