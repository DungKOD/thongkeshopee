import type { Video } from "../types";
import { computeVideo, fmtInt, fmtPct, fmtVnd } from "../formulas";
import { sumFiltered, useSettings } from "../hooks/useSettings";

interface VideoRowProps {
  video: Video;
  onEdit: () => void;
  onRemove: () => void;
  onViewDetail: () => void;
}

const cellCls = "px-3 py-2.5 text-center";

export function VideoRow({
  video,
  onEdit,
  onRemove,
  onViewDetail,
}: VideoRowProps) {
  const { settings } = useSettings();
  const c = computeVideo(video, settings.profitFees);
  const shopeeClicks = sumFiltered(
    video.shopeeClicksByReferrer,
    settings.clickSources,
  );
  const profitCls =
    c.profit > 0
      ? "text-green-400"
      : c.profit < 0
      ? "text-red-400"
      : "text-gray-400";

  return (
    <tr
      onClick={onViewDetail}
      className="cursor-pointer border-b border-surface-8 text-white/80 transition-colors hover:bg-surface-4"
    >
      <td className={cellCls}>
        {video.name || (
          <span className="italic text-gray-500">(chưa đặt tên)</span>
        )}
      </td>
      <td className={`${cellCls} tabular-nums`}>{fmtInt(video.clicks)}</td>
      <td className={`${cellCls} tabular-nums`}>{fmtInt(shopeeClicks)}</td>
      <td className={`${cellCls} tabular-nums text-gray-400`}>
        {fmtVnd(c.cpc)}
      </td>
      <td className={`${cellCls} tabular-nums`}>{fmtVnd(video.totalSpend)}</td>
      <td className={`${cellCls} tabular-nums`}>{fmtInt(video.orders)}</td>
      <td className={`${cellCls} tabular-nums text-gray-400`}>
        {fmtPct(c.conversionRate)}
      </td>
      <td className={`${cellCls} tabular-nums text-gray-400`}>
        {fmtVnd(c.orderValue)}
      </td>
      <td className={`${cellCls} tabular-nums`}>{fmtVnd(video.commission)}</td>
      <td className={`${cellCls} tabular-nums font-medium ${profitCls}`}>
        {fmtVnd(c.profit)}
      </td>
      <td className={`${cellCls} tabular-nums ${profitCls}`}>
        {fmtPct(c.profitMargin)}
      </td>
      <td className={cellCls}>
        <div className="flex justify-center gap-0.5">
          <button
            onClick={(e) => {
              e.stopPropagation();
              onEdit();
            }}
            className="btn-ripple flex h-8 w-8 items-center justify-center rounded-full text-shopee-400 hover:bg-shopee-500/10"
            title="Sửa"
            aria-label="Sửa"
          >
            <span className="material-symbols-rounded text-lg">edit</span>
          </button>
          <button
            onClick={(e) => {
              e.stopPropagation();
              onRemove();
            }}
            className="btn-ripple flex h-8 w-8 items-center justify-center rounded-full text-white/60 hover:bg-red-500/10 hover:text-red-400"
            title="Xóa"
            aria-label="Xóa"
          >
            <span className="material-symbols-rounded text-lg">delete</span>
          </button>
        </div>
      </td>
    </tr>
  );
}
