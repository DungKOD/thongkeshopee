import { useState } from "react";
import {
  fmtInt,
  fmtPct,
  fmtVnd,
  type AggregatedProductRow,
  type SourceFilter,
} from "../formulas";
import { ROI_TOOLTIP } from "./OverviewKpiSection";

/**
 * Header cho bảng sản phẩm aggregate. `adsOnly=true` → chỉ show khi source=all.
 * `showAdsLabel` + `shopeeLabel` → label thay đổi theo source (vd Lợi nhuận ↔ Hoa hồng ròng).
 */
const PRODUCT_HEADERS: Array<{
  label: string;
  tooltip?: string;
  align: "narrow" | "left" | "center";
  adsOnly?: boolean;
  showAdsLabel?: boolean;
  shopeeLabel?: string;
}> = [
  { label: "#", align: "narrow" },
  { label: "Sản phẩm", align: "left" },
  { label: "Ngày", tooltip: "Số ngày sản phẩm này có data trong khoảng", align: "center" },
  {
    label: "Click ADS",
    tooltip: "Tổng click quảng cáo FB (link_clicks) — sum qua tất cả ngày",
    align: "center",
    adsOnly: true,
  },
  {
    label: "Click Shopee",
    tooltip: "Tổng click affiliate về Shopee (lọc theo nguồn trong Cài đặt) — sum qua tất cả ngày",
    align: "center",
  },
  {
    label: "Tổng tiền chạy",
    tooltip: "Spend FB (đã trừ ngân sách chưa tiêu) — sum qua tất cả ngày",
    align: "center",
    adsOnly: true,
  },
  {
    label: "Số đơn",
    tooltip: "Tổng số đơn (COUNT DISTINCT order_id) — sum qua tất cả ngày",
    align: "center",
  },
  {
    label: "CR",
    tooltip: "CR = Tổng số đơn / Tổng Click Shopee × 100%",
    align: "center",
  },
  {
    label: "GMV",
    tooltip: "Tổng Giá trị đơn hàng — sum qua tất cả ngày",
    align: "center",
  },
  {
    label: "Hoa hồng",
    tooltip: "Tổng hoa hồng gross (chưa trừ phí sàn/thuế) — sum qua tất cả ngày",
    align: "center",
  },
  {
    label: "Lợi nhuận",
    shopeeLabel: "Hoa hồng ròng",
    showAdsLabel: true,
    tooltip:
      "Lợi nhuận = Hoa hồng × (1 − thuế − dự phòng) − Tiền ads.\nShopee-only: không trừ ads → bằng Hoa hồng ròng.",
    align: "center",
  },
  { label: "ROI", tooltip: ROI_TOOLTIP, align: "center", adsOnly: true },
];

export function ProductsTable({
  rows,
  source,
  onSelectProduct,
}: {
  rows: AggregatedProductRow[];
  source: SourceFilter;
  onSelectProduct: (row: AggregatedProductRow) => void;
}) {
  const showAds = source === "all";
  if (rows.length === 0) {
    return (
      <section className="rounded-xl border border-dashed border-surface-8 bg-surface-1 px-6 py-10 text-center text-white/60">
        <span className="material-symbols-rounded mb-2 block text-4xl text-shopee-400">
          inventory_2
        </span>
        Không có sản phẩm nào trong khoảng + filter đã chọn
      </section>
    );
  }

  return (
    <section className="overflow-hidden rounded-xl bg-surface-2 shadow-elev-2">
      <header className="flex items-center justify-between border-b border-surface-8 px-5 py-3">
        <div className="flex items-center gap-2">
          <span className="material-symbols-rounded text-shopee-400">inventory_2</span>
          <h3 className="text-sm font-semibold uppercase tracking-wider text-white/85">
            Sản phẩm (tổng hợp)
          </h3>
          <span className="rounded-full bg-shopee-900/40 px-2 py-0.5 text-xs text-shopee-300">
            {fmtInt(rows.length)}
          </span>
        </div>
        <span className="text-xs text-white/50">
          Sắp xếp theo {showAds ? "Lợi nhuận" : "Hoa hồng ròng"} ↓
        </span>
      </header>

      <div>
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr className="border-b-2 border-shopee-500/50 bg-gradient-to-b from-shopee-900/35 to-shopee-900/15 text-shopee-100">
              {PRODUCT_HEADERS.filter((h) => showAds || !h.adsOnly).map((h) => (
                <th
                  key={h.label}
                  title={h.tooltip}
                  className={`py-3.5 text-xs font-bold uppercase tracking-wider whitespace-nowrap ${
                    h.align === "left"
                      ? "min-w-[220px] px-4 text-left"
                      : h.align === "narrow"
                      ? "w-12 px-2 text-center"
                      : "px-3 text-center"
                  } ${h.tooltip ? "cursor-help" : ""}`}
                >
                  <span className="inline-flex items-center gap-1">
                    <span>{h.showAdsLabel && !showAds ? h.shopeeLabel : h.label}</span>
                    {h.tooltip && (
                      <span className="text-[11px] leading-none text-shopee-300/60">ⓘ</span>
                    )}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <ProductRow
                key={r.subIds.join("\x1f")}
                row={r}
                index={i + 1}
                showAds={showAds}
                onClick={() => onSelectProduct(r)}
              />
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function ProductRow({
  row,
  index,
  showAds,
  onClick,
}: {
  row: AggregatedProductRow;
  index: number;
  showAds: boolean;
  onClick: () => void;
}) {
  const [copied, setCopied] = useState(false);
  const cr =
    row.shopeeClicks > 0 ? (row.ordersCount / row.shopeeClicks) * 100 : null;
  const roi = row.totalSpend > 0 ? (row.profit / row.totalSpend) * 100 : null;
  const profitCls =
    row.profit > 0
      ? "text-green-400"
      : row.profit < 0
      ? "text-red-400"
      : "text-gray-400";
  const cellCls = "px-3 py-2.5 text-center";
  const naCls = "text-white/30";

  function handleCopy(e: React.MouseEvent) {
    e.stopPropagation();
    if (!row.displayName) return;
    navigator.clipboard.writeText(row.displayName).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }

  return (
    <tr
      onClick={() => {
        // Không trigger dialog nếu user đang bôi đen text trong row.
        const selection = window.getSelection();
        if (selection && selection.toString().length > 0) return;
        onClick();
      }}
      className="h-[52px] cursor-pointer border-b border-surface-8 text-white/80 transition-colors hover:bg-shopee-500/15 hover:shadow-[inset_3px_0_0_0] hover:shadow-shopee-500"
      title={row.displayName}
    >
      <td className="w-12 px-2 py-2.5 text-center text-sm tabular-nums text-white/50">
        {index}
      </td>
      <td
        className="max-w-[280px] px-4 py-2.5 text-left text-sm font-semibold text-white"
        title={row.displayName}
      >
        <div className="flex min-w-0 items-center gap-1.5">
          <span className="truncate">
            {row.displayName || (
              <span className="italic font-normal text-gray-500">(chưa đặt tên)</span>
            )}
          </span>
          {row.displayName && (
            <button
              type="button"
              onClick={handleCopy}
              title={copied ? "Đã copy!" : "Copy tên sản phẩm"}
              className={`flex-none flex h-5 w-5 items-center justify-center rounded transition-all ${
                copied
                  ? "text-green-400"
                  : "text-white/20 hover:text-white/80"
              }`}
            >
              <span className="material-symbols-rounded text-[13px]">
                {copied ? "check" : "content_copy"}
              </span>
            </button>
          )}
        </div>
      </td>
      <td className={`${cellCls} tabular-nums text-white/60`}>{row.daysActive}</td>
      {showAds && (
        <td className={`${cellCls} tabular-nums ${row.adsClicks === 0 ? naCls : ""}`}>
          {row.adsClicks > 0 ? fmtInt(row.adsClicks) : "—"}
        </td>
      )}
      <td className={`${cellCls} tabular-nums`}>{fmtInt(row.shopeeClicks)}</td>
      {showAds && (
        <td className={`${cellCls} tabular-nums ${row.totalSpend === 0 ? naCls : ""}`}>
          {row.totalSpend > 0 ? fmtVnd(row.totalSpend) : "—"}
        </td>
      )}
      <td className={`${cellCls} tabular-nums`}>{fmtInt(row.ordersCount)}</td>
      <td className={`${cellCls} tabular-nums text-gray-400 ${cr === null ? naCls : ""}`}>
        {cr !== null ? fmtPct(cr) : "—"}
      </td>
      <td className={`${cellCls} tabular-nums text-gray-400 ${row.ordersCount === 0 ? naCls : ""}`}>
        {row.ordersCount > 0 ? fmtVnd(row.orderValueTotal) : "—"}
      </td>
      <td className={`${cellCls} tabular-nums`}>{fmtVnd(row.commissionTotal)}</td>
      <td className={`${cellCls} tabular-nums font-medium ${profitCls}`}>
        {fmtVnd(row.profit)}
      </td>
      {showAds && (
        <td className={`${cellCls} tabular-nums ${roi === null ? naCls : profitCls}`}>
          {roi !== null ? fmtPct(roi) : "—"}
        </td>
      )}
    </tr>
  );
}
