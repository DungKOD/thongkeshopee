import { useCallback, useMemo, useState } from "react";
import { openUrl } from "@tauri-apps/plugin-opener";
import {
  fetchShopeeProduct,
  type ShopeeProductInfo,
} from "../lib/shopeeProduct";
import { fmtInt, fmtVnd } from "../formulas";
import { useToast } from "./ToastProvider";

type ItemStatus = "pending" | "fetching" | "done" | "failed";

interface BatchItem {
  id: string;
  url: string;
  status: ItemStatus;
  info: ShopeeProductInfo | null;
  error: string;
}

const MAX_CONCURRENT = 3;
const SHOPEE_URL_RX = /(https?:\/\/[^\s]*(?:shopee\.|shp\.ee|s\.shopee\.)[^\s]*)/gi;

function genId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
}

/** Trích link Shopee từ text user paste — chấp nhận shopee.xx, shp.ee, s.shopee.xx. */
function parseShopeeUrls(text: string): string[] {
  const found = text.match(SHOPEE_URL_RX) ?? [];
  return [...new Set(found.map((s) => s.trim()))];
}

function formatRating(rating: ShopeeProductInfo["rating"]): string {
  if (rating == null) return "—";
  const num = typeof rating === "number" ? rating : Number(rating);
  if (!Number.isFinite(num) || num <= 0) return "—";
  return num.toFixed(2);
}

function dataSourceBadge(source: string): { label: string; tone: string } {
  switch (source) {
    case "api":
      return { label: "Live", tone: "bg-emerald-500/20 text-emerald-300" };
    case "db":
      return { label: "Cache", tone: "bg-blue-500/20 text-blue-300" };
    case "fallback":
      return { label: "Fallback", tone: "bg-amber-500/20 text-amber-300" };
    default:
      return { label: source || "—", tone: "bg-surface-6 text-white/60" };
  }
}

export function ShopeeProductPage() {
  const [urlsText, setUrlsText] = useState("");
  const [items, setItems] = useState<BatchItem[]>([]);
  const [fetchingAll, setFetchingAll] = useState(false);
  /// Input text raw user gõ — giữ string để cho phép định dạng "5.000" /
  /// "5,000" / "5000" mà không phải lock format. Parse → số khi filter.
  const [minCommissionInput, setMinCommissionInput] = useState("");
  const { showToast } = useToast();

  const copyToClipboard = useCallback(
    async (text: string, label: string) => {
      if (!text) return;
      try {
        await navigator.clipboard.writeText(text);
        showToast({ message: `Đã copy ${label}`, duration: 1800 });
      } catch {
        showToast({ message: "Không truy cập được clipboard", duration: 3000 });
      }
    },
    [showToast],
  );

  const urlCount = useMemo(() => parseShopeeUrls(urlsText).length, [urlsText]);
  const doneCount = items.filter((i) => i.status === "done").length;
  const failedCount = items.filter((i) => i.status === "failed").length;
  const fetchingCount = items.filter((i) => i.status === "fetching").length;

  /// Parse min hoa hồng từ input text — strip mọi ký tự không phải digit
  /// (chấp nhận "5.000", "5,000", "5000 đ", "5k" → 5000... à 5k không hỗ trợ).
  /// 0 hoặc NaN = filter tắt.
  const minCommission = useMemo(() => {
    const digits = minCommissionInput.replace(/[^\d]/g, "");
    const n = Number(digits);
    return Number.isFinite(n) && n > 0 ? n : 0;
  }, [minCommissionInput]);
  const filterActive = minCommission > 0;
  /// Row pending/fetching/failed luôn hiện (user cần feedback trạng thái);
  /// chỉ filter row done theo hoa hồng tổng.
  const displayedItems = useMemo(() => {
    if (!filterActive) return items;
    return items.filter(
      (i) =>
        i.status !== "done" || (i.info?.commission ?? 0) >= minCommission,
    );
  }, [items, filterActive, minCommission]);
  /// Counter "X/Y SP đạt" — Y = số SP đã done, X = số SP done thỏa filter.
  const passedCount = useMemo(
    () =>
      items.filter(
        (i) => i.status === "done" && (i.info?.commission ?? 0) >= minCommission,
      ).length,
    [items, minCommission],
  );

  const fetchOne = useCallback(async (item: BatchItem) => {
    setItems((prev) =>
      prev.map((i) =>
        i.id === item.id ? { ...i, status: "fetching", error: "" } : i,
      ),
    );
    try {
      const info = await fetchShopeeProduct(item.url);
      setItems((prev) =>
        prev.map((i) =>
          i.id === item.id ? { ...i, status: "done", info } : i,
        ),
      );
    } catch (e) {
      setItems((prev) =>
        prev.map((i) =>
          i.id === item.id
            ? { ...i, status: "failed", error: String(e) }
            : i,
        ),
      );
    }
  }, []);

  const handleFetchAll = useCallback(async () => {
    const urls = parseShopeeUrls(urlsText);
    if (urls.length === 0 || fetchingAll) return;

    const newItems: BatchItem[] = urls.map((url) => ({
      id: genId(),
      url,
      status: "pending",
      info: null,
      error: "",
    }));
    setItems(newItems);
    setFetchingAll(true);

    // Pool pattern: tối đa MAX_CONCURRENT request đồng thời.
    const pool = new Set<Promise<void>>();
    for (const item of newItems) {
      const p: Promise<void> = fetchOne(item).finally(() => pool.delete(p));
      pool.add(p);
      if (pool.size >= MAX_CONCURRENT) await Promise.race(pool);
    }
    await Promise.all(pool);

    setFetchingAll(false);
  }, [urlsText, fetchingAll, fetchOne]);

  const handlePaste = useCallback(async () => {
    try {
      const text = await navigator.clipboard.readText();
      if (text.trim()) {
        setUrlsText((prev) => (prev ? `${prev}\n${text.trim()}` : text.trim()));
      }
    } catch {
      /* clipboard blocked */
    }
  }, []);

  const handleClear = useCallback(() => {
    setUrlsText("");
    setItems([]);
  }, []);

  const handleRetry = useCallback(
    (id: string) => {
      const target = items.find((i) => i.id === id);
      if (target) void fetchOne(target);
    },
    [items, fetchOne],
  );

  const handleCopyAll = useCallback(async () => {
    const rows = items
      .filter((i) => i.status === "done" && i.info)
      .map((i) => {
        const info = i.info!;
        return [
          info.itemId ?? "",
          info.productName,
          info.shopName,
          info.price,
          info.sales,
          info.commission,
          info.sellerComFinal,
          info.shopeeComFinal,
          formatRating(info.rating),
          info.productLink,
        ].join("\t");
      });
    if (rows.length === 0) return;
    const header =
      "itemId\ttên SP\tshop\tgiá\tđã bán\thoa hồng\tseller\tshopee\trating\tlink";
    await copyToClipboard(
      [header, ...rows].join("\n"),
      `${rows.length} sản phẩm (TSV)`,
    );
  }, [items, copyToClipboard]);

  /** Copy "ID\tTitle" của tất cả row done — dùng paste sang sheet 2 cột. */
  const handleCopyIdsAndTitles = useCallback(async () => {
    const lines = items
      .filter((i) => i.status === "done" && i.info)
      .map((i) => {
        const info = i.info!;
        return `${info.itemId ?? ""}\t${info.productName}`;
      });
    if (lines.length === 0) return;
    await copyToClipboard(lines.join("\n"), `ID + Tên ${lines.length} SP`);
  }, [items, copyToClipboard]);

  return (
    <div className="mx-auto max-w-[1536px] space-y-5">
      {/* ===== Hero ===== */}
      <section className="overflow-hidden rounded-2xl bg-gradient-to-br from-shopee-700 via-shopee-600 to-shopee-500 shadow-elev-4">
        <div className="flex items-center gap-4 px-6 py-5">
          <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/15 text-white shadow-inner">
            <span className="material-symbols-rounded text-2xl">
              storefront
            </span>
          </span>
          <div className="min-w-0 flex-1">
            <h1 className="text-xl font-bold text-white">
              Tra cứu sản phẩm Shopee
            </h1>
            <p className="mt-0.5 text-xs text-white/75">
              Dán nhiều link Shopee · Lấy giá, đã bán, hoa hồng affiliate · Tối
              đa {MAX_CONCURRENT} request song song
            </p>
          </div>
        </div>
      </section>

      {/* ===== URL input ===== */}
      <section className="space-y-3 rounded-2xl bg-surface-2 p-4 shadow-elev-2">
        <label className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
          <span className="material-symbols-rounded text-base">link</span>
          Link sản phẩm Shopee (mỗi link 1 dòng, hỗ trợ short link)
        </label>
        <textarea
          value={urlsText}
          onChange={(e) => setUrlsText(e.target.value)}
          rows={4}
          placeholder={
            "https://shopee.vn/product/...\nhttps://s.shopee.vn/...\nhttps://shp.ee/..."
          }
          className="w-full resize-none rounded-xl border border-surface-8 bg-surface-1 px-4 py-3 font-mono text-sm text-white/90 placeholder:text-white/25 focus:border-shopee-500 focus:outline-none focus:ring-2 focus:ring-shopee-500/30"
        />
        {urlCount > 0 && (
          <div className="flex items-center gap-1.5 text-xs text-white/50">
            <span className="material-symbols-rounded text-sm text-shopee-400">
              tag
            </span>
            <span>
              Đã nhận{" "}
              <span className="font-semibold text-shopee-300">{urlCount}</span>{" "}
              link Shopee hợp lệ
            </span>
          </div>
        )}
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={handlePaste}
            className="btn-ripple flex items-center gap-1.5 rounded-lg bg-surface-4 px-3 py-1.5 text-xs font-medium text-white/80 hover:bg-surface-6"
          >
            <span className="material-symbols-rounded text-sm">
              content_paste
            </span>
            Dán
          </button>
          <button
            type="button"
            onClick={handleClear}
            disabled={!urlsText && items.length === 0}
            className="btn-ripple flex items-center gap-1.5 rounded-lg bg-surface-4 px-3 py-1.5 text-xs font-medium text-white/60 hover:bg-surface-6 disabled:opacity-40"
          >
            <span className="material-symbols-rounded text-sm">close</span>
            Xóa
          </button>
          <div className="flex-1" />
          {doneCount > 0 && (
            <>
              <button
                type="button"
                onClick={() => void handleCopyIdsAndTitles()}
                className="btn-ripple flex items-center gap-1.5 rounded-lg border border-shopee-500/50 bg-shopee-500/10 px-3 py-1.5 text-xs font-medium text-shopee-200 hover:bg-shopee-500/20"
                title="Copy 2 cột: itemId + tên SP — paste sang Excel/Sheets"
              >
                <span className="material-symbols-rounded text-sm">
                  content_copy
                </span>
                Copy ID + Tên
              </button>
              <button
                type="button"
                onClick={() => void handleCopyAll()}
                className="btn-ripple flex items-center gap-1.5 rounded-lg border border-surface-8 bg-surface-1 px-3 py-1.5 text-xs font-medium text-white/80 hover:bg-surface-4"
                title="Copy kết quả đầy đủ dạng TSV để paste vào Excel/Sheets"
              >
                <span className="material-symbols-rounded text-sm">
                  content_copy
                </span>
                Copy TSV đầy đủ
              </button>
            </>
          )}
          <button
            type="button"
            onClick={() => void handleFetchAll()}
            disabled={urlCount === 0 || fetchingAll}
            className="btn-ripple flex items-center gap-2 rounded-xl bg-shopee-500 px-5 py-2 text-sm font-semibold text-white shadow-elev-2 transition-all hover:bg-shopee-600 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <span
              className={`material-symbols-rounded text-base ${fetchingAll ? "animate-spin" : ""}`}
            >
              {fetchingAll ? "sync" : "search"}
            </span>
            {fetchingAll
              ? `Đang tra (${fetchingCount}/${items.length})...`
              : "Tra cứu"}
          </button>
        </div>
        {items.length > 0 && (
          <div className="flex flex-wrap items-center gap-2 border-t border-surface-8 pt-3 text-xs">
            <span className="rounded-full bg-emerald-500/15 px-2.5 py-0.5 font-medium text-emerald-300">
              ✓ {doneCount} thành công
            </span>
            {fetchingCount > 0 && (
              <span className="rounded-full bg-shopee-500/15 px-2.5 py-0.5 font-medium text-shopee-300">
                ⟳ {fetchingCount} đang tra
              </span>
            )}
            {failedCount > 0 && (
              <span className="rounded-full bg-red-500/15 px-2.5 py-0.5 font-medium text-red-300">
                ✗ {failedCount} lỗi
              </span>
            )}
            <div className="flex-1" />
            <label
              className={`flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 font-medium transition-colors ${
                filterActive
                  ? "border-shopee-500/60 bg-shopee-500/15 text-shopee-200"
                  : "border-surface-8 bg-surface-2 text-white/55"
              }`}
              title="Ẩn các sản phẩm có hoa hồng tổng thấp hơn ngưỡng này"
            >
              <span className="material-symbols-rounded text-sm">
                filter_alt
              </span>
              <span className="whitespace-nowrap">Hoa hồng ≥</span>
              <input
                type="text"
                inputMode="numeric"
                value={minCommissionInput}
                onChange={(e) => setMinCommissionInput(e.target.value)}
                placeholder="0"
                className="w-20 bg-transparent text-right tabular-nums text-white/90 placeholder:text-white/30 focus:outline-none"
              />
              <span className="text-white/45">đ</span>
              {filterActive && (
                <button
                  type="button"
                  onClick={() => setMinCommissionInput("")}
                  title="Bỏ filter"
                  className="ml-0.5 flex h-4 w-4 items-center justify-center rounded-full text-white/55 hover:bg-white/10 hover:text-white"
                >
                  <span className="material-symbols-rounded text-xs">
                    close
                  </span>
                </button>
              )}
            </label>
            {filterActive && (
              <span className="rounded-full bg-shopee-500/15 px-2.5 py-0.5 font-medium text-shopee-200">
                {passedCount}/{doneCount} SP đạt
              </span>
            )}
          </div>
        )}
      </section>

      {/* ===== Results table ===== */}
      {items.length === 0 ? (
        <section className="flex flex-col items-center gap-3 rounded-2xl border border-dashed border-surface-8 bg-surface-1 px-6 py-12 text-center text-white/55">
          <span className="material-symbols-rounded text-5xl text-white/25">
            inventory_2
          </span>
          <p className="text-sm">
            Dán link Shopee phía trên rồi bấm "Tra cứu" để lấy thông tin sản
            phẩm.
          </p>
          <p className="text-xs text-white/40">
            Data được cache 24h ở server — tra lại link cũ sẽ nhanh hơn nhiều.
          </p>
        </section>
      ) : (
        <section className="overflow-hidden rounded-2xl border border-surface-8 bg-surface-1 shadow-elev-2">
          <div>
            <table className="w-full table-fixed text-sm">
              <colgroup>
                <col className="w-10" />
                <col className="w-24" />
                <col />
                <col className="w-32" />
                <col className="w-24" />
                <col className="w-52" />
                <col className="w-20" />
                <col className="w-24" />
                <col className="w-16" />
              </colgroup>
              <thead className="bg-surface-2 text-xs uppercase tracking-wider text-white/55">
                <tr>
                  <th className="px-3 py-2 text-left">#</th>
                  <th className="px-3 py-2 text-left">Ảnh</th>
                  <th className="px-3 py-2 text-left">Sản phẩm / Shop</th>
                  <th className="px-3 py-2 text-right">Giá</th>
                  <th className="px-3 py-2 text-right">Đã bán</th>
                  <th className="px-3 py-2 text-right">Hoa hồng</th>
                  <th className="px-3 py-2 text-right">Rating</th>
                  <th className="px-3 py-2 text-center">Nguồn</th>
                  <th className="px-3 py-2 text-center">Mở</th>
                </tr>
              </thead>
              <tbody>
                {displayedItems.length === 0 ? (
                  <tr>
                    <td
                      colSpan={9}
                      className="px-3 py-8 text-center text-sm text-white/55"
                    >
                      <span className="material-symbols-rounded mr-1.5 align-middle text-base text-white/35">
                        filter_alt_off
                      </span>
                      Không có sản phẩm nào đạt hoa hồng ≥{" "}
                      {fmtVnd(minCommission)}
                    </td>
                  </tr>
                ) : (
                  displayedItems.map((item, idx) => (
                    <ProductRow
                      key={item.id}
                      index={idx + 1}
                      item={item}
                      onRetry={handleRetry}
                      onCopy={copyToClipboard}
                    />
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  );
}

interface ProductRowProps {
  index: number;
  item: BatchItem;
  onRetry: (id: string) => void;
  onCopy: (text: string, label: string) => void | Promise<void>;
}

function ProductRow({ index, item, onRetry, onCopy }: ProductRowProps) {
  const info = item.info;
  const link = info?.productLink || item.url;
  const itemIdStr = info?.itemId != null ? String(info.itemId) : "";

  return (
    <tr className="border-t border-surface-8 align-top hover:bg-surface-2/50">
      <td className="px-3 py-3 text-xs text-white/40">{index}</td>

      <td className="px-3 py-3">
        {info?.imageUrl ? (
          <img
            src={info.imageUrl}
            alt={info.productName}
            loading="lazy"
            className="h-16 w-16 rounded-lg object-cover ring-1 ring-surface-8"
          />
        ) : (
          <div className="flex h-16 w-16 items-center justify-center rounded-lg bg-surface-3 text-white/30">
            <span className="material-symbols-rounded text-2xl">
              {item.status === "fetching" ? "sync" : "image_not_supported"}
            </span>
          </div>
        )}
      </td>

      <td className="px-3 py-3">
        {item.status === "fetching" ? (
          <div className="flex items-center gap-2 text-xs text-shopee-300">
            <span className="material-symbols-rounded animate-spin text-sm">
              sync
            </span>
            Đang tra cứu...
            <span className="ml-2 truncate font-mono text-[10px] text-white/40">
              {item.url}
            </span>
          </div>
        ) : item.status === "failed" ? (
          <div className="space-y-1">
            <div className="text-xs font-medium text-red-300">
              ✗ {item.error || "Lỗi không xác định"}
            </div>
            <div className="truncate font-mono text-[10px] text-white/40">
              {item.url}
            </div>
            <button
              type="button"
              onClick={() => onRetry(item.id)}
              className="btn-ripple inline-flex items-center gap-1 rounded-md border border-red-500/40 bg-red-500/10 px-2 py-0.5 text-[11px] font-medium text-red-200 hover:bg-red-500/20"
            >
              <span className="material-symbols-rounded text-xs">refresh</span>
              Thử lại
            </button>
          </div>
        ) : info ? (
          <div className="min-w-0 space-y-1.5">
            <div className="flex items-start gap-2">
              {info.productName ? (
                <button
                  type="button"
                  onClick={() => void onCopy(info.productName, "tên sản phẩm")}
                  title="Click để copy tên sản phẩm"
                  className="btn-ripple line-clamp-2 flex-1 cursor-copy rounded-md px-1.5 py-0.5 text-left text-base font-semibold leading-snug text-white hover:bg-surface-4 hover:text-shopee-200"
                >
                  {info.productName}
                </button>
              ) : (
                <div className="flex-1 px-1.5 py-0.5 text-base font-semibold text-white/50">
                  (không có tên)
                </div>
              )}
              {itemIdStr && info.productName && (
                <button
                  type="button"
                  onClick={() =>
                    void onCopy(
                      `${itemIdStr}\t${info.productName}`,
                      "ID + tên SP",
                    )
                  }
                  title="Copy ID + tên SP (2 cột)"
                  className="btn-ripple flex h-8 w-8 shrink-0 items-center justify-center rounded-md text-white/55 hover:bg-surface-4 hover:text-shopee-300"
                >
                  <span className="material-symbols-rounded text-base">
                    content_copy
                  </span>
                </button>
              )}
            </div>
            {itemIdStr && (
              <button
                type="button"
                onClick={() => void onCopy(itemIdStr, `ID ${itemIdStr}`)}
                title="Click để copy ID"
                className="btn-ripple inline-flex items-center gap-1.5 rounded-md bg-shopee-500/10 px-2 py-1 font-mono text-sm font-semibold text-shopee-200 hover:bg-shopee-500/20"
              >
                <span className="material-symbols-rounded text-base">tag</span>
                {itemIdStr}
              </button>
            )}
            <div className="flex items-center gap-1.5 text-sm text-white/65">
              <span className="material-symbols-rounded text-base">store</span>
              <span className="truncate">{info.shopName || "—"}</span>
              {info.isXtra && (
                <span className="rounded bg-amber-500/20 px-1.5 py-0.5 text-[11px] font-semibold text-amber-300">
                  Xtra
                </span>
              )}
            </div>
          </div>
        ) : (
          <div className="text-xs text-white/40">—</div>
        )}
      </td>

      <td className="px-3 py-3 text-right tabular-nums">
        {info ? (
          <span className="font-semibold text-white/90">
            {fmtVnd(info.price)}
          </span>
        ) : (
          <span className="text-white/30">—</span>
        )}
      </td>

      <td className="px-3 py-3 text-right tabular-nums text-white/80">
        {info ? fmtInt(info.sales) : <span className="text-white/30">—</span>}
      </td>

      <td className="px-3 py-3 tabular-nums">
        {info ? (
          <div className="space-y-1">
            <div className="flex items-baseline justify-between gap-2 border-b border-surface-8 pb-1">
              <span className="text-[10px] uppercase tracking-wider text-white/45">
                Tổng
              </span>
              <span className="text-sm font-bold text-shopee-300">
                {fmtVnd(info.commission)}
              </span>
            </div>
            <div className="flex items-baseline justify-between gap-2">
              <span className="text-xs text-white/60">HH Shopee</span>
              <span className="text-xs font-semibold text-white/85">
                {fmtVnd(info.shopeeComFinal)}
              </span>
            </div>
            <div className="flex items-baseline justify-between gap-2">
              <span className="text-xs text-white/60">HH Seller</span>
              <span className="text-xs font-semibold text-white/85">
                {fmtVnd(info.sellerComFinal)}
              </span>
            </div>
          </div>
        ) : (
          <span className="text-white/30">—</span>
        )}
      </td>

      <td className="px-3 py-3 text-right tabular-nums text-white/80">
        {info ? formatRating(info.rating) : <span className="text-white/30">—</span>}
      </td>

      <td className="px-3 py-3 text-center">
        {info ? (
          <span
            className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold ${dataSourceBadge(info.dataSource).tone}`}
            title={info.lastUpdate ? `Cập nhật: ${info.lastUpdate}` : undefined}
          >
            {dataSourceBadge(info.dataSource).label}
          </span>
        ) : (
          <span className="text-white/30">—</span>
        )}
      </td>

      <td className="px-3 py-3 text-center">
        <button
          type="button"
          onClick={() => void openUrl(link)}
          title="Mở link Shopee"
          className="btn-ripple inline-flex h-7 w-7 items-center justify-center rounded-full text-white/55 hover:bg-shopee-500/15 hover:text-shopee-300"
        >
          <span className="material-symbols-rounded text-base">open_in_new</span>
        </button>
      </td>
    </tr>
  );
}
