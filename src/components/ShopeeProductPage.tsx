import { useCallback, useMemo, useRef, useState } from "react";
import { openUrl } from "@tauri-apps/plugin-opener";
import {
  fetchShopeeProduct,
  type ShopeeProductInfo,
} from "../lib/shopeeProduct";
import { generateFbContent } from "../lib/aiContent";
import { useSettings, type AiContentSettings } from "../hooks/useSettings";
import { fmtInt, fmtVnd } from "../formulas";
import { useToast } from "./ToastProvider";
import { ConfirmDialog } from "./ConfirmDialog";

type ItemStatus = "pending" | "fetching" | "done" | "failed";
type AiStatus = "idle" | "generating" | "done" | "failed";

interface BatchItem {
  id: string;
  url: string;
  status: ItemStatus;
  info: ShopeeProductInfo | null;
  error: string;
  aiStatus: AiStatus;
  aiContent: string;
  aiError: string;
}

const MAX_CONCURRENT = 3;
/// AI request chậm hơn (~5-10s) và costs tiền — giới hạn 2 song song để
/// không spike rate limit của OpenAI (tier 1 = 500 RPM, đủ thoải mái nhưng
/// 2 vẫn an toàn cho user paste 50-100 link).
const MAX_AI_CONCURRENT = 2;
/// Ngưỡng cảnh báo tiền: > N link + AI bật + có API key → confirm dialog
/// nhắc user là sắp gọi OpenAI N lần (mỗi link = 1 request tốn token).
const AI_COST_WARN_THRESHOLD = 10;
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
  const [minSalesInput, setMinSalesInput] = useState("");
  /// Khi user bấm "Tra cứu" mà thỏa điều kiện cảnh báo cost AI → mở dialog
  /// xác nhận. null = không pending; mảng url = chờ confirm thì fetch.
  const [pendingAiConfirm, setPendingAiConfirm] = useState<string[] | null>(null);
  const { showToast } = useToast();
  const { settings, setAiContent } = useSettings();
  const ai = settings.aiContent;
  /// Snapshot AI config trong ref để fetchOne callback luôn đọc value mới
  /// nhất (user có thể bật/tắt giữa batch). Ref tránh phải đưa ai vào
  /// dependency của fetchOne → tránh recreate callback trigger re-render
  /// các row đang in-flight.
  const aiRef = useRef<AiContentSettings>(ai);
  aiRef.current = ai;
  /// Semaphore-style pool cho AI request: queue + active counter. Khi
  /// active < MAX_AI_CONCURRENT thì shift queue chạy ngay; ngược lại item
  /// chờ trong queue. Tránh spike khi product fetch xong loạt 50 SP → AI
  /// gọi 50 request song song dễ trigger rate-limit OpenAI.
  const aiQueueRef = useRef<Array<() => Promise<void>>>([]);
  const aiActiveRef = useRef(0);
  const drainAiQueue = useCallback(() => {
    while (
      aiActiveRef.current < MAX_AI_CONCURRENT &&
      aiQueueRef.current.length > 0
    ) {
      const task = aiQueueRef.current.shift()!;
      aiActiveRef.current += 1;
      void task().finally(() => {
        aiActiveRef.current -= 1;
        drainAiQueue();
      });
    }
  }, []);

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
  const minSales = useMemo(() => {
    const digits = minSalesInput.replace(/[^\d]/g, "");
    const n = Number(digits);
    return Number.isFinite(n) && n > 0 ? n : 0;
  }, [minSalesInput]);
  const filterActive = minCommission > 0 || minSales > 0;
  /// Row done phải thỏa CẢ 2 ngưỡng (AND). Row pending/fetching/failed luôn
  /// hiện để user có feedback trạng thái.
  const rowPassesFilter = useCallback(
    (i: BatchItem) =>
      (i.info?.commission ?? 0) >= minCommission &&
      (i.info?.sales ?? 0) >= minSales,
    [minCommission, minSales],
  );
  const displayedItems = useMemo(() => {
    if (!filterActive) return items;
    return items.filter((i) => i.status !== "done" || rowPassesFilter(i));
  }, [items, filterActive, rowPassesFilter]);
  /// Counter "X/Y SP đạt" — Y = số SP đã done, X = số SP done thỏa filter.
  const passedCount = useMemo(
    () => items.filter((i) => i.status === "done" && rowPassesFilter(i)).length,
    [items, rowPassesFilter],
  );

  /// Enqueue AI generate cho 1 item — pool tự drain. Đọc apiKey + model
  /// từ aiRef (snapshot mới nhất). Caller phải đảm bảo info đã fetch xong
  /// + có productName.
  const generateOne = useCallback(
    (id: string, info: ShopeeProductInfo) => {
      const cfg = aiRef.current;
      if (!cfg.enabled || !cfg.apiKey.trim() || !info.productName.trim()) {
        return;
      }
      /// Đánh dấu "generating" NGAY khi enqueue (chứ không phải khi chạy)
      /// để UI thấy phản hồi tức thì — row đang chờ pool slot cũng hiện
      /// spinner thay vì im lặng.
      setItems((prev) =>
        prev.map((i) =>
          i.id === id ? { ...i, aiStatus: "generating", aiError: "" } : i,
        ),
      );
      const task = async () => {
        // Re-read config tại thời điểm thực sự chạy (user có thể đã đổi).
        const cfgRun = aiRef.current;
        if (!cfgRun.enabled || !cfgRun.apiKey.trim()) {
          setItems((prev) =>
            prev.map((i) =>
              i.id === id
                ? { ...i, aiStatus: "failed", aiError: "AI đã bị tắt giữa chừng" }
                : i,
            ),
          );
          return;
        }
        try {
          const content = await generateFbContent({
            apiKey: cfgRun.apiKey,
            model: cfgRun.model,
            productName: info.productName,
            shopName: info.shopName,
            price: info.price,
            sales: info.sales,
          });
          setItems((prev) =>
            prev.map((i) =>
              i.id === id
                ? { ...i, aiStatus: "done", aiContent: content, aiError: "" }
                : i,
            ),
          );
        } catch (e) {
          setItems((prev) =>
            prev.map((i) =>
              i.id === id
                ? { ...i, aiStatus: "failed", aiError: String(e) }
                : i,
            ),
          );
        }
      };
      aiQueueRef.current.push(task);
      drainAiQueue();
    },
    [drainAiQueue],
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
      // Kick off AI generate (best-effort, không block trả về fetchOne).
      // Pool concurrency của AI quản lý ở handleFetchAll qua queue riêng;
      // ở đây retry handler / standalone fetch không qua pool, vẫn OK vì
      // số lượng nhỏ.
      const cfg = aiRef.current;
      if (cfg.enabled && cfg.apiKey.trim()) {
        generateOne(item.id, info);
      }
    } catch (e) {
      setItems((prev) =>
        prev.map((i) =>
          i.id === item.id
            ? { ...i, status: "failed", error: String(e) }
            : i,
        ),
      );
    }
  }, [generateOne]);

  /// Chạy batch fetch thực sự — tách khỏi handleFetchAll để có thể gọi lại
  /// sau khi user confirm dialog cảnh báo cost AI.
  const runFetch = useCallback(async (urls: string[]) => {
    const newItems: BatchItem[] = urls.map((url) => ({
      id: genId(),
      url,
      status: "pending",
      info: null,
      error: "",
      aiStatus: "idle",
      aiContent: "",
      aiError: "",
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
  }, [fetchOne]);

  const handleFetchAll = useCallback(async () => {
    const urls = parseShopeeUrls(urlsText);
    if (urls.length === 0 || fetchingAll) return;

    /// Cost guard: > 10 link + AI bật + có API key → chặn lại confirm. Tránh
    /// user paste 50 link, quên là AI đang ON rồi mất token OpenAI vô ích.
    const willCostMoney =
      ai.enabled && ai.apiKey.trim() && urls.length > AI_COST_WARN_THRESHOLD;
    if (willCostMoney) {
      setPendingAiConfirm(urls);
      return;
    }

    await runFetch(urls);
  }, [urlsText, fetchingAll, ai.enabled, ai.apiKey, runFetch]);

  const handleConfirmAiCost = useCallback(() => {
    const urls = pendingAiConfirm;
    setPendingAiConfirm(null);
    if (urls) void runFetch(urls);
  }, [pendingAiConfirm, runFetch]);

  /// Cancel = giữ nguyên text + tắt AI luôn (lý do user mở dialog là vì
  /// quên không tắt). User có thể bật lại trong Settings hoặc toggle ngay
  /// dưới textarea nếu thực sự muốn.
  const handleCancelAiCost = useCallback(() => {
    setPendingAiConfirm(null);
    setAiContent({ enabled: false });
    showToast({
      message: "Đã tắt AI tạo content — bấm Tra cứu lại để chạy không tốn tiền",
      duration: 3500,
    });
  }, [setAiContent, showToast]);

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

  const handleRetryAi = useCallback(
    (id: string) => {
      const target = items.find((i) => i.id === id);
      if (target?.info) generateOne(id, target.info);
    },
    [items, generateOne],
  );

  const handleCopyAll = useCallback(async () => {
    /// Khi có ít nhất 1 SP đã có AI content → bổ sung cột "ai content"
    /// cuối TSV. Content multi-line → escape \n thành \\n để giữ 1 row 1 SP
    /// trong Sheets (user import có thể chuyển lại sau).
    const anyAi = items.some((i) => i.aiStatus === "done" && i.aiContent);
    const rows = items
      .filter((i) => i.status === "done" && i.info)
      .map((i) => {
        const info = i.info!;
        const cells: (string | number)[] = [
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
        ];
        if (anyAi) {
          cells.push(i.aiContent ? i.aiContent.replace(/\t/g, " ").replace(/\n/g, "\\n") : "");
        }
        return cells.join("\t");
      });
    if (rows.length === 0) return;
    const headerCols = [
      "itemId",
      "tên SP",
      "shop",
      "giá",
      "đã bán",
      "hoa hồng",
      "seller",
      "shopee",
      "rating",
      "link",
    ];
    if (anyAi) headerCols.push("ai content");
    await copyToClipboard(
      [headerCols.join("\t"), ...rows].join("\n"),
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
        {/* Toggle AI content — sync 2 chiều với Settings để user bật/tắt nhanh
            ngay tại tab Tra cứu mà không phải mở dialog. */}
        <label className="flex cursor-pointer items-start gap-3 rounded-xl border border-violet-500/25 bg-violet-950/15 px-3 py-2.5">
          <input
            type="checkbox"
            checked={ai.enabled}
            onChange={(e) => setAiContent({ enabled: e.currentTarget.checked })}
            className="mt-0.5 h-4 w-4 accent-violet-500"
          />
          <div className="min-w-0 flex-1 text-sm">
            <div className="flex flex-wrap items-center gap-2">
              <span className="material-symbols-rounded text-base text-violet-300">
                auto_awesome
              </span>
              <span className="font-semibold text-violet-100">
                Bật tự tạo content khi tra cứu SP
              </span>
              {ai.enabled ? (
                <span className="rounded bg-emerald-500/20 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-emerald-300">
                  ON
                </span>
              ) : (
                <span className="rounded bg-surface-6 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-white/45">
                  OFF
                </span>
              )}
              <span className="text-[10px] text-white/40">
                · đồng bộ với Cài đặt
              </span>
            </div>
            <p className="mt-1 text-xs text-white/55">
              Khi bật, mỗi sản phẩm tra cứu thành công sẽ tự gọi OpenAI sinh
              content. Mỗi SP gọi 1 request riêng — không cache.
            </p>
          </div>
        </label>
        {ai.enabled && !ai.apiKey.trim() && (
          <div className="flex items-start gap-2 rounded-lg border border-amber-500/40 bg-amber-950/30 px-3 py-2 text-xs text-amber-200">
            <span className="material-symbols-rounded mt-0.5 text-sm text-amber-400">
              warning
            </span>
            <span>
              AI tạo content đang BẬT nhưng chưa có API key OpenAI. Vào{" "}
              <b>Cài đặt → AI tạo content FB</b> để cấu hình.
            </span>
          </div>
        )}
        {ai.enabled && ai.apiKey.trim() && (
          <div className="flex items-center gap-1.5 text-xs text-violet-300">
            <span className="material-symbols-rounded text-sm">
              auto_awesome
            </span>
            <span>
              AI sẽ tự sinh content FB sau khi tra cứu xong · model{" "}
              <span className="font-mono">{ai.model}</span>
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
                minCommission > 0
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
              {minCommission > 0 && (
                <button
                  type="button"
                  onClick={() => setMinCommissionInput("")}
                  title="Bỏ filter hoa hồng"
                  className="ml-0.5 flex h-4 w-4 items-center justify-center rounded-full text-white/55 hover:bg-white/10 hover:text-white"
                >
                  <span className="material-symbols-rounded text-xs">
                    close
                  </span>
                </button>
              )}
            </label>
            <label
              className={`flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 font-medium transition-colors ${
                minSales > 0
                  ? "border-shopee-500/60 bg-shopee-500/15 text-shopee-200"
                  : "border-surface-8 bg-surface-2 text-white/55"
              }`}
              title="Ẩn các sản phẩm có số đã bán thấp hơn ngưỡng này"
            >
              <span className="material-symbols-rounded text-sm">
                filter_alt
              </span>
              <span className="whitespace-nowrap">Đã bán ≥</span>
              <input
                type="text"
                inputMode="numeric"
                value={minSalesInput}
                onChange={(e) => setMinSalesInput(e.target.value)}
                placeholder="0"
                className="w-16 bg-transparent text-right tabular-nums text-white/90 placeholder:text-white/30 focus:outline-none"
              />
              {minSales > 0 && (
                <button
                  type="button"
                  onClick={() => setMinSalesInput("")}
                  title="Bỏ filter đã bán"
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

      {/* Confirm cost AI khi > 10 link — chặn user paste lô lớn rồi quên tắt
          AI làm tốn token OpenAI. Danger=true để dialog đỏ + animate-shake. */}
      <ConfirmDialog
        isOpen={pendingAiConfirm !== null}
        danger
        title={`Tạo content AI cho ${pendingAiConfirm?.length ?? 0} sản phẩm?`}
        message={
          <>
            Bạn sắp tra cứu{" "}
            <b className="text-white">{pendingAiConfirm?.length ?? 0} link</b>{" "}
            với <b className="text-violet-200">AI tạo content đang BẬT</b> — mỗi
            SP sẽ gọi OpenAI 1 request riêng và{" "}
            <b className="text-red-300">tốn tiền token thật</b> (không cache).
            <br />
            <br />
            Bạn có chắc đang cần content để chạy{" "}
            <b className="text-white">FB Ads</b> không? Nếu chỉ muốn xem giá /
            hoa hồng thì tắt AI để khỏi mất tiền.
          </>
        }
        confirmLabel={`Có, tạo content cho ${pendingAiConfirm?.length ?? 0} SP`}
        cancelLabel="Hủy & tắt AI"
        onConfirm={handleConfirmAiCost}
        onClose={handleCancelAiCost}
      />

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
                      Không có sản phẩm nào đạt
                      {minCommission > 0 && (
                        <> hoa hồng ≥ {fmtVnd(minCommission)}</>
                      )}
                      {minCommission > 0 && minSales > 0 && " và"}
                      {minSales > 0 && <> đã bán ≥ {fmtInt(minSales)}</>}
                    </td>
                  </tr>
                ) : (
                  displayedItems.map((item, idx) => (
                    <ProductRow
                      key={item.id}
                      index={idx + 1}
                      item={item}
                      onRetry={handleRetry}
                      onRetryAi={handleRetryAi}
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
  onRetryAi: (id: string) => void;
  onCopy: (text: string, label: string) => void | Promise<void>;
}

function ProductRow({ index, item, onRetry, onRetryAi, onCopy }: ProductRowProps) {
  const info = item.info;
  const link = info?.productLink || item.url;
  const itemIdStr = info?.itemId != null ? String(info.itemId) : "";
  const showAiBlock =
    item.status === "done" &&
    (item.aiStatus === "generating" ||
      item.aiStatus === "done" ||
      item.aiStatus === "failed");

  return (
    <>
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
    {showAiBlock && (
      <tr className="border-t border-surface-8/50 bg-violet-950/15">
        <td />
        <td colSpan={8} className="px-3 pb-4 pt-1">
          <AiContentBlock
            item={item}
            onRetry={() => onRetryAi(item.id)}
            onCopy={onCopy}
          />
        </td>
      </tr>
    )}
    </>
  );
}

interface AiContentBlockProps {
  item: BatchItem;
  onRetry: () => void;
  onCopy: (text: string, label: string) => void | Promise<void>;
}

function AiContentBlock({ item, onRetry, onCopy }: AiContentBlockProps) {
  if (item.aiStatus === "generating") {
    return (
      <div className="flex items-center gap-2 rounded-lg border border-violet-500/30 bg-violet-500/5 px-3 py-2 text-xs text-violet-200">
        <span className="material-symbols-rounded animate-spin text-sm">
          auto_awesome
        </span>
        AI đang viết content quảng cáo FB...
      </div>
    );
  }
  if (item.aiStatus === "failed") {
    return (
      <div className="flex items-start gap-2 rounded-lg border border-red-500/40 bg-red-500/10 px-3 py-2 text-xs text-red-200">
        <span className="material-symbols-rounded mt-0.5 text-sm">error</span>
        <div className="min-w-0 flex-1">
          <div className="font-medium">AI lỗi: {item.aiError}</div>
          <button
            type="button"
            onClick={onRetry}
            className="btn-ripple mt-1 inline-flex items-center gap-1 rounded-md border border-red-500/40 bg-red-500/10 px-2 py-0.5 text-[11px] font-medium hover:bg-red-500/20"
          >
            <span className="material-symbols-rounded text-xs">refresh</span>
            Tạo lại
          </button>
        </div>
      </div>
    );
  }
  if (item.aiStatus === "done" && item.aiContent) {
    return (
      <div className="overflow-hidden rounded-lg border border-violet-500/30 bg-violet-950/20">
        <div className="flex items-center justify-between gap-2 border-b border-violet-500/20 bg-violet-500/10 px-3 py-1.5">
          <div className="flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-wider text-violet-200">
            <span className="material-symbols-rounded text-sm">
              auto_awesome
            </span>
            Content FB ads do AI tạo
          </div>
          <div className="flex items-center gap-1">
            <button
              type="button"
              onClick={() => void onCopy(item.aiContent, "content FB")}
              title="Copy toàn bộ content"
              className="btn-ripple flex items-center gap-1 rounded-md bg-violet-500/15 px-2 py-0.5 text-[11px] font-medium text-violet-100 hover:bg-violet-500/25"
            >
              <span className="material-symbols-rounded text-xs">
                content_copy
              </span>
              Copy
            </button>
            <button
              type="button"
              onClick={onRetry}
              title="Sinh lại nội dung mới"
              className="btn-ripple flex items-center gap-1 rounded-md bg-violet-500/15 px-2 py-0.5 text-[11px] font-medium text-violet-100 hover:bg-violet-500/25"
            >
              <span className="material-symbols-rounded text-xs">
                refresh
              </span>
              Tạo lại
            </button>
          </div>
        </div>
        <pre className="max-h-72 overflow-auto whitespace-pre-wrap break-words bg-surface-1/40 px-3 py-2 font-sans text-xs leading-relaxed text-white/85">
          {item.aiContent}
        </pre>
      </div>
    );
  }
  return null;
}
