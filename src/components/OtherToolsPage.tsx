import { useCallback, useMemo, useState } from "react";
import { openUrl } from "@tauri-apps/plugin-opener";
import {
  cleanShopeeLinks,
  convertAffWithPaste,
  findShopeeAffId,
  type AffLinkResult,
  type CleanLinksResult,
  type ShopeeAffIdResult,
} from "../lib/shopeeTools";
import { useToast } from "./ToastProvider";

/// Catalog tool sidebar — thêm tool mới chỉ cần push vào TOOLS + thêm case
/// trong renderTool() switch. Mỗi tool component tự quản state riêng.
type ToolKey = "find-aff-id" | "clean-link" | "aff-paste";

interface ToolMeta {
  key: ToolKey;
  label: string;
  icon: string;
  desc: string;
}

const TOOLS: ToolMeta[] = [
  {
    key: "find-aff-id",
    label: "Tìm AFF ID",
    icon: "badge",
    desc: "Trích Affiliate Publisher ID từ link Shopee (short hoặc full).",
  },
  {
    key: "clean-link",
    label: "Lấy link gốc",
    icon: "cleaning_services",
    desc: "Mở rộng link rút gọn Shopee + loại bỏ tham số tracking trong văn bản.",
  },
  {
    key: "aff-paste",
    label: "Convert AFF (dán cookies)",
    icon: "vpn_key",
    desc: "Dán cookies + proxy → tạo affiliate link không cần login. Demo multi-account.",
  },
];

export function OtherToolsPage() {
  const [activeTool, setActiveTool] = useState<ToolKey>("find-aff-id");

  return (
    <div className="mx-auto flex max-w-[1536px] flex-col gap-4 lg:flex-row">
      {/* Sidebar list tool */}
      <aside className="shrink-0 lg:w-64">
        <div className="overflow-hidden rounded-2xl bg-surface-2 shadow-elev-2">
          <div className="flex items-center gap-2 border-b border-surface-8 px-4 py-3">
            <span className="material-symbols-rounded text-base text-violet-400">
              build
            </span>
            <h2 className="text-sm font-semibold uppercase tracking-wider text-white/70">
              Tools khác
            </h2>
          </div>
          <ul className="divide-y divide-surface-8">
            {TOOLS.map((t) => (
              <li key={t.key}>
                <button
                  type="button"
                  onClick={() => setActiveTool(t.key)}
                  className={`flex w-full items-start gap-3 px-4 py-3 text-left transition-colors ${
                    activeTool === t.key
                      ? "bg-violet-500/10"
                      : "hover:bg-surface-4"
                  }`}
                >
                  <span
                    className={`material-symbols-rounded mt-0.5 text-base ${
                      activeTool === t.key ? "text-violet-300" : "text-white/45"
                    }`}
                  >
                    {t.icon}
                  </span>
                  <div className="min-w-0 flex-1">
                    <div
                      className={`text-sm font-semibold ${
                        activeTool === t.key
                          ? "text-violet-200"
                          : "text-white/85"
                      }`}
                    >
                      {t.label}
                    </div>
                    <div className="mt-0.5 text-[11px] leading-snug text-white/45">
                      {t.desc}
                    </div>
                  </div>
                </button>
              </li>
            ))}
          </ul>
        </div>
      </aside>

      {/* Main tool area */}
      <main className="min-w-0 flex-1">
        {activeTool === "find-aff-id" && <FindAffIdTool />}
        {activeTool === "clean-link" && <CleanLinkTool />}
        {activeTool === "aff-paste" && <AffPasteTool />}
      </main>
    </div>
  );
}

// ============================================================
// Tool 1 — Find AFF ID (batch)
// ============================================================

type ItemStatus = "pending" | "fetching" | "done" | "failed";

interface AffIdItem {
  id: string;
  url: string;
  status: ItemStatus;
  result: ShopeeAffIdResult | null;
  error: string;
}

const MAX_CONCURRENT = 5;
const SHOPEE_URL_RX = /(https?:\/\/[^\s]*(?:shopee\.|shp\.ee|s\.shopee\.)[^\s]*)/gi;

function genId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
}

function parseShopeeUrls(text: string): string[] {
  const found = text.match(SHOPEE_URL_RX) ?? [];
  return [...new Set(found.map((s) => s.trim()))];
}

function FindAffIdTool() {
  const [urlsText, setUrlsText] = useState("");
  const [items, setItems] = useState<AffIdItem[]>([]);
  const [running, setRunning] = useState(false);
  const { showToast } = useToast();

  const urlCount = useMemo(() => parseShopeeUrls(urlsText).length, [urlsText]);
  const doneCount = items.filter((i) => i.status === "done").length;
  const failedCount = items.filter((i) => i.status === "failed").length;
  const fetchingCount = items.filter((i) => i.status === "fetching").length;
  const withAffCount = items.filter(
    (i) => i.status === "done" && i.result?.affId != null,
  ).length;

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

  const fetchOne = useCallback(async (item: AffIdItem) => {
    setItems((prev) =>
      prev.map((i) =>
        i.id === item.id ? { ...i, status: "fetching", error: "" } : i,
      ),
    );
    try {
      const result = await findShopeeAffId(item.url);
      setItems((prev) =>
        prev.map((i) =>
          i.id === item.id ? { ...i, status: "done", result } : i,
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

  const handleRun = useCallback(async () => {
    const urls = parseShopeeUrls(urlsText);
    if (urls.length === 0 || running) return;
    const newItems: AffIdItem[] = urls.map((url) => ({
      id: genId(),
      url,
      status: "pending",
      result: null,
      error: "",
    }));
    setItems(newItems);
    setRunning(true);

    // Pool MAX_CONCURRENT — find-aff-id chỉ là HEAD-like follow redirect
    // nên có thể parallel cao hơn product fetch.
    const pool = new Set<Promise<void>>();
    for (const it of newItems) {
      const p: Promise<void> = fetchOne(it).finally(() => pool.delete(p));
      pool.add(p);
      if (pool.size >= MAX_CONCURRENT) await Promise.race(pool);
    }
    await Promise.all(pool);
    setRunning(false);
  }, [urlsText, running, fetchOne]);

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
      const t = items.find((i) => i.id === id);
      if (t) void fetchOne(t);
    },
    [items, fetchOne],
  );

  const handleCopyIds = useCallback(async () => {
    const ids = items
      .filter((i) => i.status === "done" && i.result?.affId != null)
      .map((i) => String(i.result!.affId));
    if (ids.length === 0) return;
    /// Unique + giữ order — user thường paste nhiều link cùng KOL chỉ cần
    /// distinct list publisher ID. join("\n") để paste sang Excel ra 1 cột.
    const unique = [...new Set(ids)];
    await copyToClipboard(
      unique.join("\n"),
      `${unique.length} AFF ID${unique.length !== ids.length ? ` (lọc trùng từ ${ids.length})` : ""}`,
    );
  }, [items, copyToClipboard]);

  const handleCopyTsv = useCallback(async () => {
    const rows = items
      .filter((i) => i.status === "done")
      .map((i) => {
        const r = i.result!;
        return [
          i.url,
          r.affId ?? "",
          r.source,
          r.finalUrl,
        ].join("\t");
      });
    if (rows.length === 0) return;
    const header = "link gốc\taff id\tsource\tfinal url";
    await copyToClipboard(
      [header, ...rows].join("\n"),
      `${rows.length} dòng TSV`,
    );
  }, [items, copyToClipboard]);

  return (
    <div className="space-y-5">
      {/* Hero */}
      <section className="overflow-hidden rounded-2xl bg-gradient-to-br from-violet-700 via-violet-600 to-purple-600 shadow-elev-4">
        <div className="flex items-center gap-4 px-6 py-5">
          <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/15 text-white shadow-inner">
            <span className="material-symbols-rounded text-2xl">badge</span>
          </span>
          <div className="min-w-0 flex-1">
            <h1 className="text-xl font-bold text-white">
              Tìm Affiliate ID Shopee
            </h1>
            <p className="mt-0.5 text-xs text-white/75">
              Dán link Shopee · Lấy Publisher ID (số) của KOL đang gắn tag ·
              Batch tối đa {MAX_CONCURRENT} link song song
            </p>
          </div>
        </div>
      </section>

      {/* URL input */}
      <section className="space-y-3 rounded-2xl bg-surface-2 p-4 shadow-elev-2">
        <label className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
          <span className="material-symbols-rounded text-base">link</span>
          Link Shopee (mỗi link 1 dòng, hỗ trợ shope.ee/shp.ee/shopee.vn)
        </label>
        <textarea
          value={urlsText}
          onChange={(e) => setUrlsText(e.target.value)}
          rows={4}
          placeholder={
            "https://shope.ee/1AsWlcMsQC\nhttps://shopee.vn/product/.../...?utm_source=an_..."
          }
          className="w-full resize-none rounded-xl border border-surface-8 bg-surface-1 px-4 py-3 font-mono text-sm text-white/90 placeholder:text-white/25 focus:border-violet-500 focus:outline-none focus:ring-2 focus:ring-violet-500/30"
        />
        {urlCount > 0 && (
          <div className="flex items-center gap-1.5 text-xs text-white/50">
            <span className="material-symbols-rounded text-sm text-violet-400">
              tag
            </span>
            <span>
              Đã nhận{" "}
              <span className="font-semibold text-violet-300">{urlCount}</span>{" "}
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
          {withAffCount > 0 && (
            <>
              <button
                type="button"
                onClick={() => void handleCopyIds()}
                className="btn-ripple flex items-center gap-1.5 rounded-lg border border-violet-500/50 bg-violet-500/10 px-3 py-1.5 text-xs font-medium text-violet-200 hover:bg-violet-500/20"
                title="Copy danh sách AFF ID (1 ID/dòng, đã unique)"
              >
                <span className="material-symbols-rounded text-sm">
                  content_copy
                </span>
                Copy AFF ID
              </button>
              <button
                type="button"
                onClick={() => void handleCopyTsv()}
                className="btn-ripple flex items-center gap-1.5 rounded-lg border border-surface-8 bg-surface-1 px-3 py-1.5 text-xs font-medium text-white/80 hover:bg-surface-4"
                title="Copy bảng đầy đủ dạng TSV"
              >
                <span className="material-symbols-rounded text-sm">
                  content_copy
                </span>
                Copy TSV
              </button>
            </>
          )}
          <button
            type="button"
            onClick={() => void handleRun()}
            disabled={urlCount === 0 || running}
            className="btn-ripple flex items-center gap-2 rounded-xl bg-violet-500 px-5 py-2 text-sm font-semibold text-white shadow-elev-2 transition-all hover:bg-violet-600 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <span
              className={`material-symbols-rounded text-base ${running ? "animate-spin" : ""}`}
            >
              {running ? "sync" : "search"}
            </span>
            {running
              ? `Đang tra (${fetchingCount}/${items.length})...`
              : "Tìm AFF ID"}
          </button>
        </div>
        {items.length > 0 && (
          <div className="flex flex-wrap items-center gap-2 border-t border-surface-8 pt-3 text-xs">
            <span className="rounded-full bg-emerald-500/15 px-2.5 py-0.5 font-medium text-emerald-300">
              ✓ {doneCount} done
            </span>
            <span className="rounded-full bg-violet-500/15 px-2.5 py-0.5 font-medium text-violet-300">
              🏷 {withAffCount} có AFF
            </span>
            {fetchingCount > 0 && (
              <span className="rounded-full bg-blue-500/15 px-2.5 py-0.5 font-medium text-blue-300">
                ⟳ {fetchingCount} đang tra
              </span>
            )}
            {failedCount > 0 && (
              <span className="rounded-full bg-red-500/15 px-2.5 py-0.5 font-medium text-red-300">
                ✗ {failedCount} lỗi
              </span>
            )}
          </div>
        )}
      </section>

      {/* Results */}
      {items.length === 0 ? (
        <section className="flex flex-col items-center gap-3 rounded-2xl border border-dashed border-surface-8 bg-surface-1 px-6 py-12 text-center text-white/55">
          <span className="material-symbols-rounded text-5xl text-white/25">
            badge
          </span>
          <p className="text-sm">
            Dán link Shopee phía trên rồi bấm "Tìm AFF ID" để lấy Publisher ID
            của KOL đang gắn tag affiliate.
          </p>
          <p className="text-xs text-white/40">
            Tool follow redirect short link → parse <code>utm_source=an_*</code>{" "}
            hoặc <code>mmp_pid=an_*</code>. Không cần đăng nhập Shopee.
          </p>
        </section>
      ) : (
        <section className="overflow-hidden rounded-2xl border border-surface-8 bg-surface-1 shadow-elev-2">
          <table className="w-full table-fixed text-sm">
            <colgroup>
              <col className="w-10" />
              <col />
              <col className="w-40" />
              <col className="w-28" />
              <col className="w-16" />
            </colgroup>
            <thead className="bg-surface-2 text-xs uppercase tracking-wider text-white/55">
              <tr>
                <th className="px-3 py-2 text-left">#</th>
                <th className="px-3 py-2 text-left">Link gốc / Final URL</th>
                <th className="px-3 py-2 text-right">AFF ID</th>
                <th className="px-3 py-2 text-center">Nguồn</th>
                <th className="px-3 py-2 text-center">Mở</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item, idx) => (
                <AffIdRow
                  key={item.id}
                  index={idx + 1}
                  item={item}
                  onRetry={handleRetry}
                  onCopy={copyToClipboard}
                />
              ))}
            </tbody>
          </table>
        </section>
      )}
    </div>
  );
}

interface AffIdRowProps {
  index: number;
  item: AffIdItem;
  onRetry: (id: string) => void;
  onCopy: (text: string, label: string) => void | Promise<void>;
}

function AffIdRow({ index, item, onRetry, onCopy }: AffIdRowProps) {
  const r = item.result;
  const link = item.url;
  const affIdStr = r?.affId != null ? String(r.affId) : "";

  return (
    <tr className="border-t border-surface-8 align-top hover:bg-surface-2/50">
      <td className="px-3 py-3 text-xs text-white/40">{index}</td>

      <td className="px-3 py-3">
        <div className="min-w-0 space-y-1">
          <div className="truncate font-mono text-xs text-white/80">{link}</div>
          {item.status === "fetching" && (
            <div className="flex items-center gap-1.5 text-xs text-violet-300">
              <span className="material-symbols-rounded animate-spin text-sm">
                sync
              </span>
              Đang follow redirect...
            </div>
          )}
          {item.status === "failed" && (
            <div className="space-y-1">
              <div className="text-xs font-medium text-red-300">
                ✗ {item.error || "Lỗi không xác định"}
              </div>
              <button
                type="button"
                onClick={() => onRetry(item.id)}
                className="btn-ripple inline-flex items-center gap-1 rounded-md border border-red-500/40 bg-red-500/10 px-2 py-0.5 text-[11px] font-medium text-red-200 hover:bg-red-500/20"
              >
                <span className="material-symbols-rounded text-xs">
                  refresh
                </span>
                Thử lại
              </button>
            </div>
          )}
          {item.status === "done" && r && (
            <div className="truncate font-mono text-[10px] text-white/40">
              → {r.finalUrl}
            </div>
          )}
        </div>
      </td>

      <td className="px-3 py-3 text-right">
        {item.status === "done" ? (
          affIdStr ? (
            <button
              type="button"
              onClick={() => void onCopy(affIdStr, `AFF ID ${affIdStr}`)}
              title="Click để copy"
              className="btn-ripple inline-flex items-center gap-1.5 rounded-md bg-violet-500/15 px-2.5 py-1 font-mono text-sm font-semibold text-violet-200 hover:bg-violet-500/25"
            >
              <span className="material-symbols-rounded text-base">badge</span>
              {affIdStr}
            </button>
          ) : (
            <span className="text-xs text-white/40">không có</span>
          )
        ) : (
          <span className="text-white/30">—</span>
        )}
      </td>

      <td className="px-3 py-3 text-center">
        {item.status === "done" && r ? (
          <span
            className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-mono font-semibold ${
              r.source === "utm_source"
                ? "bg-emerald-500/20 text-emerald-300"
                : r.source === "mmp_pid"
                  ? "bg-blue-500/20 text-blue-300"
                  : "bg-surface-6 text-white/50"
            }`}
            title={
              r.source === "none"
                ? "Không tìm thấy param an_<id> trong final URL"
                : `Extract từ param ${r.source}`
            }
          >
            {r.source}
          </span>
        ) : (
          <span className="text-white/30">—</span>
        )}
      </td>

      <td className="px-3 py-3 text-center">
        <button
          type="button"
          onClick={() => void openUrl(r?.finalUrl || link)}
          title="Mở final URL trên Shopee"
          className="btn-ripple inline-flex h-7 w-7 items-center justify-center rounded-full text-white/55 hover:bg-violet-500/15 hover:text-violet-300"
        >
          <span className="material-symbols-rounded text-base">
            open_in_new
          </span>
        </button>
      </td>
    </tr>
  );
}

// ============================================================
// Tool 2 — Clean Link (mở rộng + bỏ tracking trong text block)
// ============================================================

function CleanLinkTool() {
  const [inputText, setInputText] = useState("");
  const [outputText, setOutputText] = useState("");
  const [result, setResult] = useState<CleanLinksResult | null>(null);
  const [running, setRunning] = useState(false);
  const [errorMsg, setErrorMsg] = useState("");
  const { showToast } = useToast();

  const handleRun = useCallback(async () => {
    if (!inputText.trim() || running) return;
    setRunning(true);
    setErrorMsg("");
    setResult(null);
    try {
      const r = await cleanShopeeLinks(inputText);
      setResult(r);
      setOutputText(r.outputText);
    } catch (e) {
      setErrorMsg(String(e));
    } finally {
      setRunning(false);
    }
  }, [inputText, running]);

  const handlePaste = useCallback(async () => {
    try {
      const text = await navigator.clipboard.readText();
      if (text.trim()) setInputText(text);
    } catch {
      /* clipboard blocked */
    }
  }, []);

  const handleClear = useCallback(() => {
    setInputText("");
    setOutputText("");
    setResult(null);
    setErrorMsg("");
  }, []);

  const handleCopy = useCallback(async () => {
    if (!outputText) return;
    try {
      await navigator.clipboard.writeText(outputText);
      showToast({ message: "Đã copy text đã clean", duration: 1800 });
    } catch {
      showToast({ message: "Không truy cập được clipboard", duration: 3000 });
    }
  }, [outputText, showToast]);

  /// Đếm bao nhiêu link thực sự thay đổi (cleaned khác original) — feedback
  /// rõ ràng hơn cho user. Vd: paste text chỉ có full link đã clean sẵn →
  /// totalFound=2, changedCount=0.
  const changedCount = useMemo(
    () => result?.replacements.filter((r) => r.cleaned !== r.original).length ?? 0,
    [result],
  );

  return (
    <div className="space-y-5">
      {/* Hero */}
      <section className="overflow-hidden rounded-2xl bg-gradient-to-br from-teal-700 via-cyan-700 to-sky-600 shadow-elev-4">
        <div className="flex items-center gap-4 px-6 py-5">
          <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/15 text-white shadow-inner">
            <span className="material-symbols-rounded text-2xl">
              cleaning_services
            </span>
          </span>
          <div className="min-w-0 flex-1">
            <h1 className="text-xl font-bold text-white">
              Lấy link gốc (Clean link)
            </h1>
            <p className="mt-0.5 text-xs text-white/75">
              Mở rộng link rút gọn Shopee · Strip mọi tham số tracking (utm,
              mmp, af, ...) · Replace inline trong văn bản
            </p>
          </div>
        </div>
      </section>

      <section className="rounded-2xl bg-surface-2 p-4 shadow-elev-2">
        <div className="grid gap-4 lg:grid-cols-2">
          {/* Input */}
          <div className="space-y-2">
            <label className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
              <span className="material-symbols-rounded text-base">
                content_paste
              </span>
              Văn bản đầu vào
            </label>
            <textarea
              value={inputText}
              onChange={(e) => setInputText(e.target.value)}
              rows={12}
              placeholder={
                "Dán đoạn văn có link rút gọn Shopee.\nVD:\nMời ae xem nhanh: https://shope.ee/1AsWlcMsQC\nVà link này: https://s.shopee.vn/3VQO0nU7Tm"
              }
              className="w-full resize-none rounded-xl border border-surface-8 bg-surface-1 px-4 py-3 font-mono text-xs text-white/90 placeholder:text-white/25 focus:border-cyan-500 focus:outline-none focus:ring-2 focus:ring-cyan-500/30"
            />
            <div className="flex items-center gap-2">
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
                disabled={!inputText && !outputText}
                className="btn-ripple flex items-center gap-1.5 rounded-lg bg-surface-4 px-3 py-1.5 text-xs font-medium text-white/60 hover:bg-surface-6 disabled:opacity-40"
              >
                <span className="material-symbols-rounded text-sm">close</span>
                Xóa
              </button>
              <div className="flex-1" />
              <button
                type="button"
                onClick={() => void handleRun()}
                disabled={!inputText.trim() || running}
                className="btn-ripple flex items-center gap-2 rounded-xl bg-cyan-500 px-5 py-2 text-sm font-semibold text-white shadow-elev-2 transition-all hover:bg-cyan-600 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <span
                  className={`material-symbols-rounded text-base ${running ? "animate-spin" : ""}`}
                >
                  {running ? "sync" : "auto_fix_high"}
                </span>
                {running ? "Đang xử lý..." : "Xử lý"}
              </button>
            </div>
          </div>

          {/* Output */}
          <div className="space-y-2">
            <label className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
              <span className="material-symbols-rounded text-base">
                link
              </span>
              Kết quả (đã clean)
            </label>
            <textarea
              value={outputText}
              readOnly
              rows={12}
              placeholder="Kết quả sẽ hiện ở đây sau khi bấm Xử lý..."
              className="w-full resize-none rounded-xl border border-surface-8 bg-surface-1 px-4 py-3 font-mono text-xs text-white/90 placeholder:text-white/25"
            />
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => void handleCopy()}
                disabled={!outputText}
                className="btn-ripple flex items-center gap-1.5 rounded-lg border border-cyan-500/40 bg-cyan-500/10 px-3 py-1.5 text-xs font-medium text-cyan-200 hover:bg-cyan-500/20 disabled:cursor-not-allowed disabled:opacity-40"
              >
                <span className="material-symbols-rounded text-sm">
                  content_copy
                </span>
                Sao chép
              </button>
            </div>
          </div>
        </div>

        {/* Summary + errors */}
        {errorMsg && (
          <div className="mt-4 flex items-start gap-2 rounded-lg border border-red-500/40 bg-red-950/30 px-3 py-2 text-xs text-red-200">
            <span className="material-symbols-rounded mt-0.5 text-sm">
              error
            </span>
            <span>{errorMsg}</span>
          </div>
        )}
        {result && !errorMsg && (
          <div className="mt-4 flex flex-wrap items-center gap-2 border-t border-surface-8 pt-3 text-xs">
            <span className="rounded-full bg-cyan-500/15 px-2.5 py-0.5 font-medium text-cyan-300">
              🔗 {result.totalFound} link tìm thấy
            </span>
            <span className="rounded-full bg-emerald-500/15 px-2.5 py-0.5 font-medium text-emerald-300">
              ✓ {changedCount} link đã clean
            </span>
            {result.errors.length > 0 && (
              <span className="rounded-full bg-red-500/15 px-2.5 py-0.5 font-medium text-red-300">
                ✗ {result.errors.length} lỗi
              </span>
            )}
            {result.totalFound === 0 && (
              <span className="text-white/45">
                Không tìm thấy link Shopee nào trong văn bản
              </span>
            )}
          </div>
        )}
      </section>

      {/* Bảng replacements + errors */}
      {result && (result.replacements.length > 0 || result.errors.length > 0) && (
        <section className="overflow-hidden rounded-2xl border border-surface-8 bg-surface-1 shadow-elev-2">
          <div className="border-b border-surface-8 bg-surface-2 px-4 py-2 text-xs font-semibold uppercase tracking-wider text-white/70">
            Chi tiết xử lý
          </div>
          <ul className="divide-y divide-surface-8">
            {result.replacements.map((r, idx) => {
              const changed = r.cleaned !== r.original;
              return (
                <li key={`r-${idx}`} className="px-4 py-3 text-xs">
                  <div className="flex items-start gap-2">
                    <span
                      className={`material-symbols-rounded mt-0.5 text-sm ${
                        changed ? "text-emerald-400" : "text-white/30"
                      }`}
                    >
                      {changed ? "auto_fix_high" : "check"}
                    </span>
                    <div className="min-w-0 flex-1 space-y-0.5">
                      <div className="truncate font-mono text-white/55 line-through decoration-white/30">
                        {r.original}
                      </div>
                      <div className="flex items-start gap-1.5">
                        <span className="font-mono text-white/30">→</span>
                        <button
                          type="button"
                          onClick={() => {
                            void navigator.clipboard.writeText(r.cleaned);
                            showToast({
                              message: "Đã copy link cleaned",
                              duration: 1500,
                            });
                          }}
                          title="Click copy"
                          className="btn-ripple flex-1 cursor-copy truncate text-left font-mono text-cyan-300 hover:text-cyan-200"
                        >
                          {r.cleaned}
                        </button>
                        <button
                          type="button"
                          onClick={() => void openUrl(r.cleaned)}
                          title="Mở link"
                          className="btn-ripple flex h-5 w-5 shrink-0 items-center justify-center rounded text-white/55 hover:bg-cyan-500/15 hover:text-cyan-300"
                        >
                          <span className="material-symbols-rounded text-sm">
                            open_in_new
                          </span>
                        </button>
                      </div>
                    </div>
                  </div>
                </li>
              );
            })}
            {result.errors.map((e, idx) => (
              <li key={`e-${idx}`} className="px-4 py-3 text-xs">
                <div className="flex items-start gap-2">
                  <span className="material-symbols-rounded mt-0.5 text-sm text-red-400">
                    error
                  </span>
                  <div className="min-w-0 flex-1 space-y-0.5">
                    <div className="truncate font-mono text-white/70">
                      {e.original}
                    </div>
                    <div className="text-red-300">{e.error}</div>
                  </div>
                </div>
              </li>
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}

// ============================================================
// Tool 3 — Convert affiliate link bằng cookies + proxy paste
// ============================================================

const SHOPEE_PRODUCT_RX = /https?:\/\/[^\s]*(?:shopee\.|shp\.ee|s\.shopee\.|vn\.shp\.)[^\s]*/gi;

function extractLinks(text: string): string[] {
  const found = text.match(SHOPEE_PRODUCT_RX) ?? [];
  return [...new Set(found.map((s) => s.trim()))];
}

function AffPasteTool() {
  const [cookies, setCookies] = useState("");
  const [proxy, setProxy] = useState("");
  const [antiFraudToken, setAntiFraudToken] = useState("");
  const [linksText, setLinksText] = useState("");
  const [subIds, setSubIds] = useState("");
  const [results, setResults] = useState<AffLinkResult[]>([]);
  const [running, setRunning] = useState(false);
  const [errorMsg, setErrorMsg] = useState("");
  const { showToast } = useToast();

  const linksParsed = useMemo(() => extractLinks(linksText), [linksText]);
  const cookieCount = useMemo(() => {
    const t = cookies.trim();
    if (!t) return 0;
    if (t.startsWith("[")) {
      try {
        const arr = JSON.parse(t);
        return Array.isArray(arr) ? arr.length : 0;
      } catch {
        return 0;
      }
    }
    return t
      .split(/[;\n]/)
      .map((s) => s.trim())
      .filter((s) => s.includes("=")).length;
  }, [cookies]);

  const handleRun = useCallback(async () => {
    if (linksParsed.length === 0 || !cookies.trim() || running) return;
    setRunning(true);
    setErrorMsg("");
    setResults([]);
    try {
      const subIdList = subIds
        .split(/[,\n]/)
        .map((s) => s.trim())
        .filter(Boolean)
        .slice(0, 5);
      const r = await convertAffWithPaste({
        cookies,
        proxy,
        links: linksParsed,
        subIds: subIdList,
        antiFraudToken,
      });
      setResults(r);
    } catch (e) {
      setErrorMsg(String(e));
    } finally {
      setRunning(false);
    }
  }, [cookies, proxy, linksParsed, subIds, running]);

  const handlePasteCookies = useCallback(async () => {
    try {
      const t = await navigator.clipboard.readText();
      if (t.trim()) setCookies(t);
    } catch {
      /* clipboard blocked */
    }
  }, []);

  const handlePasteLinks = useCallback(async () => {
    try {
      const t = await navigator.clipboard.readText();
      if (t.trim()) setLinksText((prev) => (prev ? `${prev}\n${t}` : t));
    } catch {
      /* clipboard blocked */
    }
  }, []);

  const copyToClipboard = useCallback(
    async (text: string, label: string) => {
      if (!text) return;
      try {
        await navigator.clipboard.writeText(text);
        showToast({ message: `Đã copy ${label}`, duration: 1500 });
      } catch {
        showToast({ message: "Không truy cập được clipboard", duration: 3000 });
      }
    },
    [showToast],
  );

  const handleCopyShortLinks = useCallback(async () => {
    const lines = results
      .filter((r) => r.shortLink)
      .map((r) => r.shortLink!);
    if (lines.length === 0) return;
    await copyToClipboard(
      lines.join("\n"),
      `${lines.length} short link`,
    );
  }, [results, copyToClipboard]);

  const handleCopyTsv = useCallback(async () => {
    if (results.length === 0) return;
    const header = "link gốc\tshort link\tlong link\tlỗi";
    const rows = results.map((r) =>
      [r.originalLink, r.shortLink ?? "", r.longLink ?? "", r.error ?? ""].join("\t"),
    );
    await copyToClipboard([header, ...rows].join("\n"), `${rows.length} dòng TSV`);
  }, [results, copyToClipboard]);

  const successCount = results.filter((r) => r.shortLink).length;
  const failCount = results.length - successCount;

  return (
    <div className="space-y-5">
      {/* Hero */}
      <section className="overflow-hidden rounded-2xl bg-gradient-to-br from-rose-700 via-pink-600 to-fuchsia-600 shadow-elev-4">
        <div className="flex items-center gap-4 px-6 py-5">
          <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/15 text-white shadow-inner">
            <span className="material-symbols-rounded text-2xl">vpn_key</span>
          </span>
          <div className="min-w-0 flex-1">
            <h1 className="text-xl font-bold text-white">
              Convert AFF bằng cookies dán tay
            </h1>
            <p className="mt-0.5 text-xs text-white/75">
              Dán cookies Shopee Affiliate + proxy · Tạo link rút gọn batch ·
              Không cần login qua webview
            </p>
          </div>
        </div>
      </section>

      <section className="space-y-4 rounded-2xl bg-surface-2 p-4 shadow-elev-2">
        {/* Hàng cookies + proxy */}
        <div className="grid gap-4 lg:grid-cols-[1fr_320px]">
          {/* Cookies */}
          <div className="space-y-2">
            <label className="flex items-center justify-between gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
              <span className="flex items-center gap-1.5">
                <span className="material-symbols-rounded text-base">cookie</span>
                Cookies Shopee Affiliate
              </span>
              {cookieCount > 0 && (
                <span className="rounded-full bg-pink-500/15 px-2 py-0.5 text-[10px] font-semibold text-pink-300">
                  {cookieCount} cookie
                </span>
              )}
            </label>
            <textarea
              value={cookies}
              onChange={(e) => setCookies(e.target.value)}
              rows={6}
              placeholder={
                'Dán dạng JSON Cookie-Editor: [{"name":"SPC_F","value":"abc"}, ...]\nhoặc raw: SPC_F=abc; SPC_U=xyz; SPC_T_ID=...'
              }
              className="w-full resize-none rounded-xl border border-surface-8 bg-surface-1 px-4 py-3 font-mono text-xs text-white/90 placeholder:text-white/25 focus:border-pink-500 focus:outline-none focus:ring-2 focus:ring-pink-500/30"
            />
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={handlePasteCookies}
                className="btn-ripple flex items-center gap-1.5 rounded-lg bg-surface-4 px-3 py-1.5 text-xs font-medium text-white/80 hover:bg-surface-6"
              >
                <span className="material-symbols-rounded text-sm">
                  content_paste
                </span>
                Dán
              </button>
              <button
                type="button"
                onClick={() => setCookies("")}
                disabled={!cookies}
                className="btn-ripple flex items-center gap-1.5 rounded-lg bg-surface-4 px-3 py-1.5 text-xs font-medium text-white/60 hover:bg-surface-6 disabled:opacity-40"
              >
                <span className="material-symbols-rounded text-sm">close</span>
                Xóa
              </button>
              <span className="ml-auto text-[10px] text-white/40">
                Lấy bằng extension Cookie-Editor → Export JSON
              </span>
            </div>
          </div>

          {/* Proxy + subIds */}
          <div className="space-y-3">
            <div className="space-y-1.5">
              <label className="flex items-center justify-between gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
                <span className="flex items-center gap-1.5">
                  <span className="material-symbols-rounded text-base">
                    vpn_lock
                  </span>
                  Proxy (tuỳ chọn)
                </span>
                {!proxy.trim() ? (
                  <span className="rounded-full bg-emerald-500/15 px-2 py-0.5 text-[10px] font-semibold text-emerald-300">
                    Direct (IP máy)
                  </span>
                ) : (
                  <span className="rounded-full bg-pink-500/15 px-2 py-0.5 text-[10px] font-semibold text-pink-300">
                    Qua proxy
                  </span>
                )}
              </label>
              <input
                type="text"
                value={proxy}
                onChange={(e) => setProxy(e.target.value)}
                placeholder="host:port:user:pass (bỏ trống = dùng IP máy)"
                spellCheck={false}
                className="w-full rounded-lg border border-surface-8 bg-surface-1 px-3 py-1.5 font-mono text-xs text-white/90 placeholder:text-white/30 focus:border-pink-500 focus:outline-none focus:ring-1 focus:ring-pink-500"
              />
              <p className="text-[10px] text-white/40">
                {proxy.trim() ? (
                  <>
                    Format: <span className="font-mono text-pink-300">160.30.22.205:44014:user:pass</span> ·
                    hỗ trợ socks5://, http://
                  </>
                ) : (
                  <>Bỏ trống → kết nối thẳng IP máy bạn (mặc định)</>
                )}
              </p>
            </div>

            <div className="space-y-1.5">
              <label className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
                <span className="material-symbols-rounded text-base">tag</span>
                Sub IDs (tuỳ chọn, tối đa 5)
              </label>
              <input
                type="text"
                value={subIds}
                onChange={(e) => setSubIds(e.target.value)}
                placeholder="sub1, sub2, sub3"
                spellCheck={false}
                className="w-full rounded-lg border border-surface-8 bg-surface-1 px-3 py-1.5 font-mono text-xs text-white/90 placeholder:text-white/30 focus:border-pink-500 focus:outline-none focus:ring-1 focus:ring-pink-500"
              />
              <p className="text-[10px] text-white/40">
                Phân tách bằng dấu phẩy. Sẽ gắn vào subId1..subId5.
              </p>
            </div>

            <div className="space-y-1.5">
              <label className="flex items-center justify-between gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
                <span className="flex items-center gap-1.5">
                  <span className="material-symbols-rounded text-base">
                    encrypted
                  </span>
                  Anti-fraud token
                </span>
                {antiFraudToken.trim() ? (
                  <span className="rounded-full bg-emerald-500/15 px-2 py-0.5 text-[10px] font-semibold text-emerald-300">
                    Có
                  </span>
                ) : (
                  <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-semibold text-amber-300">
                    Có thể cần
                  </span>
                )}
              </label>
              <textarea
                value={antiFraudToken}
                onChange={(e) => setAntiFraudToken(e.target.value)}
                placeholder="af-ac-enc-sz-token từ DevTools (Network → Headers)"
                spellCheck={false}
                rows={2}
                className="w-full resize-none rounded-lg border border-surface-8 bg-surface-1 px-3 py-1.5 font-mono text-[10px] text-white/90 placeholder:text-white/30 focus:border-pink-500 focus:outline-none focus:ring-1 focus:ring-pink-500"
              />
              <p className="text-[10px] text-white/40">
                Mở DevTools → Network → submit convert trong Shopee → click
                request `batchCustomLink` → Headers → copy giá trị{" "}
                <span className="font-mono text-pink-300">af-ac-enc-sz-token</span>
              </p>
            </div>
          </div>
        </div>

        {/* Links */}
        <div className="space-y-2">
          <label className="flex items-center justify-between gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
            <span className="flex items-center gap-1.5">
              <span className="material-symbols-rounded text-base">link</span>
              Link Shopee cần convert (mỗi link 1 dòng)
            </span>
            {linksParsed.length > 0 && (
              <span className="rounded-full bg-pink-500/15 px-2 py-0.5 text-[10px] font-semibold text-pink-300">
                {linksParsed.length} link
              </span>
            )}
          </label>
          <textarea
            value={linksText}
            onChange={(e) => setLinksText(e.target.value)}
            rows={5}
            placeholder={
              "https://shopee.vn/product/...\nhttps://shope.ee/...\nhttps://s.shopee.vn/..."
            }
            className="w-full resize-none rounded-xl border border-surface-8 bg-surface-1 px-4 py-3 font-mono text-xs text-white/90 placeholder:text-white/25 focus:border-pink-500 focus:outline-none focus:ring-2 focus:ring-pink-500/30"
          />
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={handlePasteLinks}
              className="btn-ripple flex items-center gap-1.5 rounded-lg bg-surface-4 px-3 py-1.5 text-xs font-medium text-white/80 hover:bg-surface-6"
            >
              <span className="material-symbols-rounded text-sm">
                content_paste
              </span>
              Dán
            </button>
            <button
              type="button"
              onClick={() => {
                setLinksText("");
                setResults([]);
              }}
              disabled={!linksText && results.length === 0}
              className="btn-ripple flex items-center gap-1.5 rounded-lg bg-surface-4 px-3 py-1.5 text-xs font-medium text-white/60 hover:bg-surface-6 disabled:opacity-40"
            >
              <span className="material-symbols-rounded text-sm">close</span>
              Xóa
            </button>
            <div className="flex-1" />
            <button
              type="button"
              onClick={() => void handleRun()}
              disabled={
                linksParsed.length === 0 || !cookies.trim() || running
              }
              className="btn-ripple flex items-center gap-2 rounded-xl bg-pink-500 px-5 py-2 text-sm font-semibold text-white shadow-elev-2 transition-all hover:bg-pink-600 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <span
                className={`material-symbols-rounded text-base ${running ? "animate-spin" : ""}`}
              >
                {running ? "sync" : "auto_fix_high"}
              </span>
              {running ? "Đang convert..." : "Convert"}
            </button>
          </div>
        </div>

        {/* Status + error */}
        {errorMsg && (
          <div className="flex items-start gap-2 rounded-lg border border-red-500/40 bg-red-950/30 px-3 py-2 text-xs text-red-200">
            <span className="material-symbols-rounded mt-0.5 text-sm">error</span>
            <span className="font-mono">{errorMsg}</span>
          </div>
        )}
        {results.length > 0 && !errorMsg && (
          <div className="flex flex-wrap items-center gap-2 border-t border-surface-8 pt-3 text-xs">
            <span className="rounded-full bg-emerald-500/15 px-2.5 py-0.5 font-medium text-emerald-300">
              ✓ {successCount} thành công
            </span>
            {failCount > 0 && (
              <span className="rounded-full bg-red-500/15 px-2.5 py-0.5 font-medium text-red-300">
                ✗ {failCount} lỗi
              </span>
            )}
            <div className="flex-1" />
            {successCount > 0 && (
              <>
                <button
                  type="button"
                  onClick={() => void handleCopyShortLinks()}
                  className="btn-ripple flex items-center gap-1.5 rounded-lg border border-pink-500/40 bg-pink-500/10 px-3 py-1.5 text-xs font-medium text-pink-200 hover:bg-pink-500/20"
                >
                  <span className="material-symbols-rounded text-sm">
                    content_copy
                  </span>
                  Copy short links
                </button>
                <button
                  type="button"
                  onClick={() => void handleCopyTsv()}
                  className="btn-ripple flex items-center gap-1.5 rounded-lg border border-surface-8 bg-surface-1 px-3 py-1.5 text-xs font-medium text-white/80 hover:bg-surface-4"
                >
                  <span className="material-symbols-rounded text-sm">
                    content_copy
                  </span>
                  Copy TSV
                </button>
              </>
            )}
          </div>
        )}
      </section>

      {/* Empty state hoặc results */}
      {results.length === 0 ? (
        <section className="flex flex-col items-center gap-3 rounded-2xl border border-dashed border-surface-8 bg-surface-1 px-6 py-10 text-center text-white/55">
          <span className="material-symbols-rounded text-5xl text-white/25">
            vpn_key
          </span>
          <p className="text-sm">
            Dán cookies + proxy (tuỳ chọn) + link Shopee phía trên rồi bấm
            "Convert" để tạo affiliate link.
          </p>
          <p className="text-xs text-white/40">
            Cookies sẽ KHÔNG được lưu — chỉ dùng cho lần convert này. Đây là
            demo để verify approach hoạt động trước khi build multi-account.
          </p>
        </section>
      ) : (
        <section className="overflow-hidden rounded-2xl border border-surface-8 bg-surface-1 shadow-elev-2">
          <table className="w-full table-fixed text-sm">
            <colgroup>
              <col className="w-10" />
              <col />
              <col className="w-72" />
              <col className="w-16" />
            </colgroup>
            <thead className="bg-surface-2 text-xs uppercase tracking-wider text-white/55">
              <tr>
                <th className="px-3 py-2 text-left">#</th>
                <th className="px-3 py-2 text-left">Link gốc</th>
                <th className="px-3 py-2 text-left">Short link</th>
                <th className="px-3 py-2 text-center">Mở</th>
              </tr>
            </thead>
            <tbody>
              {results.map((r, idx) => (
                <tr
                  key={`${idx}-${r.originalLink}`}
                  className="border-t border-surface-8 align-top hover:bg-surface-2/50"
                >
                  <td className="px-3 py-3 text-xs text-white/40">{idx + 1}</td>
                  <td className="px-3 py-3">
                    <div className="truncate font-mono text-[11px] text-white/70">
                      {r.originalLink}
                    </div>
                  </td>
                  <td className="px-3 py-3">
                    {r.shortLink ? (
                      <button
                        type="button"
                        onClick={() =>
                          void copyToClipboard(r.shortLink!, "short link")
                        }
                        title="Click copy"
                        className="btn-ripple cursor-copy truncate font-mono text-xs font-semibold text-pink-300 hover:text-pink-200"
                      >
                        {r.shortLink}
                      </button>
                    ) : (
                      <div className="space-y-0.5">
                        <div className="text-xs font-medium text-red-300">
                          ✗ {r.error || "Lỗi"}
                        </div>
                        {r.failCode != null && (
                          <div className="font-mono text-[10px] text-white/40">
                            failCode={r.failCode}
                          </div>
                        )}
                      </div>
                    )}
                  </td>
                  <td className="px-3 py-3 text-center">
                    {r.shortLink && (
                      <button
                        type="button"
                        onClick={() => void openUrl(r.shortLink!)}
                        title="Mở short link"
                        className="btn-ripple inline-flex h-7 w-7 items-center justify-center rounded-full text-white/55 hover:bg-pink-500/15 hover:text-pink-300"
                      >
                        <span className="material-symbols-rounded text-base">
                          open_in_new
                        </span>
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
    </div>
  );
}
