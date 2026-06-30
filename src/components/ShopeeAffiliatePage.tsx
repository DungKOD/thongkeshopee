import { useCallback, useEffect, useMemo, useState } from "react";
import { openUrl } from "@tauri-apps/plugin-opener";
import {
  shopeeAffCaptureCookies,
  shopeeAffClearCookies,
  shopeeAffCloseLoginWindow,
  shopeeAffConvertLinks,
  shopeeAffConvertViaWebview,
  shopeeAffGetStatus,
  shopeeAffOpenLoginWindow,
  type LinkResult,
  type ShopeeAffStatus,
} from "../lib/shopeeAffiliate";
import { fmtTimeAgo } from "../formulas";
import { useToast } from "./ToastProvider";

const SHOPEE_URL_RX = /(https?:\/\/[^\s]*(?:shopee\.|shp\.ee|s\.shopee\.)[^\s]*)/gi;

function parseShopeeUrls(text: string): string[] {
  const found = text.match(SHOPEE_URL_RX) ?? [];
  return [...new Set(found.map((s) => s.trim()))];
}

function capturedAtMs(iso: string | null): number | null {
  if (!iso) return null;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? t : null;
}

const EMPTY_STATUS: ShopeeAffStatus = {
  hasCookies: false,
  capturedAt: null,
  cookieCount: 0,
  loginWindowOpen: false,
};

export function ShopeeAffiliatePage() {
  const { showToast } = useToast();
  const [status, setStatus] = useState<ShopeeAffStatus>(EMPTY_STATUS);
  const [linksText, setLinksText] = useState("");
  const [subIds, setSubIds] = useState<string[]>(() => {
    try {
      const raw = localStorage.getItem("shopee-aff:subIds");
      if (raw) {
        const arr = JSON.parse(raw) as unknown;
        if (Array.isArray(arr)) {
          return Array.from({ length: 5 }, (_, i) =>
            typeof arr[i] === "string" ? (arr[i] as string) : "",
          );
        }
      }
    } catch {
      /* ignore */
    }
    return ["", "", "", "", ""];
  });
  const [results, setResults] = useState<LinkResult[]>([]);
  const [converting, setConverting] = useState(false);

  useEffect(() => {
    try {
      localStorage.setItem("shopee-aff:subIds", JSON.stringify(subIds));
    } catch {
      /* quota */
    }
  }, [subIds]);

  const refreshStatus = useCallback(async () => {
    try {
      const s = await shopeeAffGetStatus();
      setStatus(s);
    } catch (e) {
      console.error("get status failed", e);
    }
  }, []);

  useEffect(() => {
    void refreshStatus();
  }, [refreshStatus]);

  const handleOpenLogin = useCallback(async () => {
    try {
      await shopeeAffOpenLoginWindow();
      showToast({
        message: "Đã mở cửa sổ Shopee Affiliate. Đăng nhập xong bấm 'Lưu cookies'.",
        duration: 4000,
      });
      await refreshStatus();
    } catch (e) {
      showToast({
        message: `Không mở được cửa sổ: ${String(e)}`,
        duration: 6000,
      });
    }
  }, [refreshStatus, showToast]);

  const handleCapture = useCallback(async () => {
    try {
      const s = await shopeeAffCaptureCookies();
      setStatus(s);
      showToast({
        message: `Đã lưu ${s.cookieCount} cookies.`,
        duration: 3000,
      });
    } catch (e) {
      showToast({
        message: String(e),
        duration: 6000,
      });
    }
  }, [showToast]);

  const handleCloseLogin = useCallback(async () => {
    try {
      await shopeeAffCloseLoginWindow();
      await refreshStatus();
    } catch (e) {
      console.error(e);
    }
  }, [refreshStatus]);

  const handleClearCookies = useCallback(async () => {
    try {
      await shopeeAffClearCookies();
      setStatus(EMPTY_STATUS);
      showToast({ message: "Đã xóa cookies đã lưu", duration: 2500 });
      await refreshStatus();
    } catch (e) {
      showToast({ message: String(e), duration: 5000 });
    }
  }, [refreshStatus, showToast]);

  const linkCount = useMemo(() => parseShopeeUrls(linksText).length, [linksText]);

  const handleConvert = useCallback(async () => {
    const links = parseShopeeUrls(linksText);
    if (links.length === 0) {
      showToast({ message: "Chưa có link Shopee nào", duration: 3000 });
      return;
    }
    if (!status.hasCookies) {
      showToast({
        message: "Chưa lưu cookies — đăng nhập trước",
        duration: 4000,
      });
      return;
    }
    setConverting(true);
    try {
      const out = await shopeeAffConvertLinks(links, subIds);
      setResults(out);
      const okCount = out.filter((r) => !!r.shortLink).length;
      showToast({
        message: `Tạo ${okCount}/${out.length} smart link thành công`,
        duration: 3000,
      });
    } catch (e) {
      showToast({ message: String(e), duration: 7000 });
    } finally {
      setConverting(false);
    }
  }, [linksText, subIds, status.hasCookies, showToast]);

  /// LV1 SAFEST: convert qua webview thật. Yêu cầu login window đang mở.
  /// Auto open nếu chưa mở để tiện UX.
  const handleConvertViaWebview = useCallback(async () => {
    const links = parseShopeeUrls(linksText);
    if (links.length === 0) {
      showToast({ message: "Chưa có link Shopee nào", duration: 3000 });
      return;
    }
    if (!status.loginWindowOpen) {
      showToast({
        message:
          "Cửa sổ login chưa mở. Bấm 'Đăng nhập' để mở webview, login xong rồi thử lại.",
        duration: 6000,
      });
      return;
    }
    setConverting(true);
    try {
      const out = await shopeeAffConvertViaWebview(links, subIds);
      setResults(out);
      const okCount = out.filter((r) => !!r.shortLink).length;
      showToast({
        message: `[WebView LV1] Tạo ${okCount}/${out.length} smart link`,
        duration: 3000,
      });
    } catch (e) {
      showToast({ message: String(e), duration: 7000 });
    } finally {
      setConverting(false);
    }
  }, [linksText, subIds, status.loginWindowOpen, showToast]);

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

  const handleCopyAllShortLinks = useCallback(async () => {
    const lines = results
      .filter((r) => !!r.shortLink)
      .map((r) => r.shortLink!) as string[];
    if (lines.length === 0) return;
    await copyToClipboard(
      lines.join("\n"),
      `${lines.length} short link`,
    );
  }, [results, copyToClipboard]);

  const handleCopyTsv = useCallback(async () => {
    const rows = results
      .filter((r) => !!r.shortLink)
      .map((r) => [r.originalLink, r.shortLink, r.longLink ?? ""].join("\t"));
    if (rows.length === 0) return;
    await copyToClipboard(
      ["originalLink\tshortLink\tlongLink", ...rows].join("\n"),
      `${rows.length} link (TSV)`,
    );
  }, [results, copyToClipboard]);

  const okCount = results.filter((r) => !!r.shortLink).length;
  const failCount = results.filter((r) => !r.shortLink).length;

  return (
    <div className="mx-auto max-w-[1536px] space-y-5">
      {/* ===== Hero ===== */}
      <section className="overflow-hidden rounded-2xl bg-gradient-to-br from-purple-700 via-purple-600 to-fuchsia-500 shadow-elev-4">
        <div className="flex items-center gap-4 px-6 py-5">
          <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/15 text-white shadow-inner">
            <span className="material-symbols-rounded text-2xl">link</span>
          </span>
          <div className="min-w-0 flex-1">
            <h1 className="text-xl font-bold text-white">
              Shopee Affiliate Smart Link
            </h1>
            <p className="mt-0.5 text-xs text-white/80">
              Convert nhiều link Shopee → affiliate link bằng cookies session
              của bạn · Không cần Open API
            </p>
          </div>
        </div>
      </section>

      {/* ===== Workflow guide khi WebView2 trắng / treo ===== */}
      <details className="overflow-hidden rounded-2xl border border-amber-500/40 bg-amber-950/20">
        <summary className="cursor-pointer list-none px-4 py-3 text-sm font-semibold text-amber-200 hover:bg-amber-500/10">
          <span className="material-symbols-rounded mr-2 align-middle text-base">
            help
          </span>
          Webview Tauri bị trắng/treo? Bấm để xem cách dùng Chrome thật
        </summary>
        <div className="space-y-3 border-t border-amber-500/30 px-4 py-3 text-xs text-amber-100/90">
          <p>
            WebView2 trên 1 số máy bị Shopee anti-fraud block khiến trang trắng.
            Workflow thay thế <b>an toàn hơn nữa</b> dùng Chrome thật:
          </p>
          <ol className="ml-5 list-decimal space-y-1.5">
            <li>
              Bấm nút{" "}
              <span className="font-mono text-amber-300">
                "Mở Shopee trong Chrome"
              </span>{" "}
              ở dưới → Chrome mặc định mở affiliate dashboard
            </li>
            <li>Login Shopee Affiliate trong Chrome bằng tay</li>
            <li>
              Cài extension{" "}
              <span className="font-mono">Cookie-Editor</span> hoặc{" "}
              <span className="font-mono">EditThisCookie</span> trên Chrome
            </li>
            <li>
              Mở extension trong tab affiliate.shopee.vn → Export → chọn{" "}
              <b>JSON</b>
            </li>
            <li>
              Quan trọng: trong DevTools (F12) → tab <b>Network</b> → submit
              convert link 1 cái bằng tay → click request{" "}
              <span className="font-mono">batchCustomLink</span> → tab Headers →
              copy giá trị header{" "}
              <span className="font-mono text-amber-300">
                af-ac-enc-sz-token
              </span>
            </li>
            <li>
              Chuyển sang tab <b>Other</b> → chọn tool{" "}
              <b>"Convert AFF (dán cookies)"</b>
            </li>
            <li>
              Paste cookies JSON + anti-fraud token + link cần convert → Convert
            </li>
          </ol>
          <p className="rounded border border-amber-500/30 bg-amber-500/10 px-2 py-1.5 text-amber-200">
            <span className="material-symbols-rounded mr-1 align-middle text-sm">
              shield
            </span>
            Cookies + token được capture từ <b>session Chrome thật của bạn</b> →
            khi convert qua app, Shopee thấy mọi anti-fraud signal match → tỉ lệ
            pass cao hơn nhiều so với Tauri webview hỏng.
          </p>
        </div>
      </details>

      {/* ===== Login / cookies status ===== */}
      <section className="space-y-3 rounded-2xl border border-surface-8 bg-surface-2 p-4 shadow-elev-2">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs font-medium uppercase tracking-wider text-white/55">
            Trạng thái đăng nhập
          </span>
          {status.hasCookies ? (
            <span
              className="flex items-center gap-1 rounded-full bg-emerald-500/20 px-2.5 py-0.5 text-xs font-semibold text-emerald-300"
              title={status.capturedAt ?? ""}
            >
              <span className="material-symbols-rounded text-sm">
                check_circle
              </span>
              Đã lưu {status.cookieCount} cookies
              {status.capturedAt && (
                <span className="ml-1 text-emerald-200/70">
                  · {fmtTimeAgo(capturedAtMs(status.capturedAt))}
                </span>
              )}
            </span>
          ) : (
            <span className="flex items-center gap-1 rounded-full bg-red-500/15 px-2.5 py-0.5 text-xs font-semibold text-red-300">
              <span className="material-symbols-rounded text-sm">cancel</span>
              Chưa đăng nhập
            </span>
          )}
          {status.loginWindowOpen && (
            <span className="rounded-full bg-blue-500/15 px-2.5 py-0.5 text-xs font-semibold text-blue-300">
              Cửa sổ login đang mở
            </span>
          )}
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {/* NÚT MỚI — fallback Chrome external khi WebView2 lỗi trắng */}
          <button
            type="button"
            onClick={() =>
              void openUrl("https://affiliate.shopee.vn/offer/custom_link")
            }
            className="btn-ripple flex items-center gap-1.5 rounded-lg bg-amber-500 px-3 py-1.5 text-xs font-semibold text-white hover:bg-amber-600"
            title="Mở Shopee trong Chrome mặc định — bypass WebView2 nếu bị trắng"
          >
            <span className="material-symbols-rounded text-sm">
              language
            </span>
            Mở Shopee trong Chrome
          </button>
          <button
            type="button"
            onClick={() => void handleOpenLogin()}
            className="btn-ripple flex items-center gap-1.5 rounded-lg bg-purple-500 px-3 py-1.5 text-xs font-semibold text-white hover:bg-purple-600"
            title="Dùng webview Tauri (có thể trắng trên 1 số máy)"
          >
            <span className="material-symbols-rounded text-sm">
              open_in_browser
            </span>
            {status.loginWindowOpen
              ? "Focus cửa sổ login"
              : "Mở webview (Tauri)"}
          </button>
          <button
            type="button"
            onClick={() => void handleCapture()}
            disabled={!status.loginWindowOpen}
            className="btn-ripple flex items-center gap-1.5 rounded-lg border border-emerald-500/50 bg-emerald-500/10 px-3 py-1.5 text-xs font-semibold text-emerald-200 hover:bg-emerald-500/20 disabled:cursor-not-allowed disabled:opacity-40"
            title="Bấm sau khi login xong trong cửa sổ Shopee Affiliate"
          >
            <span className="material-symbols-rounded text-sm">cookie</span>
            Lưu cookies
          </button>
          {status.loginWindowOpen && (
            <button
              type="button"
              onClick={() => void handleCloseLogin()}
              className="btn-ripple flex items-center gap-1.5 rounded-lg border border-surface-8 bg-surface-1 px-3 py-1.5 text-xs font-medium text-white/70 hover:bg-surface-4"
            >
              <span className="material-symbols-rounded text-sm">close</span>
              Đóng cửa sổ login
            </button>
          )}
          {status.hasCookies && (
            <button
              type="button"
              onClick={() => void handleClearCookies()}
              className="btn-ripple flex items-center gap-1.5 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-1.5 text-xs font-medium text-red-200 hover:bg-red-500/20"
            >
              <span className="material-symbols-rounded text-sm">delete</span>
              Xóa cookies
            </button>
          )}
          <button
            type="button"
            onClick={() => void refreshStatus()}
            className="btn-ripple flex items-center gap-1.5 rounded-lg border border-surface-8 bg-surface-1 px-3 py-1.5 text-xs font-medium text-white/60 hover:bg-surface-4"
          >
            <span className="material-symbols-rounded text-sm">refresh</span>
            Refresh
          </button>
        </div>
        <p className="text-[11px] leading-relaxed text-white/50">
          Hướng dẫn: bấm <b>Mở cửa sổ</b> → đăng nhập tài khoản Affiliate
          Shopee như bình thường → khi đã thấy dashboard, quay lại đây bấm{" "}
          <b>Lưu cookies</b>. Cookies WebView2 persist sẵn nên lần sau mở lại
          thường đã login luôn.
        </p>
      </section>

      {/* ===== SubID input ===== */}
      <section className="space-y-3 rounded-2xl bg-surface-2 p-4 shadow-elev-2">
        <label className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
          <span className="material-symbols-rounded text-base">tag</span>
          SubID (tối đa 5 — để trống nếu không cần)
        </label>
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-5">
          {subIds.map((v, i) => (
            <input
              key={i}
              type="text"
              value={v}
              onChange={(e) => {
                const next = [...subIds];
                next[i] = e.currentTarget.value;
                setSubIds(next);
              }}
              placeholder={`SubID ${i + 1}`}
              className="w-full rounded-md border border-surface-8 bg-surface-1 px-2.5 py-1.5 text-sm text-white/90 placeholder:text-white/30 focus:border-purple-500 focus:outline-none focus:ring-1 focus:ring-purple-500"
            />
          ))}
        </div>
      </section>

      {/* ===== Links input ===== */}
      <section className="space-y-3 rounded-2xl bg-surface-2 p-4 shadow-elev-2">
        <label className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
          <span className="material-symbols-rounded text-base">link</span>
          Link Shopee (mỗi link 1 dòng — chấp nhận full / short / shp.ee)
        </label>
        <textarea
          value={linksText}
          onChange={(e) => setLinksText(e.currentTarget.value)}
          rows={5}
          placeholder={
            "https://shopee.vn/product/...\nhttps://s.shopee.vn/...\nhttps://shp.ee/..."
          }
          className="w-full resize-none rounded-xl border border-surface-8 bg-surface-1 px-4 py-3 font-mono text-sm text-white/90 placeholder:text-white/25 focus:border-purple-500 focus:outline-none focus:ring-2 focus:ring-purple-500/30"
        />
        {linkCount > 0 && (
          <div className="flex items-center gap-1.5 text-xs text-white/55">
            <span className="material-symbols-rounded text-sm text-purple-300">
              tag
            </span>
            <span>
              Đã nhận{" "}
              <span className="font-semibold text-purple-300">{linkCount}</span>{" "}
              link Shopee hợp lệ
            </span>
          </div>
        )}
        <div className="flex flex-wrap items-center gap-2">
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
          {/* Nút LV1 — convert qua webview thật (an toàn nhất, yêu cầu login window đang mở) */}
          <button
            type="button"
            onClick={() => void handleConvertViaWebview()}
            disabled={linkCount === 0 || converting || !status.loginWindowOpen}
            className="btn-ripple flex items-center gap-2 rounded-xl border border-emerald-500/50 bg-emerald-500/10 px-4 py-2 text-sm font-semibold text-emerald-200 shadow-elev-1 transition-all hover:bg-emerald-500/20 disabled:cursor-not-allowed disabled:opacity-40"
            title={
              !status.loginWindowOpen
                ? "Bấm 'Đăng nhập' để mở cửa sổ login trước"
                : "Convert qua webview thật — an toàn nhất, Shopee không phát hiện được"
            }
          >
            <span
              className={`material-symbols-rounded text-base ${converting ? "animate-spin" : ""}`}
            >
              {converting ? "sync" : "shield"}
            </span>
            {converting ? "Đang tạo..." : "Tạo qua WebView"}
            <span className="rounded-full bg-emerald-500/20 px-1.5 py-0 text-[10px] font-bold uppercase tracking-wider">
              LV1
            </span>
          </button>
          <button
            type="button"
            onClick={() => void handleConvert()}
            disabled={linkCount === 0 || converting || !status.hasCookies}
            className="btn-ripple flex items-center gap-2 rounded-xl bg-purple-500 px-5 py-2 text-sm font-semibold text-white shadow-elev-2 transition-all hover:bg-purple-600 disabled:cursor-not-allowed disabled:opacity-50"
            title={
              !status.hasCookies
                ? "Đăng nhập + lưu cookies trước"
                : "Tạo smart link bằng cookies đã capture (LV2)"
            }
          >
            <span
              className={`material-symbols-rounded text-base ${converting ? "animate-spin" : ""}`}
            >
              {converting ? "sync" : "auto_awesome"}
            </span>
            {converting ? "Đang tạo..." : "Tạo Smart Link"}
          </button>
        </div>
      </section>

      {/* ===== Results ===== */}
      {results.length === 0 ? (
        <section className="flex flex-col items-center gap-3 rounded-2xl border border-dashed border-surface-8 bg-surface-1 px-6 py-12 text-center text-white/55">
          <span className="material-symbols-rounded text-5xl text-white/25">
            auto_awesome
          </span>
          <p className="text-sm">
            Dán link Shopee → bấm "Tạo Smart Link" để nhận affiliate link.
          </p>
        </section>
      ) : (
        <section className="space-y-2">
          <div className="flex flex-wrap items-center gap-2 px-1 text-xs">
            {okCount > 0 && (
              <span className="rounded-full bg-emerald-500/15 px-2.5 py-0.5 font-medium text-emerald-300">
                ✓ {okCount} thành công
              </span>
            )}
            {failCount > 0 && (
              <span className="rounded-full bg-red-500/15 px-2.5 py-0.5 font-medium text-red-300">
                ✗ {failCount} lỗi
              </span>
            )}
            <div className="flex-1" />
            <button
              type="button"
              onClick={() => void handleCopyAllShortLinks()}
              disabled={okCount === 0}
              className="btn-ripple flex items-center gap-1.5 rounded-lg border border-purple-500/50 bg-purple-500/10 px-3 py-1.5 text-xs font-medium text-purple-200 hover:bg-purple-500/20 disabled:opacity-40"
            >
              <span className="material-symbols-rounded text-sm">
                content_copy
              </span>
              Copy tất cả short link
            </button>
            <button
              type="button"
              onClick={() => void handleCopyTsv()}
              disabled={okCount === 0}
              className="btn-ripple flex items-center gap-1.5 rounded-lg border border-surface-8 bg-surface-1 px-3 py-1.5 text-xs font-medium text-white/80 hover:bg-surface-4 disabled:opacity-40"
            >
              <span className="material-symbols-rounded text-sm">
                table_view
              </span>
              Copy TSV
            </button>
          </div>

          <div className="overflow-hidden rounded-2xl border border-surface-8 bg-surface-1 shadow-elev-2">
            <table className="w-full table-fixed text-sm">
              <colgroup>
                <col className="w-10" />
                <col />
                <col className="w-[320px]" />
                <col className="w-16" />
              </colgroup>
              <thead className="bg-surface-2 text-xs uppercase tracking-wider text-white/55">
                <tr>
                  <th className="px-3 py-2 text-left">#</th>
                  <th className="px-3 py-2 text-left">Link gốc</th>
                  <th className="px-3 py-2 text-left">Smart link</th>
                  <th className="px-3 py-2 text-center">Mở</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r, idx) => (
                  <tr
                    key={`${r.originalLink}-${idx}`}
                    className="border-t border-surface-8 align-top hover:bg-surface-2/40"
                  >
                    <td className="px-3 py-3 text-xs text-white/40">
                      {idx + 1}
                    </td>
                    <td className="px-3 py-3">
                      <button
                        type="button"
                        onClick={() =>
                          void copyToClipboard(r.originalLink, "link gốc")
                        }
                        title="Click để copy link gốc"
                        className="btn-ripple line-clamp-2 w-full cursor-copy rounded-md px-1.5 py-0.5 text-left font-mono text-xs text-white/75 hover:bg-surface-4 hover:text-white"
                      >
                        {r.originalLink}
                      </button>
                    </td>
                    <td className="px-3 py-3">
                      {r.shortLink ? (
                        <button
                          type="button"
                          onClick={() =>
                            void copyToClipboard(r.shortLink!, "smart link")
                          }
                          title="Click để copy"
                          className="btn-ripple w-full cursor-copy rounded-md px-1.5 py-1 text-left font-mono text-sm font-semibold text-purple-200 hover:bg-surface-4 hover:text-purple-100"
                        >
                          {r.shortLink}
                        </button>
                      ) : (
                        <div className="space-y-1">
                          <div className="text-xs font-medium text-red-300">
                            ✗ {r.error || "Không có shortLink"}
                          </div>
                          {r.failCode != null && (
                            <div className="font-mono text-[10px] text-white/40">
                              failCode: {r.failCode}
                            </div>
                          )}
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-3 text-center">
                      <button
                        type="button"
                        onClick={() => void openUrl(r.shortLink || r.originalLink)}
                        title="Mở link trong trình duyệt"
                        className="btn-ripple inline-flex h-7 w-7 items-center justify-center rounded-full text-white/55 hover:bg-purple-500/15 hover:text-purple-200"
                      >
                        <span className="material-symbols-rounded text-base">
                          open_in_new
                        </span>
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  );
}
