import { useCallback, useEffect, useRef, useState } from "react";
import { open } from "@tauri-apps/plugin-dialog";
import { listen } from "@tauri-apps/api/event";
import { invoke } from "../lib/tauri";
import {
  listVideoDownloads,
  logVideoDownload,
  type VideoDownloadLog,
} from "../lib/video";
import { fmtBytes, fmtHistoryTime } from "../formulas";
import { useToast } from "./ToastProvider";

interface VideoInfo {
  title: string;
  author: string;
  cover: string;
  duration: number;
  platform: string;
  downloadUrl: string;
  filename: string;
}

interface ProgressPayload {
  downloadId: string;
  downloaded: number;
  total: number;
}

type ItemStatus = "fetching" | "ready" | "downloading" | "done" | "failed";

interface BatchItem {
  id: string;
  url: string;
  status: ItemStatus;
  info: VideoInfo | null;
  progress: ProgressPayload | null;
  error: string;
}

interface PlatformChip {
  name: string;
  icon: string;
  gradient: string;
}

const PLATFORMS: PlatformChip[] = [
  { name: "TikTok", icon: "music_note", gradient: "from-gray-700 to-gray-900" },
  { name: "Douyin", icon: "videocam", gradient: "from-gray-800 to-black" },
  { name: "Shopee", icon: "storefront", gradient: "from-orange-500 to-red-600" },
  { name: "Xiaohongshu", icon: "book", gradient: "from-red-500 to-red-700" },
  { name: "YouTube", icon: "smart_display", gradient: "from-red-600 to-red-800" },
  { name: "Facebook", icon: "thumb_up", gradient: "from-blue-600 to-blue-800" },
  {
    name: "Instagram",
    icon: "photo_camera",
    gradient: "from-purple-500 via-pink-500 to-orange-400",
  },
  { name: "Twitter/X", icon: "close", gradient: "from-gray-700 to-gray-900" },
  { name: "Reddit", icon: "forum", gradient: "from-orange-500 to-orange-700" },
];

const MAX_CONCURRENT = 3;
const MAX_CONCURRENT_FETCH = 2;
const HISTORY_PAGE_SIZE = 50;

function genId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
}

function parseUrls(text: string): string[] {
  return [
    ...new Set(
      text
        .split(/[\n,\s]+/)
        .map((s) => s.trim())
        .filter((s) => s.startsWith("http://") || s.startsWith("https://")),
    ),
  ];
}

function fmtDuration(seconds: number): string {
  if (seconds <= 0) return "";
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}:${s.toString().padStart(2, "0")}`;
}

export function DownloadVideoPage() {
  const [urlsText, setUrlsText] = useState("");
  const [items, setItems] = useState<BatchItem[]>([]);
  const [saveDir, setSaveDir] = useState("");
  const [fetchingAll, setFetchingAll] = useState(false);
  const [downloadingCount, setDownloadingCount] = useState(0);
  const [history, setHistory] = useState<VideoDownloadLog[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);

  const { showToast } = useToast();
  // Chỉ show toast 1 lần/session để không spam — khi sync nhiều video cùng lúc
  // mà Sheet down, user chỉ cần biết 1 lần là đủ.
  const sheetWarnedRef = useRef(false);
  const warnSheetOnce = useCallback(
    (err: string) => {
      if (sheetWarnedRef.current) return;
      sheetWarnedRef.current = true;
      showToast({
        message: `Lưu log lên Google Sheet thất bại: ${err}. File vẫn được tải về máy bình thường.`,
        duration: 8000,
      });
    },
    [showToast],
  );

  const refreshHistory = useCallback(async () => {
    setHistoryLoading(true);
    try {
      setHistory(await listVideoDownloads(HISTORY_PAGE_SIZE, 0));
    } catch {
      /* ignore */
    } finally {
      setHistoryLoading(false);
    }
  }, []);

  useEffect(() => {
    void refreshHistory();
  }, [refreshHistory]);

  // 1 listener toàn cục, route theo downloadId trong payload
  useEffect(() => {
    const unlisten = listen<ProgressPayload>("download-progress", (e) => {
      const p = e.payload;
      setItems((prev) =>
        prev.map((item) =>
          item.id === p.downloadId ? { ...item, progress: p } : item,
        ),
      );
    });
    return () => {
      unlisten.then((fn) => fn());
    };
  }, []);

  const downloadOne = useCallback(async (item: BatchItem, dir: string) => {
    if (!item.info) return;
    const filename =
      item.info.filename ||
      `${item.info.platform.toLowerCase()}_${Date.now()}.mp4`;
    const savePath = `${dir}/${filename}`;

    setItems((prev) =>
      prev.map((i) =>
        i.id === item.id
          ? {
              ...i,
              status: "downloading",
              progress: { downloadId: item.id, downloaded: 0, total: 0 },
              error: "",
            }
          : i,
      ),
    );
    setDownloadingCount((n) => n + 1);

    try {
      await invoke<string>("download_video", {
        downloadUrl: item.info.downloadUrl,
        savePath,
        downloadId: item.id,
      });
      setItems((prev) =>
        prev.map((i) =>
          i.id === item.id ? { ...i, status: "done", progress: null } : i,
        ),
      );
      logVideoDownload(item.url, "success").then((r) => {
        if (!r.sheetOk) warnSheetOnce(r.sheetError ?? "unknown");
      });
    } catch (e) {
      setItems((prev) =>
        prev.map((i) =>
          i.id === item.id
            ? { ...i, status: "failed", error: String(e), progress: null }
            : i,
        ),
      );
      logVideoDownload(item.url, "failed").then((r) => {
        if (!r.sheetOk) warnSheetOnce(r.sheetError ?? "unknown");
      });
    } finally {
      setDownloadingCount((n) => n - 1);
    }
  }, [warnSheetOnce]);

  const fetchOne = useCallback(async (item: BatchItem) => {
    setItems((prev) =>
      prev.map((i) =>
        i.id === item.id ? { ...i, status: "fetching", error: "" } : i,
      ),
    );
    try {
      const info = await invoke<VideoInfo>("get_video_info", { url: item.url });
      setItems((prev) =>
        prev.map((i) =>
          i.id === item.id ? { ...i, status: "ready", info } : i,
        ),
      );
      logVideoDownload(item.url, "success").then((r) => {
        if (!r.sheetOk) warnSheetOnce(r.sheetError ?? "unknown");
      });
    } catch (e) {
      setItems((prev) =>
        prev.map((i) =>
          i.id === item.id ? { ...i, status: "failed", error: String(e) } : i,
        ),
      );
      logVideoDownload(item.url, "failed").then((r) => {
        if (!r.sheetOk) warnSheetOnce(r.sheetError ?? "unknown");
      });
    }
  }, [warnSheetOnce]);

  const handleFetchAll = async () => {
    const urls = parseUrls(urlsText);
    if (urls.length === 0 || downloadingCount > 0) return;

    const newItems: BatchItem[] = urls.map((url) => ({
      id: genId(),
      url,
      status: "fetching" as ItemStatus,
      info: null,
      progress: null,
      error: "",
    }));
    setItems(newItems);
    setFetchingAll(true);

    // Pool pattern: tối đa MAX_CONCURRENT_FETCH đồng thời — tránh rate limit API
    const pool = new Set<Promise<void>>();
    for (const item of newItems) {
      const p: Promise<void> = fetchOne(item).finally(() => pool.delete(p));
      pool.add(p);
      if (pool.size >= MAX_CONCURRENT_FETCH) await Promise.race(pool);
    }
    await Promise.all(pool);

    setFetchingAll(false);
    void refreshHistory();
  };

  const handleDownloadAll = async () => {
    if (!saveDir || downloadingCount > 0) return;
    const readyItems = items.filter((i) => i.status === "ready");
    if (readyItems.length === 0) return;

    // Pool pattern: tối đa MAX_CONCURRENT luồng đồng thời
    const pool = new Set<Promise<void>>();
    for (const item of readyItems) {
      const p: Promise<void> = downloadOne(item, saveDir).finally(() =>
        pool.delete(p),
      );
      pool.add(p);
      if (pool.size >= MAX_CONCURRENT) await Promise.race(pool);
    }
    await Promise.all(pool);
    void refreshHistory();
  };

  const handlePickFolder = async () => {
    const result = await open({
      directory: true,
      title: "Chọn thư mục lưu video",
    });
    if (typeof result === "string") setSaveDir(result);
  };

  const handlePaste = async () => {
    try {
      const text = await navigator.clipboard.readText();
      if (text.trim())
        setUrlsText((prev) => (prev ? `${prev}\n${text.trim()}` : text.trim()));
    } catch {
      /* clipboard blocked */
    }
  };

  const urlCount = parseUrls(urlsText).length;
  const readyCount = items.filter((i) => i.status === "ready").length;
  const doneCount = items.filter((i) => i.status === "done").length;
  const failedCount = items.filter((i) => i.status === "failed").length;
  const fetchingCount = items.filter((i) => i.status === "fetching").length;

  return (
    <div className="mx-auto max-w-3xl space-y-5">
      {/* ===== Hero ===== */}
      <section className="overflow-hidden rounded-2xl bg-gradient-to-br from-shopee-700 via-shopee-600 to-shopee-500 shadow-elev-4">
        <div className="flex items-center gap-4 px-6 py-5">
          <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/15 text-white shadow-inner">
            <span className="material-symbols-rounded text-2xl">download</span>
          </span>
          <div className="min-w-0 flex-1">
            <h1 className="text-xl font-bold text-white">Tải video hàng loạt</h1>
            <p className="mt-0.5 text-xs text-white/75">
              Dán nhiều link cùng lúc · Tối đa {MAX_CONCURRENT} luồng song song · Hỗn hợp nền tảng
            </p>
          </div>
        </div>
        <div className="border-t border-white/10 bg-black/10 px-6 py-2.5">
          <div className="flex flex-wrap gap-1.5">
            {PLATFORMS.map((p) => (
              <span
                key={p.name}
                className={`inline-flex items-center gap-1 rounded-full bg-gradient-to-r ${p.gradient} px-2.5 py-1 text-[11px] font-medium text-white shadow-elev-1`}
              >
                <span className="material-symbols-rounded text-sm">{p.icon}</span>
                {p.name}
              </span>
            ))}
          </div>
        </div>
      </section>

      {/* ===== URL input ===== */}
      <section className="space-y-3 rounded-2xl bg-surface-2 p-4 shadow-elev-2">
        <label className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
          <span className="material-symbols-rounded text-base">link</span>
          Links video (mỗi link 1 dòng, hỗ trợ nhiều nền tảng)
        </label>
        <textarea
          value={urlsText}
          onChange={(e) => setUrlsText(e.target.value)}
          rows={4}
          placeholder={
            "https://tiktok.com/...\nhttps://youtube.com/...\nhttps://douyin.com/..."
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
              link hợp lệ
            </span>
          </div>
        )}
        <div className="flex items-center gap-2">
          <button
            onClick={handlePaste}
            className="btn-ripple flex items-center gap-1.5 rounded-lg bg-surface-4 px-3 py-1.5 text-xs font-medium text-white/80 hover:bg-surface-6"
          >
            <span className="material-symbols-rounded text-sm">content_paste</span>
            Dán
          </button>
          <button
            onClick={() => {
              setUrlsText("");
              setItems([]);
            }}
            disabled={!urlsText && items.length === 0}
            className="btn-ripple flex items-center gap-1.5 rounded-lg bg-surface-4 px-3 py-1.5 text-xs font-medium text-white/60 hover:bg-surface-6 disabled:opacity-40"
          >
            <span className="material-symbols-rounded text-sm">close</span>
            Xóa
          </button>
          <div className="flex-1" />
          <button
            onClick={handleFetchAll}
            disabled={urlCount === 0 || fetchingAll || downloadingCount > 0}
            className="btn-ripple flex items-center gap-2 rounded-xl bg-shopee-500 px-5 py-2 text-sm font-semibold text-white shadow-elev-2 transition-all hover:bg-shopee-600 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <span
              className={`material-symbols-rounded text-base ${fetchingAll ? "animate-spin" : ""}`}
            >
              {fetchingAll ? "sync" : "search"}
            </span>
            {fetchingAll
              ? `Đang lấy info... (${items.length - fetchingCount}/${items.length})`
              : `Lấy thông tin (${urlCount} link)`}
          </button>
        </div>
      </section>

      {/* ===== Items + folder + download ===== */}
      {items.length > 0 && (
        <section className="overflow-hidden rounded-2xl bg-surface-2 shadow-elev-2">
          {/* Folder + action bar */}
          <div className="flex flex-wrap items-center gap-3 border-b border-surface-8 px-5 py-3">
            <div className="flex min-w-0 flex-1 items-center gap-2">
              <span className="material-symbols-rounded shrink-0 text-base text-white/40">
                folder
              </span>
              <span
                className={`truncate text-sm ${
                  saveDir
                    ? "font-mono text-white/80"
                    : "italic text-white/35"
                }`}
              >
                {saveDir || "Chưa chọn thư mục lưu"}
              </span>
            </div>
            <button
              onClick={handlePickFolder}
              className="btn-ripple flex shrink-0 items-center gap-1.5 rounded-lg bg-surface-4 px-3 py-1.5 text-xs font-medium text-white/80 hover:bg-surface-6"
            >
              <span className="material-symbols-rounded text-sm">folder_open</span>
              Chọn thư mục
            </button>
            <button
              onClick={handleDownloadAll}
              disabled={readyCount === 0 || !saveDir || downloadingCount > 0}
              className="btn-ripple flex shrink-0 items-center gap-2 rounded-xl bg-green-500 px-4 py-2 text-sm font-semibold text-white shadow-elev-2 transition-all hover:bg-green-600 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <span
                className={`material-symbols-rounded text-base ${
                  downloadingCount > 0 ? "animate-spin" : ""
                }`}
              >
                {downloadingCount > 0 ? "sync" : "download"}
              </span>
              {downloadingCount > 0
                ? `Đang tải (${downloadingCount} luồng)...`
                : `Tải tất cả (${readyCount} video)`}
            </button>
          </div>

          {/* Stats bar */}
          {(doneCount > 0 || failedCount > 0 || downloadingCount > 0) && (
            <div className="flex items-center gap-4 border-b border-surface-8 bg-surface-1 px-5 py-2 text-xs text-white/55">
              {doneCount > 0 && (
                <span className="flex items-center gap-1">
                  <span className="material-symbols-rounded text-sm text-green-400">
                    check_circle
                  </span>
                  {doneCount} xong
                </span>
              )}
              {downloadingCount > 0 && (
                <span className="flex items-center gap-1">
                  <span className="material-symbols-rounded animate-spin text-sm text-shopee-400">
                    sync
                  </span>
                  {downloadingCount} đang tải
                </span>
              )}
              {failedCount > 0 && (
                <span className="flex items-center gap-1">
                  <span className="material-symbols-rounded text-sm text-red-400">
                    error
                  </span>
                  {failedCount} lỗi
                </span>
              )}
              <span className="ml-auto text-white/30">{items.length} tổng</span>
            </div>
          )}

          {/* Item rows */}
          <ul className="divide-y divide-surface-8">
            {items.map((item) => (
              <BatchItemRow
                key={item.id}
                item={item}
                canRetryDownload={!!saveDir && !!item.info}
                onRetryDownload={() => void downloadOne(item, saveDir)}
                onRetryFetch={() => void fetchOne(item)}
              />
            ))}
          </ul>
        </section>
      )}

      {/* ===== How to use (idle) ===== */}
      {items.length === 0 && !fetchingAll && history.length === 0 && (
        <section className="rounded-2xl border border-dashed border-surface-8 bg-surface-1 p-6">
          <div className="mb-4 flex items-center gap-2">
            <span className="material-symbols-rounded text-lg text-shopee-400">
              tips_and_updates
            </span>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-white/70">
              Cách sử dụng
            </h3>
          </div>
          <div className="space-y-2.5">
            <Step n={1}>
              Dán một hoặc nhiều link vào ô trên (mỗi link 1 dòng, hỗn hợp nền tảng OK)
            </Step>
            <Step n={2}>
              Bấm{" "}
              <span className="rounded bg-surface-6 px-1.5 py-0.5 font-mono text-[11px] text-shopee-300">
                Lấy thông tin
              </span>{" "}
              để lấy metadata song song
            </Step>
            <Step n={3}>Chọn thư mục lưu, bấm "Tải tất cả" — tối đa {MAX_CONCURRENT} luồng đồng thời</Step>
          </div>
        </section>
      )}

      {/* ===== Lịch sử ===== */}
      {history.length > 0 && (
        <section className="rounded-2xl bg-surface-2 shadow-elev-2">
          <div className="flex items-center justify-between gap-3 border-b border-surface-8 px-5 py-3">
            <div className="flex items-center gap-2">
              <span className="material-symbols-rounded text-lg text-shopee-400">
                history
              </span>
              <h3 className="text-sm font-semibold text-white/85">
                Lịch sử ({history.length}
                {history.length >= HISTORY_PAGE_SIZE ? "+" : ""})
              </h3>
            </div>
            <button
              onClick={() => void refreshHistory()}
              disabled={historyLoading}
              className="btn-ripple flex h-8 items-center gap-1.5 rounded-lg bg-surface-4 px-3 text-xs font-medium text-white/80 hover:bg-surface-6 disabled:opacity-50"
            >
              <span
                className={`material-symbols-rounded text-base ${
                  historyLoading ? "animate-spin" : ""
                }`}
              >
                refresh
              </span>
              Làm mới
            </button>
          </div>
          <ul className="divide-y divide-surface-8">
            {history.map((row) => (
              <li
                key={row.id}
                className="flex items-center gap-3 px-5 py-2.5 hover:bg-surface-1/60"
              >
                <span
                  className={`material-symbols-rounded shrink-0 text-base ${
                    row.status === "success" ? "text-green-400" : "text-red-400"
                  }`}
                >
                  {row.status === "success" ? "check_circle" : "error"}
                </span>
                <button
                  type="button"
                  onClick={() => {
                    setUrlsText((prev) =>
                      prev ? `${prev}\n${row.url}` : row.url,
                    );
                    setItems([]);
                  }}
                  className="min-w-0 flex-1 truncate text-left font-mono text-xs text-shopee-300 hover:underline"
                  title={row.url}
                >
                  {row.url}
                </button>
                <span className="shrink-0 whitespace-nowrap text-[11px] tabular-nums text-white/40">
                  {fmtHistoryTime(row.downloaded_at_ms)}
                </span>
              </li>
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}

// ===== Sub-components =====

function Step({ n, children }: { n: number; children: React.ReactNode }) {
  return (
    <div className="flex items-start gap-3">
      <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-shopee-500/20 text-xs font-bold text-shopee-300">
        {n}
      </span>
      <p className="flex-1 text-sm leading-relaxed text-white/70">{children}</p>
    </div>
  );
}

interface BatchItemRowProps {
  item: BatchItem;
  canRetryDownload: boolean;
  onRetryDownload: () => void;
  onRetryFetch: () => void;
}

function BatchItemRow({
  item,
  canRetryDownload,
  onRetryDownload,
  onRetryFetch,
}: BatchItemRowProps) {
  const { status, info, progress, error, url } = item;

  const hasTotal = (progress?.total ?? 0) > 0;
  const percent = hasTotal
    ? Math.min(100, ((progress?.downloaded ?? 0) / (progress?.total ?? 1)) * 100)
    : 0;

  const statusIcon: Record<ItemStatus, React.ReactNode> = {
    fetching: (
      <span className="material-symbols-rounded animate-spin text-base text-white/40">
        sync
      </span>
    ),
    ready: (
      <span className="material-symbols-rounded text-base text-shopee-400">
        play_circle
      </span>
    ),
    downloading: (
      <span className="material-symbols-rounded animate-spin text-base text-green-400">
        sync
      </span>
    ),
    done: (
      <span className="material-symbols-rounded text-base text-green-400">
        check_circle
      </span>
    ),
    failed: (
      <span className="material-symbols-rounded text-base text-red-400">
        error
      </span>
    ),
  };

  return (
    <li className="flex items-start gap-3 px-4 py-3">
      {/* Status icon */}
      <div className="mt-1 shrink-0">{statusIcon[status]}</div>

      {/* Thumbnail */}
      {info?.cover ? (
        <img
          src={info.cover}
          alt=""
          className="h-14 w-10 shrink-0 rounded object-cover"
          referrerPolicy="no-referrer"
          onError={(e) => {
            (e.currentTarget as HTMLImageElement).style.display = "none";
          }}
        />
      ) : (
        <div className="flex h-14 w-10 shrink-0 items-center justify-center rounded bg-surface-6">
          <span className="material-symbols-rounded text-lg text-white/20">
            {status === "fetching" ? "hourglass_empty" : "image"}
          </span>
        </div>
      )}

      {/* Content */}
      <div className="min-w-0 flex-1 space-y-1">
        {/* Platform + title */}
        <div className="flex items-center gap-2">
          {info?.platform && (
            <span className="shrink-0 rounded bg-shopee-500/20 px-1.5 py-0.5 text-[10px] font-semibold text-shopee-300">
              {info.platform}
            </span>
          )}
          <span className="truncate text-sm text-white/85">
            {info?.title ||
              (status === "fetching" ? "Đang lấy thông tin..." : "")}
          </span>
        </div>

        {/* Author + duration */}
        {info && (info.author || info.duration > 0) && (
          <div className="flex items-center gap-3 text-xs text-white/40">
            {info.author && <span>{info.author}</span>}
            {info.duration > 0 && <span>{fmtDuration(info.duration)}</span>}
          </div>
        )}

        {/* Progress bar (khi đang tải) */}
        {status === "downloading" && progress && (
          <div className="space-y-0.5">
            <div className="relative h-1.5 overflow-hidden rounded-full bg-green-500/20">
              {hasTotal ? (
                <div
                  className="h-full rounded-full bg-green-500 transition-[width] duration-150 ease-out"
                  style={{ width: `${percent}%` }}
                />
              ) : (
                <div className="animate-progress-indeterminate absolute inset-y-0 w-1/3 rounded-full bg-green-500" />
              )}
            </div>
            <div className="flex justify-between text-[10px] tabular-nums text-white/40">
              <span>
                {fmtBytes(progress.downloaded)}
                {hasTotal && ` / ${fmtBytes(progress.total)}`}
              </span>
              {hasTotal && (
                <span className="font-semibold text-green-400">
                  {percent.toFixed(1)}%
                </span>
              )}
            </div>
          </div>
        )}

        {/* Error */}
        {status === "failed" && error && (
          <p className="line-clamp-2 text-xs text-red-300/80">{error}</p>
        )}

        {/* URL */}
        <p
          className="truncate font-mono text-[10px] text-white/25"
          title={url}
        >
          {url}
        </p>
      </div>

      {/* Retry buttons */}
      {status === "failed" && (
        <div className="flex shrink-0 flex-col gap-1">
          {info && canRetryDownload && (
            <button
              onClick={onRetryDownload}
              className="btn-ripple flex items-center gap-1 rounded-lg bg-green-500/15 px-2 py-1 text-[11px] font-medium text-green-300 hover:bg-green-500/25"
            >
              <span className="material-symbols-rounded text-sm">download</span>
              Tải lại
            </button>
          )}
          {!info && (
            <button
              onClick={onRetryFetch}
              className="btn-ripple flex items-center gap-1 rounded-lg bg-shopee-500/15 px-2 py-1 text-[11px] font-medium text-shopee-300 hover:bg-shopee-500/25"
            >
              <span className="material-symbols-rounded text-sm">refresh</span>
              Tìm lại
            </button>
          )}
        </div>
      )}
    </li>
  );
}
