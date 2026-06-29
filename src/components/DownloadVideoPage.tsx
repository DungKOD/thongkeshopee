import { useCallback, useEffect, useMemo, useState } from "react";
import { open } from "@tauri-apps/plugin-dialog";
import { listen } from "@tauri-apps/api/event";
import { invoke } from "../lib/tauri";
import {
  listVideoDownloads,
  logVideoDownload,
  type VideoDownloadLog,
} from "../lib/video";
import {
  applyVideoWatermark,
  type WatermarkProgressEvent,
  type WatermarkStageEvent,
} from "../lib/videoWatermark";
import { useFbPages } from "../hooks/useFbPages";
import { useSettings } from "../hooks/useSettings";
import type { FbPage } from "../lib/fbReels";
import { fmtBytes, fmtHistoryTime } from "../formulas";

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

type ItemStatus =
  | "fetching"
  | "ready"
  | "downloading"
  | "watermarking"
  | "done"
  | "failed";

interface WatermarkState {
  stage: WatermarkStageEvent["stage"];
  message: string;
  percent: number;
}

interface BatchItem {
  id: string;
  url: string;
  status: ItemStatus;
  info: VideoInfo | null;
  progress: ProgressPayload | null;
  watermark: WatermarkState | null;
  /// Path file local sau khi download xong — dùng để gọi watermark.
  savedPath: string;
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

const LS_WATERMARK_ENABLED = "download:watermark:enabled";
const LS_WATERMARK_PAGE_ID = "download:watermark:pageId";

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

  // Watermark state: persist toggle + page chọn vào localStorage để giữ qua
  // tab switch + reload. localStorage thay vì app_settings vì state này là UI-
  // only (không cần share giữa devices).
  const { pages: fbPages } = useFbPages();
  const { settings } = useSettings();
  const [watermarkEnabled, setWatermarkEnabled] = useState<boolean>(() => {
    try {
      return JSON.parse(localStorage.getItem(LS_WATERMARK_ENABLED) ?? "false");
    } catch {
      return false;
    }
  });
  const [watermarkPageId, setWatermarkPageId] = useState<string>(() => {
    try {
      return localStorage.getItem(LS_WATERMARK_PAGE_ID) ?? "";
    } catch {
      return "";
    }
  });
  useEffect(() => {
    try {
      localStorage.setItem(
        LS_WATERMARK_ENABLED,
        JSON.stringify(watermarkEnabled),
      );
    } catch {
      /* quota */
    }
  }, [watermarkEnabled]);
  useEffect(() => {
    try {
      if (watermarkPageId) {
        localStorage.setItem(LS_WATERMARK_PAGE_ID, watermarkPageId);
      } else {
        localStorage.removeItem(LS_WATERMARK_PAGE_ID);
      }
    } catch {
      /* quota */
    }
  }, [watermarkPageId]);
  // Auto-pick page đầu tiên nếu user bật toggle mà chưa chọn page nào.
  useEffect(() => {
    if (watermarkEnabled && !watermarkPageId && fbPages.length > 0) {
      setWatermarkPageId(fbPages[0].pageId);
    }
  }, [watermarkEnabled, watermarkPageId, fbPages]);
  // Nếu Page đã chọn bị xóa khỏi danh sách → reset.
  useEffect(() => {
    if (
      watermarkPageId &&
      fbPages.length > 0 &&
      !fbPages.some((p) => p.pageId === watermarkPageId)
    ) {
      setWatermarkPageId(fbPages[0]?.pageId ?? "");
    }
  }, [fbPages, watermarkPageId]);

  const watermarkActive = useMemo(
    () => watermarkEnabled && !!watermarkPageId,
    [watermarkEnabled, watermarkPageId],
  );
  const selectedPage = useMemo(
    () => fbPages.find((p) => p.pageId === watermarkPageId) ?? null,
    [fbPages, watermarkPageId],
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

  // Watermark progress + stage listeners. `watermarkId` = item.id (1 watermark
  // per video). Backend emit cả 2 channel song song; UI merge vào `watermark`
  // field. Tách 2 channel để FE biết khi nào đang download ffmpeg (stage) vs
  // tiến độ encode (progress %).
  useEffect(() => {
    const unsubProgress = listen<WatermarkProgressEvent>(
      "watermark-progress",
      (e) => {
        const p = e.payload;
        setItems((prev) =>
          prev.map((item) =>
            item.id === p.watermarkId
              ? {
                  ...item,
                  watermark: {
                    stage: item.watermark?.stage ?? "encoding",
                    message:
                      item.watermark?.message ?? "Đang gắn logo...",
                    percent: p.percent,
                  },
                }
              : item,
          ),
        );
      },
    );
    const unsubStage = listen<WatermarkStageEvent>("watermark-stage", (e) => {
      const p = e.payload;
      setItems((prev) =>
        prev.map((item) =>
          item.id === p.watermarkId
            ? {
                ...item,
                watermark: {
                  stage: p.stage,
                  message: p.message,
                  percent: item.watermark?.percent ?? 0,
                },
              }
            : item,
        ),
      );
    });
    return () => {
      unsubProgress.then((fn) => fn());
      unsubStage.then((fn) => fn());
    };
  }, []);

  const downloadOne = useCallback(
    async (
      item: BatchItem,
      dir: string,
      watermark: { enabled: boolean; pageId: string } | null,
      watermarkOptions: {
        sizePct: number;
        opacity: number;
        paddingPct: number;
        antiTheft: boolean;
      },
    ) => {
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
                savedPath: savePath,
                watermark: null,
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

        // Sau khi tải xong: nếu watermark active, chạy gắn logo. Lỗi watermark
        // KHÔNG đánh dấu item là failed — file đã tải về OK, chỉ logo fail.
        // FE hiện trạng thái "done" + warning nhỏ để user retry watermark.
        if (watermark && watermark.enabled && watermark.pageId) {
          setItems((prev) =>
            prev.map((i) =>
              i.id === item.id
                ? {
                    ...i,
                    status: "watermarking",
                    progress: null,
                    watermark: {
                      stage: "preparing",
                      message: "Đang chuẩn bị gắn logo...",
                      percent: 0,
                    },
                  }
                : i,
            ),
          );
          try {
            await applyVideoWatermark(
              savePath,
              watermark.pageId,
              watermarkOptions,
              item.id,
            );
            setItems((prev) =>
              prev.map((i) =>
                i.id === item.id
                  ? { ...i, status: "done", watermark: null }
                  : i,
              ),
            );
          } catch (wErr) {
            setItems((prev) =>
              prev.map((i) =>
                i.id === item.id
                  ? {
                      ...i,
                      status: "done",
                      watermark: null,
                      error: `Tải OK, gắn logo lỗi: ${String(wErr)}`,
                    }
                  : i,
              ),
            );
          }
        } else {
          setItems((prev) =>
            prev.map((i) =>
              i.id === item.id ? { ...i, status: "done", progress: null } : i,
            ),
          );
        }
        void logVideoDownload(item.url, "success");
      } catch (e) {
        setItems((prev) =>
          prev.map((i) =>
            i.id === item.id
              ? { ...i, status: "failed", error: String(e), progress: null }
              : i,
          ),
        );
        void logVideoDownload(item.url, "failed");
      } finally {
        setDownloadingCount((n) => n - 1);
      }
    },
    [],
  );

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
      void logVideoDownload(item.url, "success");
    } catch (e) {
      setItems((prev) =>
        prev.map((i) =>
          i.id === item.id ? { ...i, status: "failed", error: String(e) } : i,
        ),
      );
      void logVideoDownload(item.url, "failed");
    }
  }, []);

  const handleFetchAll = async () => {
    const urls = parseUrls(urlsText);
    if (urls.length === 0 || downloadingCount > 0) return;

    const newItems: BatchItem[] = urls.map((url) => ({
      id: genId(),
      url,
      status: "fetching" as ItemStatus,
      info: null,
      progress: null,
      watermark: null,
      savedPath: "",
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

    // Snapshot watermark config 1 lần trước khi pool — tránh race state đổi
    // giữa batch. User toggle giữa chừng không ảnh hưởng video đang xử lý.
    const wmConfig = watermarkActive
      ? { enabled: true, pageId: watermarkPageId }
      : null;
    const wmOptions = {
      sizePct: settings.videoWatermark.sizePct,
      opacity: settings.videoWatermark.opacity,
      paddingPct: settings.videoWatermark.paddingPct,
      antiTheft: settings.videoWatermark.antiTheft,
    };

    // Pool pattern: tối đa MAX_CONCURRENT luồng đồng thời. Mỗi luồng:
    // download → (optional) watermark, giữ slot pool cho tới khi cả 2 xong.
    // Tốc độ batch không bị watermark step làm chậm chuyển sang video kế.
    const pool = new Set<Promise<void>>();
    for (const item of readyItems) {
      const p: Promise<void> = downloadOne(
        item,
        saveDir,
        wmConfig,
        wmOptions,
      ).finally(() => pool.delete(p));
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

      {/* ===== Watermark control ===== */}
      <WatermarkControl
        enabled={watermarkEnabled}
        onToggle={setWatermarkEnabled}
        pageId={watermarkPageId}
        onPageChange={setWatermarkPageId}
        pages={fbPages}
        selectedPage={selectedPage}
        sizePct={settings.videoWatermark.sizePct}
        opacity={settings.videoWatermark.opacity}
        paddingPct={settings.videoWatermark.paddingPct}
        antiTheft={settings.videoWatermark.antiTheft}
      />

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
                onRetryDownload={() =>
                  void downloadOne(
                    item,
                    saveDir,
                    watermarkActive
                      ? { enabled: true, pageId: watermarkPageId }
                      : null,
                    {
                      sizePct: settings.videoWatermark.sizePct,
                      opacity: settings.videoWatermark.opacity,
                      paddingPct: settings.videoWatermark.paddingPct,
                      antiTheft: settings.videoWatermark.antiTheft,
                    },
                  )
                }
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

interface WatermarkControlProps {
  enabled: boolean;
  onToggle: (v: boolean) => void;
  pageId: string;
  onPageChange: (id: string) => void;
  pages: FbPage[];
  selectedPage: FbPage | null;
  sizePct: number;
  opacity: number;
  paddingPct: number;
  antiTheft: boolean;
}

function WatermarkControl({
  enabled,
  onToggle,
  pageId,
  onPageChange,
  pages,
  selectedPage,
  sizePct,
  opacity,
  paddingPct,
  antiTheft,
}: WatermarkControlProps) {
  const hasPages = pages.length > 0;
  const active = enabled && hasPages && !!pageId;

  return (
    <section
      className={`overflow-hidden rounded-2xl border shadow-elev-2 transition-colors ${
        active
          ? "border-violet-500/40 bg-gradient-to-br from-violet-950/40 via-surface-2 to-surface-2"
          : "border-surface-8 bg-surface-2"
      }`}
    >
      <div className="flex flex-wrap items-center gap-3 px-4 py-3">
        <span
          className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-lg ${
            active
              ? "bg-violet-500/20 text-violet-300"
              : "bg-surface-6 text-white/50"
          }`}
        >
          <span className="material-symbols-rounded text-xl">
            branding_watermark
          </span>
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-white/90">
              Tự động gắn logo Page lên video
            </span>
            {active && (
              <span className="rounded-full bg-violet-500/20 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-violet-300">
                Bật
              </span>
            )}
            {active && antiTheft && (
              <span
                className="inline-flex items-center gap-0.5 rounded-full bg-amber-500/20 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-amber-300"
                title="Logo nhảy 4 góc mỗi 5s — chống ăn chôm video"
              >
                <span className="material-symbols-rounded text-xs">
                  shield
                </span>
                Anti-theft
              </span>
            )}
          </div>
          <p className="mt-0.5 text-xs text-white/55">
            Sau khi tải xong, app tự overlay avatar Page vào góc trên phải.
            Tinh chỉnh size/opacity/padding ở Cài đặt.
          </p>
        </div>
        <label className="inline-flex cursor-pointer items-center gap-2">
          <input
            type="checkbox"
            checked={enabled}
            disabled={!hasPages}
            onChange={(e) => onToggle(e.currentTarget.checked)}
            className="peer sr-only"
          />
          <span
            className={`relative h-6 w-11 rounded-full transition-colors ${
              enabled && hasPages
                ? "bg-violet-500"
                : "bg-surface-8"
            } peer-disabled:opacity-50`}
          >
            <span
              className={`absolute top-0.5 left-0.5 h-5 w-5 rounded-full bg-white shadow transition-transform ${
                enabled ? "translate-x-5" : ""
              }`}
            />
          </span>
        </label>
      </div>

      {!hasPages ? (
        <div className="flex items-start gap-2 border-t border-surface-8 bg-surface-1/60 px-4 py-2.5 text-xs text-amber-200">
          <span className="material-symbols-rounded mt-0.5 text-sm text-amber-400">
            info
          </span>
          <span>
            Chưa có Page nào — sang tab "Đăng video" để thêm Page trước.
          </span>
        </div>
      ) : enabled ? (
        <div className="flex flex-wrap items-center gap-3 border-t border-violet-500/20 bg-violet-950/20 px-4 py-2.5">
          <span className="text-xs font-medium text-white/55">Chọn Page:</span>
          <select
            value={pageId}
            onChange={(e) => onPageChange(e.currentTarget.value)}
            className="min-w-0 flex-1 rounded-lg border border-violet-500/30 bg-surface-1 px-3 py-1.5 text-sm text-white/90 focus:border-violet-400 focus:outline-none focus:ring-1 focus:ring-violet-400"
          >
            {pages.map((p) => (
              <option key={p.pageId} value={p.pageId}>
                {p.name}
                {p.tokenExpired ? " (token hết hạn)" : ""}
              </option>
            ))}
          </select>
          <span className="ml-auto flex shrink-0 items-center gap-2 text-[11px] text-white/45">
            <span className="font-mono tabular-nums">{Math.round(sizePct)}%</span>
            <span className="text-white/25">·</span>
            <span className="font-mono tabular-nums">
              {opacity.toFixed(2)}
            </span>
            <span className="text-white/25">·</span>
            <span className="font-mono tabular-nums">
              {Math.round(paddingPct)}%
            </span>
          </span>
          {selectedPage?.tokenExpired && (
            <div className="flex w-full items-start gap-1.5 rounded-lg border border-amber-500/30 bg-amber-950/20 px-2.5 py-1.5 text-[11px] text-amber-200">
              <span className="material-symbols-rounded mt-0.5 text-sm text-amber-400">
                warning
              </span>
              <span>
                Token Page hết hạn — vẫn có thể gắn logo (avatar Page là public),
                nhưng nên cập nhật token ở tab "Đăng video".
              </span>
            </div>
          )}
        </div>
      ) : null}
    </section>
  );
}

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
    watermarking: (
      <span className="material-symbols-rounded animate-spin text-base text-violet-400">
        auto_fix_high
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

        {/* Watermark progress bar */}
        {status === "watermarking" && item.watermark && (
          <div className="space-y-0.5">
            <div className="relative h-1.5 overflow-hidden rounded-full bg-violet-500/20">
              {item.watermark.stage === "encoding" &&
              item.watermark.percent > 0 ? (
                <div
                  className="h-full rounded-full bg-violet-500 transition-[width] duration-150 ease-out"
                  style={{ width: `${item.watermark.percent}%` }}
                />
              ) : (
                <div className="animate-progress-indeterminate absolute inset-y-0 w-1/3 rounded-full bg-violet-500" />
              )}
            </div>
            <div className="flex justify-between text-[10px] tabular-nums text-white/40">
              <span className="truncate">{item.watermark.message}</span>
              {item.watermark.stage === "encoding" &&
                item.watermark.percent > 0 && (
                  <span className="font-semibold text-violet-300">
                    {item.watermark.percent.toFixed(0)}%
                  </span>
                )}
            </div>
          </div>
        )}

        {/* Done with warning (tải xong nhưng watermark fail) */}
        {status === "done" && error && (
          <p className="line-clamp-2 text-xs text-amber-300/80">
            <span className="material-symbols-rounded mr-1 align-middle text-sm text-amber-400">
              warning
            </span>
            {error}
          </p>
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
