import { useState } from "react";
import { fmtBytes } from "../formulas";

/// Pretty-print 1 sync event với layout card thân thiện thay cho raw JSON.
/// Dùng chung cho MySyncLogDialog (local events) và SyncLogViewerDialog
/// (admin R2 events).
///
/// Design: icon + title/subtitle theo kind, data chính làm chips, key
/// truncate + copy. Raw JSON vẫn access được qua toggle "Raw".

export interface SyncEventView {
  eventId: number;
  ts: string;
  fingerprint: string;
  kind: string;
  ctx: unknown;
  uploadedAt: string | null;
}

interface SyncEventCardProps {
  event: SyncEventView;
}

/// Map tên table SQLite → label dễ hiểu cho user.
const TABLE_LABELS: Record<string, string> = {
  imported_files: "File import",
  raw_shopee_clicks: "Click Shopee",
  raw_shopee_order_items: "Đơn hàng Shopee",
  raw_fb_ads: "Quảng cáo FB",
  clicks_to_file: "Map Click ↔ File",
  orders_to_file: "Map Đơn ↔ File",
  fb_ads_to_file: "Map QC FB ↔ File",
  manual_entries: "Dòng nhập tay",
  tombstones: "Xóa (tombstone)",
  shopee_accounts: "TK Shopee",
};

/// Kind → visual style (icon + color + direction label).
interface KindVisual {
  icon: string;
  label: string;
  bg: string;
  fg: string;
  accent: string;
}

function kindVisual(kind: string): KindVisual {
  switch (kind) {
    case "push_start":
      return {
        icon: "upload",
        label: "Bắt đầu đẩy",
        bg: "bg-shopee-900/30",
        fg: "text-shopee-200",
        accent: "border-l-shopee-500",
      };
    case "push_upload":
      return {
        icon: "arrow_upward",
        label: "Đẩy lên R2",
        bg: "bg-shopee-900/30",
        fg: "text-shopee-200",
        accent: "border-l-shopee-500",
      };
    case "push_complete":
      return {
        icon: "task_alt",
        label: "Đẩy xong",
        bg: "bg-shopee-900/20",
        fg: "text-shopee-300",
        accent: "border-l-shopee-500",
      };
    case "pull_start":
      return {
        icon: "download",
        label: "Bắt đầu kéo",
        bg: "bg-blue-900/30",
        fg: "text-blue-200",
        accent: "border-l-blue-500",
      };
    case "pull_fetch":
      return {
        icon: "arrow_downward",
        label: "Kéo delta",
        bg: "bg-blue-900/30",
        fg: "text-blue-200",
        accent: "border-l-blue-500",
      };
    case "pull_apply":
      return {
        icon: "merge_type",
        label: "Áp dụng delta",
        bg: "bg-blue-900/20",
        fg: "text-blue-200",
        accent: "border-l-blue-500",
      };
    case "pull_complete":
      return {
        icon: "task_alt",
        label: "Kéo xong",
        bg: "bg-blue-900/20",
        fg: "text-blue-300",
        accent: "border-l-blue-500",
      };
    case "bootstrap_start":
    case "bootstrap_snapshot":
    case "bootstrap_complete":
      return {
        icon: "auto_awesome",
        label: kind.replace("bootstrap_", "Khởi tạo "),
        bg: "bg-purple-900/30",
        fg: "text-purple-200",
        accent: "border-l-purple-500",
      };
    case "compaction_start":
    case "compaction_complete":
      return {
        icon: "compress",
        label: kind === "compaction_start" ? "Bắt đầu nén" : "Nén xong",
        bg: "bg-amber-900/30",
        fg: "text-amber-200",
        accent: "border-l-amber-500",
      };
    case "cas_conflict":
      return {
        icon: "warning",
        label: "Xung đột CAS",
        bg: "bg-orange-900/30",
        fg: "text-orange-200",
        accent: "border-l-orange-500",
      };
    case "error":
      return {
        icon: "error",
        label: "Lỗi",
        bg: "bg-red-900/30",
        fg: "text-red-200",
        accent: "border-l-red-500",
      };
    case "recovery":
      return {
        icon: "healing",
        label: "Khôi phục",
        bg: "bg-green-900/30",
        fg: "text-green-200",
        accent: "border-l-green-500",
      };
    default:
      return {
        icon: "info",
        label: kind,
        bg: "bg-surface-4",
        fg: "text-white/80",
        accent: "border-l-surface-8",
      };
  }
}


function fmtCount(n: number): string {
  return n.toLocaleString("vi-VN");
}

function fmtTs(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString("vi-VN", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      day: "2-digit",
      month: "2-digit",
    });
  } catch {
    return iso;
  }
}

/// Shorten R2 key cho UI — lấy 2 phần cuối, ellipsis đầu.
/// `deltas/raw_shopee_clicks/3956_177700.ndjson.zst` → `raw_shopee_clicks/3956_177700.ndjson.zst`
function shortKey(key: string): string {
  const parts = key.split("/");
  if (parts.length <= 2) return key;
  return ".../" + parts.slice(-2).join("/");
}

function tableLabel(table: string): string {
  return TABLE_LABELS[table] ?? table;
}

export function SyncEventCard({ event }: SyncEventCardProps) {
  const [showRaw, setShowRaw] = useState(false);
  const visual = kindVisual(event.kind);
  const ctx = (event.ctx ?? {}) as Record<string, unknown>;

  return (
    <li
      className={`rounded-lg border border-surface-8 border-l-[4px] bg-surface-2 px-4 py-3 ${visual.accent}`}
    >
      {/* Header: icon + label + time + flush status */}
      <div className="flex flex-wrap items-center gap-2.5">
        <span className="text-[15px] text-white/35">#{event.eventId}</span>
        <div
          className={`flex items-center gap-1.5 rounded-md px-2 py-1 ${visual.bg}`}
        >
          <span
            className={`material-symbols-rounded text-lg leading-none ${visual.fg}`}
          >
            {visual.icon}
          </span>
          <span className={`text-base font-semibold ${visual.fg}`}>
            {visual.label}
          </span>
        </div>
        <span className="text-base text-white/70">{fmtTs(event.ts)}</span>
        <span className="ml-auto flex items-center gap-2 text-[15px]">
          {event.uploadedAt ? (
            <span
              className="flex items-center gap-1 text-green-300/80"
              title={`Log đã upload lên R2: ${event.uploadedAt}`}
            >
              <span className="h-2 w-2 rounded-full bg-green-400/80" />
              Log on R2
            </span>
          ) : (
            <span
              className="flex items-center gap-1 text-white/40"
              title="Log observability chưa upload lên R2 (flush lazy — upload theo batch/date rollover). Data đã sync bình thường, log chỉ là debug trail."
            >
              <span className="h-2 w-2 rounded-full bg-white/30" />
              Log pending
            </span>
          )}
          <button
            type="button"
            onClick={() => setShowRaw((v) => !v)}
            className="ml-1 rounded px-2 py-0.5 text-[14px] text-white/50 hover:bg-white/10 hover:text-white/80"
            title="Xem JSON gốc"
          >
            {showRaw ? "Gọn" : "Raw"}
          </button>
        </span>
      </div>

      {/* Body — theo kind */}
      <div className="mt-2.5">{renderBody(event.kind, ctx)}</div>

      {showRaw && (
        <pre className="mt-2.5 overflow-x-auto whitespace-pre-wrap break-all rounded bg-surface-0 px-3 py-2 font-mono text-[14px] text-white/60">
          {JSON.stringify(event.ctx, null, 2)}
        </pre>
      )}
    </li>
  );
}

/// Render body chính theo kind. Fallback = compact key-value list.
function renderBody(kind: string, ctx: Record<string, unknown>) {
  switch (kind) {
    case "push_upload":
      return (
        <PushUploadBody
          table={String(ctx.table ?? "?")}
          bytes={Number(ctx.bytes ?? 0)}
          rowCount={Number(ctx.row_count ?? 0)}
          cursorLo={String(ctx.cursor_lo ?? "")}
          cursorHi={String(ctx.cursor_hi ?? "")}
          deltaKey={String(ctx.delta_key ?? "")}
        />
      );
    case "pull_fetch":
      return (
        <PullFetchBody
          deltaKey={String(ctx.delta_key ?? "")}
          bytes={Number(ctx.bytes ?? 0)}
          rowCount={Number(ctx.row_count ?? 0)}
        />
      );
    case "pull_apply":
      return (
        <PullApplyBody
          deltaKey={String(ctx.delta_key ?? "")}
          rowCount={Number(ctx.row_count ?? 0)}
          skipped={Number(ctx.skipped ?? 0)}
          resurrected={Number(ctx.resurrected ?? 0)}
        />
      );
    case "push_start":
      return (
        <div className="flex flex-wrap items-center gap-1.5 text-[15px] text-white/75">
          <span className="text-white/50">Tables:</span>
          {(ctx.tables as string[])?.map((t) => (
            <Chip key={t} label={tableLabel(t)} />
          ))}
        </div>
      );
    case "push_complete":
      return (
        <div className="flex flex-wrap gap-1.5 text-[15px]">
          <Chip label={`${fmtCount(Number(ctx.uploaded_count ?? 0))} file đẩy`} />
          {Number(ctx.skipped_identical ?? 0) > 0 && (
            <Chip
              label={`${ctx.skipped_identical} skip`}
              tone="neutral"
              title="Skip-identical — nội dung table không đổi, tiết kiệm bandwidth"
            />
          )}
          {Number(ctx.total_bytes ?? 0) > 0 && (
            <Chip label={fmtBytes(Number(ctx.total_bytes ?? 0))} tone="volume" />
          )}
        </div>
      );
    case "pull_complete":
      return (
        <div className="flex flex-wrap gap-1.5 text-[15px]">
          <Chip
            label={`${fmtCount(Number(ctx.deltas_applied ?? 0))} delta apply`}
          />
          {Number(ctx.duration_ms ?? 0) > 0 && (
            <Chip label={`${ctx.duration_ms}ms`} tone="neutral" />
          )}
        </div>
      );
    case "cas_conflict":
      return (
        <div className="text-[15px] text-orange-200">
          Attempt {String(ctx.attempt ?? "?")} / {String(ctx.max_retries ?? "?")}
        </div>
      );
    case "error":
      return (
        <div className="rounded-md bg-red-950/40 px-3 py-1.5 text-[15px] text-red-200">
          {String(ctx.message ?? "Lỗi không xác định")}
        </div>
      );
    case "compaction_complete":
      return (
        <div className="flex flex-wrap gap-1.5 text-[15px]">
          <Chip
            label={`${fmtCount(Number(ctx.old_deltas_removed ?? 0))} delta cleaned`}
          />
          <Chip
            label={shortKey(String(ctx.new_snapshot_key ?? ""))}
            tone="key"
          />
        </div>
      );
    default:
      return (
        <FallbackBody ctx={ctx} />
      );
  }
}

function PushUploadBody({
  table,
  bytes,
  rowCount,
  cursorLo,
  cursorHi,
  deltaKey,
}: {
  table: string;
  bytes: number;
  rowCount: number;
  cursorLo: string;
  cursorHi: string;
  deltaKey: string;
}) {
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex flex-wrap items-center gap-1.5 text-[15px]">
        <Chip label={tableLabel(table)} tone="table" />
        <Chip label={`${fmtCount(rowCount)} dòng`} />
        <Chip label={fmtBytes(bytes)} tone="volume" />
        <span
          className="text-[14px] text-white/50"
          title={`Cursor range: ${cursorLo} → ${cursorHi}`}
        >
          {isShortCursor(cursorLo) && isShortCursor(cursorHi)
            ? `${cursorLo} → ${cursorHi}`
            : "cursor (hash)"}
        </span>
      </div>
      <KeyWithCopy k={deltaKey} />
    </div>
  );
}

function PullFetchBody({
  deltaKey,
  bytes,
  rowCount,
}: {
  deltaKey: string;
  bytes: number;
  rowCount: number;
}) {
  const match = /^deltas\/([^/]+)\//.exec(deltaKey);
  const table = match?.[1];
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex flex-wrap items-center gap-1.5 text-[15px]">
        {table && <Chip label={tableLabel(table)} tone="table" />}
        {rowCount > 0 && <Chip label={`${fmtCount(rowCount)} events`} />}
        <Chip label={fmtBytes(bytes)} tone="volume" />
      </div>
      <KeyWithCopy k={deltaKey} />
    </div>
  );
}

function PullApplyBody({
  deltaKey,
  rowCount,
  skipped,
  resurrected,
}: {
  deltaKey: string;
  rowCount: number;
  skipped: number;
  resurrected: number;
}) {
  const match = /^deltas\/([^/]+)\//.exec(deltaKey);
  const table = match?.[1];
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex flex-wrap items-center gap-1.5 text-[15px]">
        {table && <Chip label={tableLabel(table)} tone="table" />}
        <Chip label={`${fmtCount(rowCount)} events`} />
        {skipped > 0 && (
          <Chip
            label={`${skipped} skip`}
            tone="neutral"
            title="HLC skip — local row mới hơn event"
          />
        )}
        {resurrected > 0 && (
          <Chip
            label={`${resurrected} resurrect`}
            tone="neutral"
            title="Resurrect — edit local sau delete remote → giữ row"
          />
        )}
      </div>
      <KeyWithCopy k={deltaKey} />
    </div>
  );
}

function FallbackBody({ ctx }: { ctx: Record<string, unknown> }) {
  const entries = Object.entries(ctx)
    .filter(([k]) => k !== "kind")
    .slice(0, 6);
  if (entries.length === 0) {
    return <span className="text-[15px] text-white/45">(không có chi tiết)</span>;
  }
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1 text-[15px] text-white/70">
      {entries.map(([k, v]) => (
        <span key={k}>
          <span className="text-white/45">{k}:</span>{" "}
          <span className="font-mono text-white/90">{fmtVal(v)}</span>
        </span>
      ))}
    </div>
  );
}

function fmtVal(v: unknown): string {
  if (v === null || v === undefined) return "null";
  if (typeof v === "string") {
    return v.length > 40 ? v.slice(0, 40) + "…" : v;
  }
  if (typeof v === "number") return v.toLocaleString("vi-VN");
  if (typeof v === "boolean") return v ? "true" : "false";
  return JSON.stringify(v);
}

/// Cursor < 10 digits → numeric (rowid) hiện thực. > → hash content_id, hide.
function isShortCursor(s: string): boolean {
  return s.length > 0 && s.length <= 10 && /^\d+$/.test(s);
}

type ChipTone = "default" | "table" | "volume" | "neutral" | "key";

function Chip({
  label,
  tone = "default",
  title,
}: {
  label: string;
  tone?: ChipTone;
  title?: string;
}) {
  const styles: Record<ChipTone, string> = {
    default: "bg-surface-6 text-white/85",
    table: "bg-shopee-900/40 text-shopee-200",
    volume: "bg-blue-900/40 text-blue-200",
    neutral: "bg-surface-8 text-white/65",
    key: "bg-surface-0 font-mono text-[14px] text-white/55",
  };
  return (
    <span
      className={`rounded-md px-2 py-0.5 text-[15px] font-medium ${styles[tone]}`}
      title={title}
    >
      {label}
    </span>
  );
}

function KeyWithCopy({ k }: { k: string }) {
  const [copied, setCopied] = useState(false);
  if (!k) return null;
  const onCopy = async () => {
    try {
      await navigator.clipboard.writeText(k);
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    } catch {
      // ignore
    }
  };
  return (
    <div
      className="flex items-center gap-1.5 text-[14px] text-white/45"
      title={k}
    >
      <span className="material-symbols-rounded text-base">folder_data</span>
      <span className="truncate font-mono">{shortKey(k)}</span>
      <button
        type="button"
        onClick={onCopy}
        className="ml-auto shrink-0 rounded px-2 py-0.5 text-[13px] text-white/50 hover:bg-white/10 hover:text-white/80"
      >
        {copied ? "✓" : "Copy"}
      </button>
    </div>
  );
}
