/**
 * CSV import pipeline cho kiến trúc ELT.
 *
 * Flow: file → papaparse → convert rows → gửi qua Tauri invoke → Rust validate
 * single-date + INSERT raw tables + copy file gốc → trả ImportResult.
 *
 * Mỗi kind có 1 parser riêng vì schema CSV hoàn toàn khác. Rust command nhận
 * structured payload (camelCase) khớp với các interface bên dưới.
 */

import Papa from "papaparse";
import { invoke } from "./tauri";
import type { SubIds } from "../types";

// =========================================================
// Types khớp với Rust DTO (src-tauri/src/commands/imports.rs)
// =========================================================

export type CsvKind =
  | "shopee_clicks"
  | "shopee_commission"
  | "fb_ad_group"
  | "fb_campaign";

export type DetectedKind = CsvKind | "unknown";

export interface ShopeeClickRow {
  clickId: string;
  clickTime: string;
  region: string | null;
  subIdRaw: string | null;
  subIds: SubIds;
  referrer: string | null;
}

export interface ShopeeOrderRow {
  orderId: string;
  checkoutId: string;
  itemId: string;
  modelId: string;
  orderStatus: string | null;
  orderTime: string;
  completedTime: string | null;
  clickTime: string | null;
  shopId: string | null;
  shopName: string | null;
  shopType: string | null;
  itemName: string | null;
  categoryL1: string | null;
  categoryL2: string | null;
  categoryL3: string | null;
  price: number | null;
  quantity: number | null;
  orderValue: number | null;
  refundAmount: number | null;
  netCommission: number | null;
  commissionTotal: number | null;
  /** CSV col 31 "Tổng hoa hồng đơn hàng(₫)" — trước khi trừ phí MCN.
   *  Relationship: netCommission = orderCommissionTotal − mcnFee. */
  orderCommissionTotal: number | null;
  /** CSV col 35 "Phí quản lý MCN(₫)" — Shopee đã cắt sẵn khỏi netCommission. */
  mcnFee: number | null;
  subIds: SubIds;
  channel: string | null;
}

export interface FbAdGroupRow {
  adGroupName: string;
  subIds: SubIds;
  reportStart: string;
  reportEnd: string;
  status: string | null;
  spend: number | null;
  impressions: number | null;
  reach: number | null;
  frequency: number | null;
  linkClicks: number | null;
  shopClicks: number | null;
  allClicks: number | null;
  linkCpc: number | null;
  allCpc: number | null;
  linkCtr: number | null;
  allCtr: number | null;
  landingViews: number | null;
  cpm: number | null;
  resultCount: number | null;
  costPerResult: number | null;
}

export interface FbCampaignRow {
  campaignName: string;
  subIds: SubIds;
  reportStart: string;
  reportEnd: string;
  status: string | null;
  spend: number | null;
  impressions: number | null;
  reach: number | null;
  resultCount: number | null;
  resultIndicator: string | null;
  linkClicks: number | null;
  allClicks: number | null;
  linkCpc: number | null;
  allCpc: number | null;
  costPerResult: number | null;
}

export interface ImportResult {
  importedFileId: number;
  /** Earliest date — backward compat. */
  dayDate: string;
  dayDateFrom: string;
  dayDateTo: string;
  rowCount: number;
  inserted: number;
  duplicated: number;
  /** Rows bị skip do date không parse (Shopee multi-day). */
  skipped: number;
  /** Shopee commission only: số row lệch `net = order_total - mcn_fee` > 0.5đ.
   *  0 cho các kind khác. FE nên show warning banner nếu > 0. */
  mcnMismatchCount?: number;
}

// =========================================================
// Helpers
// =========================================================

/**
 * Split chuỗi bằng `-`, trim từng phần, pad/truncate về đúng 5 slot.
 * Nếu >5 phần → nối phần thừa vào slot cuối (safe: không mất data).
 * Ví dụ: `"shop-slug-0412"` → `["shop", "slug", "0412", "", ""]`
 */
export function parseSubIdString(raw: string): SubIds {
  const parts = raw
    .split("-")
    .map((p) => p.trim());
  const out: string[] = ["", "", "", "", ""];
  for (let i = 0; i < 5 && i < parts.length; i++) {
    out[i] = parts[i];
  }
  if (parts.length > 5) {
    // Slot 5 gom phần thừa để không mất thông tin khi user đặt tên nhiều hơn 5 slot.
    out[4] = parts.slice(4).join("-");
  }
  return out as SubIds;
}

function parseNumOrNull(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (!s) return null;
  // Bỏ ký tự % cuối nếu có (FB CTR).
  const cleaned = s.replace(/%$/, "").replace(/,/g, "");
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function strOrNull(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s || null;
}

// buildRawJson removed v9 — raw_json column dropped. CSV gốc lưu
// imports/<hash>.csv nếu tương lai cần re-extract field phụ.

// =========================================================
// Detect kind
// =========================================================

export function detectKind(headers: string[]): DetectedKind {
  const h = headers.map((x) => (x ?? "").toLowerCase().trim());
  const has = (needle: string) => h.includes(needle.toLowerCase());
  if (has("tên nhóm quảng cáo")) return "fb_ad_group";
  if (has("tên chiến dịch")) return "fb_campaign";
  if (has("id đơn hàng") && h.some((x) => x.startsWith("sub_id2")))
    return "shopee_commission";
  if (has("click id") && has("sub_id")) return "shopee_clicks";
  return "unknown";
}

// =========================================================
// Row converters — CSV row → Rust payload row
// =========================================================

function toShopeeClickRow(r: Record<string, string>): ShopeeClickRow | null {
  const clickId = (r["Click id"] ?? "").trim();
  const clickTime = (r["Thời gian Click"] ?? "").trim();
  if (!clickId || !clickTime) return null;
  const subIdRaw = (r["Sub_id"] ?? "").trim();
  return {
    clickId,
    clickTime,
    region: strOrNull(r["Khu vực Click"]),
    subIdRaw: subIdRaw || null,
    subIds: parseSubIdString(subIdRaw),
    referrer: strOrNull(r["Người giới thiệu"]),
  };
}

function toShopeeOrderRow(r: Record<string, string>): ShopeeOrderRow | null {
  const orderId = (r["ID đơn hàng"] ?? "").trim();
  const checkoutId = (r["Checkout id"] ?? "").trim();
  const itemId = (r["Item id"] ?? "").trim();
  const orderTime = (r["Thời Gian Đặt Hàng"] ?? "").trim();
  if (!orderId || !checkoutId || !itemId || !orderTime) return null;
  const subIds: SubIds = [
    (r["Sub_id1"] ?? "").trim(),
    (r["Sub_id2"] ?? "").trim(),
    (r["Sub_id3"] ?? "").trim(),
    (r["Sub_id4"] ?? "").trim(),
    (r["Sub_id5"] ?? "").trim(),
  ];
  return {
    orderId,
    checkoutId,
    itemId,
    modelId: (r["ID Model"] ?? "").trim(),
    orderStatus: strOrNull(r["Trạng thái đặt hàng"]),
    orderTime,
    completedTime: strOrNull(r["Thời gian hoàn thành"]),
    clickTime: strOrNull(r["Thời gian Click"]),
    shopId: strOrNull(r["Shop id"]),
    shopName: strOrNull(r["Tên Shop"]),
    shopType: strOrNull(r["Loại Shop"]),
    itemName: strOrNull(r["Tên Item"]),
    categoryL1: strOrNull(r["L1 Danh mục toàn cầu"]),
    categoryL2: strOrNull(r["L2 Danh mục toàn cầu"]),
    categoryL3: strOrNull(r["L3 Danh mục toàn cầu"]),
    price: parseNumOrNull(r["Giá(₫)"]),
    quantity: parseNumOrNull(r["Số lượng"]),
    orderValue: parseNumOrNull(r["Giá trị đơn hàng (₫)"]),
    refundAmount: parseNumOrNull(r["Số tiền hoàn trả (₫)"]),
    netCommission: parseNumOrNull(r["Hoa hồng ròng tiếp thị liên kết(₫)"]),
    commissionTotal: parseNumOrNull(r["Tổng hoa hồng sản phẩm(₫)"]),
    orderCommissionTotal: parseNumOrNull(r["Tổng hoa hồng đơn hàng(₫)"]),
    mcnFee: parseNumOrNull(r["Phí quản lý MCN(₫)"]),
    subIds,
    channel: strOrNull(r["Kênh"]),
  };
}

function toFbAdGroupRow(r: Record<string, string>): FbAdGroupRow | null {
  const name = (r["Tên nhóm quảng cáo"] ?? "").trim();
  const reportStart = (r["Lượt bắt đầu báo cáo"] ?? "").trim();
  const reportEnd = (r["Lượt kết thúc báo cáo"] ?? "").trim();
  if (!name || !reportStart || !reportEnd) return null;
  return {
    adGroupName: name,
    subIds: parseSubIdString(name),
    reportStart,
    reportEnd,
    status: strOrNull(r["Phân phối nhóm quảng cáo"]),
    spend: parseNumOrNull(r["Số tiền đã chi tiêu (VND)"]),
    impressions: parseNumOrNull(r["Lượt hiển thị"]),
    reach: parseNumOrNull(r["Người tiếp cận"]),
    frequency: parseNumOrNull(r["Tần suất"]),
    linkClicks: parseNumOrNull(r["Lượt click vào liên kết"]),
    shopClicks: parseNumOrNull(r["shop_clicks"]),
    allClicks: parseNumOrNull(r["Lượt click (tất cả)"]),
    linkCpc: parseNumOrNull(
      r["CPC (chi phí trên mỗi lượt click vào liên kết) (VND)"],
    ),
    allCpc: parseNumOrNull(r["CPC (tất cả) (VND)"]),
    linkCtr: parseNumOrNull(r["CTR (tỷ lệ click vào liên kết)"]),
    allCtr: parseNumOrNull(r["CTR (Tất cả)"]),
    landingViews: parseNumOrNull(r["Lượt xem trang đích"]),
    cpm: parseNumOrNull(r["CPM (Chi phí trên mỗi 1.000 lượt hiển thị) (VND)"]),
    resultCount: parseNumOrNull(r["Kết quả"]),
    costPerResult: parseNumOrNull(r["Chi phí trên mỗi kết quả"]),
  };
}

function toFbCampaignRow(r: Record<string, string>): FbCampaignRow | null {
  const name = (r["Tên chiến dịch"] ?? "").trim();
  const reportStart = (r["Lượt bắt đầu báo cáo"] ?? "").trim();
  const reportEnd = (r["Lượt kết thúc báo cáo"] ?? "").trim();
  if (!name || !reportStart || !reportEnd) return null;
  return {
    campaignName: name,
    subIds: parseSubIdString(name),
    reportStart,
    reportEnd,
    status: strOrNull(r["Lượt phân phối chiến dịch"]),
    spend: parseNumOrNull(r["Số tiền đã chi tiêu (VND)"]),
    impressions: parseNumOrNull(r["Lượt hiển thị"]),
    reach: parseNumOrNull(r["Người tiếp cận"]),
    resultCount: parseNumOrNull(r["Kết quả"]),
    resultIndicator: strOrNull(r["Chỉ báo kết quả"]),
    linkClicks: parseNumOrNull(r["Lượt click vào liên kết"]),
    allClicks: parseNumOrNull(r["Lượt click (tất cả)"]),
    linkCpc: parseNumOrNull(
      r["CPC (chi phí trên mỗi lượt click vào liên kết) (VND)"],
    ),
    allCpc: parseNumOrNull(r["CPC (tất cả) (VND)"]),
    costPerResult: parseNumOrNull(r["Chi phí trên mỗi kết quả"]),
  };
}

// =========================================================
// Preview types (khớp Rust `ImportPreview` trong commands/preview.rs)
// =========================================================

export interface ImportPreview {
  kind: CsvKind;
  filename: string;
  /** Earliest date — backward compat. */
  dayDate: string;
  dayDateFrom: string;
  dayDateTo: string;
  totalRows: number;
  newRows: number;
  replaceRows: number;
  sampleReplace: string[];
  dayHasData: boolean;
  /** File này đã có trong DB VÀ mọi row đã tồn tại — re-import không thêm gì.
   *  FE skip commit. Nếu hash match nhưng có row thiếu (backfill scenario),
   *  field này = false và commit sẽ reuse entry cũ + UPSERT rows thiếu. */
  alreadyImported: boolean;
  /** Hash file đã có trong DB, bất kể có row thiếu không. FE dùng show note
   *  "đã import trước đó — đang backfill N dòng". */
  hashMatch: boolean;
  /** Nếu hashMatch=true: day_date của lần import trước. */
  existingDayDate: string | null;
  /** Rows không parse được date (Shopee multi-day only). */
  skipped: number;
  /** **FB only** — số rows toàn 0 (spend=0 AND clicks=0). 0 cho Shopee. */
  emptyRows: number;
  /** **FB only** — true nếu > 50% rows toàn 0. UI cảnh báo "file rỗng,
   *  import có thể không add value mới — confirm explicit?". SQL UPSERT
   *  đã có guard không đè value > 0 bằng 0, warning bổ sung observability. */
  mostlyEmpty: boolean;
  /** File này trùng nội dung với file khác trong CÙNG batch này (FE detect
   *  client-side, tránh commit 2 file giống nhau → UNIQUE constraint fail). */
  batchDuplicate: boolean;
  /** File bị reject ở FE parse stage (unknown kind, no valid rows, missing
   *  required column...). FE synthesize preview này không call Rust; commit
   *  skip. Dialog hiển thị reason để user thấy file nào bị bỏ + lý do. */
  rejected?: boolean;
  /** Lý do reject — hiện trong dialog. Set khi rejected=true. */
  rejectReason?: string;
}

/** Parsed + typed payload của 1 file, giữ trong RAM để commit sau preview.
 *  Variant `rejected` giữ file bị loại ở parse stage — không có payload để
 *  commit, chỉ giữ reason để dialog hiển thị.
 *
 *  FB payload (ad_group / campaign) có field `taxRate` (0..100) — % thuế TK
 *  FB chịu (VAT, business tax). Default 0 lúc parse, UI override per-file
 *  trong ImportPreviewDialog trước khi commit. Rust nhân (1 + tax/100) ở
 *  query time → spend hiển thị/aggregate là chi phí thật. */
export type ParsedFile =
  | { kind: "shopee_clicks"; file: File; payload: { filename: string; rawContent: string; rows: ShopeeClickRow[] } }
  | { kind: "shopee_commission"; file: File; payload: { filename: string; rawContent: string; rows: ShopeeOrderRow[] } }
  | { kind: "fb_ad_group"; file: File; payload: { filename: string; rawContent: string; rows: FbAdGroupRow[]; taxRate: number } }
  | { kind: "fb_campaign"; file: File; payload: { filename: string; rawContent: string; rows: FbCampaignRow[]; taxRate: number } }
  | { kind: "rejected"; file: File; detectedKind: DetectedKind; reason: string };

export interface PreviewBatch {
  /** Ngày chung duy nhất của batch. */
  dayDate: string;
  files: Array<{ parsed: ParsedFile; preview: ImportPreview }>;
}

// =========================================================
// Parse 1 file → ParsedFile
// =========================================================

/**
 * Campaign/ad group "không giá trị" = spend 0 VÀ clicks 0.
 * Skip lúc import để DB không phình bởi hàng trăm row inactive mỗi ngày.
 * Clicks xét theo chain `link → all → result` (match normalize ở Rust).
 */
function isFbValuable(r: {
  spend: number | null;
  linkClicks: number | null;
  allClicks: number | null;
  resultCount: number | null;
}): boolean {
  const spend = r.spend ?? 0;
  const clicks = r.linkClicks ?? r.allClicks ?? r.resultCount ?? 0;
  return spend !== 0 || clicks !== 0;
}

/**
 * Shopee export có duplicate `(checkout_id, item_id, model_id)` — 1 row có
 * commission thật + N rows dummy (comm=0, khác tí `Hoa hồng Xtra trên sản phẩm`).
 * Nếu gửi hết sang Rust → UPSERT `ON CONFLICT DO UPDATE` đè row thật bằng
 * row dummy → mất commission. Dedup tại JS giữ row `netCommission` MAX.
 */
function dedupShopeeOrders(rows: ShopeeOrderRow[]): ShopeeOrderRow[] {
  const byKey = new Map<string, ShopeeOrderRow>();
  for (const r of rows) {
    const key = `${r.checkoutId}|${r.itemId}|${r.modelId}`;
    const ex = byKey.get(key);
    const cur = r.netCommission ?? 0;
    if (!ex || cur > (ex.netCommission ?? 0)) {
      byKey.set(key, r);
    }
  }
  return Array.from(byKey.values());
}

/** Tạo rejected ParsedFile thay vì throw — caller (previewCsvBatch) đưa vào
 *  PreviewBatch để dialog hiển thị file + reason, không abort cả batch. */
function rejectedFile(
  file: File,
  detectedKind: DetectedKind,
  reason: string,
): ParsedFile {
  return { kind: "rejected", file, detectedKind, reason };
}

async function parseFile(file: File): Promise<ParsedFile> {
  const text = await file.text();
  const parsed = Papa.parse<Record<string, string>>(text, {
    header: true,
    skipEmptyLines: true,
  });
  const headers = parsed.meta.fields ?? [];
  const kind = detectKind(headers);

  if (kind === "unknown") {
    return rejectedFile(
      file,
      "unknown",
      `Không nhận diện được loại file. File phải là WebsiteClickReport, AffiliateCommissionReport, FB Ad Group hoặc FB Campaign.`,
    );
  }

  switch (kind) {
    case "shopee_clicks": {
      const rows = parsed.data
        .map(toShopeeClickRow)
        .filter((r): r is ShopeeClickRow => r !== null);
      if (rows.length === 0)
        return rejectedFile(file, kind, "Không có click hợp lệ trong file");
      return {
        kind,
        file,
        payload: { filename: file.name, rawContent: text, rows },
      };
    }
    case "shopee_commission": {
      // Guard: "ID Model" column bắt buộc phải có. Thiếu cột này → modelId
      // default="" → UPSERT key (checkout_id, item_id, "") collapse mọi
      // model variant của cùng 1 item vào 1 row → data loss commission.
      // Shopee export thường có cột này, nhưng nếu user chọn format khác
      // khi export thì thiếu. Fail sớm tốt hơn silently mất data.
      const hasModelIdCol = headers.some(
        (h) => (h ?? "").trim().toLowerCase() === "id model",
      );
      if (!hasModelIdCol) {
        return rejectedFile(
          file,
          kind,
          `Thiếu cột "ID Model". Export lại từ Shopee với đủ cột, nếu không các item cùng order sẽ bị gộp sai.`,
        );
      }
      const parsedRows = parsed.data
        .map(toShopeeOrderRow)
        .filter((r): r is ShopeeOrderRow => r !== null);
      const rows = dedupShopeeOrders(parsedRows);
      if (rows.length === 0)
        return rejectedFile(file, kind, "Không có đơn hàng hợp lệ trong file");
      return {
        kind,
        file,
        payload: { filename: file.name, rawContent: text, rows },
      };
    }
    case "fb_ad_group": {
      const rows = parsed.data
        .map(toFbAdGroupRow)
        .filter((r): r is FbAdGroupRow => r !== null && isFbValuable(r));
      if (rows.length === 0)
        return rejectedFile(
          file,
          kind,
          "Không có ad group nào chạy (spend 0, clicks 0)",
        );
      return {
        kind,
        file,
        payload: { filename: file.name, rawContent: text, rows, taxRate: 0 },
      };
    }
    case "fb_campaign": {
      const rows = parsed.data
        .map(toFbCampaignRow)
        .filter((r): r is FbCampaignRow => r !== null && isFbValuable(r));
      if (rows.length === 0)
        return rejectedFile(
          file,
          kind,
          "Không có campaign nào chạy (spend 0, clicks 0)",
        );
      return {
        kind,
        file,
        payload: { filename: file.name, rawContent: text, rows, taxRate: 0 },
      };
    }
  }
}

const PREVIEW_CMD: Record<CsvKind, string> = {
  shopee_clicks: "preview_import_shopee_clicks",
  shopee_commission: "preview_import_shopee_orders",
  fb_ad_group: "preview_import_fb_ad_groups",
  fb_campaign: "preview_import_fb_campaigns",
};

const IMPORT_CMD: Record<CsvKind, string> = {
  shopee_clicks: "import_shopee_clicks",
  shopee_commission: "import_shopee_orders",
  fb_ad_group: "import_fb_ad_groups",
  fb_campaign: "import_fb_campaigns",
};

/** Synthesize ImportPreview cho file rejected (không call Rust). Counts = 0,
 *  date trống — dialog check cờ `rejected` để render khác. `kind` dùng làm
 *  type-narrowing key cho UI; nếu detectedKind="unknown" thì fallback
 *  "fb_campaign" (dialog không show kind label cho rejected nên giá trị
 *  này không user-visible). */
function synthRejectedPreview(
  p: Extract<ParsedFile, { kind: "rejected" }>,
): ImportPreview {
  return {
    kind: p.detectedKind === "unknown" ? "fb_campaign" : p.detectedKind,
    filename: p.file.name,
    dayDate: "",
    dayDateFrom: "",
    dayDateTo: "",
    totalRows: 0,
    newRows: 0,
    replaceRows: 0,
    sampleReplace: [],
    dayHasData: false,
    alreadyImported: false,
    hashMatch: false,
    existingDayDate: null,
    skipped: 0,
    emptyRows: 0,
    mostlyEmpty: false,
    batchDuplicate: false,
    rejected: true,
    rejectReason: p.reason,
  };
}

/**
 * Parse + preview tất cả file. Throw Error CHỈ khi không có file (0 input).
 * Lỗi per-file (unknown kind, no valid rows, FB all-zero...) wrap thành
 * ParsedFile kind="rejected" + ImportPreview rejected=true → dialog hiển thị
 * file + reason, không abort cả batch.
 *
 * Multi-day batch OK: mỗi file đã single-day (Rust validate riêng), commit
 * loop per-file transaction. Cross-file date check trước đây đã bị bỏ —
 * invariant duy nhất là 1 CSV = 1 ngày, không phải 1 batch = 1 ngày.
 */
export async function previewCsvBatch(files: File[]): Promise<PreviewBatch> {
  if (files.length === 0) throw new Error("Chưa chọn file nào");

  // Parse tuần tự để tránh stack overflow khi file lớn.
  const parsed: ParsedFile[] = [];
  for (const f of files) {
    parsed.push(await parseFile(f));
  }

  // Detect batch-local duplicate hash TRƯỚC khi preview — nếu user lỡ pick
  // cùng 1 file 2 lần (drag-drop + OS picker, hoặc clone tên khác), commit
  // sẽ fail UNIQUE(file_hash) ở file thứ 2. Mark flag để dialog hiện rõ +
  // commit skip. Rejected files không có rawContent nên skip.
  const hashSeen = new Set<string>();
  const batchDupIndices = new Set<number>();
  for (let i = 0; i < parsed.length; i++) {
    const p = parsed[i];
    if (p.kind === "rejected") continue;
    const hash = await hashSha256(p.payload.rawContent);
    if (hashSeen.has(hash)) {
      batchDupIndices.add(i);
    } else {
      hashSeen.add(hash);
    }
  }

  // Preview song song. Rejected files không call Rust — synthesize local.
  const previews = await Promise.all(
    parsed.map((p) =>
      p.kind === "rejected"
        ? Promise.resolve(synthRejectedPreview(p))
        : invoke<ImportPreview>(PREVIEW_CMD[p.kind], { payload: p.payload }),
    ),
  );

  // Merge batch-local duplicate flag vào previews.
  previews.forEach((p, i) => {
    (p as ImportPreview).batchDuplicate = batchDupIndices.has(i);
  });

  // Representative date cho batch: earliest của day_date_from. Bỏ qua
  // rejected (date trống) — nếu mọi file rejected, representative="".
  const representative =
    previews
      .filter((p) => !p.rejected)
      .map((p) => p.dayDateFrom)
      .sort()[0] ?? "";

  return {
    dayDate: representative,
    files: parsed.map((p, i) => ({ parsed: p, preview: previews[i] })),
  };
}

/**
 * Commit tất cả file trong batch (sau khi user xác nhận preview).
 * Gọi tuần tự để nếu 1 file fail thì dừng (data partial là OK vì
 * mỗi file là 1 transaction riêng).
 *
 * Sort theo dayDate ASC trước khi commit → nếu fail giữa chừng, user biết
 * chắc các ngày trước đã vào DB, các ngày sau chưa. Predictable cho retry.
 *
 * `shopeeAccountId`: TK user chọn trong ImportAccountPickerDialog TRƯỚC khi pick
 * file. Gắn cho mọi Shopee file (clicks + commission) trong batch. FB
 * (ad_group/campaign) không dùng account_id — attribution derive qua JOIN
 * sub_ids + day_date ở query time.
 *
 * `fbTaxRates`: map index file trong `batch.files` → % thuế (0..100). FB file
 * có entry sẽ override `payload.taxRate` (mặc định 0 từ parser). Shopee/rejected
 * key bị bỏ qua. Index dùng làm key vì batch.files giữ nguyên thứ tự pick file
 * suốt vòng đời preview → dialog state đồng bộ với key.
 */
export async function commitCsvBatch(
  batch: PreviewBatch,
  shopeeAccountId: string,
  fbTaxRates: Record<number, number> = {},
): Promise<ImportResult[]> {
  // Lookup tax rate via index in original batch.files BEFORE sort. Sort changes
  // iteration order nhưng index gốc trong batch.files giữ nguyên → map đúng key.
  const indexByItem = new Map(
    batch.files.map((item, idx) => [item, idx] as const),
  );
  const sorted = [...batch.files].sort((a, b) => {
    const dateCmp = a.preview.dayDate.localeCompare(b.preview.dayDate);
    if (dateCmp !== 0) return dateCmp;
    return a.preview.filename.localeCompare(b.preview.filename);
  });
  const results: ImportResult[] = [];
  for (const item of sorted) {
    const { parsed, preview } = item;
    // Skip rejected (parse-stage failure: unknown kind, no valid rows, FB
    // all-zero...) — không có payload để commit.
    if (parsed.kind === "rejected") continue;
    // Skip file duplicate trong CÙNG batch (FE detect) — commit cả 2 sẽ
    // redundant. File hash match với DB vẫn commit: Rust reuse imported_files
    // entry cũ + UPSERT raw rows (idempotent). Cho phép user re-import để
    // refresh data (vd Shopee update trạng thái đơn trong file mới) hoặc
    // backfill rows đã bị xóa.
    if (preview.batchDuplicate) continue;
    const origIdx = indexByItem.get(item) ?? -1;
    let payload: unknown;
    if (parsed.kind === "shopee_clicks" || parsed.kind === "shopee_commission") {
      payload = { ...parsed.payload, shopeeAccountId };
    } else {
      // FB: override taxRate từ user input (dialog state) nếu có; default = 0
      // từ parser. clamp 0..100 để chống invalid (NaN, âm, > 100) — Rust validate
      // lần nữa nhưng FE early-fail tốt hơn là round-trip.
      const userTax = fbTaxRates[origIdx];
      const taxRate =
        typeof userTax === "number" && Number.isFinite(userTax)
          ? Math.min(100, Math.max(0, userTax))
          : parsed.payload.taxRate;
      payload = { ...parsed.payload, taxRate };
    }
    const r = await invoke<ImportResult>(IMPORT_CMD[parsed.kind], { payload });
    results.push(r);
  }
  return results;
}

/// Compute SHA-256 hex của string qua Web Crypto. Dùng detect batch-local dup.
/// Cùng thuật toán với `compute_hash` ở Rust (imports.rs) → cùng kết quả.
async function hashSha256(content: string): Promise<string> {
  const buf = new TextEncoder().encode(content);
  const hash = await crypto.subtle.digest("SHA-256", buf);
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

export function kindLabel(kind: CsvKind): string {
  switch (kind) {
    case "shopee_clicks":
      return "Click Shopee";
    case "shopee_commission":
      return "Hoa hồng Shopee";
    case "fb_ad_group":
      return "FB Ad Group";
    case "fb_campaign":
      return "FB Campaign";
  }
}
