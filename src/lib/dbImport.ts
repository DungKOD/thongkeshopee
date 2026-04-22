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
  subIds: SubIds;
  channel: string | null;
  rawJson: string | null;
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
  rawJson: string | null;
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
  rawJson: string | null;
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

/**
 * Lọc các field đã dùng → stringify phần còn lại vào `raw_json`.
 * Cho phép lưu toàn bộ CSV row để tra cứu sau mà không cần schema hóa.
 */
function buildRawJson(
  row: Record<string, string>,
  usedKeys: ReadonlySet<string>,
): string | null {
  const extras: Record<string, string> = {};
  let count = 0;
  for (const [k, v] of Object.entries(row)) {
    if (usedKeys.has(k)) continue;
    if (v === null || v === undefined || v === "") continue;
    extras[k] = v;
    count += 1;
  }
  return count === 0 ? null : JSON.stringify(extras);
}

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

const SHOPEE_ORDER_USED = new Set([
  "ID đơn hàng",
  "Trạng thái đặt hàng",
  "Checkout id",
  "Thời Gian Đặt Hàng",
  "Thời gian hoàn thành",
  "Thời gian Click",
  "Tên Shop",
  "Shop id",
  "Loại Shop",
  "Item id",
  "Tên Item",
  "ID Model",
  "L1 Danh mục toàn cầu",
  "L2 Danh mục toàn cầu",
  "L3 Danh mục toàn cầu",
  "Giá(₫)",
  "Số lượng",
  "Giá trị đơn hàng (₫)",
  "Số tiền hoàn trả (₫)",
  "Tổng hoa hồng sản phẩm(₫)",
  "Hoa hồng ròng tiếp thị liên kết(₫)",
  "Sub_id1",
  "Sub_id2",
  "Sub_id3",
  "Sub_id4",
  "Sub_id5",
  "Kênh",
]);

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
    subIds,
    channel: strOrNull(r["Kênh"]),
    rawJson: buildRawJson(r, SHOPEE_ORDER_USED),
  };
}

const FB_AD_GROUP_USED = new Set([
  "Lượt bắt đầu báo cáo",
  "Lượt kết thúc báo cáo",
  "Tên nhóm quảng cáo",
  "Phân phối nhóm quảng cáo",
  "Số tiền đã chi tiêu (VND)",
  "Lượt hiển thị",
  "Người tiếp cận",
  "Tần suất",
  "Lượt click vào liên kết",
  "shop_clicks",
  "Lượt click (tất cả)",
  "CPC (chi phí trên mỗi lượt click vào liên kết) (VND)",
  "CPC (tất cả) (VND)",
  "CTR (tỷ lệ click vào liên kết)",
  "CTR (Tất cả)",
  "Lượt xem trang đích",
  "CPM (Chi phí trên mỗi 1.000 lượt hiển thị) (VND)",
  "Kết quả",
  "Chi phí trên mỗi kết quả",
]);

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
    rawJson: buildRawJson(r, FB_AD_GROUP_USED),
  };
}

const FB_CAMPAIGN_USED = new Set([
  "Lượt bắt đầu báo cáo",
  "Lượt kết thúc báo cáo",
  "Tên chiến dịch",
  "Lượt phân phối chiến dịch",
  "Kết quả",
  "Chỉ báo kết quả",
  "Chi phí trên mỗi kết quả",
  "Số tiền đã chi tiêu (VND)",
  "Lượt hiển thị",
  "Người tiếp cận",
  "Lượt click vào liên kết",
  "Lượt click (tất cả)",
  "CPC (chi phí trên mỗi lượt click vào liên kết) (VND)",
  "CPC (tất cả) (VND)",
]);

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
    rawJson: buildRawJson(r, FB_CAMPAIGN_USED),
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
  /** File đã import (hash match) — FE highlight + skip khỏi commit. */
  alreadyImported: boolean;
  /** Nếu alreadyImported=true: day_date của lần import trước. */
  existingDayDate: string | null;
  /** Rows không parse được date (Shopee multi-day only). */
  skipped: number;
}

/** Parsed + typed payload của 1 file, giữ trong RAM để commit sau preview. */
export type ParsedFile =
  | { kind: "shopee_clicks"; file: File; payload: { filename: string; rawContent: string; rows: ShopeeClickRow[] } }
  | { kind: "shopee_commission"; file: File; payload: { filename: string; rawContent: string; rows: ShopeeOrderRow[] } }
  | { kind: "fb_ad_group"; file: File; payload: { filename: string; rawContent: string; rows: FbAdGroupRow[] } }
  | { kind: "fb_campaign"; file: File; payload: { filename: string; rawContent: string; rows: FbCampaignRow[] } };

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

async function parseFile(file: File): Promise<ParsedFile> {
  const text = await file.text();
  const parsed = Papa.parse<Record<string, string>>(text, {
    header: true,
    skipEmptyLines: true,
  });
  const headers = parsed.meta.fields ?? [];
  const kind = detectKind(headers);

  if (kind === "unknown") {
    throw new Error(
      `Không nhận diện được loại file '${file.name}'. File phải là WebsiteClickReport, AffiliateCommissionReport, FB Ad Group hoặc FB Campaign.`,
    );
  }

  switch (kind) {
    case "shopee_clicks": {
      const rows = parsed.data
        .map(toShopeeClickRow)
        .filter((r): r is ShopeeClickRow => r !== null);
      if (rows.length === 0)
        throw new Error(`File '${file.name}' không có click hợp lệ`);
      return {
        kind,
        file,
        payload: { filename: file.name, rawContent: text, rows },
      };
    }
    case "shopee_commission": {
      const parsedRows = parsed.data
        .map(toShopeeOrderRow)
        .filter((r): r is ShopeeOrderRow => r !== null);
      const rows = dedupShopeeOrders(parsedRows);
      if (rows.length === 0)
        throw new Error(`File '${file.name}' không có đơn hàng hợp lệ`);
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
        throw new Error(
          `File '${file.name}' không có ad group nào chạy (spend 0, clicks 0)`,
        );
      return {
        kind,
        file,
        payload: { filename: file.name, rawContent: text, rows },
      };
    }
    case "fb_campaign": {
      const rows = parsed.data
        .map(toFbCampaignRow)
        .filter((r): r is FbCampaignRow => r !== null && isFbValuable(r));
      if (rows.length === 0)
        throw new Error(
          `File '${file.name}' không có campaign nào chạy (spend 0, clicks 0)`,
        );
      return {
        kind,
        file,
        payload: { filename: file.name, rawContent: text, rows },
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

/**
 * Parse + preview tất cả file, validate ngày đồng nhất giữa các file.
 * Throw Error nếu:
 * - File không nhận diện được kind
 * - File có >1 ngày (lỗi từ Rust)
 * - Các file cùng batch khác ngày nhau
 * - File đã import trước đó (hash trùng)
 */
export async function previewCsvBatch(files: File[]): Promise<PreviewBatch> {
  if (files.length === 0) throw new Error("Chưa chọn file nào");

  // Parse tuần tự để tránh stack overflow khi file lớn.
  const parsed: ParsedFile[] = [];
  for (const f of files) {
    parsed.push(await parseFile(f));
  }

  // Preview song song (mỗi file gọi 1 command khác nhau).
  const previews = await Promise.all(
    parsed.map((p) =>
      invoke<ImportPreview>(PREVIEW_CMD[p.kind], { payload: p.payload }),
    ),
  );

  // Validate cross-date CHỈ cho FB (single-date). Shopee multi-day OK.
  // Mỗi FB file phải share cùng ngày với các FB file khác nếu có nhiều; và
  // cùng ngày với Shopee files. Thực tế 1 batch thường 1 ngày FB → check nhẹ.
  const fbFiles = previews.filter(
    (p) => p.kind === "fb_ad_group" || p.kind === "fb_campaign",
  );
  if (fbFiles.length > 0) {
    const fbDates = Array.from(new Set(fbFiles.map((p) => p.dayDate))).sort();
    if (fbDates.length > 1) {
      const summary = fbFiles
        .map((p) => `  • ${p.filename}: ${p.dayDate} (${kindLabel(p.kind)})`)
        .join("\n");
      throw new Error(
        `File FB phải cùng 1 ngày:\n${summary}\n\nImport từng ngày FB riêng lẻ.`,
      );
    }
  }

  // Representative date cho batch: earliest của day_date_from của mọi file.
  const representative = previews
    .map((p) => p.dayDateFrom)
    .sort()[0];

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
 * `shopeeAccountId`: TK user chọn trong ImportAccountPickerDialog TRƯỚC khi pick
 * file. Gắn cho mọi Shopee file (clicks + commission) trong batch. FB
 * (ad_group/campaign) không dùng account_id — attribution derive qua JOIN
 * sub_ids + day_date ở query time.
 */
export async function commitCsvBatch(
  batch: PreviewBatch,
  shopeeAccountId: number,
): Promise<ImportResult[]> {
  const results: ImportResult[] = [];
  for (const { parsed, preview } of batch.files) {
    // Skip file đã import trước (hash match). FE dialog đã báo user rồi.
    if (preview.alreadyImported) continue;
    const payload =
      parsed.kind === "shopee_clicks" || parsed.kind === "shopee_commission"
        ? { ...parsed.payload, shopeeAccountId }
        : parsed.payload;
    const r = await invoke<ImportResult>(IMPORT_CMD[parsed.kind], { payload });
    results.push(r);
  }
  return results;
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
