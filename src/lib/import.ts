import Papa from "papaparse";
import type { VideoInput } from "../hooks/useStats";
import type { OrderDetail } from "../types";

export type FileKind =
  | "fb_ad_group"
  | "fb_campaign"
  | "commission"
  | "click"
  | "unknown";

export interface ParsedFile {
  name: string;
  kind: FileKind;
  rows: Record<string, string>[];
}

export interface ImportResult {
  date: string | null;
  products: VideoInput[];
  files: { name: string; kind: FileKind; rowCount: number }[];
  discoveredClickSources: Record<string, number>;
  warnings: string[];
}


interface Aggregated {
  name: string;
  clicks: number;
  shopeeClicksByReferrer: Record<string, number>;
  totalSpend: number;
  orders: number;
  commission: number;
  orderDetails: Map<string, OrderDetail>;
  /** Lưu từng (clicks, cpc) của mỗi row FB để tính weighted-average. */
  fbCpcSamples: Array<{ clicks: number; cpc: number }>;
}

/**
 * Trích xuất core product name từ chuỗi campaign/sub_id:
 * - Bỏ suffix 4 chữ số (MMDD/DDMM) như `-0411`
 * - Bỏ dấu `-` ở cuối (Shopee Sub_id thường có `----`)
 * - Lấy segment cuối sau dấu `-` (convention `<shop>-<product>[-MMDD]`)
 *
 * Ví dụ:
 *   `MuseStudio-chanvayrendai-0411` → `chanvayrendai`
 *   `MuseStudio-chanvayrendai---` → `chanvayrendai`
 *   `chanvayrendai` → `chanvayrendai`
 *
 * Giả định: product slug không chứa dấu `-` (thường viết liền hoặc camelCase).
 */
export function normalizeName(raw: string): string {
  if (!raw) return "";
  let s = raw.trim();
  s = s.replace(/-\d{4}$/, "");
  s = s.replace(/-+$/, "");
  const parts = s.split("-");
  return parts[parts.length - 1];
}

function parseNum(v: unknown): number {
  if (v === null || v === undefined) return 0;
  const s = String(v).trim();
  if (!s) return 0;
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function detectKind(headers: string[]): FileKind {
  const h = headers.map((x) => (x ?? "").toLowerCase().trim());
  const has = (needle: string) => h.some((x) => x === needle.toLowerCase());
  if (has("tên nhóm quảng cáo")) return "fb_ad_group";
  if (has("tên chiến dịch")) return "fb_campaign";
  if (has("id đơn hàng") && h.some((x) => x.startsWith("sub_id2")))
    return "commission";
  if (has("click id") && h.some((x) => x === "sub_id")) return "click";
  return "unknown";
}

export async function parseCsvFile(file: File): Promise<ParsedFile> {
  const text = await file.text();
  const result = Papa.parse<Record<string, string>>(text, {
    header: true,
    skipEmptyLines: true,
  });
  const headers = result.meta.fields ?? [];
  const kind = detectKind(headers);
  return { name: file.name, kind, rows: result.data };
}

function extractDate(row: Record<string, string>): string | null {
  const d = row["Lượt bắt đầu báo cáo"];
  if (d && /^\d{4}-\d{2}-\d{2}$/.test(d.trim())) return d.trim();
  const dt = row["Thời Gian Đặt Hàng"] || row["Thời gian Click"];
  if (dt) {
    const m = dt.match(/^(\d{4}-\d{2}-\d{2})/);
    if (m) return m[1];
  }
  return null;
}

export function aggregate(files: ParsedFile[]): ImportResult {
  const byName = new Map<string, Aggregated>();
  const warnings: string[] = [];
  const discoveredClickSources: Record<string, number> = {};
  let detectedDate: string | null = null;

  const getOrCreate = (name: string): Aggregated => {
    let p = byName.get(name);
    if (!p) {
      p = {
        name,
        clicks: 0,
        shopeeClicksByReferrer: {},
        totalSpend: 0,
        orders: 0,
        commission: 0,
        orderDetails: new Map(),
        fbCpcSamples: [],
      };
      byName.set(name, p);
    }
    return p;
  };

  for (const f of files) {
    if (f.kind === "unknown") {
      warnings.push(`Không nhận diện được loại file: ${f.name}`);
      continue;
    }

    if (f.kind === "fb_ad_group") {
      for (const row of f.rows) {
        if (!detectedDate) detectedDate = extractDate(row);
        const name = normalizeName(row["Tên nhóm quảng cáo"] ?? "");
        if (!name) continue;
        const clicks = parseNum(row["Lượt click vào liên kết"]);
        const spend = parseNum(row["Số tiền đã chi tiêu (VND)"]);
        const cpc = parseNum(
          row["CPC (chi phí trên mỗi lượt click vào liên kết) (VND)"],
        );
        if (clicks === 0 && spend === 0) continue;
        const p = getOrCreate(name);
        p.clicks += clicks;
        p.totalSpend += spend;
        if (cpc > 0 && clicks > 0) p.fbCpcSamples.push({ clicks, cpc });
      }
    } else if (f.kind === "fb_campaign") {
      for (const row of f.rows) {
        if (!detectedDate) detectedDate = extractDate(row);
        const name = normalizeName(row["Tên chiến dịch"] ?? "");
        if (!name) continue;
        const clicks = parseNum(row["Kết quả"]);
        const spend = parseNum(row["Số tiền đã chi tiêu (VND)"]);
        const cpc = parseNum(row["Chi phí trên mỗi kết quả"]);
        if (clicks === 0 && spend === 0) continue;
        const p = getOrCreate(name);
        p.clicks += clicks;
        p.totalSpend += spend;
        if (cpc > 0 && clicks > 0) p.fbCpcSamples.push({ clicks, cpc });
      }
    } else if (f.kind === "commission") {
      for (const row of f.rows) {
        if (!detectedDate) detectedDate = extractDate(row);
        const rawSubId =
          (row["Sub_id2"] ?? "").trim() || (row["Sub_id1"] ?? "").trim();
        const name = normalizeName(rawSubId);
        if (!name) continue;
        const orderId = (row["ID đơn hàng"] ?? "").trim();
        if (!orderId) continue;
        const commission = parseNum(row["Tổng hoa hồng đơn hàng(₫)"]);
        const grossValue = parseNum(row["Giá trị đơn hàng (₫)"]);
        const status = (row["Trạng thái đặt hàng"] ?? "").trim();
        const clickTime = (row["Thời gian Click"] ?? "").trim() || undefined;
        const orderTime =
          (row["Thời Gian Đặt Hàng"] ?? "").trim() || undefined;
        const p = getOrCreate(name);
        let detail = p.orderDetails.get(orderId);
        if (!detail) {
          detail = {
            id: orderId,
            status,
            grossValue: 0,
            commission: 0,
            clickTime,
            orderTime,
          };
          p.orderDetails.set(orderId, detail);
        }
        // Giá trị đơn hàng nằm từng dòng item → cộng dồn
        detail.grossValue += grossValue;
        // Tổng hoa hồng đơn hàng chỉ ghi ở row đầu của đơn → cộng an toàn
        detail.commission += commission;
      }
    } else if (f.kind === "click") {
      for (const row of f.rows) {
        if (!detectedDate) detectedDate = extractDate(row);
        const name = normalizeName(row["Sub_id"] ?? "");
        if (!name) continue;
        const referrer = (row["Người giới thiệu"] ?? "").trim() || "(khác)";
        discoveredClickSources[referrer] =
          (discoveredClickSources[referrer] ?? 0) + 1;
        const p = getOrCreate(name);
        p.shopeeClicksByReferrer[referrer] =
          (p.shopeeClicksByReferrer[referrer] ?? 0) + 1;
      }
    }
  }

  // Compute orders + commission từ orderDetails (nếu có) để đồng nhất.
  for (const p of byName.values()) {
    if (p.orderDetails.size > 0) {
      p.orders = p.orderDetails.size;
      p.commission = Array.from(p.orderDetails.values()).reduce(
        (a, d) => a + d.commission,
        0,
      );
    }
  }

  // Weighted-average CPC từ FB samples (đọc từ file, không tính từ spend/clicks).
  const cpcByName = new Map<string, number>();
  for (const p of byName.values()) {
    if (p.fbCpcSamples.length === 0) continue;
    const totalClicks = p.fbCpcSamples.reduce((a, s) => a + s.clicks, 0);
    if (totalClicks === 0) continue;
    const weightedSum = p.fbCpcSamples.reduce(
      (a, s) => a + s.clicks * s.cpc,
      0,
    );
    cpcByName.set(p.name, weightedSum / totalClicks);
  }

  const products: VideoInput[] = Array.from(byName.values())
    .filter((p) => p.totalSpend > 0 || p.commission > 0)
    .sort((a, b) => a.name.localeCompare(b.name))
    .map((p) => ({
      name: p.name,
      clicks: p.clicks,
      shopeeClicksByReferrer: p.shopeeClicksByReferrer,
      totalSpend: p.totalSpend,
      orders: p.orders,
      commission: p.commission,
      cpc: cpcByName.get(p.name),
      orderDetails:
        p.orderDetails.size > 0
          ? Array.from(p.orderDetails.values())
          : undefined,
    }));

  return {
    date: detectedDate,
    products,
    files: files.map((f) => ({
      name: f.name,
      kind: f.kind,
      rowCount: f.rows.length,
    })),
    discoveredClickSources,
    warnings,
  };
}
