// Parse CSVs trong `docs/` → JSON fixtures dùng cho Rust integration test.
//
// Output 2 file:
//  - fixtures/payloads.json: data đã convert theo Rust payload schema, nhóm
//    theo (kind, day_date). Rust test sẽ INSERT vào in-memory DB y hệt production.
//  - fixtures/csv_totals.json: raw totals tính trực tiếp từ CSV (count rows,
//    SUM(spend), SUM(net_commission), DISTINCT order_id). Dùng làm ground truth
//    so với kết quả sau INSERT + aggregate.
//
// Logic parser lấy từ `src/lib/dbImport.ts` (replicate FE behavior 1:1). Không
// dùng tauri-invoke, chạy trực tiếp Node với papaparse.
//
// Run: `node scripts/make_fixtures.mjs`

import { readFileSync, writeFileSync, readdirSync, mkdirSync } from "node:fs";
import { resolve, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Papa from "papaparse";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");
const DOCS = join(ROOT, "docs");
const OUT = join(ROOT, "fixtures");
mkdirSync(OUT, { recursive: true });

// ============================================================
// Row converters — port từ src/lib/dbImport.ts (matching FE semantics).
// ============================================================

function parseSubIdString(raw) {
  const parts = raw.split("-").map((p) => p.trim());
  const out = ["", "", "", "", ""];
  for (let i = 0; i < 5 && i < parts.length; i++) out[i] = parts[i];
  if (parts.length > 5) out[4] = parts.slice(4).join("-");
  return out;
}

function parseNumOrNull(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (!s) return null;
  const cleaned = s.replace(/%$/, "").replace(/,/g, "");
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function strOrNull(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s || null;
}

function extractDate(s) {
  // "YYYY-MM-DD HH:MM:SS" hoặc "YYYY-MM-DDTHH:MM:SS" → "YYYY-MM-DD".
  if (!s) return null;
  const m = /^(\d{4}-\d{2}-\d{2})/.exec(s);
  return m ? m[1] : null;
}

function detectKind(headers) {
  const h = headers.map((x) => (x ?? "").toLowerCase().trim());
  const has = (needle) => h.includes(needle.toLowerCase());
  if (has("tên nhóm quảng cáo")) return "fb_ad_group";
  if (has("tên chiến dịch")) return "fb_campaign";
  if (has("id đơn hàng") && h.some((x) => x.startsWith("sub_id2")))
    return "shopee_commission";
  if (has("click id") && has("sub_id")) return "shopee_clicks";
  return "unknown";
}

function toShopeeClickRow(r) {
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

function toShopeeOrderRow(r) {
  const orderId = (r["ID đơn hàng"] ?? "").trim();
  const checkoutId = (r["Checkout id"] ?? "").trim();
  const itemId = (r["Item id"] ?? "").trim();
  const orderTime = (r["Thời Gian Đặt Hàng"] ?? "").trim();
  if (!orderId || !checkoutId || !itemId || !orderTime) return null;
  const subIds = [
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
    rawJson: null,
  };
}

function toFbAdGroupRow(r) {
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
    rawJson: null,
  };
}

function toFbCampaignRow(r) {
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
    rawJson: null,
  };
}

// ============================================================
// Main
// ============================================================

const files = readdirSync(DOCS).filter((f) => f.toLowerCase().endsWith(".csv"));

// Grouped by (kind, day_date). Key = `${kind}|${day_date}`.
const groups = {};
// Totals direct từ CSV, dùng làm ground truth.
const csvTotals = {};

function ensureGroup(kind, day) {
  const key = `${kind}|${day}`;
  if (!groups[key]) groups[key] = { kind, dayDate: day, filename: "", rows: [] };
  if (!csvTotals[key]) {
    csvTotals[key] = {
      kind,
      dayDate: day,
      filenames: [],
      rowCount: 0,
      // Shopee orders — sum ở integer cents để tránh float drift
      // (IEEE 754 non-associative → JS/SQL sum order khác → cent-level khớp).
      distinctOrderIds: new Set(),
      distinctCheckoutItem: new Set(),
      sumNetCommissionCents: 0n,
      sumOrderValueCents: 0n,
      // Shopee clicks — int counts.
      sumClicks: 0,
      byReferrer: {},
      // FB ads — spend cũng integer cents, impressions/clicks int.
      sumSpendCents: 0n,
      sumImpressions: 0,
      sumLinkClicks: 0,
      sumAllClicks: 0,
    };
  }
  return { key, group: groups[key], totals: csvTotals[key] };
}

// VND × 100 → BigInt cents. Round half-away-from-zero qua Math.round
// (match Rust `(x * 100.0).round() as i64`). Null → 0n.
function toCents(v) {
  if (v === null || v === undefined) return 0n;
  return BigInt(Math.round(v * 100));
}

// Dedup maps: key = unique identity theo schema production.
//  - Shopee clicks: click_id (INSERT OR IGNORE → first wins).
//  - Shopee orders: (checkout_id, item_id, model_id) (UPSERT → last wins).
//  - FB ads: (day_date, level, name) (UPSERT → last wins).
// Dedup sao cho CSV ground truth = state DB cuối cùng, không phải raw CSV sum.
const dedupClicks = new Map(); // clickId → row
const dedupOrders = new Map(); // `${day}|${checkoutId}|${itemId}|${modelId}` → row
const dedupFb = new Map(); // `${day}|${level}|${name}` → row

for (const filename of files) {
  const path = join(DOCS, filename);
  const raw = readFileSync(path, "utf8");
  const parsed = Papa.parse(raw, {
    header: true,
    skipEmptyLines: true,
  });
  if (!parsed.meta?.fields) {
    console.warn(`[skip] ${filename}: no headers`);
    continue;
  }
  const kind = detectKind(parsed.meta.fields);
  if (kind === "unknown") {
    console.warn(`[skip] ${filename}: unknown kind`);
    continue;
  }

  for (const row of parsed.data) {
    let converted = null;
    let dayDate = null;
    if (kind === "shopee_clicks") {
      converted = toShopeeClickRow(row);
      if (converted) dayDate = extractDate(converted.clickTime);
    } else if (kind === "shopee_commission") {
      converted = toShopeeOrderRow(row);
      if (converted) dayDate = extractDate(converted.orderTime);
    } else if (kind === "fb_ad_group") {
      converted = toFbAdGroupRow(row);
      if (converted) dayDate = extractDate(converted.reportStart);
    } else if (kind === "fb_campaign") {
      converted = toFbCampaignRow(row);
      if (converted) dayDate = extractDate(converted.reportStart);
    }
    if (!converted || !dayDate) continue;

    // Dedup theo identity production dùng.
    if (kind === "shopee_clicks") {
      const key = converted.clickId;
      if (dedupClicks.has(key)) continue; // first wins
      dedupClicks.set(key, { converted, dayDate, filename });
    } else if (kind === "shopee_commission") {
      const key = `${dayDate}|${converted.checkoutId}|${converted.itemId}|${converted.modelId}`;
      dedupOrders.set(key, { converted, dayDate, filename }); // last wins
    } else {
      const name = converted.adGroupName ?? converted.campaignName;
      const level = kind === "fb_ad_group" ? "ad_group" : "campaign";
      const key = `${dayDate}|${level}|${name}`;
      dedupFb.set(key, { converted, dayDate, filename }); // last wins
    }
  }
}

// Aggregate dedup'd entries vào groups + totals.
function absorb(kind, entries) {
  for (const { converted, dayDate, filename } of entries) {
    const { group, totals } = ensureGroup(kind, dayDate);
    if (!totals.filenames.includes(filename)) totals.filenames.push(filename);
    group.filename = filename;
    group.rows.push(converted);
    totals.rowCount += 1;

    if (kind === "shopee_commission") {
      totals.distinctOrderIds.add(converted.orderId);
      totals.distinctCheckoutItem.add(
        `${converted.checkoutId}|${converted.itemId}|${converted.modelId}`,
      );
      totals.sumNetCommissionCents += toCents(converted.netCommission);
      totals.sumOrderValueCents += toCents(converted.orderValue);
    } else if (kind === "shopee_clicks") {
      totals.sumClicks += 1;
      const ref = converted.referrer ?? "(khác)";
      totals.byReferrer[ref] = (totals.byReferrer[ref] ?? 0) + 1;
    } else if (kind === "fb_ad_group" || kind === "fb_campaign") {
      totals.sumSpendCents += toCents(converted.spend);
      totals.sumImpressions += converted.impressions ?? 0;
      totals.sumLinkClicks += converted.linkClicks ?? 0;
      totals.sumAllClicks += converted.allClicks ?? 0;
    }
  }
}

absorb("shopee_clicks", dedupClicks.values());
absorb("shopee_commission", dedupOrders.values());
for (const { converted, dayDate, filename } of dedupFb.values()) {
  const kind = converted.adGroupName ? "fb_ad_group" : "fb_campaign";
  absorb(kind, [{ converted, dayDate, filename }]);
}

// Convert Sets → counts, BigInt → string cho JSON serialize.
for (const k of Object.keys(csvTotals)) {
  const t = csvTotals[k];
  t.distinctOrderIdCount = t.distinctOrderIds.size;
  t.distinctCheckoutItemCount = t.distinctCheckoutItem.size;
  delete t.distinctOrderIds;
  delete t.distinctCheckoutItem;
  // BigInt không serialize native — chuyển sang string để Rust parse i64.
  t.sumNetCommissionCents = t.sumNetCommissionCents.toString();
  t.sumOrderValueCents = t.sumOrderValueCents.toString();
  t.sumSpendCents = t.sumSpendCents.toString();
}

writeFileSync(
  join(OUT, "payloads.json"),
  JSON.stringify(Object.values(groups), null, 2),
);
writeFileSync(
  join(OUT, "csv_totals.json"),
  JSON.stringify(Object.values(csvTotals), null, 2),
);

console.log(`\n✓ Wrote ${Object.keys(groups).length} groups:`);
for (const t of Object.values(csvTotals)) {
  const extra =
    t.kind === "shopee_commission"
      ? `distinctOrders=${t.distinctOrderIdCount} commissionCents=${t.sumNetCommissionCents}`
      : t.kind === "shopee_clicks"
        ? `sumClicks=${t.sumClicks} referrers=${Object.keys(t.byReferrer).length}`
        : `spendCents=${t.sumSpendCents} sumLinkClicks=${t.sumLinkClicks}`;
  console.log(
    `  ${t.kind.padEnd(18)} ${t.dayDate}  rows=${String(t.rowCount).padStart(4)}  ${extra}`,
  );
}
console.log(`\nFixtures: ${OUT}`);
