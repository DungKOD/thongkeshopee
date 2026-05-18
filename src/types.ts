// =========================================================
// DB-backed types (khớp với Rust DTO camelCase)
// =========================================================

export type SubIds = [string, string, string, string, string];

/** 1 "dòng UI" aggregate từ raw tables + manual_entries. */
export type UiRow = {
  dayDate: string;
  subIds: SubIds;
  displayName: string;
  adsClicks: number | null;
  totalSpend: number | null;
  cpc: number | null;
  impressions: number | null;
  shopeeClicksByReferrer: Record<string, number>;
  shopeeClicksTotal: number;
  ordersCount: number;
  commissionTotal: number;
  /** Subset commissionTotal từ đơn rủi ro huỷ: "Đang chờ xử lý" + "Chưa
   *  thanh toán". FE dùng trừ `commissionPending × (returnReserveRate / 100)`
   *  khỏi net commission. Xem `computeNetCommission`. */
  commissionPending: number;
  orderValueTotal: number;
  hasFb: boolean;
  hasShopeeClicks: boolean;
  hasShopeeOrders: boolean;
  hasManual: boolean;
  /** Account id của manual entry (nếu row có hasManual). Null = không có
   *  manual → edit dialog dùng activeAccountId. String vì content_id hash
   *  có thể > 2^53. */
  shopeeAccountId: string | null;
  /** Account id mà row thuộc về (sau khi tách per-account aggregate khi
   *  filter=All). Null = "FB chung" — FB ad attribute cho ≥2 Shopee owner
   *  cùng ngày, không gắn được duy nhất 1 acc. */
  accountId: string | null;
  /** Tên account hiển thị UI; null cùng `accountId` (FB chung). */
  accountName: string | null;
  /** Cây 3 cấp campaign → nhóm → quảng cáo, build từ `raw_fb_ads_hierarchy`.
   *  `null`/`undefined` = tuple chỉ có data từ format cũ (raw_fb_ads), UI render
   *  như trước. Khi có → hiển thị nút expand inline để drill-down. */
  fbBreakdown?: FbBreakdown | null;
};

/** Cây hierarchy FB ads cho 1 UiRow (xem `UiRow.fbBreakdown`). */
export type FbBreakdown = {
  campaigns: FbCampaignGroup[];
  /** Tổng spend toàn cây (= sum mọi ad-leaf). UI dùng kiểm tra khớp với
   *  `UiRow.totalSpend`. Khác chút khi row có cả manual override. */
  totalSpend: number;
};

export type FbCampaignGroup = {
  campaignName: string;
  spend: number;
  clicks: number | null;
  cpc: number | null;
  adSets: FbAdSetGroup[];
};

export type FbAdSetGroup = {
  adSetName: string;
  spend: number;
  clicks: number | null;
  cpc: number | null;
  ads: FbAdLeaf[];
};

export type FbAdLeaf = {
  adName: string;
  /** 0..N — phân biệt nhiều ad cùng tên trong cùng adset. */
  occurrenceIdx: number;
  spend: number;
  clicks: number | null;
  cpc: number | null;
};

/** Day-level totals — tính từ MỌI tuple trước row-0 filter. KPI dùng field
 *  này để đúng 100% với raw data kể cả khi tuple chỉ có click (không spend/
 *  commission) bị filter khỏi row display. */
export type UiDayTotals = {
  adsClicks: number;
  totalSpend: number;
  impressions: number;
  shopeeClicksByReferrer: Record<string, number>;
  shopeeClicksTotal: number;
  ordersCount: number;
  commissionTotal: number;
  /** Subset commission từ đơn rủi ro huỷ: "Đang chờ xử lý" + "Chưa thanh toán". */
  commissionPending: number;
  orderValueTotal: number;
  /** Tổng phí quản lý MCN Shopee đã cắt trước payout (đã trừ sẵn trong
   *  commissionTotal — KHÔNG trừ lần nữa, chỉ hiển thị minh bạch). */
  mcnFeeTotal: number;
};

/** 1 ngày hiển thị, chứa rows (= UiRow[]). */
export type UiDay = {
  date: string;
  notes: string | null;
  rows: UiRow[];
  totals: UiDayTotals;
};

/** Key định danh 1 row UI bị stage delete.
 *  `accountId`: id account row thuộc về (sau v0.4.5+ split per-account).
 *  Khi `null` = "FB chung" row (FB ad ≥2 owner) → BE wipe cross-account.
 *  Khi `string` = scope DELETE theo `shopee_account_id = id` ở Shopee tables
 *  + `manual_entries` để không wipe data account khác trên cùng tuple+ngày. */
export type ManualRowKey = {
  dayDate: string;
  subIds: SubIds;
  accountId: string | null;
};

/** Payload INSERT/UPDATE manual entry. */
export type ManualEntryInput = {
  dayDate: string;
  subIds: SubIds;
  displayName: string | null;
  overrideClicks: number | null;
  overrideSpend: number | null;
  overrideCpc: number | null;
  overrideOrders: number | null;
  overrideCommission: number | null;
  /** Account Shopee manual entry thuộc về. FE phải set từ selector.
   *  String vì content_id hash > 2^53 (JS Number precision loss). */
  shopeeAccountId: string;
};

/** Derived values tính từ 1 UiRow. */
export type VideoComputed = {
  cpc: number;
  conversionRate: number;
  orderValue: number;
  netCommission: number;
  profit: number;
  profitMargin: number;
};

export type DayTotals = {
  clicks: number;
  shopeeClicks: number;
  totalSpend: number;
  orders: number;
  commission: number;
  profit: number;
};

/** Chi tiết 1 item trong order từ raw_shopee_order_items — dùng cho drill-down. */
export type OrderItemDetail = {
  orderId: string;
  checkoutId: string;
  itemId: string;
  modelId: string;
  itemName: string | null;
  shopName: string | null;
  orderStatus: string | null;
  orderTime: string | null;
  clickTime: string | null;
  completedTime: string | null;
  price: number | null;
  quantity: number | null;
  orderValue: number | null;
  netCommission: number | null;
  commissionTotal: number | null;
  channel: string | null;
  subIds: SubIds;
};
