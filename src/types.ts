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
  orderValueTotal: number;
  hasFb: boolean;
  hasShopeeClicks: boolean;
  hasShopeeOrders: boolean;
  hasManual: boolean;
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
  orderValueTotal: number;
};

/** 1 ngày hiển thị, chứa rows (= UiRow[]). */
export type UiDay = {
  date: string;
  notes: string | null;
  rows: UiRow[];
  totals: UiDayTotals;
};

/** Key định danh 1 row manual — dùng khi delete. */
export type ManualRowKey = {
  dayDate: string;
  subIds: SubIds;
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
  /** Account Shopee manual entry thuộc về. FE phải set từ selector. */
  shopeeAccountId: number;
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
