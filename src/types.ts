export type OrderDetail = {
  id: string;
  status: string;
  grossValue: number;
  commission: number;
  /** Thời gian user click vào link affiliate (ISO string). */
  clickTime?: string;
  /** Thời gian đặt đơn hàng (ISO string). */
  orderTime?: string;
};

export type Video = {
  id: string;
  name: string;
  clicks: number;
  shopeeClicksByReferrer: Record<string, number>;
  totalSpend: number;
  orders: number;
  commission: number;
  /** CPC đọc từ file FB (nếu có). Ưu tiên hơn giá trị tính từ spend/clicks. */
  cpc?: number;
  orderDetails?: OrderDetail[];
};

export type Day = {
  id: string;
  date: string;
  videos: Video[];
};

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
  commission: number;
  profit: number;
};
