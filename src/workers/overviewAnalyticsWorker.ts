/**
 * Web Worker: compute toàn bộ analytics cho OverviewTab off-main-thread.
 *
 * Main thread gửi `{ requestId, days, clickSources, profitFees, source }`,
 * worker chạy 11 compute function trong `formulas.ts` rồi gửi lại
 * `{ requestId, analytics }`. Main thread match `requestId` để bỏ kết quả
 * stale khi user đổi filter liên tục (race resolution).
 *
 * Cost so với main thread: serialize+deserialize qua `postMessage`
 * (structuredClone). UiDay[] với 200 ngày × 30 row ≈ vài chục KB — clone
 * <5ms. Compute thực tế tốn 50-200ms, ưu thế parallel rõ ràng.
 */

import {
  aggregateProductRows,
  computeAdsEfficiency,
  computeBreakeven,
  computeCumulativeTrend,
  computeDailyTrend,
  computeExtremumDays,
  computeFunnel,
  computeLosers,
  computeMinSpendThreshold,
  computeOverviewTotals,
  computeWinners,
  type SourceFilter,
} from "../formulas";
import type { ProfitFees } from "../lib/profitFees";
import type { UiDay } from "../types";

export interface AnalyticsRequest {
  requestId: number;
  days: UiDay[];
  clickSources: Record<string, boolean>;
  profitFees: ProfitFees;
  source: SourceFilter;
}

export interface AnalyticsResponse {
  requestId: number;
  /** Snake-case-y nhưng giữ y nguyên field name từ `formulas.ts` để
   *  OverviewTab destructure không phải đổi. */
  analytics: ReturnType<typeof computeAnalytics>;
}

export function computeAnalytics(req: AnalyticsRequest) {
  const { days, clickSources, profitFees, source } = req;
  const totals = computeOverviewTotals(days, clickSources, profitFees, source);
  const products = aggregateProductRows(days, clickSources, profitFees, source);
  products.sort((a, b) => b.profit - a.profit);

  const trendData = computeDailyTrend(days, clickSources, profitFees, source);
  const cumulativeData = computeCumulativeTrend(trendData);
  const breakeven = computeBreakeven(totals, profitFees);
  const funnel = computeFunnel(totals);
  const efficiency = computeAdsEfficiency(totals);
  const { best: bestDay, worst: worstDay } = computeExtremumDays(trendData);
  const minSpend = computeMinSpendThreshold(products);
  const winners = computeWinners(products, minSpend, 5);
  const losers = computeLosers(products, minSpend, 5);

  return {
    totals,
    products,
    trendData,
    cumulativeData,
    breakeven,
    funnel,
    efficiency,
    bestDay,
    worstDay,
    winners,
    losers,
  };
}

self.onmessage = (e: MessageEvent<AnalyticsRequest>) => {
  const req = e.data;
  const analytics = computeAnalytics(req);
  const response: AnalyticsResponse = { requestId: req.requestId, analytics };
  (self as unknown as Worker).postMessage(response);
};
