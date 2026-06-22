import { useEffect, useMemo, useRef, useState } from "react";
import type { SourceFilter } from "../formulas";
import type { ProfitFees } from "../lib/profitFees";
import type { UiDay } from "../types";
import OverviewAnalyticsWorker from "../workers/overviewAnalyticsWorker.ts?worker";
import {
  computeAnalytics,
  type AnalyticsRequest,
  type AnalyticsResponse,
} from "../workers/overviewAnalyticsWorker";

export type OverviewAnalytics = AnalyticsResponse["analytics"];

/// Empty analytics state — UI render KPI = 0 / chart empty. Worker sẽ fill
/// trong ~100ms sau mount. Tránh sync compute khi days lớn (block main thread
/// + tab transition).
const EMPTY_ANALYTICS: OverviewAnalytics = {
  totals: {
    clicks: 0,
    shopeeClicks: 0,
    totalSpend: 0,
    orders: 0,
    commission: 0,
    profit: 0,
    orderValueTotal: 0,
    netCommission: 0,
    mcnFeeTotal: 0,
    commissionPending: 0,
    impressions: 0,
    clicksByReferrer: {},
    daysCount: 0,
    rowsCount: 0,
  },
  products: [],
  trendData: [],
  cumulativeData: [],
  breakeven: { breakevenCr: null, currentCr: null, gap: null, netPerOrder: null },
  funnel: {
    impressions: 0,
    adsClicks: 0,
    shopeeClicks: 0,
    orders: 0,
    ctrFb: null,
    ctrShopee: null,
    cr: null,
  },
  efficiency: {
    roas: null,
    epc: null,
    cpm: null,
    aov: null,
    avgDailySpend: null,
  },
  bestDay: null,
  worstDay: null,
  winners: [],
  losers: [],
};

/**
 * Compute Overview analytics off-main-thread qua Web Worker.
 *
 * - Lần đầu (days rỗng / chưa có analytics): chạy sync trên main thread để
 *   user thấy KPI tức thì, không phải đợi worker boot + postMessage round-trip.
 *   Cost lần đầu = O(0) khi days rỗng → vô hại.
 * - Lần sau (days đổi): gửi job cho worker, giữ analytics cũ + `isComputing=true`
 *   trong khi chờ. UI render data cũ → KHÔNG flash. Khi worker trả → swap.
 * - Race resolution: mỗi request có `requestId` tăng dần. Response cũ bị
 *   bỏ nếu `requestId !== latestRequestId`.
 *
 * Worker chết / lỗi: fallback compute trên main thread (yield qua microtask,
 * không freeze UI 1 frame nhưng có thể block 50-200ms — vẫn là path an toàn).
 */
export function useOverviewAnalytics(
  days: readonly UiDay[],
  clickSources: Record<string, boolean>,
  profitFees: ProfitFees,
  source: SourceFilter,
): { analytics: OverviewAnalytics; isComputing: boolean } {
  const workerRef = useRef<Worker | null>(null);
  const nextRequestIdRef = useRef(1);
  const latestRequestIdRef = useRef(0);

  // Initial analytics: chỉ sync compute KHI days nhỏ (<10 ngày) — nhanh,
  // không cảm nhận được block. Khi days lớn (đa số case khi user mở Overview
  // tab sau khi đã load full data), SKIP sync để tránh block 50-200ms khi
  // tab transition. Empty default + worker tự fill ngay sau mount.
  //
  // Trước: useMemo sync compute với mọi size → tab Overview transition lag
  // mạnh khi data nhiều. Giờ: instant transition, KPI fill in ~50-150ms sau.
  const initialAnalytics = useMemo<OverviewAnalytics>(() => {
    if (days.length >= 10) return EMPTY_ANALYTICS;
    return computeAnalytics({
      requestId: 0,
      days: days as UiDay[],
      clickSources,
      profitFees,
      source,
    });
    // Chỉ tính 1 lần lúc mount — sau đó worker lo. Đừng add deps.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const [analytics, setAnalytics] = useState<OverviewAnalytics>(initialAnalytics);
  const [isComputing, setIsComputing] = useState(
    days.length >= 10, // Show computing nếu sync compute bị skip → worker đang lo
  );

  // Spawn worker 1 lần. Tránh re-spawn mỗi render (đắt + leak).
  useEffect(() => {
    const worker = new OverviewAnalyticsWorker();
    workerRef.current = worker;
    worker.onmessage = (e: MessageEvent<AnalyticsResponse>) => {
      const { requestId, analytics: result } = e.data;
      // Stale response (user đã đổi filter sau request này) → bỏ.
      if (requestId !== latestRequestIdRef.current) return;
      setAnalytics(result);
      setIsComputing(false);
    };
    worker.onerror = (e) => {
      console.error("[overviewAnalyticsWorker] error:", e.message);
      setIsComputing(false);
    };
    return () => {
      worker.terminate();
      workerRef.current = null;
    };
  }, []);

  // Trigger compute mỗi khi input đổi. Skip first run CHỈ KHI sync compute đã
  // chạy (days nhỏ, analytics đã có data thực) — không cần worker race lại.
  // Khi days lớn (sync skipped, analytics = EMPTY), fire worker NGAY để fill.
  const isFirstRunRef = useRef(true);
  const hadSyncComputeRef = useRef(days.length < 10);
  useEffect(() => {
    if (isFirstRunRef.current) {
      isFirstRunRef.current = false;
      // Sync compute đã có data → skip worker; nếu không thì fall through để
      // worker chạy ngay lần đầu.
      if (hadSyncComputeRef.current) return;
    }
    const worker = workerRef.current;
    if (!worker) return;
    const requestId = nextRequestIdRef.current++;
    latestRequestIdRef.current = requestId;
    setIsComputing(true);
    const req: AnalyticsRequest = {
      requestId,
      days: days as UiDay[],
      clickSources,
      profitFees,
      source,
    };
    worker.postMessage(req);
  }, [days, clickSources, profitFees, source]);

  return { analytics, isComputing };
}
