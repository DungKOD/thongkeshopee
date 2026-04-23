import { useEffect, useState } from "react";
import { invoke } from "../lib/tauri";
import type { AccountFilterMode } from "../hooks/useDbStats";
import { HourlyChart, type HourlyBucket } from "./HourlyChart";
import {
  ClickDelayChart,
  ReferrerEfficiencyTable,
  type DelayBucket,
  type ReferrerEfficiency,
} from "./OverviewClickInsights";

interface HourlyOrderBucketDto {
  hour: number;
  orders: number;
  orderValue: number;
  commission: number;
}

interface HourlyClickBucketDto {
  hour: number;
  clicks: number;
}

interface FilterInput {
  fromDate?: string;
  toDate?: string;
  limit?: number;
  accountFilter?: AccountFilterMode;
  /// Sub_id tuple exact của product — BE prefix-match filter.
  subIds: [string, string, string, string, string];
}

interface Props {
  /// Filter scope cho BE queries. `subIds` bắt buộc — đây là dialog per-product.
  filter: FilterInput;
}

/**
 * 4 analysis sections cho Chi tiết sản phẩm / Chi tiết campaign, scoped theo
 * sub_ids của product. Share giữa `AggregateProductDialog` + `ProductDetailDialog`:
 *
 * - Hourly clicks: giờ user click vào link affiliate nhiều nhất
 * - Hourly orders: giờ user chốt đơn nhiều nhất
 * - Click delay histogram: impulse vs consider behavior
 * - Referrer efficiency: nguồn traffic nào convert tốt cho SP này
 *
 * Fetch 4 BE commands song song qua Promise.all → 1 round-trip.
 */
export function ProductClickInsights({ filter }: Props) {
  const [hourlyOrders, setHourlyOrders] = useState<HourlyBucket[]>([]);
  const [hourlyClicks, setHourlyClicks] = useState<HourlyBucket[]>([]);
  const [referrerEff, setReferrerEff] = useState<ReferrerEfficiency[]>([]);
  const [delays, setDelays] = useState<DelayBucket[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    const beFilter = {
      fromDate: filter.fromDate,
      toDate: filter.toDate,
      limit: filter.limit,
      accountFilter: filter.accountFilter,
      subIds: filter.subIds,
    };
    Promise.all([
      invoke<HourlyOrderBucketDto[]>("load_hourly_orders", { filter: beFilter }),
      invoke<HourlyClickBucketDto[]>("load_hourly_clicks", { filter: beFilter }),
      invoke<ReferrerEfficiency[]>("load_referrer_efficiency", { filter: beFilter }),
      invoke<DelayBucket[]>("load_click_order_delays", { filter: beFilter }),
    ])
      .then(([orders, clicks, referrers, delayData]) => {
        if (cancelled) return;
        setHourlyOrders(
          orders.map((b) => ({
            hour: b.hour,
            orders: b.orders,
            orderValue: b.orderValue,
            commission: b.commission,
            clicks: 0,
          })),
        );
        setHourlyClicks(
          clicks.map((b) => ({
            hour: b.hour,
            orders: 0,
            orderValue: 0,
            commission: 0,
            clicks: b.clicks,
          })),
        );
        setReferrerEff(referrers);
        setDelays(delayData);
      })
      .catch((e) => {
        console.error("[product click insights] load failed:", e);
        if (!cancelled) {
          setHourlyOrders([]);
          setHourlyClicks([]);
          setReferrerEff([]);
          setDelays([]);
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [
    filter.fromDate,
    filter.toDate,
    filter.limit,
    filter.accountFilter,
    // Dependency on subIds — array ref comparison OK vì parent pass stable.
    filter.subIds[0],
    filter.subIds[1],
    filter.subIds[2],
    filter.subIds[3],
    filter.subIds[4],
  ]);

  return (
    <div className="space-y-4">
      <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <HourlyChart
          data={hourlyClicks}
          title="Giờ click Shopee"
          metric="clicks"
          icon="mouse"
          loading={loading}
        />
        <HourlyChart
          data={hourlyOrders}
          title="Giờ chốt đơn"
          metric="orders"
          icon="schedule"
          loading={loading}
        />
      </section>

      <ClickDelayChart data={delays} />

      <ReferrerEfficiencyTable rows={referrerEff} />
    </div>
  );
}
