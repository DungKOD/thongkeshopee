import { memo, useEffect, useRef, useState } from "react";
import { DayBlock } from "./DayBlock";
import type { UiDay, UiRow } from "../types";
import type { AccountFilterMode } from "../hooks/useDbStats";

interface LazyDayBlockProps {
  day: UiDay;
  /** True = mount DayBlock ngay frame đầu (cho block đầu tiên trong viewport).
   *  False = chỉ mount khi IntersectionObserver phát hiện gần viewport. */
  eager: boolean;
  pendingDayDeletes: ReadonlySet<string>;
  pendingRowDeletes: ReadonlyMap<string, unknown>;
  onToggleDayDelete: (date: string) => void;
  onToggleRowDelete: (row: UiRow) => void;
  onEditRow: (row: UiRow) => void;
  onEditDay: (date: string) => void;
  readOnly?: boolean;
  accountFilter?: AccountFilterMode;
}

/**
 * Lazy mount wrapper cho DayBlock. Mỗi block chỉ thực sự mount React component
 * (chạy hooks, useMemo O(N²), build DOM table 14 cột × N rows) khi sắp scroll
 * vào viewport. Trước đó: placeholder div với `min-height` để layout ổn định.
 *
 * Tại sao cần dù đã có `content-visibility: auto`?
 * - `content-visibility` chỉ skip browser LAYOUT/PAINT cho off-screen — DOM
 *   nodes + React state vẫn được tạo. Với 30+ DayBlock (mỗi cái có 5 useState,
 *   5 useRef, IntersectionObserver setup, useMemo O(N²)) → React mount cost
 *   ~30ms/block × 30 = 900ms blocking trên main thread.
 * - Lazy mount → 30 divs cheap + 1-2 DayBlock thực sự render. Initial paint
 *   ~30-60ms thay vì 500-900ms.
 *
 * `eager` cho block trên cùng (sẵn trong viewport) để render ngay, không cần
 * đợi IntersectionObserver callback (luôn có ≥1 frame delay).
 *
 * `rootMargin: 400px` mount trước khi user scroll tới → user không thấy
 * placeholder, cảm giác như đã sẵn sàng.
 */
function LazyDayBlockImpl({
  eager,
  day,
  ...rest
}: LazyDayBlockProps) {
  const [mounted, setMounted] = useState(eager);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (mounted) return;
    const el = ref.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting) {
          setMounted(true);
          obs.disconnect();
        }
      },
      { rootMargin: "400px" },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [mounted]);

  return (
    <div
      ref={ref}
      style={{
        contentVisibility: "auto",
        containIntrinsicSize: "auto 800px",
        minHeight: mounted ? undefined : 600,
      }}
    >
      {mounted ? <DayBlock day={day} {...rest} /> : null}
    </div>
  );
}

export const LazyDayBlock = memo(LazyDayBlockImpl);
