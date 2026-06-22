import { useEffect, useState } from "react";

/**
 * Trả về số item nên render ở thời điểm hiện tại — mở rộng dần đến `total`
 * qua các frame idle. Mỗi lần `resetKey` đổi → reset count về `initial`.
 *
 * Dùng để cap số DayBlock mount trong 1 frame: render JSX + tạo DOM của
 * hàng chục bảng to trong 1 lúc làm chiếm main thread > 16ms (frame drop +
 * UI đơ). Chia thành nhiều batch để mỗi frame chỉ tăng `step` item, browser
 * có cơ hội paint + handle input giữa các batch.
 */
export function useProgressiveCount(
  total: number,
  resetKey: unknown,
  initial: number = 8,
  step: number = 6,
): number {
  const [count, setCount] = useState(() => Math.min(total, initial));

  // Reset count khi list đổi (filter switch / refetch). Dùng resetKey thay vì
  // ref equality — caller có thể truyền `days` (changes when fetched), hoặc
  // filterKey (changes when filter switches), tùy ngữ cảnh.
  useEffect(() => {
    setCount(Math.min(total, initial));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resetKey, initial]);

  // Tăng dần đến total qua requestIdleCallback. Timeout=50ms để không quá
  // chậm khi browser bận liên tục.
  useEffect(() => {
    if (count >= total) return;
    const ric: (cb: () => void) => number =
      typeof requestIdleCallback === "function"
        ? (cb) => requestIdleCallback(cb, { timeout: 50 })
        : (cb) => setTimeout(cb, 16) as unknown as number;
    const cancel: (id: number) => void =
      typeof cancelIdleCallback === "function"
        ? cancelIdleCallback
        : (id) => clearTimeout(id);
    const id = ric(() => {
      setCount((c) => Math.min(c + step, total));
    });
    return () => cancel(id);
  }, [count, total, step]);

  return count;
}
