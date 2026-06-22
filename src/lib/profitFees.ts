/**
 * Pure types/utils dùng chung cho compute (FE + Web Worker).
 *
 * Tách khỏi `hooks/useSettings.tsx` để worker import không kéo React vào
 * worker bundle. Module này KHÔNG được import React/JSX/Context.
 */

export interface ProfitFees {
  /** % thuế + phí sàn khấu trừ từ hoa hồng (vd 10.98). */
  taxAndPlatformRate: number;
  /** % dự phòng hoàn/hủy đơn (vd 9). */
  returnReserveRate: number;
}

/**
 * Tính shopeeClicks hiển thị từ breakdown theo settings.
 * Referrer không có trong settings (vd "Nhập tay" khi chưa đăng ký) mặc định = enabled.
 */
export function sumFiltered(
  breakdown: Record<string, number>,
  clickSources: Record<string, boolean>,
): number {
  let total = 0;
  for (const [ref, n] of Object.entries(breakdown)) {
    if (clickSources[ref] !== false) total += n;
  }
  return total;
}
