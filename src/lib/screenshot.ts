//! Helper chụp ảnh DOM → PNG Blob cho tính năng screenshot DayBlock.
//!
//! Tối ưu: cache `fontEmbedCSS` (Material Icons, Inter) vào memory — lần chụp
//! đầu tiên sẽ tính toàn bộ @font-face thành data URL (chậm), các lần sau
//! dùng lại ngay. Nút camera có thể gọi `prefetchFontEmbedCSS()` khi hover
//! để warm up cache trước khi user click.

import { getFontEmbedCSS, toBlob } from "html-to-image";

let cachedFontEmbedCSS: string | null = null;
let pendingFontEmbedCSS: Promise<string> | null = null;

async function computeFontEmbedCSS(): Promise<string> {
  try {
    const css = await getFontEmbedCSS(document.body);
    cachedFontEmbedCSS = css;
    return css;
  } catch (e) {
    console.warn("getFontEmbedCSS failed — fallback without embed", e);
    cachedFontEmbedCSS = "";
    return "";
  } finally {
    pendingFontEmbedCSS = null;
  }
}

/** Warm-up cache — gọi khi user hover nút camera. Noop nếu đã cache hoặc đang chạy. */
export function prefetchFontEmbedCSS(): void {
  if (cachedFontEmbedCSS !== null || pendingFontEmbedCSS) return;
  pendingFontEmbedCSS = computeFontEmbedCSS();
}

async function getFontEmbedCSSCached(): Promise<string> {
  if (cachedFontEmbedCSS !== null) return cachedFontEmbedCSS;
  if (pendingFontEmbedCSS) return pendingFontEmbedCSS;
  pendingFontEmbedCSS = computeFontEmbedCSS();
  return pendingFontEmbedCSS;
}

interface CaptureOptions {
  /** Override cứng pixelRatio (1× = native, 2× = retina, 3-4× = upscale). */
  pixelRatio?: number;
  /** Output tối thiểu rộng N px (auto upscale pixelRatio để đạt). Mặc định
   *  2560 (= 2K horizontal) để khi zoom vẫn rõ chữ + số. */
  targetMinWidth?: number;
  backgroundColor?: string;
}

/**
 * Chụp 1 HTMLElement thành PNG Blob. Đợi fonts ready + 2×RAF cho layout
 * settle trước khi render, dùng `fontEmbedCSS` cache để tránh tính lại.
 *
 * Output mặc định ≥ 2K rộng (target 2560px). Tự compute pixelRatio dựa trên
 * CSS width của element, cap ở 4× để tránh OOM khi element vốn đã rộng.
 */
export async function captureElementToBlob(
  element: HTMLElement,
  opts: CaptureOptions = {},
): Promise<Blob> {
  await document.fonts.ready;
  await new Promise<void>((resolve) =>
    requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
  );
  const fontEmbedCSS = await getFontEmbedCSSCached();

  const targetMin = opts.targetMinWidth ?? 2560;
  const cssWidth = Math.max(1, element.offsetWidth);
  const cssHeight = Math.max(1, element.offsetHeight);
  // Canvas Chromium tối đa ~16384px mỗi chiều — vượt sẽ bị cắt mất phần
  // dưới/phải. Cap theo cả 2 chiều với safety margin.
  const MAX_DIM = 16000;
  const dimCap = Math.min(MAX_DIM / cssWidth, MAX_DIM / cssHeight);
  // Floor 2× (retina baseline), ceil targetMin/width, cap 4× upper bound,
  // rồi cap thêm theo dimension limit (quan trọng cho element rất tall như
  // OverviewTab nhiều rows).
  const wanted = Math.max(2, Math.ceil(targetMin / cssWidth));
  const autoRatio = Math.max(1, Math.min(4, wanted, dimCap));
  const pixelRatio = opts.pixelRatio ?? autoRatio;

  const blob = await toBlob(element, {
    pixelRatio,
    backgroundColor: opts.backgroundColor ?? "#121212",
    fontEmbedCSS: fontEmbedCSS || undefined,
  });
  if (!blob) throw new Error("Screenshot returned empty blob");
  return blob;
}
