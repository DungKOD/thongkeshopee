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
  pixelRatio?: number;
  backgroundColor?: string;
}

/**
 * Chụp 1 HTMLElement thành PNG Blob. Đợi fonts ready + 2×RAF cho layout
 * settle trước khi render, dùng `fontEmbedCSS` cache để tránh tính lại.
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
  const blob = await toBlob(element, {
    pixelRatio: opts.pixelRatio ?? 2,
    backgroundColor: opts.backgroundColor ?? "#121212",
    fontEmbedCSS: fontEmbedCSS || undefined,
  });
  if (!blob) throw new Error("Screenshot returned empty blob");
  return blob;
}
