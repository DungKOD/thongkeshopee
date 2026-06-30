import { useEffect } from "react";

/// Custom event bus cho token changes — cross-component auto-refresh.
///
/// Tại sao dùng `window.dispatchEvent` thay vì React Context:
/// - Token data lưu ở Rust DB (fb_auth_tokens, fb_pages, fb_ad_accounts,
///   app_settings cho Shopee/OpenAI), KHÔNG ở React state. Khi dialog A
///   (Token Manager) save token, component B (Upload page ở tab khác) phải
///   refetch từ DB → cần signal cross-component.
/// - Tránh prop drilling callback `onChanged` qua 3-4 layer.
/// - Match cross-cutting concern: token có thể đổi ở 6+ chỗ (Token Manager,
///   FbPageManager, FbAdAccountManager, Shopee login flow, Settings AI key,
///   Upload page paste token).
///
/// Trade-off với Context: window event không có TypeScript safety mạnh, phải
/// dispatch + listen cùng type. Đổi lại: zero re-render cascade, listener
/// chỉ chạy khi emit, không gây overhead khi idle.

export type TokenKind =
  | "fb_user"
  | "fb_page"
  | "fb_ad_account"
  | "shopee"
  | "openai";

interface TokensChangedDetail {
  kind: TokenKind | "all";
}

const EVENT_NAME = "tokens-changed";

/// Broadcast token change. `kind = "all"` để ép mọi listener refetch (vd
/// sau import/restore DB). Caller bình thường nên emit kind cụ thể.
export function emitTokensChanged(kind: TokenKind | "all" = "all"): void {
  window.dispatchEvent(
    new CustomEvent<TokensChangedDetail>(EVENT_NAME, { detail: { kind } }),
  );
}

/// Subscribe token change. `filter` = mảng kind quan tâm; undefined = mọi
/// kind. Khi emit `kind === "all"`, handler luôn chạy bất kể filter (vì
/// "all" là broadcast tổng, vd sau workspace switch).
///
/// `handler` nhận `kind` để consumer biết EVENT gốc là gì — hữu ích khi 1
/// component cần refetch khác nhau theo loại token đổi.
///
/// QUAN TRỌNG: handler phải stable (useCallback) hoặc subscribe sẽ re-add
/// listener mỗi render → memory leak + double-fire.
export function useTokensChanged(
  handler: (kind: TokenKind | "all") => void,
  filter?: readonly TokenKind[],
): void {
  useEffect(() => {
    const fn = (e: Event) => {
      const detail = (e as CustomEvent<TokensChangedDetail>).detail;
      const kind = detail?.kind ?? "all";
      if (!filter || kind === "all" || filter.includes(kind as TokenKind)) {
        handler(kind);
      }
    };
    window.addEventListener(EVENT_NAME, fn);
    return () => window.removeEventListener(EVENT_NAME, fn);
  }, [handler, filter]);
}
