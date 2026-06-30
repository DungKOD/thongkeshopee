import { invoke } from "./tauri";

export interface ShopeeAffIdResult {
  affId: number | null;
  finalUrl: string;
  /** "utm_source" | "mmp_pid" | "none" */
  source: string;
}

/**
 * Tra Affiliate Publisher ID từ Shopee link (short hoặc full).
 * Follow redirect → parse `utm_source=an_<id>` hoặc `mmp_pid=an_<id>`.
 * Throw nếu URL không phải Shopee hoặc network fail.
 */
export function findShopeeAffId(url: string): Promise<ShopeeAffIdResult> {
  return invoke<ShopeeAffIdResult>("find_shopee_aff_id", { url });
}

export interface CleanLinkReplacement {
  original: string;
  cleaned: string;
}

export interface CleanLinkError {
  original: string;
  error: string;
}

export interface CleanLinksResult {
  outputText: string;
  replacements: CleanLinkReplacement[];
  errors: CleanLinkError[];
  totalFound: number;
}

/**
 * Tìm tất cả link Shopee trong text → follow redirect → strip tracking
 * params → replace inline. Trả output text mới + danh sách replacements.
 */
export function cleanShopeeLinks(inputText: string): Promise<CleanLinksResult> {
  return invoke<CleanLinksResult>("clean_shopee_links", { inputText });
}

