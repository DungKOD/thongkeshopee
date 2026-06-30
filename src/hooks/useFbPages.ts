import { useCallback, useEffect, useState } from "react";
import { fbListPages, type FbPage } from "../lib/fbReels";
import { useTokensChanged } from "../lib/tokenEvents";

export interface UseFbPagesResult {
  pages: FbPage[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

const FILTER = ["fb_page", "fb_user"] as const;

export function useFbPages(): UseFbPagesResult {
  const [pages, setPages] = useState<FbPage[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const list = await fbListPages();
      setPages(list);
      setError(null);
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // Auto-refresh khi token đổi ở bất cứ component nào khác (Token Manager,
  // FbPageManagerDialog, v.v.). Listen cả `fb_page` (page save/delete) lẫn
  // `fb_user` (user token đổi có thể ảnh hưởng tới page list nếu discover lại).
  useTokensChanged(refresh, FILTER);

  return { pages, loading, error, refresh };
}
