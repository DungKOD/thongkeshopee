import { useCallback, useEffect, useState } from "react";
import { fbListPages, type FbPage } from "../lib/fbReels";

export interface UseFbPagesResult {
  pages: FbPage[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

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

  return { pages, loading, error, refresh };
}
