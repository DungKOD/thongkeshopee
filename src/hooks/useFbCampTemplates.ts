import { useCallback, useEffect, useState } from "react";
import { fbAdsListTemplates, type FbCampTemplate } from "../lib/fbAds";

export interface UseFbCampTemplatesResult {
  templates: FbCampTemplate[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

export function useFbCampTemplates(
  accountId?: string,
): UseFbCampTemplatesResult {
  const [templates, setTemplates] = useState<FbCampTemplate[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setTemplates(await fbAdsListTemplates(accountId));
      setError(null);
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setLoading(false);
    }
  }, [accountId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return { templates, loading, error, refresh };
}
