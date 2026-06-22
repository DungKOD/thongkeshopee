import { useCallback, useEffect, useState } from "react";
import { fbAdsListDrafts, type FbCampDraftSummary } from "../lib/fbAds";

export interface UseFbCampDraftsResult {
  drafts: FbCampDraftSummary[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

export function useFbCampDrafts(): UseFbCampDraftsResult {
  const [drafts, setDrafts] = useState<FbCampDraftSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setDrafts(await fbAdsListDrafts());
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

  return { drafts, loading, error, refresh };
}
