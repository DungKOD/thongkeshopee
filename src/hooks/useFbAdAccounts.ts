import { useCallback, useEffect, useState } from "react";
import { fbAdsListAccounts, type FbAdAccount } from "../lib/fbAds";
import { useTokensChanged } from "../lib/tokenEvents";

export interface UseFbAdAccountsResult {
  accounts: FbAdAccount[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

const FILTER = ["fb_ad_account", "fb_user"] as const;

export function useFbAdAccounts(): UseFbAdAccountsResult {
  const [accounts, setAccounts] = useState<FbAdAccount[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setAccounts(await fbAdsListAccounts());
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

  // Auto-refresh khi ad account hoặc fb_user token đổi ở component khác.
  useTokensChanged(refresh, FILTER);

  return { accounts, loading, error, refresh };
}
