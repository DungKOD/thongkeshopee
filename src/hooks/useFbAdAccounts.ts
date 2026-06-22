import { useCallback, useEffect, useState } from "react";
import { fbAdsListAccounts, type FbAdAccount } from "../lib/fbAds";

export interface UseFbAdAccountsResult {
  accounts: FbAdAccount[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

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

  return { accounts, loading, error, refresh };
}
