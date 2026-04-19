import { useUserProfile } from "./useUserProfile";

export type PremiumStatus = "loading" | "inactive" | "active" | "expired";

interface UsePremiumResult {
  status: PremiumStatus;
  expiredAt: Date | null;
  error: string | null;
}

export function usePremium(): UsePremiumResult {
  const { profile, loading, error } = useUserProfile();

  if (loading || (!profile && !error)) {
    return { status: "loading", expiredAt: null, error };
  }
  if (!profile) {
    return { status: "inactive", expiredAt: null, error };
  }

  const expiredAt = profile.expiredAt;

  if (!profile.premium) {
    return { status: "inactive", expiredAt, error };
  }
  if (expiredAt && expiredAt.getTime() < Date.now()) {
    return { status: "expired", expiredAt, error };
  }
  return { status: "active", expiredAt, error };
}

export function useIsAdmin(): boolean {
  const { profile } = useUserProfile();
  return profile?.admin === true;
}
