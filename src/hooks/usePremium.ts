import { useUserProfile } from "./useUserProfile";

export type PremiumStatus =
  | "loading"
  | "inactive"
  | "active"
  | "expired"
  /// Firestore không emit profile trong 8s + không có cached profile. UI
  /// nên render PaywallScreen kèm message "Không xác minh được trạng thái —
  /// kiểm tra mạng" + retry/relogin button thay vì splash mãi.
  | "verify_failed";

interface UsePremiumResult {
  status: PremiumStatus;
  expiredAt: Date | null;
  error: string | null;
  /** True khi status đang bypass real-time check (dùng cached profile vì
   *  Firestore lag/error). UI có thể hiện banner "Đang dùng cached" nhỏ. */
  usingCache: boolean;
}

export function usePremium(): UsePremiumResult {
  const { profile, loading, error, timedOut } = useUserProfile();

  // Có profile (snapshot mới HOẶC cached từ localStorage) → trust nó luôn.
  // Premium user đã từng login máy này → cache hit → không bao giờ bị paywall
  // false-positive khi Firestore lag/error. Cache stale tối đa = thời gian
  // user offline; khi online lại snapshot fire sẽ refresh.
  if (profile) {
    const expiredAt = profile.expiredAt;
    const usingCache = (loading || !!error || timedOut) && profile !== null;
    if (!profile.premium) {
      return { status: "inactive", expiredAt, error, usingCache };
    }
    if (expiredAt && expiredAt.getTime() < Date.now()) {
      return { status: "expired", expiredAt, error, usingCache };
    }
    return { status: "active", expiredAt, error, usingCache };
  }

  // Chưa có profile + chưa timeout + không error → vẫn đợi Firestore (cold
  // start, splash legitimate). Loading <8s vì timeout sẽ kick in.
  if (loading && !timedOut && !error) {
    return { status: "loading", expiredAt: null, error: null, usingCache: false };
  }

  // Chưa có profile + (timed out HOẶC error) → user lần đầu login trên máy
  // này nhưng Firestore unreachable. KHÔNG thể tự động trust premium (rủi ro
  // free user dùng trial vô hạn). UI hiện màn lỗi với retry/relogin.
  return {
    status: "verify_failed",
    expiredAt: null,
    error: error ?? "Không xác minh được trạng thái tài khoản",
    usingCache: false,
  };
}

export function useIsAdmin(): boolean {
  const { profile } = useUserProfile();
  return profile?.admin === true;
}
