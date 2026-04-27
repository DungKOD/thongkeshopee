import { useEffect, useRef, useState } from "react";
import {
  doc,
  onSnapshot,
  serverTimestamp,
  setDoc,
  Timestamp,
} from "firebase/firestore";
import { db } from "../lib/firebase";
import { useAuth } from "../contexts/AuthContext";

export interface UserProfile {
  uid: string;
  email: string;
  premium: boolean;
  admin: boolean;
  expiredAt: Date | null;
  createdAt: Date | null;
}

interface UseUserProfileResult {
  profile: UserProfile | null;
  loading: boolean;
  error: string | null;
  /** True khi onSnapshot không emit trong 8s (Firestore lag/timeout). UI
   *  combine với `profile=null` để fallback paywall thay vì splash mãi. */
  timedOut: boolean;
}

/// localStorage cache để premium check work offline + chống treo splash khi
/// Firestore lag. Snapshot mỗi lần fire ghi cache; useEffect mount load cache
/// trước khi onSnapshot fire → user đã từng login → instant profile, không
/// chờ network.
const CACHE_PREFIX = "thongkeshopee.user_profile_v1.";
const SNAPSHOT_TIMEOUT_MS = 8000;

interface CachedProfile {
  uid: string;
  email: string;
  premium: boolean;
  admin: boolean;
  expiredAtMs: number | null;
  createdAtMs: number | null;
}

function loadCachedProfile(uid: string): UserProfile | null {
  try {
    const raw = localStorage.getItem(CACHE_PREFIX + uid);
    if (!raw) return null;
    const c = JSON.parse(raw) as CachedProfile;
    if (c.uid !== uid) return null;
    return {
      uid: c.uid,
      email: c.email,
      premium: c.premium,
      admin: c.admin,
      expiredAt: c.expiredAtMs ? new Date(c.expiredAtMs) : null,
      createdAt: c.createdAtMs ? new Date(c.createdAtMs) : null,
    };
  } catch {
    return null;
  }
}

function saveCachedProfile(p: UserProfile): void {
  try {
    const c: CachedProfile = {
      uid: p.uid,
      email: p.email,
      premium: p.premium,
      admin: p.admin,
      expiredAtMs: p.expiredAt?.getTime() ?? null,
      createdAtMs: p.createdAt?.getTime() ?? null,
    };
    localStorage.setItem(CACHE_PREFIX + p.uid, JSON.stringify(c));
  } catch {
    // quota / privacy mode — ignore
  }
}

export function useUserProfile(): UseUserProfileResult {
  const { user } = useAuth();
  const [profile, setProfile] = useState<UserProfile | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [timedOut, setTimedOut] = useState(false);
  // Snapshot của admin/premium/expiredAt lần trước để detect change → force
  // refresh ID token. Worker đọc claims từ token (không fetch Firestore nữa),
  // nên token CŨ mang claim cũ → user phải getIdToken(true) để Worker thấy
  // quyền mới. Delay 3s chờ Cloud Function syncUserClaims kịp chạy.
  const prevClaimsRef = useRef<{
    admin: boolean;
    premium: boolean;
    expiredMs: number | null;
  } | null>(null);

  useEffect(() => {
    if (!user) {
      setProfile(null);
      setLoading(false);
      setError(null);
      setTimedOut(false);
      return;
    }

    setError(null);
    setTimedOut(false);

    // Hydrate từ cache NGAY → premium check không chờ network. Nếu cache hit,
    // loading=false ngay; snapshot sẽ overwrite khi Firestore response.
    const cached = loadCachedProfile(user.uid);
    if (cached) {
      setProfile(cached);
      setLoading(false);
      // Reset prevClaimsRef = cached → snapshot lần đầu không trigger force
      // refresh nhầm (claims chưa thực sự đổi, chỉ là cache vs server).
      prevClaimsRef.current = {
        admin: cached.admin,
        premium: cached.premium,
        expiredMs: cached.expiredAt?.getTime() ?? null,
      };
    } else {
      setLoading(true);
    }

    // 8s timeout — nếu onSnapshot không emit (Firestore không response, IndexedDB
    // cache cũng miss vì user chưa login bao giờ trên máy này), set timedOut.
    // usePremium dùng flag này để fallback paywall thay vì treo splash mãi.
    const timeoutId = window.setTimeout(() => {
      setTimedOut(true);
    }, SNAPSHOT_TIMEOUT_MS);

    const ref = doc(db, "users", user.uid);

    const unsub = onSnapshot(
      ref,
      async (snap) => {
        window.clearTimeout(timeoutId);
        setTimedOut(false);
        if (!snap.exists()) {
          try {
            await setDoc(ref, {
              email: user.email ?? "",
              premium: false,
              admin: false,
              expiredAt: null,
              createdAt: serverTimestamp(),
            });
          } catch (e) {
            setError(`Không tạo được hồ sơ: ${(e as Error).message}`);
            setLoading(false);
          }
          return;
        }

        const data = snap.data();
        const expiredAt = (data.expiredAt as Timestamp | null | undefined)
          ?.toDate?.() ?? null;
        const createdAt = (data.createdAt as Timestamp | null | undefined)
          ?.toDate?.() ?? null;

        const admin = data.admin === true;
        const premium = data.premium === true;
        const expiredMs = expiredAt?.getTime() ?? null;

        const next: UserProfile = {
          uid: user.uid,
          email: (data.email as string | undefined) ?? user.email ?? "",
          premium,
          admin,
          expiredAt,
          createdAt,
        };
        setProfile(next);
        saveCachedProfile(next);
        setError(null);
        setLoading(false);

        // Force-refresh ID token khi claim-relevant field đổi (skip lần load
        // đầu vì token client-side đã có claim đúng rồi). Delay 3s cho Cloud
        // Function syncUserClaims kịp ghi claim mới vào Auth.
        const prev = prevClaimsRef.current;
        prevClaimsRef.current = { admin, premium, expiredMs };
        if (
          prev !== null &&
          (prev.admin !== admin ||
            prev.premium !== premium ||
            prev.expiredMs !== expiredMs)
        ) {
          setTimeout(() => {
            user.getIdToken(true).catch((e) =>
              console.warn("[useUserProfile] force refresh token failed:", e),
            );
          }, 3000);
        }
      },
      (err) => {
        window.clearTimeout(timeoutId);
        setError(err.message);
        setLoading(false);
      },
    );

    return () => {
      window.clearTimeout(timeoutId);
      unsub();
    };
  }, [user]);

  return { profile, loading, error, timedOut };
}
