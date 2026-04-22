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
}

export function useUserProfile(): UseUserProfileResult {
  const { user } = useAuth();
  const [profile, setProfile] = useState<UserProfile | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
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
      return;
    }

    setLoading(true);
    const ref = doc(db, "users", user.uid);

    const unsub = onSnapshot(
      ref,
      async (snap) => {
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

        setProfile({
          uid: user.uid,
          email: (data.email as string | undefined) ?? user.email ?? "",
          premium,
          admin,
          expiredAt,
          createdAt,
        });
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
        setError(err.message);
        setLoading(false);
      },
    );

    return () => unsub();
  }, [user]);

  return { profile, loading, error };
}
