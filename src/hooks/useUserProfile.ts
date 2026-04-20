import { useEffect, useState } from "react";
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

        setProfile({
          uid: user.uid,
          email: (data.email as string | undefined) ?? user.email ?? "",
          premium: data.premium === true,
          admin: data.admin === true,
          expiredAt,
          createdAt,
        });
        setError(null);
        setLoading(false);
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
