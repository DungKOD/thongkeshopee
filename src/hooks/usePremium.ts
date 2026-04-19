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

export type PremiumStatus = "loading" | "inactive" | "active" | "expired";

interface UsePremiumResult {
  status: PremiumStatus;
  expiredAt: Date | null;
  error: string | null;
}

export function usePremium(): UsePremiumResult {
  const { user } = useAuth();
  const [status, setStatus] = useState<PremiumStatus>("loading");
  const [expiredAt, setExpiredAt] = useState<Date | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!user) {
      setStatus("loading");
      setExpiredAt(null);
      setError(null);
      return;
    }

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
          }
          return;
        }

        const data = snap.data();
        const premium = data.premium === true;
        const exp = data.expiredAt as Timestamp | null | undefined;
        const expDate = exp?.toDate?.() ?? null;
        setExpiredAt(expDate);

        if (!premium) {
          setStatus("inactive");
        } else if (expDate && expDate.getTime() < Date.now()) {
          setStatus("expired");
        } else {
          setStatus("active");
        }
        setError(null);
      },
      (err) => {
        setError(err.message);
        setStatus("inactive");
      },
    );

    return () => unsub();
  }, [user]);

  return { status, expiredAt, error };
}
