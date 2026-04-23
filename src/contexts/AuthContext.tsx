import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import {
  createUserWithEmailAndPassword,
  onAuthStateChanged,
  sendPasswordResetEmail,
  signInWithEmailAndPassword,
  signInWithPopup,
  signOut as fbSignOut,
  type User,
} from "firebase/auth";
import { httpsCallable } from "firebase/functions";
import { auth, functions, googleProvider } from "../lib/firebase";

interface AuthContextValue {
  user: User | null;
  loading: boolean;
  signInWithGoogle: () => Promise<void>;
  signInWithEmail: (email: string, password: string) => Promise<void>;
  signUpWithEmail: (email: string, password: string) => Promise<void>;
  resetPassword: (email: string) => Promise<void>;
  signOut: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

interface AuthProviderProps {
  children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, (u) => {
      setUser(u);
      setLoading(false);
    });
    return () => unsub();
  }, []);

  const signInWithGoogle = useCallback(async () => {
    await signInWithPopup(auth, googleProvider);
  }, []);

  const signInWithEmail = useCallback(async (email: string, password: string) => {
    await signInWithEmailAndPassword(auth, email, password);
  }, []);

  const signUpWithEmail = useCallback(async (email: string, password: string) => {
    await createUserWithEmailAndPassword(auth, email, password);
  }, []);

  const resetPassword = useCallback(async (email: string) => {
    // Rate-limit check qua Cloud Function: 5 lần/24h + cooldown 1 phút.
    // Function throw HttpsError nếu vượt → FE bắt error, show message.
    const requestReset = httpsCallable<{ email: string }, { allowed: boolean }>(
      functions,
      "requestPasswordReset",
    );
    await requestReset({ email });
    // Quota OK → gửi email reset qua Firebase Auth.
    await sendPasswordResetEmail(auth, email);
  }, []);

  const signOut = useCallback(async () => {
    await fbSignOut(auth);
  }, []);

  const value = useMemo<AuthContextValue>(
    () => ({
      user,
      loading,
      signInWithGoogle,
      signInWithEmail,
      signUpWithEmail,
      resetPassword,
      signOut,
    }),
    [
      user,
      loading,
      signInWithGoogle,
      signInWithEmail,
      signUpWithEmail,
      resetPassword,
      signOut,
    ],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
