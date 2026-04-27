import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
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
import {
  registerMyDeviceLogin,
  subscribeDeviceRevocation,
} from "../lib/deviceGate";

interface AuthContextValue {
  user: User | null;
  loading: boolean;
  /// Lỗi từ onAuthStateChanged listener (Firebase SDK init fail, network
  /// fail nghiêm trọng). Khác `deviceCheckError`: error này = không xác định
  /// được auth state, app phải block. UI render splash error + reload.
  authError: string | null;
  /// Set khi register device entry sau login fail vì lỗi system (RTDB write
  /// throw, không phải RTDB chưa config — case đó fail-soft). Caller
  /// (App.tsx) render dialog blocking + nút Đăng xuất. `null` khi chưa
  /// register / register pass.
  deviceCheckError: string | null;
  /// Set khi admin xóa entry device hiện tại trong lúc user đang chạy app.
  /// Caller render toast / dialog rồi gọi signOut.
  deviceRevoked: boolean;
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
  const [authError, setAuthError] = useState<string | null>(null);
  const [deviceCheckError, setDeviceCheckError] = useState<string | null>(null);
  const [deviceRevoked, setDeviceRevoked] = useState(false);
  /// Track uid đang được enforce → tránh race khi onAuthStateChanged fire
  /// nhiều lần trong khi enforce async chưa xong.
  const enforcedUidRef = useRef<string | null>(null);

  useEffect(() => {
    // 10s safety timeout — nếu onAuthStateChanged không emit trong 10s
    // (Firebase SDK init bị treo, IndexedDB lock, network DNS fail), set
    // authError + loading=false để AuthGate hiện splash lỗi thay vì spinner
    // mãi. Firebase normally fire trong <100ms (cached) hoặc <2s (server).
    const timeoutId = window.setTimeout(() => {
      setAuthError(
        "Firebase Auth không phản hồi trong 10s — có thể SDK init lỗi hoặc " +
          "mạng chậm. Thử reload app hoặc kiểm tra kết nối.",
      );
      setLoading(false);
    }, 10000);

    const unsub = onAuthStateChanged(
      auth,
      (u) => {
        window.clearTimeout(timeoutId);
        setAuthError(null);
        setUser(u);
        setLoading(false);
        if (!u) {
          // Logout → reset state, cleanup ref.
          enforcedUidRef.current = null;
          setDeviceCheckError(null);
          setDeviceRevoked(false);
        }
      },
      (err) => {
        window.clearTimeout(timeoutId);
        console.error("[AuthContext] onAuthStateChanged error:", err);
        setAuthError(`Lỗi xác thực: ${err.message}`);
        setLoading(false);
      },
    );
    return () => {
      window.clearTimeout(timeoutId);
      unsub();
    };
  }, []);

  /// Effect riêng cho device gate. Chạy mỗi khi `user` đổi (login/logout).
  /// Force refresh ID token để pickup admin claim mới nhất, rồi enforce
  /// limit + subscribe revocation. Cleanup unsub khi user đổi.
  useEffect(() => {
    if (!user) return;
    if (enforcedUidRef.current === user.uid) return;
    enforcedUidRef.current = user.uid;

    let unsubRevoke: (() => void) | null = null;
    let cancelled = false;

    void (async () => {
      try {
        // Force refresh để custom claim `admin` mới nhất hiện trong token.
        let tokenResult = await user.getIdTokenResult(true);
        let isAdmin = tokenResult.claims.admin === true;

        // Auto-heal: token claim missing nhưng user CÓ thể là admin theo
        // Firestore profile. forceSyncUserClaims callable check Firestore
        // → setCustomUserClaims nếu user là admin → trả success. Non-admin
        // sẽ bị reject (permission-denied) — bắt + ignore.
        //
        // Use case cover:
        //  - Bootstrap admin đầu tiên (Firestore admin=true set thủ công
        //    trước khi syncUserClaims function deploy).
        //  - Cloud Function không fire cho user này (event drop, function
        //    deploy lỗi giữa session).
        //  - Custom claim drift sau migration / restore.
        //
        // Cost: 1 callable round-trip mỗi lần login (~200ms). Skip khi
        // claim đã có. Không loop — sau heal, refresh token sẽ thấy claim.
        if (!isAdmin) {
          try {
            const forceSync = httpsCallable<
              { uid: string },
              { success: boolean; admin: boolean }
            >(functions, "forceSyncUserClaims");
            const r = await forceSync({ uid: user.uid });
            if (r.data?.admin === true) {
              // Function đã set claim → refresh token để pickup.
              tokenResult = await user.getIdTokenResult(true);
              isAdmin = tokenResult.claims.admin === true;
              if (!isAdmin) {
                console.warn(
                  "[AuthContext] forceSyncUserClaims success nhưng claim chưa pickup — Firebase eventual consistency, sẽ thấy ở lần login kế",
                );
              } else {
                console.log("[AuthContext] auto-healed admin token claim");
              }
            }
          } catch (e) {
            // permission-denied = non-admin user, expected. Mọi lỗi khác log
            // nhưng không block flow (registerMyDeviceLogin vẫn chạy bình thường).
            const code = (e as { code?: string }).code;
            if (code !== "functions/permission-denied") {
              console.warn(
                "[AuthContext] forceSyncUserClaims failed (non-fatal):",
                e,
              );
            }
          }
        }

        const result = await registerMyDeviceLogin(user.uid);
        if (cancelled) return;
        if (!result.ok) {
          // KHÔNG signOut ngay — để AuthGate hiện dialog blocking với nút
          // "Đăng xuất". User đọc message rồi tự bấm. Nếu signOut auto
          // thì user → null → AuthGate switch sang LoginScreen, dialog mất.
          setDeviceCheckError(result.message);
          return;
        }
        unsubRevoke = subscribeDeviceRevocation(
          user.uid,
          result.deviceInfo.fingerprint,
          () => {
            setDeviceRevoked(true);
          },
        );
      } catch (e) {
        if (cancelled) return;
        console.error("[AuthContext] device register error:", e);
        setDeviceCheckError(
          `Không đăng ký được thiết bị: ${(e as Error).message}`,
        );
      }
    })();

    return () => {
      cancelled = true;
      if (unsubRevoke) unsubRevoke();
    };
  }, [user]);

  const signInWithGoogle = useCallback(async () => {
    setDeviceCheckError(null);
    setDeviceRevoked(false);
    await signInWithPopup(auth, googleProvider);
  }, []);

  const signInWithEmail = useCallback(async (email: string, password: string) => {
    setDeviceCheckError(null);
    setDeviceRevoked(false);
    await signInWithEmailAndPassword(auth, email, password);
  }, []);

  const signUpWithEmail = useCallback(async (email: string, password: string) => {
    setDeviceCheckError(null);
    setDeviceRevoked(false);
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
    setDeviceCheckError(null);
    setDeviceRevoked(false);
    await fbSignOut(auth);
  }, []);

  const value = useMemo<AuthContextValue>(
    () => ({
      user,
      loading,
      authError,
      deviceCheckError,
      deviceRevoked,
      signInWithGoogle,
      signInWithEmail,
      signUpWithEmail,
      resetPassword,
      signOut,
    }),
    [
      user,
      loading,
      authError,
      deviceCheckError,
      deviceRevoked,
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
