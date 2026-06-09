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
import type { Unsubscribe } from "firebase/firestore";
import { auth, googleProvider } from "../lib/firebase";
import {
  claimSession,
  clearSessionCache,
  verifySession,
  watchSession,
} from "../lib/deviceSession";
import {
  ensureUserProfile,
  getCachedUserProfile,
  watchUserProfile,
  type UserProfile,
} from "../lib/userProfile";

export interface SessionKickInfo {
  deviceName: string;
  platform: string;
  reason: "kicked" | "verify-failed";
}

interface AuthContextValue {
  user: User | null;
  loading: boolean;
  authError: string | null;
  /**
   * Error từ flow login phía Firestore (claim session fail, profile setup
   * fail). Persist qua null fire của onAuthStateChanged để user thấy lý do
   * khi bị đẩy về LoginScreen. Clear khi user login thành công lần sau.
   */
  lastSignInError: string | null;
  clearLastSignInError: () => void;
  /** Set khi máy này bị máy khác chiếm session — UI render dialog block. */
  kickInfo: SessionKickInfo | null;
  /** Clear kickInfo + đưa user về LoginScreen (gọi sau khi đóng dialog). */
  acknowledgeKick: () => void;
  /** Doc /users/{uid} realtime — null = chưa load hoặc doc không tồn tại. */
  userProfile: UserProfile | null;
  /** True khi đang fetch profile lần đầu sau login (chưa biết premium hay chưa). */
  profileLoading: boolean;
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
  const [lastSignInError, setLastSignInError] = useState<string | null>(null);
  const [kickInfo, setKickInfo] = useState<SessionKickInfo | null>(null);
  const [userProfile, setUserProfile] = useState<UserProfile | null>(null);
  const [profileLoading, setProfileLoading] = useState(false);

  const clearLastSignInError = useCallback(() => {
    setLastSignInError(null);
  }, []);

  // Unsubscribe handle cho realtime listener — phải clear khi user đổi hoặc
  // signOut, nếu không doc thay đổi sẽ trigger kick cho tài khoản đã logout.
  const watcherRef = useRef<Unsubscribe | null>(null);
  const stopWatcher = useCallback(() => {
    if (watcherRef.current) {
      watcherRef.current();
      watcherRef.current = null;
    }
  }, []);

  // Watcher cho /users/{uid} (premium/admin flags). Tách khỏi session watcher
  // vì 2 doc khác collection và lifecycle độc lập (vd: profile có thể fail
  // nhưng session vẫn ok).
  const profileWatcherRef = useRef<Unsubscribe | null>(null);
  const stopProfileWatcher = useCallback(() => {
    if (profileWatcherRef.current) {
      profileWatcherRef.current();
      profileWatcherRef.current = null;
    }
  }, []);

  // True nếu auth state change sắp tới là do user chủ động bấm "Đăng nhập"
  // (vs auto-restore từ persistence khi mở lại app). Khi manual = true thì
  // bỏ qua verify và claim luôn (máy mới thắng — đúng spec "auto-kick"); khi
  // false thì verify trước, nếu kicked thì giữ kick (không cướp session máy
  // khác chỉ vì restart app).
  const manualSignInRef = useRef(false);

  // Generation counter để invalidate stale async work khi onAuthStateChanged
  // fire liên tiếp (user A → null → user B). Mỗi fire tăng counter; async
  // closure capture giá trị lúc fire, check lại sau mỗi `await` — nếu counter
  // đã đổi thì abort + unsubscribe ngay (tránh leak watcher của user cũ).
  const authGenRef = useRef(0);

  // Retry state cho trường hợp auto-restore mà claim/watch fail (offline).
  // Khi online lại sẽ retry; cũng có exponential backoff timer phòng khi
  // `online` event không fire (Tauri webview hiếm khi miss, nhưng cẩn thận).
  const retryRef = useRef<{
    uid: string | null;
    timer: number | null;
    onOnline: (() => void) | null;
    delay: number;
  }>({ uid: null, timer: null, onOnline: null, delay: 5000 });

  const stopRetry = useCallback(() => {
    if (retryRef.current.timer !== null) {
      window.clearTimeout(retryRef.current.timer);
      retryRef.current.timer = null;
    }
    if (retryRef.current.onOnline) {
      window.removeEventListener("online", retryRef.current.onOnline);
      retryRef.current.onOnline = null;
    }
    retryRef.current.uid = null;
    retryRef.current.delay = 5000;
  }, []);

  // Tách thành callback để gọi từ cả listener (auto-kick) lẫn verify-failed.
  const handleKick = useCallback(
    (info: SessionKickInfo) => {
      stopWatcher();
      stopRetry();
      stopProfileWatcher();
      setKickInfo(info);
      // signOut Firebase nhưng GIỮ kickInfo để dialog hiện thị tên máy chiếm.
      // acknowledgeKick() sẽ clear sau khi user bấm "Đăng nhập lại".
      void fbSignOut(auth).catch((e) =>
        console.warn("[AuthContext] signOut after kick failed:", e),
      );
    },
    [stopWatcher, stopRetry, stopProfileWatcher],
  );

  // Claim doc + setup realtime watcher. Throws nếu Firestore unreachable.
  // Return unsub thay vì mutate `watcherRef` trực tiếp — caller có thể discard
  // nếu sau khi await xong phát hiện generation đã obsolete.
  const claimAndWatch = useCallback(
    async (uid: string): Promise<Unsubscribe> => {
      await claimSession(uid);
      return await watchSession(uid, (kicked) => {
        handleKick({
          deviceName: kicked.deviceName,
          platform: kicked.platform,
          reason: "kicked",
        });
      });
    },
    [handleKick],
  );

  // Setup watcher cho /users/{uid}. Strategy:
  // 1. Set up onSnapshot watcher TRƯỚC — Firestore SDK auto-queue subscription
  //    và fire khi mạng OK. Nếu doc đã có (existing user), snapshot fire ngay
  //    sau khi resolved → userProfile được set, profileLoading=false.
  // 2. ensureUserProfile (race 15s timeout) chạy SAU. Cho new user mạng OK,
  //    nó sẽ tạo doc → snapshot fire. Cho existing user, doc đã có, no-op.
  //    Lỗi/timeout của ensure KHÔNG kill watcher (best-effort).
  // 3. Nếu ensure timeout VÀ snapshot chưa fire trong 15s → load cache + set
  //    profileLoading(false) để khỏi splash forever.
  // Return Unsubscribe để caller mutate ref + check generation. Hàm này
  // KHÔNG throw — ensure timeout/fail dùng cache fallback internal.
  const setupProfileWatcher = useCallback(
    async (uid: string, email: string | null): Promise<Unsubscribe> => {
      const unsub = watchUserProfile(uid, (profile) => {
        setUserProfile(profile);
        setProfileLoading(false);
      });
      try {
        await Promise.race([
          ensureUserProfile(uid, email),
          new Promise<never>((_, reject) =>
            window.setTimeout(
              () => reject(new Error("ensureUserProfile timeout 15s")),
              15000,
            ),
          ),
        ]);
      } catch (e) {
        console.warn(
          "[AuthContext] ensureUserProfile failed or timed out:",
          e,
        );
        // Fallback cache để khỏi flash PremiumLockedScreen khi mạng chậm.
        // Snapshot listener vẫn active → sẽ refresh khi online lại.
        const cached = getCachedUserProfile(uid);
        if (cached) setUserProfile(cached);
        setProfileLoading(false);
      }
      return unsub;
    },
    [],
  );

  // Retry attempt: re-verify trước khi claim — tránh trường hợp giữa lúc fail
  // và lúc retry có máy khác đã hợp lệ chiếm session (vd user đi sang máy C
  // login). Nếu retry kick được = đúng intent; nếu doc đã thuộc C thì verify
  // trả "kicked" và máy này tự signOut, không cướp ngược của C.
  const attemptRetry = useCallback(
    async (uid: string) => {
      // Race guard: user có thể đã signOut hoặc đổi uid trong khi chờ retry.
      if (retryRef.current.uid !== uid) return;
      const myGen = authGenRef.current;
      try {
        const result = await verifySession(uid);
        if (retryRef.current.uid !== uid || authGenRef.current !== myGen) {
          return;
        }
        if (result.status === "kicked") {
          stopRetry();
          handleKick({
            deviceName: result.deviceName,
            platform: result.platform,
            reason: "verify-failed",
          });
          return;
        }
        const sessionUnsub = await claimAndWatch(uid);
        if (retryRef.current.uid !== uid || authGenRef.current !== myGen) {
          sessionUnsub();
          return;
        }
        if (watcherRef.current) watcherRef.current();
        watcherRef.current = sessionUnsub;

        // Retry thành công → setup profile watcher (chỉ nếu chưa có).
        if (!profileWatcherRef.current) {
          const email = auth.currentUser?.email ?? null;
          const profileUnsub = await setupProfileWatcher(uid, email);
          if (retryRef.current.uid !== uid || authGenRef.current !== myGen) {
            profileUnsub();
            return;
          }
          profileWatcherRef.current = profileUnsub;
        }
        stopRetry();
      } catch (e) {
        console.warn("[AuthContext] retry claim still failing:", e);
        const nextDelay = Math.min(60000, retryRef.current.delay * 2);
        retryRef.current.delay = nextDelay;
        retryRef.current.timer = window.setTimeout(() => {
          void attemptRetry(uid);
        }, nextDelay);
      }
    },
    [claimAndWatch, handleKick, setupProfileWatcher, stopRetry],
  );

  const scheduleRetry = useCallback(
    (uid: string) => {
      stopRetry();
      retryRef.current.uid = uid;
      retryRef.current.delay = 5000;
      retryRef.current.timer = window.setTimeout(() => {
        void attemptRetry(uid);
      }, 5000);
      const onOnline = () => {
        if (retryRef.current.timer !== null) {
          window.clearTimeout(retryRef.current.timer);
          retryRef.current.timer = null;
        }
        void attemptRetry(uid);
      };
      window.addEventListener("online", onOnline);
      retryRef.current.onOnline = onOnline;
    },
    [attemptRetry, stopRetry],
  );

  useEffect(() => {
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
        // lastSignInError: chỉ clear khi user login thành công (u != null).
        // Khi u=null (signOut sau claim fail), giữ lại để LoginScreen hiện.
        if (u) setLastSignInError(null);
        setUser(u);
        setLoading(false);

        // Tăng generation để invalidate mọi async closure pending từ fire trc.
        const myGen = ++authGenRef.current;
        stopWatcher();
        stopRetry();
        stopProfileWatcher();
        if (!u) {
          setUserProfile(null);
          setProfileLoading(false);
          return;
        }
        // User vừa login — profile chưa biết, set loading để AuthGate hiện
        // splash thay vì flash PremiumLockedScreen trong lúc fetch.
        setProfileLoading(true);

        // Phân biệt 2 luồng:
        // - Manual sign-in (user vừa bấm Đăng nhập): claim luôn → máy này
        //   thắng. Bỏ verify vì nếu không sẽ kẹt kick loop (sau khi bị kick,
        //   không login lại được trên máy cũ).
        // - Auto-restore (mở lại app, auth persist): verify trước. Nếu doc
        //   đã thuộc máy khác → giữ nguyên kick, không cướp session.
        const isManual = manualSignInRef.current;
        manualSignInRef.current = false;

        void (async () => {
          try {
            if (!isManual) {
              const result = await verifySession(u.uid);
              if (authGenRef.current !== myGen) return;
              if (result.status === "kicked") {
                handleKick({
                  deviceName: result.deviceName,
                  platform: result.platform,
                  reason: "verify-failed",
                });
                return;
              }
            }
            const sessionUnsub = await claimAndWatch(u.uid);
            // Sau await: nếu user đã đổi (gen mismatch), unsub watcher mình
            // vừa tạo để không leak listener của user cũ.
            if (authGenRef.current !== myGen) {
              sessionUnsub();
              return;
            }
            if (watcherRef.current) watcherRef.current();
            watcherRef.current = sessionUnsub;

            // Session đã claim → setup profile watcher (premium/admin flags).
            // setupProfileWatcher KHÔNG throw (cache fallback internal).
            const profileUnsub = await setupProfileWatcher(u.uid, u.email);
            if (authGenRef.current !== myGen) {
              profileUnsub();
              return;
            }
            if (profileWatcherRef.current) profileWatcherRef.current();
            profileWatcherRef.current = profileUnsub;
          } catch (e) {
            if (authGenRef.current !== myGen) return;
            if (isManual) {
              // User vừa bấm Đăng nhập mà claim fail = mạng đang lỗi. Báo
              // rõ qua lastSignInError (persist qua null fire) và signOut —
              // không retry background vì user đang chờ feedback ngay.
              console.warn("[AuthContext] manual claim failed:", e);
              setLastSignInError(
                "Không xác nhận được phiên đăng nhập trên Firestore. " +
                  "Kiểm tra kết nối mạng và thử lại.",
              );
              setProfileLoading(false);
              void fbSignOut(auth).catch(() => {});
              return;
            }
            // Auto-restore + claim fail: app đã chạy được (cache còn fresh).
            // Schedule retry khi `online` event fire hoặc backoff timer.
            // Retry sẽ re-verify trước claim để không cướp session máy khác.
            console.warn(
              "[AuthContext] auto-restore claim failed, scheduling retry:",
              e,
            );
            // Load cached profile để tránh flash PremiumLockedScreen khi user
            // tạm thời offline. Cache TTL 7 ngày — khi online lại snapshot sẽ
            // refresh + ghi đè cache. Nếu chưa có cache thì userProfile vẫn
            // null → lock screen (đúng cho user mới chưa từng load).
            const cached = getCachedUserProfile(u.uid);
            if (cached) setUserProfile(cached);
            setProfileLoading(false);
            scheduleRetry(u.uid);
          }
        })();
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
      stopWatcher();
      stopRetry();
      stopProfileWatcher();
      unsub();
    };
  }, [
    handleKick,
    stopWatcher,
    stopRetry,
    stopProfileWatcher,
    claimAndWatch,
    scheduleRetry,
    setupProfileWatcher,
  ]);

  // Khi expiredAt là Timestamp tương lai → schedule re-render đúng lúc nó hết
  // hạn để AuthGate lock app. Nếu kh có expiredAt (null) hoặc đã hết hạn từ
  // trước → no-op (isPremiumActive đã trả false sẵn).
  useEffect(() => {
    const expiredAtMs = userProfile?.expiredAt?.toMillis() ?? null;
    if (expiredAtMs === null) return;
    const delay = expiredAtMs - Date.now();
    if (delay <= 0) return;
    // setTimeout dùng int32, cap 24 ngày — vượt thì bỏ qua, đến lúc gần hạn
    // (admin update doc) snapshot listener tự fire và lập timer mới.
    if (delay > 2_147_483_000) return;
    const timer = window.setTimeout(() => {
      // Clone object → reference đổi → React re-render → isPremiumActive
      // trả false → AuthGate render PremiumLockedScreen.
      setUserProfile((p) => (p ? { ...p } : p));
    }, delay);
    return () => window.clearTimeout(timer);
  }, [userProfile]);

  // Mỗi sign-in method bật manualSignInRef trước khi gọi Firebase. Nếu Firebase
  // throw (sai mật khẩu, popup bị cancel...) → clear cờ trong catch để lần
  // auto-restore tiếp theo không bị nhầm là manual. Nếu thành công → để cờ
  // nguyên, onAuthStateChanged sẽ đọc + reset.
  const signInWithGoogle = useCallback(async () => {
    manualSignInRef.current = true;
    try {
      await signInWithPopup(auth, googleProvider);
    } catch (e) {
      manualSignInRef.current = false;
      throw e;
    }
  }, []);

  const signInWithEmail = useCallback(async (email: string, password: string) => {
    manualSignInRef.current = true;
    try {
      await signInWithEmailAndPassword(auth, email, password);
    } catch (e) {
      manualSignInRef.current = false;
      throw e;
    }
  }, []);

  const signUpWithEmail = useCallback(async (email: string, password: string) => {
    manualSignInRef.current = true;
    try {
      await createUserWithEmailAndPassword(auth, email, password);
    } catch (e) {
      manualSignInRef.current = false;
      throw e;
    }
  }, []);

  const resetPassword = useCallback(async (email: string) => {
    await sendPasswordResetEmail(auth, email);
  }, []);

  const signOut = useCallback(async () => {
    stopWatcher();
    stopRetry();
    stopProfileWatcher();
    clearSessionCache();
    await fbSignOut(auth);
  }, [stopWatcher, stopRetry, stopProfileWatcher]);

  const acknowledgeKick = useCallback(() => {
    clearSessionCache();
    setKickInfo(null);
  }, []);

  const value = useMemo<AuthContextValue>(
    () => ({
      user,
      loading,
      authError,
      lastSignInError,
      clearLastSignInError,
      kickInfo,
      acknowledgeKick,
      userProfile,
      profileLoading,
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
      lastSignInError,
      clearLastSignInError,
      kickInfo,
      acknowledgeKick,
      userProfile,
      profileLoading,
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
