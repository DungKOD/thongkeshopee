import { initializeApp, type FirebaseApp } from "firebase/app";
import {
  getAuth,
  type Auth,
  GoogleAuthProvider,
  browserLocalPersistence,
  setPersistence,
} from "firebase/auth";
import {
  initializeFirestore,
  persistentLocalCache,
  persistentSingleTabManager,
  type Firestore,
} from "firebase/firestore";
import { getDatabase, type Database } from "firebase/database";
import { getFunctions, type Functions } from "firebase/functions";
import { timed } from "./net_log";

const firebaseConfig = {
  apiKey: import.meta.env.VITE_FIREBASE_API_KEY,
  authDomain: import.meta.env.VITE_FIREBASE_AUTH_DOMAIN,
  projectId: import.meta.env.VITE_FIREBASE_PROJECT_ID,
  storageBucket: import.meta.env.VITE_FIREBASE_STORAGE_BUCKET,
  messagingSenderId: import.meta.env.VITE_FIREBASE_MESSAGING_SENDER_ID,
  appId: import.meta.env.VITE_FIREBASE_APP_ID,
  databaseURL: import.meta.env.VITE_FIREBASE_DATABASE_URL,
};

export const firebaseApp: FirebaseApp = initializeApp(firebaseConfig);
export const auth: Auth = getAuth(firebaseApp);
/// Firestore với persistent IndexedDB cache. `useUserProfile` onSnapshot
/// sẽ emit data từ cache khi offline → paywall check không block user đã
/// từng load profile online. Single-tab manager đủ cho app desktop (1 window).
export const db: Firestore = initializeFirestore(firebaseApp, {
  localCache: persistentLocalCache({
    tabManager: persistentSingleTabManager({}),
  }),
});
/// RTDB cho presence tracking (online/offline). Chỉ init nếu env có URL —
/// feature optional, app vẫn work nếu chưa setup RTDB.
export const rtdb: Database | null = import.meta.env.VITE_FIREBASE_DATABASE_URL
  ? getDatabase(firebaseApp)
  : null;
/// Cloud Functions client — region match với deploy (asia-southeast1 Singapore).
export const functions: Functions = getFunctions(firebaseApp, "asia-southeast1");
export const googleProvider = new GoogleAuthProvider();

void setPersistence(auth, browserLocalPersistence);

/** Lấy ID token của user hiện tại. Throw nếu chưa login — caller nên guard
 *  `auth.currentUser` trước khi gọi hoặc catch error. `forceRefresh=true`
 *  để bypass cache 1h (rare — dùng khi claim admin vừa toggle).
 *
 *  **Logging policy**: chỉ log khi forceRefresh=true. Lý do:
 *  - SDK cache 1h → 99% calls return từ memory ~1ms, KHÔNG hit network
 *  - Net_log mục đích show network activity → cached calls = noise (user
 *    thấy 3 getIdToken cùng giây tưởng bug, thực ra 0 request mạng)
 *  - Force refresh thực sự hit network (POST securetoken.googleapis.com)
 *    → log để observability + cost tracking
 */
export async function getAuthToken(forceRefresh = false): Promise<string> {
  const current = auth.currentUser;
  if (!current) throw new Error("Chưa đăng nhập");
  if (!forceRefresh) {
    // Cache hit path — không log (instant, không hit network).
    return current.getIdToken(false);
  }
  return timed(
    "firebase_token",
    "getIdToken(force)",
    () => current.getIdToken(true),
    { forceRefresh: "1" },
  );
}

// Dev-only: expose `auth` lên window cho DevTools debug. Remove sau khi xong.
if (import.meta.env.DEV) {
  (globalThis as unknown as { __auth: Auth }).__auth = auth;
}
