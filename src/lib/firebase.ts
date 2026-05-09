import { initializeApp, type FirebaseApp } from "firebase/app";
import {
  getAuth,
  type Auth,
  GoogleAuthProvider,
  browserLocalPersistence,
  setPersistence,
} from "firebase/auth";

const firebaseConfig = {
  apiKey: import.meta.env.VITE_FIREBASE_API_KEY,
  authDomain: import.meta.env.VITE_FIREBASE_AUTH_DOMAIN,
  projectId: import.meta.env.VITE_FIREBASE_PROJECT_ID,
  storageBucket: import.meta.env.VITE_FIREBASE_STORAGE_BUCKET,
  messagingSenderId: import.meta.env.VITE_FIREBASE_MESSAGING_SENDER_ID,
  appId: import.meta.env.VITE_FIREBASE_APP_ID,
};

export const firebaseApp: FirebaseApp = initializeApp(firebaseConfig);
export const auth: Auth = getAuth(firebaseApp);
export const googleProvider = new GoogleAuthProvider();

void setPersistence(auth, browserLocalPersistence);

/** Lấy ID token của user hiện tại. Throw nếu chưa login. */
export async function getAuthToken(forceRefresh = false): Promise<string> {
  const current = auth.currentUser;
  if (!current) throw new Error("Chưa đăng nhập");
  return current.getIdToken(forceRefresh);
}

if (import.meta.env.DEV) {
  (globalThis as unknown as { __auth: Auth }).__auth = auth;
}
