import { initializeApp, type FirebaseApp } from "firebase/app";
import {
  getAuth,
  type Auth,
  GoogleAuthProvider,
  browserLocalPersistence,
  setPersistence,
} from "firebase/auth";
import { getFirestore, type Firestore } from "firebase/firestore";
import { getDatabase, type Database } from "firebase/database";

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
export const db: Firestore = getFirestore(firebaseApp);
/// RTDB cho presence tracking (online/offline). Chỉ init nếu env có URL —
/// feature optional, app vẫn work nếu chưa setup RTDB.
export const rtdb: Database | null = import.meta.env.VITE_FIREBASE_DATABASE_URL
  ? getDatabase(firebaseApp)
  : null;
export const googleProvider = new GoogleAuthProvider();

void setPersistence(auth, browserLocalPersistence);

// Dev-only: expose `auth` lên window cho DevTools debug. Remove sau khi xong.
if (import.meta.env.DEV) {
  (globalThis as unknown as { __auth: Auth }).__auth = auth;
}
