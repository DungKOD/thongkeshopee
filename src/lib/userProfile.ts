import {
  doc,
  getDoc,
  onSnapshot,
  serverTimestamp,
  setDoc,
  Timestamp,
  type Unsubscribe,
} from "firebase/firestore";
import { db } from "./firebase";

/**
 * Document shape lưu ở Firestore `/users/{uid}`.
 * Schema khớp với data đã có trong Firebase Console.
 *
 * - `premium` + `admin`: default false khi tạo doc; chỉ admin được set true qua
 *   Firebase Console (rule deny update từ client).
 * - `expiredAt`: null = không có hạn; nếu set Timestamp → premium hết hạn sau
 *   thời điểm đó.
 */
export interface UserProfile {
  premium: boolean;
  admin: boolean;
  email: string;
  createdAt: Timestamp | null;
  expiredAt: Timestamp | null;
}

const USERS_COLLECTION = "users";

/** Cache TTL — 7 ngày offline grace cho profile (nhiều hơn session vì profile ít đổi). */
const PROFILE_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const PROFILE_CACHE_KEY = (uid: string) => `userProfile:cache:${uid}`;

/**
 * Shape lưu cache trong localStorage. `expiredAtMs` lưu raw milliseconds vì
 * Firestore Timestamp không serialize qua JSON.
 */
interface CachedProfile {
  premium: boolean;
  admin: boolean;
  email: string;
  expiredAtMs: number | null;
  cachedAt: number;
}

export function getCachedUserProfile(uid: string): UserProfile | null {
  try {
    const raw = localStorage.getItem(PROFILE_CACHE_KEY(uid));
    if (!raw) return null;
    const cached = JSON.parse(raw) as CachedProfile;
    if (Date.now() - cached.cachedAt > PROFILE_CACHE_TTL_MS) {
      // TTL quá → coi như không có cache. Không xóa để debug, sẽ ghi đè khi
      // online lại.
      return null;
    }
    return {
      premium: cached.premium,
      admin: cached.admin,
      email: cached.email,
      // createdAt không cache (không dùng cho check premium).
      createdAt: null,
      expiredAt:
        cached.expiredAtMs !== null
          ? Timestamp.fromMillis(cached.expiredAtMs)
          : null,
    };
  } catch {
    return null;
  }
}

function writeCachedProfile(uid: string, profile: UserProfile): void {
  try {
    const payload: CachedProfile = {
      premium: profile.premium,
      admin: profile.admin,
      email: profile.email,
      expiredAtMs: profile.expiredAt?.toMillis() ?? null,
      cachedAt: Date.now(),
    };
    localStorage.setItem(PROFILE_CACHE_KEY(uid), JSON.stringify(payload));
  } catch {
    /* quota */
  }
}

export function clearCachedUserProfile(uid: string): void {
  try {
    localStorage.removeItem(PROFILE_CACHE_KEY(uid));
  } catch {
    /* ignore */
  }
}

/**
 * Premium đang active khi:
 * - `premium === true` VÀ
 * - `expiredAt === null` HOẶC `expiredAt` còn ở tương lai.
 *
 * Admin = true KHÔNG tự bypass premium check — admin vẫn cần premium=true để
 * vào app. Nếu muốn admin auto-bypass, sửa thành `profile.admin || (...)`.
 */
export function isPremiumActive(profile: UserProfile | null): boolean {
  if (!profile || !profile.premium) return false;
  if (!profile.expiredAt) return true;
  return profile.expiredAt.toMillis() > Date.now();
}

/**
 * Tạo doc `/users/{uid}` với default values (premium=false, admin=false) nếu
 * chưa tồn tại. Idempotent — gọi nhiều lần an toàn.
 *
 * Race với admin: nếu admin tạo doc trước (set premium=true), client gọi hàm
 * này sẽ thấy doc đã exist → return ngay, không overwrite.
 */
export async function ensureUserProfile(
  uid: string,
  email: string | null,
): Promise<void> {
  const ref = doc(db, USERS_COLLECTION, uid);
  const snap = await getDoc(ref);
  if (snap.exists()) return;
  await setDoc(ref, {
    premium: false,
    admin: false,
    email: email ?? "",
    createdAt: serverTimestamp(),
    expiredAt: null,
  });
}

/**
 * Subscribe Firestore doc `/users/{uid}` realtime.
 *
 * `onChange(null)` khi doc bị xóa hoặc chưa tồn tại — UI nên hiển thị
 * PremiumLockedScreen vì xem như không có quyền truy cập.
 *
 * Trả về unsubscribe function — caller phải gọi khi signOut hoặc unmount.
 */
export function watchUserProfile(
  uid: string,
  onChange: (profile: UserProfile | null) => void,
): Unsubscribe {
  const ref = doc(db, USERS_COLLECTION, uid);
  return onSnapshot(
    ref,
    (snap) => {
      if (!snap.exists()) {
        onChange(null);
        return;
      }
      const data = snap.data();
      const profile: UserProfile = {
        premium: data.premium === true,
        admin: data.admin === true,
        email: typeof data.email === "string" ? data.email : "",
        createdAt:
          data.createdAt instanceof Timestamp ? data.createdAt : null,
        expiredAt:
          data.expiredAt instanceof Timestamp ? data.expiredAt : null,
      };
      writeCachedProfile(uid, profile);
      onChange(profile);
    },
    (err) => {
      console.warn("[userProfile] snapshot error:", err);
    },
  );
}
