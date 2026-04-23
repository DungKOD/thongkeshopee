import { onDocumentWritten } from "firebase-functions/v2/firestore";
import { onCall, HttpsError } from "firebase-functions/v2/https";
import { setGlobalOptions, logger } from "firebase-functions/v2";
import { initializeApp } from "firebase-admin/app";
import { getAuth } from "firebase-admin/auth";
import { getFirestore } from "firebase-admin/firestore";

initializeApp();

// Giới hạn concurrency toàn project — defense-in-depth chống runaway loop.
setGlobalOptions({
  region: "asia-southeast1",
  maxInstances: 5,
});

// Convert Firestore Timestamp / Date / number → epoch ms. Null-safe.
function toMillis(v: unknown): number | null {
  if (v == null) return null;
  if (typeof v === "number") return v;
  if (typeof v === "object" && "toMillis" in v && typeof (v as { toMillis: () => number }).toMillis === "function") {
    return (v as { toMillis: () => number }).toMillis();
  }
  if (v instanceof Date) return v.getTime();
  return null;
}

// Sync Firestore `users/{uid}` → Firebase Auth custom claims.
// Fields synced: { admin: boolean, premium: boolean, expiredAt: number|null }
//
// Trigger: onDocumentWritten (cover create + update + delete).
// Action: setCustomUserClaims — KHÔNG ghi lại Firestore → no trigger loop.
//
// Guards (defense-in-depth):
//   1. Skip nếu admin/premium/expiredAt không đổi (bỏ qua update không liên quan).
//   2. maxInstances: 5 → concurrency cap cho Cloud Functions.
//   3. timeoutSeconds: 30 → function tự chết nếu treo.
//
// Worker + RTDB rules đọc claims từ ID token → không cần Firestore reads nữa cho auth.
// User phải refresh ID token (logout/login hoặc getIdToken(true)) sau khi claim đổi.
export const syncUserClaims = onDocumentWritten(
  {
    document: "users/{uid}",
    timeoutSeconds: 30,
    memory: "256MiB",
    maxInstances: 5,
  },
  async (event) => {
    const uid = event.params.uid;
    const before = event.data?.before?.data();
    const after = event.data?.after?.data();

    const beforeAdmin = before?.admin === true;
    const afterAdmin = after?.admin === true;
    const beforePremium = before?.premium === true;
    const afterPremium = after?.premium === true;
    const beforeExpiredMs = toMillis(before?.expiredAt);
    const afterExpiredMs = toMillis(after?.expiredAt);

    // Guard — skip nếu không có change nào relevant cho claims.
    if (
      beforeAdmin === afterAdmin &&
      beforePremium === afterPremium &&
      beforeExpiredMs === afterExpiredMs
    ) {
      logger.info(
        `[syncUserClaims] uid=${uid} no claim-relevant change, skip`,
      );
      return;
    }

    try {
      const user = await getAuth().getUser(uid);
      const currentClaims = user.customClaims ?? {};
      const newClaims = {
        ...currentClaims,
        admin: afterAdmin,
        premium: afterPremium,
        expiredAt: afterExpiredMs,
      };
      await getAuth().setCustomUserClaims(uid, newClaims);

      logger.info(
        `[syncUserClaims] uid=${uid} synced: admin=${afterAdmin} premium=${afterPremium} expiredAt=${afterExpiredMs}`,
      );
    } catch (e) {
      logger.error(`[syncUserClaims] uid=${uid} failed:`, e);
      throw e;
    }
  },
);

// Rate-limit password reset requests per-email (5 lần / 24h rolling window).
//
// Firebase Auth built-in protection chỉ rate-limit theo IP (~100 req/hour).
// User có thể spam chính họ (VD: quên pass, liên tục "quên") → Firebase gửi 5-10
// email chưa chắc block. Function này giữ counter per-email trong Firestore,
// reject khi vượt quota.
//
// Flow: FE gọi function trước → allowed=true thì FE tiếp tục gọi
// `sendPasswordResetEmail` (client SDK). Nếu function reject → throw error.
//
// Bypass risk: user tech có thể call `sendPasswordResetEmail` trực tiếp qua
// DevTools → skip counter. Nhưng Firebase IP rate-limit vẫn block.
// Mục tiêu là UX limit, không phải adversarial security.
export const requestPasswordReset = onCall(
  {
    timeoutSeconds: 10,
    memory: "256MiB",
    maxInstances: 5,
  },
  async (request) => {
    const rawEmail = request.data?.email;
    if (typeof rawEmail !== "string") {
      throw new HttpsError("invalid-argument", "Thiếu email");
    }
    const email = rawEmail.trim().toLowerCase();
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      throw new HttpsError("invalid-argument", "Email không hợp lệ");
    }

    const MAX_PER_DAY = 5;
    const WINDOW_MS = 24 * 60 * 60 * 1000;
    const COOLDOWN_MS = 60 * 1000;
    const now = Date.now();

    const db = getFirestore();
    const docRef = db.collection("passwordResetQuota").doc(email);

    // Transaction: check cooldown + daily quota. Atomic → concurrent calls từ
    // cùng email serialize. Block reason phân biệt "cooldown" (1 phút giữa 2
    // lần) và "daily" (5 lần / 24h).
    const result = await db.runTransaction(async (tx) => {
      const snap = await tx.get(docRef);
      const raw = snap.exists ? ((snap.data()?.attempts as number[]) ?? []) : [];
      const active = raw.filter((t) => now - t < WINDOW_MS);

      // 1. Cooldown 1 phút giữa 2 lần liên tiếp.
      if (active.length > 0) {
        const last = active[active.length - 1];
        if (now - last < COOLDOWN_MS) {
          return {
            blocked: true,
            reason: "cooldown" as const,
            retryAtMs: last + COOLDOWN_MS,
            count: active.length,
          };
        }
      }

      // 2. Quota 5 lần / 24h rolling.
      if (active.length >= MAX_PER_DAY) {
        const oldest = active[0];
        return {
          blocked: true,
          reason: "daily" as const,
          retryAtMs: oldest + WINDOW_MS,
          count: active.length,
        };
      }

      active.push(now);
      tx.set(docRef, {
        attempts: active,
        lastEmail: email,
        updatedAt: now,
      });
      return {
        blocked: false as const,
        reason: "ok" as const,
        retryAtMs: 0,
        count: active.length,
      };
    });

    if (result.blocked) {
      const remainMs = result.retryAtMs - now;
      if (result.reason === "cooldown") {
        const secs = Math.max(1, Math.ceil(remainMs / 1000));
        throw new HttpsError(
          "resource-exhausted",
          `Vui lòng đợi ${secs} giây trước khi yêu cầu lại (giới hạn 1 phút/lần).`,
        );
      }
      // reason === "daily"
      const mins = Math.ceil(remainMs / 60_000);
      const hours = Math.floor(mins / 60);
      const timeStr =
        hours > 0
          ? `~${hours}h${mins % 60 > 0 ? ` ${mins % 60}p` : ""}`
          : `~${mins} phút`;
      throw new HttpsError(
        "resource-exhausted",
        `Đã yêu cầu đặt lại mật khẩu ${result.count} lần trong 24h qua (giới hạn ${MAX_PER_DAY}). Thử lại sau ${timeStr}.`,
      );
    }

    logger.info(
      `[requestPasswordReset] email=${email} allowed (${result.count}/${MAX_PER_DAY})`,
    );
    return {
      allowed: true,
      remaining: MAX_PER_DAY - result.count,
      maxPerDay: MAX_PER_DAY,
    };
  },
);

// Callable function để admin ép sync claim cho 1 uid. Use cases:
//  - Bootstrap admin đầu tiên trước khi Cloud Function deploy.
//  - Resync claim khi bị drift (script ngoài luồng set claim).
export const forceSyncUserClaims = onCall(
  {
    timeoutSeconds: 30,
    memory: "256MiB",
    maxInstances: 3,
  },
  async (request) => {
    const callerUid = request.auth?.uid;
    if (!callerUid) {
      throw new HttpsError("unauthenticated", "Chưa đăng nhập");
    }
    const targetUid = request.data?.uid;
    if (typeof targetUid !== "string" || !targetUid) {
      throw new HttpsError("invalid-argument", "Thiếu uid");
    }

    // Check caller là admin qua Firestore (KHÔNG qua claim — vì bootstrap
    // trường hợp claim chưa có).
    const db = getFirestore();
    const callerDoc = await db.collection("users").doc(callerUid).get();
    const callerIsAdmin = callerDoc.exists && callerDoc.data()?.admin === true;

    if (!callerIsAdmin) {
      throw new HttpsError(
        "permission-denied",
        "Chỉ admin (Firestore users.admin=true) mới được gọi",
      );
    }

    const targetDoc = await db.collection("users").doc(targetUid).get();
    const data = targetDoc.data();
    const targetAdmin = data?.admin === true;
    const targetPremium = data?.premium === true;
    const targetExpiredMs = toMillis(data?.expiredAt);

    try {
      const user = await getAuth().getUser(targetUid);
      const currentClaims = user.customClaims ?? {};
      const newClaims = {
        ...currentClaims,
        admin: targetAdmin,
        premium: targetPremium,
        expiredAt: targetExpiredMs,
      };
      await getAuth().setCustomUserClaims(targetUid, newClaims);

      logger.info(
        `[forceSyncUserClaims] caller=${callerUid} target=${targetUid} synced`,
      );
      return {
        success: true,
        admin: targetAdmin,
        premium: targetPremium,
        expiredAt: targetExpiredMs,
      };
    } catch (e) {
      logger.error(
        `[forceSyncUserClaims] caller=${callerUid} target=${targetUid} failed:`,
        e,
      );
      throw new HttpsError("internal", (e as Error).message);
    }
  },
);
