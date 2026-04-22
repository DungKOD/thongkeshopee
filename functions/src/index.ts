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
