import { jwtVerify, createRemoteJWKSet } from 'jose';
import type { Env, AuthContext } from './types';

const JWKS = createRemoteJWKSet(
  new URL('https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com'),
);

export function extractBearer(req: Request): string | null {
  const raw = req.headers.get('Authorization') ?? req.headers.get('authorization');
  if (!raw) return null;
  const m = /^Bearer\s+(.+)$/i.exec(raw);
  return m ? m[1].trim() : null;
}

/// Verify JWT + resolve auth context từ **custom claims** (KHÔNG fetch Firestore).
///
/// Claims (`admin`, `premium`, `expiredAt`) được sync tự động từ Firestore
/// `users/{uid}` qua Cloud Function `syncUserClaims`. Sau khi Firestore thay đổi:
///  1. Cloud Function fire trong 1-2s → setCustomUserClaims.
///  2. Client force refresh token (useUserProfile tự handle qua getIdToken(true))
///     hoặc đợi Firebase auto refresh 1h.
///  3. Token mới carry claims → Worker đọc ngay, không cần Firestore.
///
/// Trade-off: admin toggle premium cho user đang online → user phải refresh
/// token (auto sau 3s qua UI, hoặc manual logout/login) → Worker thấy quyền mới.
///
/// Fallback `ADMIN_UIDS` env: emergency bootstrap nếu Cloud Function bị disable
/// (budget cap) hoặc claim chưa sync — tránh locked out admin.
export async function verifyFirebaseToken(token: string, env: Env): Promise<AuthContext> {
  const { payload } = await jwtVerify(token, JWKS, {
    issuer: `https://securetoken.google.com/${env.FIREBASE_PROJECT_ID}`,
    audience: env.FIREBASE_PROJECT_ID,
  });

  const uid = typeof payload.sub === 'string' ? payload.sub : '';
  if (!uid) throw new Error('Token missing sub claim');

  const email = typeof payload.email === 'string' ? payload.email : null;
  const claimAdmin = payload.admin === true;
  const claimPremium = payload.premium === true;
  const claimExpiredMs =
    typeof payload.expiredAt === 'number' ? payload.expiredAt : null;

  const fallbackAdminUids = (env.ADMIN_UIDS ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  const isAdmin = claimAdmin || fallbackAdminUids.includes(uid);

  return {
    uid,
    email,
    idToken: token,
    isAdmin,
    premium: claimPremium,
    expiredAt: claimExpiredMs ? new Date(claimExpiredMs).toISOString() : null,
    // createdAt không có trong claim — auth context không dùng, chỉ admin list
    // users cần (vẫn fetch Firestore riêng trong adminUsersRoute).
    createdAt: null,
  };
}
