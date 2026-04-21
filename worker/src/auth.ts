import { jwtVerify, createRemoteJWKSet } from 'jose';
import type { Env, AuthContext } from './types';
import { fetchUserProfile } from './firestore';

const JWKS = createRemoteJWKSet(
  new URL('https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com'),
);

export function extractBearer(req: Request): string | null {
  const raw = req.headers.get('Authorization') ?? req.headers.get('authorization');
  if (!raw) return null;
  const m = /^Bearer\s+(.+)$/i.exec(raw);
  return m ? m[1].trim() : null;
}

/// Verify JWT + fetch profile từ Firestore để resolve admin/premium role.
/// - Firestore `users/{uid}` là single source of truth cho admin/premium.
/// - ADMIN_UIDS secret chỉ dùng fallback nếu Firestore fetch fail (rules deny,
///   network error...) — emergency access không bị locked out.
export async function verifyFirebaseToken(token: string, env: Env): Promise<AuthContext> {
  const { payload } = await jwtVerify(token, JWKS, {
    issuer: `https://securetoken.google.com/${env.FIREBASE_PROJECT_ID}`,
    audience: env.FIREBASE_PROJECT_ID,
  });

  const uid = typeof payload.sub === 'string' ? payload.sub : '';
  if (!uid) throw new Error('Token missing sub claim');

  const emailFromToken = typeof payload.email === 'string' ? payload.email : null;

  // Primary: Firestore profile. Null = doc chưa tạo hoặc rules deny.
  const profile = await fetchUserProfile(uid, token, env);

  // Fallback: ADMIN_UIDS secret (legacy bootstrap). OR-merge với Firestore —
  // nếu profile null (fetch fail) vẫn cho phép admin bootstrap qua secret.
  const fallbackAdminUids = (env.ADMIN_UIDS ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  const isAdmin = profile?.admin === true || fallbackAdminUids.includes(uid);

  return {
    uid,
    email: profile?.email ?? emailFromToken,
    idToken: token,
    isAdmin,
    premium: profile?.premium === true,
    expiredAt: profile?.expiredAt ?? null,
    createdAt: profile?.createdAt ?? null,
  };
}
