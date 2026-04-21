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

export async function verifyFirebaseToken(token: string, env: Env): Promise<AuthContext> {
  const { payload } = await jwtVerify(token, JWKS, {
    issuer: `https://securetoken.google.com/${env.FIREBASE_PROJECT_ID}`,
    audience: env.FIREBASE_PROJECT_ID,
  });

  const uid = typeof payload.sub === 'string' ? payload.sub : '';
  if (!uid) throw new Error('Token missing sub claim');

  const email = typeof payload.email === 'string' ? payload.email : null;
  const adminUids = (env.ADMIN_UIDS ?? '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  const isAdmin = adminUids.includes(uid);

  return { uid, email, isAdmin };
}
