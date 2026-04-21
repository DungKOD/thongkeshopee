import type { Env, AuthContext } from '../types';
import { jsonOk, jsonError } from '../response';
import { base64Encode } from '../base64';

// Phase 2: cần nguồn user profile (email, premium, admin role, expiredAt).
// Hiện Apps Script đọc từ Firestore + một config sheet. Options cho Worker:
//   - KV binding `USER_PROFILES` — admin tự maintain qua script/dashboard
//   - Firestore REST từ Worker (service account signed JWT)
//   - Giữ Apps Script chỉ cho `listUsers`, R2 Worker lo file sync
// Stub tạm thời: list chỉ file metadata, profile fields null.
export async function adminUsersRoute(
  _req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  if (!auth.isAdmin) return jsonError(403, 'Admin only');

  const users: Array<Record<string, unknown>> = [];
  let cursor: string | undefined;
  do {
    const page = await env.DB_BUCKET.list({
      prefix: 'users/',
      cursor,
      limit: 1000,
    });
    for (const obj of page.objects) {
      const m = /^users\/([^/]+)\/db\.gz$/.exec(obj.key);
      if (!m) continue;
      const uid = m[1];
      users.push({
        uid,
        email: null,
        localPart: null,
        premium: false,
        admin: false,
        expiredAt: null,
        createdAt: null,
        file: {
          fileId: obj.key,
          sizeBytes: obj.size,
          lastModified: obj.customMetadata?.mtimeMs
            ? Number(obj.customMetadata.mtimeMs)
            : obj.uploaded.getTime(),
        },
      });
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);

  return jsonOk({ users });
}

export async function adminDownloadRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  if (!auth.isAdmin) return jsonError(403, 'Admin only');

  const url = new URL(req.url);
  const targetUid = url.searchParams.get('uid');
  if (!targetUid) return jsonError(400, 'Missing uid query param');

  const key = `users/${targetUid}/db.gz`;
  const obj = await env.DB_BUCKET.get(key);
  if (!obj) return jsonError(404, 'Target user has no backup');

  const bytes = new Uint8Array(await obj.arrayBuffer());
  const mtimeFromMeta = obj.customMetadata?.mtimeMs;
  const lastModified = mtimeFromMeta ? Number(mtimeFromMeta) : obj.uploaded.getTime();

  return jsonOk({
    base64Data: base64Encode(bytes),
    sizeBytes: bytes.byteLength,
    lastModified,
  });
}
