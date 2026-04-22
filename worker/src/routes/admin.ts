import type { Env, AuthContext } from '../types';
import { jsonOk, jsonError } from '../response';
import { base64Encode } from '../base64';
import { listAllUsers, emailLocalPart } from '../firestore';

/// List tất cả user trong Firestore + merge R2 file metadata nếu có.
///
/// Firestore là **nguồn chính** (profile source of truth) — hiện cả user
/// chưa migrate sang R2 (còn dùng Apps Script/Drive). R2 file metadata
/// (size, mtime) chỉ hiện nếu user đã upload DB qua Worker.
export async function adminUsersRoute(
  _req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  if (!auth.isAdmin) return jsonError(403, 'Admin only');

  // 1. List tất cả user từ Firestore.
  const profiles = await listAllUsers(auth.idToken, env);

  // 2. List R2 objects `users/*/db.gz` — build map uid → file meta.
  type FileMeta = {
    fileId: string;
    sizeBytes: number;
    lastModified: number;
  };
  const filesByUid = new Map<string, FileMeta>();
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
      filesByUid.set(m[1], {
        fileId: obj.key,
        sizeBytes: obj.size,
        lastModified: obj.customMetadata?.mtimeMs
          ? Number(obj.customMetadata.mtimeMs)
          : obj.uploaded.getTime(),
      });
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);

  // 3. Merge: Firestore primary. Thêm R2-orphan users (có file nhưng không có
  //    Firestore doc — hiếm, có thể do delete Firestore sau khi upload).
  const seenUids = new Set<string>();
  const users = profiles.map((p) => {
    seenUids.add(p.uid);
    return {
      uid: p.uid,
      email: p.email,
      localPart: emailLocalPart(p.email),
      premium: p.premium,
      admin: p.admin,
      expiredAt: p.expiredAt,
      createdAt: p.createdAt,
      file: filesByUid.get(p.uid) ?? null,
    };
  });
  for (const [uid, file] of filesByUid) {
    if (seenUids.has(uid)) continue;
    users.push({
      uid,
      email: null,
      localPart: null,
      premium: false,
      admin: false,
      expiredAt: null,
      createdAt: null,
      file,
    });
  }

  return jsonOk({ users });
}

/// Clean R2 orphan files — xoá `users/{uid}/db.gz` khi `{uid}` không tồn tại
/// trong Firestore `users` collection. Admin-only.
///
/// Use case: đổi Firebase project → UIDs cũ không còn trong Firestore mới
/// nhưng R2 vẫn giữ file. Gọi endpoint này dọn 1 phát.
export async function adminCleanupOrphansRoute(
  _req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  if (!auth.isAdmin) return jsonError(403, 'Admin only');

  const profiles = await listAllUsers(auth.idToken, env);
  const validUids = new Set(profiles.map((p) => p.uid));

  const deleted: string[] = [];
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
      if (validUids.has(uid)) continue;
      await env.DB_BUCKET.delete(obj.key);
      deleted.push(uid);
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);

  return jsonOk({ deleted });
}

/// Download DB của user target. Admin-only.
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
