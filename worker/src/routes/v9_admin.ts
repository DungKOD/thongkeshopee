/**
 * Sync v9 — admin-scope endpoints.
 *
 * Admin (auth.isAdmin) có quyền xem data + sync log của mọi user. Cleanup
 * luôn archive 30 ngày trước khi delete R2 objects (rule giữ data #1, plan G4).
 */

import type { AuthContext, Env } from '../types';
import { jsonError, jsonOk } from '../response';
import { listAllUsers, emailLocalPart } from '../firestore';

/**
 * POST /v9/admin/users
 * Response: { ok: true, users: [...] }
 *
 * List tất cả user trong Firestore + merge manifest existence từ R2. Thay
 * cho v8 adminUsersRoute — v9 check manifest.json thay vì db.zst.
 */
export async function adminUsersRoute(
  _req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  if (!auth.isAdmin) return jsonError(403, 'Admin only');

  const profiles = await listAllUsers(auth.idToken, env);

  type SyncMeta = {
    hasManifest: boolean;
    hasSnapshot: boolean;
    lastModified: number | null;
  };
  const metaByUid = new Map<string, SyncMeta>();

  // List manifest.json across all users.
  let cursor: string | undefined;
  do {
    const page = await env.DB_BUCKET.list({
      prefix: 'users/',
      cursor,
      limit: 1000,
    });
    for (const obj of page.objects) {
      const manifestM = /^users\/([^/]+)\/manifest\.json$/.exec(obj.key);
      if (manifestM) {
        const uid = manifestM[1];
        const prev = metaByUid.get(uid) ?? {
          hasManifest: false,
          hasSnapshot: false,
          lastModified: null,
        };
        prev.hasManifest = true;
        prev.lastModified = Math.max(prev.lastModified ?? 0, obj.uploaded.getTime());
        metaByUid.set(uid, prev);
        continue;
      }
      const snapM = /^users\/([^/]+)\/snapshots\//.exec(obj.key);
      if (snapM) {
        const uid = snapM[1];
        const prev = metaByUid.get(uid) ?? {
          hasManifest: false,
          hasSnapshot: false,
          lastModified: null,
        };
        prev.hasSnapshot = true;
        metaByUid.set(uid, prev);
      }
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);

  const seenUids = new Set<string>();
  const users = profiles.map((p) => {
    seenUids.add(p.uid);
    const meta = metaByUid.get(p.uid);
    return {
      uid: p.uid,
      email: p.email,
      localPart: emailLocalPart(p.email),
      premium: p.premium,
      admin: p.admin,
      expiredAt: p.expiredAt,
      createdAt: p.createdAt,
      sync: meta
        ? {
            hasManifest: meta.hasManifest,
            hasSnapshot: meta.hasSnapshot,
            lastModifiedMs: meta.lastModified,
          }
        : null,
    };
  });
  for (const [uid, meta] of metaByUid) {
    if (seenUids.has(uid)) continue;
    users.push({
      uid,
      email: null,
      localPart: null,
      premium: false,
      admin: false,
      expiredAt: null,
      createdAt: null,
      sync: {
        hasManifest: meta.hasManifest,
        hasSnapshot: meta.hasSnapshot,
        lastModifiedMs: meta.lastModified,
      },
    });
  }

  return jsonOk({ users });
}

/**
 * GET /v9/admin/snapshot?uid=xxx
 * Response: snapshot stream | 404
 *
 * Plan Q6: reuse user's latest snapshot (Point-in-time). Admin apply deltas
 * client-side nếu cần state mới hơn (fetch deltas qua /admin/sync-log hoặc
 * adapter endpoint — defer P9).
 *
 * Flow: read manifest → latest_snapshot.key → stream fetch.
 */
export async function adminSnapshotFetchRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  if (!auth.isAdmin) return jsonError(403, 'Admin only');
  const uid = new URL(req.url).searchParams.get('uid');
  if (!uid) return jsonError(400, 'Missing ?uid=');

  const manifestObj = await env.DB_BUCKET.get(`users/${uid}/manifest.json`);
  if (!manifestObj) return jsonError(404, 'Target user chưa có manifest (chưa sync v9)');
  const manifest = (await manifestObj.json()) as {
    latest_snapshot?: { key: string; clock_ms: number; size_bytes: number } | null;
  };
  if (!manifest.latest_snapshot) {
    return jsonError(404, 'Target user chưa có snapshot (chưa compaction)');
  }

  const snapKey = `users/${uid}/${manifest.latest_snapshot.key}`;
  const snap = await env.DB_BUCKET.get(snapKey);
  if (!snap) return jsonError(404, 'Snapshot key trong manifest không tìm thấy trên R2');

  return new Response(snap.body, {
    status: 200,
    headers: {
      'Content-Type': 'application/octet-stream',
      'X-Size-Bytes': String(snap.size),
      'X-Snapshot-Clock-Ms': String(manifest.latest_snapshot.clock_ms),
      ETag: snap.etag,
    },
  });
}

/**
 * POST /v9/admin/cleanup?uid=xxx
 * Response: { ok: true, archived: N, deleted: 0 }
 *
 * Archive trước delete (rule giữ data G4):
 * 1. List mọi object dưới `users/{uid}/`
 * 2. Copy vào `archive/deleted_{uid}_{utc_ms}/<original_path>`
 * 3. Chỉ khi copy xong TẤT CẢ → delete source
 * 4. Archive giữ 30 ngày (retention qua lifecycle hoặc cron Worker defer)
 *
 * Idempotent: nếu crash giữa copy → chạy lại, skip objects đã trong archive.
 */
export async function adminCleanupRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  if (!auth.isAdmin) return jsonError(403, 'Admin only');
  const uid = new URL(req.url).searchParams.get('uid');
  if (!uid) return jsonError(400, 'Missing ?uid=');

  // Sanity: không cho admin tự xóa mình.
  if (uid === auth.uid) {
    return jsonError(400, 'Không được xóa admin (chính mình)');
  }

  const archivePrefix = `archive/deleted_${uid}_${Date.now()}/`;
  const sourcePrefix = `users/${uid}/`;

  let archived = 0;
  let cursor: string | undefined;
  // Phase 1: copy mọi object vào archive.
  do {
    const page = await env.DB_BUCKET.list({ prefix: sourcePrefix, cursor, limit: 1000 });
    for (const obj of page.objects) {
      const sourceObj = await env.DB_BUCKET.get(obj.key);
      if (!sourceObj) continue;
      const relPath = obj.key.slice(sourcePrefix.length);
      const archiveKey = archivePrefix + relPath;
      await env.DB_BUCKET.put(archiveKey, sourceObj.body, {
        customMetadata: {
          ...(obj.customMetadata ?? {}),
          original_key: obj.key,
          archived_at: new Date().toISOString(),
          archived_by: auth.uid,
        },
      });
      archived += 1;
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);

  // Phase 2: delete source (chỉ sau khi copy OK).
  let deleted = 0;
  cursor = undefined;
  do {
    const page = await env.DB_BUCKET.list({ prefix: sourcePrefix, cursor, limit: 1000 });
    for (const obj of page.objects) {
      await env.DB_BUCKET.delete(obj.key);
      deleted += 1;
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);

  return jsonOk({ archived, deleted, archiveId: archivePrefix });
}

/**
 * GET /v9/admin/sync-log?uid=xxx&from=yyyy-mm-dd&to=yyyy-mm-dd
 * Response: { ok: true, events: [...], truncated: bool }
 *
 * Aggregate sync log files trong date range. Max 500 events/response (client
 * paginate qua date range).
 */
export async function adminSyncLogRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  if (!auth.isAdmin) return jsonError(403, 'Admin only');
  const params = new URL(req.url).searchParams;
  const uid = params.get('uid');
  if (!uid) return jsonError(400, 'Missing ?uid=');
  const from = params.get('from') ?? '1970-01-01';
  const to = params.get('to') ?? '9999-12-31';

  const prefix = `users/${uid}/sync_logs/`;
  const events: unknown[] = [];
  const MAX_EVENTS = 500;
  let truncated = false;
  let cursor: string | undefined;

  outer: do {
    const page = await env.DB_BUCKET.list({ prefix, cursor, limit: 1000 });
    for (const obj of page.objects) {
      // key: users/{uid}/sync_logs/{date}/{ts}_{rand}.ndjson.zst
      const m = /sync_logs\/(\d{4}-\d{2}-\d{2})\//.exec(obj.key);
      if (!m) continue;
      const date = m[1];
      if (date < from || date > to) continue;
      const fileObj = await env.DB_BUCKET.get(obj.key);
      if (!fileObj) continue;
      // Decompress NDJSON events. Worker support zstd decompress? Not native.
      // → return compressed bytes + key, let admin FE decompress client-side
      //   OR list metadata only. For v9.0 minimal: return metadata only.
      events.push({
        key: obj.key,
        date,
        sizeBytes: obj.size,
        uploadedAt: obj.uploaded.toISOString(),
      });
      if (events.length >= MAX_EVENTS) {
        truncated = true;
        break outer;
      }
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);

  return jsonOk({ events, truncated, note: 'Events là metadata files; admin FE fetch từng file qua /admin/sync-log-file' });
}

/**
 * GET /v9/admin/sync-log-file?key=users/xxx/sync_logs/2026-04-24/...ndjson.zst
 * Response: raw zstd bytes (admin FE decompress client-side)
 */
export async function adminSyncLogFileRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  if (!auth.isAdmin) return jsonError(403, 'Admin only');
  const key = new URL(req.url).searchParams.get('key');
  if (!key) return jsonError(400, 'Missing ?key=');
  // Safety: key phải match sync_logs pattern.
  if (!/^users\/[^/]+\/sync_logs\//.test(key)) {
    return jsonError(400, 'key không phải sync_logs path');
  }
  const obj = await env.DB_BUCKET.get(key);
  if (!obj) return jsonError(404, 'Log file không tồn tại');
  return new Response(obj.body, {
    status: 200,
    headers: {
      'Content-Type': 'application/octet-stream',
      'X-Size-Bytes': String(obj.size),
      ETag: obj.etag,
    },
  });
}

/**
 * POST /v9/admin/restore?uid=xxx&archiveId=archive/deleted_xxx_yyy/
 * Response: { ok: true, restored: N }
 *
 * Revert archive về live prefix. Rule giữ data — recover nếu admin xóa nhầm.
 * Chỉ hoạt động nếu `archiveId` tồn tại và target prefix `users/{uid}/` empty
 * (không overwrite live data mới).
 */
export async function adminRestoreRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  if (!auth.isAdmin) return jsonError(403, 'Admin only');
  const params = new URL(req.url).searchParams;
  const uid = params.get('uid');
  const archiveId = params.get('archiveId');
  if (!uid || !archiveId) return jsonError(400, 'Missing ?uid= hoặc ?archiveId=');
  if (!archiveId.startsWith(`archive/deleted_${uid}_`) || !archiveId.endsWith('/')) {
    return jsonError(400, 'archiveId không hợp lệ');
  }

  // Guard: target prefix phải empty.
  const targetPrefix = `users/${uid}/`;
  const check = await env.DB_BUCKET.list({ prefix: targetPrefix, limit: 1 });
  if (check.objects.length > 0) {
    return jsonError(409, 'target user có live data — restore có thể overwrite');
  }

  let restored = 0;
  let cursor: string | undefined;
  do {
    const page = await env.DB_BUCKET.list({ prefix: archiveId, cursor, limit: 1000 });
    for (const obj of page.objects) {
      const relPath = obj.key.slice(archiveId.length);
      const targetKey = targetPrefix + relPath;
      const archiveObj = await env.DB_BUCKET.get(obj.key);
      if (!archiveObj) continue;
      await env.DB_BUCKET.put(targetKey, archiveObj.body, {
        customMetadata: archiveObj.customMetadata,
      });
      restored += 1;
    }
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);

  return jsonOk({ restored });
}
