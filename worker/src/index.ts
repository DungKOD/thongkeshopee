import type { Env } from './types';
import { extractBearer, verifyFirebaseToken } from './auth';
import { jsonError } from './response';
import {
  manifestGetRoute,
  manifestPutRoute,
  deltaUploadRoute,
  deltaFetchRoute,
  snapshotUploadRoute,
  snapshotFetchRoute,
  syncLogPushRoute,
} from './routes/v9';
import {
  adminUsersRoute,
  adminSnapshotFetchRoute,
  adminManifestFetchRoute,
  adminDeltaFetchRoute,
  adminCleanupRoute,
  adminSyncLogRoute,
  adminSyncLogFileRoute,
  adminRestoreRoute,
} from './routes/v9_admin';

/**
 * Sync v9 Worker — per-table incremental delta sync.
 *
 * v8 endpoints (/metadata, /upload, /download, /admin/users, /admin/download,
 * /admin/cleanup-orphans) đã removed (plan nguyên tắc #4). App chưa có user
 * thật → không dual-write.
 */
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname.replace(/\/+$/, '') || '/';

    if (req.method === 'GET' && path === '/health') {
      return new Response('ok v9', { status: 200 });
    }

    const token = extractBearer(req);
    if (!token) return jsonError(401, 'Missing Authorization header');

    let auth;
    try {
      auth = await verifyFirebaseToken(token, env);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Token verification failed';
      return jsonError(401, msg);
    }

    try {
      // User-scope endpoints — auth.uid tự động qua path.
      if (req.method === 'POST' && path === '/v9/manifest/get') return manifestGetRoute(req, auth, env);
      if (req.method === 'POST' && path === '/v9/manifest/put') return manifestPutRoute(req, auth, env);
      if (req.method === 'POST' && path === '/v9/delta/upload') return deltaUploadRoute(req, auth, env);
      if (req.method === 'GET' && path === '/v9/delta/fetch') return deltaFetchRoute(req, auth, env);
      if (req.method === 'POST' && path === '/v9/snapshot/upload') return snapshotUploadRoute(req, auth, env);
      if (req.method === 'GET' && path === '/v9/snapshot/fetch') return snapshotFetchRoute(req, auth, env);
      if (req.method === 'POST' && path === '/v9/sync-log/push') return syncLogPushRoute(req, auth, env);

      // Admin-scope endpoints — auth.isAdmin check inside.
      if (req.method === 'POST' && path === '/v9/admin/users') return adminUsersRoute(req, auth, env);
      if (req.method === 'GET' && path === '/v9/admin/snapshot') return adminSnapshotFetchRoute(req, auth, env);
      if (req.method === 'GET' && path === '/v9/admin/manifest') return adminManifestFetchRoute(req, auth, env);
      if (req.method === 'GET' && path === '/v9/admin/delta-fetch') return adminDeltaFetchRoute(req, auth, env);
      if (req.method === 'POST' && path === '/v9/admin/cleanup') return adminCleanupRoute(req, auth, env);
      if (req.method === 'GET' && path === '/v9/admin/sync-log') return adminSyncLogRoute(req, auth, env);
      if (req.method === 'GET' && path === '/v9/admin/sync-log-file') return adminSyncLogFileRoute(req, auth, env);
      if (req.method === 'POST' && path === '/v9/admin/restore') return adminRestoreRoute(req, auth, env);

      return jsonError(404, `No route: ${req.method} ${path}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Internal error';
      return jsonError(500, msg);
    }
  },
} satisfies ExportedHandler<Env>;
