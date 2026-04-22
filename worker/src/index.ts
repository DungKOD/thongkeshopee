import type { Env } from './types';
import { extractBearer, verifyFirebaseToken } from './auth';
import { jsonError } from './response';
import { metadataRoute } from './routes/metadata';
import { uploadRoute } from './routes/upload';
import { downloadRoute } from './routes/download';
import { adminUsersRoute, adminDownloadRoute } from './routes/admin';

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname.replace(/\/+$/, '') || '/';

    if (req.method === 'GET' && path === '/health') {
      return new Response('ok', { status: 200 });
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
      if (req.method === 'POST' && path === '/metadata') return metadataRoute(req, auth, env);
      if (req.method === 'POST' && path === '/upload') return uploadRoute(req, auth, env);
      if (req.method === 'POST' && path === '/download') return downloadRoute(req, auth, env);
      if (req.method === 'POST' && path === '/admin/users') return adminUsersRoute(req, auth, env);
      if (req.method === 'GET' && path === '/admin/download') return adminDownloadRoute(req, auth, env);
      return jsonError(404, `No route: ${req.method} ${path}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Internal error';
      return jsonError(500, msg);
    }
  },
} satisfies ExportedHandler<Env>;
