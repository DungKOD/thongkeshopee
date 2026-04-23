import type { Env, AuthContext } from '../types';
import { jsonError } from '../response';

/// Download DB — v8.1+ return raw zstd bytes (Content-Type application/octet-stream).
/// Metadata trong response headers:
///   - X-Size-Bytes: kích thước bytes
///   - X-Last-Modified-Ms: timestamp ms
///   - ETag: CAS guard cho upload tiếp theo
export async function downloadRoute(
  _req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  const key = `users/${auth.uid}/db.zst`;
  const obj = await env.DB_BUCKET.get(key);
  if (!obj) return jsonError(404, 'No backup found for user');

  const mtimeFromMeta = obj.customMetadata?.mtimeMs;
  const lastModified = mtimeFromMeta ? Number(mtimeFromMeta) : obj.uploaded.getTime();

  return new Response(obj.body, {
    status: 200,
    headers: {
      'Content-Type': 'application/octet-stream',
      'Content-Length': String(obj.size),
      'X-Size-Bytes': String(obj.size),
      'X-Last-Modified-Ms': String(lastModified),
      ETag: obj.etag,
    },
  });
}
