import type { Env, AuthContext } from '../types';
import { jsonOk } from '../response';

export async function metadataRoute(
  _req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  const key = `users/${auth.uid}/db.gz`;
  const head = await env.DB_BUCKET.head(key);

  if (!head) {
    return jsonOk({
      exists: false,
      fileId: null,
      sizeBytes: null,
      lastModified: null,
      fingerprint: null,
    });
  }

  const mtimeFromMeta = head.customMetadata?.mtimeMs;
  const lastModified = mtimeFromMeta ? Number(mtimeFromMeta) : head.uploaded.getTime();
  const fingerprint = head.customMetadata?.fingerprint ?? null;

  return jsonOk({
    exists: true,
    fileId: key,
    sizeBytes: head.size,
    lastModified,
    fingerprint,
  });
}
