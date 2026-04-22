import type { Env, AuthContext } from '../types';
import { jsonOk, jsonError } from '../response';
import { base64Encode } from '../base64';

export async function downloadRoute(
  _req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  const key = `users/${auth.uid}/db.gz`;
  const obj = await env.DB_BUCKET.get(key);
  if (!obj) return jsonError(404, 'No backup found for user');

  const bytes = new Uint8Array(await obj.arrayBuffer());
  const mtimeFromMeta = obj.customMetadata?.mtimeMs;
  const lastModified = mtimeFromMeta ? Number(mtimeFromMeta) : obj.uploaded.getTime();

  return jsonOk({
    base64Data: base64Encode(bytes),
    sizeBytes: bytes.byteLength,
    lastModified,
  });
}
