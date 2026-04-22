import type { Env, AuthContext, UploadRequest } from '../types';
import { jsonOk, jsonError } from '../response';
import { base64Decode } from '../base64';

export async function uploadRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  let body: UploadRequest;
  try {
    body = (await req.json()) as UploadRequest;
  } catch {
    return jsonError(400, 'Invalid JSON body');
  }

  if (!body.base64Data || typeof body.base64Data !== 'string') {
    return jsonError(400, 'Missing base64Data');
  }
  const mtimeMs = Number(body.mtimeMs) || Date.now();
  const fingerprint = typeof body.fingerprint === 'string' ? body.fingerprint : '';

  const bytes = base64Decode(body.base64Data);
  const key = `users/${auth.uid}/db.gz`;

  await env.DB_BUCKET.put(key, bytes, {
    customMetadata: {
      fingerprint,
      mtimeMs: String(mtimeMs),
      uid: auth.uid,
    },
  });

  return jsonOk({
    fileId: key,
    sizeBytes: bytes.byteLength,
    lastModified: mtimeMs,
    fingerprint,
  });
}
