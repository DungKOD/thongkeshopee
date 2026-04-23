import type { Env, AuthContext } from '../types';
import { jsonOk } from '../response';

export async function metadataRoute(
  _req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  const key = `users/${auth.uid}/db.zst`;
  const head = await env.DB_BUCKET.head(key);

  if (!head) {
    return jsonOk({
      exists: false,
      fileId: null,
      sizeBytes: null,
      lastModified: null,
      fingerprint: null,
      etag: null,
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
    // CAS: client có thể dùng etag này nếu chỉ gọi metadata rồi upload (không
    // pull). Thường client sẽ pull mỗi khi etag thay đổi, nhưng return etag ở
    // đây cho trường hợp startup check upload-init khi remote empty/local fresh.
    etag: head.etag,
  });
}
