import type { Env, AuthContext } from '../types';
import { jsonOk, jsonError } from '../response';

/// Upload DB với CAS (Compare-And-Swap) guard chống race giữa 2 máy cùng user.
///
/// v8.1+: Request body là **raw zstd bytes** (Content-Type application/octet-stream).
/// Metadata trong HTTP headers — không còn JSON base64:
///   - X-Mtime-Ms: timestamp ms
///   - X-Fingerprint: machine fingerprint
///   - X-Expected-Etag: CAS guard (optional, skip → unconditional PUT)
///
/// Flow:
/// - Client gửi raw bytes + expectedEtag header
/// - Worker dùng `onlyIf: { etagMatches }` → R2 SDK reject nếu etag hiện tại khác
///   → `put()` return null → 412
/// - Client catch 412 → pull-merge-push + retry
export async function uploadRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  const contentType = req.headers.get('Content-Type') || '';
  if (!contentType.includes('application/octet-stream')) {
    return jsonError(
      400,
      `Invalid Content-Type: expected application/octet-stream, got ${contentType}`,
    );
  }

  const bytes = new Uint8Array(await req.arrayBuffer());
  if (bytes.byteLength === 0) return jsonError(400, 'Empty body');

  const mtimeMs = Number(req.headers.get('X-Mtime-Ms')) || Date.now();
  const fingerprint = req.headers.get('X-Fingerprint') || '';
  const expectedEtagRaw = req.headers.get('X-Expected-Etag');
  const expectedEtag =
    expectedEtagRaw && expectedEtagRaw.length > 0 ? expectedEtagRaw : null;

  const key = `users/${auth.uid}/db.zst`;

  const putOptions: R2PutOptions = {
    customMetadata: {
      fingerprint,
      mtimeMs: String(mtimeMs),
      uid: auth.uid,
    },
  };
  if (expectedEtag) {
    putOptions.onlyIf = { etagMatches: expectedEtag };
  }

  const putResult = await env.DB_BUCKET.put(key, bytes, putOptions);

  if (expectedEtag && putResult === null) {
    return jsonError(
      412,
      'R2 etag mismatch — object đã thay đổi từ lúc client pull. ' +
        'Hãy pull-merge-push rồi retry upload.',
    );
  }

  if (!putResult) {
    return jsonError(500, 'R2 put failed unexpectedly');
  }

  return jsonOk({
    fileId: key,
    sizeBytes: bytes.byteLength,
    lastModified: mtimeMs,
    fingerprint,
    etag: putResult.etag,
  });
}
