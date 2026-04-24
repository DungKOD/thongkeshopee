/**
 * Sync v9 — user-scope endpoints.
 *
 * Layout R2 per user: `users/{uid}/`
 * - manifest.json — single source of truth, CAS-guarded qua etag
 * - snapshots/{key}.db.zst — compaction output + bootstrap
 * - deltas/{table}/{key}.ndjson.zst — per-table append-only delta files (immutable)
 * - sync_logs/{yyyy-mm-dd}/{ts}_{rand}.ndjson.zst — debug event log
 *
 * Security: Worker tự prepend `users/{auth.uid}/` vào mọi R2 key client pass.
 * Client không thể spoof UID khác (F1 trong plan).
 */

import type { AuthContext, Env } from '../types';
import { jsonError, jsonOk } from '../response';

/**
 * Build R2 key an toàn cho user scope.
 * Reject key có "../" hoặc bắt đầu bằng "/" (traversal attack).
 */
function userKey(uid: string, subKey: string): string | null {
  // Sanitize: no .. segments, no leading slash, chỉ ký tự safe.
  if (subKey.includes('..') || subKey.startsWith('/') || subKey === '') {
    return null;
  }
  return `users/${uid}/${subKey}`;
}

// =============================================================
// MANIFEST
// =============================================================

/**
 * POST /v9/manifest/get
 * Body: (empty)
 * Response: { ok: true, manifest: {...}|null, etag: string|null }
 *
 * Trả manifest hiện tại trên R2. Nếu chưa có (first sync) → manifest=null,
 * etag=null, client build empty manifest.
 */
export async function manifestGetRoute(
  _req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  const key = `users/${auth.uid}/manifest.json`;
  const obj = await env.DB_BUCKET.get(key);
  if (!obj) {
    return jsonOk({ manifest: null, etag: null });
  }
  const text = await obj.text();
  let manifest: unknown;
  try {
    manifest = JSON.parse(text);
  } catch {
    return jsonError(500, 'manifest corrupt — không phải JSON');
  }
  return jsonOk({ manifest, etag: obj.etag });
}

/**
 * POST /v9/manifest/put
 * Body: { manifest: {...}, expectedEtag: string|null }
 * Response: { ok: true, etag: new_etag } | 412 nếu CAS mismatch
 *
 * expectedEtag=null → upload ONLY nếu manifest chưa tồn tại (first create).
 * expectedEtag=string → upload ONLY nếu R2 etag match (CAS).
 * Mismatch → 412, client re-fetch + re-append + retry.
 */
export async function manifestPutRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  let body: { manifest: unknown; expectedEtag: string | null };
  try {
    body = await req.json();
  } catch {
    return jsonError(400, 'body không phải JSON hợp lệ');
  }
  if (!body.manifest) {
    return jsonError(400, 'body thiếu manifest');
  }

  const key = `users/${auth.uid}/manifest.json`;
  const bytes = new TextEncoder().encode(JSON.stringify(body.manifest));

  let putOptions: R2PutOptions;
  if (body.expectedEtag === null) {
    // First create: reject nếu key đã có. R2 onlyIf có etagDoesNotMatch...
    // Workaround: HEAD trước, nếu có → 412.
    const existing = await env.DB_BUCKET.head(key);
    if (existing !== null) {
      return jsonError(412, 'manifest đã tồn tại — expectedEtag phải set');
    }
    putOptions = {
      customMetadata: { uid: auth.uid },
      httpMetadata: { contentType: 'application/json' },
    };
  } else {
    putOptions = {
      customMetadata: { uid: auth.uid },
      httpMetadata: { contentType: 'application/json' },
      onlyIf: { etagMatches: body.expectedEtag },
    };
  }

  const result = await env.DB_BUCKET.put(key, bytes, putOptions);
  if (result === null) {
    return jsonError(412, 'CAS mismatch — manifest etag khác expectedEtag');
  }
  return jsonOk({ etag: result.etag });
}

// =============================================================
// DELTA FILES (immutable — no CAS)
// =============================================================

/**
 * POST /v9/delta/upload?key=deltas/raw_shopee_clicks/5000_1234.ndjson.zst
 * Body: raw zstd bytes
 * Response: { ok: true, etag, sizeBytes }
 *
 * Delta files immutable — 2 upload cùng key ghi đè idempotent (hash check ở
 * client, plan skip-identical).
 */
export async function deltaUploadRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  const subKey = new URL(req.url).searchParams.get('key');
  if (!subKey) return jsonError(400, 'query thiếu ?key=');
  if (!subKey.startsWith('deltas/')) {
    return jsonError(400, 'key phải bắt đầu bằng "deltas/"');
  }
  const fullKey = userKey(auth.uid, subKey);
  if (!fullKey) return jsonError(400, 'key không hợp lệ');

  const bytes = await req.arrayBuffer();
  if (bytes.byteLength === 0) {
    return jsonError(400, 'body rỗng');
  }

  const result = await env.DB_BUCKET.put(fullKey, bytes, {
    customMetadata: { uid: auth.uid },
    httpMetadata: { contentType: 'application/octet-stream' },
  });
  if (result === null) {
    return jsonError(500, 'R2 put trả null (bất thường — delta không CAS)');
  }
  return jsonOk({ etag: result.etag, sizeBytes: bytes.byteLength });
}

/**
 * GET /v9/delta/fetch?key=deltas/raw_shopee_clicks/5000_1234.ndjson.zst
 * Response: raw zstd bytes, headers: X-Size-Bytes, ETag
 */
export async function deltaFetchRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  const subKey = new URL(req.url).searchParams.get('key');
  if (!subKey) return jsonError(400, 'query thiếu ?key=');
  if (!subKey.startsWith('deltas/')) {
    return jsonError(400, 'key phải bắt đầu bằng "deltas/"');
  }
  const fullKey = userKey(auth.uid, subKey);
  if (!fullKey) return jsonError(400, 'key không hợp lệ');

  const obj = await env.DB_BUCKET.get(fullKey);
  if (!obj) return jsonError(404, 'delta không tồn tại');

  return new Response(obj.body, {
    status: 200,
    headers: {
      'Content-Type': 'application/octet-stream',
      'X-Size-Bytes': String(obj.size),
      ETag: obj.etag,
    },
  });
}

// =============================================================
// SNAPSHOT (stream pass-through)
// =============================================================

/**
 * POST /v9/snapshot/upload?key=snapshots/snap_1234.db.zst
 * Body: raw zstd bytes (SQLite VACUUM INTO output)
 * Response: { ok: true, etag, sizeBytes }
 */
export async function snapshotUploadRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  const subKey = new URL(req.url).searchParams.get('key');
  if (!subKey) return jsonError(400, 'query thiếu ?key=');
  if (!subKey.startsWith('snapshots/')) {
    return jsonError(400, 'key phải bắt đầu bằng "snapshots/"');
  }
  const fullKey = userKey(auth.uid, subKey);
  if (!fullKey) return jsonError(400, 'key không hợp lệ');

  const bytes = await req.arrayBuffer();
  if (bytes.byteLength === 0) return jsonError(400, 'body rỗng');

  const result = await env.DB_BUCKET.put(fullKey, bytes, {
    customMetadata: { uid: auth.uid },
    httpMetadata: { contentType: 'application/octet-stream' },
  });
  if (result === null) return jsonError(500, 'R2 put trả null');
  return jsonOk({ etag: result.etag, sizeBytes: bytes.byteLength });
}

/**
 * GET /v9/snapshot/fetch?key=snapshots/snap_1234.db.zst
 * Response: stream pass-through raw zstd bytes (500MB+ OK với Worker CPU budget)
 *
 * Plan Q8: Worker stream `bucket.get(key).body` — CF không charge CPU cho
 * body pass-through. Chấp nhận tới vài GB. Nếu đụng limit sẽ switch presigned
 * URL ở v9.1.
 */
export async function snapshotFetchRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  const subKey = new URL(req.url).searchParams.get('key');
  if (!subKey) return jsonError(400, 'query thiếu ?key=');
  if (!subKey.startsWith('snapshots/')) {
    return jsonError(400, 'key phải bắt đầu bằng "snapshots/"');
  }
  const fullKey = userKey(auth.uid, subKey);
  if (!fullKey) return jsonError(400, 'key không hợp lệ');

  const obj = await env.DB_BUCKET.get(fullKey);
  if (!obj) return jsonError(404, 'snapshot không tồn tại');

  return new Response(obj.body, {
    status: 200,
    headers: {
      'Content-Type': 'application/octet-stream',
      'X-Size-Bytes': String(obj.size),
      ETag: obj.etag,
    },
  });
}

// =============================================================
// SYNC LOG (append-only, daily-rotated)
// =============================================================

/**
 * POST /v9/sync-log/push?date=2026-04-24
 * Body: raw zstd bytes (NDJSON events)
 * Response: { ok: true, key }
 *
 * R2 key: `users/{uid}/sync_logs/{date}/{utc_ms}_{rand6}.ndjson.zst`
 * Unique key per upload → append semantics via multi-file (R2 không có
 * native append). Admin viewer list prefix aggregate.
 */
export async function syncLogPushRoute(
  req: Request,
  auth: AuthContext,
  env: Env,
): Promise<Response> {
  const date = new URL(req.url).searchParams.get('date');
  if (!date) return jsonError(400, 'query thiếu ?date=');
  // Validate date format yyyy-mm-dd defensive.
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return jsonError(400, 'date phải yyyy-mm-dd');
  }

  const bytes = await req.arrayBuffer();
  if (bytes.byteLength === 0) return jsonError(400, 'body rỗng');
  // Sanity cap: 1 push ≤ 1MB (ring buffer 5000 events × ~200B ~= 1MB nén).
  if (bytes.byteLength > 1_048_576) {
    return jsonError(413, 'sync log push > 1MB');
  }

  const utcMs = Date.now();
  const rand = Math.random().toString(36).slice(2, 8);
  const subKey = `sync_logs/${date}/${utcMs}_${rand}.ndjson.zst`;
  const fullKey = userKey(auth.uid, subKey);
  if (!fullKey) return jsonError(500, 'internal key build failed');

  const result = await env.DB_BUCKET.put(fullKey, bytes, {
    customMetadata: { uid: auth.uid, kind: 'sync_log' },
    httpMetadata: { contentType: 'application/octet-stream' },
  });
  if (result === null) return jsonError(500, 'R2 put trả null');
  return jsonOk({ key: subKey });
}
