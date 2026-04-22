//! Firestore REST client — đọc user profile (admin/premium/email/expiredAt).
//!
//! Dùng Firebase ID token của caller để authenticate với Firestore (Google
//! Cloud Identity Platform). Rules của user `users/{uid}` cần cho phép:
//!   - User đọc doc của chính họ: `request.auth.uid == userId`
//!   - Admin đọc tất cả: `get(...users/$(request.auth.uid)).data.admin == true`
//!
//! Không dùng service account JSON — simpler, không cần quản lý private key.

import type { Env } from './types';

/// Shape sau khi parse Firestore doc `users/{uid}`.
export interface UserProfile {
  admin: boolean;
  premium: boolean;
  email: string | null;
  expiredAt: string | null;
  createdAt: string | null;
}

function firestoreDocUrl(env: Env, uid: string): string {
  return `https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents/users/${encodeURIComponent(uid)}`;
}

/// Parse Firestore REST document format (`fields.*.{booleanValue|stringValue|timestampValue}`)
/// thành shape gọn.
function parseUserDoc(doc: unknown): UserProfile | null {
  if (!doc || typeof doc !== 'object') return null;
  const fields = (doc as { fields?: Record<string, unknown> }).fields;
  if (!fields) return null;

  const boolField = (name: string): boolean => {
    const f = fields[name] as { booleanValue?: boolean } | undefined;
    return f?.booleanValue === true;
  };
  const stringField = (name: string): string | null => {
    const f = fields[name] as { stringValue?: string } | undefined;
    return typeof f?.stringValue === 'string' ? f.stringValue : null;
  };
  const timestampField = (name: string): string | null => {
    const f = fields[name] as { timestampValue?: string } | undefined;
    return typeof f?.timestampValue === 'string' ? f.timestampValue : null;
  };

  return {
    admin: boolField('admin'),
    premium: boolField('premium'),
    email: stringField('email'),
    expiredAt: timestampField('expiredAt'),
    createdAt: timestampField('createdAt'),
  };
}

/// Fetch profile cho 1 UID qua REST API.
/// Caller pass `idToken` của user đang request — Firestore rules sẽ decide
/// có được đọc hay không. Trả null nếu doc không tồn tại hoặc bị rules deny.
export async function fetchUserProfile(
  uid: string,
  idToken: string,
  env: Env,
): Promise<UserProfile | null> {
  const url = firestoreDocUrl(env, uid);
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${idToken}` },
  });
  if (!res.ok) {
    // 404 = doc chưa tạo, 403 = rules deny. Cả 2 coi như "không có profile".
    return null;
  }
  const doc = (await res.json()) as unknown;
  return parseUserDoc(doc);
}

/// Fetch profile cho nhiều UID. Dùng Firestore REST `documents:batchGet` —
/// 1 HTTP call cho toàn bộ batch (max 500 docs/batch theo Google limits).
/// Giảm subrequest count từ N → 1 (Worker free tier cap 50 subrequest/invocation).
export async function fetchUserProfilesBatch(
  uids: string[],
  idToken: string,
  env: Env,
): Promise<Map<string, UserProfile>> {
  const out = new Map<string, UserProfile>();
  if (uids.length === 0) return out;

  // Firestore batchGet limit: 500 docs/request — chunk phòng scale.
  const BATCH_LIMIT = 500;
  for (let i = 0; i < uids.length; i += BATCH_LIMIT) {
    const chunk = uids.slice(i, i + BATCH_LIMIT);
    const docsPath = chunk.map(
      (uid) =>
        `projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents/users/${uid}`,
    );

    const res = await fetch(
      `https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents:batchGet`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${idToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ documents: docsPath }),
      },
    );

    if (!res.ok) continue; // Rules deny / network error → profile null cho cả batch.
    const body = (await res.json()) as Array<{
      found?: { name: string; fields?: Record<string, unknown> };
      missing?: string;
    }>;

    for (const item of body) {
      if (!item.found) continue;
      const m = /\/users\/([^/]+)$/.exec(item.found.name);
      if (!m) continue;
      const uid = m[1];
      const profile = parseUserDoc(item.found);
      if (profile) out.set(uid, profile);
    }
  }
  return out;
}

/// Derive email local part (trước dấu `@`). Backward compat với shape cũ.
export function emailLocalPart(email: string | null): string | null {
  if (!email) return null;
  const at = email.indexOf('@');
  return at > 0 ? email.slice(0, at) : email;
}

/// List toàn bộ user docs trong collection `users` qua Firestore REST.
/// Admin caller pass idToken — rules cần cho phép admin `list`.
/// Paginate qua `nextPageToken`, dừng khi hết hoặc khi rules deny.
export async function listAllUsers(
  idToken: string,
  env: Env,
): Promise<Array<UserProfile & { uid: string }>> {
  const out: Array<UserProfile & { uid: string }> = [];
  let pageToken: string | undefined;
  const PAGE_SIZE = 300;
  do {
    const params = new URLSearchParams({ pageSize: String(PAGE_SIZE) });
    if (pageToken) params.set('pageToken', pageToken);
    const url = `https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents/users?${params}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${idToken}` },
    });
    if (!res.ok) break; // rules deny / error → stop pagination, return phần đã lấy.
    const data = (await res.json()) as {
      documents?: Array<{ name: string; fields?: Record<string, unknown> }>;
      nextPageToken?: string;
    };
    for (const doc of data.documents ?? []) {
      const m = /\/users\/([^/]+)$/.exec(doc.name);
      if (!m) continue;
      const profile = parseUserDoc(doc);
      if (profile) out.push({ uid: m[1], ...profile });
    }
    pageToken = data.nextPageToken;
  } while (pageToken);
  return out;
}
