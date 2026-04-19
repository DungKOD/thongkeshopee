/**
 * ThongKe Shopee — Drive proxy + Admin endpoints
 *
 * Deploy as Web App (execute as Me, access Anyone). Client gửi Firebase ID token;
 * script verify qua Firebase REST → thao tác Drive dưới tài khoản owner.
 *
 * CONFIG: hardcoded dưới đây. Có thể override qua Script Properties nếu cần
 * (Project Settings → Script Properties → add key cùng tên).
 *
 * File naming convention: {email_local_part}.db
 *   - `abc@gmail.com` → `abc.db`
 *   - Collision detection qua file.description chứa owner UID.
 */

const CONFIG_DEFAULTS = {
  FIREBASE_PROJECT_ID: 'thongkeshopee-62d61',
  FIREBASE_API_KEY: 'AIzaSyBciz2PyfarMqVU8VmGXgFxlzrAWuBwqwA',
  DRIVE_FOLDER_ID: '1_N8hcw7oyVbI40P_uiyXWssvgWY_DKJb',
};

const DB_FILENAME_SUFFIX = '.db';
const MIME_SQLITE = 'application/vnd.sqlite3';

function doPost(e) {
  try {
    const body = JSON.parse(e.postData.contents || '{}');
    const { action, idToken } = body;

    if (!idToken) return jsonError(401, 'Missing idToken');
    if (!action) return jsonError(400, 'Missing action');

    const user = verifyFirebaseToken(idToken);
    if (!user) return jsonError(401, 'Invalid token');

    switch (action) {
      case 'checkOrCreate':
        return handleCheckOrCreate(user);
      case 'download':
        return handleDownload(user);
      case 'upload':
        return handleUpload(user, body.base64Data, body.mtimeMs, body.fingerprint);
      case 'metadata':
        return handleMetadata(user);
      case 'listUsers':
        return handleListUsers(user, idToken);
      case 'downloadForUser':
        return handleDownloadForUser(user, idToken, body.targetLocalPart);
      default:
        return jsonError(400, 'Unknown action: ' + action);
    }
  } catch (err) {
    return jsonError(500, String(err));
  }
}

function doGet() {
  return jsonOk({ service: 'ThongKeShopee Drive Proxy', version: 2 });
}

// ============================================================
// Token verification + user lookup
// ============================================================

/**
 * Verify Firebase ID token qua Identity Toolkit API.
 * Trả về {uid, email} nếu hợp lệ, null nếu không.
 */
function verifyFirebaseToken(idToken) {
  const apiKey = getProp_('FIREBASE_API_KEY');
  const url =
    'https://identitytoolkit.googleapis.com/v1/accounts:lookup?key=' + apiKey;

  const res = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({ idToken: idToken }),
    muteHttpExceptions: true,
  });

  if (res.getResponseCode() !== 200) return null;

  const data = JSON.parse(res.getContentText());
  if (!data.users || data.users.length === 0) return null;

  const u = data.users[0];
  if (!u.localId || !u.email) return null;
  return { uid: u.localId, email: u.email };
}

/**
 * Lấy Firestore user doc qua REST bằng ID token của chính user.
 * Rules Firestore quyết định có đọc được hay không.
 */
function firestoreGetDoc(collection, docId, idToken) {
  const projectId = getProp_('FIREBASE_PROJECT_ID');
  const url =
    'https://firestore.googleapis.com/v1/projects/' +
    projectId +
    '/databases/(default)/documents/' +
    collection +
    '/' +
    docId;
  const res = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: { Authorization: 'Bearer ' + idToken },
    muteHttpExceptions: true,
  });
  const code = res.getResponseCode();
  if (code === 404) return { ok: true, exists: false };
  if (code !== 200)
    return { ok: false, error: 'Firestore ' + code + ': ' + res.getContentText() };
  return { ok: true, exists: true, doc: JSON.parse(res.getContentText()) };
}

function firestoreListDocs(collection, idToken, pageSize) {
  const projectId = getProp_('FIREBASE_PROJECT_ID');
  const url =
    'https://firestore.googleapis.com/v1/projects/' +
    projectId +
    '/databases/(default)/documents/' +
    collection +
    '?pageSize=' +
    (pageSize || 1000);
  const res = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: { Authorization: 'Bearer ' + idToken },
    muteHttpExceptions: true,
  });
  const code = res.getResponseCode();
  if (code !== 200)
    return { ok: false, error: 'Firestore ' + code + ': ' + res.getContentText() };
  return { ok: true, data: JSON.parse(res.getContentText()) };
}

/** Extract typed value từ Firestore REST field format. */
function firestoreValue(fieldObj) {
  if (!fieldObj) return null;
  if ('stringValue' in fieldObj) return fieldObj.stringValue;
  if ('booleanValue' in fieldObj) return fieldObj.booleanValue;
  if ('integerValue' in fieldObj) return parseInt(fieldObj.integerValue, 10);
  if ('doubleValue' in fieldObj) return fieldObj.doubleValue;
  if ('timestampValue' in fieldObj) return fieldObj.timestampValue;
  if ('nullValue' in fieldObj) return null;
  return null;
}

/**
 * Check user có admin==true không. Gọi Firestore qua user's ID token.
 */
function isAdmin(user, idToken) {
  const res = firestoreGetDoc('users', user.uid, idToken);
  if (!res.ok || !res.exists) return false;
  const fields = res.doc.fields || {};
  return firestoreValue(fields.admin) === true;
}

// ============================================================
// File naming + collision detection
// ============================================================

/**
 * Local part của email (phần trước '@'), lowercase, giữ chars an toàn cho filename.
 * Reject nếu local-part chứa ký tự lạ (defensive — Firebase validate email rồi
 * nhưng vẫn check thêm).
 */
function emailToLocalPart(email) {
  if (!email) throw new Error('Email trống');
  const at = email.indexOf('@');
  if (at <= 0) throw new Error('Email không hợp lệ: ' + email);
  const local = email.substring(0, at).toLowerCase();
  if (!/^[a-z0-9._+-]+$/.test(local))
    throw new Error('Email local-part có ký tự không hỗ trợ: ' + local);
  return local;
}

function filenameFor(user) {
  return emailToLocalPart(user.email) + DB_FILENAME_SUFFIX;
}

/**
 * Tìm file theo tên trong folder root. Trả null nếu không có.
 * Bao gồm cả file trong trash không (iterator DriveApp mặc định loại trash).
 */
function findFileInFolder_(folder, name) {
  const it = folder.getFilesByName(name);
  return it.hasNext() ? it.next() : null;
}

/**
 * Parse file.description → {ownerUid, fingerprint, mtime}.
 * Format mới: "owner_uid=<uid>|fingerprint=<hex>|mtime=<ms>"
 * Format cũ: "owner_uid=<uid>" (migration — fingerprint/mtime null).
 */
function parseFileMetadata_(file) {
  const desc = file.getDescription();
  if (!desc) return { ownerUid: null, fingerprint: null, mtime: null };
  const parts = desc.split('|');
  const result = { ownerUid: null, fingerprint: null, mtime: null };
  parts.forEach(function (part) {
    const idx = part.indexOf('=');
    if (idx <= 0) return;
    const key = part.substring(0, idx).trim();
    const val = part.substring(idx + 1).trim();
    if (key === 'owner_uid') result.ownerUid = val;
    else if (key === 'fingerprint') result.fingerprint = val;
    else if (key === 'mtime') result.mtime = parseInt(val, 10) || null;
  });
  return result;
}

function getFileOwnerUid_(file) {
  return parseFileMetadata_(file).ownerUid;
}

function setFileMetadata_(file, uid, fingerprint, mtime) {
  const parts = ['owner_uid=' + uid];
  if (fingerprint) parts.push('fingerprint=' + fingerprint);
  if (mtime) parts.push('mtime=' + mtime);
  file.setDescription(parts.join('|'));
}

function setFileOwnerUid_(file, uid) {
  // Migration wrapper — giữ owner, không động fingerprint/mtime.
  const existing = parseFileMetadata_(file);
  setFileMetadata_(file, uid, existing.fingerprint, existing.mtime);
}

/**
 * Check collision: nếu file filename tồn tại nhưng owner UID ≠ requester UID → throw.
 * Trả về file nếu hợp lệ (cùng owner), null nếu chưa tồn tại.
 */
function resolveUserFile_(folder, user) {
  const name = filenameFor(user);
  const existing = findFileInFolder_(folder, name);
  if (!existing) return null;
  const ownerUid = getFileOwnerUid_(existing);
  if (ownerUid && ownerUid !== user.uid) {
    throw new Error(
      'COLLISION: file ' +
        name +
        ' thuộc user khác (uid=' +
        ownerUid +
        '). Email local-part đã được user khác dùng.',
    );
  }
  // File cũ không có description → attach owner ngay (migration).
  if (!ownerUid) setFileOwnerUid_(existing, user.uid);
  return existing;
}

// ============================================================
// Handlers
// ============================================================

function handleCheckOrCreate(user) {
  const folder = DriveApp.getFolderById(getProp_('DRIVE_FOLDER_ID'));
  const existing = resolveUserFile_(folder, user);

  if (existing) {
    const meta = parseFileMetadata_(existing);
    return jsonOk({
      existed: true,
      fileId: existing.getId(),
      sizeBytes: existing.getSize(),
      lastModified: existing.getLastUpdated().getTime(),
      filename: existing.getName(),
      fingerprint: meta.fingerprint,
    });
  }

  const filename = filenameFor(user);
  const empty = Utilities.newBlob(new Uint8Array(0).buffer, MIME_SQLITE, filename);
  const created = folder.createFile(empty);
  setFileMetadata_(created, user.uid, null, null);
  return jsonOk({
    existed: false,
    fileId: created.getId(),
    sizeBytes: 0,
    lastModified: created.getLastUpdated().getTime(),
    filename: filename,
    fingerprint: null,
  });
}

function handleMetadata(user) {
  const folder = DriveApp.getFolderById(getProp_('DRIVE_FOLDER_ID'));
  const file = resolveUserFile_(folder, user);
  if (!file) return jsonOk({ exists: false });
  const meta = parseFileMetadata_(file);
  return jsonOk({
    exists: true,
    fileId: file.getId(),
    sizeBytes: file.getSize(),
    lastModified: file.getLastUpdated().getTime(),
    filename: file.getName(),
    fingerprint: meta.fingerprint,
  });
}

function handleDownload(user) {
  const folder = DriveApp.getFolderById(getProp_('DRIVE_FOLDER_ID'));
  const file = resolveUserFile_(folder, user);
  if (!file) return jsonError(404, 'File not found');

  const bytes = file.getBlob().getBytes();
  const base64 = Utilities.base64Encode(bytes);
  const meta = parseFileMetadata_(file);
  return jsonOk({
    fileId: file.getId(),
    sizeBytes: bytes.length,
    lastModified: file.getLastUpdated().getTime(),
    base64Data: base64,
    filename: file.getName(),
    fingerprint: meta.fingerprint,
  });
}

function handleUpload(user, base64Data, mtimeMs, fingerprint) {
  if (!base64Data) return jsonError(400, 'Missing base64Data');

  const folder = DriveApp.getFolderById(getProp_('DRIVE_FOLDER_ID'));
  const filename = filenameFor(user);
  const bytes = Utilities.base64Decode(base64Data);
  const blob = Utilities.newBlob(bytes, MIME_SQLITE, filename);

  // Resolve → check collision (throws nếu owner khác). Nếu cùng owner thì trash + re-create.
  const existing = resolveUserFile_(folder, user);
  if (existing) existing.setTrashed(true);
  const file = folder.createFile(blob);
  setFileMetadata_(file, user.uid, fingerprint || null, mtimeMs || null);

  return jsonOk({
    fileId: file.getId(),
    sizeBytes: bytes.length,
    lastModified: file.getLastUpdated().getTime(),
    clientMtimeMs: mtimeMs || null,
    filename: filename,
    fingerprint: fingerprint || null,
  });
}

/**
 * Admin-only: download DB file của user khác theo local-part.
 * Verify admin qua Firestore (user.admin == true).
 */
function handleDownloadForUser(user, idToken, targetLocalPart) {
  if (!isAdmin(user, idToken)) return jsonError(403, 'Admin required');
  if (!targetLocalPart || typeof targetLocalPart !== 'string') {
    return jsonError(400, 'Missing targetLocalPart');
  }
  // Sanitize: chỉ cho a-z 0-9 . _ + -
  if (!/^[a-z0-9._+-]+$/.test(targetLocalPart)) {
    return jsonError(400, 'Invalid targetLocalPart');
  }

  const folder = DriveApp.getFolderById(getProp_('DRIVE_FOLDER_ID'));
  const filename = targetLocalPart + DB_FILENAME_SUFFIX;
  const it = folder.getFilesByName(filename);
  if (!it.hasNext()) return jsonError(404, 'User DB not found');

  const file = it.next();
  const bytes = file.getBlob().getBytes();
  const base64 = Utilities.base64Encode(bytes);
  const meta = parseFileMetadata_(file);
  return jsonOk({
    fileId: file.getId(),
    sizeBytes: bytes.length,
    lastModified: file.getLastUpdated().getTime(),
    base64Data: base64,
    filename: filename,
    fingerprint: meta.fingerprint,
    ownerUid: meta.ownerUid,
  });
}

/**
 * Admin-only: list toàn bộ users/ collection + metadata file Drive.
 */
function handleListUsers(user, idToken) {
  if (!isAdmin(user, idToken)) return jsonError(403, 'Admin required');

  const listRes = firestoreListDocs('users', idToken, 1000);
  if (!listRes.ok) return jsonError(500, listRes.error);

  const folder = DriveApp.getFolderById(getProp_('DRIVE_FOLDER_ID'));
  const filesByName = {};
  const it = folder.getFiles();
  while (it.hasNext()) {
    const f = it.next();
    filesByName[f.getName()] = {
      fileId: f.getId(),
      sizeBytes: f.getSize(),
      lastModified: f.getLastUpdated().getTime(),
    };
  }

  const docs = (listRes.data && listRes.data.documents) || [];
  const users = docs.map(function (d) {
    const fields = d.fields || {};
    const email = firestoreValue(fields.email);
    const premium = firestoreValue(fields.premium) === true;
    const admin = firestoreValue(fields.admin) === true;
    const expiredAt = firestoreValue(fields.expiredAt);
    const createdAt = firestoreValue(fields.createdAt);
    const uid = d.name.split('/').pop();
    let localPart = null;
    let filename = null;
    let file = null;
    try {
      localPart = email ? emailToLocalPart(email) : null;
      filename = localPart ? localPart + DB_FILENAME_SUFFIX : null;
      file = filename ? filesByName[filename] || null : null;
    } catch (e) {
      // Ignore bad emails (defensive).
    }
    return {
      uid: uid,
      email: email,
      localPart: localPart,
      premium: premium,
      admin: admin,
      expiredAt: expiredAt,
      createdAt: createdAt,
      file: file,
    };
  });

  return jsonOk({ users: users });
}

// ============================================================
// Utils
// ============================================================

function getProp_(key) {
  // Prefer Script Properties (runtime override), fallback sang hardcoded.
  const override = PropertiesService.getScriptProperties().getProperty(key);
  if (override) return override;
  const fallback = CONFIG_DEFAULTS[key];
  if (fallback) return fallback;
  throw new Error('Missing config: ' + key);
}

function jsonOk(obj) {
  return ContentService.createTextOutput(
    JSON.stringify(Object.assign({ ok: true }, obj)),
  ).setMimeType(ContentService.MimeType.JSON);
}

function jsonError(code, message) {
  return ContentService.createTextOutput(
    JSON.stringify({ ok: false, code: code, error: message }),
  ).setMimeType(ContentService.MimeType.JSON);
}
