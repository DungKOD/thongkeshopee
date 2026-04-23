/**
 * ThongKe Shopee — Drive proxy + Admin endpoints + Video Log Sheet
 *
 * Deploy as Web App (execute as Me, access Anyone). Client gửi Firebase ID token;
 * script verify qua Firebase REST → thao tác Drive + Google Sheet dưới tài khoản owner.
 *
 * CONFIG: hardcoded dưới đây. Có thể override qua Script Properties nếu cần
 * (Project Settings → Script Properties → add key cùng tên).
 *
 * File naming convention: {email_local_part}.db
 *   - `abc@gmail.com` → `abc.db`
 *   - Collision detection qua file.description chứa owner UID.
 *
 * Video log Sheet: tab name = email local-part. 3 cột: Thời gian | Link | Trạng thái.
 * Upsert theo URL (mỗi URL chỉ 1 row, giữ status cuối). Sort newest-first theo timestamp.
 */

// Firebase config — phải khớp project mà FE sign-in vào. Token verify ở
// `verifyFirebaseToken` qua Identity Toolkit `accounts:lookup?key=<apiKey>`,
// API key PHẢI thuộc project đã phát hành token (aud claim). Sai project →
// Google reject → AS trả 401 "Invalid token".
//
// Source of truth: FE `.env.local` VITE_FIREBASE_PROJECT_ID + VITE_FIREBASE_API_KEY.
// Override runtime qua Script Properties (Project Settings → Script Properties).
const CONFIG_DEFAULTS = {
  FIREBASE_PROJECT_ID: 'thongkeshopee-9b2d9',
  FIREBASE_API_KEY: 'AIzaSyBcnNmUdkA1fNzBFsp2UQeDBpqY3Mlhjfk',
  DRIVE_FOLDER_ID: '1_N8hcw7oyVbI40P_uiyXWssvgWY_DKJb',
};

const DB_FILENAME_SUFFIX = '.db';
const MIME_SQLITE = 'application/vnd.sqlite3';

const VIDEO_LOG_SHEET_ID = '1LcUA9kQRhWl_Hq7qi_fJ8zgTfUSneRzxNAEcm1iiYLI';
const VIDEO_LOG_HEADER = ['Thời gian', 'Link', 'Trạng thái'];

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
      case 'logVideoDownload':
        return handleLogVideoDownload(
          user,
          body.videoUrl,
          body.videoStatus,
          body.videoTimestamp,
        );
      case 'readUserVideoLog':
        return handleReadUserVideoLog(
          user,
          idToken,
          body.targetUid,
          body.targetLocalPart,
          body.limit,
          body.offset,
        );
      case 'deleteUserVideoLogRow':
        return handleDeleteUserVideoLogRow(
          user,
          idToken,
          body.targetUid,
          body.targetLocalPart,
          body.videoTimestamp,
          body.videoUrl,
          body.videoStatus,
        );
      case 'deleteUserVideoLogSheet':
        return handleDeleteUserVideoLogSheet(
          user,
          idToken,
          body.targetUid,
          body.targetLocalPart,
        );
      default:
        return jsonError(400, 'Unknown action: ' + action);
    }
  } catch (err) {
    return jsonError(500, String(err));
  }
}

function doGet() {
  return jsonOk({ service: 'ThongKeShopee Drive Proxy', version: 4 });
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
// Video log — Google Sheet as primary audit store
// Sheet tab name = email local-part. 3 cột: Thời gian | Link | Trạng thái.
// Upsert theo URL: mỗi URL chỉ 1 row, luôn giữ status mới nhất.
// ============================================================

function vnStatus_(s) {
  if (s === 'success') return 'thành công';
  if (s === 'failed') return 'thất bại';
  return String(s || '');
}

/**
 * Parse timestamp "HH:MM:SS DD/MM/YYYY" → epoch ms. Trả 0 nếu format lỗi.
 * Dùng để sort newest-first bất kể row order trong sheet.
 */
function parseTs_(s) {
  const parts = String(s || '').split(' ');
  if (parts.length !== 2) return 0;
  const hms = parts[0].split(':').map(Number);
  const dmy = parts[1].split('/').map(Number);
  const t = new Date(
    dmy[2] || 1970,
    (dmy[1] || 1) - 1,
    dmy[0] || 1,
    hms[0] || 0,
    hms[1] || 0,
    hms[2] || 0,
  ).getTime();
  return isNaN(t) ? 0 : t;
}

/**
 * Get-or-create sheet tab cho 1 user. Tab name = email local-part (phần
 * trước @) — dễ đọc cho admin. Collision guard qua developer metadata
 * `owner_uid`: 2 user khác provider cùng local-part → user sau bị reject
 * với error rõ ràng (không ghi đè im lặng).
 *
 * Migration: nếu có tab cũ tên UID (từ v2 fix trước khi revert) + tab
 * localPart chưa tồn tại → rename uid → localPart, stamp metadata. Data
 * được preserve, admin view tự động ra đúng tab.
 */
function getOrCreateLogSheetForUser_(user) {
  const ss = SpreadsheetApp.openById(VIDEO_LOG_SHEET_ID);
  const uid = user.uid;
  const localPart = emailToLocalPart(user.email);

  // Tab localPart đã tồn tại → verify ownership.
  let sheet = ss.getSheetByName(localPart);
  if (sheet) {
    const claimedUid = getSheetOwnerUid_(sheet);
    if (!claimedUid) {
      // Tab cũ (pre-metadata) → claim cho user hiện tại (first-come-first-serve).
      setSheetOwnerUid_(sheet, uid);
      return sheet;
    }
    if (claimedUid === uid) return sheet;
    // COLLISION: 2 user khác nhau cùng local-part. Reject rõ ràng — không
    // ghi đè log user khác. User bị block phải liên hệ admin đổi sang email
    // khác hoặc admin xóa tab cũ.
    throw new Error(
      'COLLISION: tab "' + localPart + '" đã thuộc user khác (uid=' + claimedUid + '). ' +
        'Local-part email trùng — liên hệ admin để resolve.',
    );
  }

  // Migration từ v2: tab tên uid tồn tại → rename về localPart.
  const legacyUidSheet = ss.getSheetByName(uid);
  if (legacyUidSheet) {
    legacyUidSheet.setName(localPart);
    setSheetOwnerUid_(legacyUidSheet, uid);
    return legacyUidSheet;
  }

  // Tạo tab mới.
  sheet = ss.insertSheet(localPart);
  setSheetOwnerUid_(sheet, uid);
  sheet.getRange(1, 1, sheet.getMaxRows(), 3).setNumberFormat('@');
  sheet
    .getRange(1, 1, 1, VIDEO_LOG_HEADER.length)
    .setValues([VIDEO_LOG_HEADER])
    .setFontWeight('bold');
  sheet.setFrozenRows(1);
  sheet.setColumnWidth(1, 160);
  sheet.setColumnWidth(2, 480);
  sheet.setColumnWidth(3, 100);
  return sheet;
}

/**
 * Lookup tab của target user cho admin read/delete. Ưu tiên localPart
 * (naming convention hiện tại); fallback uid tab (user chưa migrate sau
 * revert v3). Verify ownership qua metadata nếu có. Trả null nếu không có.
 */
function resolveUserLogSheet_(ss, targetUid, targetLocalPart) {
  if (targetLocalPart) {
    const byLocal = ss.getSheetByName(targetLocalPart);
    if (byLocal) {
      // Verify không phải tab của user khác (nếu có claim metadata).
      const claimedUid = getSheetOwnerUid_(byLocal);
      if (!claimedUid || !targetUid || claimedUid === targetUid) {
        return byLocal;
      }
    }
  }
  // Fallback: user chưa migrate từ v2 uid-named tab → tìm theo uid.
  if (targetUid) {
    const byUid = ss.getSheetByName(targetUid);
    if (byUid) return byUid;
  }
  return null;
}

function getSheetOwnerUid_(sheet) {
  try {
    const metas = sheet
      .createDeveloperMetadataFinder()
      .withKey('owner_uid')
      .find();
    if (metas && metas.length > 0) return metas[0].getValue();
  } catch (e) {
    /* legacy sheet without metadata API access */
  }
  return null;
}

function setSheetOwnerUid_(sheet, uid) {
  try {
    const existing = sheet
      .createDeveloperMetadataFinder()
      .withKey('owner_uid')
      .find();
    if (existing) existing.forEach(function (m) { m.remove(); });
    sheet.addDeveloperMetadata('owner_uid', uid);
  } catch (e) {
    /* best-effort — legacy sheet có thể không support metadata */
  }
}

/**
 * Upsert theo URL: xóa row cũ (nếu có) cùng URL → insert row mới ngay sau header.
 * Sheet view + UI đều newest-first. Mỗi URL giữ 1 row với status cuối.
 *
 * Tab name = `user.uid` (không phải email local-part) → tránh collision
 * cross-provider same-local-part. Migration cũ → uid trong `getOrCreateLogSheetForUser_`.
 */
function handleLogVideoDownload(user, videoUrl, videoStatus, videoTimestamp) {
  if (!videoUrl || !videoStatus || !videoTimestamp) {
    return jsonError(400, 'Missing videoUrl/videoStatus/videoTimestamp');
  }
  if (videoStatus !== 'success' && videoStatus !== 'failed') {
    return jsonError(400, 'Invalid videoStatus: ' + videoStatus);
  }

  const sheet = getOrCreateLogSheetForUser_(user);
  const statusVN = vnStatus_(videoStatus);

  // Xóa row cũ (nếu có) match theo col B (URL). Iterate bottom-up để
  // delete safely theo index (legacy có thể có nhiều duplicate).
  const lastRow = sheet.getLastRow();
  let replaced = false;
  if (lastRow > 1) {
    const urls = sheet.getRange(2, 2, lastRow - 1, 1).getValues();
    for (let i = urls.length - 1; i >= 0; i--) {
      if (String(urls[i][0]) === videoUrl) {
        sheet.deleteRow(i + 2);
        replaced = true;
      }
    }
  }

  // Insert row mới ngay sau header → sheet view newest-first.
  sheet.insertRowBefore(2);
  sheet.getRange(2, 1, 1, 3).setValues([[videoTimestamp, videoUrl, statusVN]]);

  return jsonOk({ upserted: true, replaced: replaced });
}

/**
 * Admin-only: đọc sheet tab của target user. Ưu tiên tab `targetUid`;
 * fallback `targetLocalPart` cho user chưa migrate. Sort newest-first bằng
 * parse timestamp — robust với cả legacy rows (append-bottom cũ) + rows mới
 * (insert-top). Pagination limit/offset áp dụng trên danh sách đã sort.
 */
function handleReadUserVideoLog(
  user,
  idToken,
  targetUid,
  targetLocalPart,
  limit,
  offset,
) {
  if (!isAdmin(user, idToken)) return jsonError(403, 'Admin required');
  if (!targetUid && !targetLocalPart) {
    return jsonError(400, 'Missing targetUid/targetLocalPart');
  }

  const lim = Math.max(1, Math.min(5000, parseInt(limit, 10) || 100));
  const off = Math.max(0, parseInt(offset, 10) || 0);

  const ss = SpreadsheetApp.openById(VIDEO_LOG_SHEET_ID);
  const sheet = resolveUserLogSheet_(ss, targetUid, targetLocalPart);
  if (!sheet) return jsonOk({ videoLogs: [] });

  const lastRow = sheet.getLastRow();
  if (lastRow <= 1) return jsonOk({ videoLogs: [] });

  const all = sheet.getRange(2, 1, lastRow - 1, 3).getValues();
  all.sort(function (a, b) {
    return parseTs_(b[0]) - parseTs_(a[0]);
  });

  const page = all.slice(off, off + lim);
  const videoLogs = page.map(function (r) {
    return {
      timestamp: String(r[0] || ''),
      url: String(r[1] || ''),
      status: String(r[2] || ''),
    };
  });

  return jsonOk({ videoLogs: videoLogs });
}

/**
 * Admin-only: xóa 1 row match (timestamp, url, status). First-match only —
 * nếu sheet có duplicate chính xác cả 3 cột (hiếm) thì chỉ xóa row đầu tiên.
 */
function handleDeleteUserVideoLogRow(
  user,
  idToken,
  targetUid,
  targetLocalPart,
  timestamp,
  videoUrl,
  status,
) {
  if (!isAdmin(user, idToken)) return jsonError(403, 'Admin required');
  if (!targetUid && !targetLocalPart) {
    return jsonError(400, 'Missing targetUid/targetLocalPart');
  }
  if (!timestamp || !videoUrl || !status) {
    return jsonError(400, 'Missing timestamp/videoUrl/status');
  }

  const ss = SpreadsheetApp.openById(VIDEO_LOG_SHEET_ID);
  const sheet = resolveUserLogSheet_(ss, targetUid, targetLocalPart);
  if (!sheet) return jsonError(404, 'Sheet tab not found');

  const lastRow = sheet.getLastRow();
  if (lastRow <= 1) return jsonError(404, 'Sheet empty');

  const data = sheet.getRange(2, 1, lastRow - 1, 3).getValues();
  for (let i = 0; i < data.length; i++) {
    if (
      String(data[i][0]) === timestamp &&
      String(data[i][1]) === videoUrl &&
      String(data[i][2]) === status
    ) {
      sheet.deleteRow(i + 2);
      return jsonOk({ deleted: true, rowIndex: i + 2 });
    }
  }
  return jsonError(404, 'Row not found (tuple mismatch)');
}

/**
 * Admin-only: xóa tab (sheet) của 1 user. File Sheet gốc không bị xóa.
 * Xóa cả tab `localPart` (naming convention hiện tại) lẫn tab `uid` cũ
 * (từ v2 — nếu user chưa migrate) để cleanup triệt để.
 */
function handleDeleteUserVideoLogSheet(user, idToken, targetUid, targetLocalPart) {
  if (!isAdmin(user, idToken)) return jsonError(403, 'Admin required');
  if (!targetUid && !targetLocalPart) {
    return jsonError(400, 'Missing targetUid/targetLocalPart');
  }

  const ss = SpreadsheetApp.openById(VIDEO_LOG_SHEET_ID);
  let deletedAny = false;
  if (targetLocalPart) {
    const byLocal = ss.getSheetByName(targetLocalPart);
    if (byLocal) {
      // Chỉ xóa tab localPart nếu không claim bởi user KHÁC.
      const claimedUid = getSheetOwnerUid_(byLocal);
      if (!claimedUid || !targetUid || claimedUid === targetUid) {
        ss.deleteSheet(byLocal);
        deletedAny = true;
      }
    }
  }
  if (targetUid) {
    const byUid = ss.getSheetByName(targetUid);
    if (byUid) {
      ss.deleteSheet(byUid);
      deletedAny = true;
    }
  }
  if (!deletedAny) return jsonOk({ deleted: false, reason: 'tab already missing' });
  return jsonOk({ deleted: true });
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
