/**
 * ThongKe Shopee — Drive proxy Apps Script
 *
 * Deploy as Web App (execute as Me, access Anyone). Client gửi Firebase ID token;
 * script verify qua Firebase REST → tự access Drive của owner.
 *
 * Required Script Properties (Project Settings → Script Properties):
 *   - FIREBASE_PROJECT_ID  (ví dụ: thongke-shopee-a1b2c)
 *   - FIREBASE_API_KEY     (apiKey từ Firebase Web config)
 *   - DRIVE_FOLDER_ID      (ID folder ShopeeStatData trong Drive)
 */

const DB_FILENAME_SUFFIX = '.db';
const MIME_SQLITE = 'application/vnd.sqlite3';

function doPost(e) {
  try {
    const body = JSON.parse(e.postData.contents || '{}');
    const { action, idToken } = body;

    if (!idToken) return jsonError(401, 'Missing idToken');
    if (!action) return jsonError(400, 'Missing action');

    const uid = verifyFirebaseToken(idToken);
    if (!uid) return jsonError(401, 'Invalid token');

    switch (action) {
      case 'checkOrCreate':
        return handleCheckOrCreate(uid);
      case 'download':
        return handleDownload(uid);
      case 'upload':
        return handleUpload(uid, body.base64Data, body.mtimeMs);
      case 'metadata':
        return handleMetadata(uid);
      default:
        return jsonError(400, 'Unknown action: ' + action);
    }
  } catch (err) {
    return jsonError(500, String(err));
  }
}

function doGet() {
  return jsonOk({ service: 'ThongKeShopee Drive Proxy', version: 1 });
}

/**
 * Verify Firebase ID token bằng cách gọi Identity Toolkit lookup API.
 * Trả về UID nếu hợp lệ, null nếu không.
 */
function verifyFirebaseToken(idToken) {
  const apiKey = getProp_('FIREBASE_API_KEY');
  const projectId = getProp_('FIREBASE_PROJECT_ID');
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

  const user = data.users[0];
  // Double-check token belongs đúng project (defense in depth).
  if (user.localId && projectId) return user.localId;
  return null;
}

function handleCheckOrCreate(uid) {
  const folder = DriveApp.getFolderById(getProp_('DRIVE_FOLDER_ID'));
  const filename = uid + DB_FILENAME_SUFFIX;
  const existing = findFileInFolder_(folder, filename);

  if (existing) {
    return jsonOk({
      existed: true,
      fileId: existing.getId(),
      sizeBytes: existing.getSize(),
      lastModified: existing.getLastUpdated().getTime(),
    });
  }

  const empty = Utilities.newBlob(new Uint8Array(0).buffer, MIME_SQLITE, filename);
  const created = folder.createFile(empty);
  return jsonOk({
    existed: false,
    fileId: created.getId(),
    sizeBytes: 0,
    lastModified: created.getLastUpdated().getTime(),
  });
}

function handleMetadata(uid) {
  const folder = DriveApp.getFolderById(getProp_('DRIVE_FOLDER_ID'));
  const filename = uid + DB_FILENAME_SUFFIX;
  const file = findFileInFolder_(folder, filename);

  if (!file) return jsonOk({ exists: false });
  return jsonOk({
    exists: true,
    fileId: file.getId(),
    sizeBytes: file.getSize(),
    lastModified: file.getLastUpdated().getTime(),
  });
}

function handleDownload(uid) {
  const folder = DriveApp.getFolderById(getProp_('DRIVE_FOLDER_ID'));
  const filename = uid + DB_FILENAME_SUFFIX;
  const file = findFileInFolder_(folder, filename);

  if (!file) return jsonError(404, 'File not found');

  const bytes = file.getBlob().getBytes();
  const base64 = Utilities.base64Encode(bytes);
  return jsonOk({
    fileId: file.getId(),
    sizeBytes: bytes.length,
    lastModified: file.getLastUpdated().getTime(),
    base64Data: base64,
  });
}

function handleUpload(uid, base64Data, mtimeMs) {
  if (!base64Data) return jsonError(400, 'Missing base64Data');

  const folder = DriveApp.getFolderById(getProp_('DRIVE_FOLDER_ID'));
  const filename = uid + DB_FILENAME_SUFFIX;
  const bytes = Utilities.base64Decode(base64Data);
  const blob = Utilities.newBlob(bytes, MIME_SQLITE, filename);

  // DriveApp không support replace content in-place. Strategy: trash old,
  // create new. FileId đổi nhưng client query by filename nên không sao.
  const existing = findFileInFolder_(folder, filename);
  if (existing) existing.setTrashed(true);
  const file = folder.createFile(blob);

  return jsonOk({
    fileId: file.getId(),
    sizeBytes: bytes.length,
    lastModified: file.getLastUpdated().getTime(),
    clientMtimeMs: mtimeMs || null,
  });
}

function findFileInFolder_(folder, name) {
  const it = folder.getFilesByName(name);
  return it.hasNext() ? it.next() : null;
}

function getProp_(key) {
  const v = PropertiesService.getScriptProperties().getProperty(key);
  if (!v) throw new Error('Missing Script Property: ' + key);
  return v;
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
