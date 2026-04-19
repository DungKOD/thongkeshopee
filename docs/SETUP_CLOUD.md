# Setup Firebase + Apps Script (bản đơn giản, free, không thẻ)

Hướng dẫn dành cho **admin** (chủ app) — làm **1 lần**.

**Thời gian: ~30 phút**. Không cần thẻ, không cần Blaze.

---

## 📋 Checklist

- [ ] 1. Tạo Firebase project
- [ ] 2. Bật Authentication (Google + Email/Password)
- [ ] 3. Tạo Firestore database
- [ ] 4. Lấy Firebase config Web
- [ ] 5. Tạo folder Drive `ShopeeStatData`
- [ ] 6. Tạo Apps Script project + deploy Web App
- [ ] 7. Tạo file `.env.local` chứa credentials
- [ ] 8. Test flow

---

## Bước 1 — Tạo Firebase project

1. Mở https://console.firebase.google.com → **Add project**
2. Tên: `thongke-shopee` → **Continue**
3. Tắt Google Analytics → **Create project** → chờ 30s

📝 Ghi lại **Project ID** (ví dụ `thongke-shopee-a1b2c`).

---

## Bước 2 — Bật Authentication

1. **Build → Authentication → Get started**
2. Tab **Sign-in method** → bật:
   - **Email/Password** → Enable → Save
   - **Google** → Enable → chọn support email → Save

---

## Bước 3 — Tạo Firestore Database

1. **Build → Firestore Database → Create database**
2. Location: **`asia-southeast1 (Singapore)`** (không đổi được sau)
3. **Production mode** → Enable

### Security rules

Vào tab **Rules**, paste đoạn sau, **Publish**:

```
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    match /users/{userId} {
      // User tự đọc doc của mình
      allow read: if request.auth.uid == userId;

      // User tự tạo doc lần đầu — chỉ với premium=false
      allow create: if request.auth.uid == userId
                    && request.resource.data.premium == false
                    && request.resource.data.email == request.auth.token.email;

      // Không ai được write từ client (admin sửa qua Console)
      allow update, delete: if false;
    }
  }
}
```

---

## Bước 4 — Lấy Firebase config Web

1. **Project settings** (icon ⚙️ góc trên trái) → tab **General**
2. Cuộn xuống **Your apps** → bấm **</> (Web)**
3. Nickname: `ThongKe Shopee Desktop` → **Register**
4. Copy config object và ghi lại:

```js
{
  apiKey: "AIzaSy...",
  authDomain: "thongke-shopee-a1b2c.firebaseapp.com",
  projectId: "thongke-shopee-a1b2c",
  storageBucket: "...",
  messagingSenderId: "...",
  appId: "..."
}
```

---

## Bước 5 — Tạo folder Drive `ShopeeStatData`

1. Mở https://drive.google.com (**đăng nhập tài khoản Google chủ** — cùng account vừa tạo Firebase)
2. **+ New → Folder** → tên `ShopeeStatData`
3. Mở folder → copy **Folder ID** từ URL:
   ```
   https://drive.google.com/drive/folders/1AbCd...XyZ
                                          ^^^^^^^^^^^ Folder ID
   ```

📝 Ghi lại **Folder ID**.

> ℹ️ Không cần share cho ai. Apps Script sẽ chạy dưới tài khoản này → có full quyền tự động.

---

## Bước 6 — Tạo Apps Script project

Apps Script = "server miễn phí" của Google. Nó chạy dưới tài khoản của bạn → truy cập Drive của bạn native.

### 6a. Tạo project

1. Mở https://script.google.com → **New project**
2. Đổi tên (góc trên trái) từ `Untitled` → `ThongKeShopee API`
3. File `Code.gs` tự hiện. **Xóa sạch nội dung**, mở file **`server/apps-script/Code.gs`** trong project này, copy toàn bộ nội dung, paste vào Apps Script editor
4. Save (Ctrl+S)

### 6b. Gắn config vào Script Properties

1. Sidebar trái → **⚙️ Project Settings** → cuộn xuống **Script Properties** → **Add script property**
2. Thêm **3 property**:
   - Key: `FIREBASE_PROJECT_ID` → Value: `thongke-shopee-a1b2c` (project id của bạn)
   - Key: `FIREBASE_API_KEY` → Value: `AIzaSy...` (apiKey từ Firebase Web config, Bước 4)
   - Key: `DRIVE_FOLDER_ID` → Value: `1AbCd...XyZ` (folder id từ bước 5)
3. **Save script properties**

### 6c. Deploy làm Web App

1. Góc trên phải: **Deploy → New deployment**
2. Icon bánh răng cạnh "Select type" → chọn **Web app**
3. Config:
   - Description: `v1`
   - Execute as: **Me (email của bạn)** ← **QUAN TRỌNG**, phải là "Me"
   - Who has access: **Anyone** ← app sẽ tự xác thực qua Firebase token
4. **Deploy**
5. Popup "Authorize access" → **Review permissions** → chọn account → **Advanced → Go to ThongKeShopee API (unsafe)** → **Allow**
   > ⚠️ Màn "unsafe" là chuyện bình thường với Apps Script tự tạo. Click Allow.
6. Copy **Web App URL** dạng `https://script.google.com/macros/s/AKfyc.../exec`

📝 Ghi lại **WEB_APP_URL**.

> ℹ️ Mỗi lần update code, phải **Deploy → Manage deployments → ✏️ edit → New version → Deploy**. URL không đổi.

---

## Bước 7 — Tạo `.env.local`

File **D:\RUSTS\ThongKeShopee\.env.local** (sẽ tự động nằm trong `.gitignore`):

```env
# Firebase Web config (PUBLIC — OK nhúng client build)
VITE_FIREBASE_API_KEY=AIzaSy...
VITE_FIREBASE_AUTH_DOMAIN=thongke-shopee-a1b2c.firebaseapp.com
VITE_FIREBASE_PROJECT_ID=thongke-shopee-a1b2c
VITE_FIREBASE_STORAGE_BUCKET=thongke-shopee-a1b2c.appspot.com
VITE_FIREBASE_MESSAGING_SENDER_ID=123456789
VITE_FIREBASE_APP_ID=1:123456789:web:abcdef

# Apps Script endpoint
VITE_APPS_SCRIPT_URL=https://script.google.com/macros/s/AKfyc.../exec

# Admin contact (hiện trên Paywall)
VITE_ADMIN_ZALO=0912345678
VITE_ADMIN_NOTE=Chuyển khoản xong nhắn Zalo gửi email để kích hoạt
```

---

## Bước 8 — Test flow end-to-end

Sau khi tôi code xong (các task sau), flow sẽ là:

1. Mở app → màn Login
2. Đăng nhập Google → app tạo doc `users/{uid}` với `premium: false`
3. App hiện màn Paywall: "Chưa kích hoạt. Zalo: 0912... Email: abc@gmail.com [Copy]"
4. Bạn (admin) mở Firebase Console → Firestore → `users/{uid}` → sửa:
   - `premium: true`
   - `expiredAt: <timestamp 30 ngày sau>`
5. App user nhận realtime → unlock trong 1-2 giây
6. App gọi Apps Script → check Drive có `{uid}.db` chưa → chưa có thì tạo mới
7. User dùng app bình thường → data local SQLite
8. Định kỳ 5 phút / exit app → Apps Script upload DB lên Drive

---

## ✅ Báo lại tôi khi xong

Reply với:

```
PROJECT_ID = ...
DRIVE_FOLDER_ID = ...
WEB_APP_URL = https://script.google.com/macros/s/.../exec
ADMIN_ZALO = ...
Firebase config: [đã copy chưa]
```

Tôi sẽ tiếp tục code Tasks 2→11.

---

## 🔐 Lưu ý bảo mật

| Thứ | Commit git? | Ghi chú |
|---|---|---|
| `.env.local` | ❌ KHÔNG | Public-safe nhưng không cần commit |
| Firebase config | ✅ Sẽ nhét vào .env.local | API key Firebase là public, KHÔNG phải secret |
| Apps Script URL | ✅ Sẽ nhét vào .env.local | Bảo vệ bằng Firebase ID token verify bên trong |
| Firestore rules | ✅ Commit `firestore.rules` | Source code |
| Apps Script code | ⚠️ Optional commit | Copy-paste từ `server/apps-script/Code.gs` vào Apps Script UI khi update |

Không có file key/secret thật sự nhạy cảm trong setup này.
