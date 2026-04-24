# Update Workflow — A → Z

Tài liệu mô tả cách app ThongKeShopee tự update và quy trình release/build cho dev.

---

## 1. Cơ chế update đang dùng

**Tauri Updater** (plugin `@tauri-apps/plugin-updater`):
- App đang chạy → user click "Kiểm tra cập nhật" (hoặc auto check)
- Plugin GET `latest.json` từ GitHub Release
- So semver: nếu version mới > current → return Update object
- User click "Tải" → download bundle (`.exe.nsis.zip` Win, `.app.tar.gz` Mac)
- Verify chữ ký minisign qua public key trong `tauri.conf.json`
- Install + auto relaunch

### Cấu hình hiện tại

`src-tauri/tauri.conf.json`:
```json
{
  "version": "0.4.2",
  "bundle": { "createUpdaterArtifacts": true },
  "plugins": {
    "updater": {
      "pubkey": "<base64 minisign public key>",
      "endpoints": [
        "https://github.com/DungKOD/thongkeshopee/releases/latest/download/latest.json"
      ]
    }
  }
}
```

**Endpoint** trỏ vào release **mới nhất** (không phải draft, không phải prerelease).

**Public key** đã hardcode trong app — bundle phải sign bằng private key tương ứng (lưu trong GitHub Secrets `TAURI_SIGNING_PRIVATE_KEY`).

### CI artifacts khi tag `v*`

Workflow `.github/workflows/build.yml` matrix build 3 platform:
- `macos-arm64` (M1/M2/M3 Mac)
- `macos-x86_64` (Intel Mac)
- `windows-x64` (NSIS installer)

Khi push tag `v*`, `tauri-action` tự động:
1. Build bundle cho mỗi platform
2. Sign bằng `TAURI_SIGNING_PRIVATE_KEY` → tạo file `.sig`
3. Tạo **Draft Release** trên GitHub với artifacts:
   - `ThongKeShopee_0.4.3_x64-setup.nsis.zip` + `.sig`
   - `ThongKeShopee_0.4.3_aarch64.app.tar.gz` + `.sig`
   - `ThongKeShopee_0.4.3_x64.app.tar.gz` + `.sig`
   - `latest.json` — manifest cho updater (URLs + sigs + version + notes)
4. Upload artifacts vào Actions tab (cho dev download)

---

## 2. Quy trình release bản update (production)

### Step-by-step

**Trước khi release**:
- [ ] Đảm bảo branch master clean, tests pass (`cargo test --lib --quiet`, `npm run build`)
- [ ] Test app local (`npm run tauri dev`) — feature mới hoạt động đúng
- [ ] Review commit log từ tag cũ → nay (`git log v0.4.2..HEAD --oneline`)

**Bump version** (1 commit duy nhất):
```bash
# Sửa version trong 2 file (giữ đồng bộ):
# - src-tauri/tauri.conf.json: "version": "0.4.3"
# - package.json:              "version": "0.4.3"

# Cập nhật src-tauri/Cargo.toml nếu có version (optional)
# - src-tauri/Cargo.toml:      version = "0.4.3"

git add src-tauri/tauri.conf.json package.json src-tauri/Cargo.toml
git commit -m "chore: bump version 0.4.3"
git push
```

**Tag + push**:
```bash
git tag v0.4.3
git push origin v0.4.3
```

→ CI tự fire (matrix build + tag detected → tauri-action tạo Draft Release).

**Chờ CI build xong** (~15-30 phút):
- Actions tab → workflow "Build Tauri App" → 3 jobs `build (matrix)` xanh
- Releases tab → thấy "Draft" release mới với artifacts đầy đủ

**Edit Draft Release** (manual):
1. Vào Releases → click vào Draft mới
2. Edit notes — mô tả tính năng/fix:
   ```markdown
   ## Tính năng mới
   - Hybrid trigger debounce 45s + count 100 + max-wait 5min
   - Snapshot restore tự động khi local clock tụt sau remote

   ## Fixes
   - Multi-device presence không còn đè nhau
   - FB import file rỗng không đè value cũ
   ```
3. Verify "Set as latest release" checkbox CHECKED
4. KHÔNG check "Set as pre-release"
5. Click **Publish release**

**Sau publish**:
- Endpoint `releases/latest/download/latest.json` redirect tới release mới
- User mở app → tự check → thấy update available → install
- Hoặc user vào Settings → "Kiểm tra cập nhật" → manual trigger

### Verify đã hoạt động

1. Mở app version cũ (vd 0.4.2)
2. Click "Kiểm tra cập nhật" trong UpdatesDropdown
3. Thấy notification "Có bản 0.4.3" → click "Cập nhật"
4. Download progress → install → app relaunch ở 0.4.3

---

## 3. Quy trình build mà KHÔNG release (dev/test)

**Khi nào**:
- Đang test feature chưa stable
- Cần artifact để cài thử trên máy khác trước khi release chính thức
- Code đang refactor lớn, chưa muốn user nâng cấp

**Cách 1: Push lên master không tag**
```bash
git push origin master   # KHÔNG tag
```
→ CI chạy:
- Preflight (test + typecheck) — gate regression
- Matrix build 3 platform
- Upload artifacts vào Actions tab (retention 14 ngày)
- **KHÔNG tạo Release** (vì không có tag)
- **KHÔNG update `latest.json`** → user không thấy update

**Download artifact để cài thử**:
1. Actions tab → workflow run → scroll xuống "Artifacts"
2. Download `windows-x64.zip` / `macos-arm64.zip` / `macos-x86_64.zip`
3. Unzip → tìm bundle file (.nsis.zip / .app.tar.gz / .msi)
4. Cài tay trên máy test

**Cách 2: Manual trigger workflow (workflow_dispatch)**
- Actions tab → "Build Tauri App" → "Run workflow" → chọn branch
- CI build cho branch đó (kể cả non-master)
- Vẫn upload artifacts only, không release

**Cách 3: Tag prerelease**
```bash
git tag v0.4.3-beta.1
git push origin v0.4.3-beta.1
```
→ Tạo Draft release. **KHÔNG publish** → endpoint `latest` vẫn trỏ về release stable cũ → user không thấy update. Beta tester có thể download artifact từ Draft release link share.

⚠️ Không check "Set as pre-release" và publish — sẽ làm endpoint `releases/latest` shift sang prerelease. Để Draft.

---

## 4. Khi nào tạm thời tắt auto-update

### Scenario: Đang refactor lớn, không muốn user upgrade

**Option A — Đơn giản nhất**: Đừng tag mới. Push code lên master → CI build artifacts nhưng không release. User mở app → check update → thấy version hiện tại == latest → no update.

**Option B — Disable update check trong app**:
- Comment out call `checkForUpdate()` trong `UpdatesDropdown.tsx`
- Bump version + tag → release bản "no update check"
- Sau khi xong refactor → bump version + bật lại check

**Option C — Gỡ endpoint trong build**:
- Xóa hoặc comment `endpoints` trong `tauri.conf.json`
- Build + release → app mới sẽ throw error khi check (bắt error → silent)
- Không khuyến nghị: user version cũ vẫn check endpoint, vẫn thấy version mới

**Khuyến nghị**: dùng Option A. Đừng tag = đừng release = updater không thấy gì mới.

### Scenario: Phát hiện bug critical sau khi vừa release

1. **Kéo release vừa publish về Draft** (Releases → release vừa publish → Edit → uncheck "Set as latest" → Save)
2. Endpoint `releases/latest` shift về release trước đó (vd v0.4.2 nếu đã có)
3. User cài bản lỗi rollback bằng cách reinstall manual từ artifact cũ
4. Fix bug → bump patch version (v0.4.3 → v0.4.4) → tag + release lại

### Scenario: Cần stop user upgrade ngay (rollback urgent)

1. **Delete release** (Releases → release lỗi → Delete)
2. Endpoint `releases/latest` về release stable trước
3. User mới cài app → vẫn cài bản cũ
4. User đã upgrade lên bản lỗi → cần manual reinstall

⚠️ Lưu ý: tag và artifact files vẫn còn trên Git. Chỉ release entry bị xóa. Tag có thể delete riêng:
```bash
git push --delete origin v0.4.3
git tag -d v0.4.3
```

---

## 5. CI/CD có cần thay đổi gì không?

**Không cần thay đổi** cho cả 2 trường hợp release / không release. Workflow hiện tại đã đúng:

| Trigger | Behavior | Release? |
|---|---|---|
| `push` lên `master` (không tag) | preflight + matrix build + artifacts | ❌ |
| `push` tag `v*` | preflight + matrix build + Draft Release auto | ✓ Draft |
| `workflow_dispatch` (manual) | preflight + matrix build + artifacts | ❌ |
| Pull Request | (không trigger — chỉ master/tags) | ❌ |

Logic trong workflow:
```yaml
tagName: ${{ startsWith(github.ref, 'refs/tags/v') && github.ref_name || '' }}
```
→ Chỉ tạo release khi ref là tag `v*`. Push commit thường thì `tagName` = empty → tauri-action skip tạo release, chỉ upload artifacts.

### Chỉ thay đổi CI khi:

- **Đổi platform** (vd thêm Linux, ARM Windows): sửa `matrix.include`
- **Đổi update endpoint** (vd self-host thay GitHub): sửa `tauri.conf.json` + có thể setup workflow upload `latest.json` lên server riêng
- **Đổi signing key**: rotate `TAURI_SIGNING_PRIVATE_KEY` trong Secrets — nhưng app cũ với pubkey cũ sẽ KHÔNG verify được bundle mới ⚠️
- **Đổi release strategy** (vd auto-publish thay Draft): đổi `releaseDraft: true` → `false` (không khuyến nghị, mất chance review notes)

---

## 6. Quy trình cụ thể theo case

### Case 1: Release minor feature (vd 0.4.2 → 0.4.3)

```bash
# 1. Verify local
cargo test --lib --quiet
npm run build
npm run tauri dev   # Test feature manually

# 2. Bump version
# Edit tauri.conf.json:  "version": "0.4.3"
# Edit package.json:     "version": "0.4.3"
git add -A
git commit -m "chore: bump 0.4.3"

# 3. Tag + push
git push
git tag v0.4.3
git push origin v0.4.3

# 4. Chờ CI ~20 phút
# Actions tab → workflow xanh

# 5. Edit Draft Release trên GitHub
# - Write release notes
# - Verify artifacts đầy đủ (3 platform + latest.json + .sig files)
# - Set as latest release: ✓
# - Publish

# 6. Verify update flow
# Mở app cũ → Kiểm tra cập nhật → Update → Install → app relaunch ở 0.4.3
```

### Case 2: Hot fix critical bug (0.4.3 → 0.4.4)

```bash
# 1. Fix bug + commit
git commit -m "fix: critical bug X"

# 2. Bump patch
# tauri.conf.json + package.json: "0.4.4"
git commit -am "chore: bump 0.4.4"

# 3. Tag + push
git push
git tag v0.4.4
git push origin v0.4.4

# 4. CI build → Edit Draft → Publish ngay (không cần waiting beta)

# 5. Tổng thời gian từ commit fix → user nhận update ≈ 30 phút
```

### Case 3: Đang refactor lớn, không muốn user upgrade

```bash
# Cứ commit + push master
git commit -m "wip: refactor sync"
git push

# CI build artifacts (lưu Actions, không release)
# Tester download artifact từ Actions tab cài máy mình
# User existing → mở app → no update available (vì không tag)

# Khi refactor xong, ổn định:
git commit -m "chore: bump 0.5.0 — sync v10"
git tag v0.5.0
git push origin v0.5.0
# → release như Case 1
```

### Case 4: Build cho beta tester nhỏ trước khi public release

```bash
# Tag prerelease
git tag v0.4.3-beta.1
git push origin v0.4.3-beta.1

# CI tạo Draft release v0.4.3-beta.1
# Edit Draft:
#   - Write notes "Beta — please test feature X"
#   - Set as pre-release: ✓
#   - Set as latest release: ❌ (KHÔNG)
#   - Publish

# Endpoint `releases/latest` vẫn trỏ về v0.4.2 stable
# → User không thấy beta. Beta tester download artifact từ Releases tab manual.

# Sau test OK → bump bản chính thức:
git tag v0.4.3
git push origin v0.4.3
# → Release stable như Case 1
```

### Case 5: Release đã publish có bug, cần rollback

```bash
# 1. Vào Releases trên GitHub
# 2. Click vào release v0.4.3 (lỗi) → Edit
# 3. Uncheck "Set as latest release" → Save
# 4. Endpoint /latest sẽ shift về release v0.4.2 (latest cũ)

# Tester reinstall manual từ artifact v0.4.2:
# Releases → v0.4.2 → download bundle → cài lại

# Fix + release v0.4.4:
# (Case 1 flow)
```

---

## 7. Troubleshooting

### Build CI fail

**Preflight fail**:
- TS error → fix code, push lại
- Cargo test fail → fix code, push lại
- Tauri Linux deps install fail → check workflow yml step "Install Tauri Linux deps"

**Matrix build fail**:
- Missing secret → check Settings → Secrets → secret tên đúng
- Tauri-action sign fail → check `TAURI_SIGNING_PRIVATE_KEY` + password trong Secrets

### User không thấy update

1. Check release đã publish chưa (không phải Draft)
2. Check "Set as latest release" CHECKED
3. Check `latest.json` artifact có trong release
4. Check version trong `latest.json` > version cài
5. Open Settings → log net_log → xem có request tới `releases/latest/download/latest.json` không (404 hoặc 200)

### Update download fail / signature mismatch

- Public key trong app build cũ ≠ key bundle hiện tại → reinstall manual
- Khuyến nghị: KHÔNG rotate signing key trừ khi bắt buộc (security incident)

### App version local sai (build dev = production version)

- Đảm bảo bump cả 2 file: `tauri.conf.json` + `package.json`
- `cargo clean` trước build nếu cache stale
- Hard refresh browser cache nếu dev mode (Ctrl+Shift+R)

---

## 8. Tổng kết

**TL;DR**:
- **Tag `v*` = release**. Push commit không tag = build only, không release.
- CI/CD đã đúng cho cả 2 case, **không cần thay đổi**.
- Tạm thời không muốn release: **đừng tag**. User không thấy update.
- Bug critical: rollback bằng cách uncheck "latest" trên release lỗi.

**Files liên quan**:
- `.github/workflows/build.yml` — CI workflow
- `src-tauri/tauri.conf.json` — version + updater config
- `package.json` — version (giữ đồng bộ với tauri.conf.json)
- `src/lib/updater.ts` — FE wrapper plugin-updater
- `src/components/UpdatesDropdown.tsx` — UI check + download
