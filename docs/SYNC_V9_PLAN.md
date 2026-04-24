# Sync v9 — Per-table Incremental Delta Plan

**Status:** ✅ **DECISIONS LOCKED 2026-04-24.** Ready to implement when user says go.
**Last updated:** 2026-04-24
**Context:** App chưa có user thật → free hand, không cần backward-compat với v8.

## Nguyên tắc bất khả xâm phạm (user locked 2026-04-24)

Mọi implementation phase phải tuân thủ 3 nguyên tắc này. Xung đột với các spec khác → 3 nguyên tắc này THẮNG:

### 1. 🛡️ ƯU TIÊN GIỮ DATA — không được mất data trong mọi trường hợp

**Cụ thể:**
- **Edit thắng delete** (resurrect rule): tombstone KHÔNG xóa row nếu `row.updated_at > tombstone.deleted_at`
- **Local-wins merge**: pull không bao giờ `DELETE` local row chưa sync lên remote. Chỉ tombstone mới trigger delete, và tombstone phải qua resurrect check
- **Fresh-install guard**: local DB empty + có sync_cursor_state rỗng + remote manifest tồn tại → BẮT BUỘC vào bootstrap mode (pull only), KHÔNG push empty state đè remote
- **Upload-before-switch**: logout/switch user → push dirty deltas TRƯỚC KHI swap DbState. Nếu push fail → block logout, hiện warning
- **Atomic snapshot restore**: download snapshot vào `{db}.pending`, verify schema + owner_uid + integrity (PRAGMA integrity_check) → mới swap với live DB. Fail giữa chừng = giữ live DB cũ
- **Transactional delta apply**: mỗi delta file apply trong 1 TX. Crash/error giữa chừng → ROLLBACK, retry từ đầu file (idempotent via INSERT OR IGNORE)
- **Compaction safety**: sinh snapshot mới + verify integrity + upload OK → **rồi mới** xóa delta files cũ. Fail ở bất kỳ bước nào = giữ nguyên deltas
- **Admin delete user**: 2-step confirm UI + trước khi DELETE R2 objects phải clone snapshot vào `archive/deleted_{uid}_{ts}/` (giữ 30 ngày) để recovery. **User KHÔNG có quyền self-delete** (Q7=B) — mọi delete data phải qua admin
- **Bump `updated_at` trên mọi mutation**: dùng `next_hlc_rfc3339(&tx)` trong cùng transaction. Không dùng `Utc::now()` cho sync-critical timestamps
- **Tombstone trước, raw xóa sau**: delete day → emit tombstone delta TRƯỚC, CASCADE delete raw rows SAU, cùng TX

**Test requirement:** Phase 9 integration test phải cover các kịch bản mất-data tiềm năng và verify recovery.

### 2. ⚙️ ĐẢM BẢO LOGIC HIỆN TẠI — không phá business rules

**Preserve nguyên vẹn:**
- Day-centric import (1 CSV = 1 ngày, mix → reject)
- Display name theo sub_id (không dùng item_name)
- Prefix-matching sub_id (FB + Shopee tuple merge theo prefix, dài hơn làm canonical)
- Staged delete UX (gạch ngang → "Lưu thay đổi" mới commit DB)
- UI filter row 0 (chỉ hiện row có spend hoặc commission ≠ 0)
- DB value priority (field trong DB bắt buộc lấy trực tiếp, không tính lại)
- Sub_id & date conventions (FB campaign name hyphen-joined, order date = DATE(order_time))
- KPI từ `UiDay.totals` pre-filter
- Multi-tenant DB isolation (`users/{uid}/` folder, `switch_db_to_user`, SettingsProvider key remount, wipeUserLocalStorage)
- Video logs.db per-user scope
- Firebase Auth + RTDB admin whitelist
- Import preview flow (parse → preview → validate cross-date → confirm → commit)
- DB mutation rule (bump sync + cleanup orphan references)

**v9 chỉ thay format sync (full-DB snapshot → per-table delta). Business logic UI/import/KPI KHÔNG động vào.**

### 3. 💰 TỐI ƯU REQUEST + STORAGE — giảm chi phí Cloudflare

**Cụ thể:**
- **Skip-identical hash per table**: nếu `hash(delta_content) == sync_cursor_state.last_uploaded_hash` → skip upload (save Class A op)
- **Manifest etag cache**: FE cache manifest + etag. Gọi `HEAD` trước khi `GET` để check etag đổi không (nếu Worker support) HOẶC chỉ fetch manifest khi có trigger (dirty local, user manual sync, periodic)
- **No-op sync detection**: nếu `sync_cursor_state` tất cả tables đều `last_uploaded_cursor >= current max cursor` → skip push entirely
- **Batch multiple events trong 1 delta file**: 1 import CSV = 1 file delta (không phải 1 event = 1 file)
- **zstd L3 multi-thread**: balance nén/CPU tốt hơn L9
- **Compaction khi delta count > 100**: giảm manifest size + giảm số GET ops bootstrap
- **Debounce sync trigger**: 2s debounce cho "Sync now" button; auto-sync interval ≥ 5 phút
- **Lazy snapshot download**: chỉ fetch snapshot khi bootstrap (local empty) — sync thường chỉ pull deltas
- **Manifest size cap**: nếu manifest.deltas > 500 entries → trigger compaction ngay, không đợi scheduled
- **Storage GC**: compaction xong, xóa delta files cũ hơn snapshot (best-effort; giữ 7 ngày grace period để rollback nếu snapshot bị lỗi)

**Target cost (100 user × 30 sync/ngày):** dưới $2/tháng (chủ yếu là R2 storage). Operations đều phải nằm trong free tier.

### 4. 🧹 XÓA CODE CŨ KHI V9 THAY XONG (user yêu cầu 2026-04-24)

**Nguyên văn user:** *"code cũ không dùng nữa nhớ phải loại bỏ đi nhé"*

**Nguyên tắc:**
- Không giữ dead code v8 sau khi phase v9 tương ứng deploy.
- Không giữ "fallback về v8" vì app chưa có user thật — không cần backward-compat.
- Mỗi phase replace xong module → phase tiếp hoặc phase chuyên-cleanup delete code cũ.
- Cleanup là **work item bắt buộc**, không phải tùy chọn. Tracked trong commit message + Phần 6.1 schedule.

**Cụ thể — những gì sẽ xóa:**

| v8 code | Xóa ở phase | Thay bằng |
|---|---|---|
| `src-tauri/src/commands/sync.rs` (1,611 LOC) | P8 (commands wiring) | `src-tauri/src/sync_v9/*` + `commands/sync_v9_cmds.rs` |
| `src-tauri/src/commands/sync_client.rs` (378 LOC) | P8 | `sync_v9/client.rs` |
| `sync_state` columns v8: `dirty`, `change_id`, `last_uploaded_change_id`, `last_remote_etag`, `last_uploaded_hash` | P8 (migration v12) | `sync_cursor_state` + `sync_manifest_state` (đã có v11) |
| Triggers bump change_id trên raw/manual/tombstone tables | P8 (migration v12) | Cursor-based capture (no triggers) |
| `worker/src/index.ts` v8 endpoints (`/upload`, `/download`, `/metadata`) | P6 | `/v9/manifest/*`, `/v9/delta/*`, `/v9/snapshot/*`, `/v9/sync-log/*` |
| `worker/src/admin.ts` v8 endpoints | P8 | `/v9/admin/*` |
| `src/lib/sync.ts` (183 LOC) | P7 | `src/lib/sync_v9.ts` |
| `src/hooks/useCloudSync.ts` (596 LOC) | P7 | rewrite |
| `src-tauri/src/commands/sync.rs::next_hlc_ms` / `next_hlc_rfc3339` / `absorb_remote_clock` | P8 | Move sang `sync_v9/hlc.rs` (KHÔNG delete, chỉ relocate — v9 reuse) |
| FE `SyncBadge` v8-specific logic | P7 | Adapt cho v9 state model |

**Exception (KHÔNG xóa):**
- HLC helpers (`next_hlc_ms`, `next_hlc_rfc3339`, `absorb_remote_clock`) — v9 dùng nguyên vẹn. Relocate sang `sync_v9/hlc.rs`, giữ tests.
- `switch_db_to_user` + `apply_pending_sync` — multi-tenant isolation, không thuộc sync engine.
- `machine_fingerprint` — dùng cho log + admin, không thuộc sync.
- Admin view state + admin_list_users logic business — keep, chỉ endpoint HTTP path đổi.

---

## TL;DR

- R2 vẫn là storage backend duy nhất. Không thêm D1/Postgres.
- Thay vì upload/download FULL SQLite file mỗi sync, sync ở mức **per-table delta** (NDJSON batches).
- Keep all sống sót logic từ v8: HLC clock, tombstone resurrect, multi-tenant folder, switch_db_to_user, CAS (now trên manifest).
- Rewrite core sync engine (~3,300 LOC): `sync.rs`, `sync_client.rs`, Worker endpoints, `useCloudSync.ts`.
- **Lý do migrate:** DB có thể đạt 1-2GB trong 6-12 tháng → Worker body limit 100MB (free) / 500MB (paid) → v8 full-DB upload fail ở ngưỡng đó.

---

## Phần 1: User scenarios (cases phải cover)

⚠ = case v8 từng gặp bug, phải giữ cover trong v9.

### A. Auth & session (multi-tenant)

| # | Case | Yêu cầu |
|---|---|---|
| A1 | Lần đầu login, chưa có local + chưa có remote | Khởi tạo empty, không fail |
| A2 | Login trên máy mới, remote có data | Bootstrap từ snapshot + deltas |
| A3 | Login lại trên máy cũ | Pull deltas mới, push deltas pending |
| A4 ⚠ | Logout A → login B cùng máy | Isolate folder `users/{uid}/`; clear React state (SettingsProvider remount) |
| A5 ⚠ | Login A → login B → login A lại | Không leak state B sang A |
| A6 ⚠ | Admin view user C → exit → refetch phải dùng DB admin, không phải C |
| A7 | Pending deltas chưa push khi logout | Giữ trong folder A, không mất. Lần sau login A push tiếp |
| A8 | JWT expire giữa sync | Refresh token, resume cùng cursor |
| A9 ⚠ | User bị revoke admin giữa session admin view | Endpoint reject, exit view mode |

### B. Sync convergence (2+ máy cùng account)

| # | Case | Yêu cầu |
|---|---|---|
| B1 | Máy A import CSV → push → máy B pull | B thấy đúng data |
| B2 | 2 máy import cùng 1 file (cùng hash) | Idempotent, remote không dup |
| B3 | 2 máy import khác file cùng ngày | Merge cả 2 file vào cùng day |
| B4 ⚠ | 2 máy edit cùng `manual_entry` | HLC: edit sau thắng |
| B5 ⚠ | Máy A delete day, máy B edit manual_entry trong day đó | Day tombstone unconditional cascade (xóa thắng — giữ v8 rule) |
| B6 ⚠ | Máy A delete manual_entry, máy B edit sau | Resurrect: edit thắng (giữ v8 rule) |
| B7 | 2 máy push đồng thời | CAS trên manifest (delta file immutable, không cần CAS per-file) |
| B8 | 3 máy cùng active | N-way converge qua manifest + delta replay |
| B9 ⚠ | Clock máy A chậm 10 phút vs B | HLC monotonic, B không lose merge |
| B10 | Máy A offline 1 tuần, edit → về mạng | Push tất cả dirty deltas, merge remote deltas tuần đó |

### C. Bootstrap & recovery

| # | Case | Yêu cầu |
|---|---|---|
| C1 | Máy mới, remote có 6 tháng data | Fetch snapshot gần nhất + deltas sau snapshot |
| C2 ⚠ | Reinstall cùng máy (local DB bị xóa) | Detect fresh install → pull full, KHÔNG push empty đè remote |
| C3 | Format máy, MachineGuid đổi | Fingerprint mới nhưng cùng UID → treat như máy mới |
| C4 | Local DB corrupt | Nút "reset local, pull từ cloud" → xóa local DB, fresh bootstrap |
| C5 | Remote bị admin xóa (cleanup-orphans) | Local có data → next sync push lên lại |
| C6 | Admin muốn download full DB user X | Build snapshot on-the-fly từ chain (hoặc dùng snapshot đã compacted) |

### D. Data integrity

| # | Case | Yêu cầu |
|---|---|---|
| D1 | Delta upload thành công, cursor state local fail update → retry upload | Idempotent (cùng file key = overwrite OK) |
| D2 | Delta apply nửa chừng crash | Transaction per delta file; crash = rollback, retry apply từ đầu file |
| D3 | FK gãy: raw_clicks ref imported_files chưa có | Upload order: imported_files trước, raw sau |
| D4 ⚠ | Delete day → raw_* rows xóa + imported_files xóa + sync_state bumped | Delta phải emit tombstone + imported_files delete cùng transaction |
| D5 | Tombstone apply nhưng raw rows remote chưa có (máy khác push sau) | Apply-time check: raw row có day_date = tombstone day → skip insert |
| D6 | Import lại file đã import | File hash UNIQUE → local skip. Push delta chỉ nếu thực sự mới |

### E. Performance & network

| # | Case | Yêu cầu |
|---|---|---|
| E1 | Import CSV 100MB raw → 1 delta file 100MB | Split theo batch 5MB/file |
| E2 | Fresh install kéo 50 delta files | Parallel download concurrency 3-5 |
| E3 | Upload chậm vì 3G | Progress per-file; resume được nếu fail giữa chừng |
| E4 | User click "Sync now" liên tục | Debounce 2s; đang sync → ignore |
| E5 | User edit trong khi sync đang chạy | Edit vào DB bình thường (không lock). Sync next cycle push |
| E6 | Delta history quá nhiều (500+ files sau 1 năm) | Compaction tạo snapshot mới, xóa deltas cũ |

### F. Security & abuse

| # | Case | Yêu cầu |
|---|---|---|
| F1 | User A cố upload vào path `users/{B_uid}/` | Worker assert JWT claim `uid == path uid` |
| F2 ⚠ | Non-admin cố gọi admin endpoints | `assert_admin_claim` trên tất cả `/admin/*` |
| F3 | User edit JWT bypass | Firebase Auth verify signature, reject invalid |
| F4 | R2 quota hit | Worker trả 507, FE show banner "dung lượng đầy" |

### G. Admin tooling

| # | Case | Yêu cầu |
|---|---|---|
| G1 | Admin list users | Query Firestore + R2 ListObjects prefix `users/` |
| G2 | Admin view DB user X | Build/fetch snapshot → download → ATTACH read-only → swap DbState |
| G3 | Admin exit view → back DB của admin | Restore DbState từ `admin_db_path_backup` (v7 đã có) |
| G4 | Admin xóa user đã rời Firebase | cleanup-orphans: delete all R2 objects + Firestore doc |

### H. Multi-device + large DB (CONCRETE SCENARIOS)

#### H1 — 3 máy A/B/C, A offline 15 ngày, B+C thêm 100 changes → A login lại

**Giả định mix 100 changes:**
- 30 manual_entry upsert
- 60 events từ 8 CSV imports (~400k clicks + 40k orders + 8 imported_files rows + mapping tables)
- 10 tombstones

**Delta files trên R2 khi A login:**

| Table | Rows | Delta files (5MB/file) |
|---|---|---|
| `manual_entries` | 30 | 1 |
| `imported_files` | 8 | 1 |
| `raw_shopee_clicks` | 400k × 300B → ~30MB zstd | ~6 |
| `raw_shopee_order_items` | 40k × 400B → ~4MB zstd | 1 |
| `clicks_to_file` | 400k × 32B → ~2MB zstd | 1 |
| `orders_to_file` | 40k × 32B → ~0.3MB | 1 |
| `tombstones` | 10 | 1 |
| **Tổng** | | **~12 delta files** |

**Request flow A:**

```
1. switch_db_to_user(uid)                  → 0 R2 requests (local)
2. GET /v9/manifest/get                    → 1 R2 GET
3. Diff manifest vs local cursor_state     → 0 requests
4. Parallel download 12 delta files        → 12 R2 GET (concurrency 3-5)
5. Apply 12 files trong 1 transaction      → 0 requests
6. Update local cursor_state               → 0 requests
7. Push local dirty (nếu có)               → 0 hoặc vài PUT
```

**Tổng: ~13 R2 requests, ~40MB download.**
**Thời gian:** ~20s trên mạng 20Mbps.

**So với v8:** Upload + download FULL 1GB DB = 2GB bandwidth. Đây giảm **98%**.

#### H2 — DB đã 2GB, login máy D mới toanh

**Không thể chỉ dùng delta replay** — 6 tháng data có thể có hàng nghìn delta files. Bắt buộc có **snapshot compaction**.

**Snapshot compaction:**
- Weekly cron Worker HOẶC client trigger khi delta count > 100:
  1. Apply tất cả deltas vào temp SQLite
  2. `VACUUM INTO` → zstd compress → upload `users/{uid}/snapshots/snap_{clock_ms}.db.zst`
  3. Update manifest: `latest_snapshot_key` + `latest_snapshot_clock_ms`
  4. Delete R2 delta files có `clock_ms < snapshot.clock_ms`

**Máy D bootstrap 2GB DB:**

```
1. switch_db_to_user(uid) → DB empty                        → 0 R2 requests
2. GET /v9/manifest/get                                     → 1 R2 GET
3. Manifest có latest_snapshot + 15 deltas sau snapshot
4. Fetch snapshot (500MB zstd, decompress = 2GB)            → 1 R2 GET
5. Decompress → restore thành local DB
6. Parallel download 15 deltas                              → 15 R2 GET
7. Apply deltas trong transaction
8. Set cursor_state
```

**Tổng: ~17 R2 requests, ~550MB download.**

**Thời gian D bootstrap** (mạng 50Mbps ~6MB/s):

| Bước | Thời gian |
|---|---|
| Download snapshot 500MB | ~85s |
| Decompress zstd 500MB → 2GB | ~25s |
| Write 2GB to disk | ~30s |
| Download 15 deltas (~50MB) | ~10s |
| Apply deltas | ~15s |
| **Tổng** | **~3 phút** |

**UX requirement:** Dedicated SplashScreen với progress chi tiết: "Đang tải DB... 85% (425/500MB)".

**Worker CPU limit với file lớn:**
- **Option A (khuyến nghị v9.0):** Worker `bucket.get(key)` → `return new Response(object.body)` stream pass-through. CF không tính CPU cho stream body. OK với file vài GB.
- **Option B (nếu đụng limit):** Worker sign R2 S3 presigned URL (HMAC), client GET thẳng R2 endpoint. Phức tạp hơn, defer đến khi cần.

### I. Storage & filesystem safety (thêm 2026-04-24)

| # | Case | Yêu cầu |
|---|---|---|
| I1 | Disk full khi download snapshot 500MB | Check free space trước download; fail gracefully, không ghi partial file |
| I2 | `VACUUM INTO` cần free space ≥ DB size | Check disk ≥ 2×DB_size trước snapshot; nếu không đủ → defer compaction, alert UI |
| I3 | WAL/SHM files khi swap snapshot | Trước swap: checkpoint full (`PRAGMA wal_checkpoint(TRUNCATE)`), close conn, swap, reopen |
| I4 | SQLite BUSY lock (2 Tauri instance) | single-instance plugin đã có (commit e5d48ea); test reproduce case 2 instance và assert focus old window |
| I5 | Crash khi ghi delta NDJSON file | Write vào `.tmp`, fsync, rename atomic. Decompress fail khi apply → quarantine file, không advance cursor |

### J. Encoding & data format

| # | Case | Yêu cầu |
|---|---|---|
| J1 | Sub_id/manual comment có emoji, tiếng Việt có dấu, zero-width space | serde_json UTF-8 strict; test fixture có các ký tự này; assert round-trip bytes-identical |
| J2 | Float precision `spend`/`commission` across Rust f64 / JS Number / SQLite REAL | Store as TEXT với format cố định (5 số thập phân), parse an toàn. Skip-identical hash normalize trước khi hash |
| J3 | Manual entry value = 0 vs NULL vs missing field | 3 trạng thái phân biệt: `{value: 0}` / `{value: null}` / field vắng. NDJSON explicit null, không dùng omit |
| J4 | CSV UTF-8 BOM | Strip BOM trong parse (đã có ở import hiện tại — verify preserve trong v9) |
| J5 | Timezone edge cases — user đổi TZ, system clock đặt về 2020 hoặc tương lai | HLC absorb_remote handle; RFC3339 luôn có Z suffix (UTC); parse reject không-Z timestamps |

### K. Schema evolution (ĐỊNH NGHĨA TỪ ĐẦU — sửa sau rất khó)

| # | Case | Yêu cầu |
|---|---|---|
| K1 | v9.1 thêm column mới vào raw_shopee_clicks | Delta v9.0 không có column đó → apply: column thiếu = NULL hoặc default. Delta format có `schema_version` field per event |
| K2 | v9.1 rename column | Migration function `migrate_delta_event(old_schema_ver → new_schema_ver)` trong apply path |
| K3 | v9.1 thêm table mới | Bootstrap từ snapshot cũ: sau restore phải chạy migration thêm table. Snapshot metadata R2 header `x-schema-version` |

**Design decision:** Delta event thêm field `"sv": 9` (schema_version). Snapshot R2 object có header `x-schema-version: 9`. Apply logic:
```rust
match (event_sv, current_sv) {
    (9, 9) => apply_direct(),
    (9, 10) => migrate_9_to_10(event).apply(),
    (10, 9) => Err("CLIENT_OUTDATED_UPDATE_APP"),
}
```

### L. Concurrency subtle

| # | Case | Yêu cầu |
|---|---|---|
| L1 | User đang gõ form manual, sync pull apply đè row đó | FE: detect "có form dirty cho row R" → defer apply event cho R cho tới khi form save/cancel. Hoặc warn user "có update mới cho row này, apply?" |
| L2 | User close app giữa sync | TX rollback OK. Delta file upload xong, cursor chưa bump → retry upload next start, skip-identical hash phát hiện → không dup |
| L3 | Laptop suspend → wifi drop → resume | HTTP retry với exponential backoff; mutex release sau timeout 60s để không stuck |
| L4 | Power cut giữa `VACUUM INTO` | Fresh start: PRAGMA integrity_check → fail → auto trigger recovery flow (fetch snapshot từ remote, archive corrupt local) |
| L5 | Auto-sync timer overlap với manual "Sync now" | Global sync mutex (1 at a time). Trigger thứ 2 trong khi 1 đang chạy → queue hoặc skip, không parallel |

### M. Cloudflare quirks

| # | Case | Yêu cầu |
|---|---|---|
| M1 | R2 eventual consistency — PUT OK, GET ngay trả 404 (rare) | Delta GET 404 + manifest có entry → retry 3 lần với backoff 1s/3s/9s trước khi error |
| M2 | Worker subrequest limit 50 (free) / 1000 (paid) | Bootstrap parallel download limit concurrency ≤ 10; nhiều delta hơn → serialize qua nhiều Worker calls hoặc direct R2 |

### N. Migration từ v8 (dev machine hiện có)

| # | Case | Yêu cầu |
|---|---|---|
| N1 | Dev máy đang có DB v8 → chạy build v9 | Migration `v8 → v9` chạy ở `init_db_at`: detect schema_version=8 → convert state, không mất data. One-shot, không reverse |
| N2 | R2 còn `users/{uid}/db.zst` (v8) mà manifest chưa có (v9) | Worker/client fallback: GET /v9/manifest/get trả 404 + HEAD db.zst OK → trigger one-time migration: download db.zst → restore → export thành snapshot v9 → upload + tạo manifest |

### O. Recovery & observability

| # | Case | Yêu cầu |
|---|---|---|
| O1 | User báo "mất data" — cần debug tool | Sync event log (chi tiết O1a-O1e dưới) |
| O2 | User muốn force re-pull toàn bộ (nghi ngờ local corrupt) | Settings → "Reset local, kéo từ cloud": archive local DB → clear sync_cursor_state → trigger fresh bootstrap |
| O3 | Admin xóa nhầm user → archive 30 ngày nhưng không có UI restore | Admin endpoint `/v9/admin/restore?uid=&archive_id=`: list archives, copy ngược R2 objects, tạo manifest từ archive metadata |

#### O1 — Sync event log (expanded 2026-04-24)

**O1a. Local ring buffer:** table `sync_event_log` (5000 events, FIFO auto-prune). Fields: `event_id INTEGER PK AUTOINCREMENT`, `ts TEXT (HLC RFC3339)`, `fingerprint TEXT`, `kind TEXT`, `ctx_json TEXT`, `uploaded_at TEXT NULL`.

**O1b. Event kinds:**
- `push_start` / `push_upload` / `push_complete`
- `pull_start` / `pull_fetch` / `pull_apply` / `pull_complete`
- `bootstrap_start` / `bootstrap_snapshot` / `bootstrap_complete`
- `compaction_start` / `compaction_complete`
- `cas_conflict` (với retry number)
- `error` (với phase + error_code + error_msg)
- `recovery` (user trigger force re-pull)

**O1c. Privacy rule — BẮT BUỘC:**
- ✅ Log: table_name, row_count, cursor range, delta_key, etag, error_code, fingerprint, HLC timestamp, bytes
- ❌ KHÔNG log: sub_id values, manual comment text, click_id, spend/commission values, file paths chứa username
- Implementation: dedicated helper `log_sync_event(kind, ctx: Ctx)` — `Ctx` là struct typed, không accept raw Value. Unit test regex verify không có leak PII
- Log scanner test tự động fail nếu phát hiện các pattern (email, digit sequence >8 chars, v.v.)

**O1d. Upload lên R2:**
- Path: `users/{uid}/sync_logs/{yyyy-mm-dd}.ndjson.zst` (daily rotation)
- Trigger upload: (1) async background sau mỗi sync complete (non-blocking), (2) sau mỗi error event (immediate), (3) user click "Gửi log"
- Incremental: chỉ upload events có `uploaded_at IS NULL`; sau upload OK set `uploaded_at = now`
- Size: ~10-20KB/sync compressed. 30 sync/ngày × 10KB = 300KB/ngày/user. 100 user = 30MB/ngày, ~1GB/tháng tổng
- Retention: daily files giữ 30 ngày. Cleanup qua R2 Object Lifecycle Rule hoặc Worker cron

**O1e. Admin viewer:**
- Endpoint `/v9/admin/sync-log?uid=&from=&to=&kind=&fingerprint=` → paginate 100 events/page
- Admin UI: filter theo date range, machine fingerprint, kind (errors only), full-text trong ctx
- Export CSV cho support ticket
- Log NEVER show raw PII (filtered ở Worker layer luôn, không chỉ tin client)

**Cost impact:** ~$0.05/tháng cho 100 user (3GB R2 storage + 90k PUT ops/tháng). Vẫn trong free tier ops.

---

## Phần 2: Thiết kế

### 2.1. R2 layout

```
users/{uid}/
├── manifest.json                                       # Single source of truth (CAS)
├── snapshots/
│   └── snap_{clock_ms}.db.zst                          # Compaction output, bootstrap fast
└── deltas/
    ├── imported_files/
    │   └── {cursor_hi}_{clock_ms}.ndjson.zst
    ├── raw_shopee_clicks/
    ├── raw_shopee_order_items/
    ├── raw_fb_ads/
    ├── clicks_to_file/
    ├── orders_to_file/
    ├── fb_ads_to_file/
    ├── manual_entries/
    └── tombstones/
```

### 2.2. Manifest schema

`users/{uid}/manifest.json`:
```json
{
  "version": 9,
  "uid": "xxx",
  "latest_snapshot": {
    "key": "snapshots/snap_1745234567890.db.zst",
    "clock_ms": 1745234567890,
    "size_bytes": 524288000
  },
  "deltas": [
    {
      "table": "raw_shopee_clicks",
      "key": "deltas/raw_shopee_clicks/5000_1745234600000.ndjson.zst",
      "cursor_lo": 4001,
      "cursor_hi": 5000,
      "clock_ms": 1745234600000,
      "size_bytes": 4521000,
      "row_count": 1000
    }
  ],
  "updated_at_ms": 1745234600000
}
```

CAS chỉ trên `manifest.json` (etag). Delta files + snapshots immutable (key unique) → không cần CAS.

### 2.3. Delta file format (NDJSON + zstd)

Mỗi line 1 JSON event:
```jsonl
{"op":"insert","table":"raw_shopee_clicks","pk":{"click_id":"abc123"},"row":{...},"clock_ms":...}
{"op":"upsert","table":"manual_entries","pk":{"sub_id1":"x",...},"row":{...},"updated_at":"...","clock_ms":...}
{"op":"tombstone","entity_type":"day","entity_key":"2026-04-20","deleted_at":"...","clock_ms":...}
```

**Lý do NDJSON thay vì SQLite binary dump:**
- Human-readable khi debug
- Append-only (ghi mỗi dòng, flush được)
- Dễ compaction (merge + dedup)
- zstd nén ~5-10x vì repetitive

### 2.4. Schema v9

**Thêm:**
```sql
CREATE TABLE sync_cursor_state (
  table_name TEXT PRIMARY KEY,
  last_uploaded_cursor TEXT NOT NULL DEFAULT '0',
  last_pulled_cursor TEXT NOT NULL DEFAULT '0',
  last_uploaded_hash TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE sync_manifest_state (
  id INTEGER PRIMARY KEY CHECK(id=1),
  last_remote_etag TEXT,
  last_pulled_manifest_clock_ms INTEGER DEFAULT 0,
  last_snapshot_key TEXT,
  last_snapshot_clock_ms INTEGER DEFAULT 0,
  fresh_install_pending INTEGER DEFAULT 0
);

CREATE TABLE sync_event_log (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,                -- HLC RFC3339
  fingerprint TEXT NOT NULL,
  kind TEXT NOT NULL,              -- 'push_start' | 'push_upload' | ... (O1b)
  ctx_json TEXT NOT NULL,          -- typed Ctx serialized; NEVER raw PII (O1c)
  uploaded_at TEXT                 -- NULL = chưa upload lên R2
);
CREATE INDEX idx_sync_event_log_uploaded ON sync_event_log(uploaded_at) WHERE uploaded_at IS NULL;
CREATE INDEX idx_sync_event_log_ts ON sync_event_log(ts);
```

**Bỏ từ v8 `sync_state`:**
- `last_uploaded_hash`, `last_remote_etag` (chuyển qua 2 bảng mới, per-table/per-manifest)
- `dirty`, `change_id`, `last_uploaded_change_id` (không còn semantic toàn-DB)

**Giữ từ v8 `sync_state`:**
- `owner_uid` (multi-tenant guard)
- `last_known_clock_ms` (HLC singleton)
- `last_synced_at_ms` (UI display)

### 2.5. Cursor per table

| Table | Cursor column | Kiểu |
|---|---|---|
| `imported_files` | `id` | Append + occasional delete (qua tombstones) |
| `raw_shopee_clicks` | `rowid` | Append-only |
| `raw_shopee_order_items` | `rowid` | Append-only |
| `raw_fb_ads` | `rowid` | Append-only |
| `clicks_to_file` | `rowid` | Append-only |
| `orders_to_file` | `rowid` | Append-only |
| `fb_ads_to_file` | `rowid` | Append-only |
| `manual_entries` | `updated_at` (HLC RFC3339) | Mutable |
| `shopee_accounts` | `updated_at` (HLC) | Mutable |
| `tombstones` | `deleted_at` (HLC) | Append-only |
| `days` | ❌ không sync trực tiếp | Derived từ raw `day_date` FK; insert lazy khi apply raw delta |

### 2.6. Worker v9 endpoints

```
POST /v9/manifest/get       → { manifest, etag }
POST /v9/manifest/put       → body: { manifest, expectedEtag } → CAS, 412 nếu mismatch
POST /v9/delta/upload       → query: table, key; body: zst bytes → R2 put (immutable)
GET  /v9/delta/fetch        → query: key → zst bytes
POST /v9/snapshot/upload    → query: snapshot_key; body: zst → R2 put
GET  /v9/snapshot/fetch     → query: snapshot_key → zst bytes (stream pass-through)

POST /v9/sync-log/push      → query: date (yyyy-mm-dd); body: NDJSON events → R2 append daily file

POST /v9/admin/users
GET  /v9/admin/snapshot     → query: uid → latest snapshot hoặc trigger build
POST /v9/admin/cleanup      → query: uid → delete all R2 + Firestore
GET  /v9/admin/sync-log     → query: uid, from, to, kind?, fingerprint? → paginated events
GET  /v9/admin/restore      → query: uid, archive_id → restore từ archive/
```

**Mọi endpoint** assert JWT `uid == path uid`.
**Admin endpoints** assert admin claim (Firebase custom claim hoặc whitelist UID).

### 2.7. Push flow

```
1. Acquire sync mutex
2. For each table T in dependency order (imported_files → raw → mapping → manual → tombstones):
   a. cursor = sync_cursor_state[T].last_uploaded_cursor
   b. rows = SELECT * FROM T WHERE <cursor_col> > cursor ORDER BY <cursor_col>
   c. Nếu rỗng: continue
   d. Split rows thành batches ≤ 5MB (serialized NDJSON before zstd)
   e. For each batch:
      - Serialize NDJSON
      - zstd compress L3
      - hash compressed bytes (skip-identical check)
      - key = deltas/{T}/{batch_cursor_hi}_{hlc_ms}.ndjson.zst
      - POST /v9/delta/upload
      - UPDATE sync_cursor_state SET last_uploaded_cursor = batch_cursor_hi, last_uploaded_hash = hash WHERE table_name = T
3. Fetch latest manifest with etag
4. Append new delta entries vào manifest.deltas
5. POST /v9/manifest/put với expectedEtag
   - 412 → re-fetch, re-append (idempotent append), retry (max 3)
6. UPDATE sync_manifest_state SET last_remote_etag = new_etag
7. Release mutex
```

### 2.8. Pull flow

```
1. Acquire sync mutex
2. GET /v9/manifest/get → manifest + etag
3. Nếu manifest.updated_at_ms <= sync_manifest_state.last_pulled_manifest_clock_ms: done
4. Diff: delta entries trong manifest mà không có trong sync_cursor_state
5. Parallel download (concurrency 3-5) các delta files mới
6. Sort deltas theo clock_ms ASC (causal ordering)
7. BEGIN TRANSACTION
   a. For each delta file (sorted):
      - Decompress zstd, parse NDJSON
      - For each event:
        * op=insert (raw): INSERT OR IGNORE INTO {table} VALUES (...)
        * op=upsert (manual/shopee_accounts): INSERT OR REPLACE với HLC check — nếu local updated_at > event.updated_at → skip
        * op=tombstone: apply_tombstones logic:
          - entity_type=day: CASCADE delete unconditional
          - entity_type=ui_row|manual_entry: resurrect check (manual_entries.updated_at > deleted_at → skip)
      - UPDATE sync_cursor_state SET last_pulled_cursor = delta.cursor_hi WHERE table_name = delta.table
   b. absorb_remote_clock(max event.clock_ms trong batch)
   c. UPDATE sync_manifest_state.last_pulled_manifest_clock_ms = manifest.updated_at_ms
8. COMMIT (1 TX cho toàn batch pull, hoặc 1 TX per file nếu batch lớn)
9. Release mutex
```

### 2.9. Bootstrap flow (fresh install)

```
1. Detect fresh install: local DB empty (no raw rows, no manual_entries) + sync_cursor_state rỗng
2. Set sync_manifest_state.fresh_install_pending = 1
3. GET /v9/manifest/get
4. Nếu manifest.latest_snapshot tồn tại:
   a. GET /v9/snapshot/fetch → download 500MB+ zst
   b. Decompress → write tạm file.db
   c. Verify schema version, owner_uid match
   d. Swap file thành active DB
   e. Populate sync_cursor_state từ snapshot metadata (cursor per table = max trong snapshot)
   f. Set sync_manifest_state.last_snapshot_key + clock_ms
5. Run Pull flow để apply deltas sau snapshot
6. Clear sync_manifest_state.fresh_install_pending = 0
7. **Không push** trong suốt bootstrap (tránh đè remote empty)
```

### 2.10. Snapshot compaction

Chạy background (client trigger khi delta count > threshold, hoặc Worker cron):

```
1. Fetch manifest
2. Nếu delta count < 100: skip
3. Download all deltas + existing snapshot
4. Apply trong temp SQLite
5. VACUUM INTO → zstd
6. Upload snapshots/snap_{now_ms}.db.zst
7. Manifest update: latest_snapshot + remove old deltas (clock_ms < snap.clock_ms) từ manifest.deltas
8. CAS manifest PUT
9. Delete old snapshot + old delta R2 files (best-effort, không critical)
```

Có thể defer compaction sang **v9.1** — 3-4 tháng đầu delta count chưa tới ngưỡng.

### 2.11. Xử lý các case ⚠

| Case | Giải pháp |
|---|---|
| A4-A6 | `users/{uid}/` folder + switch_db_to_user + SettingsProvider key — **giữ nguyên v8** |
| B5 (day delete vs edit) | Day tombstone emit vào `deltas/tombstones/` → apply trước raw deltas cùng clock range (sorted by clock_ms) |
| B6 (resurrect) | manual_entry/ui_row tombstone check `manual_entries.updated_at > deleted_at` → skip delete |
| B9 (clock drift) | HLC `next_hlc_rfc3339`, `absorb_remote_clock` — **giữ nguyên v8** |
| C2 (fresh install guard) | `fresh_install_pending=1` flag trong bootstrap, clear sau pull xong. Khi flag=1 thì push path KHÔNG upload empty state |
| D4 (delete day cleanup) | Emit batch: tombstone 'day' + tombstones cho imported_files rows + raw rows có day_date=X bị DELETE qua CASCADE |
| F1 (uid spoof) | Worker: `if (jwt.uid !== pathUid) return 403` trên mọi endpoint user-scope |

---

## Phần 3: Implementation plan

| Phase | Ngày | Output |
|---|---|---|
| **P0. Design freeze** | 0.5 | Review doc này xong → lock spec |
| **P1. Schema v9 + Rust types** | 1 | Migration script, structs `DeltaEvent`/`Manifest`/`CursorState` với serde |
| **P2. Delta capture (push)** | 2 | `push_deltas()` per-table, batch split 5MB, upload, cursor advance. Unit tests |
| **P3. Manifest CAS flow** | 0.5 | Fetch/update manifest với etag retry (max 3) |
| **P4. Delta apply (pull)** | 2 | `pull_deltas()`: diff manifest, parallel download, sort by clock, TX apply, HLC absorb, tombstone rules |
| **P5. Bootstrap + fresh-install guard** | 1 | Snapshot download + restore, fresh_install_pending flag |
| **P6. Worker v9 endpoints** | 1 | 8 endpoints mới + JWT uid-claim guard + admin claim guard |
| **P7. FE `useCloudSync` refactor + Sync log UI** | 2 | Hook rewrite, progress events per-table, SyncBadge, SplashScreen bootstrap, Sync log viewer (user + admin), log capture hooks |
| **P8. Admin v9** | 0.5 | List users, snapshot fetch (or build-on-demand), cleanup-orphans |
| **P9. Integration tests** | **3-4** | **Full test matrix Phần 8** — cover mọi case A1-H2 + data-preservation stress tests + manual smoke checklist. KHÔNG được skip case nào |
| **P10. Snapshot/compaction** (có thể defer v9.1) | 0.5 | Manual trigger command + weekly cron Worker |

**Tổng MVP v9.0 (bỏ P10): ~16-18 ngày focus** (đã bao gồm coverage 24 case tiềm ẩn I1-O3 + Sync log admin viewer + data-preservation stress tests).
**Full v9 với compaction: ~17-19 ngày focus.**
**Part-time 1-2h/ngày: ~6-8 tuần.**

**Breakdown phase mới sau khi thêm nhóm I-O:**
- P1 Schema: +0.5 ngày (schema_version field trong delta + snapshot header, sync_event_log table)
- P2 Push: +0.5 ngày (atomic NDJSON write, quarantine fail files)
- P4 Pull: +1 ngày (form-dirty defer, R2 eventual consistency retry, concurrency cap)
- P5 Bootstrap: +0.5 ngày (disk space precheck, v8 fallback N2)
- P7 FE: +0.5 ngày (Sync log UI, force re-pull button, form-dirty detect)
- P9 Tests: +1-2 ngày (24 case mới × ~1h each)

---

## Phần 4: Decisions ✅ LOCKED 2026-04-24

User chọn "all khuyến nghị" — mọi option ⭐.

| # | Decision | Chọn |
|---|---|---|
| Q1 | Delta batch size | **5MB** per file (sau zstd) |
| Q2 | Snapshot | **Include trong v9.0** (không defer) |
| Q3 | Compaction trigger | **Client-side** khi delta count > 100 |
| Q4 | Day tombstone | **Unconditional cascade** (giữ v8 rule) |
| Q5 | Bootstrap UX | **Dedicated SplashScreen** với progress per-phase |
| Q6 | Admin snapshot | **Reuse user snapshot + apply deltas mới** |
| Q7 | Delete account endpoint | **KHÔNG** — user không tự xóa được. Chỉ admin xóa qua `/v9/admin/cleanup` (vẫn archive 30 ngày trước khi xóa R2 để recovery) |
| Q8 | Snapshot download | **Worker stream pass-through** (presigned URL defer v9.1 nếu cần) |

**User nguyên văn:** "all khuyến nghị v9 v9 delta luôn phải đảm bảo logic hiện tại, tối ưu request, db cho đỡ chi phí của tôi, đừng làm user mất data trong mọi TH ưu tiên giữ data"

→ Ba nguyên tắc ở đầu doc (giữ data / logic hiện tại / tối ưu cost) là **bắt buộc**, override mọi spec khác khi xung đột.

---

## Phần 5: KHÔNG thay đổi (preserve từ v8)

- Multi-tenant folder layout `users/{uid}/`
- `switch_db_to_user` + pre_auth placeholder
- `SettingsProvider key={uid}` remount
- `wipeUserLocalStorage`
- HLC logic (`next_hlc_ms`, `next_hlc_rfc3339`, `absorb_remote_clock`)
- `video_logs.db` per-user (v8)
- Firebase Auth flow
- RTDB admin whitelist
- UI business logic (import preview, KPI, overview, manual entry)
- Fingerprint stable (machine-uid crate)

Chỉ viết lại **sync engine** (~3,300 LOC) và **Worker endpoints**.

---

## Phần 6: Rollout strategy

Vì chưa có user thật → **không cần dual-write / feature flag / migration**:

1. Tạo branch `v9-sync-rewrite`
2. Implement theo phases P1-P9
3. Integration test local với 2-3 Windows instances
4. Deploy Worker v9 lên staging environment (riêng route `/v9/*`)
5. Build app binary với v9 client
6. Test end-to-end: fresh install, 3-máy convergence, admin view, DB 500MB+ scenario
7. Deploy Worker v9 production, release app binary
8. Monitor 1 tuần trước khi đóng endpoints v8 (có thể xóa code v8 luôn nếu không có user)

### 6.1. Cleanup schedule v8 (nguyên tắc #4)

Mỗi phase dưới đây có **2 phần**: implement v9 + **delete v8 tương ứng**. Cleanup là work item bắt buộc trong cùng PR, không defer.

| Phase | Implement v9 | Delete v8 (cùng PR) |
|---|---|---|
| P1 ✅ | schema v11 (additive) + types + event_log | (không xóa — v8 sync.rs vẫn cần build compile) |
| P2 ✅ | capture + push payload builder | (chưa — cần sync.rs cho Tauri commands hiện tại) |
| P3 | manifest CAS flow | (chưa) |
| P4 | pull flow (apply deltas) | (chưa) |
| P5 | bootstrap + fresh-install guard | (chưa) |
| **P6** | **Worker v9 endpoints (`/v9/*`)** | **Delete `worker/src/index.ts` v8 handlers (`/upload`, `/download`, `/metadata`)**, giữ admin dành P8 |
| **P7** | **FE `useCloudSync_v9` + `sync_v9.ts` + Sync log UI** | **Delete `src/lib/sync.ts` + `src/hooks/useCloudSync.ts` v8**, rewrite `SyncBadge` cho v9 state |
| **P8** | **Tauri commands v9 wired up (sync_v9_cmds.rs) + migration v12 drop sync_state v8 columns + drop triggers** | **Delete `src/commands/sync.rs` (1611 LOC) + `sync_client.rs` (378 LOC) + commands registration ở lib.rs. Move HLC helpers sang `sync_v9/hlc.rs`.** Delete `worker/src/admin.ts` v8 routes |
| P9 | Integration tests full matrix | Grep cuối: `v8`, `dirty=1`, `change_id` → phải không còn reference |
| P10 | Compaction (defer v9.1 OK) | - |

**Gate cho mỗi PR:**
- `cargo build` + `cargo test` pass
- `grep -r "v8\|dirty=1\|last_uploaded_change_id\|change_id" src-tauri/src/ worker/src/ src/` → chỉ còn references trong migration history comments hoặc đã được wrap trong `#[deprecated]` removal path
- Clippy không warn dead_code (nếu có → hoặc dùng, hoặc xóa)

**Post-merge final sweep (sau P9):**
- Delete `docs/SETUP_CLOUD.md` nếu chứa v8-only instructions
- Review memory entries: `project_sync_v8_cas_hlc.md` → archive hoặc rewrite cho v9
- `_schema_version` table: giữ hàng v1-v11 cho history, không delete

---

## Phần 7: Reference — code files sẽ touch

| File | Hiện tại (LOC) | Sau v9 |
|---|---|---|
| `src-tauri/src/commands/sync.rs` | 1,611 | **DELETE** ở P8 (replaced bởi `sync_v9/*` + `commands/sync_v9_cmds.rs`) |
| `src-tauri/src/commands/sync_client.rs` | 378 | **DELETE** ở P8 (replaced bởi `sync_v9/client.rs`) |
| `src-tauri/src/sync_v9/` (mới) | — | ~1,500 LOC: types, descriptors, capture, compress, push, pull, snapshot, hlc, client, event_log, commands |
| `src-tauri/src/db/mod.rs::migrate_v11_sync_infra` | — | ✅ Phase 1 done (additive) |
| `src-tauri/src/db/mod.rs::migrate_v12_drop_v8_sync` | — | Phase 8: drop sync_state v8 columns + triggers |
| `worker/src/index.ts` | 288 | **DELETE v8 handlers** ở P6, thay bằng `/v9/*` routes ~450 LOC |
| `worker/src/admin.ts` | 146 | **DELETE v8 routes** ở P8, thay bằng `/v9/admin/*` ~180 LOC |
| `src/lib/sync.ts` | 183 | Rewrite ~250 |
| `src/hooks/useCloudSync.ts` | 596 | Rewrite ~500 |
| `src/components/SyncBadge.tsx` | 171 | Minor updates |
| `src/components/BootstrapSplash.tsx` (mới) | — | ~200 |

---

## Phần 8: Test plan — BẮT BUỘC cover mọi case Phần 1

User yêu cầu 2026-04-24: **"phải test kỹ các case mà chúng ta đã bàn rồi"**. Phase 9 integration test KHÔNG được skip case nào từ Phần 1.

**Nguyên tắc test:**
- Mỗi case A1-H2 phải có **ít nhất 1 test** tương ứng (unit hoặc integration)
- Test phải verify **hậu quả** (không chỉ "không crash") — assert data integrity, không mất row, cursor advance đúng
- Setup/teardown phải clean — dùng fixture SQLite + R2 mock (hoặc test bucket riêng)
- CI chạy toàn bộ trước mỗi merge vào `master`

### 8.1. Unit tests (Rust `src-tauri/`)

| Module | Coverage target |
|---|---|
| `delta_capture::capture_for_table` | Raw append-only: cursor=rowid; mutable: cursor=updated_at; empty table → no output; batch split theo 5MB |
| `delta_apply::apply_event` | op=insert idempotent; op=upsert HLC skip-if-older; op=tombstone resurrect rule |
| `manifest::append_delta_entry` | Idempotent append (key đã có → skip); sort theo clock_ms |
| `snapshot::create_from_db` | VACUUM INTO → zstd; decompress roundtrip; integrity_check pass |
| `hlc::next_hlc_rfc3339` | Monotonic kể cả cùng ms; absorb_remote không tụt |
| `compaction::should_trigger` | delta_count > 100 → true; < 100 → false |
| `fresh_install::is_fresh` | Local empty + remote có → true; local có data → false |

### 8.2. Integration tests (2-3 máy simulated, Tauri test harness)

Map 1-1 với cases Phần 1. Mỗi test viết vào `tests/integration/sync_v9/`:

**A. Auth & session**
| Case | Test file | Assert |
|---|---|---|
| A1 | `test_first_login_empty.rs` | Fresh app + fresh remote → local DB created, manifest empty, no error |
| A2 | `test_login_new_machine.rs` | Remote có 500MB snapshot + 20 deltas → bootstrap pull all, local = remote state |
| A3 | `test_relogin_same_machine.rs` | 1 delta mới trên remote → pull chỉ delta đó, không re-download snapshot |
| A4 ⚠ | `test_user_switch_isolation.rs` | User A → B cùng máy → folder A giữ nguyên, B fresh bootstrap, AccountContext B không thấy A data |
| A5 ⚠ | `test_user_swap_back.rs` | A → B → A → data A nguyên vẹn, không leak B |
| A6 ⚠ | `test_admin_exit_refetch.rs` | Admin view C → exit → AccountContext refetch DB admin, không phải C |
| A7 | `test_pending_delta_on_logout.rs` | User có dirty local → logout → re-login → push deltas pending, không mất |
| A8 | `test_jwt_refresh_mid_sync.rs` | Mock JWT expire → refresh → resume sync cùng cursor |
| A9 ⚠ | `test_admin_revoke_mid_session.rs` | Admin revoke claim giữa chừng → endpoint trả 403, UI exit view |

**B. Sync convergence**
| Case | Test file | Assert |
|---|---|---|
| B1 | `test_import_push_pull.rs` | A import CSV → push → B pull → B thấy đủ raw rows |
| B2 | `test_duplicate_import_idempotent.rs` | A+B import cùng file hash → remote chỉ có 1 imported_files entry |
| B3 | `test_different_files_same_day.rs` | A import file1, B import file2 cùng day_date → merge, cả 2 file tồn tại |
| B4 ⚠ | `test_concurrent_edit_hlc.rs` | A+B edit cùng manual_entry → edit có HLC lớn hơn thắng |
| B5 ⚠ | `test_delete_day_beats_edit.rs` | A delete day, B edit manual trong day đó → sau merge: day deleted (unconditional) |
| B6 ⚠ | `test_edit_resurrects_tombstone.rs` | A delete manual, B edit sau (updated_at > deleted_at) → row survive |
| B7 | `test_concurrent_push_cas.rs` | A+B push đồng thời → 1 thắng CAS, đứa còn lại retry pull-merge-push, không mất data |
| B8 | `test_three_way_converge.rs` | 3 máy edit khác rows → sau convergence tất cả thấy union |
| B9 ⚠ | `test_clock_drift.rs` | A clock -10 phút, B clock đúng → A edit sau merge vẫn thắng (HLC absorb) |
| B10 | `test_offline_week_replay.rs` | A offline 7 ngày, B push 50 deltas → A online → pull all, apply theo thứ tự clock |

**C. Bootstrap & recovery**
| Case | Test file | Assert |
|---|---|---|
| C1 | `test_fresh_install_with_snapshot.rs` | Remote có snapshot + 15 deltas → bootstrap ~3 phút, local state đúng |
| C2 ⚠ | `test_reinstall_same_machine.rs` | Xóa local DB, fingerprint cùng → fresh_install_pending=1, KHÔNG push empty |
| C3 | `test_machine_guid_changed.rs` | Fingerprint mới nhưng UID cũ → bootstrap như máy mới |
| C4 | `test_local_db_corrupt_recovery.rs` | PRAGMA integrity_check fail → user trigger reset → pull từ remote |
| C5 | `test_remote_deleted_local_survives.rs` | Admin xóa R2 user → local sync push lại từ đầu → remote restore |
| C6 | `test_admin_snapshot_build.rs` | Admin request snapshot user X → reuse latest + apply deltas sau → consistent state |

**D. Data integrity**
| Case | Test file | Assert |
|---|---|---|
| D1 | `test_upload_retry_idempotent.rs` | Mock fail sau R2 put OK → retry → không dup trên R2 |
| D2 | `test_apply_crash_rollback.rs` | Inject panic giữa apply delta → TX rollback, cursor không advance, retry apply lại OK |
| D3 | `test_fk_order.rs` | imported_files phải push trước raw_* trong dependency order |
| D4 ⚠ | `test_delete_day_cascade_sync.rs` | Delete day → emit tombstone + delete imported_files + raw rows trong cùng TX + sync lên remote đầy đủ |
| D5 | `test_tombstone_future_raw.rs` | Tombstone 'day=X' apply trước khi raw rows cho day X được pull → raw rows bị skip insert |
| D6 | `test_reimport_blocked.rs` | Import file đã có → UNIQUE constraint reject, không push delta dup |

**E. Performance & network**
| Case | Test file | Assert |
|---|---|---|
| E1 | `test_batch_split_5mb.rs` | Import 100MB raw → split thành ~20 delta files ≤5MB each |
| E2 | `test_parallel_download.rs` | 50 delta files → concurrency 3-5, tất cả download OK, apply đúng thứ tự clock |
| E3 | `test_resume_interrupted_upload.rs` | Upload delta file fail giữa → retry → file upload xong, cursor advance |
| E4 | `test_sync_debounce.rs` | Click "Sync now" 10 lần/s → chỉ 1 sync execute |
| E5 | `test_edit_during_sync.rs` | Sync đang chạy, user edit → edit vào local OK, next sync push edit đó |
| E6 | `test_compaction_triggers.rs` | Manifest.deltas > 100 → client auto compact, snapshot mới, manifest.deltas reset |

**F. Security**
| Case | Test file | Assert |
|---|---|---|
| F1 | `test_uid_spoof_rejected.rs` | User A JWT cố POST vào path `users/{B_uid}/` → Worker return 403 |
| F2 ⚠ | `test_non_admin_admin_endpoint.rs` | Non-admin UID gọi `/v9/admin/*` → 403 |
| F3 | `test_invalid_jwt.rs` | Tampered JWT → Worker reject, không ghi R2 |
| F4 | `test_r2_quota_hit.rs` | Mock R2 full → Worker trả 507, FE show banner |

**G. Admin**
| Case | Test file | Assert |
|---|---|---|
| G1 | `test_admin_list_users.rs` | GET /admin/users → union Firestore + R2 ListObjects, dedup, sort |
| G2 | `test_admin_view_db.rs` | Admin view user X → snapshot fetch + apply deltas → DbState swap read-only |
| G3 | `test_admin_exit_restore.rs` | Admin exit view → DbState restore admin DB (không hardcode path) |
| G4 | `test_admin_cleanup_archive.rs` | Admin cleanup → R2 clone sang `archive/deleted_{uid}_{ts}/` trước → DELETE; archive giữ 30 ngày |

**H. Multi-device + large DB (scenarios user hỏi)**
| Case | Test file | Assert |
|---|---|---|
| H1 | `test_h1_offline_15d_catchup.rs` | Setup: 3 máy, A offline 15 ngày, B+C push 100 changes (30 manual + 60 import events + 10 tombstones) → A online → pull 12 delta files, ~13 R2 requests, converge trong 30s |
| H2 | `test_h2_2gb_fresh_machine.rs` | Setup: 2GB DB compacted thành snapshot 500MB + 15 deltas → máy D fresh → bootstrap trong 3 phút, ~17 R2 requests, local DB = remote state |

**I. Storage & filesystem**
| Case | Test file | Assert |
|---|---|---|
| I1 | `test_disk_full_snapshot.rs` | Mock filesystem với 100MB free → download snapshot 500MB → fail trước khi ghi, local DB nguyên vẹn |
| I2 | `test_vacuum_needs_2x_space.rs` | DB 1GB + disk 1.5GB free → VACUUM INTO fail → defer compaction, UI alert hiện |
| I3 | `test_wal_checkpoint_before_swap.rs` | DB có uncommitted WAL → swap snapshot → kiểm tra WAL flush trước, data WAL không mất |
| I4 | `test_single_instance_lock.rs` | Launch instance 2 → focus instance 1, không mở DB lần 2, không SQLite BUSY |
| I5 | `test_delta_write_atomic.rs` | Kill process giữa ghi delta file → file `.tmp` hoặc final file valid, không partial corrupt |

**J. Encoding**
| Case | Test file | Assert |
|---|---|---|
| J1 | `test_unicode_roundtrip.rs` | Sub_id "🚀 đơn hàng ​" → push → pull → bytes-identical |
| J2 | `test_float_precision.rs` | spend=0.1+0.2 → serialize → deserialize → compare equal (via normalization), skip-identical hash stable qua round-trip |
| J3 | `test_manual_null_vs_missing.rs` | 3 events: `{value:0}`, `{value:null}`, field vắng → apply phân biệt override 0 / clear override / no-op |
| J4 | `test_csv_bom_strip.rs` | CSV có BOM → parse OK, sub_id không có "﻿" prefix |
| J5 | `test_clock_future_and_past.rs` | System clock đặt về 2020 rồi 2040 → HLC monotonic, không tụt |

**K. Schema evolution**
| Case | Test file | Assert |
|---|---|---|
| K1 | `test_schema_add_column.rs` | Apply delta v9 (không có col mới) vào schema v10 → col = default/NULL, không fail |
| K2 | `test_schema_rename_column.rs` | Delta v9 có `old_name` → migrate hàm chạy → insert vào `new_name` v10 |
| K3 | `test_schema_add_table.rs` | Restore snapshot v9 → schema migration v10 chạy sau → table mới được tạo |

**L. Concurrency**
| Case | Test file | Assert |
|---|---|---|
| L1 | `test_form_dirty_defer_apply.rs` | User đang edit form row R + sync pull event cho R → apply defer cho đến khi form save/cancel, không đè |
| L2 | `test_close_app_mid_sync.rs` | Kill app giữa upload → restart → retry upload, skip-identical hash → không dup |
| L3 | `test_suspend_resume.rs` | Mock network drop 30s + resume → retry exponential backoff, mutex release sau timeout |
| L4 | `test_power_cut_vacuum.rs` | Kill process giữa VACUUM → fresh start → integrity_check detect corrupt → recovery flow trigger |
| L5 | `test_auto_manual_sync_mutex.rs` | Auto-sync đang chạy + user click "Sync now" → mutex block, không 2 sync parallel |

**M. Cloudflare quirks**
| Case | Test file | Assert |
|---|---|---|
| M1 | `test_r2_eventual_consistency_retry.rs` | Mock delta GET 404 3 lần rồi OK → client retry backoff 1s/3s/9s → thành công, không error |
| M2 | `test_worker_subrequest_limit.rs` | Bootstrap 100 deltas → concurrency cap ≤ 10 → không hit Worker subrequest limit |

**N. v8 → v9 migration**
| Case | Test file | Assert |
|---|---|---|
| N1 | `test_v8_local_db_migration.rs` | Startup với schema v8 local → migrate → schema v9, data nguyên vẹn, sync_cursor_state init đúng |
| N2 | `test_v8_r2_db_fallback.rs` | R2 có `users/{uid}/db.zst` (v8) nhưng không có manifest → one-time convert → snapshot v9 + manifest tạo mới |

**O. Recovery & observability**
| Case | Test file | Assert |
|---|---|---|
| O1a | `test_sync_event_log_ring.rs` | Ring buffer 5000 events → rotate đúng, event thứ 5001 push ra event #1 |
| O1b | `test_sync_event_kinds.rs` | Mọi push/pull/bootstrap flow emit đúng kind; không flow nào miss event |
| O1c | `test_sync_log_no_pii.rs` | Regex scanner: log không chứa email, sub_id actual, spend values. Fail test nếu phát hiện |
| O1d | `test_sync_log_daily_upload.rs` | Events 2 ngày → 2 file R2 riêng. Incremental: events uploaded không re-upload |
| O1e | `test_admin_sync_log_viewer.rs` | Admin query với filter kind=error + date range → đúng events trả về, pagination hoạt động |
| O2 | `test_force_repull.rs` | User trigger reset → archive local DB → sync_cursor_state cleared → bootstrap fresh với remote state |
| O3 | `test_admin_restore_archive.rs` | Admin cleanup user X → archive tồn tại → admin restore → R2 state user X back → sync từ máy user vẫn OK |

### 8.3. Data-preservation stress tests (3 nguyên tắc #1)

Thêm các test riêng verify "không mất data":

| Test | Scenario |
|---|---|
| `test_no_data_loss_concurrent_3machine.rs` | 3 máy A/B/C mỗi máy edit 20 rows khác nhau, sync đồng thời → union 60 rows, 0 rows lost |
| `test_no_data_loss_crash_midway.rs` | Inject crash ở mỗi bước (upload, apply, manifest PUT) → data consistency verify sau recovery |
| `test_no_data_loss_network_flap.rs` | Mock network drop random trong sync → retry → 0 rows lost |
| `test_no_data_loss_user_switch.rs` | User A có pending dirty → switch sang B → login lại A → pending deltas vẫn còn, push được |
| `test_no_data_loss_compaction.rs` | Compaction crash giữa chừng (snapshot upload OK, delete delta fail) → data nguyên vẹn, retry compaction idempotent |
| `test_no_data_loss_admin_delete_recovery.rs` | Admin cleanup → data vào archive → restore từ archive trong 30 ngày → data về nguyên trạng |

### 8.4. CI matrix

```yaml
# .github/workflows/sync-v9.yml
jobs:
  unit_tests:
    - cargo test --lib sync_v9
    - npm test src/hooks/useCloudSync
  integration_tests:
    - cargo test --test sync_v9 -- --test-threads=1
  data_preservation:
    - cargo test --test no_data_loss -- --ignored  # slow, run full on main
```

**Pre-merge gate:** Tất cả unit + integration pass; data_preservation pass trên PR về `master`.

### 8.5. Manual smoke test checklist (trước release)

Test thủ công trên 2 máy Windows thật:

- [ ] Cài fresh máy 1, login user test1 → import 3 ngày CSV → push thành công
- [ ] Cài fresh máy 2, login user test1 → bootstrap pull, thấy đủ 3 ngày
- [ ] Máy 1 edit manual_entry → máy 2 pull → thấy edit
- [ ] Máy 2 edit cùng row → máy 1 pull → HLC resolve, máy 2 thắng nếu sau
- [ ] Máy 1 delete 1 ngày → máy 2 pull → ngày biến mất
- [ ] Máy 1 offline, edit 10 rows → online lại → push OK, máy 2 pull thấy
- [ ] Logout test1, login test2 → test1 data không hiện
- [ ] Login admin, view test1 → thấy DB test1 read-only
- [ ] Admin exit → back DB admin
- [ ] Tắt wifi giữa sync → UI hiện banner retry, không crash
- [ ] Import CSV 50MB → progress bar smooth, không freeze
- [ ] Database file 1GB+ → fresh install máy mới → SplashScreen progress chạy, bootstrap xong

---

## Appendix: Glossary

- **Delta file**: file NDJSON+zstd chứa batch events (insert/upsert/tombstone) cho 1 table, 1 cursor range
- **Manifest**: JSON index trên R2 liệt kê tất cả delta files + latest snapshot của user
- **Snapshot**: SQLite binary file nén zstd, chứa full DB state tại 1 điểm time (bootstrap + compaction)
- **Cursor**: high-water-mark giá trị của cột dùng để track sync progress per table (rowid hoặc updated_at)
- **CAS (Compare-And-Swap)**: R2 etag match check khi PUT manifest → prevent concurrent overwrite
- **HLC (Hybrid Logical Clock)**: monotonic timestamp chống clock drift, v8 inherit
- **Tombstone**: bản ghi deletion để sync xóa qua máy khác
- **Resurrect rule**: tombstone không xóa row nếu row đã updated_at > tombstone.deleted_at (edit-wins-over-delete khi concurrent)
