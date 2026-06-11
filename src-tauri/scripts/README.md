# Dev signing workaround cho Smart App Control / WDAC

## Mục đích

Workaround Smart App Control (SAC) / WDAC trên Windows 11 mà không cần tắt
SAC trong Settings. Tạo self-signed cert + auto-sign mọi `cargo run` /
`npm run tauri dev` build qua cargo runner config.

## Setup (1 lần)

Mở **PowerShell as Administrator**, chạy:

```powershell
cd D:\RUSTS\ThongKeShopee\src-tauri
.\scripts\setup-dev-cert.ps1
```

Script sẽ:

1. Tạo self-signed cert `CN=ThongKeShopee Dev` trong `CurrentUser\My`
2. Install cert vào `LocalMachine\Root` (Trusted Root)
3. Install cert vào `LocalMachine\TrustedPublisher`

Cert valid 5 năm.

## Sau setup

Chạy `npm run tauri dev` như bình thường. Cargo runner (`.cargo/config.toml`)
sẽ tự gọi `sign-and-run.ps1` mỗi lần execute binary → sign với cert → run.

## Giới hạn (đọc kỹ)

**Smart App Control vẫn có thể block** kể cả khi binary đã sign với trusted
cert. Lý do: SAC dùng **cloud reputation database** đánh giá mỗi hash exe
mới — self-signed cert không có reputation history → AI vote "unknown" →
block.

Test sau setup:

```
npm run tauri dev
```

- ✅ Run được → SAC accept signature, done
- ❌ Vẫn `os error 4551` → SAC reject cloud reputation, không có code workaround.
  Lựa chọn duy nhất: tắt SAC qua Settings (Privacy & Security → Windows Security
  → App & browser control → Smart App Control settings → Off → reboot).

## Tháo gỡ

Xóa cert:

```powershell
Get-ChildItem Cert:\CurrentUser\My -CodeSigningCert |
    Where-Object Subject -eq "CN=ThongKeShopee Dev" | Remove-Item
Get-ChildItem Cert:\LocalMachine\Root |
    Where-Object Subject -eq "CN=ThongKeShopee Dev" | Remove-Item
Get-ChildItem Cert:\LocalMachine\TrustedPublisher |
    Where-Object Subject -eq "CN=ThongKeShopee Dev" | Remove-Item
```

Xóa `.cargo/config.toml` để cargo runner về default.
