#requires -RunAsAdministrator
# Setup self-signed code signing cert + install to trust stores so Smart
# App Control / WDAC accepts unsigned Rust dev builds.
#
# Run once. After that every `cargo run` / `npm run tauri dev` auto-signs
# the binary with this cert via cargo runner config + sign-and-run.ps1.
#
# Must run in PowerShell as Administrator.

$ErrorActionPreference = "Stop"

$CertSubject = "CN=ThongKeShopee Dev"
$CertFriendlyName = "ThongKeShopee Dev Signing"

Write-Host "==> Creating self-signed code signing cert..." -ForegroundColor Cyan

# Check if cert already exists - reuse instead of creating duplicate.
$existing = Get-ChildItem Cert:\CurrentUser\My -CodeSigningCert -ErrorAction SilentlyContinue |
    Where-Object { $_.Subject -eq $CertSubject } |
    Select-Object -First 1

if ($existing) {
    Write-Host "    Existing cert found, reusing: $($existing.Thumbprint)" -ForegroundColor Gray
    $cert = $existing
} else {
    $cert = New-SelfSignedCertificate `
        -Subject $CertSubject `
        -CertStoreLocation "Cert:\CurrentUser\My" `
        -Type CodeSigningCert `
        -KeyAlgorithm RSA `
        -KeyLength 2048 `
        -HashAlgorithm SHA256 `
        -NotAfter (Get-Date).AddYears(5) `
        -FriendlyName $CertFriendlyName
    Write-Host "    Thumbprint: $($cert.Thumbprint)" -ForegroundColor Gray
}

# Export cert (no private key) to import into trust stores.
$tempCer = Join-Path $env:TEMP "thongkeshopee-dev.cer"
Export-Certificate -Cert $cert -FilePath $tempCer | Out-Null

try {
    Write-Host "==> Import to LocalMachine\Root (Trusted Root)..." -ForegroundColor Cyan
    Import-Certificate -FilePath $tempCer -CertStoreLocation "Cert:\LocalMachine\Root" | Out-Null

    Write-Host "==> Import to LocalMachine\TrustedPublisher..." -ForegroundColor Cyan
    Import-Certificate -FilePath $tempCer -CertStoreLocation "Cert:\LocalMachine\TrustedPublisher" | Out-Null

    Write-Host ""
    Write-Host "Setup done." -ForegroundColor Green
    Write-Host "Subject : $CertSubject" -ForegroundColor Gray
    Write-Host "Expires : $($cert.NotAfter)" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Next: run 'npm run tauri dev'. Cargo runner will auto-sign binary." -ForegroundColor Yellow
    Write-Host "If SAC still blocks (cloud reputation), must disable SAC in Settings." -ForegroundColor Yellow
} finally {
    Remove-Item $tempCer -ErrorAction SilentlyContinue
}
