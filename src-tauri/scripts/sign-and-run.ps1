# Cargo runner wrapper: sign binary with dev cert then execute.
#
# Called by cargo with binary path as first arg. Looks up cert by Subject
# (set up by setup-dev-cert.ps1), signs binary with Authenticode SHA256,
# then executes with remaining args passed through.

$ErrorActionPreference = "Stop"

$exePath = $args[0]
$exeArgs = if ($args.Count -gt 1) { $args[1..($args.Count - 1)] } else { @() }

if (-not (Test-Path $exePath)) {
    Write-Error "Binary not found: $exePath"
    exit 1
}

$CertSubject = "CN=ThongKeShopee Dev"

# Find cert in CurrentUser\My by subject. -CodeSigningCert filters to certs
# with Code Signing EKU. If not found, fall back to running unsigned.
$cert = Get-ChildItem Cert:\CurrentUser\My -CodeSigningCert -ErrorAction SilentlyContinue |
    Where-Object { $_.Subject -eq $CertSubject } |
    Select-Object -First 1

if ($null -eq $cert) {
    Write-Warning "Dev cert '$CertSubject' not found."
    Write-Warning "Run scripts/setup-dev-cert.ps1 (Admin PowerShell) once."
    Write-Warning "Skipping sign - SAC will likely block."
} else {
    # Check if binary already signed with same cert. Cargo incremental builds
    # often leave the final binary unchanged, so we can skip re-signing.
    $existing = Get-AuthenticodeSignature -FilePath $exePath
    $needSign = $existing.Status -ne "Valid" -or `
                $existing.SignerCertificate.Thumbprint -ne $cert.Thumbprint
    if ($needSign) {
        $result = Set-AuthenticodeSignature `
            -FilePath $exePath `
            -Certificate $cert `
            -HashAlgorithm SHA256 `
            -ErrorAction Continue
        if ($result.Status -ne "Valid") {
            Write-Warning "Sign failed: $($result.StatusMessage)"
        }
    }
}

# Execute with args passed through. & operator runs native exe.
& $exePath @exeArgs
exit $LASTEXITCODE
