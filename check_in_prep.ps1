# check_in_prep.ps1

$root = Get-Location
Write-Host "=== StreamKernel Release Prep Tool ===" -ForegroundColor Cyan

# ==========================================
# 1. Dependency Auditor (Jackson Version Check)
# ==========================================
Write-Host "`n[1/3] Auditing build.gradle files for hardcoded Jackson versions..." -ForegroundColor Yellow
$gradleFiles = Get-ChildItem -Path $root -Recurse -Filter "build.gradle"
$versionReport = @()

foreach ($file in $gradleFiles) {
    $content = Get-Content $file.FullName

    $jacksonLines = $content | Select-String -Pattern 'jackson.*?[''"](\d+\.\d+\.\d+)[''"]'

    foreach ($match in $jacksonLines) {
        $relativePath = $file.FullName.Replace($root.Path, "")
        $versionReport += [PSCustomObject]@{
            Module  = $relativePath
            Library = $match.Line.Trim()
        }
    }
}

if ($versionReport.Count -gt 0) {
    $versionReport | Sort-Object Module | Format-Table -AutoSize
    Write-Host "[ACTION REQUIRED] Found hardcoded versions above. Move these to root build.gradle!" -ForegroundColor Red
} else {
    Write-Host "[OK] No hardcoded Jackson versions found." -ForegroundColor Green
}

# ==========================================
# 2. License Header Enforcer
# ==========================================
Write-Host "`n[2/3] Enforcing Apache 2.0 Headers on all Java files..." -ForegroundColor Yellow

$correctHeader = @"
/*
 * Copyright 2026 Steven Lopez
 * SPDX-License-Identifier: Apache-2.0
 */

"@

$javaFiles = Get-ChildItem -Recurse -Filter "*.java"
$fixedCount = 0

foreach ($file in $javaFiles) {
    $content = Get-Content $file.FullName -Raw

    # We look specifically for the SPDX tag
    if ($content -notmatch "SPDX-License-Identifier: Apache-2.0") {
        # Preserve original content, just prepend header
        $newContent = $correctHeader + $content
        Set-Content -Path $file.FullName -Value $newContent -NoNewline
        $fixedCount++
    }
}
Write-Host "[OK] Applied headers to $fixedCount files." -ForegroundColor Green

# ==========================================
# 3. Final Build Verification
# ==========================================
Write-Host "`n[3/3] Verifying Build Integrity (skipping tests)..." -ForegroundColor Yellow
cmd /c "gradlew clean build -x test"

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n[SUCCESS] READY TO COMMIT!" -ForegroundColor Green
    # Simplified string to ensure compatibility
    Write-Host "You can now run: git add . ; git commit -m 'Initial Commit'" -ForegroundColor Gray
} else {
    Write-Host "`n[FAIL] Build failed. Fix errors before committing." -ForegroundColor Red
}