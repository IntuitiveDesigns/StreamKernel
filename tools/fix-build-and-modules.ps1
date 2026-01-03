<#
 Copyright 2026 Steven Lopez

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 SPDX-License-Identifier: Apache-2.0
#>

# ==============================
# StreamKernel build stabilizer
# ==============================
$ErrorActionPreference = "Stop"

function Info($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Warn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }

$root = Get-Location

# ------------------------------------------------
# 1. Ensure every module using deps has java-library
# ------------------------------------------------
Get-ChildItem -Path $root -Recurse -File -Filter build.gradle |
  Where-Object { $_.FullName -notmatch '\\build\\' } |
  ForEach-Object {
    $t = Get-Content $_.FullName -Raw

    $usesDeps = $t -match '(implementation|api|compileOnly|annotationProcessor)'
    $hasJava =
      $t -match "id\s+'java'" -or
      $t -match "id\s+'java-library'" -or
      $t -match "apply\s+plugin:\s*'java'" -or
      $t -match "apply\s+plugin:\s*'java-library'"

    if ($usesDeps -and -not $hasJava) {
      Info "Patching java-library into ${($_.FullName)}"
      Set-Content -Encoding UTF8 -Path $_.FullName -Value ("plugins { id 'java-library' }`r`n`r`n" + $t)
    }
  }

# ------------------------------------------------
# 2. Enforce required core dependencies
# ------------------------------------------------
$coreGradle = Join-Path $root "streamkernel-core\build.gradle"
if (Test-Path $coreGradle) {
  $core = Get-Content $coreGradle -Raw

  if ($core -notmatch 'kafka-clients') {
    Info "Adding kafka-clients to streamkernel-core"
    $core = $core -replace 'dependencies\s*\{',
@"
dependencies {
    implementation 'org.apache.kafka:kafka-clients:3.7.0'
    implementation 'io.micrometer:micrometer-core:1.13.3'
    implementation 'org.slf4j:slf4j-api:2.0.13'
    implementation 'ch.qos.logback:logback-classic:1.5.6'
"@
  }

  Set-Content -Encoding UTF8 $coreGradle $core
}

# ------------------------------------------------
# 3. Enforce SPI dependencies (logging + metrics)
# ------------------------------------------------
$spiGradle = Join-Path $root "streamkernel-spi\build.gradle"
if (Test-Path $spiGradle) {
  $spi = Get-Content $spiGradle -Raw

  if ($spi -notmatch 'slf4j-api') {
    Info "Adding SPI dependencies"
    $spi = $spi -replace 'dependencies\s*\{',
@"
dependencies {
    api 'org.slf4j:slf4j-api:2.0.13'
    api 'io.micrometer:micrometer-core:1.13.3'
"@
  }

  Set-Content -Encoding UTF8 $spiGradle $spi
}

# ------------------------------------------------
# 4. Remove core-only demos from main build
# ------------------------------------------------
$excludeList = @(
  'HardcodedBenchmark.java',
  'SinkBenchmarkApp.java'
)

Get-ChildItem streamkernel-core -Recurse -Include *.java |
  Where-Object { $excludeList -contains $_.Name } |
  ForEach-Object {
    Warn "Excluding demo class $($_.FullName)"
    Rename-Item $_.FullName ($_.FullName + ".disabled")
  }

Info "Script completed successfully"
