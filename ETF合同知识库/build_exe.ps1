$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

$specFile = Get-ChildItem $projectRoot -Filter "*.spec" | Select-Object -First 1
if ($null -eq $specFile) {
    throw "Spec file not found in project root."
}

$launcherFile = Get-ChildItem $projectRoot -Filter "*.bat" |
    Where-Object { $_.Name -ne "start.bat" } |
    Select-Object -First 1
if ($null -eq $launcherFile) {
    throw "Launcher batch file not found in project root."
}

Write-Host "[1/4] Preparing packaged_assets ..."
python -m packaging_support --project-root $projectRoot --clean
if ($LASTEXITCODE -ne 0) {
    throw "Failed to prepare packaged_assets."
}

Write-Host "[2/4] Cleaning build/dist ..."
foreach ($dirName in @("build", "dist")) {
    $fullPath = Join-Path $projectRoot $dirName
    if (Test-Path $fullPath) {
        Remove-Item $fullPath -Recurse -Force
    }
}

Write-Host "[3/4] Running PyInstaller ..."
python -m PyInstaller --noconfirm --clean $specFile.FullName
if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller build failed."
}

$distRoot = Get-ChildItem (Join-Path $projectRoot "dist") -Directory | Select-Object -First 1
if ($null -eq $distRoot -or -not (Test-Path $distRoot.FullName)) {
    throw "Build output directory not found."
}

Write-Host "[4/4] Copying runtime data and launcher ..."
foreach ($file in Get-ChildItem $projectRoot -File | Where-Object { $_.Extension -in @(".md", ".json") }) {
    Copy-Item $file.FullName (Join-Path $distRoot.FullName $file.Name) -Force
}

foreach ($dirName in @("templates", "manual_regression_20260310_201555", "prospectus_materials", "packaged_assets")) {
    $sourceDir = Join-Path $projectRoot $dirName
    if (-not (Test-Path $sourceDir)) {
        continue
    }
    $targetDir = Join-Path $distRoot.FullName $dirName
    if (Test-Path $targetDir) {
        Remove-Item $targetDir -Recurse -Force
    }
    Copy-Item $sourceDir $targetDir -Recurse -Force
}

Copy-Item $launcherFile.FullName (Join-Path $distRoot.FullName $launcherFile.Name) -Force

Write-Host ""
Write-Host "Build complete. Output directory:"
Write-Host $distRoot.FullName
