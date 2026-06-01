$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Join-Chars {
    param([Parameter(Mandatory=$true)][int[]]$CodePoints)
    return -join ($CodePoints | ForEach-Object { [char]$_ })
}

$NameContractKb = Join-Chars @(0x5408, 0x540C, 0x77E5, 0x8BC6, 0x5E93)
$NameConsole = Join-Chars @(0x5408, 0x540C, 0x77E5, 0x8BC6, 0x5E93, 0x63A7, 0x5236, 0x53F0)
$NameLinked = Join-Chars @(0x8054, 0x63A5, 0x57FA, 0x91D1)
$NameLegalFiles = Join-Chars @(0x6CD5, 0x5F8B, 0x6587, 0x4EF6)
$NameFundContract = Join-Chars @(0x57FA, 0x91D1, 0x5408, 0x540C)
$NameProspectus = Join-Chars @(0x62DB, 0x52DF, 0x8BF4, 0x660E, 0x4E66)
$NameProductSummary = Join-Chars @(0x4EA7, 0x54C1, 0x8D44, 0x6599, 0x6982, 0x8981)
$NameNotice = Join-Chars @(0x516C, 0x544A)
$NameUsage = Join-Chars @(0x4F7F, 0x7528, 0x8BF4, 0x660E)

$ReleaseName = $NameConsole
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
$BuildDir = Join-Path $ProjectRoot "build"
$DistDir = Join-Path $ProjectRoot "dist"
$ReleaseDir = Join-Path $DistDir $ReleaseName
$ZipPath = Join-Path $DistDir "$ReleaseName.zip"
$SpecPath = Join-Path $ProjectRoot "launcher.spec"
$PatchScript = Join-Path $ProjectRoot "patch_release.py"
$TemplatesSource = Join-Path $ProjectRoot "templates"
$Desktop = [Environment]::GetFolderPath("Desktop")
$WorkspaceRoot = Split-Path -Parent $ProjectRoot
$ConfiguredSystemSourceRoot = $env:CONTRACT_KB_SYSTEM_ROOT
if ([string]::IsNullOrWhiteSpace($ConfiguredSystemSourceRoot)) {
    $ConfiguredSystemSourceRoot = $WorkspaceRoot
}
$SystemSourceRoot = [System.IO.Path]::GetFullPath($ConfiguredSystemSourceRoot)
$EtfSourceName = "ETF" + $NameContractKb
$LinkedSourceName = "ETF" + $NameLinked + $NameContractKb
$LinkedLegalSourceName = $NameLinked + $NameLegalFiles
$EtfSource = Join-Path $SystemSourceRoot $EtfSourceName
if (-not (Test-Path -LiteralPath (Join-Path $EtfSource "app.py"))) {
    $EtfSource = Join-Path $Desktop $EtfSourceName
}
$LinkedSource = Join-Path $SystemSourceRoot $LinkedSourceName
if (-not (Test-Path -LiteralPath (Join-Path $LinkedSource "app.py"))) {
    $LinkedSource = Join-Path $Desktop $LinkedSourceName
}
$EtfLegalSource = Join-Path $SystemSourceRoot $NameFundContract
if (-not (Test-Path -LiteralPath $EtfLegalSource)) {
    $EtfLegalSource = Join-Path $Desktop $NameFundContract
}
$LinkedLegalSource = Join-Path $SystemSourceRoot $LinkedLegalSourceName
if (-not (Test-Path -LiteralPath $LinkedLegalSource)) {
    $LinkedLegalSource = Join-Path $Desktop $LinkedLegalSourceName
}
$SystemsDir = Join-Path $ReleaseDir "systems"

$ExcludedDirectoryNames = @(
    ".git",
    ".pytest_cache",
    "__pycache__",
    ".venv",
    "venv",
    "tests",
    "build",
    "dist",
    "logs",
    "outputs",
    "output",
    "backups"
)

$ExcludedFileNames = @(
    "nul"
)

function Assert-UnderRoot {
    param(
        [Parameter(Mandatory=$true)][string]$Path,
        [Parameter(Mandatory=$true)][string]$Root
    )

    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $fullRoot = [System.IO.Path]::GetFullPath($Root)
    if (-not $fullRoot.EndsWith([System.IO.Path]::DirectorySeparatorChar)) {
        $fullRoot += [System.IO.Path]::DirectorySeparatorChar
    }

    if (-not $fullPath.StartsWith($fullRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Unsafe path outside project root: $fullPath"
    }
}

function Remove-SafeItem {
    param([Parameter(Mandatory=$true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }
    Assert-UnderRoot -Path $Path -Root $ProjectRoot
    Remove-Item -LiteralPath $Path -Recurse -Force
}

function Copy-DirectoryFiltered {
    param(
        [Parameter(Mandatory=$true)][string]$Source,
        [Parameter(Mandatory=$true)][string]$Destination
    )

    if (-not (Test-Path -LiteralPath $Source)) {
        throw "Source directory not found: $Source"
    }

    if (Test-Path -LiteralPath $Destination) {
        Remove-SafeItem -Path $Destination
    }
    New-Item -ItemType Directory -Path $Destination -Force | Out-Null

    $sourceFull = [System.IO.Path]::GetFullPath($Source).TrimEnd('\', '/')
    foreach ($item in Get-ChildItem -LiteralPath $sourceFull -Force -Recurse) {
        if (-not $item.PSIsContainer -and (
            ($ExcludedFileNames -contains $item.Name) -or
            $item.Name -like "test_*.py" -or
            $item.Name -like "*_test.py"
        )) {
            continue
        }

        $relative = $item.FullName.Substring($sourceFull.Length).TrimStart('\', '/')
        if ([string]::IsNullOrWhiteSpace($relative)) {
            continue
        }

        $parts = $relative -split '[\\/]'
        $skip = $false
        foreach ($part in $parts) {
            if ($ExcludedDirectoryNames -contains $part) {
                $skip = $true
                break
            }
        }
        if ($skip) {
            continue
        }

        $target = Join-Path $Destination $relative
        if ($item.PSIsContainer) {
            New-Item -ItemType Directory -Path $target -Force | Out-Null
        } else {
            $targetParent = Split-Path -Parent $target
            New-Item -ItemType Directory -Path $targetParent -Force | Out-Null
            Copy-Item -LiteralPath $item.FullName -Destination $target -Force
        }
    }
}

function Write-UsageNote {
    $notePath = Join-Path $ReleaseDir ($NameUsage + ".txt")
    @"
Contract Knowledge Console

1. Double-click the exe to start the launcher.
2. The browser opens automatically. Busy ports are skipped automatically.
3. systems\etf and systems\linked are editable runtime resources.
4. systems\linked\packaged_assets\product_summary_templates stores linked-fund summary Word templates.
5. systems\etf\packaged_assets\contract_templates and systems\linked\packaged_assets\legal_templates store contract and prospectus Word page-style references.
6. logs stores launcher and child service logs.

Keep the exe, templates, and systems folders in the same directory.
"@ | Set-Content -LiteralPath $notePath -Encoding UTF8
}

function Copy-EtfContractTemplates {
    param([Parameter(Mandatory=$true)][string]$EtfSystemDir)

    if (-not (Test-Path -LiteralPath $EtfLegalSource)) {
        throw "ETF legal reference directory not found: $EtfLegalSource"
    }

    $target = Join-Path $EtfSystemDir "packaged_assets\contract_templates"
    New-Item -ItemType Directory -Path $target -Force | Out-Null

    $templates = Get-ChildItem -LiteralPath $EtfLegalSource -File -Filter "*.docx" |
        Where-Object {
            $_.Name.Contains($NameFundContract) -and
            -not $_.Name.Contains($NameLinked) -and
            -not $_.Name.Contains($NameProductSummary) -and
            -not $_.Name.Contains($NameNotice)
        }

    if (@($templates).Count -lt 1) {
        throw "Expected ETF contract DOCX templates in: $EtfLegalSource"
    }

    foreach ($template in $templates) {
        Copy-Item -LiteralPath $template.FullName -Destination (Join-Path $target $template.Name) -Force
    }
}

function Copy-LinkedLegalTemplates {
    param([Parameter(Mandatory=$true)][string]$LinkedSystemDir)

    if (-not (Test-Path -LiteralPath $LinkedLegalSource)) {
        throw "Linked legal reference directory not found: $LinkedLegalSource"
    }

    $target = Join-Path $LinkedSystemDir "packaged_assets\legal_templates"
    New-Item -ItemType Directory -Path $target -Force | Out-Null

    $templates = Get-ChildItem -LiteralPath $LinkedLegalSource -File -Filter "*.docx" |
        Where-Object {
            ($_.Name.Contains($NameFundContract) -or $_.Name.Contains($NameProspectus)) -and
            -not $_.Name.Contains($NameProductSummary) -and
            -not $_.Name.Contains($NameNotice)
        }

    $contractTemplates = @($templates | Where-Object { $_.Name.Contains($NameFundContract) })
    $prospectusTemplates = @($templates | Where-Object { $_.Name.Contains($NameProspectus) })

    if ($contractTemplates.Count -lt 1) {
        throw "Expected linked contract DOCX templates in: $LinkedLegalSource"
    }
    if ($prospectusTemplates.Count -lt 1) {
        throw "Expected linked prospectus DOCX templates in: $LinkedLegalSource"
    }

    foreach ($template in $templates) {
        Copy-Item -LiteralPath $template.FullName -Destination (Join-Path $target $template.Name) -Force
    }
}

function Copy-LinkedProductSummaryTemplates {
    param([Parameter(Mandatory=$true)][string]$LinkedSystemDir)

    if (-not (Test-Path -LiteralPath $LinkedLegalSource)) {
        throw "Linked legal reference directory not found: $LinkedLegalSource"
    }

    $target = Join-Path $LinkedSystemDir "packaged_assets\product_summary_templates"
    New-Item -ItemType Directory -Path $target -Force | Out-Null

    $templates = Get-ChildItem -LiteralPath $LinkedLegalSource -File -Filter "*.docx" |
        Where-Object { $_.Name.Contains($NameProductSummary) }

    if (@($templates).Count -lt 2) {
        throw "Expected linked product summary DOCX templates in: $LinkedLegalSource"
    }

    foreach ($template in $templates) {
        Copy-Item -LiteralPath $template.FullName -Destination (Join-Path $target $template.Name) -Force
    }
}

if (-not (Test-Path -LiteralPath $SpecPath)) {
    throw "Spec file not found: $SpecPath"
}
if (-not (Test-Path -LiteralPath $PatchScript)) {
    throw "Patch script not found: $PatchScript"
}
if (-not (Test-Path -LiteralPath (Join-Path $EtfSource "app.py"))) {
    throw "ETF source system not found: $EtfSource"
}
if (-not (Test-Path -LiteralPath (Join-Path $LinkedSource "app.py"))) {
    throw "ETF linked source system not found: $LinkedSource"
}
if (-not (Test-Path -LiteralPath $LinkedLegalSource)) {
    throw "Linked legal reference directory not found: $LinkedLegalSource"
}
if (-not (Test-Path -LiteralPath $EtfLegalSource)) {
    throw "ETF legal reference directory not found: $EtfLegalSource"
}

Write-Host "[1/5] Cleaning build output..."
Remove-SafeItem -Path $BuildDir
Remove-SafeItem -Path $ReleaseDir
if (Test-Path -LiteralPath $ZipPath) {
    Remove-SafeItem -Path $ZipPath
}
New-Item -ItemType Directory -Path $DistDir -Force | Out-Null

Write-Host "[2/5] Running PyInstaller..."
python -m PyInstaller --noconfirm --clean $SpecPath
if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller build failed."
}
if (-not (Test-Path -LiteralPath $ReleaseDir)) {
    throw "Expected release directory was not created: $ReleaseDir"
}

Write-Host "[3/5] Copying launcher templates and editable systems..."
Copy-DirectoryFiltered -Source $TemplatesSource -Destination (Join-Path $ReleaseDir "templates")
New-Item -ItemType Directory -Path $SystemsDir -Force | Out-Null
$EtfSystemDir = Join-Path $SystemsDir "etf"
Copy-DirectoryFiltered -Source $EtfSource -Destination $EtfSystemDir
Copy-EtfContractTemplates -EtfSystemDir $EtfSystemDir
$LinkedSystemDir = Join-Path $SystemsDir "linked"
Copy-DirectoryFiltered -Source $LinkedSource -Destination $LinkedSystemDir
Copy-LinkedProductSummaryTemplates -LinkedSystemDir $LinkedSystemDir
Copy-LinkedLegalTemplates -LinkedSystemDir $LinkedSystemDir
python $PatchScript $ReleaseDir
if ($LASTEXITCODE -ne 0) {
    throw "Release patching failed."
}
New-Item -ItemType Directory -Path (Join-Path $ReleaseDir "logs") -Force | Out-Null
Write-UsageNote

Write-Host "[4/5] Verifying release layout..."
foreach ($required in @(
    (Join-Path $ReleaseDir "$ReleaseName.exe"),
    (Join-Path $ReleaseDir "templates\index.html"),
    (Join-Path $ReleaseDir "systems\etf\app.py"),
    (Join-Path $ReleaseDir "systems\linked\app.py"),
    (Join-Path $ReleaseDir "systems\etf\packaged_assets\contract_templates"),
    (Join-Path $ReleaseDir "systems\linked\packaged_assets\product_summary_templates"),
    (Join-Path $ReleaseDir "systems\linked\packaged_assets\legal_templates")
)) {
    if (-not (Test-Path -LiteralPath $required)) {
        throw "Required release file missing: $required"
    }
}

Write-Host "[5/5] Creating zip package..."
Compress-Archive -LiteralPath $ReleaseDir -DestinationPath $ZipPath -Force

Write-Host ""
Write-Host "Build complete."
Write-Host "Release directory: $ReleaseDir"
Write-Host "Zip package:       $ZipPath"
