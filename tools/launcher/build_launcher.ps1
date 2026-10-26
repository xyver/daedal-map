param(
    [Parameter(Mandatory = $true)][string]$BuildPython,
    [Parameter(Mandatory = $true)][string]$UvExe,
    [Parameter(Mandatory = $true)][string]$OutputRoot
)

$ErrorActionPreference = 'Stop'
$wrapperRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$programsRoot = Split-Path -Parent $wrapperRoot
$publicRoot = Split-Path -Parent $programsRoot
$output = [System.IO.Path]::GetFullPath($OutputRoot)
$dist = Join-Path $output 'dist'
$build = Join-Path $output 'work'
$spec = Join-Path $output 'spec'
$bundle = Join-Path $dist 'DaedalMapLauncher'
$zip = Join-Path $output 'DaedalMapLauncher-windows-test.zip'

if (-not (Test-Path -LiteralPath $BuildPython -PathType Leaf)) { throw "Build Python missing: $BuildPython" }
if (-not (Test-Path -LiteralPath $UvExe -PathType Leaf)) { throw "uv.exe missing: $UvExe" }
$uvVersion = (& $UvExe --version).Trim()
if ($LASTEXITCODE -ne 0 -or $uvVersion -notmatch '^uv 0\.13\.5(?:\s|$)') {
    throw "Expected uv 0.13.5 for this launcher build; found: $uvVersion"
}
$uvHash = (Get-FileHash -LiteralPath $UvExe -Algorithm SHA256).Hash.ToLowerInvariant()
if ($uvHash -ne 'f45d1cb9d9140ca6d3f4005fdfc0ebeccf849fd04d1dc85804155cb923f1597b') {
    throw "uv.exe does not match the pinned Windows build"
}
if (-not (Test-Path -LiteralPath (Join-Path $publicRoot 'local_data_store.py') -PathType Leaf)) {
    throw "Public local_data_store.py missing: $publicRoot"
}
$brandAsset = Join-Path $wrapperRoot 'assets\brand_mark.png'
if (-not (Test-Path -LiteralPath $brandAsset -PathType Leaf)) { throw "Brand mark missing: $brandAsset" }
$brandData = "$brandAsset;assets"
New-Item -ItemType Directory -Force -Path $output | Out-Null
& $BuildPython -m PyInstaller --noconfirm --clean --onedir --windowed `
    --name DaedalMapLauncher --collect-all tkinterdnd2 `
    --paths $publicRoot --hidden-import local_data_store --add-data $brandData `
    --distpath $dist --workpath $build --specpath $spec `
    (Join-Path $wrapperRoot 'launcher.py')
if ($LASTEXITCODE -ne 0) { throw "PyInstaller exited $LASTEXITCODE" }
Copy-Item -LiteralPath $UvExe -Destination (Join-Path $bundle 'uv.exe') -Force
Copy-Item -LiteralPath (Join-Path $wrapperRoot 'TEST_BUILD_README.txt') -Destination (Join-Path $bundle 'README.txt') -Force
Add-Type -AssemblyName System.IO.Compression.FileSystem
for ($attempt = 1; $attempt -le 5; $attempt++) {
    if (Test-Path -LiteralPath $zip) { Remove-Item -LiteralPath $zip -Force }
    try {
        [System.IO.Compression.ZipFile]::CreateFromDirectory(
            $bundle, $zip, [System.IO.Compression.CompressionLevel]::Optimal, $true)
        break
    } catch {
        if ($attempt -eq 5) { throw }
        Start-Sleep -Seconds 2
    }
}
Write-Output $zip
