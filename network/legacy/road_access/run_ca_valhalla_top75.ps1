param(
  [int] $MaxOrigins = 0
)

$ErrorActionPreference = "Stop"

$Root = "C:\Users\Bryan\Desktop\global map"
$Python = "C:\Python312\python.exe"
$LogDir = Join-Path $Root "county-map-private\build\data_prep\road_access\logs"
$OutputDir = "county-map-private\build\data_prep\road_access\output\road_access_ca"
$LogPath = Join-Path $LogDir "ca_valhalla_top75_combined.log"
$SummaryOutput = "$OutputDir\access_summary_admin4_valhalla_top75.parquet"
$EdgeOutput = "$OutputDir\access_edges_within_60min_admin4_valhalla_top75.parquet"

if ($MaxOrigins -gt 0) {
  $SummaryOutput = "$OutputDir\access_summary_admin4_valhalla_top75_smoke.parquet"
  $EdgeOutput = "$OutputDir\access_edges_within_60min_admin4_valhalla_top75_smoke.parquet"
}

New-Item -ItemType Directory -Force $LogDir | Out-Null
Set-Location $Root

$ArgsList = @(
  "county-map-private\build\data_prep\road_access\run_access_calculator.py",
  "--input-dir", $OutputDir,
  "--output-path", $SummaryOutput,
  "--edge-output-path", $EdgeOutput,
  "--edge-within-minutes", "60",
  "--provider", "valhalla_matrix",
  "--valhalla-url", "http://localhost:8002",
  "--checkpoint-every", "100",
  "--valhalla-target-batch-size", "25",
  "--valhalla-max-target-haversine-km", "90",
  "--valhalla-max-target-candidates", "75",
  "--valhalla-search-radius-meters", "5000",
  "--valhalla-timeout-seconds", "30"
)

if ($MaxOrigins -gt 0) {
  $ArgsList += @("--max-origins", "$MaxOrigins")
}

& $Python @ArgsList *>&1 | Tee-Object -FilePath $LogPath

exit $LASTEXITCODE
