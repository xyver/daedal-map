# Download and prepare data

This guide gets a new collaborator from a clean clone to a runnable Lake
County Road Network Editor. Start with **Route A** unless you are deliberately
rebuilding the Lake County road graph or refreshing its grey shelter snapshot.

- **Route A — run the prepared prototype (recommended for collaborators):**
  clone the repository, add the supplied `lake-county-editor-baseline.zip`, and
  start Docker. No road download or County Map data is needed.
- **Route B — prepare a new baseline (maintainer/data-refresh work):** download
  the California road extract and provide the two prepared County Map files.

The editor keeps its generated baseline outside Git. This is why a supplied
baseline ZIP is needed for Route A.

## Start from scratch: Route A (recommended)

These are the complete instructions for a new Windows collaborator.

### A1. Install prerequisites

1. Install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/).
2. Open Docker Desktop and wait until it says the engine is running.
3. Install [Git for Windows](https://git-scm.com/download/win) if `git` is not
   already available in PowerShell.
4. Open a new PowerShell window and confirm Docker is available:

```powershell
docker version
```

If this command fails, start Docker Desktop, wait a minute, and retry. An LLM
assistant can help diagnose Docker Desktop or WSL setup, but do not continue
until `docker version` prints both Client and Server information.

### A2. Clone the editor

Choose a working folder, then run:

```powershell
git clone https://github.com/xyver/daedal-map.git
Set-Location .\daedal-map\network
```

If Git is not practical, download the repository ZIP from GitHub, extract it,
and open PowerShell in the extracted `network` folder instead.

### A3. Add the prepared Lake County baseline

Obtain `lake-county-editor-baseline.zip` from the project maintainer. Copy it
into the repository folder beside `README.md`, then run:

```powershell
Expand-Archive .\lake-county-editor-baseline.zip -DestinationPath .\data
Test-Path .\data\baseline\graph.json
Test-Path .\data\baseline\shelters.geojson
```

Both checks must print `True`. The archive already contains the `baseline/`
folder, so extracting it to `data/` creates the required
`data/baseline/` path.

### A4. Start the editor

Run:

```powershell
docker compose -f docker-compose.prepared.yml up --build
```

The first run downloads the Python/Docker image and can take several minutes.
Leave that PowerShell window running. When it says `Uvicorn running on
http://0.0.0.0:8123`, open <http://localhost:8123> in a browser.

To stop the editor, return to that PowerShell window and press `Ctrl+C`. To run
it in the background instead, use:

```powershell
docker compose -f docker-compose.prepared.yml up --build -d
```

Then stop it later with:

```powershell
docker compose -f docker-compose.prepared.yml down
```

Route A does **not** require either `ROAD_EDITOR_OSM_DIR` or
`ROAD_EDITOR_SOURCE_DATA_DIR`.

### A5. Optional: keep or share a scenario

The prepared baseline ZIP is immutable road/shelter input data. It does not
include any local road edits, shelter changes, or saved scenarios. To retain a
scenario on one machine, use **Save scenario** in the map. To pass that state
to batch tools or another collaborator, export the named UI save as documented
in [Scenario Optimizer](SCENARIO_OPTIMIZER.md). The exported JSON is small and
can travel separately from the baseline ZIP.

### A6. Optional: run batch analysis or optimization

Batch scripts need four distinct kinds of local input. They are intentionally
separate so a study can keep its population origins inside Lake County while
allowing routes to reach shelters across the county border.

| Input | Required for | Where it comes from |
| --- | --- | --- |
| `data/baseline/` | every calculation | the prepared baseline ZIP (road graph and filtered shelter snapshot) |
| exported scenario JSON | edited-road/edited-shelter analysis | **Save scenario** in the UI, then export it; optional for an unchanged baseline run |
| origin CSV | access/coverage calculations | Lake County addresses, census centroids, or another list of demand locations |
| candidate CSV | shelter optimization only | a vetted list of plausible future shelter sites |

The initial study convention is **Lake County origins, cross-border
destinations**: people represented by the origin table live in Lake County,
but the active baseline or scenario shelter list may include reachable
shelters in the road graph's small adjacent-county buffer. This is realistic
and is not a data error.

For the generated Census-centroid origin files, obtain or retain these four
prepared geometry-spine files from a County Map/DaedalMap data workspace:

```text
county-map-data/countries/USA/geometry/
  county.parquet
  tract/USA-CA.parquet
  blockgroup/USA-CA.parquet
  block/USA-CA.parquet
```

Then, from this repository, run:

```powershell
python scripts/prepare_census_origins.py `
  --geometry-root "C:\path\to\county-map-data\countries\USA\geometry"
```

This writes Lake-only county, tract, block-group, and block centroid CSVs to
`data/analysis/census_origins/`, after checking their distance to the prepared
road graph. The UI and the scenario scripts can then use them directly. See
[Scenario Optimizer](SCENARIO_OPTIMIZER.md) and [Local MCP setup](MCP_SETUP.md)
for CSV examples and commands.

### Getting new geometry or coordinate inputs

For a new list of homes, potential shelters, or other locations, you do not
need geometry files: make a CSV with a stable ID, longitude, and latitude.
The MCP validates and automatically snaps it to the prepared graph.

For new Census-derived origins, prefer the prepared geometry spine above so
IDs remain compatible with County Map. If it is unavailable, download the
appropriate county, tract, block-group, or tabulation-block geometry from the
[U.S. Census TIGER/Line download page](https://www.census.gov/geographies/mapping-files/2025/geo/tiger-line-file.html).
Use its internal-point longitude/latitude fields or compute polygon centroids,
then write the standard origin CSV. Preserve the Census vintage and GEOID in
your file metadata/IDs.

Adding points outside the prepared Lake-plus-buffer road graph is not enough
to create a valid new study area. First rebuild the baseline with OSM roads and
shelters covering the larger area using Route B; then prepare or import the
corresponding origins and candidates.

## Prepare a new baseline: Route B

Use this route only to rebuild the road network or update the static grey
shelter inventory. It needs the additional data described below.

## What you need

- Docker Desktop, running
- about 3 GB free disk space for the California road download and Docker
  images; Route A needs substantially less after Docker is installed
- an internet connection for the initial downloads, map tiles, and the live
  FEMA Open Shelters overlay

The setup needs these two input roots:

```text
editor-inputs/
  osm/
    california-latest.osm.pbf
  source-data/
    countries/USA/geometry/county.parquet
    countries/USA/nss_shelters/shelters.parquet
```

`source-data/` is intentionally a *small, prepared data root*, not a copy of
the entire County Map project. The editor reads only the two listed files.

## B1. Download California roads

1. Open the [Geofabrik California download page](https://download.geofabrik.de/north-america/us/california.html).
2. Download **`california-latest.osm.pbf`**.
3. Place it at `editor-inputs/osm/california-latest.osm.pbf`.

The preparation step extracts Lake County plus a roughly 9 km border buffer;
it does not retain California's whole road network in the editor baseline.
OpenStreetMap-derived data requires OpenStreetMap attribution and is available
under the ODbL; the editor already shows attribution on its base map.

## B2. Get the prepared shelter and county inputs

### Preferred: use the project input bundle

Ask the project maintainer for the current **Lake editor input bundle**, then
unzip or copy its `source-data/` folder into `editor-inputs/source-data/`.
Before continuing, confirm both paths below exist:

```powershell
Test-Path .\editor-inputs\source-data\countries\USA\geometry\county.parquet
Test-Path .\editor-inputs\source-data\countries\USA\nss_shelters\shelters.parquet
```

This is the best path for a collaborator such as Fatemeh: it preserves the
shared County Map geometry IDs and the QA-cleaned FEMA shelter snapshot without
requiring the private conversion pipeline.

### If you already have County Map data

You may point the editor directly at an existing `county-map-data` directory
instead of making `editor-inputs/source-data/`. It must contain:

```text
county-map-data/
  countries/USA/geometry/county.parquet
  countries/USA/nss_shelters/shelters.parquet
```

The shelter parquet is the **Pattern B** FEMA National Shelter System (NSS)
inventory: it provides the complete facility snapshot used for the map's grey
pins. It is normally mostly closed, because it is an inventory rather than an
incident-specific open-shelter feed.

### Refreshing the static FEMA snapshot

The authoritative full inventory is FEMA's `Shelter Locations` ArcGIS layer:

<https://gis.fema.gov/arcgis/rest/services/NSS/FEMA_NSS/FeatureServer/5>

The current County Map import process downloads that service and creates
`shelters.parquet`, including location QA and county identifiers. Do not copy
the raw FEMA CSV directly into this project: the editor expects the prepared
parquet schema above. Until the standalone input bundle is published, ask the
maintainer to refresh and supply that two-file bundle when a new static
snapshot is needed.

FEMA data are U.S. federal-government data; retain the source and retrieval
date when sharing a refreshed snapshot.

## B3. Configure the editor

From the repository root, choose one of the following source-data locations.

### Using a compact input folder

```powershell
$env:ROAD_EDITOR_OSM_DIR = "$PWD\editor-inputs\osm"
$env:ROAD_EDITOR_SOURCE_DATA_DIR = "$PWD\editor-inputs\source-data"
```

### Using an existing County Map workspace

```powershell
$env:ROAD_EDITOR_OSM_DIR = "C:\path\to\editor-inputs\osm"
$env:ROAD_EDITOR_SOURCE_DATA_DIR = "C:\path\to\county-map-data"
```

The Docker mounts are read-only: building the editor will not modify either
input folder.

## B4. Build the local Lake County baseline

Run these commands from the `network/` directory of the public repository:

```powershell
docker compose build
docker compose run --rm editor python scripts/prepare_lake_network.py
docker compose up -d
```

Open <http://localhost:8123> when the final command finishes.

The preparation command writes the small generated road graph, map tiles,
scope, and grey shelter snapshot to `data/baseline/`. Those files are local and
gitignored; delete `data/baseline/` and rerun this step whenever the input road
or static shelter data changes.

## Live open shelters

No local download is needed for current open shelters. While the editor is
running, its server calls FEMA's live Open Shelters service:

<https://gis.fema.gov/arcgis/rest/services/NSS/OpenShelters/FeatureServer/0>

Only open shelters inside the editable Lake County road scope are returned as
green pins. The server caches the response for two minutes and the map asks
for an update every five minutes. It is normal for this layer to show zero
pins when there is no active shelter operation in the area.

## Create a shareable prepared baseline

After a maintainer has completed step 4 once, the original California PBF and
the larger County Map data root are no longer needed to run the editor. The
ready-to-share payload is this folder:

```text
data/baseline/
  graph.json
  lake_region.osm.pbf
  overview_roads.geojson
  road_tiles/
  scope.json
  shelters.geojson
  study_area.geojson
```

It is roughly 46 MB in the current Lake County prototype. `shelters.geojson`
is a small, filtered FEMA NSS snapshot for the road scope; it is not the full
national shelter parquet. Live green pins still come directly from FEMA when
the recipient opens the map.

To make an email-friendly handoff archive from PowerShell:

```powershell
Compress-Archive -Path .\data\baseline -DestinationPath .\lake-county-editor-baseline.zip -Force
```

The recipient should follow Route A above. In short, they clone the repository,
extract this archive to `data/`, then run:

```powershell
docker compose -f docker-compose.prepared.yml up --build
```

They can then open <http://localhost:8123>. This prepared-data command does
not require either `ROAD_EDITOR_OSM_DIR` or `ROAD_EDITOR_SOURCE_DATA_DIR`.
If they later need to rebuild roads or refresh the static grey shelter
snapshot, they need the original inputs and should follow Route B.

For batch scenario analysis after setup, see [Scenario Optimizer](SCENARIO_OPTIMIZER.md).

## Troubleshooting

| Problem | Check |
| --- | --- |
| Docker says `ROAD_EDITOR_OSM_DIR` is missing | Set both environment variables in the same PowerShell window before running `docker compose`. |
| Preparation cannot find the PBF | Verify the file name is exactly `california-latest.osm.pbf` inside the OSM directory. |
| Preparation cannot find county or shelter parquet | Verify `ROAD_EDITOR_SOURCE_DATA_DIR` points to the folder *containing* `countries/`, not the `countries/USA` folder itself. |
| No green pins appear | The live FEMA feed may have no open shelters in the Lake road scope. The grey NSS inventory pins should still appear. |
| I only have a prepared baseline ZIP | Extract it to `data/` so `data/baseline/` exists, then run `docker compose -f docker-compose.prepared.yml up --build`. |
| Need to reset generated data | Stop the stack with `docker compose down`, remove only this repository's `data/baseline/`, then rerun Route B. |
