# Network

## Mapping expansion: first global viewer test

Network now has a local Mapping adapter in `app/mapping_extension.py` and
`app/static/mapping_extension.js`. With the optional Network package present,
the local Mapping runtime shows a **Network layers** panel on its existing
MapLibre map. It discovers validated `network-pack-v1` folders under
`DATA_ROOT/networks/packs/`; a source checkout can set `NETWORK_PACKS_DIR` to
`network/data/packs` to inspect the current rail, power, and river candidates.
The pack reader checks manifest hashes before displaying a layer. The UI opens
any of the four current pack types: road, rail, power, and river.

The preferred edit loop uses the public launcher in Local Custom mode with
linked Mapping and Network checkouts. From the public launcher checkout, the
source entry point can use the same data library without building program ZIPs:

```powershell
python tools/launcher/launcher.py --local-source --library-root "<launcher-data-folder>"
```

In the local launcher Programs tab, **Open Network Map** opens Network on the
Mapping map, including when automatic Network startup is off. **Open Network
Editor** opens a standalone spreadsheet at `http://127.0.0.1:8124/`. It reads
validated candidate packs under `network/data/packs` without starting Mapping
or showing a map. Nodes, links, and features are paged, searchable tables.
Editable cells are saved as draft Parquet under the launcher's data library in
`networks/editor_drafts/<pack_id>/edits.parquet`; the source packs remain
immutable. Graph IDs and feature IDs are fixed. The packaged Network program
currently has only the Mapping view; packaging the spreadsheet editor is next.

The editor also starts directly from the `county-map` checkout, without the
launcher or Mapping:

```powershell
& ..\.venv\Scripts\python.exe -m uvicorn network.spreadsheet_editor:app --host 127.0.0.1 --port 8124
```

Set `NETWORK_PACKS_DIR` to another validated pack folder and
`NETWORK_EDITOR_DRAFTS_DIR` to another draft folder when needed.

Choose **Open Local Map** in its Explore Map tab. The launcher and map mark the
session **LOCAL SOURCE MODE**. It uses this checkout's Mapping and Network
code, the workspace Python, and the launcher's existing data packs. Stop and
reopen the map after Python edits; refresh the browser after JavaScript edits.

The direct `serve.py` entry point is only a developer fixture for the Mapping
overlay. It runs a map and is not the standalone Network editor. From the
workspace root:

```powershell
& .\.venv\Scripts\python.exe county-map/network/serve.py
```

Open `http://127.0.0.1:7000/`, select a Network layer, and inspect nodes or
links. Stop and rerun the server after Python edits or refresh the browser
after JavaScript edits. The repeatable localhost smoke in
`county-map-private/wrapper/README.md` checks the launcher integration without a ZIP.
This direct entry point uses an empty `.scratch` library and will show the
first-data-pack prompt even if the normal launcher already has installed data.

The separate Network trial package also runs without Mapping as a CLI. List
validated graph packs or export nodes and spatial features as GeoParquet plus
links and future `loc_id` relationships as Parquet. The current relationship
table is empty and explicitly marked `not_enriched`; the manifest separately
marks a feature's own reference `loc_id` as `not_assigned`. The exported
`containing_loc_id` column is currently null. Enrichment will choose the
smallest admin spine polygon containing the full geometry, moving to a common
parent when the feature crosses siblings:

```powershell
Set-Location county-map
& ..\.venv\Scripts\python.exe -m network.cli --packs-dir network/data/packs list
& ..\.venv\Scripts\python.exe -m network.cli --packs-dir network/data/packs export-all --output ..\.scratch\network-parquet
```

The launcher can export the same tables into its shared library. Mapping is
the only map UI. The archived Lake County editor below is retained solely as
a source reference and is not part of the current program or launcher path.

### River size and flow

The current Northern California HydroRIVERS pack stores each reach's
`stream_order`, length, downstream reach ID, basin ID, and boundary status in
its feature properties. It does **not** store discharge. Its 2,803 reaches
include 1,403 order 1 reaches, so a default view that draws every reach is
dense. HydroRIVERS source data also provides `DIS_AV_CMS`, an estimated long
term average discharge in cubic meters per second **for a reach**, not a
measured flow at each graph node. The current builder does not import it.
Future river display controls can first filter or style by stream order:
**All** shows 2,803 reaches in this trial, **order 3+** shows 741, and
**order 5+** shows 115. These are useful display presets for this candidate,
not universal definitions of river size. Add discharge thresholds after the
builder preserves and validates that field. See the
[HydroRIVERS technical documentation](https://data.hydrosheds.org/file/technical-documentation/HydroRIVERS_TechDoc_v10.pdf).

### Source fields retained in candidate packs

These three Northern California packs are **local, gitignored candidates**, not
published Network data downloads. Their builders select a bounded region and
only a few source attributes. The raw source files remain separate and have
not been altered. No release process discarded columns; the omissions happen
in the candidate builders before `network-pack-v1` is written. The field
selection kept graph IDs and attributes needed for an initial inspection UI,
but it was not a reviewed policy for a full data pack.

| Source | Kept or transformed | Present in source but omitted from candidate |
| --- | --- | --- |
| HydroRIVERS | Reach ID, downstream ID, length, Strahler order, basin level 12 ID, line geometry | Main river ID, distances to outlet and headwater, local catchment and total upstream area, endorheic flag, average discharge, classical and flow orders (`MAIN_RIV`, `DIST_DN_KM`, `DIST_UP_KM`, `CATCH_SKM`, `UPLAND_SKM`, `ENDORHEIC`, `DIS_AV_CMS`, `ORD_CLAS`, `ORD_FLOW`) |
| NARN rail arcs | Arc and endpoint IDs, miles converted to `distance_km`, first owner, state, network class, passenger flag, line geometry | County and district codes, other owners and track-rights holders, division/subdivision/branch/yard, track count, source `KM`, timezone, source shape length and related fields |
| NARN rail nodes | Referenced node IDs and point geometry | Country, state and county codes, district, passenger and station flags, boundary flag. The builder reads `PASSNGRSTN` but currently does not save it. |
| Federal transmission lines | Line ID, first and second substation labels, status, voltage, line geometry; graph endpoints inferred from rounded line ends | Type, owner, voltage class, inferred flag, NAICS, source, source/validation dates and method, source shape lengths and object IDs. Four multipart features were skipped because the initial builder handles only single lines. |

The river and rail selections should be reviewed before a downloadable data
pack is built. Discharge, upstream area, river ordering and rail track/owner
attributes have clear uses for filtering and analysis; their absence should
not be treated as a property of the source. Power topology remains
inspection-only because the inferred endpoint matches have not been verified.
Each future pack release should record a source-field inventory and explicit
reasons for omissions alongside its provenance and redistribution review.

### California rail geometry assignment trial

`network.geometry_assign` is the first bounded Geometry consumer for Network.
It reads the NARN candidate and the pinned USA Geometry detail 1.3.5 ZIP,
isolates lines fully covered by `USA-CA`, resolves referenced rail nodes by
exact point containment, and verifies each track against its full line. It
writes `node_loc_ids.parquet`, `feature_loc_ids.parquet`, and `report.json` to a
separate output folder without changing either source artifact:

```powershell
Set-Location county-map
& ..\.venv\Scripts\python.exe -m network.geometry_assign `
  --pack-dir network/data/packs/usa-narn-rail-norcal-2024 `
  --geometry-zip ../county-map-data/downloadable/geometry/USA/united-states-geometry-detail/stable/1.3.5/united-states-geometry-detail-1.3.5.zip `
  --state-loc-id USA-CA --output ../.scratch/network-ca-geometry-v1
```

This v1 trial stops at Admin 3 and records a common parent for points or
lines on shared boundaries. It does not yet produce all intersected-area or
clipped-length relationships, so these placement tables are not a complete
geographic analysis export.

For larger real-source tests, `python -m network.geometry_benchmark` accepts a
validated rail, power, or river pack, `--geometry-zip`, `--sizes`, and a small
JSON `--output`. It samples links with a fixed seed, discards each size's
temporary Parquet tables, and records separate geometry read, index, scope,
node, line, and write timings. The national source packs used for the first
scale trial are temporary local inputs, not package assets; its durable result
is in the private Network Geometry scale note. `--profile-only` measures state
spread, vertices, and spatial-index candidates; `--points-only` repeats
equal-sized node batches on one loaded exact index to isolate point-predicate
cost from line and export work.

Raw PBF, GeoPackage, Shapefile, GeoJSON, and GeoParquet imports still need
type-specific graph adapters. The spreadsheet currently records draft cell
edits; draft validation and publishing back to a new pack version are next.
The Lake County window below is archived reference material only.

Run the focused contract check from `network/` with the workspace environment:

```powershell
& '..\..\.venv\Scripts\python.exe' -m unittest discover -s tests -p 'test_mapping_extension.py' -v
```

## Archived Lake County road editor reference

The remaining sections describe the archived Lake County road scenario editor.
They are retained for feature inspiration and historical reproduction. The
current launcher, standalone spreadsheet, Network pack viewer, and future
program ZIP do not launch or bundle that editor.

Local-only prototype for road-closure scenarios. The source now lives in
`network/` inside the public DaedalMap repository. It remains runnable on its
own for now: it does not import the Mapping app and accepts prepared/source
artifacts as explicit Docker mounts. A later Network extension will use the
shared Mapping display interface.

The [Network tool entry](../tools/network/README.md) records the planned
contained source location and the current migration boundary. The
[single-tool guide](../docs/WORK_ON_ONE_TOOL.md#network-source-checkout)
shows how to check out and adapt only this prototype.

The program source is licensed under [MIT](LICENSE), matching the public
DaedalMap map runtime. Source data, prepared network graphs, shelter snapshots,
and other installed packs retain their own source terms; the software licence
does not grant redistribution rights for those inputs.

## Scope

- study origins: Lake County, California
- road context: Lake County only, plus a small (~9 km) cross-border buffer so
  routes do not stop artificially at the county line
- road source: the local California Geofabrik PBF
- shelter source: the local NSS shelter parquet

The regional graph is never overwritten. Each scenario stores only a mapping
of `segment_id -> state`:

`open -> forward_only -> reverse_only -> closed -> open`

`forward` and `reverse` refer to the direction in which the segment geometry
is stored and drawn. The map highlights the active state after every click.

## First run

Read [Download and prepare data](docs/DATA_SETUP.md) before running the
commands below. It explains exactly which road and shelter inputs are needed,
where to get them, how the static FEMA inventory differs from live open
shelters, and how to create the small standalone input folder.

From the public repository root:

```powershell
Set-Location network
$env:ROAD_EDITOR_OSM_DIR = "C:\path\to\geofabrik"
$env:ROAD_EDITOR_SOURCE_DATA_DIR = "C:\path\to\county-map-data"

docker compose build
docker compose run --rm editor python scripts/prepare_lake_network.py
docker compose up
```

Then open `http://localhost:8123`.

For development after the dependencies have been built once, run the prepared
compose file with `docker-compose.dev.yml` as an overlay:

```powershell
docker compose -f docker-compose.prepared.yml -f docker-compose.dev.yml up -d editor
```

This mounts `app/` and `scripts/` into the container. HTML, CSS, and JavaScript
edits appear after a browser refresh; Python changes restart Uvicorn
automatically. Rebuild only when dependencies or the Dockerfile change.
`docker-compose.prepared.yml` alone remains the frozen-image smoke test. The
downloadable Network extension will run on the Mapping local runtime; it will
not require Docker for ordinary users.

The prep command writes these local, gitignored artifacts under `data/baseline/`:

- `lake_region.osm.pbf` - trimmed regional road extract
- `road_tiles/` - viewport-sized clickable physical road chunks
- `graph.json` - directed graph used by the scenario routing API
- `scope.json` - selected counties and bounds

This means a clone contains the application and reproducible preparation code,
but not the generated road graph, road tiles, shelter snapshot, or the old
repository's California reference-data package. The old data package remains
available in the frozen [road editor repository](https://github.com/xyver/road-network-editor/tree/main/reference_data/california);
its upstream terms apply separately. See
[Download and prepare data](docs/DATA_SETUP.md) for the required two-file
prepared data bundle and the California road download.

For a collaborator who only needs to run the already-prepared Lake County
prototype, send them a ZIP of `data/baseline/`. After extracting it into their
clone, they can start the editor with no source-data mounts:

```powershell
docker compose -f docker-compose.prepared.yml up --build
```

The mounted inputs are read-only. For this workspace, use:

```powershell
$env:ROAD_EDITOR_OSM_DIR = "C:\Users\Bryan\Desktop\global map\county-map-raw\source_data\osm\geofabrik"
$env:ROAD_EDITOR_SOURCE_DATA_DIR = "C:\Users\Bryan\Desktop\global map\county-map-data"
```

The map cannot zoom or pan outside this small Lake County scope. Its widest
view shows only major roads; zoom in for the full local editable network.

## Typed network pack candidates

The shared pack reader and `/packs` preview can inspect local rail, river, and
power candidates beside the existing road editor. Candidate outputs belong in
`data/packs/` (gitignored); they are not release data. The reader checks payload
hashes and graph references before activation. Each source builder needs the
geospatial Python environment; the viewer only needs the prepared pack.

For the future shared local library, set `DATA_ROOT` to the same directory used
by Mapping, Research MCP, and Geometry MCP. Network then reads and writes under
`DATA_ROOT/networks/` (`packs/`, `pack_scenarios/`, `baseline/`, and `tiles/`).
`ROAD_EDITOR_DATA_DIR` takes precedence for this prototype and the Docker
config currently sets it to `/app/data`; without either setting, Network uses
this checkout's `data/` directory. Existing prototype files are not migrated
automatically. A normal user download must install validated network packs
into the shared library and keep scenarios there across software updates.

From `network/`, build the bounded Northern California examples with:

```powershell
python scripts/build_narn_rail_pack.py --source-archive "../../county-map-raw/Raw data/network_sources/USA/2026-09-17/fra_bts_narn/dot_53568_DS3.zip"
python scripts/build_hydrorivers_pack.py --source-archive "../../county-map-raw/geometry/hydrosheds/hydrorivers/HydroRIVERS_v10_na_shp.zip"
python scripts/build_transmission_pack.py --source-archive "../../county-map-raw/Raw data/network_sources/USA/2026-10-02/federal_energy_layers/transmission_lines_archive.geojson"
```

The shared API lists validated packs at `GET /api/packs`, serves map geometry
and feature details under `/api/packs/{pack_id}/features`, and accepts bounded
rail route and river trace requests. Rail arcs preserve FRA IDs and allow
distance detours after an arc is closed. River links follow source `NEXT_DOWN`
IDs; an exit from the selected area ends at a marked boundary node. The power
candidate preserves line IDs, voltage, status, and labels for inspection. Its
coordinate-derived endpoint joins are unverified, so power connectivity and
outage analysis are disabled.

The preview uses external Leaflet assets and OpenStreetMap standard tiles for
interactive viewing; it is not an offline release. Do not bulk download or
prefetch tiles from this public server. An offline package needs a separate
licensed or self-hosted basemap. The road editor's **Map tiles** setting is the
local Mapping default; **Choose network** opens the pack viewer, which inherits
that setting unless its own Network tile override is selected. Both accept an
XYZ URL template and attribution. These settings live in this browser's local
storage on this origin; they do not yet read the online Explore MapLibre style.
For an already licensed local XYZ folder, unpack `z/x/y.png` under
`network/data/tiles/` and choose `/tiles/{z}/{x}/{y}.png`. Leaflet and the
starter basemap remain online dependencies until the download bundles the
library and a local tile source. Rail closures and river trace exclusions can
be saved as scenarios under the gitignored `data/pack_scenarios/` folder. A saved scenario
is tied to the exact pack manifest hash; reopening it against a changed pack
returns an error instead of silently applying stale feature IDs.
No candidate pack should be distributed until its source terms and validation
receipt are reviewed.

Run `python scripts/check_pack_candidates.py` to verify the three local packs.
With the prepared Docker editor running on port 8123, run
`python scripts/check_pack_service.py` to check the live preview and scenario
save/load/delete API; it removes its own test scenario.

## Batch scenario analysis

The interactive map edits one scenario at a time. For Lake County batch
analysis—multiple homes/origins, scenario comparison, and first-pass shelter
placement recommendations—start with [Scenario Optimizer](docs/SCENARIO_OPTIMIZER.md).

To let a local LLM run those operations through narrow, structured tools, see
[Local MCP setup](docs/MCP_SETUP.md).

California-wide population, shelter, and risk context used for planning is
documented in [California reference data](docs/REFERENCE_DATA.md).
It explains the baseline-versus-scenario model, origin/candidate inputs, and
how the scripts support future local MCP tools. Command details are in
[scripts/README.md](scripts/README.md).

## Deliberate build order

1. **Now:** base map tiles and an editable road-network layer. Clicking a
   segment proves that the UI and persisted graph overlay agree on its stable
   ID and its four-state availability cycle.
2. **Now:** display the scoped FEMA NSS inventory as grey pins and query FEMA
   Open Shelters live for green pins.
3. **Next:** add a map click / geocoder origin control and enable travel-time
   calculations against the active road scenario and selected shelters.

The first graph is intentionally a transparent prototype graph: OSM road ways
are split at their existing OSM nodes, travel times use OSM `maxspeed` where
available and conservative highway-class defaults otherwise, and turn
restrictions are not yet modeled. It is suitable for editing and scenario UX,
not an evacuation-direction authority. The next calibration step is comparing
its baseline times against the existing Valhalla results.

## API

- `GET /api/network?zoom=...&bbox=min_lon,min_lat,max_lon,max_lat` - clickable road
  GeoJSON for the current map viewport, with its scenario state
- `POST /api/scenarios/{scenario_id}/segments/{segment_id}/cycle` - advances
  the four-state cycle
- `POST /api/scenarios/{scenario_id}/routes` - snaps a start point and shelter
  points to the editable graph, then returns travel minutes and the count
  reachable within a requested time limit

- `GET /api/shelters` - scoped NSS inventory snapshot used for gray pins
- `GET /api/shelters/live` - current FEMA Open Shelters for the editable road
  scope; cached by the editor for two minutes and refreshed on the map every
  five minutes
- `GET /api/scenarios/{scenario_id}/shelters` - added and hidden shelter
  changes for a scenario

Scenario state is persisted as JSON in `data/scenarios/` and remains after a
container restart. Road edits and shelter additions/removals are saved in the
scenario; the selected start point remains a browser-session control.
