# Lake County Road Network Editor

Local-only prototype for road-closure scenarios. The source now lives in
`network/` inside the public DaedalMap repository. It remains runnable on its
own for now: it does not import the Mapping app and accepts prepared/source
artifacts as explicit Docker mounts. A later Network extension will use the
shared Mapping display interface.

The [Network tool entry](../tools/network/README.md) records the planned
contained source location and the current migration boundary. The
[single-tool guide](../docs/WORK_ON_ONE_TOOL.md#network-prototype-available-now)
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
