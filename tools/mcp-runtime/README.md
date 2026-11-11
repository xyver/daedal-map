# DaedalMap Local MCP Runtime

This headless package runs the Geometry MCP, Research MCP, or both against a
DaedalMap data folder. It does not install the map UI, launcher, or every data
pack.

In the public source checkout, this folder is the shared local server and
smoke check for Research MCP (`tools/research-mcp/`) and Geometry MCP
(`tools/geometry-mcp/`). Run `server.py` from the repository
root with `--services research` or `--services geometry` and the selected
`--source`. The commands below apply to an extracted service ZIP, whose
files are placed at the ZIP root by the exporter.
The extracted ZIP includes `SOURCE.json` with its public source revision and
clean-worktree state. Local Research uses the selected data folder and does
not forward queries to the hosted API.

## Requirements

- Python 3.11 or newer for this source-distribution release
- a DaedalMap data folder containing `catalog.json` for Research
- extracted geometry data packages (each retains its `geometry.json`) for
  local Geometry, a full developer data root with
  `geometry/geometry_catalog.json`, or an explicitly selected cloud folder
  containing that catalog

After extracting the release:

```powershell
py -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python smoke_test.py
.\.venv\Scripts\python server.py --source "D:\DaedalMapData"
```

`smoke_test.py` is offline and creates tiny temporary catalogs. It verifies
the services actually included in this download: startup, MCP initialization and tool
discovery; and all current Geometry and Research tool dispatch paths. Run it
after every fresh install or upgrade before attaching a client.

When `BUNDLE-MANIFEST.json` is present, its `services` list is authoritative:
the server exposes only those services and rejects requests for omitted
domains. A source checkout without that manifest keeps the Geometry + Research
default for development compatibility.

The default endpoints are:

- `http://127.0.0.1:7010/mcp/geometry`
- `http://127.0.0.1:7011/mcp/research`
- `http://127.0.0.1:7010/health`

## Select one data folder

The downloaded server reads one folder selected at startup. `catalog.json`
for Research, `geometry/geometry_catalog.json` for Geometry, source metadata,
and their referenced payloads all belong under that same folder. Select a
local path or an S3-compatible bucket/prefix; restart to switch folders. MCP
tool arguments cannot change the source. A missing selected catalog or payload
is an error, with no fallback to another local or cloud folder.

Set cloud credentials in the local environment through the normal AWS
credential chain; never send them in an MCP call or put them in the source URI.
For Cloudflare R2:

```powershell
python server.py --services geometry,research --source s3://your-bucket/staging `
  --cloud-endpoint "https://your-account.r2.cloudflarestorage.com" `
  --cloud-region auto
```

For AWS S3, use the bucket's region and omit `--cloud-endpoint`:

```powershell
python server.py --services geometry,research --source s3://your-bucket/your-folder `
  --cloud-region us-west-2
```

Backblaze B2, Google Cloud Storage's XML interoperability API, Wasabi, and
other S3-compatible stores use the same `--source` shape with their own
`--cloud-endpoint`, signing region, and compatible access keys. Those providers
share the configuration shape; run a catalog and data call to verify each
endpoint before relying on it. The cloud
adapter uses S3 `GetObject`, `HeadObject`, and ranged Parquet reads where the
Geometry runtime supports them. Azure Blob and consumer file-sync services
need separate adapters; an ordinary HTTPS share link is not an S3 source.

`--cache-dir` optionally chooses disposable local space for cloud reads.
Research downloads its selected Parquet file there before querying it;
Geometry uses the existing artifact reader and its own bounded cache. The
cache never supplies a catalog or payload if the selected cloud object is
missing. For large Research sources, allow disk space for the selected file.
The source is read-only; the local program never publishes to the selected
folder. The legacy `--data-root` and `--cloud-bucket`/`--cloud-prefix` options
still work, but `--source` is the simpler selection.

For an import or release candidate, run representative Geometry calls against
the local candidate first. After uploading the selected catalog and payload to
staging, restart this local server with that explicit cloud prefix and repeat
the calls. Compare selected catalog/release pins and results. After promotion,
use the live hosted Geometry MCP for a short check of the published selection.
The hosted service reads published data only. Structural import and release
checks remain separate from these MCP calls. Research/data uses the same
selected folder; Network is a separate program and does not yet use this
downloadable MCP source selector.

If the selected local data root has no global `geometry/geometry_catalog.json`
but contains one or more extracted Geometry package `geometry.json` sidecars,
the server uses its offline package-scoped reader. It reads only sidecar-listed
Parquet members and supports catalog discovery, location lookup, point
resolution and geometry checks/reads. It never falls back to
the app or cloud. Detail provides exact polygons, Display provides simplified
polygons, and Lite provides reference identities only. Every member read is
verified against its sidecar size and SHA-256. Reads and exports remain scoped
to installed data; overlapping packages prefer exact over simplified geometry.

Extract data packages into separate directories under one local root, for
example `D:\DaedalMapData\Germany`, `D:\DaedalMapData\France`, and
`D:\DaedalMapData\Global`. Do not overlay their root-level sidecars. Pass the
parent directory to `server.py --data-root`; single-package roots also work.
Restart the server after installing/removing packages to refresh discovery.

Downloaded-package mode exposes the current 14-tool Geography MCP roster
and the same tool schemas as the hosted facade: `get_tool_help`, `get_catalog`,
`get_pack`, `get_geography`, `resolve_point`, `resolve_deep_point`,
`resolve_bbox`, `resolve_deep_bbox`, `get_loc_id_info`,
`identify_dataset_geography`, `identify_reference_system`, `convert_reference`,
`compare_geographies`, and `get_shape`. It returns typed unavailable results
when installed packages lack the needed data. Local package sidecars are
read through the package-scoped fallback when no full geometry catalog is
installed. Lite is reference-only; Display and Detail add polygons.
`get_shape` accepts exact `loc_id` values and includes GeoJSON only when
`include_polygon=true`. Use `tools/list` for the active contract.

Run only one server with `--services geometry` or `--services research`. The
default port is 7010 for Geometry and 7011 for Research, allowing both servers
to run simultaneously against the same data root. When only one service is
enabled, `/mcp` is also an alias for that endpoint.

Keep the default loopback host. Local mode intentionally has no bearer-token
requirement; binding it to a LAN/public interface would expose the tools.

Point an MCP client that supports Streamable HTTP at the desired endpoint. For
clients that use JSON configuration, the conceptual entries are:

```json
{
  "mcpServers": {
    "daedalmap-geometry": {"type": "http", "url": "http://127.0.0.1:7010/mcp/geometry"},
    "daedalmap-research": {"type": "http", "url": "http://127.0.0.1:7010/mcp/research"}
  }
}
```

The exact configuration filename and HTTP type label depend on the MCP client.

## How to discover what to do

MCP servers do not have one universal command-line `--help` convention. An MCP
client discovers the server through `initialize`, `tools/list`, tool schemas and
descriptions, optional prompts, and documentation resources.

Both DaedalMap services are self-describing:

- send `GET` to the endpoint for a compact server summary and `howToStart` list;
- ask the client to list its tools and read their descriptions;
- on Research, call `how_research_mcp_works` for the family workflow;
- on Research, call `get_tool_help` with an exact tool name for its strict
  schema, example, refusal guidance, outputs, and likely next calls;
- on Geometry, call `get_tool_help` with `topic="geometry"` for the family
  workflow and use the discovery tools advertised by `tools/list` for current
  contracts.

### First Research call

After the server is running, point your MCP client at
`http://127.0.0.1:7010/mcp/research` and ask:

```text
How does the Research MCP work?
```

The client should call `how_research_mcp_works`. To verify the same behavior
without relying on a client UI:

```powershell
$body = @{
  jsonrpc = "2.0"
  id = 1
  method = "tools/call"
  params = @{
    name = "how_research_mcp_works"
    arguments = @{}
  }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
  -Uri "http://127.0.0.1:7010/mcp/research" `
  -Method Post `
  -ContentType "application/json" `
  -Headers @{"MCP-Protocol-Version" = "2025-06-18"} `
  -Body $body
```

Then use this sequence:

1. `where_is_research_data` - confirm which local catalog is active.
2. `resolve_research_corpus_intent` or `search_research_sources` - find relevant
   packs and sources without guessing ids.
3. `ask_research_sources` - bind explicit pack/source ids and the question.
4. `get_research_pack` - read exact metrics, filters, versions, and query rules.
5. `query_research_source_data` - retrieve deterministic evidence rows; the
   calling client model performs the reasoning.

Research is source-bound by design. Do not make claims outside the selected
source ids, returned versions, geographic scope, or evidence rows.

### First Geometry call

Point your MCP client at `http://127.0.0.1:7010/mcp/geometry` and ask:

```text
List my installed geometry packages and their capabilities.
```

The client should call `get_catalog` with `catalog="geometry"`. The same direct PowerShell smoke
test is:

```powershell
$body = @{
  jsonrpc = "2.0"
  id = 1
  method = "tools/call"
  params = @{
    name = "get_catalog"
    arguments = @{ catalog = "geometry" }
  }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
  -Uri "http://127.0.0.1:7010/mcp/geometry" `
  -Method Post `
  -ContentType "application/json" `
  -Headers @{"MCP-Protocol-Version" = "2025-06-18"} `
  -Body $body
```

Then use this sequence:

1. `get_catalog` with `catalog="geometry"` to inspect installed families and scopes.
2. `get_tool_help` with the exact tool you intend to call.
3. `resolve_point` for coordinates when polygon data is installed, or
   `identify_reference_system` for an identifier in a declared country scope.
4. `get_loc_id_info` to inspect the resolved identity and direct relationships.
5. `get_shape` for an exact ID; request `include_polygon=true` only when coordinates are needed.
