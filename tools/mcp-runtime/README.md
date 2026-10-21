# DaedalMap Local MCP Runtime

This headless package runs the Geometry MCP, Research MCP, or both against a
DaedalMap data folder. It does not install the map UI, launcher, or every data
pack.

In the public source checkout, this folder is the shared local server and
smoke check for Research MCP (`tools/research-mcp/`) and Geometry MCP
(`tools/geometry-mcp/`). Run `server.py` from the repository
root with `--services research` or `--services geometry` and the selected
`--data-root`. The commands below apply to an extracted service ZIP, whose
files are placed at the ZIP root by the exporter.
The extracted ZIP includes `SOURCE.json` with its public source revision and
clean-worktree state. Local Research uses the selected data folder and does
not forward queries to the hosted API.

## Requirements

- Python 3.11 or newer for this source-distribution release
- a DaedalMap data folder containing `catalog.json` for Research
- extracted geometry data packages (each retains its `geometry.json`) for
  Geometry, or a full developer data root with `geometry/geometry_catalog.json`

After extracting the release:

```powershell
py -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python smoke_test.py
.\.venv\Scripts\python server.py --data-root "D:\DaedalMapData"
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

If the selected local data root has no global `geometry/geometry_catalog.json`
but contains one or more extracted Geometry package `geometry.json` sidecars,
the server uses its offline package-scoped reader. It reads only sidecar-listed
Parquet members and supports catalog discovery, location lookup, point
resolution, geometry checks/reads, and GeoJSON export. It never falls back to
the app or cloud. Detail provides exact polygons, Display provides simplified
polygons, and Lite provides reference identities only. Every member read is
verified against its sidecar size and SHA-256. Reads and exports remain scoped
to installed data; overlapping packages prefer exact over simplified geometry.

Extract data packages into separate directories under one local root, for
example `D:\DaedalMapData\Germany`, `D:\DaedalMapData\France`, and
`D:\DaedalMapData\Global`. Do not overlay their root-level sidecars. Pass the
parent directory to `server.py --data-root`; single-package roots also work.
Restart the server after installing/removing packages to refresh discovery.

In downloaded-package mode, use `read_geometry_catalog`, `get_tool_help`,
`loc_id_info`, `get_geometry`, `resolve_point`, and
`create_geometry_export`. Export takes explicit `loc_ids` or `country` with an
optional `admin_level` and returns a GeoJSON FeatureCollection to save locally.
It refuses missing polygons or selections above `max_features` (up to 10,000)
instead of silently returning a partial export. Source licences travel with the
result. Full-data-root mode retains the broader production tool inventory;
always use `tools/list` to discover the operations available in your mode.

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

The client should call `read_geometry_catalog`. The same direct PowerShell smoke
test is:

```powershell
$body = @{
  jsonrpc = "2.0"
  id = 1
  method = "tools/call"
  params = @{
    name = "read_geometry_catalog"
    arguments = @{ view = "capabilities" }
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

1. `read_geometry_catalog` with `view="capabilities"` and a country when known.
2. `get_tool_help` with the exact tool you intend to call.
3. `resolve_point` for coordinates when polygon data is installed. The broader
   `identify_reference_system` / `resolve_reference` tools require full-data-root
   mode; they are not advertised in downloaded-package mode.
4. `loc_id_info` to inspect the resolved identity and relationships.
5. `get_geometry` with `include_polygon=false` to check availability; repeat with `include_polygon=true` only when coordinates are needed.
