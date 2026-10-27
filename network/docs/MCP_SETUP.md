# Local MCP setup

The Road Network Editor MCP server is a local `stdio` server. It does not
upload the Lake graph, scenario files, or analysis results anywhere. Its only
read/write data boundary is this repository's `data/` folder, and its analysis
input/output boundary is `data/analysis/`.

It gives an LLM small, structured tools over the local scenario scripts:

- inspect origin or candidate CSV/Parquet files and their road-snap quality
- export the active or named UI scenario to portable JSON
- import a CSV/Parquet list as custom shelters in the active UI scenario
- run access, compare results, and recommend candidate shelters

## Prerequisites

1. Install Docker Desktop and make sure it is running.
2. Prepare the Lake County baseline and confirm the editor opens locally. See
   [DATA_SETUP.md](DATA_SETUP.md).
3. Create `data/analysis/` if it is not already present. This is where you put
   input CSVs and where the MCP writes Parquet/JSON results.

The prepared Docker Compose file is enough. No California-wide source PBF or
County Map data folder is needed after the baseline is prepared.

## Build the local MCP image

From the repository root:

```powershell
docker compose -f docker-compose.prepared.yml build mcp
```

The first build installs the Python MCP SDK into the same local image that
runs the editor. Later starts reuse that image unless its code or dependencies
change.

## Connect an MCP client

Configure your desktop LLM client to start this command as a `stdio` MCP
server. Replace the path with your own clone location.

```json
{
  "mcpServers": {
    "lake-road-network": {
      "command": "docker",
      "args": [
        "compose",
        "-f",
        "C:\\path\\to\\daedal-map\\network\\docker-compose.prepared.yml",
        "run",
        "--rm",
        "-i",
        "mcp"
      ]
    }
  }
}
```

The MCP client owns the process: it starts the command when needed and stops
it when the conversation ends. Do not run it as a web server or leave a
terminal window open for it. The editor itself remains a separate local web
service:

```powershell
docker compose -f docker-compose.prepared.yml up -d editor
```

## Input CSVs

Put files under `data/analysis/`. CSV is the easiest format; Parquet is also
accepted for larger tables.

Origins (homes/demand locations):

```csv
origin_id,origin_name,longitude,latitude,weight
bg-001,Example block group,-122.75,39.05,1250
```

Candidate shelters (possible sites):

```csv
candidate_id,name,longitude,latitude
site-001,Example community center,-122.76,39.06
```

`id`, `lon`, and `lat` are accepted aliases. The server automatically snaps
locations to the nearest graph node at runtime; do not pre-snap coordinates.
Use `inspect_locations` first. It reports points farther than the default
3,000-meter tolerance so an LLM cannot silently optimize a location outside
the prepared road network. The tolerance is deliberately visible and can be
lowered for stricter work.

For the initial Lake study, use Lake County locations as origins. Shelters are
destinations: the baseline/scenario shelter list may include sites in the small
prepared cross-border buffer, and routes may cross that border. The road graph
is the authority on what is reachable.

To make a CSV list into editable custom shelters, ask the MCP to run
`import_custom_shelters`. It validates the same snap rule and then adds the
locations to the active UI scenario. Refresh the browser to see them, then use
**Save scenario** if you want a named scenario save.

## Typical local conversation

After connecting the MCP, a useful request is:

> Inspect `origins.csv` and `candidates.csv`. If their snap reports are clean,
> export the saved `lake-fire-closure-01` scenario, calculate 30-minute
> access, and recommend ten candidate shelters.

The MCP returns concise JSON summaries and writes detailed artifacts under
`data/analysis/`. Those artifacts are intentionally ignored by Git because
they may contain local planning inputs and can be regenerated.

## Current limitations

- Lake County plus the prepared cross-border buffer only.
- Snapping is to the nearest graph node. It is automatic and checked, but a
  future upgrade can snap onto an edge for more exact long-rural-road behavior.
- The first optimizer is transparent greedy weighted coverage; capacity,
  hazards, equity constraints, and site suitability data come later.
