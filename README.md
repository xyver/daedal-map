# DaedalMap

**Bring your data. Connect its geography.**

DaedalMap is geographic interoperability infrastructure: a `loc_id` identity
model, versioned boundary geometry, published crosswalks between geography
systems, and maintained data packs. Resolve coordinates and reference codes to
administrative matches, pull compatible shapes and data, and follow crosswalks
into other systems such as census tracts, postal areas, watersheds, and
electoral districts.

Use it through a remote MCP server, an HTTP API, a map app, downloadable files,
or this repository, which is the open runtime behind all of them.

[Website](https://www.daedalmap.com) |
[App](https://app.daedalmap.com) |
[Agent docs](https://www.daedalmap.com/docs/for-agents) |
[Geometry](https://www.daedalmap.com/geometry) |
[Data packs](https://www.daedalmap.com/packs) |
[Downloads](https://www.daedalmap.com/downloadable/) |
[Convert a file](https://www.daedalmap.com/convert) |
[llms.txt](https://www.daedalmap.com/llms.txt)

## Connect an agent

The hosted MCP server needs no install or account for discovery:

```text
https://app.daedalmap.com/mcp
```

Claude Code:

```bash
claude mcp add --transport http daedalmap https://app.daedalmap.com/mcp
```

Codex:

```bash
codex mcp add daedalmap --url https://app.daedalmap.com/mcp
```

Any client that supports streamable HTTP MCP can use the same URL. Start with
`get_tool_help` with `topic='geometry'` for geography jobs, or `get_catalog`, then `get_pack`,
then `get_data` for data. Setup for other clients is in the
[agent docs](https://www.daedalmap.com/docs/for-agents).

Discovery and small geography lookups are free. Larger batches, exports, and
some data packs are metered, and paid calls return an x402 payment challenge
before any charge. `get_catalog` reports the access lane for each pack.

## Tools

| Job | Tools |
|---|---|
| Learn the model | `get_tool_help` with a topic or exact tool name |
| Find what exists | `read_geometry_catalog`, `list_reference_systems`, `get_catalog`, `get_pack` |
| Coordinates to places | `resolve_point`, `resolve_points`, `resolve_deep_point`, `resolve_deep_points` |
| Identify a column of codes | `identify_dataset_geography`, `identify_reference_system` |
| Translate codes between systems | `resolve_reference`, `convert_reference` |
| Inspect and relate places | `loc_id_info`, `compare_geographies`, `resolve_loc_id_scope` |
| Shapes and exports | `check_geometry`, `get_geometry`, `estimate_geometry_package`, `create_geometry_export` |
| Convert your own rows | `estimate_conversion_job`, `create_conversion_job`, `get_job_status` |
| Query maintained data | `get_data` with a `pack_id`, exact metrics, and structured filters |
| Live feeds | `get_live_earthquake_events`, `get_live_volcano_events` |
| Cross-hazard links | `get_disaster_links_for_event`, `get_disaster_link_chain`, `search_disaster_links` |

Each tool takes strict JSON arguments. The calling model turns a user's
question into those arguments, and the server returns typed errors with
recovery guidance when a call is malformed.

## The loc_id model

`loc_id` is DaedalMap's geographic identity model. The administrative spine is
the main hierarchy and default join path:

```text
USA                      country
USA-CA                   state
USA-CA-037               county
USA-CA-037-221710        census tract
```

Other geography families, such as postal areas, watersheds, tribal areas, and
marine regions, keep their own `loc_id` identities. Published crosswalks connect
them to the spine and to each other, and each crosswalk row carries its
relationship type, overlap weight, source, and vintage. A direct join works when
both datasets declare the same identity; otherwise the connection runs through a
crosswalk.

Schema details are in [docs/DATA_SCHEMAS.md](docs/DATA_SCHEMAS.md).

## Coverage

**Geometry: global baseline plus 8+ deeper countries.** The same geography
tools work worldwide down to Admin 2. Country releases add deeper
administrative tiers and reference families for Australia, Brazil, Canada,
France, Germany, Mexico, the United Kingdom, the United States, and more as
releases publish.

**Data: 20+ maintained data packs** covering natural hazards (earthquakes,
tsunamis, volcanoes, hurricanes, tornadoes, wildfires, floods, weather alerts),
hazard risk and environmental burden, economic and business indicators,
currency rates, population, and climate.

The catalogs are the authority for what is available now:

- Geometry: [app.daedalmap.com/api/v1/geometry/catalog](https://app.daedalmap.com/api/v1/geometry/catalog)
- Data packs: [app.daedalmap.com/api/v1/catalog](https://app.daedalmap.com/api/v1/catalog)

## Downloads

The [Downloads page](https://www.daedalmap.com/downloadable/) has three kinds
of file:

- **Program** - the map app or local MCP runtime, to run DaedalMap on your machine.
- **Data pack** - maintained records for a subject, with source and release evidence.
- **Geometry package** - boundaries and reference geography for a country or family.

## Run it yourself

This repository is the open runtime: the FastAPI server, MCP and HTTP API, and
map frontend. Use it to self-host, run against your own data, or extend the
engine.

```powershell
cd county-map
pip install -r requirements.txt
Copy-Item .env.example .env
```

Set a data folder in `.env`:

```env
INSTALL_MODE=local
RUNTIME_MODE=local
DATA_ROOT=C:/path/to/your/data
```

Leave `DATA_ROOT` blank to use the default app-data folder. No cloud storage,
database, or account is needed. The repository does not ship a data folder;
[docs/DATA_INSTALLATION.md](docs/DATA_INSTALLATION.md) covers installing packs.

```powershell
python app.py
```

The app opens at `http://localhost:7000`, and the same MCP server is at
`http://localhost:7000/mcp`. Discovery routes:

- `GET /api/v1/guide`
- `GET /api/v1/catalog`
- `GET /api/v1/packs/{pack_id}`
- `POST /api/v1/query/dataset`

Model API keys are optional. `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` powers the
built-in chat panel only; MCP clients bring their own model. A self-hosted
instance returns `commercial_access_unavailable` on paid lanes unless you
configure a commercial verifier.

### Runtime modes

| `INSTALL_MODE` | `RUNTIME_MODE` | Use |
|---|---|---|
| `local` | `local` | Your machine, your data folder |
| `local` | `cloud` | Your machine, Parquet read from object storage you configure |
| `cloud` | `cloud` | A server deployment reading object storage |

In `cloud` data mode the runtime caches small metadata files locally and queries
Parquet in object storage through DuckDB `httpfs`. Details are in
[docs/LOCAL_AND_HOSTED.md](docs/LOCAL_AND_HOSTED.md).

## Documentation

- [docs/CONTEXT.md](docs/CONTEXT.md) - technical router for the codebase
- [docs/API_AND_MCP.md](docs/API_AND_MCP.md) - HTTP routes and MCP surfaces
- [docs/DATA_SCHEMAS.md](docs/DATA_SCHEMAS.md) - schemas and `loc_id` conventions
- [docs/DATA_PREPARATION.md](docs/DATA_PREPARATION.md) - convert and validate your own data
- [docs/DATA_INSTALLATION.md](docs/DATA_INSTALLATION.md) - install data locally
- [docs/PACK_AUTHORING.md](docs/PACK_AUTHORING.md) - build research packs and corpora
- [docs/RESEARCH_MCP.md](docs/RESEARCH_MCP.md) - research with the hosted MCP
- [docs/RUNTIME_MODES.md](docs/RUNTIME_MODES.md) - Explore, Research, Ops, and Tutorial
- [docs/SECURITY_AND_SELF_HOSTING.md](docs/SECURITY_AND_SELF_HOSTING.md) - self-hosting securely

## Contact

Questions, feedback, and self-hosting issues: support@daedalmap.com

## License

MIT. Data packs and geometry carry their own source licenses, listed in each
catalog entry.
