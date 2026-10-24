# Geometry MCP

Geometry MCP is a standalone headless program for local geometry packages in
the launcher's shared `DATA_ROOT`. Its downloaded-package backend and selected
requirements now live here. Full-data-root Geometry tools use the public
`mapmover.routes.mcp` runtime. The shared local server and post-install smoke
check live in [`tools/mcp-runtime/`](../mcp-runtime/README.md). The exporter
assembles only the selected service and its dependency closure into a
standalone source ZIP with `LICENSE`. Private hosted provider code remains in
the operator worktree.

From a public source checkout, install `requirements.txt` in a Python 3.11+
environment, then run from the repository root:

```powershell
python tools/mcp-runtime/server.py --services geometry --data-root "C:\path\to\DaedalMapData"
```

Connect to `http://127.0.0.1:7010/mcp/geometry`. The
[single-tool guide](../../docs/WORK_ON_ONE_TOOL.md#research-and-geometry-mcp)
explains the source export and remaining release gate.

Call `get_catalog({"catalog":"geometry"})` after installing or changing
packages. Country administrative geometry, Marine families, and external
reference bridges are discovered from separate package sidecars in the shared
data root. A Bridge is listed under `external_reference_bridges`; it is a
reference system, not a geometry family. `convert_reference` returns a scalar
`resolved_loc_id` from an Overture GERS key only when the installed Bridge has
one admitted `equivalent_identity` edge. Overlap and containment edges remain
typed relationships in `matches`.

Before publishing a runtime update, check Lite, Display, and Detail ZIPs for
every published country against the same sidecar and Parquet layout contract.
Country-specific bank counts and depth may differ. Rerun installed-package
checks for country, Marine, and Bridge releases whenever that layout changes.
