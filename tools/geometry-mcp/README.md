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
