# Research MCP

Research MCP is a standalone headless program that reads the launcher's shared
`DATA_ROOT` and its catalog. Its local facade, module, setup resources, and
requirements now live here. The shared local server and post-install smoke
check live in [`tools/mcp-runtime/`](../mcp-runtime/README.md); shared reader
modules live in the public runtime. The exporter assembles only the selected
service and its dependency closure into a standalone source ZIP with `LICENSE`.
The private hosted provider remains in the operator worktree and is not the
downloadable source.

From a public source checkout, install `requirements.txt` in a Python 3.11+
environment, then run from the repository root:

```powershell
python tools/mcp-runtime/server.py --services research --data-root "C:\path\to\DaedalMapData"
```

Connect to `http://127.0.0.1:7011/mcp/research`. The
[single-tool guide](../../docs/WORK_ON_ONE_TOOL.md#research-and-geometry-mcp)
explains the source export and remaining release gate.
