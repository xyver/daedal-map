# Local Research MCP setup

Install the selected Research MCP program with the DaedalMap launcher, then
choose a shared data folder and install a research data pack. The launcher
creates `catalog.json` even before a pack is installed. Research can start
with an empty catalog, but evidence queries need installed sources.

For development from this public repository, install the requirements in
`tools/research-mcp/requirements.txt`, then run from the repository root:

```powershell
python tools/mcp-runtime/server.py --services research --data-root "C:\path\to\DaedalMapData"
```

Connect a Streamable HTTP MCP client to
`http://127.0.0.1:7011/mcp/research`. Keep the server bound to loopback. The
local service uses the selected files on your machine and does not require a
hosted account token. The Research MCP never downloads packs by itself; use
the launcher to add or update content in the shared folder.
