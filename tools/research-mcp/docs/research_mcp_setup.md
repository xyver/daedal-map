# Local Research MCP setup

Install the selected Research MCP program with the DaedalMap launcher, then
choose a shared data folder and install a research data pack. The launcher
creates `catalog.json` even before a pack is installed. Research can start
with an empty catalog, but evidence queries need installed sources.

For development from this public repository, install the requirements in
`tools/research-mcp/requirements.txt`, then run from the repository root:

```powershell
python tools/mcp-runtime/server.py --services research --source "C:\path\to\DaedalMapData"
```

Connect a Streamable HTTP MCP client to
`http://127.0.0.1:7011/mcp/research`. Keep the server bound to loopback. The
local service uses the selected files on your machine and does not require a
hosted account token. For an S3-compatible cloud folder, use
`--source s3://bucket/folder` with that provider's endpoint and signing region;
the selected folder must contain `catalog.json`, metadata, and data together.
The Research MCP caches a selected cloud Parquet for execution but does not
install packs into the source folder. Use the launcher to add or update local
content.
