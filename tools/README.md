# Local tool source

This directory is the source entry point for DaedalMap's selectable local
tools. The launcher and shared runtime are infrastructure; installed data and
geometry packs live in the user's selected data folder. Each tool has its own
download and source export, even when it imports shared runtime code.

| Tool | Source entry | Current implementation | Relationship |
|---|---|---|---|
| [Mapping](mapping/README.md) | `tools/mapping/` | Root `app.py`, `mapmover/`, frontend assets | Base interactive program |
| [Network](network/README.md) | `tools/network/` | `network/` | Standalone editor; Mapping can later display validated Network layers |
| [Research MCP](research-mcp/README.md) | `tools/research-mcp/` | Local facade and selected requirements here; shared server in `tools/mcp-runtime/` | Headless standalone program |
| [Geometry MCP](geometry-mcp/README.md) | `tools/geometry-mcp/` | Downloaded-package backend and selected requirements here; shared server in `tools/mcp-runtime/` | Headless standalone program |
| [AI](ai/README.md) | `tools/ai/` | Chat/provider code currently mixed into Mapping | Optional Mapping expansion |

The entry folders establish ownership. Mapping, Network, and AI still need
contained source moves. The MCP exporter assembles each MCP's public tool
folder and its exact shared-runtime closure into a separate ZIP; archiving a
single folder without that closure would be incomplete.

Every tool follows the same delivery contract:

1. A small source checkout or source export contains the editable tool and
   exactly the shared modules it needs. It includes `LICENSE`, an upstream URL
   and commit, dependency profile, local setup guide, and an independent smoke
   command.
2. A versioned program artifact contains only the tool selected by the user.
   The launcher resolves compatible dependencies in a shared environment and
   passes the same `DATA_ROOT` to all installed tools.
3. The source export and program artifact identify the same public commit and
   have separate verified manifests. AI additionally records its compatible
   Mapping interface/version range.
4. A failed install or update leaves the previous working program and shared
   data intact. Data packs and projects never enter the source export.

For usable commands and the present gaps, see
[Work on one local tool](../docs/WORK_ON_ONE_TOOL.md). GitHub forks of this
repository include all source history; a person starting a separate project
can instead initialize a new repository from one complete source export.
