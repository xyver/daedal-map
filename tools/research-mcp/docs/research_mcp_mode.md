# Local Research MCP mode

The Research MCP reads one selected `DATA_ROOT` and the `catalog.json` in that
folder. The same root can be used by Mapping and Geometry MCP. Its data tools
operate only over installed sources. `where_is_research_data` reports the
active root; `tools/list` and `get_tool_help` give the exact available tool
contracts.

Research follows an explicit source boundary: select packs, bind source IDs,
read the pack contract, then query one source at a time. A result's provenance
and coverage come from the installed pack metadata. Do not infer coverage
outside the returned sources or treat client model knowledge as pack evidence.

The local MCP is intended for loopback use. Program updates and pack downloads
are separate; the launcher manages the shared data folder and content
catalog.
