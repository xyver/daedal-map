# Research MCP bootstrap

Call `where_is_research_data` to identify the installed data source, then
`get_catalog` with `{"catalog":"data"}` to see available packs and local
source IDs. Call `how_research_mcp_works` for the workflow, then find
relevant packs with `search_research_sources` or
`resolve_research_corpus_intent`. Use
`ask_research_sources` to bind the selected pack and source IDs. Read their
contracts with `get_research_pack`, then call `query_research_source_data`
using the returned IDs, metrics, filters, and time shape. Resolve human place
names with `resolve_research_geography` before using region filters.

The client model interprets the question and presents the answer. The local
server returns source-bound data and explicit errors. An empty catalog has no
evidence sources until a pack is installed.

The catalog is only a directory. `ask_research_sources` establishes the active
source boundary for a question; `query_research_source_data` must receive the
full returned `source_ids` list and choose one `query.source_id` inside it.
Call `get_tool_help` with an exact tool name whenever a schema or refusal is
unclear. The published 1.0.2 download predates `get_catalog`; this sequence
applies to current source and to the next packaged local release.
