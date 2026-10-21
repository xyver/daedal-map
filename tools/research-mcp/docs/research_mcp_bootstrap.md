# Research MCP bootstrap

Call `how_research_mcp_works` for the workflow, then discover available packs
with `search_research_sources` or `resolve_research_corpus_intent`. Use
`ask_research_sources` to bind the selected pack and source IDs. Read their
contracts with `get_research_pack`, then call `query_research_source_data`
using the returned IDs, metrics, filters, and time shape. Resolve human place
names with `resolve_research_geography` before using region filters.

The client model interprets the question and presents the answer. The local
server returns source-bound data and explicit errors. An empty catalog has no
evidence sources until a pack is installed.
