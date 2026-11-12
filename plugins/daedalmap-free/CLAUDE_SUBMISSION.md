# Claude connector submission handoff

Prepared 2026-11-11 for the DaedalMap-controlled paid Claude account. This is a remote **MCP connector** submission, not a plugin bundle or MCP App. The connector has no interactive UI, so Claude's connector instructions do not ask for a demo video or carousel screenshots. Submission and real-client testing remain to be done from the new Claude account.

## Portal entry

Open https://claude.ai/directory/manage in the account that should own the listing, choose **Submit new → MCP connector**, and use:

| Field | Entry |
| --- | --- |
| Server URL | `https://app.daedalmap.com/mcp/free` |
| URL mode | One fixed URL |
| Authentication | No sign-in / no authentication |
| Name | DaedalMap Geography |
| One-liner | Geographic IDs and published data |
| Description | Identify geographic references in tabular data, convert place names and external identifiers to DaedalMap `loc_id`, resolve coordinates, compare places, and query published country-level data. Coverage varies by location and published source. |
| Documentation | https://www.daedalmap.com/agents |
| Privacy policy | https://www.daedalmap.com/privacy |
| Support | https://www.daedalmap.com/support and `contact@daedalmap.com` |
| Company | DaedalMap |
| Company website | https://www.daedalmap.com |
| Icon | `assets/icon.svg` in this directory; follow the portal's accepted format and size if it requires another file |
| Slug | `daedalmap-geography` if available; inspect before saving because the published slug is permanent |
| Categories | Choose the closest geography/data/developer categories actually offered by the portal; do not invent a category |
| User setup | No DaedalMap account or credential is needed for this public endpoint. Connect the URL and enable the connector in a Claude chat. |
| Access | Read-only geographic reference and published data queries; no writes or purchases through this endpoint |

For **Test & launch**, provide the endpoint, no-auth access steps, and the test prompts below. The portal's generic test-account request does not imply credentials exist for this anonymous endpoint. Enter “No account required; the endpoint is publicly accessible” where it asks for reviewer access instructions. Do not create or paste a private account merely to satisfy generic wording.

For **Data handling** and the seven **Compliance** acknowledgments, answer from the actual deployed behavior and current privacy policy. The service records MCP usage events for reliability and debugging. Do not claim it stores no telemetry or that it receives a user's full Claude conversation. Confirm any precise collection/retention claim before checking the portal box. The server is DaedalMap's own hosted MCP/API surface over published, source-attributed data.

## Connection and tool check

First add the endpoint as a custom connector in **Customize → Connectors**. In each test chat, enable it from **+ → Connectors**. Confirm the connector says Connected and the portal discovers these 13 tools, each with a title and read-only annotation:

`get_tool_help`, `get_catalog`, `get_pack`, `get_geography`, `resolve_point`, `resolve_bbox`, `resolve_deep_bbox`, `get_loc_id_info`, `identify_dataset_geography`, `identify_reference_system`, `convert_reference`, `compare_geographies`, `get_data`.

The existing hosted 24-case USA 1.3.5 suite covers all 13 tool names. It is backend evidence, not proof of Claude client behavior. Claude's checklist asks the submitter to exercise every tool. The direct prompts below can be run in one or a few chats; they do not each need a fresh chat. Record tool name, arguments, success/error, and whether Claude's answer matches the result. Do not mark a tool tested because Claude answered without calling it.

The first Claude portal Tools scan found all 13 tools and marked them read-only and idempotent, but showed 13 suggestions: each tool lacked `annotations.title` despite having a top-level `title`. A public runtime change now copies each existing title into `annotations.title`. After that runtime is live, reconnect the connector and confirm the suggestions disappear before continuing.

| Tool | Claude prompt | Expected check |
| --- | --- | --- |
| `get_tool_help` | “Call DaedalMap `get_tool_help` for `get_data`. Summarize its example and row limit.” | Help returns without error; limit is 100 per free call. |
| `get_catalog` | “Call DaedalMap `get_catalog` for the data catalog. Which of `world_bank_wdi`, `un_wpp`, `un_sdg`, and `world_factbook` are eligible for free `get_data`?” | Reports policy values from the live result; eligibility is not a row guarantee. |
| `get_pack` | “Call DaedalMap `get_pack` for `world_bank_wdi`. Show its published source and metric metadata.” | Pack metadata returned. |
| `get_geography` | “Call DaedalMap `get_geography` for USA, year 2019, then 2020. Report each `year_selection.status`, selected year, and `edition_years` exactly.” | Both years select exactly in the published 1.3.5 surface; list includes 2019 and 2020. |
| `resolve_point` | “In DaedalMap's USA administrative family, resolve latitude 38.8977, longitude -77.0365. Show the returned hierarchy and deepest resolved `loc_id`.” | An administrative hierarchy is returned; report the deepest level actually resolved. |
| `resolve_bbox` | “Call DaedalMap `resolve_bbox` with corners (60.95, -112.1), (60.95, -112.05), (61.02, -112.1), (61.02, -112.05). Show the shallow geography result.” | Bounded Canadian Northwest Territories result; avoid asserting a deeper level. |
| `resolve_deep_bbox` | “Call DaedalMap `resolve_deep_bbox` with those four corners and `shallow_loc_id: CAN-NT`. Show the deeper result.” | Returns a scoped deeper result or an explicit coverage limit. |
| `get_loc_id_info` | “Call DaedalMap `get_loc_id_info` for `USA-CA-CD119-0623`. Show its family and a direct connection to Los Angeles County.” | District resolves as `electoral_district`; typed `area_overlap` edge to `USA-CA-037`. |
| `identify_dataset_geography` | “My Brazil CSV has a `municipality_code` column with 1200013 and 1200054, plus a `population` column. Use DaedalMap to identify the geography columns, with Brazil as country scope.” | Identifies municipality codes; reports vintage/system ambiguity rather than guessing. |
| `identify_reference_system` | “Call DaedalMap `identify_reference_system` for 06073000100 and 06073000201, expecting US Census 2020 tract GEOIDs with USA scope. Show the returned match.” | A matching system or clear ambiguity; no fabricated conversion. |
| `convert_reference` | “Use DaedalMap to convert Fairfax County, Virginia to a `loc_id`, with USA country scope and admin level 2. Show whether it is canonical.” | `USA-VA-059`, canonical match. |
| `compare_geographies` | “Use DaedalMap to compare `USA-CA-CD119-0623` with `USA-CA-037`. Show the spatial relation and graph evidence.” | `overlaps` with `area_overlap` evidence; do not claim polygon loading or dated overlap. |
| `get_data` | “Use DaedalMap's `world_bank_wdi` pack to get Canada's 2020 `gdp_current_usd` row, limit 3. Report the source, metric, value, and row count.” | One published Canada GDP row. |

## Fresh-chat routing checks

Run each numbered prompt in its **own new Claude conversation** with the connector enabled. These probe tool choice without previous conversation context. For the Fairfax follow-up, continue in that same chat.

1. “Which free country-level data packs can I actually retrieve through DaedalMap? Distinguish catalog eligibility from packs whose rows you successfully retrieve, and don't describe an untested pack as confirmed working.” Expected: use catalog and real row calls; do not infer all eligible packs work.
2. “I have a spreadsheet with a place called Fairfax County, Virginia. What geographic identifier should I use to join it to DaedalMap data?” Expected: `convert_reference` and `USA-VA-059`. Follow up: “Using that identifier, tell me its parent geography.” Expected: `get_loc_id_info` and `USA-VA`.
3. “What was Canada's GDP in current US dollars in 2020, according to DaedalMap's World Bank WDI data?” Expected: a real `get_data` call, not a recalled or web value.
4. “Draft a friendly email asking my colleague whether tomorrow's meeting can start 30 minutes later.” Expected: no DaedalMap tool call.
5. “Retrieve paid earthquake event rows using the free DaedalMap connection.” Expected: denied with `free_pack_required`; no rows.
6. “Get 1,000 World Bank WDI rows in a single call through the free DaedalMap connection.” Expected: reject or explain the per-call maximum of 100, without claiming 1,000 rows were returned.

The first prompt may require several calls; accept a smaller confirmed set when Claude truthfully labels untested packs. `un_sdg` pack-level `get_data` with `ind_1_1_1` was separately confirmed in ChatGPT after the source-routing fix, but verify it afresh in Claude if Claude claims it works.

## Review and launch

- Check synced tool names, descriptions, titles, and read-only annotations in the Claude portal; fix any scan findings before submission.
- Use reviewer instructions that permit direct calls without authentication. Do not attach the OpenAI video; Claude's current connector submission guide does not request one for a tool-only connector.
- Save the Claude test results and final listing copy. Submit only from the DaedalMap-controlled account that should own the directory listing. The connector guide says a passing submission is listed as Community by default after automatic scanning, so treat Submit as a potential public launch.

Official guidance: https://claude.com/docs/connectors/building/submission , https://claude.com/docs/connectors/building/testing , https://claude.com/docs/directory/publish .
