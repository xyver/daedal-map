# DaedalMap Free Geography store submission

Status: OpenAI portal draft uploaded on 2026-11-11. Domain challenge endpoint is live; portal Connect and Scan Tools still need to be completed. No review submission or approval is claimed here.

The production MCP endpoint is `https://app.daedalmap.com/mcp/free`. It is intended to provide bounded, anonymous geographic reference and published free-data workflows. The OpenAI package source is `.codex-plugin/plugin.json`, `.mcp.json`, and `assets/icon.svg` in this directory. The Claude connector uses the same endpoint directly.

## Listing text

- Name: DaedalMap Geography
- One-line description: Geographic IDs and data
- Description: Identify geographic references in tabular data, convert place names and external identifiers to DaedalMap `loc_id`, resolve coordinates, compare places, and query published country-level data. Coverage varies by location and published source.
- Documentation: https://www.daedalmap.com/agents
- Support: https://www.daedalmap.com/support
- Privacy: https://www.daedalmap.com/privacy
- Terms: https://www.daedalmap.com/terms
- Publisher website: https://www.daedalmap.com
- Authentication: none for this free endpoint
- Data handling: first-party DaedalMap MCP service; verify portal answers against the current privacy policy and live telemetry before submission.

Check the text against the live tool list and website before copying it to either portal. Select the closest truthful portal category; do not claim an MCP App UI or paid-account access for this candidate.

## Reviewer flows

The OpenAI package has five positive and three negative cases with observable expected behavior. Use those same prompts in fresh ChatGPT and Claude conversations. Also try an indirect geography request and a follow-up that reuses a returned `loc_id`. Record tool selection, arguments, response, elapsed time, and any failure for each case. Check that an unrelated request does not invoke DaedalMap and that paid-pack and above-ceiling requests receive a clear denial or narrowing path without a charge.

The November 11 ChatGPT custom-connector checks passed the Canada WDI query in a fresh chat, Fairfax conversion and parent follow-up, unrelated-email nonactivation, paid-earthquake denial, and the 100-row single-call ceiling. The Brazil municipality-code probe correctly returned ambiguity across matching reference systems; its review expectation now permits that result. After the catalog update deployed, two separate fresh ChatGPT chats confirmed that `get_catalog(data)` returns `eligible` for `world_bank_wdi`, `un_wpp`, and `un_sdg`, and `blocked_paid_pack` for `world_factbook`. The first chat quoted the catalog's caveat that eligibility does not guarantee material clearance or an API-ready source. The second distinguished successful Canada 2020 row queries for WDI and WPP from an `unknown_source` failure for the UN SDG pack-level query; it did not count UN SDG as confirmed retrievable. This passes the catalog review behavior. The UN SDG resolver was subsequently fixed in public commit `2613de1`: the pre-fix hosted source query passed while the MCP pack query failed, and the settled post-fix hosted suite passed 3/3, including source groups `01` and `17`. A subsequent ChatGPT query using `pack_id: "un_sdg"` without `source_id` returned source `01`, three `ind_1_1_1` rows, and no error or warnings. This closes the tested UN SDG browser regression; the corrected 0.1.1 package has not yet been scanned in the portal.

The first OpenAI portal draft upload flagged pricing language in the listing description, an inconclusive automated privacy-policy assessment, and incomplete review information. Package version 0.1.1 removes access and pricing claims from the listing copy. The five positive and three negative test cases are present in the ZIP; the reviewer video URL was supplied through the portal rather than `review.demo_recording_url` in the ZIP. Check Review information → Review details for any other missing fields. The privacy finding requires checking the published policy against OpenAI's stated requirements; an inconclusive automated assessment is not evidence of a policy failure.

The subsequent portal draft blocked review with “Scan tools before submitting.” Its MCP card showed “Domain not verified,” no discovered tools, and a 404 at `https://app.daedalmap.com/.well-known/openai-apps-challenge`. Public commit `c60f2e9` added that exact plain-text challenge route. Live readback after deployment returned HTTP 200, `text/plain`, and the exact portal token; anonymous `/mcp/free` `tools/list` still returned 13 tools. The operator must now open MCPs → DaedalMap Geography → Connect, verify the domain, connect with No Auth, wait for Scan Tools, and inspect any new tool findings before review submission. The privacy-policy assessment is a separate finding that may proceed to manual review.

Claude's suggested working prompts:

1. Get Canada's 2020 GDP in current US dollars from World Bank WDI on DaedalMap.
2. What is the DaedalMap loc_id for Fairfax County, Virginia?
3. In the USA administrative family, which area contains latitude 38.8977, longitude -77.0365?

The point tool requires an explicit scope and family. The November 11 hosted contract passed with `scope_id=USA` and `family=administrative`; the prompt names both. Treat these as test candidates until the live client results and selected release are recorded. Do not supply a fabricated expected numeric GDP value or a deeper administrative level than the service returns.

## Portal steps

- OpenAI: upload the ZIP as a draft with MCP, verify the publisher and endpoint domain, inspect the automated package and live tool scans, attach the final demo recording, submit for review, then publish only after approval. Review cases are in `plugin.json`; reviewer credentials, if ever required, belong in the secure portal rather than this repository.
- Claude: create an MCP connector draft with this HTTPS endpoint and no authentication, check every scanned tool title and applicable annotations, enter listing and company details, test every tool in Claude, and review all policy declarations before Submit. Claude may list a passing submission automatically as a Community connector, so Submit is a public launch step.

Official procedures: https://developers.openai.com/plugins/deploy/submission and https://claude.com/docs/connectors/building/submission .
