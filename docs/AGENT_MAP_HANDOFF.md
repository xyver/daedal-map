# External agent to local map handoff

This is the first collaborator workflow for an agent running beside a local
DaedalMap server. It uses the app's existing Explore share-state route. The
agent chooses an installed source and gives the user a link to open in the
local app; opening the link runs the same confirmed-order execution and map
display path used by the app's chat panel. No model key is needed in the app.

## Prepare the local runtime

Run the public source checkout or launcher with `INSTALL_MODE=local`,
`RUNTIME_MODE=local`, and the intended `DATA_ROOT`. Install the metric pack and
compatible display geometry in that data root. The agent must inspect the
**local** catalog and select a concrete `source_id` and its `pack_id`. A result
from the hosted MCP catalog alone does not prove the source is installed here.

Research/Geometry MCP can help the agent find data and geography. Keep the
selected source, metric, filters, geography, and time explicit in the handoff;
do not substitute a similarly named hosted source. A source with no metric or
filters uses its authored default load. For a selected metric, put its concrete
key in `metric`. Confirmed-order scope fields such as `region`, `geo_level`,
`year`, `year_start`, and `year_end` stay at the load's top level; put only
source-specific conditions in `filters`.

## Open a selected view

The existing share state has this shape (replace the example IDs and filters
with values from the installed catalog and source metadata):

```json
{
  "v": 1,
  "lane": "explore",
  "loads": [
    {
      "kind": "source",
      "source_id": "EXACT_INSTALLED_SOURCE_ID",
      "pack_id": "EXACT_INSTALLED_PACK_ID",
      "mode": "metrics",
      "metric": "SOURCE_METRIC_KEY",
      "geo_level": "admin_2",
      "year": 2020,
      "filters": {}
    }
  ],
  "overlays": []
}
```

Save the selected state as `map-state.json`. Encode its compact UTF-8 JSON as
unpadded base64url and append it to `http://localhost:7000/?state=...`. In
PowerShell, this produces the link without an extra app endpoint:

```powershell
$mapState = Get-Content .\map-state.json -Raw | ConvertFrom-Json
$json = ConvertTo-Json -InputObject $mapState -Depth 20 -Compress
$encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($json)).TrimEnd('=').Replace('+','-').Replace('/','_')
$mapUrl = "http://localhost:7000/?state=$encoded"
$mapUrl
```

Opening the link makes the browser decode the state, execute the source load
through `/chat` with `confirmed_order`, and route the returned payload to the
existing renderer. The link can also include the current share-state camera,
overlays, and Explore time fields. Use the app's **Share map** command after
adjusting the view to get its own link for reopening. A link changes the map
when it is opened or navigated to; it does not push changes into a browser tab
that is already open elsewhere.

## Current limits and next proof

- Share state restores source loads, overlays, time, focus, and camera. Its
  current capture records only the primary `currentData` source load, so it is
  not yet a complete ordered multi-layer project export.
- Share links contain source and pack IDs but no exact content release pins,
  geometry release pins, provenance, or named project state. Record those
  separately for a collaborator result and do not claim a reproducible saved
  project from a share URL alone.
- The browser reports a failed load through the existing chat/error path; the
  link itself is not a typed agent acknowledgement. A future agent action
  ingress should reuse confirmed-order validation and return applied state or
  a typed refusal after display.
- The end-to-end installed-content check still needs one metric pack and
  compatible geometry in a selected local `DATA_ROOT`: open the generated
  link, verify the map and source context, share the resulting view, reopen it,
  and record exact content releases and any missing display behavior.

The Mapping project tracks release-pinned projects and a richer agent action
contract separately. Add those by extending the existing confirmed-order,
share-state, and renderer path.
