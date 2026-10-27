# Lake County Scenario Optimizer

This document describes the batch-analysis and optimization layer built on top
of the Road Network Editor. It is deliberately scoped to the prepared Lake
County road graph and its small cross-border buffer.

## The two data layers

### 1. Immutable trimmed baseline

`data/baseline/` is the prepared, small Lake County dataset. It contains the
editable road graph, road geometry, scope, and the filtered FEMA NSS shelter
snapshot. The graph is the routing authority for this prototype.

The baseline does not change when a user closes a road, changes direction, or
edits shelters.

### 2. Editable scenario

The UI stores local scenario state in `data/scenarios/`:

- road segment states: open, forward-only, reverse-only, or closed
- user-added shelters
- baseline/custom shelters closed in the scenario
- named UI saves also include the selected start point

These files are local runtime data, not browser cache. They survive a browser
or container restart, but are gitignored and are not part of the baseline ZIP.

Use **Save scenario** in the UI to create a named save. Export it before batch
analysis:

```powershell
docker compose run --rm editor python scripts/export_scenario.py `
  --saved-name lake-fire-closure-01 `
  --output /app/data/analysis/lake-fire-closure-01.json
```

The exported JSON is the portable scenario contract. It records the graph
fingerprint, road changes, shelter changes, scope, and saved start point.

## Batch workflow

```text
Prepared Lake baseline --\
                          +--> exported scenario JSON --> access / comparison / optimization scripts
UI edits + named save ----/
```

1. Prepare a table of multiple origins.
2. Export the desired UI scenario, or use the active scenario.
3. Run scenario access to calculate nearest active-shelter travel time at all
   origins.
4. Compare the result to a baseline result, if useful.
5. Provide candidate sites and run the greedy optimizer.

The scripts live in [`../scripts/README.md`](../scripts/README.md). They use a
reverse multi-source Dijkstra search from all active shelters, so one graph
traversal calculates nearest-shelter access for many origins. This is the
correct fast approach for the small dynamically edited Lake graph.

## Origin and candidate contracts

For now, batch tools accept CSV or Parquet coordinate tables.

Origins require:

```text
origin_id, longitude, latitude
```

Optional fields:

```text
origin_name, weight
```

`weight` is where population, equity weighting, or another planning priority
belongs. Address lists are coordinate CSVs. The copied legacy Road Access
pipeline can prepare block, block-group, tract, or county origin tables; only
Lake-area rows should be brought into this prototype.

`scripts/prepare_census_origins.py` is the direct local shortcut when the
DaedalMap USA geometry spine is available. It creates four Lake-road-scope CSV
files from county, tract, block-group, and block centroids. By default it
creates **Lake County origins only**. The active shelter list and calculated
routes still use the full prepared Lake-plus-border graph, so a Lake resident
can realistically reach a shelter just over a county line. Use
`--include-buffer-origins` only for a study that also treats nearby
cross-border residents as origins.

Candidates require:

```text
candidate_id, longitude, latitude
```

Optional `name` makes recommendations readable. Candidates should be credible
sites--schools, community centers, fairgrounds, lodging, or vetted
road-accessible points--not an arbitrary unrestricted grid.

## Current optimizer

`optimize_shelters.py` uses transparent greedy maximum coverage:

1. Find origins outside the target time (default 30 minutes).
2. For each candidate, find underserved origins it can reach within that time.
3. Select the candidate covering the greatest currently uncovered total
   `weight`.
4. Repeat until the requested number of shelters is selected or no candidate
   improves access.

It returns a compact JSON recommendation list with the newly covered origin
count and weight for every selected site. It does not yet model capacity,
hazard safety, or demand allocation; those are planned objective/constraint
extensions.

## Making it usable by an LLM

An LLM should not inspect graph edges or thousands of origin rows directly. It
needs narrow, deterministic operations with JSON summaries:

| Operation | Inputs | Returns |
| --- | --- | --- |
| List scenarios | none | saved scenario names, timestamps, scope |
| Export scenario | saved name | portable scenario JSON path/summary |
| Describe origins | origin table | count, origin type, weight total, bounds |
| Run access | scenario, origins, thresholds | compact access summary plus result path |
| Compare access | baseline result, scenario result | gained/lost access summary plus result path |
| Optimize shelters | scenario, origins, candidates, objective, count | ranked recommendations and coverage gains |
| Inspect recommendation | recommendation/candidate ID | affected origins, travel-time and weight details |

The local MCP server should be a thin wrapper around these scripts—not a new
routing implementation. It should validate scope, paths, thresholds, and
candidate counts; run the deterministic tools; and return small JSON summaries
with paths to Parquet/GeoJSON detail files. This lets an LLM say, for example,
"optimize ten shelters for 30-minute population-weighted access under the
saved fire-closure scenario," without loading the full graph into context.

The first local-only wrapper and its CSV contracts are documented in
[MCP setup](MCP_SETUP.md).

## Relationship to the prior California work

`legacy/road_access/` is a copied reference to the earlier California-wide
Valhalla matrix pipeline. It remains valuable for statewide baseline research.
The editor-native scenario scripts are intentionally Lake-only because they
apply segment-level road changes directly. A future larger-region deployment
can reuse these contracts while building an appropriately bounded graph and
origin/candidate inputs.
