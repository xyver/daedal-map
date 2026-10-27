# Legacy County Map road-access scripts

This folder is a verbatim working reference copy of the earlier Road Access
pipeline. It prepared Census admin origins, routed them to FEMA NSS shelters
through a local Valhalla matrix service, and wrote reusable summaries and edge
tables.

It is retained here so the Road Network Editor can build on the proven input,
checkpoint, provider, and result contracts without changing the County Map
repository.

## Important boundary

These scripts still expect the original County Map workspace, including its
`path_bootstrap`, geometry data, and prepared FEMA shelter parquet. They are
not the new editor scenario runner and should not be modified to support
interactive road closures.

The editor-native, scenario-aware tools live in `scripts/`. They operate on
the Lake graph's stable segment IDs and therefore directly respect saved road
and shelter edits.

## Prior baseline result

The original California run used 25,586 block-group origins and 6,600 NSS
shelters with a nearest-75 candidate policy. Its Valhalla output remains the
statewide baseline/reference; the new scripts begin with dynamic county-scale
scenarios.
