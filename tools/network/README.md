# Network

The current [Network source](../../network/README.md) lives under `network/`.
It is an optional Mapping expansion: Mapping owns the single interactive map,
and Network supplies validated rail, river, and power layers. Network also has
a standalone, non-map spreadsheet editor and a Parquet export CLI. The
spreadsheet runs without Mapping; the old Lake County road map editor is
archived reference code.

The Network source is not yet a maintained program download. Its first
separate ZIP must include the Mapping adapter, graph reader, CLI, spreadsheet
server and HTML, and runtime requirements. Network data packs stay separate
from program ZIPs and live in the launcher data library. The
[single-tool guide](../../docs/WORK_ON_ONE_TOOL.md#network-source-checkout)
shows a small checkout for source work.
