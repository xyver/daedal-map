# Network changelog

Versions below describe published program changes.

## 0.1.0 — released (2026-10-27)

### Added

- A separate Network program ZIP carries the graph pack reader, Mapping
  adapter, Parquet CLI, and standalone spreadsheet editor.
- The spreadsheet lists and searches nodes, links, and features without
  Mapping. Cell edits are saved as draft Parquet outside immutable source packs.
- Mapping can show validated rail, river, and power packs and inspect their
  graph records. Network can also export graph tables to Parquet/GeoParquet.

### Compatibility and limits

- The Mapping overlay uses interface version 1. The spreadsheet and CLI run
  without Mapping.
- Graph data packs are separate downloads. None of the local test packs are
  included in this program ZIP.
- Draft edits cannot yet be validated and published as a new pack version.
  The archived Lake County map editor is excluded.
