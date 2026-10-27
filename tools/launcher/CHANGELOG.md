# Launcher changelog

Versions below describe program changes. A **candidate** has been built
locally but has not been published to the download catalog.

## 0.1.6 — candidate (2026-10-27)

### Added

- **Programs** has Cloud and Installed views. Local Custom shows editable
  source links and installed programs in the same launcher library.
- Network's local source card opens its Mapping overlay or standalone
  spreadsheet. An installed Network program can also open the spreadsheet.
- The default online basemap has streets and labels; the built-in and detailed
  offline world maps remain selectable.

### Changed

- The Connected/Local Custom setting uses one switch with a restart prompt.
- The header identifies Local Custom with one colored status badge.
- Data folder changes can be reviewed and indexed through **Update Catalog**.
- Source launcher runtime setup uses the pinned `uv 0.13.5` helper.

### Compatibility and limits

- The spreadsheet editor runs without Mapping. The Network map requires a
  compatible Mapping program and Network interface version 1.
- A public 0.1.6 launcher download has not been published.
