# Mapping changelog

Versions below describe program changes. A **candidate** has been built
locally but has not been published to the download catalog.

## 0.1.1 — candidate (2026-10-27)

### Added

- Local Mapping can load an optional, compatible Network expansion and show
  rail, river, and power candidate layers on its existing map.
- Network layers have an in-map open/close action and anchored feature and
  node detail popups. The launcher can open Network directly even when
  automatic start is off.
- Local Mapping defaults to an online basemap with streets and labels; offline
  world map choices remain available.

### Compatibility and limits

- The Mapping ZIP contains no Network implementation or Network data. Mapping
  works without the expansion.
- Network interface version 1 is required for the visual overlay.
- A public 0.1.1 Mapping download has not been published.
