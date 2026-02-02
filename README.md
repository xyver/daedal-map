# Geographic Data Explorer

An interactive map system that lets you explore global data through natural language. Ask questions about any location and see the answers visualized on a 3D globe.

**Live Demo**: [county-map.up.railway.app]([https://county-map.up.railway.app](https://county-map-production.up.railway.app/))

---

## This Is a Demo

This repository is a self-contained demo version of the full Geographic Data Explorer. It ships with a curated subset of public-domain data so you can clone it and run it immediately without any external data downloads.

**What the demo includes:**
- 7 disaster datasets (earthquakes, hurricanes, wildfires, tornadoes, floods, tsunamis, volcanoes) -- mostly 2015-2025
- All 17 UN Sustainable Development Goal indicator sets (~191 countries)
- Country-level geometry (admin_0 boundaries for 217 countries)

**What the full version adds:**
- Sub-national boundaries (states, provinces, counties) for 267 countries
- Extended disaster history (1M+ earthquake records back to 1521, etc.)
- Additional indicator sources (OWID, World Bank, national statistics)
- Higher-resolution geometry and more granular data

The demo is fully functional -- the same app code runs both versions. The only difference is the data available in the `data/` folder. See "Adding Your Own Data" below to extend it.

---

## Quick Start

```bash
git clone <this repo>
cd county-map
pip install -r requirements.txt
```

Create a `.env` file with your Anthropic API key:
```
ANTHROPIC_API_KEY=your_key_here
```

Run:
```bash
python app.py
```

Open http://localhost:7000 -- the bundled demo data loads automatically.

---

## What You Can Do

**Ask questions in plain language:**
- "Show me earthquakes in Japan"
- "Hurricane tracks in the Atlantic 2020"
- "What volcanoes have erupted near Indonesia?"
- "UN poverty indicators for Africa"

**The chat understands three things:** location (where), data (what), and time (when).
Type "help" or "how do you work?" for a full guide.

**Interactive 3D Globe:**
- MapLibre GL JS with globe projection
- Choropleth coloring for indicator data
- Point markers with radius scaling for events
- Animated track lines for hurricanes
- Time slider for filtering by year/date range

---

## Included Demo Data (~46 MB)

The `data/` folder contains a filtered subset ready to use out of the box:

| Dataset | Source | Filter | Events |
|---------|--------|--------|--------|
| Earthquakes | USGS + NOAA | M5.0+, 2015-2025 | 19,132 |
| Hurricanes | IBTrACS | 2015-2025 | 1,177 storms |
| Wildfires | Global Fire Atlas | 200+ km2, excl USA/CAN | 11,235 |
| Tornadoes | NOAA + Canada | 2015-2025 | 17,338 |
| Floods | DFO | 2015-2019 | 610 |
| Tsunamis | NOAA NCEI | 2015-2025 | 193 |
| Volcanoes | Smithsonian GVP | 2015-2025 | 383 |
| UN SDGs | UN Stats | All 17 goals, ~191 countries | -- |
| Geometry | GADM (simplified) | Country outlines only | 217 countries |

All data sourced from public domain / open-license APIs.

---

## Adding Your Own Data

This project uses a schema-driven approach. If your data matches the format, it renders automatically -- no app code changes needed.

### For event/disaster data:

```python
import pandas as pd

df = pd.DataFrame({
    "event_id": ["EQ-001", "EQ-002"],
    "timestamp": ["2024-01-15", "2024-02-20"],
    "latitude": [35.6, -33.8],
    "longitude": [139.7, 151.2],
    "magnitude": [6.2, 5.8],
    "loc_id": ["JPN-13-QUAKE-001", "AUS-NSW-QUAKE-002"],
    "year": [2024, 2024]
})
df.to_parquet("data/global/disasters/my_events/events.parquet")
```

### For country-level indicators:

```python
df = pd.DataFrame({
    "loc_id": ["USA", "GBR", "DEU"],
    "year": [2022, 2022, 2022],
    "my_metric": [100, 85, 92]
})
df.to_parquet("data/global/my_source/all_countries.parquet")
```

Add a `metadata.json` next to your parquet file describing the source, then rebuild the catalog:

```bash
python converters/catalog_builder.py data/
```

See [docs/DATA_SCHEMAS.md](docs/DATA_SCHEMAS.md) for the full schema specification.

---

## Expanding the Data

The demo ships with country-level geometry only. For sub-national boundaries (states, provinces, counties), download GADM data:

```bash
# Download GADM boundaries for a specific country
python converters/setup_gadm.py --country USA
```

For the full production dataset (1M+ earthquake events, 50+ indicators, sub-national geometry for 267 countries), see the data expansion section in the docs.

---

## Converters

The `converters/` folder contains tools for building and managing data:

| Script | Purpose |
|--------|---------|
| `catalog_builder.py` | Build catalog.json and index.json from metadata files |
| `setup_gadm.py` | Download and process GADM sub-national boundaries |

To rebuild the catalog after adding/modifying data:
```bash
python converters/catalog_builder.py data/
python converters/catalog_builder.py data/ --catalog-only
python converters/catalog_builder.py data/ --indexes-only
```

---

## Architecture

```
User Query -> Preprocessor -> LLM -> Postprocessor -> Order Executor -> Map Display
               (hints)       (interpret)  (validate)    (fetch data)     (render)
```

The system uses MessagePack for all API responses (not JSON). See the docs for details.

**Data path resolution** (in `mapmover/paths.py`):
1. `DATA_ROOT` env var (deployment)
2. Sibling `county-map-data/` folder (local dev with full data)
3. Bundled `data/` folder (demo fallback)

---

## Documentation

| Document | Purpose |
|----------|---------|
| [docs/DATA_SCHEMAS.md](docs/DATA_SCHEMAS.md) | Data formats, loc_id specification, parquet schemas |
| [docs/public reference.md](docs/public%20reference.md) | Data source attribution and licensing |

---

## License

MIT License

---

*Last Updated: 2026-01-24*
