# Data Schemas

This is the simplified schema guide for DaedalMap data.

Use it when you are:
- trying to understand how the app expects data to be shaped
- building a new converter
- preparing a source pack for local use or hosted release

This is not the full internal pipeline spec. It is the practical public version.

---

## Core Idea

DaedalMap works because different datasets can meet on the same geography.

The main join key is:

`loc_id`

And for time-based aggregate data, the main join is:

`(loc_id, year)`

That rule is what lets the app combine:
- geometry
- country and subnational indicators
- event overlays
- pack metadata

---

## `loc_id`

`loc_id` is the canonical location identifier used across the system.

Basic pattern:

```text
{country}[-{admin1}[-{admin2}[-{admin3}...]]]
```

Examples:

| Level | Example | Meaning |
|-------|---------|---------|
| Country | `USA` | United States |
| State / province | `USA-CA` | California |
| County / district | `USA-CA-6037` | Los Angeles County |
| Canada province | `CAN-BC` | British Columbia |
| Europe region | `DEU-DE11` | NUTS region |

Rules:
- country codes are ISO 3166-1 alpha-3, uppercase
- `loc_id` must match the geometry files
- do not invent extra location name columns if `loc_id` already identifies the place

For most app behavior, `loc_id` is the shared language between data and map layers.

---

## Main Dataset Types

DaedalMap uses a small number of recurring schema shapes.

### 1. Aggregate Data

This is the standard format for indicators such as:
- population
- GDP
- risk scores
- emissions
- health measures

Typical shape:

| loc_id | year | metric_a | metric_b |
|--------|------|----------|----------|
| USA | 2020 | ... | ... |
| USA-CA | 2020 | ... | ... |
| USA-CA-6037 | 2020 | ... | ... |

Required columns:
- `loc_id`
- `year`

Recommended:
- one or more numeric metric columns

Avoid:
- redundant name columns like `state_name`, `county_name`, `region_name`
- duplicate ID systems like raw FIPS or GEOID if they are already encoded into `loc_id`

Common file names:
- `aggregates.parquet`
- `{ISO3}.parquet`
- `all_countries.parquet`

The exact filename can vary by scope, but the row logic is the same.

### 2. Event Data

This is the standard format for discrete incidents such as:
- earthquakes
- tornadoes
- eruptions
- tsunamis
- wildfire events

Typical shape:

| event_id | timestamp | latitude | longitude | event_type | loc_id |
|----------|-----------|----------|-----------|------------|--------|
| eq_001 | 2024-01-15T08:30:00Z | 34.05 | -118.24 | earthquake | USA-CA |

Required columns:
- `event_id`
- `timestamp`
- `latitude`
- `longitude`

Common columns:
- `event_type`
- `loc_id`
- `name`
- `status`
- magnitude / severity fields

Common file name:
- `events.parquet`

### 3. Event Areas

Some hazards also need an affected-area layer separate from the event point or track.

Examples:
- flood extents
- wildfire perimeters
- evacuation or impact polygons

Typical use:
- the event record identifies the incident
- the event-area file describes where it physically spread or what it affected

Common file name:
- `event_areas.parquet`

### 4. Links

Some sources need a link table that ties related records together.

Examples:
- event to article/reference links
- event to area links
- cross-source event matching

Common file name:
- `links.parquet`

### 5. Progression Data

Some hazards change over time and need a time-sequenced geometry layer.

Examples:
- wildfire progression
- flood progression
- storm track positions

This is not required for every source, but it is part of the broader event model for dynamic hazards.

---

## Geometry Schema

Geometry is stored separately from most indicator data and joins through `loc_id`.

Typical geometry columns:

| Column | Description |
|--------|-------------|
| `loc_id` | Canonical location ID |
| `name` | Display name |
| `admin_level` | Geographic level |
| `parent_id` | Parent location |
| `geometry` | Polygon / multipolygon geometry |
| `centroid_lat` | Latitude for center point |
| `centroid_lon` | Longitude for center point |

Geometry gives the app:
- map boundaries
- hierarchy navigation
- location names
- centroids and bounds for display and filtering

Data should generally not duplicate geometry metadata unless the source truly requires it.

---

## Metadata Schema

Every source should have a `metadata.json`.

This is the source's public description layer. It tells the app and the user what the data is.

Typical metadata fields:

```json
{
  "source_id": "usgs_earthquakes",
  "source_name": "USGS Earthquake Catalog",
  "source_url": "https://earthquake.usgs.gov/",
  "description": "Global earthquake event data.",
  "license": "Public Domain",
  "last_updated": "2026-03-01",
  "geographic_coverage": {
    "type": "global",
    "admin_levels": [0]
  },
  "temporal_coverage": {
    "start": 1900,
    "end": 2026,
    "field": "timestamp"
  },
  "metrics": {
    "magnitude": {
      "name": "Magnitude",
      "unit": "Mw"
    }
  }
}
```

At minimum, metadata should answer:
- what is this source
- where did it come from
- what geography does it cover
- what time period does it cover
- what fields matter

Good metadata is important for:
- the UI
- source transparency
- pack documentation
- QA and release decisions

---

## Folder Shape

The exact internal data tree can be large, but the public mental model is simple:

```text
county-map-data/
  catalog.json
  index.json

  global/
    {source}/
      all_countries.parquet
      metadata.json
      reference.json

  countries/
    {ISO3}/
      index.json
      crosswalk.json
      {source}/
        {ISO3}.parquet
        events.parquet
        event_areas.parquet
        links.parquet
        metadata.json
        reference.json

  geometry/
    {ISO3}.parquet
```

Not every source has every file.

In practice:
- aggregate indicator sources usually have one main parquet plus metadata
- event sources usually have `events.parquet`
- richer hazard sources may also have `event_areas`, `links`, or progression files

---

## Global vs Country Data

There are two common scopes.

### Global Sources

Used for:
- country-level world data
- broad cross-country comparisons

Examples:
- `global/owid_co2/`
- `global/who_health/`
- `global/disasters/earthquakes/`

### Country or Regional Sources

Used for:
- subnational data
- country-specific admin hierarchies
- sources with deeper local detail

Examples:
- `countries/USA/...`
- `countries/CAN/...`
- `countries/AUS/...`
- `countries/EUR/...`

The core schema logic stays the same. Only the scope changes.

---

## What a Good Source Looks Like

A source is in good shape when it has:

1. geometry-compatible `loc_id` values
2. clear time fields (`year` or `timestamp`)
3. numeric metrics where appropriate
4. a valid `metadata.json`
5. a clear source identity and update story

For hazard and event sources, a mature package often grows into:
- `events`
- `event_areas`
- `aggregates`
- `links`
- progression data if the hazard changes through time

Not every source starts there, but that is the direction of a well-structured pack.

---

## What to Avoid

Avoid these common mistakes:

- storing names instead of stable IDs
- mixing multiple geography systems without a crosswalk
- saving metrics as strings when they should be numeric
- duplicating geometry information in every data row
- using inconsistent event timestamps
- shipping a parquet file without metadata

If a dataset cannot cleanly join to `loc_id`, it is not really ready for the app.

---

## Practical Conversion Pattern

Most converters follow the same shape:

1. load raw data
2. normalize geography into `loc_id`
3. normalize time into `year` or `timestamp`
4. keep only the meaningful data columns
5. write parquet
6. write or update metadata

Simple example:

```python
import pandas as pd

df = pd.read_csv("raw_data.csv")
df = df.rename(columns={"region_code": "loc_id", "data_year": "year"})
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df["value"] = pd.to_numeric(df["value"], errors="coerce")
df = df[["loc_id", "year", "value"]]
df.to_parquet("output.parquet", index=False)
```

---

## In Short

If you remember only a few rules, remember these:

- `loc_id` is the core geographic join key
- aggregate data usually joins on `(loc_id, year)`
- event data centers on `event_id`, `timestamp`, and coordinates
- geometry lives separately and should be reused, not duplicated
- every source should describe itself with metadata

That is the foundation the rest of the app builds on.

---

## Related Docs

- [APP_OVERVIEW.md](APP_OVERVIEW.md)
- [LOCAL_AND_HOSTED.md](LOCAL_AND_HOSTED.md)
- [public reference.md](public%20reference.md)
