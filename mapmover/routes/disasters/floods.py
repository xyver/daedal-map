"""Flood disaster endpoints."""

from fastapi import APIRouter

from mapmover.disaster_filters import apply_location_filters, get_default_min_year
from mapmover.duckdb_helpers import (
    duckdb_available, is_s3_mode, make_cache_key, parquet_available,
    select_filtered_event_rows, select_filtered_event_rows_cached,
)
from mapmover.logging_analytics import logger
from mapmover.paths import GLOBAL_DIR

from .helpers import filter_by_time_range, msgpack_error, msgpack_response


router = APIRouter()


@router.get("/api/floods/geojson")
async def get_floods_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_year: int = None,
    max_year: int = None,
    include_geometry: bool = False,
    loc_prefix: str = None,
    affected_loc_id: str = None,
):
    """Get global floods as GeoJSON for map display."""
    import json as json_lib
    import pandas as pd

    if min_year is None:
        min_year = get_default_min_year("floods", fallback=1985)

    try:
        events_path = GLOBAL_DIR / "disasters/floods/events_enriched.parquet"
        if not events_path.exists():
            events_path = GLOBAL_DIR / "disasters/floods/events.parquet"
        if not parquet_available(events_path):
            return msgpack_error("Flood data not available", 404)

        use_duckdb = duckdb_available()
        if use_duckdb:
            if year is not None:
                df = select_filtered_event_rows_cached(
                    events_path,
                    cache_key=make_cache_key("floods", year=year),
                    year=year,
                )
            elif start is not None or end is not None:
                df = select_filtered_event_rows(events_path, start=start, end=end)
            else:
                df = select_filtered_event_rows_cached(
                    events_path,
                    cache_key=make_cache_key("floods", min_year=min_year),
                    min_value_filters={"year": min_year} if min_year is not None else None,
                )
                if max_year is not None and "year" in df.columns:
                    df = df[df["year"] <= max_year]
        else:
            df = pd.read_parquet(events_path)
            if year is not None:
                df = df[df["year"] == year]
            elif start is not None or end is not None:
                df = filter_by_time_range(df, start, end)
            else:
                if min_year:
                    df = df[df["year"] >= min_year]
                if max_year:
                    df = df[df["year"] <= max_year]

        df = apply_location_filters(
            df,
            "floods",
            loc_prefix=loc_prefix,
            affected_loc_id=affected_loc_id,
        )

        valid_mask = df["latitude"].notna() & df["longitude"].notna()
        records = df[valid_mask].to_dict("records")

        features = []
        for row in records:
            event_id = row.get("event_id", "")

            geom = None
            if include_geometry:
                perimeter = row.get("perimeter")
                if pd.notna(perimeter) and perimeter:
                    try:
                        geom = json_lib.loads(perimeter) if isinstance(perimeter, str) else perimeter
                    except Exception as e:
                        logger.warning(f"Failed to parse flood perimeter for {event_id}: {e}")

            if not geom:
                geom = {"type": "Point", "coordinates": [float(row["longitude"]), float(row["latitude"])]}

            ts = row.get("timestamp")
            ts_str = (
                ts.isoformat()
                if ts is not None and pd.notna(ts) and hasattr(ts, "isoformat")
                else (str(ts) if pd.notna(ts) else None)
            )
            end_ts = row.get("end_timestamp")
            end_ts_str = (
                end_ts.isoformat()
                if end_ts is not None and pd.notna(end_ts) and hasattr(end_ts, "isoformat")
                else (str(end_ts) if pd.notna(end_ts) else None)
            )

            props = {
                "event_id": event_id,
                "year": int(row["year"]) if pd.notna(row.get("year")) else None,
                "timestamp": ts_str,
                "end_timestamp": end_ts_str,
                "duration_days": int(row["duration_days"]) if pd.notna(row.get("duration_days")) else None,
                "country": str(row.get("country", "")) if pd.notna(row.get("country")) else None,
                "area_km2": float(row["area_km2"]) if pd.notna(row.get("area_km2")) else None,
                "severity": int(row["severity"]) if pd.notna(row.get("severity")) else None,
                "deaths": int(row["deaths"]) if pd.notna(row.get("deaths")) else None,
                "displaced": int(row["displaced"]) if pd.notna(row.get("displaced")) else None,
                "source": str(row.get("source", "")) if pd.notna(row.get("source")) else None,
                "has_geometry": bool(row.get("has_geometry", False)),
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "loc_id": str(row.get("loc_id", "")) if pd.notna(row.get("loc_id")) else None,
                "parent_loc_id": str(row.get("parent_loc_id", "")) if pd.notna(row.get("parent_loc_id")) else None,
                "sibling_level": int(row["sibling_level"]) if pd.notna(row.get("sibling_level")) else None,
                "iso3": str(row.get("iso3", "")) if pd.notna(row.get("iso3")) else None,
                "loc_confidence": float(row["loc_confidence"]) if pd.notna(row.get("loc_confidence")) else None,
            }

            features.append({"type": "Feature", "geometry": geom, "properties": props})

        return msgpack_response(
            {
                "type": "FeatureCollection",
                "features": features,
                "metadata": {
                    "count": len(features),
                    "min_year": min_year,
                    "max_year": max_year or 2019,
                    "include_geometry": include_geometry,
                },
            }
        )
    except Exception as e:
        logger.error(f"Error fetching floods: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/floods/{event_id}/geometry")
async def get_flood_geometry(event_id: str):
    """Get the flood extent polygon for a specific flood event."""
    import json as json_lib

    try:
        geometry_dir = GLOBAL_DIR / "disasters/floods/geometries"
        geom_file = geometry_dir / f"flood_{event_id}.geojson"
        if not geom_file.exists():
            return msgpack_error(f"Geometry not found for {event_id}", 404)

        with open(geom_file, "r") as f:
            geom_data = json_lib.load(f)
        return msgpack_response(geom_data)
    except Exception as e:
        logger.error(f"Error fetching flood geometry: {e}")
        return msgpack_error(str(e), 500)
