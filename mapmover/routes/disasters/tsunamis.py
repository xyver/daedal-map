"""Tsunami disaster endpoints."""

from fastapi import APIRouter

from mapmover.disaster_filters import apply_location_filters, get_default_min_year
from mapmover.logging_analytics import logger
from mapmover.paths import GLOBAL_DIR

from .helpers import (
    build_geojson_features,
    ensure_year_column,
    filter_by_proximity,
    filter_by_time_range,
    filter_by_time_window,
    msgpack_error,
    msgpack_response,
    safe_float,
    safe_int,
    safe_str,
)


router = APIRouter()


def get_tsunami_property_builders():
    """Return property builders dict for tsunami source event GeoJSON features."""
    return {
        "event_id": lambda r: r.get("event_id", ""),
        "year": lambda r: safe_int(r, "year"),
        "timestamp": lambda r: safe_str(r, "timestamp", None),
        "country": lambda r: r.get("country", ""),
        "location": lambda r: safe_str(r, "location", None),
        "cause": lambda r: r.get("cause", ""),
        "cause_code": lambda r: safe_int(r, "cause_code"),
        "eq_magnitude": lambda r: safe_float(r, "eq_magnitude"),
        "max_water_height_m": lambda r: safe_float(r, "max_water_height_m"),
        "intensity": lambda r: safe_float(r, "intensity"),
        "runup_count": lambda r: safe_int(r, "runup_count", 0),
        "deaths": lambda r: safe_int(r, "deaths"),
        "damage_millions": lambda r: safe_float(r, "damage_millions"),
        "loc_id": lambda r: r.get("loc_id", ""),
        "latitude": lambda r: safe_float(r, "latitude"),
        "longitude": lambda r: safe_float(r, "longitude"),
        "is_source": lambda r: True,
    }


@router.get("/api/tsunamis/geojson")
async def get_tsunamis_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_year: int = None,
    cause: str = None,
    loc_prefix: str = None,
    affected_loc_id: str = None,
):
    """Get tsunami source events as GeoJSON points for map display."""
    import pandas as pd

    if min_year is None:
        min_year = get_default_min_year("tsunamis", fallback=1900)

    try:
        events_path = GLOBAL_DIR / "disasters/tsunamis/events.parquet"
        if not events_path.exists():
            return msgpack_error("Tsunami data not available", 404)

        df = pd.read_parquet(events_path)

        if year is not None:
            df = df[df["year"] == year]
        elif start is not None or end is not None:
            df = filter_by_time_range(df, start, end)
        elif min_year is not None:
            df = df[df["year"] >= min_year]
        if cause is not None:
            df = df[df["cause"].str.lower() == cause.lower()]

        df = apply_location_filters(
            df,
            "tsunamis",
            loc_prefix=loc_prefix,
            affected_loc_id=affected_loc_id,
        )

        features = build_geojson_features(df, get_tsunami_property_builders())
        return msgpack_response(
            {
                "type": "FeatureCollection",
                "features": features,
                "metadata": {
                    "count": len(features),
                    "year_range": [int(df["year"].min()), int(df["year"].max())] if len(df) > 0 else None,
                },
            }
        )
    except Exception as e:
        logger.error(f"Error fetching tsunamis GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/tsunamis/{event_id}/runups")
async def get_tsunami_runups(event_id: str):
    """Get runup observations for a specific tsunami event."""
    import pandas as pd

    try:
        runups_path = GLOBAL_DIR / "disasters/tsunamis/runups.parquet"
        events_path = GLOBAL_DIR / "disasters/tsunamis/events.parquet"

        if not runups_path.exists():
            return msgpack_error("Runup data not available", 404)

        runups_df = pd.read_parquet(runups_path)
        runups_df = runups_df[runups_df["event_id"] == event_id]
        if len(runups_df) == 0:
            return msgpack_error(f"No runups found for event {event_id}", 404)

        source_event = None
        if events_path.exists():
            events_df = pd.read_parquet(events_path)
            event_row = events_df[events_df["event_id"] == event_id]
            if len(event_row) > 0:
                row = event_row.iloc[0]
                source_event = {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [float(row["longitude"]), float(row["latitude"])]},
                    "properties": {
                        "event_id": event_id,
                        "year": int(row["year"]) if pd.notna(row.get("year")) else None,
                        "timestamp": str(row["timestamp"]) if pd.notna(row.get("timestamp")) else None,
                        "cause": row.get("cause", ""),
                        "eq_magnitude": float(row["eq_magnitude"]) if pd.notna(row.get("eq_magnitude")) else None,
                        "max_water_height_m": float(row["max_water_height_m"]) if pd.notna(row.get("max_water_height_m")) else None,
                        "_isSource": True,
                        "is_source": True,
                    },
                }

        features = []
        for _, row in runups_df.iterrows():
            if pd.isna(row["latitude"]) or pd.isna(row["longitude"]):
                continue
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [float(row["longitude"]), float(row["latitude"])]},
                    "properties": {
                        "runup_id": row.get("runup_id", ""),
                        "event_id": event_id,
                        "year": int(row["year"]) if pd.notna(row.get("year")) else None,
                        "country": row.get("country", ""),
                        "location_name": row.get("location", "") if pd.notna(row.get("location")) else None,
                        "water_height_m": float(row["water_height_m"]) if pd.notna(row.get("water_height_m")) else None,
                        "dist_from_source_km": float(row["dist_from_source_km"]) if pd.notna(row.get("dist_from_source_km")) else None,
                        "travel_time_hours": float(row["arrival_travel_time_min"]) / 60 if pd.notna(row.get("arrival_travel_time_min")) else None,
                        "deaths": int(row["deaths"]) if pd.notna(row.get("deaths")) else None,
                        "_isSource": False,
                        "is_source": False,
                    },
                }
            )

        return msgpack_response(
            {
                "type": "FeatureCollection",
                "features": features,
                "source": source_event,
                "metadata": {
                    "event_id": event_id,
                    "event_type": "tsunami",
                    "total_count": len(features),
                    "runup_count": len(features),
                },
            }
        )
    except Exception as e:
        logger.error(f"Error fetching tsunami runups: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/tsunamis/{event_id}/animation")
async def get_tsunami_animation_data(event_id: str):
    """Get combined source + runups data formatted for radial animation."""
    import pandas as pd

    try:
        runups_path = GLOBAL_DIR / "disasters/tsunamis/runups.parquet"
        events_path = GLOBAL_DIR / "disasters/tsunamis/events.parquet"
        if not events_path.exists() or not runups_path.exists():
            return msgpack_error("Tsunami data not available", 404)

        events_df = pd.read_parquet(events_path)
        event_row = events_df[events_df["event_id"] == event_id]
        if len(event_row) == 0:
            return msgpack_error(f"Event {event_id} not found", 404)
        row = event_row.iloc[0]

        features = [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(row["longitude"]), float(row["latitude"])]},
                "properties": {
                    "event_id": event_id,
                    "year": int(row["year"]) if pd.notna(row.get("year")) else None,
                    "timestamp": str(row["timestamp"]) if pd.notna(row.get("timestamp")) else None,
                    "cause": row.get("cause", ""),
                    "eq_magnitude": float(row["eq_magnitude"]) if pd.notna(row.get("eq_magnitude")) else None,
                    "max_water_height_m": float(row["max_water_height_m"]) if pd.notna(row.get("max_water_height_m")) else None,
                    "deaths": int(row["deaths"]) if pd.notna(row.get("deaths")) else None,
                    "is_source": True,
                },
            }
        ]

        runups_df = pd.read_parquet(runups_path)
        runups_df = runups_df[runups_df["event_id"] == event_id]
        for _, rrow in runups_df.iterrows():
            if pd.isna(rrow["latitude"]) or pd.isna(rrow["longitude"]):
                continue
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [float(rrow["longitude"]), float(rrow["latitude"])]},
                    "properties": {
                        "runup_id": rrow.get("runup_id", ""),
                        "event_id": event_id,
                        "country": rrow.get("country", ""),
                        "location_name": rrow.get("location", "") if pd.notna(rrow.get("location")) else None,
                        "water_height_m": float(rrow["water_height_m"]) if pd.notna(rrow.get("water_height_m")) else None,
                        "dist_from_source_km": float(rrow["dist_from_source_km"]) if pd.notna(rrow.get("dist_from_source_km")) else None,
                        "arrival_travel_time_min": float(rrow["arrival_travel_time_min"]) if pd.notna(rrow.get("arrival_travel_time_min")) else None,
                        "timestamp": str(rrow["timestamp"]) if pd.notna(rrow.get("timestamp")) else None,
                        "deaths": int(rrow["deaths"]) if pd.notna(rrow.get("deaths")) else None,
                        "is_source": False,
                    },
                }
            )

        return msgpack_response(
            {
                "type": "FeatureCollection",
                "features": features,
                "metadata": {
                    "event_id": event_id,
                    "event_type": "tsunami",
                    "total_count": len(features),
                    "source_timestamp": str(row["timestamp"]) if pd.notna(row.get("timestamp")) else None,
                    "runup_count": len(features) - 1,
                    "animation_mode": "radial",
                },
            }
        )
    except Exception as e:
        logger.error(f"Error fetching tsunami animation data: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/events/nearby-tsunamis")
async def get_nearby_tsunamis(
    lat: float,
    lon: float,
    timestamp: str = None,
    year: int = None,
    radius_km: float = 300.0,
    days_before: int = 1,
    days_after: int = 30,
):
    """Find tsunamis near a location within a time window."""
    import pandas as pd

    try:
        tsunamis_path = GLOBAL_DIR / "disasters/tsunamis/events.parquet"
        if not tsunamis_path.exists():
            return msgpack_error("Tsunami data not available", 404)

        df = pd.read_parquet(tsunamis_path)
        df = filter_by_proximity(df, lat, lon, radius_km)

        if timestamp:
            df = filter_by_time_window(df, timestamp, days_before, days_after)
        elif year:
            df = ensure_year_column(df)
            df = df[df["year"] == year]

        if len(df) == 0:
            return msgpack_response(
                {
                    "type": "FeatureCollection",
                    "features": [],
                    "count": 0,
                    "search_params": {"lat": lat, "lon": lon, "radius_km": radius_km},
                }
            )

        features = build_geojson_features(df, get_tsunami_property_builders())
        logger.info(f"Found {len(features)} tsunamis within {radius_km}km of ({lat}, {lon})")
        return msgpack_response(
            {
                "type": "FeatureCollection",
                "features": features,
                "count": len(features),
                "search_params": {
                    "lat": lat,
                    "lon": lon,
                    "radius_km": radius_km,
                    "days_before": days_before,
                    "days_after": days_after,
                },
            }
        )
    except Exception as e:
        logger.error(f"Error finding nearby tsunamis: {e}")
        return msgpack_error(str(e), 500)

