"""Earthquake disaster endpoints."""

from fastapi import APIRouter

from mapmover.disaster_filters import apply_location_filters
from mapmover.logging_analytics import logger
from mapmover.paths import GLOBAL_DIR

from .helpers import (
    build_geojson_features,
    ensure_year_column,
    filter_by_time_range,
    msgpack_error,
    msgpack_response,
    safe_float,
    safe_int,
    safe_str,
)


router = APIRouter()


def get_earthquake_property_builders():
    """Return property builders dict for earthquake GeoJSON features."""
    return {
        "event_id": lambda r: r.get("event_id", ""),
        "magnitude": lambda r: safe_float(r, "magnitude"),
        "depth_km": lambda r: safe_float(r, "depth_km"),
        "felt_radius_km": lambda r: safe_float(r, "felt_radius_km", 0),
        "damage_radius_km": lambda r: safe_float(r, "damage_radius_km", 0),
        "place": lambda r: r.get("place", ""),
        "time": lambda r: safe_str(r, "timestamp", None),
        "timestamp": lambda r: safe_str(r, "timestamp", None),
        "year": lambda r: safe_int(r, "year"),
        "loc_id": lambda r: r.get("loc_id", ""),
        "latitude": lambda r: safe_float(r, "latitude"),
        "longitude": lambda r: safe_float(r, "longitude"),
        "mainshock_id": lambda r: r.get("mainshock_id") if r.get("mainshock_id") else None,
        "sequence_id": lambda r: r.get("sequence_id") if r.get("sequence_id") else None,
    }


@router.get("/api/earthquakes/geojson")
async def get_earthquakes_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_magnitude: float = None,
    limit: int = None,
    loc_prefix: str = None,
    affected_loc_id: str = None,
):
    """Get earthquakes as GeoJSON points for map display."""
    import pandas as pd

    try:
        events_path = GLOBAL_DIR / "disasters/earthquakes/events.parquet"
        if not events_path.exists():
            return msgpack_error("Earthquake data not available", 404)

        df = pd.read_parquet(events_path)
        df = ensure_year_column(df)

        if year is not None and "year" in df.columns:
            df = df[df["year"] == year]
        elif start is not None or end is not None:
            df = filter_by_time_range(df, start, end)
        if min_magnitude is not None:
            df = df[df["magnitude"] >= min_magnitude]

        df = apply_location_filters(
            df,
            "earthquakes",
            loc_prefix=loc_prefix,
            affected_loc_id=affected_loc_id,
        )

        if limit is not None and limit > 0:
            df = df.nlargest(limit, "magnitude")

        features = build_geojson_features(df, get_earthquake_property_builders())
        return msgpack_response({"type": "FeatureCollection", "features": features})

    except Exception as e:
        logger.error(f"Error fetching earthquakes GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/earthquakes/sequence/{sequence_id}")
async def get_earthquake_sequence(sequence_id: str, min_magnitude: float = None):
    """Get all earthquakes in a specific aftershock sequence."""
    import pandas as pd

    try:
        events_path = GLOBAL_DIR / "disasters/earthquakes/events.parquet"
        if not events_path.exists():
            return msgpack_error("Earthquake data not available", 404)

        df = pd.read_parquet(events_path)
        df = df[df["sequence_id"] == sequence_id]

        if len(df) == 0:
            return msgpack_error(f"Sequence {sequence_id} not found", 404)

        if min_magnitude is not None:
            df = df[df["magnitude"] >= min_magnitude]

        df = ensure_year_column(df)
        features = build_geojson_features(df, get_earthquake_property_builders())

        logger.info(f"Returning {len(features)} events for sequence {sequence_id}")
        return msgpack_response(
            {
                "type": "FeatureCollection",
                "features": features,
                "sequence_id": sequence_id,
            }
        )

    except Exception as e:
        logger.error(f"Error fetching earthquake sequence {sequence_id}: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/earthquakes/aftershocks/{event_id}")
async def get_earthquake_aftershocks(event_id: str, min_magnitude: float = None):
    """Get mainshock + aftershocks for a specific event ID."""
    import pandas as pd

    try:
        events_path = GLOBAL_DIR / "disasters/earthquakes/events.parquet"
        if not events_path.exists():
            return msgpack_error("Earthquake data not available", 404)

        df = pd.read_parquet(events_path)

        mainshock_df = df[df["event_id"] == event_id]
        if len(mainshock_df) == 0:
            return msgpack_error(f"Event {event_id} not found", 404)

        aftershocks_df = df[df["mainshock_id"] == event_id]
        result_df = pd.concat([mainshock_df, aftershocks_df], ignore_index=True)

        if min_magnitude is not None:
            result_df = result_df[result_df["magnitude"] >= min_magnitude]

        result_df = ensure_year_column(result_df)
        features = build_geojson_features(result_df, get_earthquake_property_builders())

        logger.info(f"Returning {len(features)} events for mainshock {event_id}")
        return msgpack_response(
            {
                "type": "FeatureCollection",
                "features": features,
                "metadata": {
                    "event_id": event_id,
                    "event_type": "earthquake",
                    "total_count": len(features),
                    "aftershock_count": len(features) - 1,
                },
            }
        )

    except Exception as e:
        logger.error(f"Error fetching aftershocks for {event_id}: {e}")
        return msgpack_error(str(e), 500)

