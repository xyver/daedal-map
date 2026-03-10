"""
Shared filtering utilities for disaster API endpoints.

Provides reusable location filtering for all disaster types:
- loc_prefix: Filter by event location prefix (epicenter)
- affected_loc_id: Filter by affected areas (uses event_areas tables)

Also provides metadata loading for disaster year ranges and configuration.

Usage:
    from mapmover.disaster_filters import apply_location_filters, get_disaster_metadata

    df = apply_location_filters(
        df,
        disaster_type='earthquakes',
        loc_prefix=loc_prefix,
        affected_loc_id=affected_loc_id
    )

    meta = get_disaster_metadata('earthquakes')
    default_year = meta['default_min_year']  # 1900
"""
import json
import pandas as pd
from pathlib import Path
from . import GLOBAL_DIR
from .duckdb_helpers import select_distinct_event_loc_ids

# Metadata cache
_DISASTER_METADATA = None
_METADATA_PATH = Path(__file__).parent / "disaster_metadata.json"


def _load_metadata() -> dict:
    """Load disaster metadata from JSON file (cached)."""
    global _DISASTER_METADATA
    if _DISASTER_METADATA is None:
        try:
            with open(_METADATA_PATH, 'r') as f:
                _DISASTER_METADATA = json.load(f)
        except Exception:
            _DISASTER_METADATA = {}
    return _DISASTER_METADATA


def get_disaster_metadata(disaster_type: str) -> dict:
    """
    Get metadata for a specific disaster type.

    Args:
        disaster_type: Type of disaster (earthquakes, hurricanes, etc.)

    Returns:
        Dict with year ranges, severity filters, etc.
        Returns empty dict if disaster type not found.
    """
    metadata = _load_metadata()
    return metadata.get(disaster_type, {})


def get_default_min_year(disaster_type: str, fallback: int = 2000) -> int:
    """
    Get the default minimum year for a disaster type.

    This is the "modern reliable data" cutoff, not the earliest data available.

    Args:
        disaster_type: Type of disaster
        fallback: Value to return if metadata not found

    Returns:
        Default minimum year for API filtering
    """
    meta = get_disaster_metadata(disaster_type)
    return meta.get("default_min_year", fallback)


def get_all_disaster_metadata() -> dict:
    """Get metadata for all disaster types."""
    return _load_metadata()


def apply_location_filters(
    df: pd.DataFrame,
    disaster_type: str,
    loc_prefix: str = None,
    affected_loc_id: str = None,
    event_id_col: str = 'event_id',
    loc_id_col: str = 'loc_id'
) -> pd.DataFrame:
    """
    Apply location-based filters to a disaster DataFrame.

    Args:
        df: DataFrame with disaster events
        disaster_type: Type of disaster (earthquakes, tsunamis, tornadoes, etc.)
                       Used to find the correct event_areas table
        loc_prefix: Filter by event location prefix (e.g., "USA", "USA-CA")
                    Filters events where loc_id starts with this prefix
        affected_loc_id: Filter by affected area (e.g., "USA-CA-6037")
                         Uses event_areas table to find events that affected this location
        event_id_col: Name of the event ID column in df (default: 'event_id')
        loc_id_col: Name of the location ID column in df (default: 'loc_id')

    Returns:
        Filtered DataFrame
    """
    if df.empty:
        return df

    # Filter by epicenter location prefix
    if loc_prefix is not None and loc_id_col in df.columns:
        df = df[df[loc_id_col].str.startswith(loc_prefix, na=False)]

    # Filter by affected area (uses event_areas table)
    if affected_loc_id is not None and event_id_col in df.columns:
        areas_path = GLOBAL_DIR / "disasters/event_areas" / f"{disaster_type}.parquet"
        if areas_path.exists():
            try:
                affected_events = select_distinct_event_loc_ids(areas_path, affected_loc_id)
                if not affected_events:
                    areas_df = pd.read_parquet(areas_path)
                    affected_events = areas_df[
                        areas_df['affected_loc_id'].str.startswith(affected_loc_id, na=False)
                    ]['event_loc_id'].unique()
                df = df[df[event_id_col].isin(affected_events)]
            except Exception:
                # If event_areas fails, return unfiltered (graceful degradation)
                pass

    return df


def get_affected_event_ids(
    disaster_type: str,
    affected_loc_id: str
) -> set:
    """
    Get set of event IDs that affected a specific location.

    Args:
        disaster_type: Type of disaster (earthquakes, tsunamis, etc.)
        affected_loc_id: Location ID to check (prefix match supported)

    Returns:
        Set of event_id values that affected this location
    """
    areas_path = GLOBAL_DIR / "disasters/event_areas" / f"{disaster_type}.parquet"
    if not areas_path.exists():
        return set()

    try:
        affected = select_distinct_event_loc_ids(areas_path, affected_loc_id)
        if not affected:
            areas_df = pd.read_parquet(areas_path)
            affected = areas_df[
                areas_df['affected_loc_id'].str.startswith(affected_loc_id, na=False)
            ]['event_loc_id'].unique()
        return set(affected)
    except Exception:
        return set()


def get_events_for_location(
    disaster_type: str,
    loc_id: str,
    include_children: bool = True
) -> dict:
    """
    Get summary of disaster events for a location.

    Args:
        disaster_type: Type of disaster
        loc_id: Location ID (e.g., "USA-CA" or "USA-CA-6037")
        include_children: If True, includes events affecting child regions

    Returns:
        Dict with event counts and sample event IDs
    """
    areas_path = GLOBAL_DIR / "disasters/event_areas" / f"{disaster_type}.parquet"
    if not areas_path.exists():
        return {"count": 0, "event_ids": []}

    try:
        affected_events = select_distinct_event_loc_ids(
            areas_path,
            loc_id,
            exact=not include_children,
            limit=100,
        )
        if not affected_events:
            areas_df = pd.read_parquet(areas_path)
            if include_children:
                mask = areas_df['affected_loc_id'].str.startswith(loc_id, na=False)
            else:
                mask = areas_df['affected_loc_id'] == loc_id
            affected_events = areas_df[mask]['event_loc_id'].unique()

        return {
            "count": len(affected_events),
            "event_ids": list(affected_events[:100])  # Limit sample
        }
    except Exception:
        return {"count": 0, "event_ids": []}
