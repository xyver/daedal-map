"""Hurricane and tropical storm disaster endpoints."""

from fastapi import APIRouter

from mapmover.disaster_filters import apply_location_filters, get_default_min_year
from mapmover.logging_analytics import logger
from mapmover.paths import GLOBAL_DIR

from .helpers import filter_by_time_range, msgpack_error, msgpack_response


router = APIRouter()


@router.get("/api/storms/geojson")
async def get_storms_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_year: int = None,
    basin: str = None,
    min_category: str = None,
    loc_prefix: str = None,
    affected_loc_id: str = None,
):
    """Get tropical storms as GeoJSON points for map display."""
    import pandas as pd

    if min_year is None:
        min_year = get_default_min_year("hurricanes", fallback=1950)

    try:
        storms_path = GLOBAL_DIR / "disasters/hurricanes/storms.parquet"
        positions_path = GLOBAL_DIR / "disasters/hurricanes/positions.parquet"

        if not storms_path.exists():
            return msgpack_error("Storm data not available", 404)

        storms_df = pd.read_parquet(storms_path)
        positions_df = pd.read_parquet(positions_path)

        if year is not None:
            storms_df = storms_df[storms_df["year"] == year]
        elif start is not None or end is not None:
            storms_df = filter_by_time_range(storms_df, start, end, time_col="start_date")
        elif min_year is not None:
            storms_df = storms_df[storms_df["year"] >= min_year]

        if basin is not None:
            storms_df = storms_df[storms_df["basin"] == basin.upper()]

        if min_category is not None:
            cat_order = {"TD": 0, "TS": 1, "Cat1": 2, "Cat2": 3, "Cat3": 4, "Cat4": 5, "Cat5": 6}
            min_cat_val = cat_order.get(min_category, 0)
            storms_df = storms_df[storms_df["max_category"].map(lambda x: cat_order.get(x, 0) >= min_cat_val)]

        storms_df = apply_location_filters(
            storms_df,
            "hurricanes",
            loc_prefix=loc_prefix,
            affected_loc_id=affected_loc_id,
            event_id_col="loc_id",
            loc_id_col="loc_id",
        )

        storm_ids = storms_df["storm_id"].tolist()
        positions_subset = positions_df[positions_df["storm_id"].isin(storm_ids)].copy()
        positions_subset = positions_subset.dropna(subset=["latitude", "longitude"])
        if positions_subset.empty:
            return msgpack_response({"type": "FeatureCollection", "features": [], "count": 0})

        # If a storm has all-NaN wind values, fall back to the first valid position.
        positions_subset["wind_sort"] = positions_subset["wind_kt"].fillna(-1)
        max_positions = positions_subset.loc[positions_subset.groupby("storm_id")["wind_sort"].idxmax()]
        storms_with_pos = storms_df.merge(
            max_positions[["storm_id", "latitude", "longitude"]],
            on="storm_id",
            how="inner",
            suffixes=("", "_pos"),
        )

        valid_mask = storms_with_pos["latitude"].notna() & storms_with_pos["longitude"].notna()
        records = storms_with_pos[valid_mask].to_dict("records")

        features = []
        for storm in records:
            features.append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(storm["longitude"]), float(storm["latitude"])],
                    },
                    "properties": {
                        "storm_id": storm["storm_id"],
                        "name": storm.get("name") if pd.notna(storm.get("name")) else None,
                        "year": int(storm["year"]),
                        "basin": storm["basin"],
                        "max_wind_kt": int(storm["max_wind_kt"]) if pd.notna(storm.get("max_wind_kt")) else None,
                        "min_pressure_mb": int(storm["min_pressure_mb"]) if pd.notna(storm.get("min_pressure_mb")) else None,
                        "max_category": storm["max_category"],
                        "num_positions": int(storm["num_positions"]),
                        "start_date": str(storm["start_date"]) if pd.notna(storm.get("start_date")) else None,
                        "end_date": str(storm["end_date"]) if pd.notna(storm.get("end_date")) else None,
                        "made_landfall": bool(storm.get("made_landfall", False)),
                        "latitude": float(storm["latitude"]),
                        "longitude": float(storm["longitude"]),
                    },
                }
            )

        logger.info(f"Returning {len(features)} storms for year={year}, min_year={min_year}, basin={basin}")

        return msgpack_response({"type": "FeatureCollection", "features": features, "count": len(features)})
    except Exception as e:
        logger.error(f"Error fetching storms GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/storms/{storm_id}/track")
async def get_storm_track(storm_id: str):
    """Get full track positions for a specific storm."""
    import pandas as pd

    try:
        positions_path = GLOBAL_DIR / "disasters/hurricanes/positions.parquet"
        storms_path = GLOBAL_DIR / "disasters/hurricanes/storms.parquet"

        if not positions_path.exists():
            return msgpack_error("Storm data not available", 404)

        positions_df = pd.read_parquet(positions_path)
        storm_positions = positions_df[positions_df["storm_id"] == storm_id].sort_values("timestamp")
        if len(storm_positions) == 0:
            return msgpack_error(f"Storm {storm_id} not found", 404)

        storms_df = pd.read_parquet(storms_path)
        storm_meta = storms_df[storms_df["storm_id"] == storm_id]
        storm_name = storm_meta.iloc[0]["name"] if len(storm_meta) > 0 and pd.notna(storm_meta.iloc[0]["name"]) else storm_id

        positions = []
        for _, pos in storm_positions.iterrows():
            positions.append(
                {
                    "timestamp": str(pos["timestamp"]) if pd.notna(pos["timestamp"]) else None,
                    "latitude": float(pos["latitude"]),
                    "longitude": float(pos["longitude"]),
                    "wind_kt": int(pos["wind_kt"]) if pd.notna(pos["wind_kt"]) else None,
                    "pressure_mb": int(pos["pressure_mb"]) if pd.notna(pos["pressure_mb"]) else None,
                    "category": pos["category"],
                    "status": pos.get("status") if pd.notna(pos.get("status")) else None,
                    "r34_ne": int(pos["r34_ne"]) if pd.notna(pos.get("r34_ne")) else None,
                    "r34_se": int(pos["r34_se"]) if pd.notna(pos.get("r34_se")) else None,
                    "r34_sw": int(pos["r34_sw"]) if pd.notna(pos.get("r34_sw")) else None,
                    "r34_nw": int(pos["r34_nw"]) if pd.notna(pos.get("r34_nw")) else None,
                    "r50_ne": int(pos["r50_ne"]) if pd.notna(pos.get("r50_ne")) else None,
                    "r50_se": int(pos["r50_se"]) if pd.notna(pos.get("r50_se")) else None,
                    "r50_sw": int(pos["r50_sw"]) if pd.notna(pos.get("r50_sw")) else None,
                    "r50_nw": int(pos["r50_nw"]) if pd.notna(pos.get("r50_nw")) else None,
                    "r64_ne": int(pos["r64_ne"]) if pd.notna(pos.get("r64_ne")) else None,
                    "r64_se": int(pos["r64_se"]) if pd.notna(pos.get("r64_se")) else None,
                    "r64_sw": int(pos["r64_sw"]) if pd.notna(pos.get("r64_sw")) else None,
                    "r64_nw": int(pos["r64_nw"]) if pd.notna(pos.get("r64_nw")) else None,
                }
            )

        return msgpack_response({"storm_id": storm_id, "name": storm_name, "positions": positions, "count": len(positions)})
    except Exception as e:
        logger.error(f"Error fetching storm track: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/storms/tracks/geojson")
async def get_storm_tracks_geojson(
    year: int = None,
    start: str = None,
    end: str = None,
    min_year: int = None,
    basin: str = None,
    min_category: str = None,
):
    """Get storm tracks as GeoJSON LineStrings for yearly overview display."""
    import pandas as pd

    if min_year is None:
        min_year = get_default_min_year("hurricanes", fallback=1950)

    try:
        storms_path = GLOBAL_DIR / "disasters/hurricanes/storms.parquet"
        positions_path = GLOBAL_DIR / "disasters/hurricanes/positions.parquet"

        if not storms_path.exists():
            return msgpack_error("Storm data not available", 404)

        storms_df = pd.read_parquet(storms_path)
        positions_df = pd.read_parquet(positions_path)

        if year is not None:
            storms_df = storms_df[storms_df["year"] == year]
        elif start is not None or end is not None:
            storms_df = filter_by_time_range(storms_df, start, end, time_col="start_date")
        elif min_year is not None:
            storms_df = storms_df[storms_df["year"] >= min_year]

        if basin is not None:
            storms_df = storms_df[storms_df["basin"] == basin.upper()]

        if min_category is not None:
            cat_order = {"TD": 0, "TS": 1, "Cat1": 2, "Cat2": 3, "Cat3": 4, "Cat4": 5, "Cat5": 6}
            min_cat_val = cat_order.get(min_category, 0)
            storms_df["cat_val"] = storms_df["max_category"].map(lambda x: cat_order.get(x, 0))
            storms_df = storms_df[storms_df["cat_val"] >= min_cat_val]
            storms_df = storms_df.drop(columns=["cat_val"])

        storms_df = storms_df.set_index("storm_id")
        storm_ids_set = set(storms_df.index.tolist())

        positions_subset = positions_df[positions_df["storm_id"].isin(storm_ids_set)].copy()
        positions_subset = positions_subset.dropna(subset=["latitude", "longitude"])
        positions_subset = positions_subset.sort_values(["storm_id", "timestamp"])

        coords_by_storm = {}
        for storm_id, group in positions_subset.groupby("storm_id"):
            coords = list(zip(group["longitude"].tolist(), group["latitude"].tolist()))
            if len(coords) >= 2:
                coords_by_storm[storm_id] = [[float(lon), float(lat)] for lon, lat in coords]

        features = []
        for storm_id, coords in coords_by_storm.items():
            storm = storms_df.loc[storm_id]
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "LineString", "coordinates": coords},
                    "properties": {
                        "storm_id": storm_id,
                        "name": storm.get("name") if pd.notna(storm.get("name")) else None,
                        "year": int(storm["year"]),
                        "basin": storm["basin"],
                        "max_wind_kt": int(storm["max_wind_kt"]) if pd.notna(storm["max_wind_kt"]) else None,
                        "min_pressure_mb": int(storm["min_pressure_mb"]) if pd.notna(storm["min_pressure_mb"]) else None,
                        "max_category": storm["max_category"],
                        "num_positions": int(storm["num_positions"]),
                        "start_date": str(storm["start_date"]) if pd.notna(storm.get("start_date")) else None,
                        "end_date": str(storm["end_date"]) if pd.notna(storm.get("end_date")) else None,
                        "made_landfall": bool(storm.get("made_landfall", False)),
                    },
                }
            )

        logger.info(
            f"Returning {len(features)} storm tracks for year={year}, min_year={min_year}, basin={basin}, min_category={min_category}"
        )
        return msgpack_response({"type": "FeatureCollection", "features": features, "count": len(features)})
    except Exception as e:
        logger.error(f"Error fetching storm tracks GeoJSON: {e}")
        return msgpack_error(str(e), 500)


@router.get("/api/storms/list")
async def get_storms_list(year: int = None, min_year: int = None, basin: str = None, limit: int = 100):
    """Get list of storms with metadata for filtering/selection."""
    import pandas as pd

    try:
        if min_year is None:
            min_year = get_default_min_year("hurricanes", fallback=1950)

        storms_path = GLOBAL_DIR / "disasters/hurricanes/storms.parquet"
        if not storms_path.exists():
            return msgpack_error("Storm data not available", 404)

        storms_df = pd.read_parquet(storms_path)

        if year is not None:
            storms_df = storms_df[storms_df["year"] == year]
        elif min_year is not None:
            storms_df = storms_df[storms_df["year"] >= min_year]

        if basin is not None:
            storms_df = storms_df[storms_df["basin"] == basin.upper()]

        storms_df = storms_df.sort_values("max_wind_kt", ascending=False)
        if limit is not None and limit > 0:
            storms_df = storms_df.head(limit)

        storms = []
        for _, storm in storms_df.iterrows():
            storms.append(
                {
                    "storm_id": storm["storm_id"],
                    "name": storm.get("name") if pd.notna(storm.get("name")) else None,
                    "year": int(storm["year"]),
                    "basin": storm["basin"],
                    "max_wind_kt": int(storm["max_wind_kt"]) if pd.notna(storm.get("max_wind_kt")) else None,
                    "max_category": storm["max_category"],
                    "start_date": str(storm["start_date"]) if pd.notna(storm.get("start_date")) else None,
                }
            )

        return msgpack_response({"storms": storms, "count": len(storms)})
    except Exception as e:
        logger.error(f"Error fetching storms list: {e}")
        return msgpack_error(str(e), 500)
