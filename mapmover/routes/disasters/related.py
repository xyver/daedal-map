"""Cross-disaster relationship endpoints."""

import pandas as pd
from fastapi import APIRouter

from mapmover import logger
from mapmover.duckdb_helpers import parquet_available
from mapmover.paths import GLOBAL_DIR

from .helpers import msgpack_error, msgpack_response


router = APIRouter()


def _extract_event_type(loc_id: str) -> str:
    parts = loc_id.split("-")
    if len(parts) < 2:
        return "unknown"

    type_code = parts[-2] if len(parts) >= 3 else parts[0]
    type_map = {
        "EQ": "earthquake",
        "TSUN": "tsunami",
        "VOLC": "volcano",
        "HRCN": "hurricane",
        "TORN": "tornado",
        "FIRE": "wildfire",
        "FLOOD": "flood",
        "LAND": "landslide",
    }
    return type_map.get(type_code, "unknown")


def _extract_event_id(loc_id: str) -> str:
    parts = loc_id.split("-")
    if len(parts) >= 3:
        for i, part in enumerate(parts):
            if part in ["EQ", "TSUN", "VOLC", "HRCN", "TORN", "FIRE", "FLOOD", "LAND"]:
                return "-".join(parts[i + 1 :])
    return parts[-1] if parts else loc_id


@router.get("/api/events/related/{loc_id:path}")
async def get_related_events(loc_id: str):
    """Get related disaster events for a given event loc_id."""
    try:
        links_path = GLOBAL_DIR / "disasters/links.parquet"
        if not parquet_available(links_path):
            return msgpack_response({"event_id": loc_id, "related": [], "message": "Links data not available"})

        from mapmover.duckdb_helpers import run_df, path_to_uri
        uri = path_to_uri(links_path)
        links_df = run_df("SELECT * FROM read_parquet(?)", [uri])

        children = links_df[links_df["parent_loc_id"] == loc_id].copy()
        children["direction"] = "triggered"
        children["related_loc_id"] = children["child_loc_id"]

        parents = links_df[links_df["child_loc_id"] == loc_id].copy()
        parents["direction"] = "triggered_by"
        parents["related_loc_id"] = parents["parent_loc_id"]

        related = pd.concat([children, parents], ignore_index=True)
        if len(related) == 0:
            return msgpack_response({"event_id": loc_id, "related": [], "count": 0})

        related_list = []
        for _, row in related.iterrows():
            related_loc_id = row["related_loc_id"]
            related_list.append(
                {
                    "loc_id": related_loc_id,
                    "event_id": _extract_event_id(related_loc_id),
                    "event_type": _extract_event_type(related_loc_id),
                    "link_type": row["link_type"],
                    "direction": row["direction"],
                    "source": row["source"],
                    "confidence": row["confidence"],
                }
            )

        type_counts = {}
        for item in related_list:
            event_type = item["event_type"]
            type_counts[event_type] = type_counts.get(event_type, 0) + 1

        return msgpack_response({"event_id": loc_id, "related": related_list, "count": len(related_list), "by_type": type_counts})
    except Exception as e:
        logger.error(f"Error fetching related events for {loc_id}: {e}")
        return msgpack_error(str(e), 500)
