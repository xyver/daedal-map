"""Local Mapping adapter for installed, validated Network packs.

The optional Network package owns these routes and its Mapping UI asset. The
base Mapping runtime only registers this router when the package is installed.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from .network_pack import Pack


router = APIRouter(prefix="/api/local/network", tags=["local-network"])
_PACK_ID = re.compile(r"[A-Za-z0-9_-]+\Z")
_MAX_VISIBLE_FEATURES = 20_000
_STATIC_DIR = Path(__file__).resolve().parent / "static"


@router.get("/status")
def network_status() -> dict[str, bool]:
    status = {"auto_start": os.getenv("DAEDALMAP_NETWORK_AUTO_START", "1") == "1"}
    if os.getenv("DAEDALMAP_LOCAL_SOURCE_MODE", "") == "1":
        status["source_mode"] = True
    return status


def pack_root(data_root: Path) -> Path:
    """Use the Mapping library by default; allow an explicit source-run fixture."""
    override = os.getenv("NETWORK_PACKS_DIR", "").strip()
    return Path(override).expanduser() if override else data_root / "networks" / "packs"


def _root() -> Path:
    override = os.getenv("NETWORK_PACKS_DIR", "").strip()
    if override:
        return Path(override).expanduser()
    from mapmover.paths import DATA_ROOT

    return pack_root(DATA_ROOT)


def _open_pack(pack_id: str) -> Pack:
    if not _PACK_ID.fullmatch(pack_id):
        raise HTTPException(status_code=422, detail="Invalid network pack ID")
    directory = _root() / pack_id
    if not directory.is_dir():
        raise HTTPException(status_code=404, detail="Network pack was not found")
    try:
        pack = Pack.open(directory)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=f"Network pack failed validation: {exc}") from exc
    if pack.manifest.get("pack_id") != pack_id:
        raise HTTPException(status_code=422, detail="Pack ID differs from its folder")
    return pack


def _bounds(geometry: dict[str, Any]) -> tuple[float, float, float, float]:
    """Bounds of GeoJSON point/line/polygon coordinates without a GIS runtime."""
    points: list[tuple[float, float]] = []
    stack = [geometry.get("coordinates", [])]
    while stack:
        value = stack.pop()
        if not isinstance(value, (list, tuple)) or not value:
            continue
        if len(value) >= 2 and isinstance(value[0], (int, float)) and isinstance(value[1], (int, float)):
            points.append((float(value[0]), float(value[1])))
        else:
            stack.extend(value)
    if not points:
        raise ValueError("Feature has no coordinates")
    return (min(x for x, _ in points), min(y for _, y in points),
            max(x for x, _ in points), max(y for _, y in points))


def _intersects_bounds(feature: dict[str, Any], bbox: tuple[float, float, float, float]) -> bool:
    west, south, east, north = _bounds(feature["geometry"])
    return west <= bbox[2] and east >= bbox[0] and south <= bbox[3] and north >= bbox[1]


def _parse_bbox(value: str) -> tuple[float, float, float, float]:
    try:
        bbox = tuple(float(part) for part in value.split(","))
        if len(bbox) != 4 or not (-180 <= bbox[0] < bbox[2] <= 180) or not (-90 <= bbox[1] < bbox[3] <= 90):
            raise ValueError()
        return bbox
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid network bbox") from exc


@router.get("/ui.js", include_in_schema=False)
def mapping_ui() -> FileResponse:
    return FileResponse(_STATIC_DIR / "mapping_extension.js", media_type="application/javascript")


@router.get("/packs")
def list_packs() -> dict[str, Any]:
    root = _root()
    packs = []
    for directory in sorted(root.iterdir()) if root.is_dir() else []:
        if not directory.is_dir() or not _PACK_ID.fullmatch(directory.name):
            continue
        try:
            pack = _open_pack(directory.name)
        except HTTPException:
            continue
        packs.append({key: pack.manifest[key] for key in
                      ("pack_id", "version", "network_type", "bbox", "capabilities", "source")})
    return {"packs": packs}


@router.get("/packs/{pack_id}/features")
def pack_features(pack_id: str, bbox: str | None = None) -> dict[str, Any]:
    pack = _open_pack(pack_id)
    view = _parse_bbox(bbox) if bbox else None
    features = [feature for feature in pack.features if view is None or _intersects_bounds(feature, view)]
    nodes = [{"type": "Feature", "geometry": {"type": "Point", "coordinates": coordinate},
              "properties": {"node_id": node_id, "feature_kind": "node"}}
             for node_id, coordinate in pack.nodes.items()
             if view is None or (view[0] <= coordinate[0] <= view[2] and view[1] <= coordinate[1] <= view[3])]
    if len(features) + len(nodes) > _MAX_VISIBLE_FEATURES:
        raise HTTPException(status_code=413, detail="Too many network features in view; zoom in")
    return {"type": "FeatureCollection", "features": features + nodes,
            "pack_id": pack_id, "version": pack.manifest["version"],
            "network_type": pack.manifest["network_type"]}


@router.get("/packs/{pack_id}/feature")
def pack_feature(pack_id: str, feature_id: str) -> dict[str, Any]:
    pack = _open_pack(pack_id)
    feature = next((item for item in pack.features if item["properties"]["feature_id"] == feature_id), None)
    if feature is None:
        raise HTTPException(status_code=404, detail="Network feature was not found")
    return {"feature": feature, "links": [item for item in pack.links if item["feature_id"] == feature_id]}


@router.get("/packs/{pack_id}/node")
def pack_node(pack_id: str, node_id: str) -> dict[str, Any]:
    pack = _open_pack(pack_id)
    if node_id not in pack.nodes:
        raise HTTPException(status_code=404, detail="Network node was not found")
    return {"node_id": node_id, "coordinate": pack.nodes[node_id],
            "links": [item for item in pack.links if node_id in (item["from"], item["to"])]}
