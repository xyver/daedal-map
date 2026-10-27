"""Standalone, non-map Network table editor. Source packs remain immutable."""

from __future__ import annotations

import json
import os
import re
import threading
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .cli import packs_root
from .app.network_pack import Pack

app = FastAPI(title="Network spreadsheet editor")
_lock = threading.RLock()
_safe_id = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_tables = {"nodes", "links", "features"}
_editable = {
    "nodes": {"longitude", "latitude"},
    "links": {"from_node", "to_node", "directed", "distance_km", "attributes_json"},
    "features": {"properties_json"},
}
_schema = pa.schema([("table", pa.string()), ("row_id", pa.string()),
                     ("field", pa.string()), ("value_json", pa.string())])


def _draft_path(pack_id: str) -> Path:
    root = os.getenv("NETWORK_EDITOR_DRAFTS_DIR")
    if not root:
        root = str(Path(os.getenv("DATA_ROOT", str(Path.home() / "DaedalMap"))) / "networks" / "editor_drafts")
    return Path(root) / pack_id / "edits.parquet"


def _pack(pack_id: str) -> Pack:
    if not _safe_id.fullmatch(pack_id):
        raise HTTPException(400, "Invalid pack ID")
    path = packs_root() / pack_id
    if not (path / "manifest.json").is_file():
        raise HTTPException(404, "Network pack not found")
    try:
        return Pack.open(path)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise HTTPException(422, f"Pack validation failed: {exc}") from exc


def _base_rows(pack: Pack, table: str) -> list[dict]:
    if table == "nodes":
        return [{"node_id": key, "longitude": xy[0], "latitude": xy[1]}
                for key, xy in sorted(pack.nodes.items())]
    if table == "links":
        return [{"link_id": link["id"], "feature_id": link["feature_id"],
                 "from_node": link["from"], "to_node": link["to"],
                 "directed": link["directed"], "distance_km": link.get("distance_km"),
                 "attributes_json": json.dumps({k: v for k, v in link.items()
                                                if k not in {"id", "feature_id", "from", "to", "directed", "distance_km"}},
                                               sort_keys=True)} for link in pack.links]
    return [{"feature_id": f["properties"]["feature_id"],
             "geometry_type": f["geometry"]["type"],
             "properties_json": json.dumps(f["properties"], sort_keys=True)} for f in pack.features]


def _id_field(table: str) -> str:
    return {"nodes": "node_id", "links": "link_id", "features": "feature_id"}[table]


def _drafts(pack_id: str) -> dict[tuple[str, str, str], object]:
    path = _draft_path(pack_id)
    if not path.is_file():
        return {}
    return {(row["table"], row["row_id"], row["field"]): json.loads(row["value_json"])
            for row in pq.read_table(path, schema=_schema).to_pylist()}


def _save(pack_id: str, edits: dict[tuple[str, str, str], object]) -> None:
    path = _draft_path(pack_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"table": table, "row_id": row_id, "field": field,
             "value_json": json.dumps(value)} for (table, row_id, field), value in sorted(edits.items())]
    temporary = path.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=_schema), temporary, compression="zstd")
    temporary.replace(path)


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse((Path(__file__).parent / "spreadsheet_editor.html").read_text(encoding="utf-8"),
                        headers={"Cache-Control": "no-store"})


@app.get("/api/packs")
def list_packs() -> list[dict]:
    root = packs_root()
    found = []
    for path in sorted(root.iterdir()) if root.is_dir() else []:
        if (path / "manifest.json").is_file():
            try:
                pack = Pack.open(path)
                found.append({"pack_id": pack.manifest["pack_id"], "network_type": pack.manifest["network_type"],
                              "nodes": len(pack.nodes), "links": len(pack.links), "features": len(pack.features),
                              "draft_edits": len(_drafts(pack.manifest["pack_id"]))})
            except (OSError, ValueError, KeyError, TypeError):
                continue
    return found


@app.get("/api/packs/{pack_id}/{table}")
def rows(pack_id: str, table: str, offset: int = 0, limit: int = 100, q: str = "") -> dict:
    if table not in _tables:
        raise HTTPException(404, "Unknown table")
    if offset < 0 or not 1 <= limit <= 500:
        raise HTTPException(400, "Invalid page")
    pack = _pack(pack_id)
    with _lock:
        edits = _drafts(pack_id)
    base = _base_rows(pack, table)
    key = _id_field(table)
    for row in base:
        for field in _editable[table]:
            edit_key = (table, row[key], field)
            if edit_key in edits:
                row[field] = edits[edit_key]
    if q:
        needle = q.casefold()
        base = [row for row in base if any(needle in str(value).casefold() for value in row.values())]
    return {"rows": base[offset:offset + limit], "total": len(base), "offset": offset,
            "editable": sorted(_editable[table]), "draft_edits": len(edits)}


class CellEdit(BaseModel):
    value: object


@app.put("/api/packs/{pack_id}/{table}/{row_id}/{field}")
def edit_cell(pack_id: str, table: str, row_id: str, field: str, edit: CellEdit) -> dict:
    if table not in _tables or field not in _editable.get(table, set()):
        raise HTTPException(400, "This column cannot be edited")
    pack = _pack(pack_id)
    key = _id_field(table)
    base = next((r for r in _base_rows(pack, table) if r[key] == row_id), None)
    if base is None:
        raise HTTPException(404, "Row not found")
    value = edit.value
    if field in {"longitude", "latitude", "distance_km"}:
        try:
            value = float(value)
        except (TypeError, ValueError):
            raise HTTPException(422, "A number is required")
        import math
        if not math.isfinite(value) or (field == "longitude" and not -180 <= value <= 180) or (field == "latitude" and not -90 <= value <= 90) or (field == "distance_km" and value < 0):
            raise HTTPException(422, "Number is outside the valid range")
    elif field == "directed":
        if not isinstance(value, bool):
            raise HTTPException(422, "A true or false value is required")
    elif field in {"from_node", "to_node"}:
        if not isinstance(value, str) or value not in pack.nodes:
            raise HTTPException(422, "Endpoint must be an existing node ID")
    elif field in {"attributes_json", "properties_json"}:
        try:
            parsed = json.loads(value) if isinstance(value, str) else value
        except json.JSONDecodeError:
            raise HTTPException(422, "Valid JSON is required")
        if not isinstance(parsed, dict):
            raise HTTPException(422, "A JSON object is required")
        if field == "properties_json" and parsed.get("feature_id") != row_id:
            raise HTTPException(422, "feature_id cannot change")
        if field == "attributes_json" and any(k in parsed for k in ("id", "feature_id", "from", "to", "directed", "distance_km")):
            raise HTTPException(422, "Core link fields have their own columns")
        value = json.dumps(parsed, sort_keys=True)
    with _lock:
        edits = _drafts(pack_id)
        edit_key = (table, row_id, field)
        if value == base[field]:
            edits.pop(edit_key, None)
        else:
            edits[edit_key] = value
        _save(pack_id, edits)
    return {"value": value, "draft_edits": len(edits)}
