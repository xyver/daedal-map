"""Offline, package-scoped Geometry reader for extracted download ZIPs."""
from __future__ import annotations

import json
import hashlib
import math
import re
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pyarrow as pa
from shapely import from_wkb, from_wkt
from shapely.geometry import Point, mapping, shape


def _shape(value):
    if isinstance(value, str):
        return shape(json.loads(value)) if value.lstrip().startswith('{') else from_wkt(value)
    return from_wkb(value)


TOOLS = {
    "read_geometry_catalog", "loc_id_info", "get_geometry",
    "resolve_point", "create_geometry_export", "get_tool_help",
}
MAX_INLINE_GEOMETRY_BYTES = 16 * 1024 * 1024


def _check_inline_geometry_budget(rows):
    for row in rows:
        value = row.get("geometry")
        if value is None:
            continue
        size = len(value.encode("utf-8") if isinstance(value, str) else value)
        if size > MAX_INLINE_GEOMETRY_BYTES:
            raise ValueError(
                "geometry_too_large_for_inline: "
                f"{row['loc_id']} has {size} geometry bytes; use the extracted Parquet file"
            )
TEXT = {"type": "string", "minLength": 1}
SCHEMAS = {
    "read_geometry_catalog": ({"country": TEXT, "view": {"type": "string", "enum": ["capabilities", "packages", "files"]}}, []),
    "loc_id_info": ({"loc_id": TEXT}, ["loc_id"]),
    "get_geometry": ({"loc_id": TEXT, "include_polygon": {"type": "boolean"}}, ["loc_id"]),
    "resolve_point": ({"lat": {"type": "number", "minimum": -90, "maximum": 90}, "lon": {"type": "number", "minimum": -180, "maximum": 180}, "country": TEXT, "admin_level": {"type": "integer", "minimum": 0, "maximum": 20}}, ["lat", "lon"]),
    "create_geometry_export": ({"loc_ids": {"type": "array", "items": TEXT, "minItems": 1, "maxItems": 10000}, "country": TEXT, "admin_level": {"type": "integer", "minimum": 0, "maximum": 20}, "max_features": {"type": "integer", "minimum": 1, "maximum": 10000}}, []),
    "get_tool_help": ({"tool_name": TEXT}, ["tool_name"]),
}
DESCRIPTIONS = {
    "read_geometry_catalog": "Discover only installed geometry data packages, tiers, files and source licences.",
    "loc_id_info": "Look up an exact local reference identity by loc_id.",
    "get_geometry": "Check local polygon availability and return location metadata; optionally include a GeoJSON polygon. Lite is reference-only.",
    "resolve_point": "Find installed polygons covering a WGS84 point, including boundaries. Requires Display or Detail; returns only installed coverage.",
    "create_geometry_export": "Return a bounded GeoJSON FeatureCollection for explicit loc_ids or country/admin_level. Save this result locally; no cloud job or server file is created. Display shapes remain labelled simplified.",
    "get_tool_help": "Get the exact schema and limitations of an implemented local operation.",
}


def tool_definitions():
    return [{"name": name, "description": DESCRIPTIONS[name], "inputSchema": {
        "type": "object", "properties": SCHEMAS[name][0], "required": SCHEMAS[name][1], "additionalProperties": False}} for name in sorted(TOOLS)]


class DownloadedGeometryBackend:
    def __init__(self, data_root: str | Path):
        self.root = Path(data_root).expanduser().resolve()
        self.packages = []
        self.rows = []
        self.verified = {}
        self.schemas = {}
        for sidecar in sorted(self.root.rglob("geometry.json")):
            payload = json.loads(sidecar.read_text(encoding="utf-8"))
            sidecar.resolve().relative_to(self.root)
            if not isinstance(payload, dict) or payload.get("profile") != "downloadable_geometry_package" or payload.get("package_tier") not in {"lite", "display", "detail"} or not isinstance(payload.get("files"), list):
                raise ValueError(f"Invalid Geometry package sidecar: {sidecar}")
            payload['country_code'] = payload.get('country_code') or payload.get('release_unit_id')
            files = []
            seen = set()
            for item in payload["files"]:
                archive = str(item.get("archive_path") or "").replace("\\", "/")
                local = (sidecar.parent / archive).resolve()
                if not archive or archive.startswith("/") or ".." in Path(archive).parts or ":" in archive or "\x00" in archive:
                    raise ValueError(f"Unsafe package member path: {archive}")
                if archive in seen: raise ValueError(f"Duplicate package member: {archive}")
                seen.add(archive)
                local.relative_to(sidecar.parent.resolve())
                local.relative_to(self.root)
                if not local.is_file():
                    raise ValueError(f"Package member missing: {local}")
                if not re.fullmatch(r"[0-9a-f]{64}", str(item.get("sha256", ""))) or type(item.get("size_bytes")) is not int:
                    raise ValueError(f"Missing hash/size pin: {archive}")
                if local.stat().st_size != item["size_bytes"]:
                    raise ValueError(f"Package member size mismatch: {archive}")
                files.append({**item, "local_path": local, "archive_path": archive})
            self.packages.append({"sidecar": sidecar, "metadata": payload, "files": files})
        if not self.packages:
            raise ValueError(f"No extracted Geometry package sidecars under {self.root}")

    def catalog(self, country=None) -> dict[str, Any]:
        packages = [p for p in self.packages if not country or p['metadata'].get('country_code') in {country, 'GLOBAL'}]
        if not packages: raise ValueError(f"No installed package for country {country}")
        return {"profile": "downloaded_geometry_local", "scoped": True,
                "packages": [{k: p["metadata"].get(k) for k in ("package_id", "country_code", "version", "package_tier", "components", "source_licenses")} for p in packages],
                "files": [{"package_id": p["metadata"].get("package_id"), "archive_path": f["archive_path"], "component": f.get("component"), "size_bytes": f.get("size_bytes")} for p in packages for f in p["files"]]}

    def _records(self):
        for package in self.packages:
            for file in package["files"]:
                if str(file["archive_path"]).lower().endswith(".parquet"):
                    yield package, file

    def _load(self, *, geometry: bool = False, loc_ids=None, country=None, admin_level=None, point=None):
        wanted = {"loc_id", "parent_id", "admin_level", "type", "name", "iso_a3", "country_iso3", "centroid_lon", "centroid_lat", "has_polygon", "bbox_min_lon", "bbox_min_lat", "bbox_max_lon", "bbox_max_lat"}
        if geometry: wanted.update({"geometry", "geometry_wkb"})
        rows = []
        for package, file in self._records():
            meta = package["metadata"]
            if country and meta.get("country_code") not in {country, "GLOBAL"}: continue
            if file.get("component") not in ({"exact", "display"} if geometry else {"exact", "display", "reference"}): continue
            path = file["local_path"]
            stat = path.stat()
            state = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
            if self.verified.get(path) != state:
                digest = hashlib.sha256()
                with path.open("rb") as stream:
                    for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""): digest.update(chunk)
                if state[0] != file["size_bytes"] or digest.hexdigest() != file["sha256"]:
                    raise ValueError(
                        f"Package member hash/size mismatch: {file['archive_path']} "
                        f"expected_size={file['size_bytes']} actual_size={state[0]} "
                        f"expected_sha256={file['sha256']} actual_sha256={digest.hexdigest()}"
                    )
                after = path.stat()
                if state != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    raise ValueError(f"Package member changed during verification: {path}")
                self.verified[path] = state
                self.schemas[path] = pq.read_schema(path)
            schema = self.schemas[path]
            columns = [c for c in wanted if c in schema.names]
            if "loc_id" not in columns or (geometry and not {"geometry", "geometry_wkb"}.intersection(columns)): continue
            if pa.types.is_null(schema.field('loc_id').type): continue
            filters = []
            if loc_ids: filters.append(("loc_id", "in", loc_ids))
            if country and "iso_a3" in columns and not pa.types.is_null(schema.field('iso_a3').type): filters.append(("iso_a3", "=", country))
            elif country and "country_iso3" in columns and not pa.types.is_null(schema.field('country_iso3').type): filters.append(("country_iso3", "=", country))
            if admin_level is not None:
                if admin_level == 0 and country:
                    # Retained Admin0 partitions may omit the level column or
                    # store NULL for the country row. The ISO3 loc_id is the
                    # exact, bounded Admin0 selector in either case.
                    filters.append(("loc_id", "=", country))
                elif "admin_level" in columns and not pa.types.is_null(schema.field('admin_level').type):
                    filters.append(("admin_level", "=", admin_level))
                else:
                    continue
            if point and all(c in columns for c in ("bbox_min_lon", "bbox_min_lat", "bbox_max_lon", "bbox_max_lat")):
                filters.extend([("bbox_min_lon", "<=", point.x), ("bbox_max_lon", ">=", point.x), ("bbox_min_lat", "<=", point.y), ("bbox_max_lat", ">=", point.y)])
            table = pq.read_table(path, columns=columns, filters=filters or None)
            for row in table.to_pylist():
                if row.get("geometry") is None and row.get("geometry_wkb") is not None:
                    row["geometry"] = row.pop("geometry_wkb")
                if admin_level == 0 and row.get("admin_level") is None and row.get("loc_id") == country:
                    row["admin_level"] = 0
                row["_package_id"] = package["metadata"].get("package_id")
                row["_tier"] = package["metadata"].get("package_tier")
                row["country"] = row.get("iso_a3") or row.get('country_iso3') or (meta.get("country_code") if meta.get("country_code") != "GLOBAL" else str(row["loc_id"]).split("-")[0])
                row["polygon_kind"] = {"exact": "exact", "display": "simplified"}.get(file.get("component"))
                if geometry and row.get("geometry") is None: continue
                rows.append(row)
                if len(rows) > 100000:
                    raise ValueError("Selection exceeds local tool limit; narrow country/admin_level or loc_ids")
        return rows

    @staticmethod
    def _dedupe(rows):
        chosen = {}
        rank = {"exact": 2, "simplified": 1, None: 0}
        for row in rows:
            old = chosen.get(row["loc_id"])
            if old is None or rank[row["polygon_kind"]] > rank[old["polygon_kind"]]: chosen[row["loc_id"]] = row
        return sorted(chosen.values(), key=lambda r: (r.get("admin_level") or 0, r["loc_id"]))

    def call(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        if name not in TOOLS: raise ValueError(f"Unsupported downloaded Geometry tool: {name}")
        if not isinstance(args, dict) or set(args) - set(SCHEMAS[name][0]): raise ValueError(f"Unsupported arguments for {name}; use get_tool_help")
        if any(key not in args for key in SCHEMAS[name][1]): raise ValueError(f"Missing required arguments for {name}")
        if 'country' in args and (not isinstance(args['country'], str) or not re.fullmatch(r'[A-Z]{3}', args['country'])): raise ValueError("country must be uppercase ISO3")
        if name == "read_geometry_catalog": return self.catalog(args.get('country'))
        if name == "get_tool_help":
            definition = next((t for t in tool_definitions() if t['name'] == args['tool_name']), None)
            if definition is None: raise ValueError("Unsupported local tool")
            return definition
        if name == "resolve_point":
            lat, lon = args.get("lat"), args.get("lon")
            if type(lat) not in (int, float) or type(lon) not in (int, float) or not math.isfinite(lat) or not math.isfinite(lon) or not -90 <= lat <= 90 or not -180 <= lon <= 180:
                raise ValueError("Invalid longitude/latitude")
            level = args.get("admin_level")
            if level is not None and (type(level) is not int or not 0 <= level <= 20):
                raise ValueError("Invalid admin_level")
            if not any(p['metadata'].get('package_tier') in {'detail', 'display'} for p in self.packages):
                raise ValueError("Point resolution requires Display or Detail; Lite is reference-only")
            point = Point(lon, lat)
            rows = self._load(geometry=True, country=args.get("country"), admin_level=level, point=point)
            matches = self._dedupe([r for r in rows if _shape(r["geometry"]).covers(point)])
            return {"lat": lat, "lon": lon, "matches": [self._public(r, include_polygon=False) for r in matches], "scoped": True}
        if name == "create_geometry_export":
            ids, country = args.get("loc_ids"), args.get("country")
            if not ids and not country: raise ValueError("Export requires explicit loc_ids or country")
            if ids is not None and (not isinstance(ids, list) or not 1 <= len(ids) <= 10000 or any(not isinstance(i, str) or not i for i in ids)): raise ValueError("Invalid export loc_ids")
            level, maximum = args.get("admin_level"), args.get("max_features", 10000)
            if level is not None and (type(level) is not int or not 0 <= level <= 20): raise ValueError("Invalid admin_level")
            if type(maximum) is not int or not 1 <= maximum <= 10000: raise ValueError("Invalid max_features")
            rows = self._dedupe(self._load(geometry=True, loc_ids=ids, country=country, admin_level=level))
            missing = set(ids or []) - {r['loc_id'] for r in rows}
            if missing or not rows: raise ValueError(f"Polygon data unavailable for selection: {sorted(missing)}; install Display or Detail")
            if len(rows) > maximum: raise ValueError("Export exceeds max_features; narrow selection")
            _check_inline_geometry_budget(rows)
            return {"type": "FeatureCollection", "features": [{"type": "Feature", "id": r['loc_id'], "properties": self._public(r, include_polygon=False), "geometry": mapping(_shape(r['geometry']))} for r in rows], "source_licenses": [p['metadata'].get('source_licenses', []) for p in self.packages]}
        loc_id = args.get("loc_id")
        if not isinstance(loc_id, str) or not loc_id: raise ValueError("loc_id is required")
        prefix = loc_id.split('-')[0]
        country = prefix if re.fullmatch(r'[A-Z]{3}', prefix) and any(p['metadata'].get('country_code') in {prefix, 'GLOBAL'} for p in self.packages) else None
        rows = self._dedupe(self._load(loc_ids=[loc_id], country=country))
        if not rows: raise ValueError(f"Location {loc_id} is absent from installed packages")
        if name == "loc_id_info": return {"loc_id": loc_id, "country": rows[0]['country'], "matches": rows}
        if name == "get_geometry":
            include = args.get("include_polygon", False)
            if type(include) is not bool: raise ValueError("include_polygon must be boolean")
            metadata_shapes = [
                row for row in rows
                if row.get("polygon_kind") in {"exact", "simplified"}
                and bool(row.get("has_polygon", True))
            ]
            polygons = (
                self._dedupe(self._load(geometry=True, loc_ids=[loc_id], country=country))
                if include else []
            )
            if include and not polygons: raise ValueError("Polygon unavailable; Lite is reference-only")
            if include: _check_inline_geometry_budget(polygons)
            matches = polygons if include else rows
            return {
                "loc_id": args.get("loc_id"),
                "available": bool(polygons if include else metadata_shapes),
                "polygon_kind": (
                    (polygons if include else metadata_shapes)[0]["polygon_kind"]
                    if (polygons if include else metadata_shapes) else None
                ),
                "matches": [self._public(r, include_polygon=include) for r in matches],
            }

    @staticmethod
    def _public(row, *, include_polygon):
        out = {k: v for k, v in row.items() if k != "geometry"}
        if include_polygon and row.get("geometry") is not None: out["geometry"] = mapping(_shape(row["geometry"]))
        return out
