"""Offline, package-scoped Geometry reader for extracted download ZIPs."""
from __future__ import annotations

import json
import hashlib
import math
import re
import sys
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pyarrow as pa
from shapely import from_wkb, from_wkt
from shapely.geometry import Point, mapping, shape

_PACKAGE_ROOT = Path(__file__).resolve().parents[2]
for _runtime_root in (_PACKAGE_ROOT, _PACKAGE_ROOT / "runtime"):
    if (_runtime_root / "mcp_surface_shared.py").is_file() and str(_runtime_root) not in sys.path:
        sys.path.insert(0, str(_runtime_root))
from mcp_surface_shared import (MARINE_JURISDICTION_FAMILIES, MARINE_POINT_FAMILIES,
                                build_tool_definitions, public_geometry_family_id)


def _shape(value):
    if isinstance(value, str):
        return shape(json.loads(value)) if value.lstrip().startswith('{') else from_wkt(value)
    return from_wkb(value)


TOOLS = {
    "get_tool_help", "get_catalog", "get_pack", "get_geography",
    "resolve_point", "resolve_deep_point", "get_loc_id_info",
    "identify_dataset_geography", "identify_reference_system",
    "convert_reference", "compare_geographies", "get_shape",
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
def tool_definitions():
    """Use the same public machine contracts as the hosted Geography facade."""
    return [definition for definition in build_tool_definitions() if definition["name"] in TOOLS]


TOOL_DEFINITIONS = {item["name"]: item for item in tool_definitions()}


def _error(code: str, message: str, **details: Any) -> dict[str, Any]:
    return {"ok": False, "error": {"code": code, "message": message, "details": details}}


class DownloadedGeometryBackend:
    def __init__(self, data_root: str | Path):
        self.root = Path(data_root).expanduser().resolve()
        self.packages = []
        self.bridges = []
        self.rows = []
        self.verified = {}
        self.schemas = {}
        self.family_index = {}
        for sidecar in sorted(self.root.rglob("geometry.json")):
            payload = json.loads(sidecar.read_text(encoding="utf-8"))
            sidecar.resolve().relative_to(self.root)
            if (not isinstance(payload, dict)
                    or payload.get("profile") not in {"downloadable_geometry_package", "downloadable_external_reference_bridge"}
                    or payload.get("package_tier") not in {"lite", "display", "detail"}
                    or not isinstance(payload.get("files"), list)):
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
            package = {"sidecar": sidecar, "metadata": payload, "files": files}
            (self.bridges if payload["profile"] == "downloadable_external_reference_bridge" else self.packages).append(package)
        if not self.packages and not self.bridges:
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

    def _verify_file(self, file: dict[str, Any]) -> None:
        path = file["local_path"]
        stat = path.stat()
        state = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
        if self.verified.get(path) == state:
            return
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
                digest.update(chunk)
        if state[0] != file["size_bytes"] or digest.hexdigest() != file["sha256"]:
            raise ValueError(f"Package member hash/size mismatch: {file['archive_path']}")
        after = path.stat()
        if state != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError(f"Package member changed during verification: {path}")
        self.verified[path] = state
        self.schemas[path] = pq.read_schema(path)

    def _reference_rows(self, kind: str, *, country: str | None = None,
                        filters: list[tuple[str, str, Any]] | None = None,
                        limit: int = 10000) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for package, file in self._records():
            archive = file["archive_path"].replace("\\", "/")
            if file.get("component") != "reference" or (f"/partitions/{kind}/" not in archive and archive != f"reference/{kind}.parquet"):
                continue
            if country and package["metadata"].get("country_code") not in {country, "GLOBAL"}:
                continue
            self._verify_file(file)
            schema = self.schemas[file["local_path"]]
            if filters and any(item[0] not in schema.names for item in filters):
                continue
            local_filters = filters or None
            selected = pq.read_table(file["local_path"], filters=local_filters).to_pylist()
            for row in selected:
                row["_package_id"] = package["metadata"].get("package_id")
            rows.extend(selected)
            if len(rows) > limit:
                raise ValueError(f"Reference {kind} selection exceeds {limit} rows; narrow the request")
        return rows

    def _bridge_rows(self, *, scope: str, column: str, value: str) -> list[dict[str, Any]]:
        if column not in {"gers_division_id", "loc_id"}:
            raise ValueError("Unsupported Bridge lookup column")
        rows = []
        for package in self.bridges:
            if package["metadata"].get("external_system") != "overture_gers":
                continue
            for file in package["files"]:
                if file["archive_path"].replace("\\", "/") != "reference/overture_gers_loc_id_bridge.parquet":
                    continue
                self._verify_file(file)
                filters = [(column, "=", value)]
                if scope not in {"GLOBAL", "BRIDGES"}:
                    filters.append(("iso3", "=", scope))
                rows.extend(pq.read_table(file["local_path"], filters=filters).to_pylist())
        return rows

    def _load(self, *, geometry: bool = False, loc_ids=None, country=None, admin_level=None, point=None,
              selected_path: Path | None = None):
        wanted = {"loc_id", "parent_id", "admin_level", "type", "name", "iso_a3", "country_iso3", "family", "subtype", "level", "centroid_lon", "centroid_lat", "has_polygon", "bbox_min_lon", "bbox_min_lat", "bbox_max_lon", "bbox_max_lat"}
        if geometry: wanted.update({"geometry", "geometry_wkb"})
        rows = []
        for package, file in self._records():
            if selected_path is not None and file["local_path"] != selected_path:
                continue
            meta = package["metadata"]
            if country and meta.get("country_code") not in {country, "GLOBAL"}: continue
            if file.get("component") not in ({"exact", "display"} if geometry else {"exact", "display", "reference"}): continue
            archive = file["archive_path"].replace("\\", "/")
            if file.get("component") == "reference" and "/partitions/identities/" not in archive and archive != "reference/identities.parquet":
                continue
            path = file["local_path"]
            self._verify_file(file)
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
                family = row.get("family") or ("water_body" if meta.get("release_unit_id") == "MARINE" and row.get("level") == "water_body" else "marine_jurisdiction" if meta.get("release_unit_id") == "MARINE" else "administrative")
                row["family"] = public_geometry_family_id(family, row.get("subtype"))
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
        if name not in TOOLS:
            raise ValueError(f"Unknown Geometry tool: {name}; refresh tools/list")
        self._validate_call(name, args)
        return getattr(self, f"_call_{name}")(args)

    @staticmethod
    def _validate_call(name: str, args: dict[str, Any]) -> None:
        if not isinstance(args, dict):
            raise ValueError("Tool arguments must be an object")
        schema = TOOL_DEFINITIONS[name]["inputSchema"]
        if set(args) - set(schema.get("properties", {})):
            raise ValueError(f"Unknown arguments for {name}; use get_tool_help")
        if any(key not in args for key in schema.get("required", [])):
            raise ValueError(f"Missing required arguments for {name}")
        choices = {
            "get_tool_help": ("tool_name", "topic"),
            "get_geography": ("family_id", "scope_id"),
            "resolve_point": ("lat", "points"),
            "resolve_deep_point": ("lat", "points"),
            "get_loc_id_info": ("loc_id", "loc_ids"),
            "identify_reference_system": ("identifier", "identifiers"),
            "convert_reference": ("value", "items"),
            "compare_geographies": ("left_loc_id", "items"),
            "get_shape": ("loc_id", "loc_ids"),
        }
        if name in choices and sum(key in args for key in choices[name]) != 1:
            raise ValueError(f"Supply exactly one {choices[name]} selector for {name}")
        if name in {"resolve_point", "resolve_deep_point"} and "lat" in args and "lon" not in args:
            raise ValueError("lat and lon are required together")
        if name == "resolve_deep_point" and not str(args.get("shallow_loc_id") or ""):
            raise ValueError("shallow_loc_id is required")
        if name == "convert_reference" and (not str(args.get("iso3") or "") or
                                            ("value" in args and not str(args.get("from_system") or ""))):
            raise ValueError("convert_reference requires iso3 and a source system")
        if name == "compare_geographies" and "left_loc_id" in args and not str(args.get("right_loc_id") or ""):
            raise ValueError("right_loc_id is required")

    def _call_get_tool_help(self, args: dict[str, Any]) -> dict[str, Any]:
        if "tool_name" in args:
            definition = TOOL_DEFINITIONS.get(str(args["tool_name"]))
            if definition is None:
                return _error("unknown_tool", "The requested tool is not in tools/list")
            return {"ok": True, "tool_name": definition["name"], "title": definition.get("title"),
                    "purpose": definition["description"], "input_schema": definition["inputSchema"],
                    "output_schema": definition.get("outputSchema"),
                    "guidance": {"data_scope": "installed local packages"}}
        return {"ok": True, "topic": args["topic"], "available_tools": list(TOOL_DEFINITIONS),
                "guidance": {"data_scope": "installed local packages"}}

    def _package_families(self, package: dict[str, Any]) -> set[str]:
        """Read the small installed Reference index, never the shape banks."""
        sidecar = package["sidecar"]
        if sidecar in self.family_index:
            return self.family_index[sidecar]
        families = set()
        for file in package["files"]:
            if file["archive_path"].replace("\\", "/") != "reference/identities.parquet":
                continue
            self._verify_file(file)
            schema = self.schemas[file["local_path"]]
            columns = [name for name in ("family", "subtype", "admin_level") if name in schema.names]
            if "family" not in columns and "admin_level" not in columns:
                break
            for row in pq.read_table(file["local_path"], columns=columns).to_pylist():
                family = (public_geometry_family_id(row.get("family"), row.get("subtype"))
                          if row.get("family") else
                          "administrative" if row.get("admin_level") is not None else None)
                if family:
                    families.add(str(family))
            break
        self.family_index[sidecar] = families
        return families

    def _call_get_catalog(self, args: dict[str, Any]) -> dict[str, Any]:
        if args["catalog"] == "data":
            return _error("data_catalog_unavailable", "No installed data catalog is attached to this Geometry library")
        if args["catalog"] != "geometry":
            raise ValueError("catalog must be data or geometry")
        scopes = sorted({str(p["metadata"]["country_code"]) for p in self.packages})
        # Geometry bank IDs name artifacts. Reference identities supply the
        # families, including Global's administrative level rows. A Bridge is
        # a reference system, not a geometry family.
        families = set().union(*(self._package_families(package) for package in self.packages)) if self.packages else set()
        return {"ok": True, "catalog": "geometry", "detail": "download", "catalog_version": "local",
                "families": [{"family_id": value} for value in sorted(families)],
                "scopes": [{"scope_id": value} for value in scopes],
                "pack_count": len(self.packages) + len(self.bridges),
                "packages": self.catalog()["packages"] if self.packages else [],
                "external_reference_bridges": [{"package_id": p["metadata"].get("package_id"),
                                                "external_system": p["metadata"].get("external_system")}
                                               for p in self.bridges]}

    def _call_get_pack(self, args: dict[str, Any]) -> dict[str, Any]:
        return _error("data_catalog_unavailable", "get_pack needs an installed Research data catalog",
                      pack_id=args["pack_id"])

    def _call_get_geography(self, args: dict[str, Any]) -> dict[str, Any]:
        scope = args.get("scope_id")
        family = args.get("family_id")
        if args.get("year") is not None:
            return _error("temporal_catalog_unavailable", "Installed package metadata has no dated edition selector")
        if scope and not any(p["metadata"].get("country_code") == scope for p in self.packages):
            return _error("scope_not_found", "No installed Geometry package covers this scope", scope_id=scope)
        catalog = self._call_get_catalog({"catalog": "geometry"})
        installed_families = {row["family_id"] for row in catalog["families"]}
        if family and family not in installed_families:
            return _error("family_not_found", "No installed Geometry package covers this family", family_id=family)
        packages = [p for p in self.packages if (not scope or p["metadata"].get("country_code") == scope)
                    and (not family or family in self._package_families(p))]
        coverage = [{"scope_id": p["country_code"], "family_id": selected_family,
                     "package_id": p["package_id"], "package_tier": p["package_tier"],
                     "release_version": p.get("version")}
                    for package in packages
                    for p in [package["metadata"]]
                    for selected_family in ([family] if family else
                                            sorted(self._package_families(package)))]
        return {"ok": True, "catalog": "geometry", "family_id": family, "scope_id": scope,
                "requested_year": args.get("year"), "coverage": coverage,
                "admin_spine": {"release_unit_kind": "country"} if scope and scope != "MARINE" else {}}

    @staticmethod
    def _points(args: dict[str, Any]) -> list[dict[str, Any]]:
        points = args.get("points") if "points" in args else [{"lat": args.get("lat"), "lon": args.get("lon")}]
        if not isinstance(points, list) or not 1 <= len(points) <= 100:
            raise ValueError("points must contain 1 to 100 coordinates")
        for item in points:
            if not isinstance(item, dict) or any(type(item.get(key)) not in (int, float) for key in ("lat", "lon")):
                raise ValueError("each point needs numeric lat and lon")
            if not all(math.isfinite(item[key]) for key in ("lat", "lon")) or not -90 <= item["lat"] <= 90 or not -180 <= item["lon"] <= 180:
                raise ValueError("point is outside WGS84 bounds")
        return points

    def _selected_point_bank(self, scope: str, family: str, water_kind: str | None) -> Path | None:
        """Choose one installed point bank from one declared scope and family."""
        if scope == "GLOBAL":
            names = {"exact/admin_0.parquet", "display/admin_0.parquet"}
        elif scope == "MARINE":
            stem = ("jurisdictions" if family in MARINE_JURISDICTION_FAMILIES else
                    "named_water_areas" if water_kind == "named" else "water_bodies")
            names = {f"exact/{stem}.parquet", f"display/{stem}.parquet"}
        else:
            names = {"display/admin_0_3.parquet"}
        choices = []
        for package, file in self._records():
            meta = package["metadata"]
            if str(meta.get("country_code") or "").upper() != scope:
                continue
            if meta.get("package_tier") not in {"display", "detail"}:
                continue
            archive = file["archive_path"].replace("\\", "/")
            if scope not in {"GLOBAL", "MARINE"} and archive.startswith("exact/admin/admin_0/") and archive.endswith(".parquet"):
                pass
            elif archive not in names:
                continue
            rank = 2 if archive.startswith("exact/") else 1
            choices.append((rank, file["local_path"]))
        if not choices:
            return None
        highest = max(rank for rank, _path in choices)
        selected = {path for rank, path in choices if rank == highest}
        if len(selected) != 1:
            raise ValueError(f"Ambiguous installed point bank for {scope} {family}; remove duplicate editions")
        path = next(iter(selected))
        if scope == "MARINE" and family in MARINE_JURISDICTION_FAMILIES:
            selected_file = next(file for _package, file in self._records()
                                 if file["local_path"] == path)
            self._verify_file(selected_file)
            if "subtype" not in self.schemas[path].names:
                return None
            available = set(pq.read_table(path, columns=["subtype"])["subtype"].to_pylist())
            if family not in available:
                return None
        return path

    def _point_result(self, item: dict[str, Any], *, shallow: str | None = None,
                      family: str = "administrative", target_level: int | None = None,
                      selected_path: Path | None = None, scope: str | None = None) -> dict[str, Any]:
        point = Point(item["lon"], item["lat"])
        selected_rows = self._load(geometry=True, point=point, selected_path=selected_path)
        matches = self._dedupe([row for row in selected_rows
                                if (row.get("family") == family or scope == "MARINE" and family == "water_body")
                                and _shape(row["geometry"]).covers(point)])
        if scope == "MARINE":
            return {
                "point": {"lat": item["lat"], "lon": item["lon"]},
                "family": family,
                "family_result": {"family": family,
                                  "status": "matched" if matches else "no_point_overlap",
                                  "matches": [{"loc_id": row["loc_id"], "name": row.get("name"),
                                               "family": family} for row in matches]},
            }
        if shallow:
            anchor = self._identity(shallow)
            if anchor is None:
                return {"point": {"lat": item["lat"], "lon": item["lon"]},
                        "error": {"code": "shallow_scope_not_found", "message": "shallow_loc_id is absent from installed packages"}}
            def under_scope(row):
                current = row
                visited = set()
                while current and current["loc_id"] not in visited:
                    if current["loc_id"] == shallow:
                        return True
                    visited.add(current["loc_id"])
                    current = self._identity(current.get("parent_id")) if current.get("parent_id") else None
                return False
            matches = [row for row in matches if under_scope(row) and (row.get("admin_level") or 0) > (anchor.get("admin_level") or 0)]
        else:
            matches = [row for row in matches if (row.get("admin_level") or 0) <= 3]
        if target_level is not None:
            matches = [row for row in matches if row.get("admin_level") == target_level]
        if not matches:
            return {"point": {"lat": item["lat"], "lon": item["lon"]},
                    "error": {"code": "no_point_overlap", "message": "No installed polygon covers this point in the selected scope"}}
        stack = [{"loc_id": row["loc_id"], "name": row.get("name"), "admin_level": row.get("admin_level"),
                  "family": "administrative"} for row in matches]
        deepest = max(stack, key=lambda row: (row.get("admin_level") or 0, row["loc_id"]))
        result = {"resolution_schema_version": "1.0.0", "point": {"lat": item["lat"], "lon": item["lon"]},
                  "country": matches[0]["country"], "matched": deepest,
                  "deepest_resolved_loc_id": deepest["loc_id"],
                  "deepest_resolved_admin_level": deepest["admin_level"],
                  "deepest_resolved_family": family, "stack": stack,
                  "resolution_mode": "installed_package", "join_grain": deepest["admin_level"]}
        if shallow:
            result.update(shallow_loc_id=shallow, family=family)
        return result

    def _resolve_points(self, args: dict[str, Any], *, deep: bool) -> dict[str, Any]:
        points = self._points(args)
        scope = str(args.get("scope_id") or "").strip().upper() if not deep else ""
        water_kind = str(args.get("water_kind") or "").strip() if not deep else ""
        family = args.get("family", "administrative") if deep else "administrative"
        if not deep:
            family = str(args.get("family") or "").strip()
            if not scope:
                return _error("point_scope_required", "Select one scope_id from the installed geometry catalog")
            if scope not in {"GLOBAL", "MARINE"} and not (len(scope) == 3 and scope.isalpha()):
                return _error("invalid_point_scope", "Select GLOBAL, MARINE, or one installed ISO3 country")
            allowed = {"GLOBAL": {"administrative"},
                       "MARINE": set(MARINE_POINT_FAMILIES)}
            if not family:
                return _error("point_family_required", "Select one family for the selected scope")
            if family not in allowed.get(scope, {"administrative"}):
                return _error("invalid_point_family", "Family has no point route in the selected scope")
            if scope == "MARINE" and family == "water_body" and water_kind not in {"named", "zone"}:
                return _error("water_kind_required", "Marine water_body requires water_kind named or zone")
            if water_kind and (scope != "MARINE" or family != "water_body"):
                return _error("water_kind_not_applicable", "water_kind applies only to MARINE water_body")
        if family != "administrative":
            if deep:
                return _error("family_route_unavailable", "This installed package has no deep point route for the selected family", family=family)
        selected_path = None if deep else self._selected_point_bank(scope, family, water_kind or None)
        if not deep and selected_path is None:
            return _error("selected_scope_unavailable", "No installed Display or Detail point bank covers the selected scope and family", scope_id=scope, family=family)
        level = args.get("target_admin_level") if deep else None
        if level is not None:
            try:
                level = int(str(level).removeprefix("admin_"))
            except ValueError as exc:
                raise ValueError("target_admin_level must be an Admin level") from exc
            if not 4 <= level <= 6:
                raise ValueError("target_admin_level must be Admin 4 through 6")
        results = [self._point_result(item, shallow=args.get("shallow_loc_id") if deep else None,
                                      family=family, target_level=level,
                                      selected_path=selected_path, scope=scope if not deep else None) for item in points]
        if "points" not in args:
            return {"request_id": args.get("request_id"), **results[0]}
        return {"request_id": args.get("request_id"), "batch_id": args.get("batch_id"),
                "point_count": len(results), "resolved_count": sum("error" not in r for r in results),
                "unresolved_count": sum("error" in r for r in results),
                "results": [{"index": i, "row_index": item.get("row_index", i),
                             **({"id": item["id"]} if "id" in item else {}), **result}
                            for i, (item, result) in enumerate(zip(points, results))]}

    def _call_resolve_point(self, args: dict[str, Any]) -> dict[str, Any]:
        return self._resolve_points(args, deep=False)

    def _call_resolve_deep_point(self, args: dict[str, Any]) -> dict[str, Any]:
        return self._resolve_points(args, deep=True)

    def _identity(self, loc_id: str) -> dict[str, Any] | None:
        rows = self._dedupe(self._load(loc_ids=[loc_id]))
        return rows[0] if rows else None

    def _loc_info_item(self, loc_id: str) -> dict[str, Any]:
        identity = self._identity(loc_id)
        if identity is None:
            return {"loc_id": loc_id, "error": {"code": "not_found", "message": "loc_id has no installed identity"}}
        country = identity["country"]
        edges = self._reference_rows("relationships", country=country,
                                     filters=[("source_loc_id", "=", loc_id)], limit=10000)
        incoming = self._reference_rows("relationships", country=country,
                                        filters=[("target_loc_id", "=", loc_id)], limit=10000)
        connections = []
        for edge, direction in [(edge, "outgoing") for edge in edges] + [(edge, "incoming") for edge in incoming]:
            connections.append({"loc_id": edge["target_loc_id"] if direction == "outgoing" else edge["source_loc_id"],
                                "family": edge.get("target_family") if direction == "outgoing" else edge.get("source_family"),
                                "direction": direction, "relationship_type": edge.get("relationship_type") or edge.get("relationship") or edge.get("semantic_type"),
                                "relationship_subtype": edge.get("relationship_subtype"),
                                "relationship_id": edge.get("relationship_id"),
                                "relationship_vintage": edge.get("relationship_vintage")})
        return {"loc_id": loc_id, "name": identity.get("name"), "family": identity.get("family"),
                "admin_level": identity.get("admin_level"), "parent_id": identity.get("parent_id"),
                "connections": connections[:50], "connection_count": min(len(connections), 50),
                "connection_limit": 50, "has_more": len(connections) > 50}

    def _call_get_loc_id_info(self, args: dict[str, Any]) -> dict[str, Any]:
        ids = args.get("loc_ids") if "loc_ids" in args else [args["loc_id"]]
        if not isinstance(ids, list) or not 1 <= len(ids) <= 10 or any(not isinstance(x, str) or not x.strip() for x in ids):
            raise ValueError("loc_ids must contain 1 to 10 nonempty strings")
        results = [self._loc_info_item(value.strip().upper()) for value in ids]
        if "loc_id" in args:
            return {"request_id": args.get("request_id"), **results[0]}
        return {"request_id": args.get("request_id"), "batch_id": args.get("batch_id"),
                "loc_id_count": len(ids), "results": results,
                "found_count": sum("error" not in result for result in results),
                "missing_count": sum("error" in result for result in results)}

    def _call_identify_dataset_geography(self, args: dict[str, Any]) -> dict[str, Any]:
        from mapmover.runtime.dataset_column_selection import identify_dataset_geography
        return identify_dataset_geography(args["columns"], dataset_context=args.get("dataset_context"),
                                          country_scope=args.get("country_scope"))

    def _call_identify_reference_system(self, args: dict[str, Any]) -> dict[str, Any]:
        scope = str(args["country_scope"]).upper()
        if (args.get("expected") or {}).get("vintage"):
            return _error("temporal_reference_unavailable", "Installed alias partitions cannot verify a requested source vintage")
        values = args.get("identifiers") if "identifiers" in args else [args["identifier"]]
        if not isinstance(values, list) or not values or any(not isinstance(x, str) for x in values):
            raise ValueError("identifiers must be nonempty strings")
        if not any(p["metadata"].get("country_code") in {scope, "GLOBAL"} for p in self.packages):
            return _error("scope_not_found", "No installed reference graph covers this scope", country_scope=scope)
        systems: dict[str, dict[str, list[str]]] = {}
        for value in dict.fromkeys(values):
            for row in self._reference_rows("aliases", country=scope,
                                            filters=[("external_id", "=", value)], limit=10000):
                system = str(row.get("reference_system"))
                ids = systems.setdefault(system, {}).setdefault(value, [])
                if row.get("loc_id") and row["loc_id"] not in ids:
                    ids.append(row["loc_id"])
            for edge in self._bridge_rows(scope=scope, column="gers_division_id", value=value):
                if edge.get("relationship_type") == "equivalent_identity":
                    ids = systems.setdefault("overture_gers", {}).setdefault(value, [])
                    if edge["loc_id"] not in ids:
                        ids.append(edge["loc_id"])
            identity = self._identity(value)
            if identity and (scope == "GLOBAL" or identity["country"] == scope):
                systems.setdefault("daedalmap.loc_id", {}).setdefault(value, []).append(value)
        expected = (args.get("expected") or {}).get("system")
        if expected == "loc_id":
            expected = "daedalmap.loc_id"
        candidates = []
        for system, matched in sorted(systems.items()):
            if expected and system != expected:
                continue
            levels = sorted({f"admin_{row['admin_level']}" for matches in matched.values() for loc_id in matches
                             if (row := self._identity(loc_id)) and row.get("admin_level") is not None})
            candidates.append({"system": system, "method": "reference_graph_exact_alias" if system != "daedalmap.loc_id" else "loc_id_passthrough",
                               "match_count": len(matched), "unmatched_count": len(values) - len(matched),
                               "match_rate": round(len(matched) / len(values), 6),
                               "ambiguous_identifier_count": sum(len(ids) > 1 for ids in matched.values()),
                               "geo_levels": levels, "loc_id_resolvable": True,
                               "country_scopes": [scope],
                               "sample_matches": [{"identifier": value, "loc_ids": ids[:5], "geo_level": None}
                                                  for value, ids in matched.items()]})
        full = [candidate for candidate in candidates if candidate["match_rate"] == 1.0]
        selected = full[0] if len(full) == 1 else None
        status = "matched" if selected else "ambiguous" if len(full) > 1 else "partial_match" if candidates else "unmatched"
        binding = None if not selected else {"mode": "reference", "system": selected["system"],
                                            "geo_level": selected["geo_levels"][0] if len(selected["geo_levels"]) == 1 else None,
                                            "vintage": (args.get("expected") or {}).get("vintage"), "country_scope": scope}
        return {"ok": status != "unmatched", "request_id": args.get("request_id"), "country_scope": scope,
                "status": status, "validation_scope": args.get("validation_scope", "sample"),
                "identifier_count": len(values), "distinct_identifier_count": len(set(values)),
                "expected": args.get("expected") or None, "candidates": candidates,
                "recommended_binding": binding,
                "clarification": {"required": status == "ambiguous", "reason": "ambiguous_reference_system" if status == "ambiguous" else None}}

    def _conversion_item(self, args: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
        scope = args["iso3"].upper()
        value = item["value"]
        source = item.get("from_system") or args.get("from_system")
        target = item.get("to_system") or args.get("to_system") or "daedalmap.loc_id"
        if not isinstance(value, str) or not source:
            raise ValueError("conversion needs a string value and from_system")
        if item.get("iso3", scope).upper() != scope:
            return {"ok": False, "input": value, "error": {"code": "scope_mismatch", "message": "item iso3 differs from call scope"}}
        unsupported = [key for key in ("target_admin_level", "relationship_vintage", "crosswalk_id", "min_share",
                                       "country_hint", "admin_level_hint", "as_of") if item.get(key) is not None or args.get(key) is not None]
        if unsupported:
            return {"ok": False, "from_system": source, "input": value, "to_system": target,
                    "error": {"code": "route_unavailable", "message": "Installed reference aliases cannot evaluate " + ", ".join(unsupported)}}
        if source == "overture_gers" or target == "overture_gers":
            if not any(p["metadata"].get("external_system") == "overture_gers" for p in self.bridges):
                return {"ok": False, "input": value, "from_system": source, "to_system": target,
                        "error": {"code": "bridge_not_installed", "message": "Install the Overture GERS Bridge package"}}
            if source == "overture_gers" and target in {"loc_id", "daedalmap.loc_id"}:
                edges = self._bridge_rows(scope=scope, column="gers_division_id", value=value)
                target_key = "loc_id"
            elif source in {"loc_id", "daedalmap.loc_id"} and target == "overture_gers":
                edges = self._bridge_rows(scope=scope, column="loc_id", value=value)
                target_key = "gers_division_id"
            else:
                return {"ok": False, "input": value, "from_system": source, "to_system": target,
                        "error": {"code": "route_unavailable", "message": "The installed Bridge only converts between Overture GERS and loc_id"}}
            if not edges:
                return {"ok": False, "input": value, "from_system": source, "to_system": target,
                        "error": {"code": "not_found", "message": "No admitted Bridge edge resolves this identifier and scope"}}
            matches = [{"value": edge[target_key], "loc_id": edge["loc_id"],
                        "relationship_type": edge["relationship_type"], "iso3": edge["iso3"],
                        "our_area_share": edge.get("our_area_share"),
                        "their_area_share": edge.get("their_area_share")}
                       for edge in edges if edge.get(target_key)]
            matches.sort(key=lambda edge: (edge["relationship_type"] != "equivalent_identity", edge["value"]))
            identities = {edge["loc_id"] for edge in edges if edge["relationship_type"] == "equivalent_identity"}
            return {"ok": True, "input": value, "from_system": source, "to_system": target,
                    "resolved_loc_id": next(iter(identities)) if len(identities) == 1 else None,
                    "resolution_kind": "equivalent_identity" if len(identities) == 1 else "related_geography",
                    "matches": matches[:int(item.get("limit") or args.get("limit") or 10)],
                    "match_count": len(matches)}
        if source in {"loc_id", "daedalmap.loc_id"}:
            identity = self._identity(value)
            loc_ids = [value] if identity and (scope == "GLOBAL" or identity["country"] == scope) else []
        else:
            aliases = self._reference_rows("aliases", country=scope,
                                           filters=[("reference_system", "=", source), ("external_id", "=", value)])
            loc_ids = sorted({row["loc_id"] for row in aliases if row.get("reference_system") == source and row.get("external_id") == value})
        if not loc_ids:
            return {"ok": False, "input": value, "from_system": source, "to_system": target,
                    "error": {"code": "not_found", "message": "No installed reference resolves this value"}}
        if len(loc_ids) > 1:
            return {"ok": False, "input": value, "from_system": source, "to_system": target,
                    "candidate_loc_ids": loc_ids,
                    "error": {"code": "ambiguous_reference", "message": "The installed alias resolves to multiple loc_ids"}}
        if target in {"loc_id", "daedalmap.loc_id"}:
            values = loc_ids
        else:
            values = sorted({row["external_id"] for loc_id in loc_ids
                             for row in self._reference_rows("aliases", country=scope,
                                                              filters=[("reference_system", "=", target), ("loc_id", "=", loc_id)])
                             if row.get("reference_system") == target and row.get("loc_id") == loc_id})
        if not values:
            return {"ok": False, "input": value, "from_system": source, "to_system": target,
                    "error": {"code": "not_found", "message": "No installed target reference resolves this identity"}}
        return {"ok": True, "input": value, "from_system": source, "to_system": target,
                "resolved_loc_id": loc_ids[0] if len(loc_ids) == 1 else None,
                "matches": [{"value": result, "loc_id": loc_ids[0]} for result in values[:int(item.get("limit") or args.get("limit") or 10)]]}

    def _call_convert_reference(self, args: dict[str, Any]) -> dict[str, Any]:
        scope = str(args["iso3"]).upper()
        if not any(p["metadata"].get("country_code") in {scope, "GLOBAL"} for p in self.packages) and not self.bridges:
            return _error("scope_not_found", "No installed reference graph covers this scope", iso3=scope)
        items = args.get("items") if "items" in args else [{"value": args["value"]}]
        if not isinstance(items, list) or not 1 <= len(items) <= 25000:
            raise ValueError("items must contain 1 to 25000 entries")
        results = [self._conversion_item(args, item) for item in items]
        if "value" in args:
            return {"request_id": args.get("request_id"), **results[0]}
        return {"request_id": args.get("request_id"), "batch_id": args.get("batch_id"),
                "item_count": len(results), "results": results,
                "converted_count": sum(item.get("ok") is True for item in results),
                "unconverted_count": sum(item.get("ok") is not True for item in results)}

    def _compare_item(self, left: str, right: str) -> dict[str, Any]:
        a, b = self._identity(left), self._identity(right)
        if a is None or b is None:
            return {"left_loc_id": left, "right_loc_id": right,
                    "error": {"code": "not_found", "message": "One or both identities are absent from installed packages"}}
        if left == right:
            relation = "equals"
        else:
            def ancestors(row):
                found = set()
                while row and row.get("parent_id") and row["parent_id"] not in found:
                    parent = row["parent_id"]
                    found.add(parent)
                    row = self._identity(parent)
                return found
            relation = "contains" if left in ancestors(b) else "within" if right in ancestors(a) else "unknown"
        return {"ok": True, "left": {"loc_id": left, "name": a.get("name")},
                "right": {"loc_id": right, "name": b.get("name")},
                "temporal_relation": "coexistence_unknown", "spatial_relation": relation,
                "relationship_basis": "same_graph_identity" if relation == "equals" else "selected_reference_graph",
                "geometry_loaded": False,
                **({"intersects": True} if relation == "equals" else {})}

    def _call_compare_geographies(self, args: dict[str, Any]) -> dict[str, Any]:
        items = args.get("items") if "items" in args else [args]
        if not isinstance(items, list) or not 1 <= len(items) <= 100:
            raise ValueError("items must contain 1 to 100 pairs")
        results = []
        for item in items:
            selectors = [key for key in ("as_of", "left_as_of", "right_as_of")
                         if item.get(key) is not None or args.get(key) is not None]
            if selectors:
                results.append({"ok": False, "error": {"code": "temporal_graph_unavailable",
                                                     "message": "Installed reference graph cannot verify " + ", ".join(selectors)}})
            else:
                results.append(self._compare_item(str(item["left_loc_id"]), str(item["right_loc_id"])))
        if "items" not in args:
            return {"request_id": args.get("request_id"), **results[0]}
        return {"request_id": args.get("request_id"), "batch_id": args.get("batch_id"),
                "item_count": len(results), "results": results}

    def _shape_item(self, loc_id: str, include: bool) -> dict[str, Any]:
        identity = self._identity(loc_id)
        if identity is None:
            return {"loc_id": loc_id, "error": {"code": "not_found", "message": "loc_id is absent from installed packages"}}
        rows = self._dedupe(self._load(geometry=include, loc_ids=[loc_id])) if include else [identity]
        shape_row = rows[0] if rows else None
        if include and shape_row is None:
            return {"loc_id": loc_id, "error": {"code": "shape_unavailable", "message": "Install Display or Detail for this polygon"}}
        has_shape = bool(shape_row and shape_row.get("polygon_kind") in {"exact", "simplified"}
                         and shape_row.get("has_polygon") is not False)
        out = {"ok": has_shape, "loc_id": loc_id, "has_shape": has_shape,
               "polygon_kind": shape_row.get("polygon_kind") if shape_row else None,
               "name": identity.get("name"), "admin_level": identity.get("admin_level"),
               "centroid_lon": identity.get("centroid_lon"), "centroid_lat": identity.get("centroid_lat"),
               "bbox_min_lon": identity.get("bbox_min_lon"), "bbox_min_lat": identity.get("bbox_min_lat"),
               "bbox_max_lon": identity.get("bbox_max_lon"), "bbox_max_lat": identity.get("bbox_max_lat")}
        if not has_shape:
            out["error"] = {"code": "shape_unavailable", "message": "No installed polygon exists for this loc_id"}
        if include:
            _check_inline_geometry_budget(rows)
            out["geometry"] = mapping(_shape(shape_row["geometry"]))
        return out

    def _call_get_shape(self, args: dict[str, Any]) -> dict[str, Any]:
        ids = args.get("loc_ids") if "loc_ids" in args else [args["loc_id"]]
        if not isinstance(ids, list) or not 1 <= len(ids) <= (10 if args.get("include_polygon") else 100):
            raise ValueError("loc_ids exceeds the local shape lookup limit")
        results = [self._shape_item(value, bool(args.get("include_polygon", False))) for value in ids]
        if "loc_id" in args:
            return {"request_id": args.get("request_id"), **results[0]}
        return {"request_id": args.get("request_id"), "batch_id": args.get("batch_id"),
                "loc_id_count": len(ids), "available_shape_count": sum(item["has_shape"] for item in results if "has_shape" in item),
                "missing_shape_count": sum(not item.get("has_shape", False) for item in results), "results": results}

    @staticmethod
    def _public(row, *, include_polygon):
        out = {k: v for k, v in row.items() if k != "geometry"}
        if include_polygon and row.get("geometry") is not None: out["geometry"] = mapping(_shape(row["geometry"]))
        return out
