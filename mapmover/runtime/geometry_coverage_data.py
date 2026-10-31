"""Bounded data reader for an already selected extracted Geometry package.

The caller supplies the active package directory. This module never searches
releases or substitutes another year. Only explicit exact/history partitions
are supported until other payload layouts have selection proofs.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
from shapely import from_wkb
from shapely.geometry import mapping


PACKAGE_ENV = "DAEDALMAP_GEOMETRY_ACTIVE_PACKAGE_DIR"
MANIFEST_ENV = "DAEDALMAP_GEOMETRY_ACTIVE_MANIFEST_DIR"
SOURCE_ENV = "DAEDALMAP_GEOMETRY_DATA_SOURCE"
MAX_PAGE_SIZE = 10
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
SELECTED_COLUMNS = ("loc_id", "family", "name", "admin_level", "parent_loc_id",
                    "frame_id", "geometry")


def _error(code: str, message: str, **details) -> dict:
    return {"ok": False, "error": {"code": code, "message": message}, **details}


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_dataset(*, scope: str, family: str, year: int, cells: list[dict],
                 page: int = 0, page_size: int = 10,
                 include_geometry: bool = True) -> dict:
    if type(page) is not int or page < 0 or type(page_size) is not int or not 1 <= page_size <= MAX_PAGE_SIZE:
        return _error("invalid_page", f"page must be nonnegative and page_size 1-{MAX_PAGE_SIZE}")
    configured_source = os.getenv(SOURCE_ENV)
    if configured_source not in {"extracted", "manifest_objects"}:
        return _error("geometry_data_source_unconfigured", "Select one active Geometry data source.")
    package_root = Path(os.environ[PACKAGE_ENV]) / scope if os.getenv(PACKAGE_ENV) else None
    sidecar = package_root / "geometry.json" if package_root else None
    manifest_path = (Path(os.environ[MANIFEST_ENV]) / scope / "manifest.json"
                     if os.getenv(MANIFEST_ENV) else None)
    if configured_source == "extracted":
        if not sidecar or not sidecar.is_file():
            return _error("geometry_package_unavailable", "The declared extracted package has no geometry.json for this scope.")
        inventory = json.loads(sidecar.read_text(encoding="utf-8"))
        if ((inventory.get("country_code") or inventory.get("release_unit_id")) != scope or
                inventory.get("package_tier") != "detail"):
            return _error("geometry_package_mismatch", "The supplied Detail package belongs to another scope.")
        members = {str(item.get("archive_path")): item for item in inventory.get("files") or []}
        source_mode = "extracted_package"
    else:
        if not manifest_path or not manifest_path.is_file():
            return _error("geometry_manifest_unavailable", "The declared active manifest is absent for this scope.")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (manifest.get("release_unit") or {}).get("id") != scope:
            return _error("geometry_package_mismatch", "The supplied active manifest belongs to another scope.")
        members = {str(item["path"]): item for item in manifest["editions"]["detail"]["files"]}
        source_mode = "active_manifest_objects"
    selected = []
    for cell in cells:
        if cell["form"] != "shape" or "detail" not in cell["packs"]:
            return _error("geometry_data_route_unavailable", "This catalog cell has no Detail shape route.")
        key = f"admin_{cell['admin_level']}" if family == "administrative" else family
        prefix = f"exact/history/{key}/year={year}/"
        matches = sorted(path for path in members if path.startswith(prefix) and path.endswith(".parquet"))
        if not matches:
            return _error("geometry_data_route_unavailable", "A catalog cell has no matching active package member.",
                          missing_member_prefix=prefix)
        selected.extend(matches)
    selected = sorted(set(selected))
    if not selected:
        return _error("geometry_data_route_unavailable", "No exact history members were selected.")
    files = []
    total_rows = 0
    for member in selected:
        item = members[member]
        if source_mode == "extracted_package":
            path = (package_root / member).resolve()
            try:
                path.relative_to(package_root.resolve())
            except ValueError:
                return _error("unsafe_package_member", "A package member escapes the active package directory.")
            if not path.is_file() or path.stat().st_size != item.get("size_bytes"):
                return _error("geometry_package_member_missing", "A selected package member is absent or has the wrong size.",
                              member=member)
            if _file_hash(path) != item.get("sha256"):
                return _error("geometry_package_member_hash_mismatch", "A selected package member failed SHA-256 verification.",
                              member=member)
            reader = pq.ParquetFile(path)
            names = reader.schema.names
            count = reader.metadata.num_rows
            files.append((member, reader, names, count, None))
        else:
            digest = str(item.get("object_sha256") or "")
            if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
                return _error("geometry_object_pin_missing", "Selected member has no valid object hash.", member=member)
            unit_path = f"countries/{scope}" if len(scope) == 3 else scope.lower()
            data_root = os.getenv("DATA_ROOT")
            if not data_root:
                return _error("geometry_data_root_missing", "The active object store root was not supplied.")
            object_path = Path(data_root) / "geometry" / unit_path / "_objects/sha256" / digest[:2] / digest
            from ..duckdb_helpers import path_to_uri, run_rows
            uri = path_to_uri(object_path)
            if not uri.startswith("s3://") and not object_path.is_file():
                return _error("geometry_object_missing", "The selected object is unavailable.", member=member)
            try:
                names = [row[0] for row in run_rows("DESCRIBE SELECT * FROM read_parquet(?)", [uri], raw_geoparquet=True)]
                count = run_rows("SELECT COUNT(*) FROM read_parquet(?)", [uri], raw_geoparquet=True)[0][0]
            except duckdb.Error:
                return _error("geometry_object_unavailable", "The selected object could not be read.", member=member)
            files.append((member, None, names, count, uri))
        if "loc_id" not in names or "geometry" not in names:
            return _error("geometry_package_schema_mismatch", "A selected exact member lacks loc_id or geometry.",
                          member=member)
        total_rows += count
    start = page * page_size
    remaining = page_size
    features = []
    for member, reader, names, count, uri in files:
        if start >= count:
            start -= count
            continue
        columns = [column for column in SELECTED_COLUMNS if column in names]
        if reader is None:
            from ..duckdb_helpers import run_rows
            columns_sql = ", ".join('"' + column.replace('"', '""') + '"' for column in columns)
            try:
                values = run_rows(f"SELECT {columns_sql} FROM read_parquet(?) LIMIT ? OFFSET ?",
                                  [uri, remaining, start], raw_geoparquet=True)
            except duckdb.Error:
                return _error("geometry_object_unavailable", "The selected object could not be read.", member=member)
            batches = [[dict(zip(columns, values_row)) for values_row in values]]
            start = 0
        else:
            batches = (batch.to_pylist() for batch in reader.iter_batches(batch_size=64, columns=columns))
        for batch_rows in batches:
            if start >= len(batch_rows):
                start -= len(batch_rows)
                continue
            for row in batch_rows[start:start + remaining]:
                geometry = row.pop("geometry", None)
                if row.get("family") not in {None, member.split("/")[2]}:
                    return _error("geometry_package_row_mismatch", "A selected member contains another family.",
                                  member=member)
                feature = {"type": "Feature", "id": row.get("loc_id"), "properties": row,
                           "geometry": mapping(from_wkb(geometry)) if include_geometry and geometry else None}
                features.append(feature)
                if len(json.dumps(features, default=str)) > MAX_RESPONSE_BYTES:
                    return _error("geometry_response_too_large", "Page exceeds the inline geometry budget; reduce page_size or omit geometry.")
                remaining -= 1
                if remaining == 0:
                    break
            start = 0
            if remaining == 0:
                break
        if remaining == 0:
            break
    return {"ok": True, "catalog": "geometry_coverage_v1", "scope_id": scope,
            "family_id": family, "year": year, "page": page, "page_size": page_size,
            "total_rows": total_rows, "next_page": page + 1 if (page + 1) * page_size < total_rows else None,
            "member_source": source_mode, "members": selected, "features": features}
