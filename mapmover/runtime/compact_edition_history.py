"""Read exact WKB from a country-neutral compact edition history package."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import zstandard as zstd

from ..duckdb_helpers import select_rows
from ..paths import DATA_ROOT

DICT_LIMIT = 128 * 1024
INDEX_COLUMNS = ["loc_id", "geometry_hash", "source_id", "source_release"]


def _contained(root: Path, relative: str) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError(f"unsafe compact history path: {relative!r}")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"compact history path leaves data root: {relative!r}")
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pinned_object(root: Path, record: dict) -> Path:
    digest = record["sha256"]
    if (not isinstance(digest, str) or len(digest) != 64 or
            any(char not in "0123456789abcdef" for char in digest)):
        raise ValueError(f"invalid compact history object digest: {digest!r}")
    path = _contained(root, f"{digest[:2]}/{digest}")
    if path.stat().st_size != record["size_bytes"] or _sha256(path) != digest:
        raise ValueError(f"compact history object drift: {digest}")
    return path


def _decode(codec: str, payload: bytes, newer: bytes | None) -> bytes:
    if codec == "zstd-standalone":
        return zstd.ZstdDecompressor().decompress(payload)
    if codec == "zstd-newer-dictionary" and newer is not None:
        dictionary = zstd.ZstdCompressionDict(
            newer[:DICT_LIMIT], dict_type=zstd.DICT_TYPE_RAWCONTENT,
        )
        return zstd.ZstdDecompressor(dict_data=dictionary).decompress(payload)
    raise ValueError(f"invalid compact history codec or dictionary: {codec}")


def read_compact_edition_history_rows(bank_root: Path, loc_ids: list[str]) -> pd.DataFrame:
    """Return release-scoped source attributes and verified exact WKB."""
    if not loc_ids:
        return pd.DataFrame(columns=["loc_id", "source_id", "source_release", "geometry"])
    release = json.loads((bank_root / "manifest.json").read_text(encoding="utf-8"))
    if (release.get("storage_backend") != "compact_edition_history_v1"
            or release.get("status") != "PASS"):
        raise ValueError(f"unrecognized compact edition history bank: {bank_root}")
    root = DATA_ROOT.resolve()
    manifest_pin = release["compact_history_manifest"]
    manifest_path = _contained(root, manifest_pin["path"])
    if _sha256(manifest_path) != manifest_pin["sha256"]:
        raise ValueError(f"compact history manifest drift: {manifest_path}")
    object_root = _contained(root, release["compact_object_root"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (manifest.get("schema_version") != "0.1.0" or
            manifest.get("profile") != "geometry_compact_edition_history" or
            manifest.get("status") != "PASS"):
        raise ValueError(f"invalid compact history manifest: {manifest_path}")
    year = release["source_edition_year"]
    years = manifest["years"]
    if (type(year) is not int or years != sorted(set(years)) or
            not years or manifest["latest_year"] != years[-1] or year not in years):
        raise ValueError(f"invalid compact history edition: {year!r}")
    _pinned_object(object_root, manifest["editions"][str(year)]["metadata"])
    index = select_rows(
        bank_root / "shapes/edition_index.parquet", columns=INDEX_COLUMNS,
        in_filters={"loc_id": loc_ids},
    )
    if index.empty:
        return pd.DataFrame(columns=["loc_id", "source_id", "source_release", "geometry"])
    if index["loc_id"].duplicated().any():
        raise ValueError("duplicate compact history edition index loc_id")

    steps = manifest["reverse_steps"]
    if len(steps) != len(years) - 1:
        raise ValueError("invalid compact history transition count")
    traversed = []
    for newer, older, step in zip(reversed(years[1:]), reversed(years[:-1]), steps):
        if step["from_year"] != newer or step["to_year"] != older:
            raise ValueError("invalid compact history transition order")
        if older < year:
            break
        path = _pinned_object(object_root, step["patches"])
        if pq.ParquetFile(path).metadata.num_rows != step["patch_count"]:
            raise ValueError("compact history patch count drift")
        traversed.append(path)

    # Resolve only the shapes requested by this route. FRA Admin 4 has a large
    # baseline and several large patches; loading every WKB per point request
    # would make the runtime grow with the whole country's history.
    required = set(index["geometry_hash"])
    for path in reversed(traversed):
        links = select_rows(
            path, columns=["old_geometry_hash", "newer_geometry_hash", "codec"],
            in_filters={"old_geometry_hash": required},
        )
        for row in links.to_dict("records"):
            if row["codec"] == "zstd-newer-dictionary":
                required.add(row["newer_geometry_hash"])
    baseline_path = _pinned_object(object_root, manifest["baseline"])
    objects: dict[str, bytes] = {}
    baseline = select_rows(
        baseline_path, columns=["geometry_hash", "geometry"],
        in_filters={"geometry_hash": required},
    )
    for row in baseline.to_dict("records"):
        digest, wkb = row["geometry_hash"], row["geometry"]
        if digest in objects or hashlib.sha256(wkb).hexdigest() != digest:
            raise ValueError(f"invalid compact history baseline shape: {digest}")
        objects[digest] = wkb
    for path in traversed:
        patches = select_rows(
            path, columns=["old_geometry_hash", "newer_geometry_hash", "codec", "payload"],
            in_filters={"old_geometry_hash": required},
        )
        for row in patches.to_dict("records"):
            digest = row["old_geometry_hash"]
            if digest in objects:
                raise ValueError(f"redundant compact history shape: {digest}")
            newer_wkb = objects.get(row["newer_geometry_hash"])
            wkb = _decode(row["codec"], row["payload"], newer_wkb)
            if hashlib.sha256(wkb).hexdigest() != digest:
                raise ValueError(f"compact history patch hash mismatch: {digest}")
            objects[digest] = wkb
    rows = []
    for row in index.to_dict("records"):
        digest = row["geometry_hash"]
        if digest not in objects:
            raise ValueError(f"compact history edition geometry is unreachable: {digest}")
        rows.append({"loc_id": row["loc_id"], "source_id": row["source_id"],
                     "source_release": row["source_release"], "geometry": objects[digest]})
    return pd.DataFrame(rows)
