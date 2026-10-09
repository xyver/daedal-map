"""Resolve hash-indexed exact WKB from a newest-first reverse geometry bank.

The package identity index is small; complete WKB lives only in the shared
baseline/patch store. The reader verifies every decoded WKB against the
release-scoped index hash before returning it.
"""

from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path

import pandas as pd
import zstandard as zstd

from ..duckdb_helpers import select_rows
from ..paths import DATA_ROOT

DICT_LIMIT = 128 * 1024


def _inside_data_root(relative: str) -> Path:
    if not relative or Path(relative).is_absolute():
        raise ValueError(f"unsafe compact geometry path: {relative!r}")
    root = DATA_ROOT.resolve()
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        raise ValueError(f"compact geometry path leaves data root: {relative!r}")
    return path


@lru_cache(maxsize=32)
def _paths(bank_root: Path) -> tuple[Path, Path, dict[str, dict]]:
    manifest = json.loads((bank_root / "manifest.json").read_text(encoding="utf-8"))
    if (manifest.get("storage_backend") != "compact_reverse_wkb" or
            manifest.get("country") != "BRA" or
            manifest.get("admission_status") != "graph_candidate_runtime_pending"):
        raise ValueError(f"unrecognized compact geometry bank: {bank_root}")
    source = _inside_data_root(manifest["compact_source_manifest"]["path"]).parent
    codec = _inside_data_root(manifest["compact_codec_manifest"]["path"]).parent
    overrides = {row["loc_id"]: row for row in manifest.get("topology_overrides") or []}
    for row in overrides.values():
        path = _inside_data_root(row["path"])
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != row["sha256"]:
            raise ValueError(f"compact runtime override file hash drift: {path}")
    return source, codec, overrides


def _one(path: Path, *, columns: list[str], filters: dict,
         raw_geoparquet: bool = False) -> dict | None:
    frame = select_rows(path, columns=columns, exact_filters=filters, limit=2,
                        raw_geoparquet=raw_geoparquet)
    if frame.empty:
        return None
    if len(frame) != 1:
        raise ValueError(f"ambiguous compact geometry row: {path.name}/{filters}")
    return frame.iloc[0].to_dict()


def _bytes(value: object) -> bytes | None:
    if value is None or value is pd.NA:
        return None
    return bytes(value)


def _object(digest: str, baseline: Path, patches: Path,
            memo: dict[str, bytes], visiting: set[str]) -> bytes:
    if digest in memo:
        return memo[digest]
    if digest in visiting:
        raise ValueError(f"cyclic reverse geometry dependency: {digest}")
    visiting.add(digest)
    baseline_row = _one(baseline, columns=["geometry_hash", "geometry"],
                        filters={"geometry_hash": digest})
    if baseline_row is not None:
        result = _bytes(baseline_row["geometry"])
    else:
        matches = select_rows(
            patches,
            columns=["old_geometry_hash", "newer_geometry_hash", "codec", "payload"],
            exact_filters={"old_geometry_hash": digest},
        )
        if matches.empty:
            raise ValueError(f"compact WKB hash has no object or patch: {digest}")
        payloads = [row for row in matches.to_dict("records")
                    if row["codec"] != "existing-object-reference"]
        if not payloads:
            raise ValueError(f"compact WKB reference has no stored payload: {digest}")
        patch = payloads[0]
        codec = patch["codec"]
        payload = _bytes(patch["payload"])
        if payload is None:
            raise ValueError(f"missing reverse payload: {digest}")
        if codec == "zstd-standalone":
            result = zstd.ZstdDecompressor().decompress(payload)
        elif codec == "zstd-newer-dictionary":
            newer_hash = patch["newer_geometry_hash"]
            if not newer_hash:
                raise ValueError(f"missing newer shape dependency: {digest}")
            newer = _object(str(newer_hash), baseline, patches, memo, visiting)
            dictionary = zstd.ZstdCompressionDict(
                newer[:DICT_LIMIT], dict_type=zstd.DICT_TYPE_RAWCONTENT,
            )
            result = zstd.ZstdDecompressor(dict_data=dictionary).decompress(payload)
        else:
            raise ValueError(f"unsupported reverse geometry codec: {codec}")
    if result is None or hashlib.sha256(result).hexdigest() != digest:
        raise ValueError(f"decoded compact WKB hash mismatch: {digest}")
    visiting.remove(digest)
    memo[digest] = result
    return result


def read_compact_reverse_rows(bank_root: Path, loc_ids: list[str]) -> pd.DataFrame:
    """Resolve release-scoped history IDs to exact WKB and source attributes."""
    if not loc_ids:
        return pd.DataFrame()
    _source, codec, overrides = _paths(bank_root)
    index = select_rows(
        bank_root / "shapes/edition_index.parquet",
        columns=["loc_id", "year", "admin_level", "source_code",
                 "geometry_hash", "source_release_id"],
        in_filters={"loc_id": loc_ids},
    )
    if index.empty:
        return pd.DataFrame()
    baseline = codec / "current_geometry_objects.parquet"
    patches = codec / "reverse_geometry_patches.parquet"
    memo: dict[str, bytes] = {}
    rows = []
    for row in index.to_dict("records"):
        digest = str(row["geometry_hash"])
        geometry = _object(digest, baseline, patches, memo, set())
        override = overrides.get(row["loc_id"])
        if override is not None:
            if override["source_geometry_hash"] != digest:
                raise ValueError(f"runtime override source hash drift: {row['loc_id']}")
            override_path = _inside_data_root(override["path"])
            override_row = _one(override_path,
                                columns=["source_code", "year", "source_geometry_hash",
                                         "runtime_geometry_hash", "geometry"],
                                filters={"source_code": row["source_code"], "year": row["year"]},
                                raw_geoparquet=True)
            if override_row is None or override_row["source_geometry_hash"] != digest:
                raise ValueError(f"runtime override row missing: {row['loc_id']}")
            geometry = _bytes(override_row["geometry"])
            if (geometry is None or
                    hashlib.sha256(geometry).hexdigest() != override["runtime_geometry_hash"] or
                    override_row["runtime_geometry_hash"] != override["runtime_geometry_hash"]):
                raise ValueError(f"runtime override geometry hash drift: {row['loc_id']}")
        rows.append({"loc_id": row["loc_id"], "source_id": row["source_code"],
                     "source_release": row["source_release_id"],
                     "geometry": geometry})
    return pd.DataFrame(rows)
