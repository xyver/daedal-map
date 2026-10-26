"""Discover local data folders and refresh the launcher's catalog in place."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from pathlib import Path

from local_data_store import _overlay_tree


SKIP_ROOTS = {"geometry", "networks", "incoming", "cache", "packs"}
CONTROL_FILES = {"metadata.json", "catalog.json", "index.json", "package_manifest.json", ".daedal_runtime_root.json"}


def _files(data_root: Path) -> list[Path]:
    files = []
    for parent, directories, names in os.walk(data_root, followlinks=False):
        here = Path(parent)
        if here == data_root:
            directories[:] = [name for name in directories if name not in SKIP_ROOTS]
        for name in directories + names:
            item = here / name
            if item.is_symlink() or (hasattr(item, "is_junction") and item.is_junction()):
                raise ValueError(f"Data folder contains a link requiring review: {item}")
        files.extend(here / name for name in names)
    return sorted(files)


def scan_data_folder(data_root: Path) -> tuple[list[dict], str]:
    """Return source records and a cheap fingerprint of data and metadata changes."""
    data_root = Path(data_root).resolve()
    data_root.mkdir(parents=True, exist_ok=True)
    files = _files(data_root)
    digest = hashlib.sha256()
    metadata = []
    for path in files:
        relative = path.relative_to(data_root)
        if path.name != "metadata.json" and path.suffix.lower() not in {".parquet", ".geojson", ".csv"}:
            continue
        stat = path.stat()
        digest.update(f"{relative.as_posix()}\0{stat.st_size}\0{stat.st_mtime_ns}\n".encode())
        if path.name == "metadata.json":
            value = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(value, dict):
                raise ValueError(f"Data metadata must be a JSON object: {path}")
            if value.get("source_id"):
                metadata.append((path, value))

    candidates: dict[str, tuple[Path, dict]] = {}
    for path, value in sorted(metadata, key=lambda pair: (not bool(pair[1].get("pack_id")), len(pair[0].parts))):
        source_id = str(value["source_id"]).strip()
        prior = candidates.get(source_id)
        if prior:
            if path.is_relative_to(prior[0].parent):
                continue  # Detail metadata inside an existing source folder.
            raise ValueError(f"Two folders declare source_id {source_id}: {prior[0]} and {path}")
        candidates[source_id] = (path, value)

    sources = []
    for source_id, (path, value) in sorted(candidates.items()):
        relative = path.parent.relative_to(data_root)
        source = dict(value)
        source["source_id"] = source_id
        source["source_name"] = str(source.get("source_name") or source_id.replace("_", " ").title())
        source["pack_id"] = str(source.get("pack_id") or f"local_{source_id}")
        source["path"] = relative.as_posix()
        source["scope"] = (relative.parts[1].lower() if len(relative.parts) > 1 and relative.parts[0] == "countries"
                           else str(source.get("scope") or "global"))
        source["runtime_data_root"] = str(data_root)
        source["local_folder"] = True
        source.setdefault("catalog_surfaces", ["research"])
        temporal = source.get("temporal_coverage") or {}
        if isinstance(temporal, dict):
            source.setdefault("time_field", temporal.get("field"))
            source.setdefault("time_granularity", temporal.get("granularity"))
        sources.append(source)

    # A simple top-level Parquet folder also appears in the catalog. Richer
    # metadata can be added later in metadata.json without moving its files.
    covered = {Path(source["path"]).parts[0] for source in sources if source["path"] != "."}
    for folder in sorted(data_root.iterdir()):
        if not folder.is_dir() or folder.name in SKIP_ROOTS | {"global", "countries"} or folder.name in covered:
            continue
        if not any(path.is_relative_to(folder) and path.suffix.lower() == ".parquet" for path in files):
            continue
        source_id = re.sub(r"[^a-z0-9_]+", "_", folder.name.lower()).strip("_")
        if not source_id or any(source["source_id"] == source_id for source in sources):
            raise ValueError(f"Data folder name needs a distinct source ID: {folder}")
        sources.append({"source_id": source_id, "source_name": folder.name, "pack_id": f"local_{source_id}",
                        "path": folder.name, "scope": "global", "data_type": "table",
                        "catalog_surfaces": ["research"], "runtime_data_root": str(data_root),
                        "local_folder": True})
    return sorted(sources, key=lambda source: source["source_id"]), digest.hexdigest()


def catalog_needs_refresh(data_root: Path, snapshot_path: Path, sources: list[dict], fingerprint: str) -> bool:
    try:
        catalog = json.loads((data_root / "catalog.json").read_text(encoding="utf-8-sig"))
        current = {(source["source_id"], source["path"], source.get("runtime_data_root"))
                   for source in catalog.get("sources", [])}
    except (OSError, ValueError, KeyError, TypeError):
        return True
    desired = {(source["source_id"], source["path"], source["runtime_data_root"]) for source in sources}
    if current != desired:
        return True
    try:
        saved = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False  # An existing matching catalog is the initial baseline.
    return saved.get("fingerprint") != fingerprint


def save_snapshot(snapshot_path: Path, fingerprint: str) -> None:
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = snapshot_path.with_name(f".{snapshot_path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps({"fingerprint": fingerprint}), encoding="utf-8")
    os.replace(temporary, snapshot_path)


def refresh_catalog(data_root: Path, snapshot_path: Path, sources: list[dict], fingerprint: str) -> dict:
    data_root = Path(data_root).resolve()
    catalog_path = data_root / "catalog.json"
    try:
        previous = json.loads(catalog_path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        previous = {}
    prior_packs = {pack.get("pack_id"): pack for pack in previous.get("packs", []) if isinstance(pack, dict)}
    grouped: dict[str, list[dict]] = {}
    for source in sources:
        grouped.setdefault(source["pack_id"], []).append(source)
    packs = []
    for pack_id, members in sorted(grouped.items()):
        pack = dict(prior_packs.get(pack_id) or {})
        pack.update({"pack_id": pack_id, "pack_name": pack.get("pack_name") or pack_id.replace("_", " ").title(),
                     "source_ids": sorted(source["source_id"] for source in members),
                     "source_count": len(members)})
        if pack_id.startswith("local_"):
            pack.setdefault("release_state", "local")
            pack.setdefault("catalog_surfaces", ["research"])
        packs.append(pack)
    catalog = {"catalog_version": 1, "sources": sources, "packs": packs,
               "total_sources": len(sources), "total_packs": len(packs),
               "overlay_tree": _overlay_tree(sources),
               "runtime_pack_state": {"catalog_mode": "unmanaged_data_root",
                                      "active_pack_ids": sorted(grouped)}}
    temporary = catalog_path.with_name(f".{catalog_path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(catalog, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, catalog_path)
    save_snapshot(snapshot_path, fingerprint)
    return catalog
