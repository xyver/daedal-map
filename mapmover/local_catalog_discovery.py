"""Discover local source trees that a user placed in the runtime data folder."""

from __future__ import annotations

import json
import re
import zipfile
from pathlib import Path

from .paths import CATALOG_PATH, DATA_ROOT


def ensure_empty_catalog() -> bool:
    """Create the first local catalog once; never replace an existing catalog."""
    if CATALOG_PATH.exists():
        return False
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with CATALOG_PATH.open("x", encoding="utf-8") as fh:
            json.dump({"sources": [], "packs": [], "total_sources": 0, "total_packs": 0,
                       "generated_by": "local_runtime"}, fh, indent=2)
    except FileExistsError:
        return False
    return True


def scan_unindexed_sources() -> list[dict]:
    """Find complete source roots absent from the current catalog, without changing them."""
    try:
        catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
        known = {str(source.get("source_id")) for source in catalog.get("sources", [])}
    except (OSError, ValueError):
        known = set()
    candidates = []
    for scope_root in (DATA_ROOT / "global", DATA_ROOT / "countries"):
        if not scope_root.is_dir():
            continue
        for metadata_path in scope_root.rglob("metadata.json"):
            if metadata_path.is_symlink():
                continue
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                source_id = str(metadata.get("source_id") or "")
                pack_id = str(metadata.get("pack_id") or "")
                primary = str(metadata.get("runtime_primary_file") or "")
                if not re.fullmatch(r"[A-Za-z0-9_-]+", source_id) or not re.fullmatch(r"[A-Za-z0-9_-]+", pack_id):
                    continue
                if source_id in known or not primary:
                    continue
                primary_path = (metadata_path.parent / primary).resolve()
                if not primary_path.is_relative_to(metadata_path.parent.resolve()) or not primary_path.is_file():
                    continue
                path = metadata_path.parent.relative_to(DATA_ROOT)
                candidates.append({"source_id": source_id, "pack_id": pack_id,
                                   "source_name": metadata.get("source_name") or source_id,
                                   "path": path.as_posix(), "scope": "global" if path.parts[0] == "global" else path.parts[1]})
            except (OSError, ValueError, TypeError):
                continue
    return sorted(candidates, key=lambda item: (item["pack_id"], item["source_id"]))


def scan_local_archives(*, include_installed: bool = False) -> list[dict]:
    """Find downloadable pack ZIPs placed at the data root or in its incoming folder."""
    from .pack_state import load_pack_state

    installed_versions = {entry["pack_id"]: str(entry.get("version") or "")
                          for entry in load_pack_state().get("installed_packs", [])}
    found = []
    for folder in (DATA_ROOT, DATA_ROOT / "incoming"):
        if not folder.is_dir():
            continue
        for path in folder.glob("*.zip"):
            if path.is_symlink():
                continue
            try:
                with zipfile.ZipFile(path) as archive:
                    manifest = json.loads(archive.read("data/manifest.json"))
                pack_id = str(manifest.get("pack_id") or "")
                if not re.fullmatch(r"[A-Za-z0-9_-]+", pack_id):
                    continue
                version = str(manifest.get("pack_version") or "local-import")
                if not include_installed and installed_versions.get(pack_id) == version:
                    continue
                found.append({"pack_id": pack_id, "filename": path.name,
                              "path": path.relative_to(DATA_ROOT).as_posix(),
                              "version": version,
                              "source_ids": manifest.get("source_ids") or []})
            except (OSError, ValueError, KeyError, zipfile.BadZipFile):
                continue
    return sorted(found, key=lambda item: item["path"])


def catalog_fragment_for_candidates(pack_id: str, candidates: list[dict]) -> dict:
    """Build a catalog fragment from reviewed on-disk metadata for one pack."""
    sources = []
    for candidate in candidates:
        if candidate["pack_id"] != pack_id:
            continue
        metadata = json.loads((DATA_ROOT / candidate["path"] / "metadata.json").read_text(encoding="utf-8"))
        source = dict(metadata)
        source["path"] = candidate["path"]
        source["scope"] = candidate["scope"]
        sources.append(source)
    if not sources:
        raise ValueError(f"No new local sources found for {pack_id}")
    pack = {"pack_id": pack_id, "pack_name": pack_id.replace("_", " ").title(),
            "source_ids": sorted(source["source_id"] for source in sources),
            "source_count": len(sources), "release_state": "local"}
    return {"sources": sources, "packs": [pack], "total_sources": len(sources), "total_packs": 1}
