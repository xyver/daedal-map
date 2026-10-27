"""Build a runtime catalog from a verified downloadable data pack."""

from __future__ import annotations

import json
import re
import shutil
import tempfile
from pathlib import Path, PurePosixPath

from .artifact_utils import sha256_file


def _relative_path(value: str) -> Path:
    raw = str(value or "").strip()
    posix = PurePosixPath(raw)
    if (
        not raw or posix.is_absolute() or str(posix) != raw
        or any(part in {"", ".", ".."} or ":" in part for part in posix.parts)
    ):
        raise RuntimeError(f"Invalid pack member path: {raw}")
    return Path(*posix.parts)


def build_local_pack_catalog(extract_root: Path, data_root: Path, version_manifest: dict) -> dict:
    """Verify package members, restore source paths, and write a local catalog."""
    pack_id = str(version_manifest.get("pack_id") or version_manifest.get("source_id") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", pack_id):
        raise RuntimeError("Downloadable pack has an invalid pack_id")
    package_manifest_path = extract_root / "data" / "manifest.json"
    if not package_manifest_path.is_file():
        raise RuntimeError("Downloaded pack has no data/manifest.json")
    package_manifest = json.loads(package_manifest_path.read_text(encoding="utf-8"))
    if str(package_manifest.get("pack_id") or "").strip() != pack_id:
        raise RuntimeError("Downloadable pack manifest identity does not match its release")

    for item in version_manifest.get("files") or []:
        member = extract_root / _relative_path(item.get("path"))
        if not member.is_file() or sha256_file(member).lower() != str(item.get("sha256") or "").lower():
            raise RuntimeError(f"Downloadable pack member failed verification: {item.get('path')}")

    source_paths: dict[str, Path] = {}
    for item in package_manifest.get("files") or []:
        source_id = str(item.get("source_id") or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]+", source_id):
            raise RuntimeError("Downloadable pack has an invalid source_id")
        source_path = _relative_path(item.get("source_path"))
        archive_path = _relative_path(item.get("archive_path"))
        if source_path.parts[0] not in {"global", "countries"} or archive_path.parts[:3] != ("data", "sources", source_id):
            raise RuntimeError("Downloadable pack has an unsupported source path")
        if source_path.parts[-1] != archive_path.parts[-1] or len(source_path.parts) < 3:
            raise RuntimeError("Downloadable pack source path does not match its archive member")
        # A source can contain nested partitions (for example admin2 tables).
        # Its catalog path is the root metadata member, not every file's parent.
        if source_path.name == "metadata.json" and archive_path.parts == ("data", "sources", source_id, "metadata.json"):
            prior = source_paths.setdefault(source_id, source_path.parent)
            if prior != source_path.parent:
                raise RuntimeError(f"Source {source_id} has inconsistent root metadata paths")
        member = extract_root / archive_path
        if not member.is_file():
            raise RuntimeError(f"Downloadable pack member is missing: {archive_path}")
        destination = data_root / source_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(member, destination)

    declared_source_ids = {str(value) for value in package_manifest.get("source_ids") or []}
    if not source_paths or declared_source_ids != set(source_paths):
        raise RuntimeError("Downloadable pack source inventory does not match its manifest")

    sources = []
    for source_id, source_dir in sorted(source_paths.items()):
        metadata_path = data_root / source_dir / "metadata.json"
        if not metadata_path.is_file():
            raise RuntimeError(f"Source metadata is missing for {source_id}")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if metadata.get("source_id") != source_id or metadata.get("pack_id") != pack_id:
            raise RuntimeError(f"Source metadata identity does not match {source_id}")
        source = dict(metadata)
        source["path"] = source_dir.as_posix()
        source["scope"] = "global" if source_dir.parts[0] == "global" else source_dir.parts[1]
        temporal = source.get("temporal_coverage") or {}
        source.setdefault("time_field", temporal.get("field"))
        source.setdefault("time_granularity", temporal.get("granularity"))
        sources.append(source)

    surfaces = sorted({surface for source in sources for surface in source.get("catalog_surfaces") or []})
    pack = {
        "pack_id": pack_id,
        "pack_name": str(package_manifest.get("pack_name") or pack_id),
        "pack_version": str(version_manifest.get("version") or package_manifest.get("pack_version") or ""),
        "source_ids": sorted(source_paths),
        "source_count": len(sources),
        "catalog_surfaces": surfaces,
        "release_state": "published",
        "license": package_manifest.get("pack_license"),
    }
    catalog = {"sources": sources, "packs": [pack], "total_sources": len(sources), "total_packs": 1}
    data_root.mkdir(parents=True, exist_ok=True)
    (data_root / "catalog.json").write_text(json.dumps(catalog, indent=2), encoding="utf-8")
    (data_root / "index.json").write_text(
        json.dumps({"pack_id": pack_id, "source_ids": sorted(source_paths)}, indent=2), encoding="utf-8"
    )
    (data_root / "package_manifest.json").write_text(json.dumps(package_manifest, indent=2), encoding="utf-8")
    return catalog


def import_local_pack_zip(zip_path: Path) -> dict:
    """Admit a user-placed package ZIP after explicit UI review."""
    from .artifact_utils import safe_extract_zip
    from .pack_manager import _build_manifest, install_pack_from_manifest
    from .paths import CACHE_DIR

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="local-pack-import-", dir=CACHE_DIR) as temporary:
        stage_root = Path(temporary)
        extract_root = stage_root / "extracted"
        safe_extract_zip(zip_path, extract_root)
        package_manifest = json.loads((extract_root / "data" / "manifest.json").read_text(encoding="utf-8"))
        pack_id = str(package_manifest.get("pack_id") or "").strip()
        version = str(package_manifest.get("pack_version") or "local-import").strip()
        catalog = build_local_pack_catalog(
            extract_root, stage_root / "data", {"pack_id": pack_id, "version": version, "files": []},
        )
        manifest, _manifest_hash = _build_manifest(pack_id, version, stage_root / "data", catalog)
        (stage_root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return install_pack_from_manifest(
            stage_root / "manifest.json", activate=True, replace_existing=True, adopt_unmanaged=True,
        )
