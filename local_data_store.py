"""Standard-library data-pack installer shared by launcher-managed local tools."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse


CONTROLS = {"catalog.json", "index.json", "package_manifest.json", ".daedal_runtime_root.json"}
TOKEN = re.compile(r"[A-Za-z0-9_-]+\Z")


def _stamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _relative(value: str) -> Path:
    raw = str(value or "")
    posix = PurePosixPath(raw)
    if (not raw or "\\" in raw or posix.is_absolute() or str(posix) != raw
            or any(part in {".", ".."} or ":" in part for part in posix.parts)):
        raise ValueError(f"Unsafe pack path: {raw}")
    return Path(*posix.parts)


def _json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def ensure_empty_catalog(data_root: Path) -> bool:
    """Make a readable empty store without touching user-provided files."""
    data_root.mkdir(parents=True, exist_ok=True)
    catalog = data_root / "catalog.json"
    if catalog.exists():
        _json(catalog)  # Invalid user catalogs need review, not replacement.
        return False
    _write_json(catalog, {
        "catalog_version": 1, "generated_by": "local_runtime", "sources": [],
        "packs": [], "total_sources": 0, "total_packs": 0, "overlay_tree": {},
    })
    return True


def _legacy_package(extract_root: Path, version: dict) -> dict:
    """Translate a verified older ZIP into source mappings; never admit its catalog."""
    legacy = _json(extract_root / "data" / "catalog.json")
    pack_id = str(version.get("pack_id") or "")
    sources = [source for source in legacy.get("sources") or [] if isinstance(source, dict)]
    if not sources or any(source.get("pack_id") != pack_id for source in sources):
        raise ValueError("Legacy pack catalog identity differs from published download")
    files = []
    source_ids = []
    for source in sources:
        source_id = str(source.get("source_id") or "")
        source_root = _relative(source.get("path"))
        archive_root = extract_root / "data" / "sources" / source_id
        if (not TOKEN.fullmatch(source_id) or source_root.parts[0] not in {"global", "countries"}
                or not (archive_root / "metadata.json").is_file()):
            raise ValueError("Legacy pack has invalid source mapping")
        source_ids.append(source_id)
        for member in archive_root.rglob("*"):
            if member.is_file():
                relative = member.relative_to(archive_root)
                files.append({"source_id": source_id,
                              "source_path": (source_root / relative).as_posix(),
                              "archive_path": member.relative_to(extract_root).as_posix()})
    return {"pack_id": pack_id, "pack_version": version.get("version"),
            "pack_name": pack_id.replace("_", " ").title(),
            "source_ids": source_ids, "files": files}


def _extract_verified(archive_path: Path, extract_root: Path, version: dict) -> dict:
    with zipfile.ZipFile(archive_path) as archive:
        names: set[str] = set()
        for member in archive.infolist():
            relative = _relative(member.filename.rstrip("/"))
            if member.filename in names or (member.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError(f"Duplicate or symbolic-link pack member: {member.filename}")
            names.add(member.filename)
            if member.is_dir():
                continue
            dest = extract_root / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, dest.open("wb") as output:
                shutil.copyfileobj(source, output)
    declared = version.get("files")
    if not isinstance(declared, list) or not declared:
        raise ValueError("Pack release has no verified member inventory")
    verified: set[str] = set()
    for entry in declared:
        member_path = _relative(entry.get("path"))
        verified.add(member_path.as_posix())
        member = extract_root / member_path
        expected = str(entry.get("sha256") or "").lower()
        if not expected or not member.is_file() or _hash(member) != expected:
            raise ValueError(f"Pack member failed verification: {entry.get('path')}")
    archived_files = {name for name in names if not name.endswith("/")}
    if verified != archived_files:
        raise ValueError("Pack archive members differ from verified inventory")
    package_path = extract_root / "data" / "manifest.json"
    if package_path.is_file():
        return _json(package_path)
    if version.get("legacy_catalog_archive") and (extract_root / "data" / "catalog.json").is_file():
        return _legacy_package(extract_root, version)
    raise ValueError("Pack ZIP has no supported source manifest")


def install_verified_data_zip(archive_path: Path, storage_root: Path, *, pack_id: str,
                              release: dict, log=lambda _: None) -> dict:
    """Install a ZIP matched to the saved Downloads catalog without network reads."""
    archive_path = Path(archive_path)
    package_id = str(release.get("packageId") or "")
    version = str(release.get("version") or "")
    expected_sha = str(release.get("sha256") or "").lower()
    expected_size = release.get("size")
    if (not TOKEN.fullmatch(pack_id) or not TOKEN.fullmatch(package_id)
            or not version or not isinstance(expected_size, int)
            or not re.fullmatch(r"[0-9a-f]{64}", expected_sha)
            or archive_path.stat().st_size != expected_size or _hash(archive_path) != expected_sha):
        raise ValueError("ZIP does not match its published Downloads record")
    members = []
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            with archive.open(member) as source:
                digest = hashlib.sha256()
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(block)
            members.append({"path": member.filename, "sha256": digest.hexdigest()})
    manifest = {"pack_id": pack_id, "version": version,
                "artifact": {"sha256": expected_sha, "size_bytes": expected_size,
                             "download_url": archive_path.as_uri()},
                "files": members, "legacy_catalog_archive": True}
    pointer = {"pack_id": pack_id, "current_version": version,
               "version_manifest_url": "local-verified-version"}
    return install_data_pack("local-verified-current", storage_root,
                             fetch_json=lambda key: pointer if key == "local-verified-current" else manifest,
                             download=lambda _url, _path: None,
                             archive_path=archive_path, log=log)


def _prepare_pack_data(extract_root: Path, pack_root: Path, package: dict, version: dict) -> dict:
    pack_id = str(version.get("pack_id") or version.get("source_id") or "")
    if not TOKEN.fullmatch(pack_id) or package.get("pack_id") != pack_id:
        raise ValueError("Pack release and package identities differ")
    if package.get("pack_version") not in (None, version.get("version")):
        raise ValueError("Pack release and package versions differ")
    data = pack_root / "data"
    sources: dict[str, Path] = {}
    reference_geometry_path: Path | None = None
    for entry in package.get("files") or []:
        source_id = str(entry.get("source_id") or "")
        source_path = _relative(entry.get("source_path"))
        archive_path = _relative(entry.get("archive_path"))
        if entry.get("reference_geometry") is True:
            if (reference_geometry_path is not None or source_id != "admitted_reference_geometry"
                    or archive_path.parts != ("data", "geometry", "geometry.parquet")
                    or source_path.parts[:1] != ("geometry",)):
                raise ValueError("Invalid reference geometry mapping in pack")
            member = extract_root / archive_path
            if not member.is_file():
                raise ValueError(f"Missing reference geometry member: {archive_path}")
            reference_geometry_path = Path("geometry") / "data_packs" / pack_id / "geometry.parquet"
            target = data / reference_geometry_path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(member, target)
            continue
        if (not TOKEN.fullmatch(source_id) or source_path.parts[0] not in {"global", "countries"}
                or archive_path.parts[:3] != ("data", "sources", source_id)
                or source_path.name != archive_path.name or len(source_path.parts) < 3):
            raise ValueError("Invalid source mapping in pack")
        member = extract_root / archive_path
        if not member.is_file():
            raise ValueError(f"Missing source member: {archive_path}")
        dest = data / source_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(member, dest)
        if source_path.name == "metadata.json" and archive_path.parts == ("data", "sources", source_id, "metadata.json"):
            if source_id in sources and sources[source_id] != source_path.parent:
                raise ValueError(f"Conflicting source path: {source_id}")
            sources[source_id] = source_path.parent
    if not sources or set(sources) != set(package.get("source_ids") or []):
        raise ValueError("Pack source inventory differs from package manifest")
    catalog_sources = []
    for source_id, source_path in sorted(sources.items()):
        metadata = _json(data / source_path / "metadata.json")
        if metadata.get("source_id") != source_id or metadata.get("pack_id") != pack_id:
            raise ValueError(f"Source identity mismatch: {source_id}")
        source = dict(metadata)
        source["path"] = source_path.as_posix()
        source["scope"] = "global" if source_path.parts[0] == "global" else source_path.parts[1]
        temporal = source.get("temporal_coverage") or {}
        source.setdefault("time_field", temporal.get("field"))
        source.setdefault("time_granularity", temporal.get("granularity"))
        catalog_sources.append(source)
    pack = {
        "pack_id": pack_id, "pack_name": package.get("pack_name") or pack_id,
        "pack_version": version.get("version"), "source_ids": sorted(sources),
        "source_count": len(sources), "release_state": "published",
        "catalog_surfaces": sorted({v for s in catalog_sources for v in s.get("catalog_surfaces") or []}),
        "license": package.get("pack_license"),
    }
    if reference_geometry_path is not None:
        pack["reference_geometry_path"] = reference_geometry_path.as_posix()
    fragment = {"sources": catalog_sources, "packs": [pack], "total_sources": len(catalog_sources), "total_packs": 1}
    _write_json(data / "catalog.json", fragment)
    _write_json(data / "index.json", {"pack_id": pack_id, "source_ids": sorted(sources)})
    _write_json(data / "package_manifest.json", package)
    return fragment


def _overlay_tree(sources: list[dict]) -> dict:
    tree: dict = {}
    for source in sources:
        overlay = source.get("overlay")
        path = overlay if isinstance(overlay, str) else overlay.get("path") if isinstance(overlay, dict) else ""
        parts = [part for part in str(path or "").split("/") if part]
        if not parts:
            continue
        branch = tree
        for part in parts[:-1]:
            branch = branch.setdefault(part, {"label": part.replace("_", " ").title(), "children": {}})["children"]
        leaf = parts[-1]
        label = overlay.get("label") if isinstance(overlay, dict) else None
        branch.setdefault(leaf, {"label": label or leaf.replace("_", " ").title(), "sources": []})["sources"].append({
            key: source.get(key) for key in ("source_id", "pack_id", "scope", "data_type", "geojson_shape",
                                        "geometry_family", "display_contract", "default_load", "default_question", "default_response")
        })
    return tree


def _materialize(data_root: Path, entries: list[dict], active_ids: list[str], stage_root: Path,
                 old_entries: list[dict]) -> dict:
    old_owned: set[str] = set()
    for entry in old_entries:
        old_data = Path(entry["data_root"])
        if old_data.is_dir():
            old_owned.update(path.relative_to(old_data).as_posix() for path in old_data.rglob("*") if path.is_file())
    if data_root.exists():
        for source in data_root.rglob("*"):
            if source.is_symlink():
                raise ValueError(f"Shared data contains a symbolic link requiring review: {source}")
            if not source.is_file():
                continue
            relative = source.relative_to(data_root)
            if relative.as_posix() in old_owned or relative.as_posix() in CONTROLS:
                continue
            dest = stage_root / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, dest)
    sources: list[dict] = []
    packs: list[dict] = []
    seen_sources: set[str] = set()
    for entry in entries:
        if entry["pack_id"] not in active_ids:
            continue
        pack_data = Path(entry["data_root"])
        fragment = _json(pack_data / "catalog.json")
        for source in fragment.get("sources") or []:
            source_id = source.get("source_id")
            if not source_id or source_id in seen_sources:
                raise ValueError(f"Duplicate source identity: {source_id}")
            seen_sources.add(source_id)
            source = dict(source)
            source["runtime_data_root"] = str(pack_data)
            sources.append(source)
        packs.extend(fragment.get("packs") or [])
        for member in pack_data.rglob("*"):
            if not member.is_file():
                continue
            relative = member.relative_to(pack_data)
            if relative.as_posix() in CONTROLS:
                continue
            dest = stage_root / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.exists() and _hash(dest) != _hash(member):
                raise ValueError(f"Pack path conflicts with existing data: {relative}")
            if not dest.exists():
                try:
                    dest.hardlink_to(member)
                except OSError:
                    shutil.copy2(member, dest)
    catalog = {
        "catalog_version": 1, "sources": sources, "packs": packs,
        "total_sources": len(sources), "total_packs": len(packs),
        "overlay_tree": _overlay_tree(sources),
        "runtime_pack_state": {"catalog_mode": "managed_packs", "installed_pack_ids": [e["pack_id"] for e in entries],
                               "active_pack_ids": active_ids},
    }
    _write_json(stage_root / "catalog.json", catalog)
    _write_json(stage_root / "index.json", {"version": 1, "catalog_mode": "managed_packs", "generated_at": _stamp(),
                                           "active_pack_ids": active_ids, "source_count": len(sources)})
    _write_json(stage_root / ".daedal_runtime_root.json", {"version": 1, "catalog_mode": "managed_packs",
                                                        "active_pack_ids": active_ids, "updated_at": _stamp()})
    return catalog


def install_data_pack(current_url: str, storage_root: Path, *, fetch_json, download,
                      archive_path: Path | None = None, log=lambda _: None) -> dict:
    """Install a published data ZIP without requiring Mapping or an MCP runtime."""
    storage_root = Path(storage_root)
    data_root = storage_root / "data"
    packs_root = storage_root / "packs"
    state_path = storage_root / "state" / "pack_state.json"
    cache_root = storage_root / "cache"
    for root in (data_root, packs_root, cache_root, state_path.parent):
        root.mkdir(parents=True, exist_ok=True)
    ensure_empty_catalog(data_root)
    existing_catalog = _json(data_root / "catalog.json")
    marker = data_root / ".daedal_runtime_root.json"
    if state_path.exists():
        if not marker.is_file():
            raise ValueError("Pack state exists without its managed data marker; review the data store before installing")
    elif marker.exists() or existing_catalog.get("generated_by") != "local_runtime" or existing_catalog.get("sources"):
        raise ValueError("Existing data catalog requires review before launcher-managed installation")
    current = fetch_json(current_url)
    version_url = str(current.get("version_manifest_url") or "")
    if not version_url:
        raise ValueError("Pack pointer has no version manifest URL")
    version = fetch_json(version_url)
    pack_id = str(version.get("pack_id") or version.get("source_id") or "")
    pack_version = str(version.get("version") or "")
    artifact = version.get("artifact") or {}
    expected_sha = str(artifact.get("sha256") or "").lower()
    if (not TOKEN.fullmatch(pack_id) or not TOKEN.fullmatch(pack_version.replace(".", "_"))
            or not re.fullmatch(r"[0-9a-f]{64}", expected_sha) or not artifact.get("download_url")):
        raise ValueError("Pack release has incomplete identity or artifact verification")
    if current.get("pack_id") not in (None, pack_id):
        raise ValueError("Pack pointer identity differs from release")
    if current.get("current_version") not in (None, pack_version):
        raise ValueError("Pack pointer version differs from release")
    origin = urlparse(current_url)
    if origin.scheme in {"http", "https"}:
        for candidate in (version_url, str(artifact["download_url"])):
            target = urlparse(candidate)
            if origin.scheme != "https" or target.scheme != "https" or target.netloc != origin.netloc:
                raise ValueError("Pack release URL is outside its HTTPS download origin")
    expected_size = artifact.get("size_bytes")
    with tempfile.TemporaryDirectory(prefix="pack-download-", dir=cache_root) as temporary:
        work = Path(temporary)
        archive = work / "pack.zip"
        if archive_path is None:
            log(f"Downloading {pack_id} {pack_version}")
            download(str(artifact["download_url"]), archive)
        else:
            source = Path(archive_path)
            if not source.is_file():
                raise ValueError(f"Data pack ZIP is missing: {source}")
            log(f"Using downloaded ZIP for {pack_id} {pack_version}")
            shutil.copy2(source, archive)
        if expected_size is not None and archive.stat().st_size != int(expected_size):
            raise ValueError("Pack artifact size mismatch")
        if _hash(archive) != expected_sha:
            raise ValueError("Pack artifact SHA-256 mismatch")
        extract_root = work / "extract"
        package = _extract_verified(archive, extract_root, version)
        prepared = work / "prepared"
        fragment = _prepare_pack_data(extract_root, prepared, package, version)
        old_state = _json(state_path) if state_path.exists() else {"installed_packs": [], "active_pack_ids": []}
        old_entries = [dict(item) for item in old_state.get("installed_packs") or []]
        old_active = [str(item) for item in old_state.get("active_pack_ids") or []]
        install_root = packs_root / pack_id
        old_install = packs_root / f".{pack_id}.previous.{uuid.uuid4().hex}"
        old_data = data_root.resolve()
        staged_data = Path(tempfile.mkdtemp(prefix=".daedal_stage_", dir=old_data.parent))
        data_backup = old_data.parent / f".daedal_pre_materialize_{uuid.uuid4().hex}"
        replaced_install = False
        replaced_data = False
        try:
            if install_root.exists():
                install_root.rename(old_install)
                for prior in old_entries:
                    if prior.get("pack_id") == pack_id:
                        prior["data_root"] = str(old_install / "data")
            prepared.rename(install_root)
            replaced_install = True
            entries = [item for item in old_entries if item.get("pack_id") != pack_id]
            entry = {"pack_id": pack_id, "version": pack_version, "install_root": str(install_root),
                     "data_root": str(install_root / "data"), "catalog_path": str(install_root / "data" / "catalog.json"),
                     "manifest_path": str(install_root / "data" / "package_manifest.json"),
                     "manifest_hash": _hash(install_root / "data" / "package_manifest.json"),
                     "source_ids": [item["source_id"] for item in fragment["sources"]],
                     "installed_at": _stamp(), "status": "installed"}
            entries.append(entry)
            active = list(dict.fromkeys([*old_active, pack_id]))
            catalog = _materialize(old_data, entries, active, staged_data, old_entries)
            old_data.rename(data_backup)
            staged_data.rename(old_data)
            replaced_data = True
            state = {"version": 1, "catalog_mode": "managed_packs", "installed_packs": entries,
                     "active_pack_ids": active, "updated_at": _stamp()}
            _write_json(state_path, state)
        except Exception:
            if replaced_data and data_backup.exists():
                failed_data = old_data.parent / f".daedal_failed_install_{uuid.uuid4().hex}"
                old_data.rename(failed_data)
                data_backup.rename(old_data)
            elif data_backup.exists() and not old_data.exists():
                data_backup.rename(old_data)
            if replaced_install and install_root.exists():
                failed_pack = packs_root / f".{pack_id}.failed_install.{uuid.uuid4().hex}"
                install_root.rename(failed_pack)
            if old_install.exists() and not install_root.exists():
                old_install.rename(install_root)
            raise
        finally:
            if staged_data.exists():
                shutil.rmtree(staged_data)
        for recovery in (data_backup, old_install):
            if recovery.exists():
                try:
                    shutil.rmtree(recovery)
                except OSError:
                    log(f"Previous install retained at {recovery}")
        return {"pack_id": pack_id, "version": pack_version, "source_count": catalog["total_sources"],
                "data_root": str(data_root), "catalog_path": str(data_root / "catalog.json")}
