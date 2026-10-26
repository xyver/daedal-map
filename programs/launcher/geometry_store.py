"""Verified local installation of public Geometry download ZIPs."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import quote


class GeometryStoreError(ValueError):
    pass


TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
SHA = re.compile(r"[0-9a-f]{64}\Z")
GEOMETRY_TIERS = ("lite", "display", "full")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def releases_from_storefront(catalog: dict) -> list[dict]:
    categories = catalog.get("categories") if isinstance(catalog, dict) else None
    entries = categories.get("geometry") if isinstance(categories, dict) else None
    if not isinstance(entries, list):
        raise GeometryStoreError("Storefront catalog is missing its Geometry list")
    releases = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        package_id = str(entry.get("packageId") or "")
        version = str(entry.get("version") or "")
        artifact_url = str(entry.get("artifactUrl") or "")
        checksum = str(entry.get("sha256") or "").lower()
        size = entry.get("size")
        if (not TOKEN.fullmatch(package_id) or not TOKEN.fullmatch(version)
                or not SHA.fullmatch(checksum) or type(size) is not int or size <= 0
                or not artifact_url.startswith("https://downloads.daedalmap.com/downloadable/geometry/")
                or not artifact_url.lower().endswith(".zip")):
            continue
        releases.append(dict(entry, packageId=package_id, version=version, sha256=checksum))
    return releases


def group_geometry_releases(releases: list[dict], families: list[dict] | None = None) -> list[dict]:
    """Project edition records into one catalog card per release unit."""
    groups: dict[tuple[str, str], dict] = {}
    for release in releases:
        section = str(release.get("geometrySection") or "countries")
        unit = str(release.get("countryCode") or release.get("releaseUnitId")
                   or release.get("packageId") or "").upper()
        tier = str(release.get("tier") or "").lower()
        if not unit or tier not in GEOMETRY_TIERS:
            continue
        key = (section, unit)
        group = groups.setdefault(key, {"section": section, "unit": unit, "tiers": {}})
        if tier in group["tiers"]:
            raise GeometryStoreError(f"Duplicate {tier} Geometry edition for {unit}")
        group["tiers"][tier] = release
    for group in groups.values():
        example = group["tiers"].get("lite") or next(iter(group["tiers"].values()))
        title = str(example.get("countryName") or example.get("title") or group["unit"])
        if group["section"] == "bridges":
            title = re.sub(r"\s+loc_id\b", "", title, flags=re.IGNORECASE)
        else:
            title = re.sub(r"(?: Reference Lite| Geometry (?:Lite|Display|Detail)| (?:Lite|Display|Detail))$", "", title)
        group["title"] = title
        group["profile_id"] = str(example.get("profileId") or "").strip().lower()
        group["families"] = []
        for family in families or []:
            code = str(family.get("country_code") or family.get("release_unit_id") or "").upper()
            if code == group["unit"]:
                if group["section"] != "bridges":
                    group["title"] = str(family.get("label") or title)
                group["profile_id"] = str(family.get("profile_id") or group["profile_id"]).strip().lower()
                group["families"] = [str(item.get("label") or item.get("family_id") or "")
                                     for item in family.get("geometry_families") or [] if isinstance(item, dict)]
                break
    return sorted(groups.values(), key=lambda group: (
        {"countries": 0, "global": 1, "bridges": 2}.get(group["section"], 3), group["title"]))


def geometry_info_url(group: dict) -> str:
    """Link a Geometry card to its public details, or its filtered download entry."""
    if group.get("section") == "bridges":
        return "https://www.daedalmap.com/docs/external-bridges"
    profile_id = str(group.get("profile_id") or "").strip().lower()
    if re.fullmatch(r"[a-z0-9][a-z0-9-]*", profile_id):
        return f"https://www.daedalmap.com/geometry/{profile_id}"
    return ("https://www.daedalmap.com/downloadable/?category=geometry&q="
            + quote(str(group.get("title") or group.get("unit") or "")))


def installed_geometry(data_root: Path) -> dict[str, dict]:
    root = Path(data_root) / "geometry_downloads"
    result = {}
    if not root.is_dir():
        return result
    for sidecar in root.glob("*/geometry.json"):
        try:
            payload = json.loads(sidecar.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        package_id = str(payload.get("package_id") or "")
        if package_id == sidecar.parent.name and payload.get("profile") == "downloadable_geometry_package":
            result[package_id] = {"version": str(payload.get("version") or ""), "path": str(sidecar.parent),
                                  "tier": str(payload.get("package_tier") or ""),
                                  "countryCode": str(payload.get("country_code") or payload.get("release_unit_id") or "")}
    return result


def install_geometry_zip(archive_path: Path, data_root: Path, release: dict) -> Path:
    """Install one catalog-pinned ZIP atomically in the shared, GIS-readable data root."""
    archive_path = Path(archive_path)
    data_root = Path(data_root)
    package_id = str(release.get("packageId") or "")
    version = str(release.get("version") or "")
    checksum = str(release.get("sha256") or "").lower()
    size = release.get("size")
    if (not TOKEN.fullmatch(package_id) or not TOKEN.fullmatch(version)
            or not SHA.fullmatch(checksum) or type(size) is not int or size <= 0):
        raise GeometryStoreError("Invalid published Geometry release")
    if not archive_path.is_file() or archive_path.stat().st_size != size or _sha256(archive_path) != checksum:
        raise GeometryStoreError("Geometry ZIP size or SHA-256 differs from Downloads")
    root = data_root / "geometry_downloads"
    root.mkdir(parents=True, exist_ok=True)
    destination = root / package_id
    stage = Path(tempfile.mkdtemp(prefix=".geometry-stage-", dir=data_root.parent))
    backup = data_root.parent / (".geometry-backup-" + package_id)
    if backup.exists():
        raise GeometryStoreError(f"Previous Geometry recovery folder needs review: {backup}")
    moved_old = False
    try:
        with zipfile.ZipFile(archive_path) as archive:
            members = archive.infolist()
            names = [member.filename for member in members if not member.is_dir()]
            if len(names) != len(set(names)) or "geometry.json" not in names:
                raise GeometryStoreError("Geometry ZIP has duplicate members or no geometry.json")
            for member in members:
                target = (stage / member.filename).resolve()
                if not target.is_relative_to(stage.resolve()):
                    raise GeometryStoreError(f"Unsafe Geometry ZIP member: {member.filename}")
            archive.extractall(stage)
        sidecar = json.loads((stage / "geometry.json").read_text(encoding="utf-8"))
        if (sidecar.get("profile") != "downloadable_geometry_package"
                or sidecar.get("package_id") != package_id
                or str(sidecar.get("version") or "") != version
                or sidecar.get("package_tier") not in {"lite", "display", "detail"}
                or not isinstance(sidecar.get("files"), list)):
            raise GeometryStoreError("Geometry ZIP identity or sidecar is invalid")
        for item in sidecar["files"]:
            name = str(item.get("archive_path") or "")
            target = (stage / name).resolve()
            if not name or not target.is_relative_to(stage.resolve()) or not target.is_file():
                raise GeometryStoreError(f"Missing or unsafe Geometry member: {name}")
            if (target.stat().st_size != item.get("size_bytes")
                    or _sha256(target) != str(item.get("sha256") or "").lower()):
                raise GeometryStoreError(f"Geometry member hash or size mismatch: {name}")
        if destination.exists():
            os.replace(destination, backup)
            moved_old = True
        try:
            os.replace(stage, destination)
        except Exception:
            if moved_old:
                os.replace(backup, destination)
                moved_old = False
            raise
        if moved_old:
            shutil.rmtree(backup)
        return destination
    finally:
        if stage.exists():
            shutil.rmtree(stage)
