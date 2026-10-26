"""Verified release discovery and download for the Windows launcher."""

from __future__ import annotations

import hashlib
import json
import os
import re
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

LAUNCHER_VERSION = "0.1.5"
PRODUCT = "daedalmap-launcher"
PLATFORM = "windows-x86_64"
CHANNEL = "stable"
PUBLIC_BASE_URL = "https://downloads.daedalmap.com"
RELEASE_PREFIX = f"downloadable/launcher/{PLATFORM}/{CHANNEL}"


class LauncherReleaseError(ValueError):
    pass


def is_newer_version(available: str, installed: str = LAUNCHER_VERSION) -> bool:
    pattern = r"(\d+)\.(\d+)\.(\d+)"
    next_match = re.fullmatch(pattern, available)
    local_match = re.fullmatch(pattern, installed)
    return bool(next_match and local_match and
                tuple(map(int, next_match.groups())) > tuple(map(int, local_match.groups())))


def _origin(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"https", "http"} or not parsed.netloc or parsed.username or parsed.password:
        raise LauncherReleaseError("Launcher release URL is invalid")
    return f"{parsed.scheme}://{parsed.netloc}"


def _same_origin(url: str, base_url: str) -> str:
    if _origin(url) != _origin(base_url):
        raise LauncherReleaseError("Launcher release URL changed origin")
    return url


def _read(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read(1024 * 1024 + 1)


def current_url(base_url: str = PUBLIC_BASE_URL) -> str:
    return f"{base_url.rstrip('/')}/{RELEASE_PREFIX}/current.json"


def check_release(base_url: str = PUBLIC_BASE_URL) -> dict:
    """Return the verified current release; a missing pointer means no release yet."""
    base_url = base_url.rstrip("/")
    if _origin(base_url) != "https://downloads.daedalmap.com" and os.environ.get("DAEDALMAP_LAUNCHER_DEV") != "1":
        raise LauncherReleaseError("Launcher release origin must be downloads.daedalmap.com")
    pointer_raw = _read(current_url(base_url))
    if len(pointer_raw) > 1024 * 1024:
        raise LauncherReleaseError("Launcher release pointer is too large")
    pointer = json.loads(pointer_raw)
    if pointer.get("product") != PRODUCT or pointer.get("platform") != PLATFORM or pointer.get("channel") != CHANNEL:
        raise LauncherReleaseError("Launcher release pointer has the wrong identity")
    version = str(pointer.get("current_version") or "")
    if not version or not all(c.isalnum() or c in ".-_" for c in version):
        raise LauncherReleaseError("Launcher release version is invalid")
    manifest_name = str(pointer.get("release_manifest") or "")
    if manifest_name != f"{version}/manifest.json":
        raise LauncherReleaseError("Launcher release pointer has an unexpected manifest path")
    manifest_url = _same_origin(f"{base_url}/{RELEASE_PREFIX}/{manifest_name}", base_url)
    manifest_raw = _read(manifest_url)
    expected_hash = str(pointer.get("release_manifest_sha256") or "").lower()
    if hashlib.sha256(manifest_raw).hexdigest() != expected_hash:
        raise LauncherReleaseError("Launcher release manifest checksum mismatch")
    manifest = json.loads(manifest_raw)
    if manifest.get("product") != PRODUCT or manifest.get("platform") != PLATFORM or manifest.get("version") != version:
        raise LauncherReleaseError("Launcher release manifest has the wrong identity")
    artifact = manifest.get("artifact") or {}
    filename = str(artifact.get("filename") or "")
    if filename != f"DaedalMapLauncher-{version}-{PLATFORM}.zip":
        raise LauncherReleaseError("Launcher release filename is invalid")
    artifact_url = _same_origin(str(artifact.get("download_url") or ""), base_url)
    if not artifact_url.endswith(f"/{RELEASE_PREFIX}/{version}/{filename}"):
        raise LauncherReleaseError("Launcher release artifact path is invalid")
    if not isinstance(artifact.get("size_bytes"), int) or artifact["size_bytes"] <= 0:
        raise LauncherReleaseError("Launcher release size is invalid")
    if len(str(artifact.get("sha256") or "")) != 64:
        raise LauncherReleaseError("Launcher release checksum is invalid")
    return manifest


def download_release(manifest: dict, destination: Path) -> Path:
    """Download into a staging file and activate only after size and hash match."""
    artifact = manifest["artifact"]
    filename = artifact["filename"]
    target = destination / filename
    destination.mkdir(parents=True, exist_ok=True)
    pending = destination / f".{filename}.{uuid.uuid4().hex}.tmp"
    digest = hashlib.sha256()
    size = 0
    try:
        with urllib.request.urlopen(artifact["download_url"], timeout=60) as response, pending.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                size += len(chunk)
                if size > artifact["size_bytes"]:
                    raise LauncherReleaseError("Launcher download exceeds its release size")
                digest.update(chunk)
                output.write(chunk)
        if size != artifact["size_bytes"] or digest.hexdigest() != artifact["sha256"]:
            raise LauncherReleaseError("Launcher download checksum or size mismatch")
        pending.replace(target)
    finally:
        pending.unlink(missing_ok=True)
    return target
