"""
Pack artifact staging/downloader helpers.

This module downloads or copies a staged pack artifact into CACHE_DIR so the
runtime can verify it and then install it through the manifest-driven path.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

from .paths import CACHE_DIR, ensure_dir


PACK_STAGING_ROOT = CACHE_DIR / "pack-staging"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _looks_like_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https", "file"}


def _resolve_local_source(ref: str) -> Path:
    parsed = urlparse(ref)
    if parsed.scheme == "file":
        return Path(parsed.path)
    return Path(ref)


def _download_to_path(source: str, dest: Path) -> None:
    ensure_dir(dest.parent)
    if _looks_like_url(source):
        parsed = urlparse(source)
        if parsed.scheme == "file":
            shutil.copy2(_resolve_local_source(source), dest)
            return
        with requests.get(source, stream=True, timeout=120) as response:
            response.raise_for_status()
            with dest.open("wb") as fh:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)
        return
    shutil.copy2(_resolve_local_source(source), dest)


def _load_json_from_ref(ref: str) -> dict:
    if _looks_like_url(ref) and urlparse(ref).scheme in {"http", "https"}:
        response = requests.get(ref, timeout=60)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else {}
    source_path = _resolve_local_source(ref)
    with source_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    return data if isinstance(data, dict) else {}


def _default_artifact_base(manifest_ref: str) -> str:
    if _looks_like_url(manifest_ref):
        if manifest_ref.endswith("manifest.json"):
            return manifest_ref[: -len("manifest.json")] + "data/"
        return manifest_ref.rstrip("/") + "/data/"
    manifest_path = _resolve_local_source(manifest_ref)
    return str(manifest_path.parent / "data")


def stage_pack_artifact(manifest_ref: str, artifact_base_ref: str | None = None) -> dict:
    manifest = _load_json_from_ref(manifest_ref)
    pack_id = str(manifest.get("pack_id") or "").strip()
    pack_version = str(manifest.get("pack_version") or manifest.get("version") or "").strip() or "unknown"
    if not pack_id:
        raise RuntimeError(f"Manifest missing pack_id: {manifest_ref}")

    stage_root = PACK_STAGING_ROOT / pack_id / pack_version
    if stage_root.exists():
        shutil.rmtree(stage_root)
    data_root = stage_root / "data"
    ensure_dir(data_root)

    manifest_path = stage_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    artifact_base_ref = artifact_base_ref or manifest.get("artifact_base_url") or _default_artifact_base(manifest_ref)

    downloaded_files = []
    for file_info in manifest.get("files", []):
        rel_path = str(file_info.get("path") or "").strip()
        if not rel_path:
            continue
        dest_path = data_root / rel_path.replace("/", "\\")
        if _looks_like_url(artifact_base_ref):
            source_ref = urljoin(artifact_base_ref if artifact_base_ref.endswith("/") else artifact_base_ref + "/", rel_path)
        else:
            source_ref = str(Path(artifact_base_ref) / rel_path.replace("/", "\\"))
        _download_to_path(source_ref, dest_path)
        expected_hash = str(file_info.get("sha256") or "").strip().lower()
        if expected_hash and _sha256_file(dest_path).lower() != expected_hash:
            raise RuntimeError(f"Hash mismatch for staged file: {rel_path}")
        downloaded_files.append(rel_path)

    return {
        "pack_id": pack_id,
        "pack_version": pack_version,
        "stage_root": str(stage_root),
        "manifest_path": str(manifest_path),
        "data_root": str(data_root),
        "downloaded_files": downloaded_files,
    }
