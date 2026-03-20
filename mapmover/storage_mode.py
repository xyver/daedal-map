"""
Storage-mode helpers for local folders vs S3-backed local cache.

The runtime still expects real filesystem paths for DATA_ROOT. To support object
storage without rewriting the rest of the app, S3 mode hydrates a local mirror
cache and returns that cache path as DATA_ROOT.

Startup sync is two-phase:
  Phase 1 (blocking): sync non-parquet files only (catalog.json, index.json, etc.)
                      These are small and needed immediately for catalog init.
  Phase 2 (background thread): sync remaining parquet files.
                      These are only needed when queries come in.
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path

try:
    import boto3
except ImportError:
    boto3 = None


logger = logging.getLogger("mapmover")

# Extensions synced synchronously at startup (small, needed for catalog init)
_EAGER_EXTENSIONS = {".json", ".csv", ".txt", ".md"}

# Small parquet files that are used frequently enough to be worth hydrating into
# the local Railway cache during the blocking startup sync.
_EAGER_PARQUET_PATHS = {
    "global/world_factbook_static/all_countries.parquet",
}


def get_storage_mode() -> str:
    """Return configured storage mode."""
    return os.environ.get("STORAGE_MODE", "local").strip().lower() or "local"


def get_s3_cache_root(project_root: Path) -> Path:
    """Resolve the local cache folder used for S3-backed runtime data."""
    env_root = os.environ.get("S3_LOCAL_CACHE")
    if env_root:
        return Path(env_root)
    return project_root.parent / "county-map-data"


def _normalize_prefix(prefix: str) -> str:
    prefix = (prefix or "").strip().strip("/")
    return f"{prefix}/" if prefix else ""


def _build_s3_client():
    if boto3 is None:
        raise RuntimeError("S3 mode requires boto3 to be installed")

    client_kwargs = {}
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    region_name = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    if region_name:
        client_kwargs["region_name"] = region_name
    return boto3.client("s3", **client_kwargs)


def _should_download(local_path: Path, remote_size: int, remote_mtime: float) -> bool:
    if not local_path.exists():
        return True
    try:
        stat = local_path.stat()
        if stat.st_size != remote_size:
            return True
        # Remote timestamp wins if it is meaningfully newer.
        if remote_mtime - stat.st_mtime > 1:
            return True
        return False
    except OSError:
        return True


def _download_object(client, bucket: str, key: str, local_path: Path, remote_mtime: float) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, key, str(local_path))
    if remote_mtime:
        os.utime(local_path, (remote_mtime, remote_mtime))


def _is_eager_object(relative_key: str) -> bool:
    """Return True when an S3 object should be hydrated during startup."""
    rel = relative_key.replace("\\", "/")
    if Path(rel).suffix.lower() in _EAGER_EXTENSIONS:
        return True
    return rel in _EAGER_PARQUET_PATHS


def _sync_objects(client, bucket: str, prefix: str, cache_root: Path, objects: list[dict]) -> tuple[int, int]:
    """Download a list of objects into cache_root. Returns (synced, downloaded)."""
    synced = 0
    downloaded = 0
    for obj in objects:
        key = obj["key"]
        local_path = obj["local_path"]
        remote_size = obj["remote_size"]
        remote_mtime = obj["remote_mtime"]
        if _should_download(local_path, remote_size, remote_mtime):
            try:
                _download_object(client, bucket, key, local_path, remote_mtime)
                downloaded += 1
            except Exception as exc:
                logger.warning("S3 download failed for %s: %s", key, exc)
        synced += 1
    return synced, downloaded


def _background_sync(client, bucket: str, deferred: list[dict]) -> None:
    """Background thread: sync deferred (parquet) files into cache."""
    if not deferred:
        return
    logger.info("S3 background sync started: %d parquet files to check", len(deferred))
    synced, downloaded = _sync_objects(client, bucket, "", Path("/"), deferred)
    logger.info("S3 background sync complete: files=%d downloaded=%d", synced, downloaded)


def ensure_s3_data_root(cache_root: Path) -> Path:
    """
    Sync the configured S3 prefix into a local cache folder and return it.

    Phase 1 (blocking): syncs non-parquet files (.json, .csv, etc.) needed at startup.
    Phase 2 (background): syncs parquet files without blocking app startup.

    Required environment variables in S3 mode:
    - `S3_BUCKET`

    Optional:
    - `S3_PREFIX`
    - `S3_ENDPOINT_URL` (for R2/MinIO/etc.)
    - normal AWS credential env vars
    """
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise RuntimeError("STORAGE_MODE=s3 requires S3_BUCKET")

    prefix = _normalize_prefix(os.environ.get("S3_PREFIX", ""))
    cache_root.mkdir(parents=True, exist_ok=True)

    client = _build_s3_client()
    paginator = client.get_paginator("list_objects_v2")

    eager = []
    deferred = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if not key or key.endswith("/"):
                continue

            relative_key = key[len(prefix):] if prefix and key.startswith(prefix) else key
            local_path = cache_root / relative_key
            remote_size = int(obj.get("Size", 0))
            remote_mtime = obj["LastModified"].timestamp() if obj.get("LastModified") else 0.0

            entry = {
                "key": key,
                "local_path": local_path,
                "remote_size": remote_size,
                "remote_mtime": remote_mtime,
            }

            if _is_eager_object(relative_key):
                eager.append(entry)
            else:
                deferred.append(entry)

    # Phase 1: sync catalog/index/metadata files now (blocking), plus a tiny
    # allowlist of hot parquet files that benefit from local cache hydration.
    synced1, downloaded1 = _sync_objects(client, bucket, prefix, cache_root, eager)
    logger.info(
        "S3 eager sync complete: bucket=%s prefix=%s files=%d downloaded=%d cache=%s",
        bucket,
        prefix or "(root)",
        synced1,
        downloaded1,
        cache_root,
    )

    # Parquet files are NOT synced - DuckDB queries them directly from R2 via httpfs.
    if deferred:
        logger.info(
            "S3 mode: %d parquet files will be queried directly from R2 via DuckDB httpfs (not synced locally)",
            len(deferred),
        )

    # Log all eagerly-synced files so we can verify what landed in cache
    synced_paths = sorted(str(obj["local_path"]) for obj in eager)
    logger.info("S3 cache contents (%d files):\n%s", len(synced_paths), "\n".join(synced_paths))

    return cache_root
