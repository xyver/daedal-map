"""
Runtime-mode helpers for local folders vs cloud-backed local cache.

The runtime still expects real filesystem paths for `DATA_ROOT`. To support
object storage without rewriting the rest of the app, `cloud` mode hydrates the
required metadata into a local cache and returns that cache path as `DATA_ROOT`.

Startup sync is two-phase:
  Phase 1 (blocking): sync non-parquet files only (catalog.json, index.json, etc.)
                      These are small and needed immediately for catalog init.
  Phase 2 (background thread): parquet files stay remote and are queried through
                      DuckDB httpfs on demand.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

try:
    import boto3
except ImportError:
    boto3 = None

from .runtime_config import get_runtime_config

logger = logging.getLogger("mapmover")

_EAGER_EXTENSIONS = {".json", ".csv", ".txt", ".md"}
_EAGER_PARQUET_PATHS = {
    "global/world_factbook_static/all_countries.parquet",
}


def get_runtime_mode(configured_mode: str | None = None) -> str:
    if configured_mode:
        mode = str(configured_mode).strip().lower()
    else:
        mode = str(get_runtime_config().get("runtime_mode", "local")).strip().lower() or "local"
    if mode not in {"local", "cloud"}:
        raise RuntimeError(f"Unsupported RUNTIME_MODE: {mode}")
    return mode


def get_cloud_cache_root(default_root: Path) -> Path:
    env_root = os.environ.get("CLOUD_CACHE_ROOT", "").strip()
    if env_root:
        return Path(env_root)
    configured_root = str(get_runtime_config().get("cloud", {}).get("cache_root", "")).strip()
    if configured_root:
        return Path(configured_root)
    return default_root


def _normalize_prefix(prefix: str) -> str:
    prefix = (prefix or "").strip().strip("/")
    return f"{prefix}/" if prefix else ""


def _build_s3_client():
    if boto3 is None:
        raise RuntimeError("Cloud mode requires boto3 to be installed")

    client_kwargs = {}
    cloud_cfg = get_runtime_config().get("cloud", {})
    endpoint_url = os.environ.get("S3_ENDPOINT_URL") or cloud_cfg.get("endpoint_url")
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
    rel = relative_key.replace("\\", "/")
    if Path(rel).suffix.lower() in _EAGER_EXTENSIONS:
        return True
    return rel in _EAGER_PARQUET_PATHS


def _sync_objects(client, bucket: str, objects: list[dict]) -> tuple[int, int]:
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
                logger.warning("Cloud sync download failed for %s: %s", key, exc)
        synced += 1
    return synced, downloaded


def ensure_cloud_data_root(cache_root: Path) -> Path:
    """
    Sync the configured object-storage prefix into a local cache folder and return it.

    Required environment variables in cloud mode:
    - `S3_BUCKET`

    Optional:
    - `S3_PREFIX`
    - `S3_ENDPOINT_URL`
    - normal AWS credential env vars
    """
    cloud_cfg = get_runtime_config().get("cloud", {})
    bucket = os.environ.get("S3_BUCKET", "").strip() or str(cloud_cfg.get("bucket", "")).strip()
    if not bucket:
        raise RuntimeError("RUNTIME_MODE=cloud requires S3_BUCKET")

    prefix = _normalize_prefix(os.environ.get("S3_PREFIX", "") or str(cloud_cfg.get("prefix", "")))
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

    synced, downloaded = _sync_objects(client, bucket, eager)
    logger.info(
        "Cloud eager sync complete: bucket=%s prefix=%s files=%d downloaded=%d cache=%s",
        bucket,
        prefix or "(root)",
        synced,
        downloaded,
        cache_root,
    )

    if deferred:
        logger.info(
            "Cloud mode: %d parquet files will be queried directly from object storage via DuckDB httpfs",
            len(deferred),
        )

    synced_paths = sorted(str(obj["local_path"]) for obj in eager)
    logger.info("Cloud cache contents (%d files):\n%s", len(synced_paths), "\n".join(synced_paths))

    return cache_root
