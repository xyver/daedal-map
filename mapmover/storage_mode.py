"""
Storage-mode helpers for local folders vs S3-backed local cache.

The runtime still expects real filesystem paths for DATA_ROOT. To support object
storage without rewriting the rest of the app, S3 mode hydrates a local mirror
cache and returns that cache path as DATA_ROOT.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

try:
    import boto3
except ImportError:
    boto3 = None


logger = logging.getLogger("mapmover")


def get_storage_mode() -> str:
    """Return configured storage mode."""
    return os.environ.get("STORAGE_MODE", "local").strip().lower() or "local"


def get_s3_cache_root(project_root: Path) -> Path:
    """Resolve the local cache folder used for S3-backed runtime data."""
    env_root = os.environ.get("S3_LOCAL_CACHE")
    if env_root:
        return Path(env_root)
    return project_root / ".data_s3_cache"


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


def ensure_s3_data_root(cache_root: Path) -> Path:
    """
    Sync the configured S3 prefix into a local cache folder and return it.

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

    synced_files = 0
    downloaded_files = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if not key or key.endswith("/"):
                continue

            relative_key = key[len(prefix):] if prefix and key.startswith(prefix) else key
            local_path = cache_root / relative_key
            remote_size = int(obj.get("Size", 0))
            remote_mtime = obj["LastModified"].timestamp() if obj.get("LastModified") else 0.0

            if _should_download(local_path, remote_size, remote_mtime):
                local_path.parent.mkdir(parents=True, exist_ok=True)
                client.download_file(bucket, key, str(local_path))
                if remote_mtime:
                    os.utime(local_path, (remote_mtime, remote_mtime))
                downloaded_files += 1
            synced_files += 1

    logger.info(
        "S3 data root synced to local cache: bucket=%s prefix=%s files=%s downloaded=%s cache=%s",
        bucket,
        prefix or "(root)",
        synced_files,
        downloaded_files,
        cache_root,
    )
    return cache_root
