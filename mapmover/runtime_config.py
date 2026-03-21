"""
Canonical runtime configuration for the public engine.

The runtime contract is intentionally small:
- `runtime_mode` controls the data plane (`local` or `cloud`)
- code/assets live under the app root and are treated as read-only
- writable runtime state lives in user/deployment-owned directories
"""

from __future__ import annotations

import json
import os
import tempfile
from copy import deepcopy
from pathlib import Path


APP_NAME = "DaedalMap"
APP_ROOT = Path(__file__).resolve().parent.parent
STATIC_ROOT = APP_ROOT / "static"
TEMPLATES_ROOT = APP_ROOT / "templates"
MAP_CONFIG_PATH = APP_ROOT / "config.json"


def _default_local_app_data_root() -> Path:
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA")
        if base:
            return Path(base) / APP_NAME
    xdg = os.environ.get("XDG_DATA_HOME", "").strip()
    if xdg:
        return Path(xdg) / APP_NAME
    return Path.home() / ".local" / "share" / APP_NAME


def _default_local_state_root() -> Path:
    if os.name == "nt":
        return _default_local_app_data_root()
    xdg = os.environ.get("XDG_STATE_HOME", "").strip()
    if xdg:
        return Path(xdg) / APP_NAME
    return Path.home() / ".local" / "state" / APP_NAME


def _default_local_cache_root() -> Path:
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA")
        if base:
            return Path(base) / APP_NAME / "cache"
    xdg = os.environ.get("XDG_CACHE_HOME", "").strip()
    if xdg:
        return Path(xdg) / APP_NAME
    return Path.home() / ".cache" / APP_NAME


def _default_cloud_root() -> Path:
    return Path(tempfile.gettempdir()) / APP_NAME


def _deep_merge(base: dict, override: dict) -> dict:
    merged = deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    return data if isinstance(data, dict) else {}


def _build_defaults() -> dict:
    local_app_data_root = _default_local_app_data_root()
    local_state_root = _default_local_state_root()
    local_cache_root = _default_local_cache_root()
    cloud_root = _default_cloud_root()
    runtime_config_path = local_app_data_root / "config" / "runtime.json"

    return {
        "install_mode": "local",
        "runtime_mode": "local",
        "app": {
            "host": "0.0.0.0",
            "port": 7000,
            "app_url": "http://localhost:7000",
            "site_url": "http://localhost:8080",
        },
        "paths": {
            "config_dir": str(local_app_data_root / "config"),
            "state_dir": str(local_state_root / "state"),
            "cache_dir": str(local_cache_root),
            "log_dir": str(local_state_root / "logs"),
            "data_root": str(local_app_data_root / "data"),
            "packs_root": str(local_app_data_root / "packs"),
            "runtime_config_path": str(runtime_config_path),
        },
        "cloud": {
            "provider": "s3",
            "bucket": "",
            "prefix": "published",
            "endpoint_url": "",
            "cache_root": str(cloud_root / "cache" / "cloud-data"),
        },
        "install_defaults": {
            "local": {
                "app_url": "http://localhost:7000",
                "site_url": "http://localhost:8080",
                "paths": {
                    "config_dir": str(local_app_data_root / "config"),
                    "state_dir": str(local_state_root / "state"),
                    "cache_dir": str(local_cache_root),
                    "log_dir": str(local_state_root / "logs"),
                    "data_root": str(local_app_data_root / "data"),
                    "packs_root": str(local_app_data_root / "packs"),
                },
            },
            "cloud": {
                "app_url": "https://app.daedalmap.com",
                "site_url": "https://daedalmap.com",
                "paths": {
                    "config_dir": str(cloud_root / "config"),
                    "state_dir": str(cloud_root / "state"),
                    "cache_dir": str(cloud_root / "cache"),
                    "log_dir": str(cloud_root / "logs"),
                    "data_root": str(cloud_root / "data"),
                    "packs_root": str(cloud_root / "packs"),
                },
            },
        },
    }


_DEFAULTS = _build_defaults()


def _get_runtime_config_path() -> Path:
    env_path = os.environ.get("RUNTIME_CONFIG_PATH", "").strip()
    if env_path:
        return Path(env_path)
    return Path(_DEFAULTS["paths"]["runtime_config_path"])


def get_runtime_config() -> dict:
    config = deepcopy(_DEFAULTS)
    runtime_config_path = _get_runtime_config_path()
    config = _deep_merge(config, _read_json(runtime_config_path))

    install_mode = os.environ.get("INSTALL_MODE", "").strip().lower()
    if install_mode:
        config["install_mode"] = install_mode

    runtime_mode = os.environ.get("RUNTIME_MODE", "").strip().lower()
    if runtime_mode:
        config["runtime_mode"] = runtime_mode

    install_mode = config.get("install_mode", "local")
    if install_mode not in {"local", "cloud"}:
        raise RuntimeError(f"Unsupported INSTALL_MODE: {install_mode}")

    runtime_mode = config.get("runtime_mode", "local")
    if runtime_mode not in {"local", "cloud"}:
        raise RuntimeError(f"Unsupported RUNTIME_MODE: {runtime_mode}")
    if install_mode == "cloud" and runtime_mode == "local":
        raise RuntimeError("INSTALL_MODE=cloud with RUNTIME_MODE=local is not a supported runtime shape")

    install_defaults = config.get("install_defaults", {}).get(install_mode, {})

    app_cfg = config.setdefault("app", {})
    app_cfg["app_url"] = install_defaults.get("app_url", app_cfg.get("app_url", ""))
    app_cfg["site_url"] = install_defaults.get("site_url", app_cfg.get("site_url", ""))
    app_cfg["host"] = os.environ.get("APP_HOST", app_cfg.get("host", "0.0.0.0")).strip() or "0.0.0.0"
    if os.environ.get("PORT", "").strip():
        app_cfg["port"] = int(os.environ["PORT"])
    app_cfg["app_url"] = os.environ.get("APP_URL", app_cfg.get("app_url", "")).strip() or app_cfg.get("app_url", "")
    app_cfg["site_url"] = os.environ.get("SITE_URL", app_cfg.get("site_url", "")).strip() or app_cfg.get("site_url", "")

    paths_cfg = config.setdefault("paths", {})
    paths_cfg.update(install_defaults.get("paths", {}))
    for env_name, key in [
        ("CONFIG_DIR", "config_dir"),
        ("STATE_DIR", "state_dir"),
        ("CACHE_DIR", "cache_dir"),
        ("LOG_DIR", "log_dir"),
        ("DATA_ROOT", "data_root"),
        ("PACKS_ROOT", "packs_root"),
    ]:
        env_value = os.environ.get(env_name, "").strip()
        if env_value:
            paths_cfg[key] = env_value
    paths_cfg["runtime_config_path"] = str(runtime_config_path)

    cloud_cfg = config.setdefault("cloud", {})
    for env_name, key in [
        ("S3_BUCKET", "bucket"),
        ("S3_PREFIX", "prefix"),
        ("S3_ENDPOINT_URL", "endpoint_url"),
        ("CLOUD_CACHE_ROOT", "cache_root"),
    ]:
        env_value = os.environ.get(env_name, "").strip()
        if env_value:
            cloud_cfg[key] = env_value

    return config
