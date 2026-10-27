"""Run the Mapping and Network viewer directly from this checkout."""

from __future__ import annotations

import os
import runpy
import sys
from pathlib import Path


MAP_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = MAP_ROOT.parent
RUNTIME_ROOT = WORKSPACE_ROOT / ".scratch" / "network-source-trial-library" / "runtime"

os.environ.update({
    "INSTALL_MODE": "local",
    "RUNTIME_MODE": "local",
    "DAEDALMAP_ACCESS_MODE": "local_wrapper",
    "APP_HOST": "127.0.0.1",
    "PORT": "7000",
    "APP_URL": "http://127.0.0.1:7000",
    "DATA_ROOT": str(RUNTIME_ROOT / "data"),
    "PACKS_ROOT": str(RUNTIME_ROOT / "packs"),
    "CONFIG_DIR": str(RUNTIME_ROOT / "config"),
    "STATE_DIR": str(RUNTIME_ROOT / "state"),
    "CACHE_DIR": str(RUNTIME_ROOT / "cache"),
    "LOG_DIR": str(RUNTIME_ROOT / "logs"),
    "RUNTIME_CONFIG_PATH": str(RUNTIME_ROOT / "config" / "runtime.json"),
})
os.environ.setdefault("NETWORK_PACKS_DIR", str(MAP_ROOT / "network" / "data" / "packs"))
os.environ["DAEDALMAP_NETWORK_ROOT"] = str(MAP_ROOT)
os.environ.setdefault("DAEDALMAP_NETWORK_AUTO_START", "1")
sys.path.insert(0, str(MAP_ROOT))

if __name__ == "__main__":
    runpy.run_path(str(MAP_ROOT / "app.py"), run_name="__main__")
