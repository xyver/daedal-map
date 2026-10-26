"""
Minimal DaedalMap launcher for local wrapper testing.

Responsibilities:
- read the stable engine manifest
- download and verify the engine artifact
- unpack the engine into a local install folder
- launch the runtime on localhost
- open the browser to the running app
"""

from __future__ import annotations

import ctypes
import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import webbrowser
import zipfile
from dataclasses import dataclass
from pathlib import Path
from socket import create_connection
from tkinter import BOTH, DISABLED, END, LEFT, NORMAL, RIGHT, SINGLE, Y, Button, Canvas, Entry, Frame, Label, Listbox, PhotoImage, Scrollbar, StringVar, Text, Tk, TclError, Toplevel, filedialog

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
except ImportError:
    DND_FILES = None
    TkinterDnD = None

from theme import THEME, app_root_kwargs, button_kwargs, label_kwargs, listbox_kwargs, panel_kwargs, scrollbar_kwargs, tab_button_kwargs, text_kwargs
from runtime_manager import RuntimeSetupError, bundled_uv_executable, prepare_shared_environment
from map_launch_page import MapLaunchPage
from launcher_release import LAUNCHER_VERSION, PUBLIC_BASE_URL as LAUNCHER_RELEASE_BASE_URL, check_release, download_release, is_newer_version
from geometry_store import (GEOMETRY_TIERS, GeometryStoreError, geometry_info_url, group_geometry_releases,
                            installed_geometry, install_geometry_zip, releases_from_storefront)
from styled_dialog import messagebox


DEFAULT_PRODUCT = "daedalmap"
NETWORK_PRODUCT = "daedalmap-network"


@dataclass(frozen=True)
class InstallActionState:
    action: str
    version_label: str

    @classmethod
    def compare(cls, installed_version: str, available_version: str) -> "InstallActionState":
        if not installed_version:
            return cls("Install", "")
        if installed_version != available_version:
            return cls("Update", "")
        return cls("", f"v{installed_version}")


class PackDownloadCancelled(Exception):
    """The transfer stopped before installation; its partial cache is reusable."""


class ActionSpinner:
    """Animate any launcher action label and restore its original text afterward."""

    FRAMES = ("◐", "◓", "◑", "◒")

    def __init__(self, root: Tk, variables: list[StringVar]):
        self.root = root
        self.variables = variables
        self.original: list[str] = []
        self.job = None
        self.step = 0
        self.verb = ""

    def start(self, verb: str) -> None:
        self.stop()
        self.original = [variable.get() for variable in self.variables]
        self.verb = verb
        self.step = 0
        self.job = True
        self._tick()

    def _tick(self) -> None:
        if self.job is None:
            return
        label = f"{self.verb} {self.FRAMES[self.step % len(self.FRAMES)]}"
        for variable in self.variables:
            variable.set(label)
        self.step += 1
        self.job = self.root.after(150, self._tick)

    def stop(self) -> None:
        if self.job is None:
            return
        self.root.after_cancel(self.job)
        self.job = None
        for variable, original in zip(self.variables, self.original):
            variable.set(original)
MCP_PRODUCTS = {"daedalmap-research-mcp", "daedalmap-geometry-mcp"}
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7000
PACKS_PREFIX = "downloadable/packs/"
DEFAULT_PUBLIC_BASE_URL = "https://downloads.daedalmap.com"
DEFAULT_STOREFRONT_CATALOG_URL = "https://www.daedalmap.com/downloadable/storefront_catalog.json"
NAV_TABS = (("home", "Home"), ("programs", "Programs"), ("library", "Data"), ("geometry", "Geometry"),
            ("advanced", "Settings"), ("runtime", "Explore Map"),
            ("network", "Network"),
            ("research", "Research MCP"), ("geometry_mcp", "Geometry MCP"), ("llm", "AI"))
HTTP_USER_AGENT = "DaedalMapLauncher/0.1 (+https://daedalmap.com)"
PROGRAM_SITE_PAGES = {
    "daedalmap": "/explore",
    "daedalmap-mapping": "/explore",
    "daedalmap-research-mcp": "/research",
    "daedalmap-geometry-mcp": "/devs",
}
HTTP_JSON_TIMEOUT_SECONDS = 30
HTTP_DOWNLOAD_TIMEOUT_SECONDS = 60
HTTP_RETRY_ATTEMPTS = 3
HTTP_RETRY_BASE_DELAY_SECONDS = 1.0
SIZE_CACHE_TTL_SECONDS = 300.0
REMOTE_STATUS_MIN_INTERVAL_SECONDS = 60.0
MAPPING_RUNTIME_START_TIMEOUT_SECONDS = 60.0
DOWNLOAD_PROGRESS_STEP_BYTES = 25 * 1024 * 1024
WINDOWS_STILL_ACTIVE_EXIT_CODE = 259


def _local_appdata() -> Path:
    root = os.environ.get("LOCALAPPDATA", "").strip()
    if root:
        return Path(root)
    return Path.home() / "AppData" / "Local"


def _configure_local_source_cli() -> None:
    """Select the launcher library and optional source override before startup."""
    if __name__ != "__main__" or len(sys.argv) < 2 or sys.argv[1] not in {"--local-source", "--library-root"}:
        return
    parser = argparse.ArgumentParser(description="Launch Mapping and Network from source")
    parser.add_argument("--local-source", action="store_true")
    parser.add_argument("--library-root", type=Path,
                        help="Existing launcher library root; defaults to the normal launcher library")
    args = parser.parse_args(sys.argv[1:])
    if args.library_root:
        os.environ["DAEDALMAP_LAUNCHER_ROOT"] = str(args.library_root.expanduser().resolve())
    if not args.local_source:
        sys.argv[:] = sys.argv[:1]
        return
    if getattr(sys, "frozen", False):
        raise SystemExit("Local source CLI mode requires the workspace launcher.py")
    source = Path(__file__).resolve().parents[2]
    configured_python = os.environ.get("DAEDALMAP_MAPPING_TEST_PYTHON", "").strip()
    candidates = ([Path(configured_python)] if configured_python else []) + [
        parent / ".venv" / "Scripts" / "python.exe" for parent in (source, *source.parents)]
    python = next((candidate.resolve() for candidate in candidates if candidate.is_file()), None)
    if not (source / "app.py").is_file():
        raise SystemExit(f"Mapping source files are missing: {source}")
    if python is None:
        raise SystemExit("Source Python is missing; create a .venv or set DAEDALMAP_MAPPING_TEST_PYTHON")
    os.environ["DAEDALMAP_LAUNCHER_LOCAL_SOURCE"] = "1"
    os.environ["DAEDALMAP_LAUNCHER_DEV"] = "1"
    os.environ["DAEDALMAP_MAPPING_SOURCE_ROOT"] = str(source)
    os.environ["DAEDALMAP_NETWORK_SOURCE_ROOT"] = str(source)
    os.environ["DAEDALMAP_MAPPING_TEST_PYTHON"] = str(python)
    os.environ["DAEDALMAP_MAPPING_LOCAL_PACKAGES"] = "0"
    sys.argv[:] = sys.argv[:1]


_configure_local_source_cli()


def local_source_mode() -> bool:
    return os.environ.get("DAEDALMAP_LAUNCHER_LOCAL_SOURCE", "").strip() == "1"


def local_custom_mode() -> bool:
    return os.environ.get("DAEDALMAP_LAUNCHER_LOCAL_CUSTOM", "").strip() == "1"


def _default_app_root() -> Path:
    override = os.environ.get("DAEDALMAP_LAUNCHER_ROOT", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    owner = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent
    portable = owner / "launcher-data"
    if getattr(sys, "frozen", False):
        return portable
    legacy = _local_appdata() / "DaedalMapLauncher"
    # Keep the older source-checkout trial usable without changing the ZIP default.
    if not portable.exists() and (legacy / "runtime_config.json").exists():
        return legacy
    return portable


APP_ROOT = _default_app_root()
LAUNCHER_MODE_PATH = APP_ROOT / "launcher_mode.json"
LOCAL_CUSTOM_ROOT = APP_ROOT / "custom"


def load_launcher_mode() -> dict[str, str]:
    if not LAUNCHER_MODE_PATH.exists():
        return {"mode": "connected"}
    try:
        value = json.loads(LAUNCHER_MODE_PATH.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return {"mode": "local_custom"}
    if not isinstance(value, dict) or value.get("mode") not in {"connected", "local_custom"}:
        return {"mode": "local_custom"}
    return {"mode": value["mode"]}


def save_launcher_mode(mode: str) -> None:
    if mode not in {"connected", "local_custom"}:
        raise LauncherError(f"Invalid launcher mode: {mode}")
    APP_ROOT.mkdir(parents=True, exist_ok=True)
    payload = {"mode": mode}
    temporary = LAUNCHER_MODE_PATH.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temporary, LAUNCHER_MODE_PATH)


def launcher_restart_command() -> list[str]:
    command = [str(Path(sys.executable).resolve())]
    if not getattr(sys, "frozen", False):
        command.append(str(Path(__file__).resolve()))
    return [*command, "--library-root", str(APP_ROOT)]


def launcher_restart_env() -> dict[str, str]:
    env = os.environ.copy()
    for name in ("DAEDALMAP_LAUNCHER_LOCAL_SOURCE", "DAEDALMAP_LAUNCHER_LOCAL_CUSTOM",
                 "DAEDALMAP_LAUNCHER_DEV",
                 "DAEDALMAP_MAPPING_SOURCE_ROOT", "DAEDALMAP_NETWORK_SOURCE_ROOT",
                 "DAEDALMAP_MAPPING_TEST_PYTHON", "DAEDALMAP_MAPPING_LOCAL_PACKAGES",
                 "DAEDALMAP_LOCAL_SOURCE_MODE", "DAEDALMAP_LOCAL_CUSTOM_MODE",
                 "DAEDALMAP_NETWORK_ROOT", "NETWORK_PACKS_DIR"):
        env.pop(name, None)
    env["DAEDALMAP_LAUNCHER_ROOT"] = str(APP_ROOT)
    return env


def _configure_saved_launcher_mode() -> None:
    if local_source_mode():
        return  # An explicit CLI override wins for this session.
    if load_launcher_mode()["mode"] != "local_custom":
        return
    os.environ["DAEDALMAP_LAUNCHER_LOCAL_SOURCE"] = "1"
    os.environ["DAEDALMAP_LAUNCHER_LOCAL_CUSTOM"] = "1"
    os.environ["DAEDALMAP_LAUNCHER_DEV"] = "1"
    os.environ["DAEDALMAP_MAPPING_SOURCE_ROOT"] = str(LOCAL_CUSTOM_ROOT / "mapping")
    os.environ["DAEDALMAP_NETWORK_SOURCE_ROOT"] = str(LOCAL_CUSTOM_ROOT / "network")
    os.environ.pop("DAEDALMAP_MAPPING_TEST_PYTHON", None)
    os.environ["DAEDALMAP_MAPPING_LOCAL_PACKAGES"] = "0"


_configure_saved_launcher_mode()
ENGINE_ROOT = APP_ROOT / "engine"
PROGRAMS_ROOT = APP_ROOT / "programs"
PROGRAMS_STATE_PATH = APP_ROOT / "programs.json"
CACHE_ROOT = APP_ROOT / "cache"
STATE_PATH = APP_ROOT / "state.json"
RUNTIME_CONFIG_PATH = APP_ROOT / "runtime_config.json"
MAP_TILES_CONFIG_PATH = APP_ROOT / "map_tiles.json"
LOGS_ROOT = APP_ROOT / "runtime" / "logs"
RUNTIME_ROOT = APP_ROOT / "runtime"
RUNTIME_CONFIG_DIR = RUNTIME_ROOT / "config"
RUNTIME_STATE_DIR = RUNTIME_ROOT / "state"
RUNTIME_CACHE_DIR = RUNTIME_ROOT / "cache"
RUNTIME_LOG_DIR = RUNTIME_ROOT / "logs"
RUNTIME_DATA_ROOT = RUNTIME_ROOT / "data"
RUNTIME_PACKS_ROOT = RUNTIME_ROOT / "packs"
MAP_DETAIL_FILES = {
    "ne_10m_land.geojson": "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_land.geojson",
    "ne_10m_admin_0_countries.geojson": "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_0_countries.geojson",
}
LOCAL_CORPORA_PATH = RUNTIME_STATE_DIR / "local_corpora.json"
LAUNCHER_NAVIGATION_PATH = RUNTIME_STATE_DIR / "launcher_navigation.json"
PUBLIC_REPO_ROOT = Path(__file__).resolve().parents[2]
WRAPPER_ROOT = PUBLIC_REPO_ROOT.parent / "county-map-private"
if str(PUBLIC_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(PUBLIC_REPO_ROOT))
from local_data_store import ensure_empty_catalog, install_data_pack, install_verified_data_zip


@dataclass
class EngineState:
    version: str = ""
    install_dir: str = ""
    source_commit: str = ""
    release_manifest_url: str = ""
    runtime_pid: int = 0


@dataclass
class AvailablePack:
    pack_id: str
    version: str
    package_id: str = ""
    package_tier: str = "lite"
    current_manifest_url: str = ""
    source_name: str = ""
    description: str = ""
    notes: list[str] | None = None
    topic_category: str = ""
    size_bytes: int = 0
    sha256: str = ""


@dataclass
class AvailableProgram:
    product: str
    title: str
    version: str
    manifest_url: str
    artifact_url: str
    sha256: str
    size_bytes: int
    description: str = ""
    badges: tuple[str, ...] = ()
    page_url: str = ""


@dataclass
class LocalPackStatus:
    installed_version: str = ""
    active: bool = False


@dataclass
class LocalCorpus:
    corpus_id: str
    name: str
    pack_ids: list[str]
    updated_at: str


class LauncherError(RuntimeError):
    pass


def require_connected_mode() -> None:
    if local_source_mode():
        raise LauncherError("DaedalMap downloads are disabled in Local custom mode")


def _default_storage_root() -> Path:
    return APP_ROOT / "runtime"


def _build_runtime_path_map(storage_root: Path) -> dict[str, Path]:
    root = Path(storage_root)
    return {
        "storage_root": root,
        "config_dir": root / "config",
        "state_dir": root / "state",
        "cache_dir": root / "cache",
        "log_dir": root / "logs",
        "data_root": root / "data",
        "packs_root": root / "packs",
    }


def _write_runtime_config(storage_root: Path) -> None:
    path_map = _build_runtime_path_map(storage_root)
    payload = {
        "install_mode": "local",
        "runtime_mode": "local",
        "paths": {
            "config_dir": str(path_map["config_dir"]),
            "state_dir": str(path_map["state_dir"]),
            "cache_dir": str(path_map["cache_dir"]),
            "log_dir": str(path_map["log_dir"]),
            "data_root": str(path_map["data_root"]),
            "packs_root": str(path_map["packs_root"]),
            "runtime_config_path": str(RUNTIME_CONFIG_PATH),
        },
        "local_wrapper": {
            "storage_root": str(path_map["storage_root"]),
        },
    }
    RUNTIME_CONFIG_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_storage_root() -> Path:
    if RUNTIME_CONFIG_PATH.exists():
        try:
            payload = json.loads(RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            local_wrapper = payload.get("local_wrapper") if isinstance(payload.get("local_wrapper"), dict) else {}
            configured = str(local_wrapper.get("storage_root") or "").strip()
            if not configured:
                paths = payload.get("paths") if isinstance(payload.get("paths"), dict) else {}
                data_root = str(paths.get("data_root") or "").strip()
                if data_root:
                    try:
                        configured = str(Path(data_root).resolve().parent)
                    except Exception:
                        configured = str(Path(data_root).parent)
            if configured:
                return Path(configured)
    return _default_storage_root()


def apply_runtime_storage_root(storage_root: Path, *, persist: bool = False) -> Path:
    global LOGS_ROOT, RUNTIME_ROOT, RUNTIME_CONFIG_DIR, RUNTIME_STATE_DIR, RUNTIME_CACHE_DIR, RUNTIME_LOG_DIR, RUNTIME_DATA_ROOT, RUNTIME_PACKS_ROOT, LOCAL_CORPORA_PATH
    path_map = _build_runtime_path_map(storage_root)
    RUNTIME_ROOT = path_map["storage_root"]
    RUNTIME_CONFIG_DIR = path_map["config_dir"]
    RUNTIME_STATE_DIR = path_map["state_dir"]
    RUNTIME_CACHE_DIR = path_map["cache_dir"]
    RUNTIME_LOG_DIR = path_map["log_dir"]
    RUNTIME_DATA_ROOT = path_map["data_root"]
    RUNTIME_PACKS_ROOT = path_map["packs_root"]
    LOGS_ROOT = RUNTIME_LOG_DIR
    LOCAL_CORPORA_PATH = RUNTIME_STATE_DIR / "local_corpora.json"
    if persist:
        APP_ROOT.mkdir(parents=True, exist_ok=True)
        _write_runtime_config(path_map["storage_root"])
    return path_map["storage_root"]


def _format_bytes(value: int | float | None) -> str:
    try:
        size = float(value or 0)
    except (TypeError, ValueError):
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    index = 0
    while size >= 1024 and index < len(units) - 1:
        size /= 1024.0
        index += 1
    if index == 0:
        return f"{int(size)} {units[index]}"
    return f"{size:.1f} {units[index]}"


def load_map_tile_preferences() -> dict:
    try:
        payload = json.loads(MAP_TILES_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "mode": payload.get("mode") if payload.get("mode") in {"offline_basic", "offline_detail"} else "offline_basic",
    }


def save_map_tile_preferences(mode: str) -> None:
    if mode not in {"offline_basic", "offline_detail"}:
        raise LauncherError("Unsupported map detail mode")
    APP_ROOT.mkdir(parents=True, exist_ok=True)
    temporary = MAP_TILES_CONFIG_PATH.with_suffix(".json.tmp")
    temporary.write_text(json.dumps({"mode": mode}, indent=2), encoding="utf-8")
    os.replace(temporary, MAP_TILES_CONFIG_PATH)


def map_detail_available() -> bool:
    detail_root = RUNTIME_ROOT / "tiles"
    return all((detail_root / name).is_file() for name in MAP_DETAIL_FILES)


class MapDetailDownloadCancelled(Exception):
    """The user stopped an optional world-map download."""


def download_map_detail(log, cancel_event: threading.Event | None = None) -> int:
    """Install the optional detailed Natural Earth world geometry atomically."""
    require_connected_mode()
    detail_root = RUNTIME_ROOT / "tiles"
    detail_root.mkdir(parents=True, exist_ok=True)
    staged: list[tuple[Path, Path]] = []
    try:
        for filename, url in MAP_DETAIL_FILES.items():
            if cancel_event is not None and cancel_event.is_set():
                raise MapDetailDownloadCancelled()
            destination = detail_root / filename
            if destination.is_file():
                continue
            temporary = destination.with_suffix(".geojson.download")
            staged.append((temporary, destination))
            log(f"Downloading {filename}")
            with urllib.request.urlopen(url, timeout=10) as response, temporary.open("wb") as output:
                total = 0
                while True:
                    if cancel_event is not None and cancel_event.is_set():
                        raise MapDetailDownloadCancelled()
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > 40 * 1024 * 1024:
                        raise LauncherError("Map detail file exceeded the expected size")
                    output.write(chunk)
            if cancel_event is not None and cancel_event.is_set():
                raise MapDetailDownloadCancelled()
            with temporary.open("r", encoding="utf-8") as source:
                payload = json.load(source)
            if payload.get("type") != "FeatureCollection" or not payload.get("features"):
                raise LauncherError(f"Map detail download is invalid: {filename}")
        if cancel_event is not None and cancel_event.is_set():
            raise MapDetailDownloadCancelled()
        for temporary, destination in staged:
            os.replace(temporary, destination)
    except Exception:
        if cancel_event is not None and cancel_event.is_set():
            raise MapDetailDownloadCancelled() from None
        raise
    finally:
        for temporary, _destination in staged:
            temporary.unlink(missing_ok=True)
    return sum((detail_root / name).stat().st_size for name in MAP_DETAIL_FILES)


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            try:
                total += child.stat().st_size
            except OSError:
                continue
    return total


def _format_micro_usd_balance(value: int | float | None) -> str:
    try:
        micro = int(value or 0)
    except (TypeError, ValueError):
        return "unknown"
    return f"${micro / 1_000_000:.2f}"


def engine_install_path(state: EngineState) -> Path:
    return Path(state.install_dir) if state.install_dir else Path()


def local_mapping_source_root() -> Path | None:
    """Opt-in source checkout for launcher-driven development without an export."""
    if os.environ.get("DAEDALMAP_LAUNCHER_DEV", "").strip() != "1":
        return None
    configured = os.environ.get("DAEDALMAP_MAPPING_SOURCE_ROOT", "").strip()
    if not configured:
        return None
    root = Path(configured).expanduser().resolve()
    if not (root / "app.py").is_file() or not (root / "requirements.txt").is_file():
        if local_source_mode():
            return None
        raise LauncherError(f"Mapping source folder needs app.py and requirements.txt: {root}")
    return root


def local_network_source_root() -> Path | None:
    if not local_source_mode():
        return None
    mapping_root = local_mapping_source_root()
    configured = os.environ.get("DAEDALMAP_NETWORK_SOURCE_ROOT", "").strip()
    if mapping_root is None or not configured:
        return None
    root = Path(configured).expanduser().resolve()
    try:
        manifest = json.loads((root / "network" / "extension.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if (manifest.get("product") != NETWORK_PRODUCT or manifest.get("mapping_interface") != 1
            or not (root / "network" / "app" / "mapping_extension.py").is_file()):
        return None
    return root


def local_mapping_packages_root() -> Path | None:
    if (os.environ.get("DAEDALMAP_LAUNCHER_DEV", "").strip() == "1" and
            os.environ.get("DAEDALMAP_MAPPING_LOCAL_PACKAGES", "").strip() == "1"):
        return APP_ROOT / "local-releases"
    return None


def configure_local_mapping_trial() -> None:
    """Use the local package workflow when running this workspace's source launcher."""
    if LAUNCHER_MODE_PATH.exists() and load_launcher_mode()["mode"] == "connected":
        return
    if (getattr(sys, "frozen", False) or
            "DAEDALMAP_MAPPING_LOCAL_PACKAGES" in os.environ or
            os.environ.get("DAEDALMAP_MAPPING_SOURCE_ROOT", "").strip() or
            os.environ.get("DAEDALMAP_ENGINE_CURRENT_URL", "").strip()):
        return
    test_python = WRAPPER_ROOT.parent / ".venv" / "Scripts" / "python.exe"
    if ((PUBLIC_REPO_ROOT / "app.py").is_file() and
            (WRAPPER_ROOT / "build" / "export_downloadable_engine.py").is_file() and
            test_python.is_file()):
        os.environ["DAEDALMAP_LAUNCHER_DEV"] = "1"
        os.environ["DAEDALMAP_MAPPING_LOCAL_PACKAGES"] = "1"
        os.environ.setdefault("DAEDALMAP_MAPPING_TEST_PYTHON", str(test_python))


def has_usable_installed_engine(state: EngineState | None = None) -> bool:
    if local_mapping_source_root() is not None:
        return True
    current = state or load_state()
    install_dir = engine_install_path(current)
    return bool(current.version and current.install_dir and (install_dir / "app.py").exists())


def visible_launcher_tabs(mapping_installed: bool, installed_products: set[str]) -> set[str]:
    if local_source_mode():
        return {"home", "library", "geometry", "advanced", "runtime", "network"}
    tabs = {"home", "library", "geometry", "programs", "advanced"}
    if mapping_installed:
        tabs.add("runtime")
    if NETWORK_PRODUCT in installed_products:
        tabs.add("network")
    if "daedalmap-research-mcp" in installed_products:
        tabs.add("research")
    if "daedalmap-geometry-mcp" in installed_products:
        tabs.add("geometry_mcp")
    if mapping_installed and any(product == "daedalmap-ai" or product.startswith("daedalmap-ai-")
                                 for product in installed_products):
        tabs.add("llm")
    return tabs


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_local_corpora() -> list[LocalCorpus]:
    ensure_dirs()
    if not LOCAL_CORPORA_PATH.exists():
        return []
    try:
        payload = json.loads(LOCAL_CORPORA_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    items = payload if isinstance(payload, list) else payload.get("corpora", [])
    corpora: list[LocalCorpus] = []
    if not isinstance(items, list):
        return corpora
    for entry in items:
        if not isinstance(entry, dict):
            continue
        corpora.append(
            LocalCorpus(
                corpus_id=str(entry.get("corpus_id") or "").strip(),
                name=str(entry.get("name") or "").strip(),
                pack_ids=[str(pack_id).strip() for pack_id in entry.get("pack_ids", []) if str(pack_id).strip()],
                updated_at=str(entry.get("updated_at") or "").strip(),
            )
        )
    return corpora


def save_local_corpora(corpora: list[LocalCorpus]) -> None:
    ensure_dirs()
    payload = [
        {
            "corpus_id": corpus.corpus_id,
            "name": corpus.name,
            "pack_ids": corpus.pack_ids,
            "updated_at": corpus.updated_at,
        }
        for corpus in corpora
    ]
    LOCAL_CORPORA_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def is_runtime_reachable() -> bool:
    try:
        with create_connection((DEFAULT_HOST, DEFAULT_PORT), timeout=0.5):
            return True
    except OSError:
        return False


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong(0)
            if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return exit_code.value == WINDOWS_STILL_ACTIVE_EXIT_CODE
            return True
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _terminate_pid_tree(pid: int) -> None:
    if pid <= 0:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            check=False,
        )
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if not _pid_is_alive(pid):
            return
        time.sleep(0.2)
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def fetch_engine_current_version() -> str:
    # Background status poll: a single attempt keeps offline detection fast.
    current = read_json_url(engine_current_url(), attempts=1)
    return str(current.get("current_version") or "").strip()


def ensure_dirs() -> None:
    APP_ROOT.mkdir(parents=True, exist_ok=True)
    ENGINE_ROOT.mkdir(parents=True, exist_ok=True)
    PROGRAMS_ROOT.mkdir(parents=True, exist_ok=True)
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    if not RUNTIME_CONFIG_PATH.exists():
        _write_runtime_config(load_storage_root())
    LOGS_ROOT.mkdir(parents=True, exist_ok=True)
    RUNTIME_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_STATE_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_LOG_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_DATA_ROOT.mkdir(parents=True, exist_ok=True)
    RUNTIME_PACKS_ROOT.mkdir(parents=True, exist_ok=True)


def load_public_env() -> None:
    # Developer convenience only: reading the sibling public repo .env is
    # gated so a normal end-user install never picks up stray env files.
    if os.environ.get("DAEDALMAP_LAUNCHER_DEV", "").strip() != "1":
        return
    env_path = PUBLIC_REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if value:
            os.environ.setdefault(key, value)


def load_state() -> EngineState:
    ensure_dirs()
    if not STATE_PATH.exists():
        return EngineState()
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return EngineState()
    if not isinstance(data, dict):
        return EngineState()
    return EngineState(
        version=str(data.get("version") or "").strip(),
        install_dir=str(data.get("install_dir") or "").strip(),
        source_commit=str(data.get("source_commit") or "").strip(),
        release_manifest_url=str(data.get("release_manifest_url") or "").strip(),
        runtime_pid=int(data.get("runtime_pid") or 0),
    )


def save_state(state: EngineState) -> None:
    ensure_dirs()
    payload = {
        "version": state.version,
        "install_dir": state.install_dir,
        "source_commit": state.source_commit,
        "release_manifest_url": state.release_manifest_url,
        "runtime_pid": state.runtime_pid,
    }
    STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _is_retryable_http_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code >= 500
    return isinstance(exc, (urllib.error.URLError, TimeoutError, ConnectionError))


def _with_retries(operation_name: str, fn, *, log=None, attempts: int = HTTP_RETRY_ATTEMPTS):
    delay = HTTP_RETRY_BASE_DELAY_SECONDS
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt >= attempts or not _is_retryable_http_error(exc):
                raise
            if log:
                log(f"{operation_name} failed (attempt {attempt} of {attempts}): {exc}. Retrying in {delay:.0f}s")
            time.sleep(delay)
            delay *= 2


def read_json_url(url: str, *, log=None, attempts: int = HTTP_RETRY_ATTEMPTS) -> dict:
    require_connected_mode()
    def fetch() -> bytes:
        request = urllib.request.Request(url, headers={"User-Agent": HTTP_USER_AGENT})
        with urllib.request.urlopen(request, timeout=HTTP_JSON_TIMEOUT_SECONDS) as response:
            return response.read()

    try:
        raw = _with_retries(f"Fetch {url}", fetch, log=log, attempts=attempts)
        data = json.loads(raw.decode("utf-8"))
    except urllib.error.URLError as exc:
        raise LauncherError(f"Could not fetch {url}: {exc}") from exc
    except (TimeoutError, ConnectionError) as exc:
        raise LauncherError(f"Could not fetch {url}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise LauncherError(f"Invalid JSON at {url}: {exc}") from exc
    if not isinstance(data, dict):
        raise LauncherError(f"Expected JSON object at {url}")
    return data


def _make_download_progress_reporter(log):
    state = {"next_percent": 10, "next_bytes": DOWNLOAD_PROGRESS_STEP_BYTES}

    def report(bytes_done: int, total: int | None) -> None:
        if total and total > 0:
            percent = int(bytes_done * 100 / total)
            if percent >= state["next_percent"]:
                log(f"Download progress: {percent}% ({_format_bytes(bytes_done)} of {_format_bytes(total)})")
                state["next_percent"] = (percent // 10 + 1) * 10
        elif bytes_done >= state["next_bytes"]:
            log(f"Download progress: {_format_bytes(bytes_done)} downloaded (total size unknown)")
            state["next_bytes"] = bytes_done + DOWNLOAD_PROGRESS_STEP_BYTES

    return report


def _download_file_once(url: str, destination: Path, chunk_size: int, *, log=None, progress=None,
                        cancel_event: threading.Event | None = None) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise PackDownloadCancelled()
    existing_bytes = destination.stat().st_size if destination.exists() else 0
    headers = {"User-Agent": HTTP_USER_AGENT}
    if existing_bytes > 0:
        headers["Range"] = f"bytes={existing_bytes}-"
    request = urllib.request.Request(url, headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=HTTP_DOWNLOAD_TIMEOUT_SECONDS)
    except urllib.error.HTTPError as exc:
        if exc.code == 416 and existing_bytes > 0:
            if log:
                log("Server reports requested range unavailable; treating cached file as a complete candidate")
            return
        raise
    with response:
        status = getattr(response, "status", None) or response.getcode()
        content_length = response.headers.get("Content-Length")
        try:
            remaining = int(content_length) if content_length is not None else None
        except ValueError:
            remaining = None
        if status == 206 and not existing_bytes:
            raise LauncherError("Server returned a partial response to a full download request")
        if status == 206 and existing_bytes > 0:
            content_range = response.headers.get("Content-Range") or ""
            if not re.fullmatch(rf"bytes {existing_bytes}-\d+/(?:\d+|\*)", content_range):
                raise LauncherError(f"Server returned an invalid resume range: {content_range}")
            mode = "ab"
            bytes_done = existing_bytes
            total = existing_bytes + remaining if remaining is not None else None
            if log:
                log(f"Resuming download at {_format_bytes(existing_bytes)}")
        else:
            mode = "wb"
            bytes_done = 0
            total = remaining
            if existing_bytes > 0 and log:
                log("Server does not support resume; restarting download from the beginning")
        with destination.open(mode) as out:
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    raise PackDownloadCancelled()
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                out.write(chunk)
                bytes_done += len(chunk)
                if progress:
                    progress(bytes_done, total)
                if total is not None and bytes_done >= total:
                    break
            if remaining is not None and bytes_done != total:
                raise ConnectionError("Download ended before the advertised Content-Length")


def download_file(url: str, destination: Path, chunk_size: int = 1024 * 1024, *, log=None, progress=None,
                  cancel_event: threading.Event | None = None) -> None:
    require_connected_mode()
    def attempt() -> None:
        _download_file_once(url, destination, chunk_size, log=log, progress=progress,
                            cancel_event=cancel_event)

    try:
        _with_retries(f"Download {url}", attempt, log=log)
    except urllib.error.URLError as exc:
        raise LauncherError(f"Could not download {url}: {exc}") from exc
    except (TimeoutError, ConnectionError) as exc:
        raise LauncherError(f"Could not download {url}: {exc}") from exc


def download_verified_file(url: str, destination: Path, expected_size: int, expected_sha: str,
                           *, log=None, progress=None, cancel_event: threading.Event | None = None) -> None:
    """Keep a verified ZIP intact while a resumable partial transfer is in progress."""
    if (destination.is_file() and destination.stat().st_size == expected_size
            and sha256_file(destination) == expected_sha):
        return
    partial = destination.with_name(destination.name + f".{expected_sha}.part")
    if partial.exists() and partial.stat().st_size == expected_size and sha256_file(partial) == expected_sha:
        if cancel_event is not None and cancel_event.is_set():
            raise PackDownloadCancelled()
        os.replace(partial, destination)
        return
    if partial.exists() and partial.stat().st_size >= expected_size:
        partial.unlink()
    download_file(url, partial, log=log, progress=progress, cancel_event=cancel_event)
    if partial.stat().st_size != expected_size or sha256_file(partial) != expected_sha:
        partial.unlink(missing_ok=True)
        raise LauncherError("Downloaded ZIP size or SHA-256 does not match its published release")
    if cancel_event is not None and cancel_event.is_set():
        raise PackDownloadCancelled()
    os.replace(partial, destination)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_extract_zip(zip_path: Path, dest_dir: Path) -> None:
    dest_root = dest_dir.resolve()
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.infolist():
            member_name = member.filename
            member_path = Path(member_name)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise LauncherError(f"Unsafe archive path: {member_name}")
            resolved = (dest_dir / member_name).resolve()
            if not resolved.is_relative_to(dest_root):
                raise LauncherError(f"Unsafe archive path: {member_name}")
        archive.extractall(dest_dir)


def wait_for_port(host: str, port: int, timeout_seconds: float = 20.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def initialize_runtime_root(log=None) -> None:
    """Create the shared library even when no local program is installed."""
    ensure_dirs()
    if ensure_empty_catalog(RUNTIME_DATA_ROOT) and log:
        log(f"Created empty shared catalog at {RUNTIME_DATA_ROOT / 'catalog.json'}")


def load_pack_state() -> dict:
    initialize_runtime_root()
    pack_state_path = RUNTIME_STATE_DIR / "pack_state.json"
    if not pack_state_path.exists():
        return {"installed_packs": [], "active_pack_ids": []}
    try:
        data = json.loads(pack_state_path.read_text(encoding="utf-8"))
    except Exception:
        return {"installed_packs": [], "active_pack_ids": []}
    return data if isinstance(data, dict) else {"installed_packs": [], "active_pack_ids": []}


def get_local_pack_status(pack_id: str) -> LocalPackStatus:
    state = load_pack_state()
    installed_version = ""
    for entry in state.get("installed_packs", []):
        if str(entry.get("pack_id") or "").strip() == pack_id:
            installed_version = str(entry.get("version") or "").strip()
            break
    active = pack_id in {str(pid).strip() for pid in state.get("active_pack_ids", [])}
    return LocalPackStatus(installed_version=installed_version, active=active)


def packs_for_view(available: list[AvailablePack], pack_state: dict, view: str) -> list[tuple[AvailablePack, LocalPackStatus, bool]]:
    installed = {
        str(entry.get("pack_id") or "").strip(): str(entry.get("version") or "").strip()
        for entry in pack_state.get("installed_packs", []) if isinstance(entry, dict) and entry.get("pack_id")
    }
    active = {str(pack_id).strip() for pack_id in pack_state.get("active_pack_ids", [])}
    if view == "cloud":
        return [(pack, LocalPackStatus(installed.get(pack.pack_id, ""), pack.pack_id in active), True)
                for pack in available]
    by_id = {pack.pack_id: pack for pack in available}
    return [(by_id.get(pack_id) or AvailablePack(pack_id, version, source_name=pack_id),
             LocalPackStatus(version, pack_id in active), pack_id in by_id)
            for pack_id, version in installed.items()]


def public_base_url() -> str:
    explicit = os.environ.get("DAEDALMAP_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if explicit:
        return explicit
    engine_url = os.environ.get("DAEDALMAP_ENGINE_CURRENT_URL", "").strip()
    if engine_url and "/downloadable/engine/" in engine_url:
        return engine_url.split("/downloadable/engine/", 1)[0].rstrip("/")
    return DEFAULT_PUBLIC_BASE_URL


def engine_current_url() -> str:
    explicit = os.environ.get("DAEDALMAP_ENGINE_CURRENT_URL", "").strip()
    if explicit:
        return explicit
    return f"{public_base_url()}/downloadable/engine/{DEFAULT_PRODUCT}/stable/current.json"


def packs_index_url() -> str:
    return f"{public_base_url()}/downloadable/packs/index.json"


def storefront_catalog_url() -> str:
    return os.environ.get("DAEDALMAP_STOREFRONT_CATALOG_URL", "").strip() or DEFAULT_STOREFRONT_CATALOG_URL


def fetch_storefront_catalog(log, *, attempts: int = HTTP_RETRY_ATTEMPTS) -> dict:
    """Read and cache the same discovery catalog as the Downloads page."""
    catalog = read_json_url(storefront_catalog_url(), log=log, attempts=attempts)
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    cache = CACHE_ROOT / "storefront_catalog.json"
    temp_cache = cache.with_suffix(".json.tmp")
    temp_cache.write_text(json.dumps(catalog), encoding="utf-8")
    os.replace(temp_cache, cache)
    return catalog


def storefront_catalog_for_import(log) -> dict:
    try:
        return fetch_storefront_catalog(log, attempts=1)
    except Exception as exc:
        cache = CACHE_ROOT / "storefront_catalog.json"
        if not cache.is_file():
            raise LauncherError("Cannot verify this ZIP: Downloads is unavailable and no release list is saved") from exc
        log("Using the saved Downloads list")
        return json.loads(cache.read_text(encoding="utf-8"))


def cached_storefront_catalog() -> dict | None:
    cache = CACHE_ROOT / "storefront_catalog.json"
    try:
        catalog = json.loads(cache.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return catalog if isinstance(catalog, dict) else None


def fetch_available_programs(log, *, attempts: int = HTTP_RETRY_ATTEMPTS) -> list[AvailableProgram]:
    return _programs_from_catalog(fetch_storefront_catalog(log, attempts=attempts), log)


def _programs_from_catalog(catalog: dict, log) -> list[AvailableProgram]:
    categories = catalog.get("categories") if isinstance(catalog, dict) else None
    entries = categories.get("programs") if isinstance(categories, dict) else None
    if not isinstance(entries, list):
        raise LauncherError("Storefront catalog is missing its programs list")
    programs = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        product = str(item.get("packageId") or "").strip()
        if product == "daedalmap-launcher":
            continue
        version = str(item.get("version") or "").strip()
        manifest_url = str(item.get("manifestUrl") or "").strip()
        artifact_url = str(item.get("artifactUrl") or "").strip()
        sha256 = str(item.get("sha256") or "").strip().lower()
        size = item.get("size")
        if not (product and version and manifest_url and artifact_url and len(sha256) == 64 and isinstance(size, int) and size > 0):
            log(f"Skipping incomplete storefront program: {product or 'unknown'}")
            continue
        programs.append(AvailableProgram(product, program_display_name(item.get("title") or product), version,
                                         manifest_url, artifact_url, sha256, size,
                                         str(item.get("description") or "").strip(),
                                         tuple(str(badge) for badge in item.get("badges", []) if isinstance(badge, str)),
                                         program_page_path(product)))
    return programs


def program_display_name(title: str) -> str:
    """Keep the product name short in the launcher; release IDs stay unchanged."""
    original = str(title or "").strip()
    return re.sub(r"^DaedalMap(?:\s*[-:]\s*|\s+)", "", original, flags=re.IGNORECASE).strip() or original


def program_page_path(product: str) -> str:
    """Use the website's existing tool pages for the matching local programs."""
    return PROGRAM_SITE_PAGES.get(product, "")


def programs_for_import(log) -> list[AvailableProgram]:
    return _programs_from_catalog(storefront_catalog_for_import(log), log)


def identify_data_zip_release(zip_path: Path, catalog: dict) -> dict:
    """Match a local data ZIP to exactly one published Downloads entry."""
    if not zip_path.is_file() or zip_path.suffix.lower() != ".zip":
        raise LauncherError("Choose a data pack ZIP file")
    categories = catalog.get("categories", {}) if isinstance(catalog, dict) else {}
    entries = categories.get("data", []) if isinstance(categories, dict) else []
    size = zip_path.stat().st_size
    digest = sha256_file(zip_path)
    matches = [item for item in entries if isinstance(item, dict)
               and item.get("size") == size and str(item.get("sha256") or "").lower() == digest]
    if len(matches) != 1:
        raise LauncherError("ZIP does not match a published data pack. Check Downloads for the matching release.")
    return matches[0]


def identify_program_zip(zip_path: Path, programs: list[AvailableProgram]) -> AvailableProgram:
    if not zip_path.is_file() or zip_path.suffix.lower() != ".zip":
        raise LauncherError("Choose a program ZIP file")
    size = zip_path.stat().st_size
    digest = sha256_file(zip_path)
    matches = [item for item in programs if item.size_bytes == size and item.sha256 == digest]
    if len(matches) != 1:
        raise LauncherError("ZIP does not match a published program in the current or cached catalog. Refresh Programs and try again.")
    return matches[0]


def load_program_state() -> dict:
    if not PROGRAMS_STATE_PATH.is_file():
        return {"schema_version": "1.0", "installed_programs": []}
    data = json.loads(PROGRAMS_STATE_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("installed_programs"), list):
        raise LauncherError("Program install registry is invalid")
    return data


def network_start_with_mapping() -> bool:
    settings = load_program_state().get("settings", {})
    network = settings.get("network", {}) if isinstance(settings, dict) else {}
    if not isinstance(network, dict):
        return True
    return network.get("start_with_mapping", True) is not False


def save_network_start_with_mapping(enabled: bool) -> None:
    state = load_program_state()
    settings = state.get("settings")
    if not isinstance(settings, dict):
        settings = state["settings"] = {}
    network = settings.get("network")
    if not isinstance(network, dict):
        network = settings["network"] = {}
    network["start_with_mapping"] = bool(enabled)
    save_program_state(state)


def installed_network_root() -> Path | None:
    entries = load_program_state()["installed_programs"]
    entry = next((item for item in reversed(entries) if item.get("product") == NETWORK_PRODUCT), None)
    if not entry:
        return None
    location = str(entry.get("install_dir") or "").strip()
    if not location:
        return None
    root = Path(location).resolve()
    manifest = root / "network" / "extension.json"
    try:
        metadata = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if (root.is_relative_to(PROGRAMS_ROOT.resolve()) and
            metadata.get("product") == NETWORK_PRODUCT and
            metadata.get("mapping_interface") == 1 and
            (root / "network" / "app" / "mapping_extension.py").is_file()):
        return root
    return None


def _copy_local_custom_tree(source: Path, destination: Path) -> None:
    """Create an editable copy once; later switches preserve user changes."""
    base = LOCAL_CUSTOM_ROOT.resolve()
    target = destination.resolve()
    if not target.is_relative_to(base):
        raise LauncherError(f"Custom source must stay inside the launcher folder: {target}")
    if target.exists():
        if not (target / ".daedalmap-local-custom.json").is_file():
            raise LauncherError(f"Incomplete custom copy at {target}; review that folder before continuing")
        return
    if not source.is_dir():
        raise LauncherError(f"Installed program folder is missing: {source}")
    base.mkdir(parents=True, exist_ok=True)
    stage = base / f".{target.name}-stage-{uuid.uuid4().hex}"
    if not stage.resolve().is_relative_to(base):
        raise LauncherError("Custom source staging path escaped the launcher folder")
    try:
        shutil.copytree(source, stage)
        (stage / ".daedalmap-local-custom.json").write_text(
            json.dumps({"source": str(source), "created_at": _now_iso()}, indent=2), encoding="utf-8")
        os.replace(stage, target)
    finally:
        if stage.exists() and stage.resolve().is_relative_to(base):
            shutil.rmtree(stage)


def prepare_local_custom_sources() -> Path:
    mapping = LOCAL_CUSTOM_ROOT / "mapping"
    if not mapping.exists():
        state = load_state()
        installed = engine_install_path(state)
        if not (state.version and (installed / "app.py").is_file()):
            raise LauncherError("Install Explore Map while connected before switching to Local custom")
        _copy_local_custom_tree(installed, mapping)
    elif (not (mapping / ".daedalmap-local-custom.json").is_file() or
          not (mapping / "app.py").is_file() or not (mapping / "requirements.txt").is_file()):
        raise LauncherError(f"Incomplete custom Mapping copy at {mapping}")
    network = installed_network_root()
    if (LOCAL_CUSTOM_ROOT / "network").exists() and not (
            LOCAL_CUSTOM_ROOT / "network" / ".daedalmap-local-custom.json").is_file():
        raise LauncherError(f"Incomplete custom Network copy at {LOCAL_CUSTOM_ROOT / 'network'}")
    if network and not (LOCAL_CUSTOM_ROOT / "network").exists():
        _copy_local_custom_tree(network, LOCAL_CUSTOM_ROOT / "network")
    return mapping


def installed_runtime_entries(mapping_state: EngineState | None = None) -> list[dict]:
    """Resolve every installed service and Mapping into one shared environment."""
    entries = list(load_program_state()["installed_programs"])
    state = mapping_state or load_state()
    if has_usable_installed_engine(state):
        entries.append({"product": "daedalmap-mapping", "version": state.version,
                        "install_dir": state.install_dir, "source_commit": state.source_commit})
    return entries


def save_program_state(state: dict) -> None:
    PROGRAMS_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = PROGRAMS_STATE_PATH.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(state, indent=2), encoding="utf-8")
    os.replace(temporary, PROGRAMS_STATE_PATH)


def include_installed_programs(published: list[AvailableProgram]) -> list[AvailableProgram]:
    """Keep installed tools visible and runnable with no network or cache."""
    result = list(published)
    known = {(item.product, item.version) for item in result}
    for entry in load_program_state()["installed_programs"]:
        key = (str(entry.get("product") or ""), str(entry.get("version") or ""))
        if not all(key) or key in known:
            continue
        result.append(AvailableProgram(key[0], program_display_name(entry.get("title") or key[0]), key[1],
                                       str(entry.get("release_manifest_url") or ""), "",
                                       str(entry.get("sha256") or ""), 0,
                                       "Installed on this computer.", (),
                                       program_page_path(key[0])))
        known.add(key)
    return result


def program_cards_for_view(programs: list[AvailableProgram], installed_entries: list[dict]) -> list[tuple[AvailableProgram, dict | None, bool]]:
    """Show one current release per product, with the selected local version."""
    installed = {str(entry.get("product") or ""): entry for entry in installed_entries}
    published = {item.product: item for item in programs if item.artifact_url and item.size_bytes > 0}
    displayed: dict[str, AvailableProgram] = dict(published)
    for item in programs:
        displayed.setdefault(item.product, item)
    for product, entry in installed.items():
        if str(entry.get("version") or "").startswith("local-dev-"):
            local = next((item for item in programs if item.product == product and
                          item.version == entry["version"]), None)
            if local is not None:
                displayed[product] = local
    return [(item, installed.get(product), bool(product in published and installed.get(product)
                                             and not str(installed[product].get("version") or "").startswith("local-dev-")
                                             and installed[product].get("version") != item.version))
            for product, item in displayed.items()]


def program_kind_label(product: str) -> str:
    if product == NETWORK_PRODUCT:
        return "MAPPING EXPANSION"
    if product in MCP_PRODUCTS:
        return "MCP SERVICE"
    if product == "daedalmap-historical-version-generator":
        return "CLI TOOL"
    return "TOOL"


def program_use_label(product: str, running: bool, *, mapping_installed: bool = False) -> str:
    if product == NETWORK_PRODUCT:
        return "Open Mapping" if mapping_installed else "Export Parquet"
    if product in MCP_PRODUCTS:
        return "Stop" if running else "Start"
    return "Open Files"


def verify_program_release(program: AvailableProgram, log) -> None:
    """Recheck the immutable Cloudflare release before using catalog update data."""
    release = read_json_url(program.manifest_url, log=log)
    if not isinstance(release, dict):
        raise LauncherError(f"Published release manifest is invalid for {program.title}")
    artifact = release.get("artifact") if isinstance(release.get("artifact"), dict) else {}
    manifest_url = urllib.parse.urljoin(program.manifest_url, str(artifact.get("download_url") or artifact.get("filename") or ""))
    if (str(release.get("product") or "") != program.product or
            str(release.get("version") or "") != program.version or
            manifest_url != program.artifact_url or
            str(artifact.get("sha256") or "").lower() != program.sha256 or
            artifact.get("size_bytes") != program.size_bytes):
        raise LauncherError(f"Published release manifest no longer matches {program.title} {program.version}; refresh Programs")


def install_program_zip(zip_path: Path, program: AvailableProgram) -> Path:
    """Use one verified installer for store downloads and user-supplied ZIPs."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", program.product) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", program.version):
        raise LauncherError("Unsafe program ID or version")
    if zip_path.stat().st_size != program.size_bytes or sha256_file(zip_path) != program.sha256:
        raise LauncherError("Program ZIP size or SHA-256 does not match the release catalog")
    product_root = PROGRAMS_ROOT / program.product
    product_root.mkdir(parents=True, exist_ok=True)
    install_dir = product_root / program.version
    if install_dir.exists():
        state = load_program_state()
        matching = next((item for item in state["installed_programs"]
                         if item.get("product") == program.product and item.get("version") == program.version
                         and item.get("sha256") == program.sha256 and item.get("install_dir") == str(install_dir)), None)
        if matching is not None:
            state["installed_programs"].remove(matching)
            state["installed_programs"].append(matching)
            save_program_state(state)
            return install_dir
        raise LauncherError(f"An unregistered folder already exists at {install_dir}; nothing was overwritten")
    stage = Path(tempfile.mkdtemp(prefix=".install-", dir=product_root))
    moved = False
    try:
        safe_extract_zip(zip_path, stage)
        if not any(stage.iterdir()):
            raise LauncherError("Program ZIP is empty")
        state = load_program_state()
        shutil.move(str(stage), str(install_dir))
        moved = True
        entry = {"product": program.product, "title": program.title, "version": program.version,
                 "install_dir": str(install_dir), "sha256": program.sha256,
                 "release_manifest_url": program.manifest_url}
        state["installed_programs"] = [item for item in state["installed_programs"]
                                       if item.get("product") != program.product or item.get("version") != program.version]
        state["installed_programs"].append(entry)
        save_program_state(state)
        return install_dir
    except Exception:
        if moved and install_dir.exists() and install_dir.resolve().is_relative_to(PROGRAMS_ROOT.resolve()):
            shutil.rmtree(install_dir)
        raise
    finally:
        if stage.exists() and stage.resolve().is_relative_to(PROGRAMS_ROOT.resolve()):
            shutil.rmtree(stage)


def install_published_program(program: AvailableProgram, log, prepare_runtime) -> Path:
    """Activate a verified release only after its shared runtime is ready."""
    verify_program_release(program, log)
    CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    zip_path = CACHE_ROOT / f"{program.product}-{program.version}.zip"
    if not zip_path.is_file() or zip_path.stat().st_size != program.size_bytes or sha256_file(zip_path) != program.sha256:
        if zip_path.exists():
            zip_path.unlink()
        download_file(program.artifact_url, zip_path, log=log,
                      progress=_make_download_progress_reporter(log))
    previous_state = load_program_state()
    install_dir = PROGRAMS_ROOT / program.product / program.version
    existed = install_dir.exists()
    installed = install_program_zip(zip_path, program)
    try:
        if program.product != "daedalmap-historical-version-generator":
            prepare_runtime()
    except Exception:
        save_program_state(previous_state)
        if not existed and installed.exists() and installed.resolve().is_relative_to(PROGRAMS_ROOT.resolve()):
            shutil.rmtree(installed)
        raise
    return installed


def fetch_pack_info(pack_id: str, package_id: str = "") -> AvailablePack:
    current_url = stable_pack_current_url(pack_id, package_id=package_id)
    current = read_json_url(current_url)
    version_url = str(current.get("version_manifest_url") or "").strip()
    if not version_url:
        raise LauncherError(f"Pack current manifest missing version_manifest_url for {pack_id}")
    version = read_json_url(version_url)
    return AvailablePack(
        pack_id=pack_id,
        version=str(version.get("version") or current.get("current_version") or "").strip(),
        package_id=str(current.get("package_id") or package_id or f"{pack_id}-lite").strip(),
        package_tier=str(current.get("package_tier") or "lite").strip(),
        current_manifest_url=current_url,
        source_name=str(version.get("source_name") or current.get("source_name") or pack_id).strip(),
        description=str(version.get("description") or current.get("description") or "").strip(),
        notes=list(version.get("notes") or current.get("notes") or []),
        size_bytes=int((version.get("artifact") or {}).get("size_bytes") or 0),
        sha256=str((version.get("artifact") or {}).get("sha256") or "").lower(),
    )


def fetch_available_packs(log) -> list[AvailablePack]:
    return _packs_from_catalog(fetch_storefront_catalog(log), log)


def _packs_from_catalog(catalog: dict, log) -> list[AvailablePack]:
    categories = catalog.get("categories") if isinstance(catalog, dict) else None
    entries = categories.get("data") if isinstance(categories, dict) else None
    if not isinstance(entries, list):
        raise LauncherError("Storefront catalog is missing its data pack list")
    packs: list[AvailablePack] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        package_id = str(entry.get("packageId") or "").strip()
        version = str(entry.get("version") or "").strip()
        manifest_url = str(entry.get("manifestUrl") or "").strip()
        parsed = urllib.parse.urlparse(manifest_url)
        parts = parsed.path.split("/")
        try:
            marker = parts.index("packs")
            pack_id, path_package_id, channel = parts[marker + 1:marker + 4]
        except (ValueError, IndexError):
            log(f"Skipping incomplete storefront data pack: {package_id or 'unknown'}")
            continue
        if (not package_id or package_id != path_package_id or not version
                or not pack_id or channel != "stable" or parsed.scheme != "https"):
            log(f"Skipping incomplete storefront data pack: {package_id or 'unknown'}")
            continue
        current_url = parsed._replace(path="/".join(parts[:marker + 4]) + "/current.json",
                                      params="", query="", fragment="").geturl()
        packs.append(AvailablePack(
            pack_id=pack_id, version=version, package_id=package_id,
            package_tier=str(entry.get("tier") or "lite").strip(),
            current_manifest_url=current_url,
            source_name=str(entry.get("title") or pack_id).strip(),
            description=str(entry.get("description") or "").strip(),
            topic_category=str(entry.get("topicCategory") or "").strip(),
            size_bytes=entry.get("size") if isinstance(entry.get("size"), int) else 0,
            sha256=str(entry.get("sha256") or "").lower(),
        ))
    return packs


def _apply_storefront_pack_copy(packs: list[AvailablePack], catalog: dict) -> None:
    """Display the same title and description as the website Downloads cards."""
    categories = catalog.get("categories", {}) if isinstance(catalog, dict) else {}
    entries = categories.get("data", []) if isinstance(categories, dict) else []
    by_package = {str(item.get("packageId")): item for item in entries if isinstance(item, dict)}
    for pack in packs:
        item = by_package.get(pack.package_id)
        if not item or str(item.get("version")) != pack.version:
            continue
        pack.source_name = str(item.get("title") or pack.source_name).strip()
        pack.description = str(item.get("description") or pack.description).strip()


def stable_pack_current_url(
    pack_id: str,
    channel: str = "stable",
    package_id: str = "",
) -> str:
    edition_id = str(package_id or f"{pack_id}-lite").strip()
    return f"{public_base_url()}/downloadable/packs/{pack_id}/{edition_id}/{channel}/current.json"


def build_runtime_env() -> dict[str, str]:
    env = os.environ.copy()
    if local_mapping_source_root() is None:
        env.pop("DAEDALMAP_NETWORK_ROOT", None)
    env["INSTALL_MODE"] = "local"
    env["RUNTIME_MODE"] = "local"
    env["DAEDALMAP_ACCESS_MODE"] = "local_wrapper"
    env["PORT"] = str(DEFAULT_PORT)
    env["DATA_ROOT"] = str(RUNTIME_DATA_ROOT)
    env["PACKS_ROOT"] = str(RUNTIME_PACKS_ROOT)
    env["CONFIG_DIR"] = str(RUNTIME_CONFIG_DIR)
    env["STATE_DIR"] = str(RUNTIME_STATE_DIR)
    env["CACHE_DIR"] = str(RUNTIME_CACHE_DIR)
    env["LOG_DIR"] = str(RUNTIME_LOG_DIR)
    env["RUNTIME_CONFIG_PATH"] = str(RUNTIME_CONFIG_PATH)
    env["DAEDALMAP_MAP_TILES_CONFIG"] = str(MAP_TILES_CONFIG_PATH)
    env["DAEDALMAP_MAP_TILES_ROOT"] = str(RUNTIME_ROOT / "tiles")
    env["S3_PREFIX"] = ""
    if local_source_mode():
        if local_mapping_source_root() is None:
            raise LauncherError("Local custom source is unavailable; choose a valid Mapping source in Settings")
        network_root = local_network_source_root()
        env["DAEDALMAP_LOCAL_SOURCE_MODE"] = "1"
        if local_custom_mode():
            env["DAEDALMAP_LOCAL_CUSTOM_MODE"] = "1"
        else:
            env.pop("DAEDALMAP_LOCAL_CUSTOM_MODE", None)
        if network_root and not local_custom_mode():
            env["NETWORK_PACKS_DIR"] = str(network_root / "network" / "data" / "packs")
        else:
            env.pop("NETWORK_PACKS_DIR", None)
            if not network_root:
                env.pop("DAEDALMAP_NETWORK_ROOT", None)
    else:
        env.pop("DAEDALMAP_LOCAL_SOURCE_MODE", None)
        env.pop("DAEDALMAP_LOCAL_CUSTOM_MODE", None)
        env.pop("NETWORK_PACKS_DIR", None)
        network_root = installed_network_root()
    if network_root:
        env["DAEDALMAP_NETWORK_ROOT"] = str(network_root)
        env["DAEDALMAP_NETWORK_AUTO_START"] = "1" if network_start_with_mapping() else "0"
    return env


def fetch_release_info(log) -> tuple[dict, dict]:
    current = read_json_url(engine_current_url(), log=log)
    release_url = str(current.get("release_manifest_url") or "").strip()
    if not release_url:
        raise LauncherError("Engine current manifest is missing release_manifest_url")
    release = read_json_url(release_url, log=log)
    log("Fetched engine manifests via public HTTP URL")
    return current, release


def install_engine(log) -> EngineState:
    ensure_dirs()
    load_public_env()
    current, release = fetch_release_info(log)

    version = str(release.get("version") or "").strip()
    source_commit = str(release.get("source_commit") or "").strip()
    release_url = str(current.get("release_manifest_url") or "").strip()
    artifact = release.get("artifact") or {}
    artifact_url = str(artifact.get("download_url") or "").strip()
    expected_sha = str(artifact.get("sha256") or "").strip().lower()
    expected_size = artifact.get("size_bytes")
    if not version or not artifact_url or not expected_sha:
        raise LauncherError("Release manifest is missing version, artifact URL, or sha256")
    if str(release.get("product") or "") != DEFAULT_PRODUCT or str(current.get("current_version") or "") != version:
        raise LauncherError("Engine current pointer and versioned release do not match")
    if not isinstance(expected_size, int) or expected_size <= 0:
        raise LauncherError("Engine release is missing a valid artifact size")

    install_dir = ENGINE_ROOT / version
    if (install_dir / "app.py").exists():
        log(f"Engine {version} already installed at {install_dir}")
        state = EngineState(
            version=version,
            install_dir=str(install_dir),
            source_commit=source_commit,
            release_manifest_url=release_url,
        )
        save_state(state)
        return state

    cache_zip = CACHE_ROOT / f"{DEFAULT_PRODUCT}-engine-{version}.zip"
    log(f"Downloading engine {version}")
    download_file(artifact_url, cache_zip, log=log, progress=_make_download_progress_reporter(log))
    log("Downloaded engine artifact via public HTTP URL")

    log("Verifying SHA256")
    if cache_zip.stat().st_size != expected_size:
        raise LauncherError("Engine artifact size does not match its release manifest")
    actual_sha = sha256_file(cache_zip).lower()
    if actual_sha != expected_sha:
        try:
            cache_zip.unlink()
            log(f"Deleted cached artifact after hash mismatch: {cache_zip}")
        except OSError as exc:
            log(f"Could not delete cached artifact {cache_zip}: {exc}")
        raise LauncherError(
            "Engine artifact hash mismatch. "
            f"Expected {expected_sha}, got {actual_sha}."
        )

    with tempfile.TemporaryDirectory(prefix="daedalmap-engine-") as temp_dir:
        extract_root = Path(temp_dir) / "extract"
        extract_root.mkdir(parents=True, exist_ok=True)
        log("Extracting engine")
        safe_extract_zip(cache_zip, extract_root)
        if not (extract_root / "app.py").exists():
            raise LauncherError("Extracted engine does not contain app.py at the root")
        if install_dir.exists():
            shutil.rmtree(install_dir)
        shutil.move(str(extract_root), str(install_dir))

    state = EngineState(
        version=version,
        install_dir=str(install_dir),
        source_commit=source_commit,
        release_manifest_url=release_url,
    )
    save_state(state)
    log(f"Installed engine {version} to {install_dir}")
    return state


def build_and_install_local_mapping(log, *, before_replace=None) -> EngineState:
    """Refresh one reusable local test package and keep one previous install."""
    output_root = local_mapping_packages_root()
    if output_root is None or getattr(sys, "frozen", False):
        raise LauncherError("Local Mapping packages require the source launcher in development mode")
    source = PUBLIC_REPO_ROOT.resolve()
    exporter = WRAPPER_ROOT / "build" / "export_downloadable_engine.py"
    python = Path(os.environ.get("DAEDALMAP_MAPPING_TEST_PYTHON", "") or sys.executable).resolve()
    if not (source / "app.py").is_file() or not exporter.is_file() or not python.is_file():
        raise LauncherError("Local Mapping source, exporter, or Python is missing")
    version = "local-dev"
    APP_ROOT.mkdir(parents=True, exist_ok=True)
    ENGINE_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".mapping-package-", dir=APP_ROOT) as temporary:
        temporary_root = Path(temporary)
        command = [str(python), str(exporter), "--source-root", str(source),
                   "--local-output", str(temporary_root), "--version", version]
        log("Packaging current Mapping source into the local test slot")
        result = subprocess.run(command, cwd=str(WRAPPER_ROOT.parent), capture_output=True,
                                text=True, encoding="utf-8", errors="replace")
        if result.returncode:
            raise LauncherError(f"Local Mapping package failed: {(result.stderr or result.stdout)[-1200:]}")
        release_path = temporary_root / "downloadable" / "engine" / DEFAULT_PRODUCT / "stable" / version / "release.json"
        release = json.loads(release_path.read_text(encoding="utf-8"))
        artifact = release.get("artifact") if isinstance(release.get("artifact"), dict) else {}
        filename = str(artifact.get("filename") or "")
        archive = (release_path.parent / filename).resolve()
        if not filename or archive.parent != release_path.parent.resolve() or not archive.is_file():
            raise LauncherError("Local Mapping release has no safe ZIP artifact")
        expected_size = artifact.get("size_bytes")
        expected_sha = str(artifact.get("sha256") or "").lower()
        if (release.get("product") != DEFAULT_PRODUCT or release.get("version") != version or
                release.get("release_status") != "local_test_candidate" or
                not isinstance(expected_size, int) or archive.stat().st_size != expected_size or
                sha256_file(archive) != expected_sha):
            raise LauncherError("Local Mapping ZIP does not match its release manifest")
        staging = Path(tempfile.mkdtemp(prefix=".local-dev-stage-", dir=ENGINE_ROOT))
        try:
            safe_extract_zip(archive, staging)
            if not (staging / "app.py").is_file() or not (staging / "requirements.txt").is_file():
                raise LauncherError("Local Mapping ZIP is missing its runtime entry or requirements")
            if before_replace is not None:
                before_replace()
            install_dir = ENGINE_ROOT / version
            backup = ENGINE_ROOT / ".local-dev.previous"
            if backup.exists() and not install_dir.exists():
                os.replace(backup, install_dir)  # Recover an interrupted prior swap.
            if backup.exists():
                shutil.rmtree(backup)
            old_installed = install_dir.exists()
            old_moved = False
            new_installed = False
            try:
                if old_installed:
                    os.replace(install_dir, backup)
                    old_moved = True
                os.replace(staging, install_dir)
                new_installed = True
                output_root.mkdir(parents=True, exist_ok=True)
                package_copy = output_root / "local-dev.zip"
                release_copy = output_root / "local-dev-release.json"
                shutil.copy2(archive, package_copy.with_suffix(".zip.tmp"))
                os.replace(package_copy.with_suffix(".zip.tmp"), package_copy)
                release["artifact"] = {**artifact, "filename": package_copy.name,
                                       "download_url": package_copy.as_uri()}
                release_copy.with_suffix(".json.tmp").write_text(json.dumps(release, indent=2), encoding="utf-8")
                os.replace(release_copy.with_suffix(".json.tmp"), release_copy)
                state = EngineState(version=version, install_dir=str(install_dir),
                                    source_commit=str(release.get("source_commit") or ""),
                                    release_manifest_url=release_copy.as_uri())
                save_state(state)
            except Exception:
                if new_installed and install_dir.exists():
                    shutil.rmtree(install_dir)
                if old_moved and backup.exists():
                    os.replace(backup, install_dir)
                raise
            log(f"Installed local Mapping test build at {install_dir} (SHA-256 {expected_sha})")
            return state
        finally:
            if staging.exists() and staging.resolve().is_relative_to(ENGINE_ROOT.resolve()):
                shutil.rmtree(staging)


def local_mapping_source_fingerprint() -> str:
    exporter = WRAPPER_ROOT / "build" / "export_downloadable_engine.py"
    source = PUBLIC_REPO_ROOT.resolve()
    python = Path(os.environ.get("DAEDALMAP_MAPPING_TEST_PYTHON", "") or sys.executable).resolve()
    result = subprocess.run([str(python), str(exporter), "--source-root", str(source),
                             "--source-fingerprint"], capture_output=True, text=True,
                            encoding="utf-8", errors="replace", cwd=str(WRAPPER_ROOT.parent))
    fingerprint = result.stdout.strip()
    if result.returncode or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
        raise LauncherError(f"Could not compare local Mapping source: {(result.stderr or result.stdout)[-500:]}")
    return fingerprint


def local_mapping_installed_fingerprint() -> str:
    release = local_mapping_packages_root()
    if release is None:
        return ""
    path = release / "local-dev-release.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        fingerprint = str(data.get("source_fingerprint") or "")
        return fingerprint if re.fullmatch(r"[0-9a-f]{64}", fingerprint) else ""
    except (OSError, ValueError):
        return ""


def default_launcher_window_geometry(screen_width: int, screen_height: int) -> str:
    """Fit and center the default window within the current screen."""
    width = min(1400, max(1, screen_width - 80))
    height = min(900, max(1, screen_height - 120))
    left = max(0, (screen_width - width) // 2)
    top = max(0, (screen_height - height) // 2)
    return f"{width}x{height}+{left}+{top}"


class LauncherApp:
    def __init__(self, root: Tk):
        self.root = root
        self.map_launch_page: MapLaunchPage | None = None
        self.root.title("DaedalMap | LOCAL CUSTOM" if local_custom_mode() else
                        "DaedalMap | LOCAL SOURCE" if local_source_mode() else "DaedalMap")
        self.root.geometry(default_launcher_window_geometry(root.winfo_screenwidth(), root.winfo_screenheight()))
        self.root.configure(**app_root_kwargs())
        self.brand_icon: PhotoImage | None = None
        self.brand_mark: PhotoImage | None = None
        brand_path = Path(__file__).resolve().parent / "assets" / "brand_mark.png"
        try:
            self.brand_icon = PhotoImage(data=base64.b64encode(brand_path.read_bytes()).decode("ascii"))
            self.brand_mark = self.brand_icon.subsample(3, 3)
            self.root.iconphoto(True, self.brand_icon)
        except (OSError, TclError):
            pass  # The text header remains usable when an asset is missing.
        self.status_var = StringVar(value="Ready")
        self.installed_var = StringVar(value="")
        self.online_var = StringVar(value="Checking network...")
        self.launcher_update_var = StringVar(value=f"Launcher v{LAUNCHER_VERSION}")
        self.launcher_available_release: dict | None = None
        self.online_dot_var = StringVar(value="●")
        self.update_var = StringVar(value="Checking Mapping...")
        self.store_var = StringVar(value="Data packs: checking...")
        self.runtime_var = StringVar(value="")
        self.pack_search_var = StringVar(value="")
        self.pack_filter = "cloud"
        self.geometry_search_var = StringVar(value="")
        self.geometry_filter = "cloud"
        self.geometry_section = "all"
        self.research_var = StringVar(value="")
        self.llm_var = StringVar(value="")
        self.advanced_var = StringVar(value="")
        self.launcher_mode_var = StringVar(value="")
        self.install_button_var = StringVar(value="Checking...")
        self.install_button_idle_label = "Checking..."
        self.mapping_spinner_job = None
        self.mapping_spinner_step = 0
        self.mapping_spinner_verb = "Installing"
        self.open_map_button_var = StringVar(value="Open Map")
        self.mapping_launch_spinner = ActionSpinner(root, [self.open_map_button_var])
        self.map_tiles_download_var = StringVar(value="Download detailed world map")
        self.map_tiles_download_spinner = ActionSpinner(root, [self.map_tiles_download_var])
        self.map_detail_cancel_event = threading.Event()
        self.pack_download_cancel_event = threading.Event()
        self.map_tiles_status_var = StringVar(value="")
        self.active_pack_spinner: ActionSpinner | None = None
        self.active_pack_verb = "Installing Pack"
        self.pack_action_vars: dict[tuple[str, str], StringVar] = {}
        self.active_program_spinner: ActionSpinner | None = None
        self.active_program_verb = "Installing"
        self.program_action_vars: dict[tuple[str, str], StringVar] = {}
        self.mapping_badge_var = StringVar(value="MAP  /  CHECKING")
        self.process: subprocess.Popen | None = None
        self.program_processes: dict[str, subprocess.Popen] = {}
        self.program_log_handles: dict[str, object] = {}
        self.ui_thread_id = threading.get_ident()
        self.runtime_stdout_handle = None
        self.runtime_stderr_handle = None
        self.worker: threading.Thread | None = None
        self.size_cache: dict[str, int] = {"data": 0, "packs": 0, "cache": 0}
        self.size_cache_time = 0.0
        self.size_refresh_in_flight = False
        self.last_remote_status_kick = 0.0
        self.available_packs: list[AvailablePack] = []
        self.available_geometry: list[dict] = []
        self.available_geometry_families: list[dict] = []
        self.available_programs: list[AvailableProgram] = include_installed_programs([])
        self.program_action_buttons: list[Button] = []
        self.pack_action_buttons: list[Button] = []
        self.geometry_action_buttons: list[Button] = []
        self.last_program_drop: tuple[str, float] = ("", 0.0)
        self.incoming_snapshots: dict[Path, tuple[int, int]] = {}
        self.incoming_reviewed: set[tuple[str, int, int]] = set()
        self.local_corpora: list[LocalCorpus] = []
        self.active_tab = "home"
        self.tab_buttons: dict[str, Button] = {}
        self.tab_frames: dict[str, Frame] = {}
        self.visible_tab_ids: set[str] = set()
        self.remote_status_thread: threading.Thread | None = None
        self.remote_status_in_flight = False
        self.latest_engine_version = ""
        self.mapping_download_available = False
        self.mapping_install_needed = False
        self.store_loaded_once = False
        self.programs_loaded_once = False
        self.catalog_refresh_in_flight = False
        self.catalog_source = "loading"

        page_pad = THEME["spacing"]["page_pad"]
        section_gap = THEME["spacing"]["section_gap"]
        button_gap = THEME["spacing"]["button_gap"]

        top = Frame(root, **panel_kwargs())
        top.pack(fill="x", padx=page_pad, pady=page_pad)
        if local_source_mode():
            Label(top, text=(("LOCAL CUSTOM MODE  |  Editable programs stay in this launcher folder  |  "
                              if local_custom_mode() else
                              "LOCAL SOURCE MODE  |  Mapping runs from source; Network loads when present  |  ") +
                             f"Data library: {RUNTIME_ROOT}"),
                  bg=THEME["colors"]["panel_bg"], fg=THEME["colors"]["accent"],
                  font=("Segoe UI", 10, "bold"), wraplength=1200,
                  justify=LEFT).pack(fill="x", padx=14, pady=(10, 0))

        header_row = Frame(top, bg=THEME["colors"]["panel_bg"])
        header_row.pack(fill="x", padx=14, pady=(12, 8))
        if self.brand_mark is not None:
            Label(header_row, image=self.brand_mark, bg=THEME["colors"]["panel_bg"]).pack(
                side=LEFT, padx=(0, 14))
        hero_copy = Frame(header_row, bg=THEME["colors"]["panel_bg"])
        hero_copy.pack(side=LEFT, fill="x", expand=True)
        Label(hero_copy, text="DAEDALMAP", bg=THEME["colors"]["panel_bg"],
              fg=THEME["colors"]["accent"], font=("Segoe UI", 10, "bold")).pack(anchor="w")
        Label(hero_copy, text="Your local map library", bg=THEME["colors"]["panel_bg"],
              fg=THEME["colors"]["text"], font=("Segoe UI", 20, "bold")).pack(anchor="w", pady=(1, 1))
        Label(hero_copy, text="Data packs and tools, together on this computer.",
              **label_kwargs("muted")).pack(anchor="w")

        indicator_row = Frame(header_row, bg=THEME["colors"]["panel_bg"])
        indicator_row.pack(side=RIGHT)
        self.online_dot_label = Label(indicator_row, textvariable=self.online_dot_var, bg=THEME["colors"]["panel_bg"], fg=THEME["colors"]["muted_text"], font=("Segoe UI Symbol", 12))
        self.online_dot_label.pack(side=LEFT, padx=(0, 6))
        Label(indicator_row, textvariable=self.online_var, **label_kwargs("muted")).pack(side=LEFT)
        Label(indicator_row, textvariable=self.launcher_update_var, **label_kwargs("muted")).pack(
            side=LEFT, padx=(12, 0))
        self.launcher_update_button = Button(indicator_row, text="Download Update",
                                             command=self.download_launcher_update_clicked,
                                             **button_kwargs("accent"))

        accent_line = Frame(top, height=2, bg=THEME["colors"]["accent_strong"])
        accent_line.pack(fill="x", padx=14, pady=(0, 4))

        self.installed_text = self._create_selectable_text(top, height=1, muted=True)
        self.installed_text.pack(fill="x", padx=12, pady=(6, 0))
        self.status_text = self._create_selectable_text(top, height=1, muted=False)
        self.status_text.pack(fill="x", padx=12, pady=(6, 12))

        navigation = Frame(root, bg=THEME["colors"]["app_bg"])
        navigation.pack(fill="x", padx=page_pad, pady=(0, section_gap))
        for tab_id, title in NAV_TABS:
            button = Button(navigation, text=title, command=lambda value=tab_id: self.show_tab(value),
                            **tab_button_kwargs(False))
            self.tab_buttons[tab_id] = button

        content = Frame(root, bg=THEME["colors"]["app_bg"])
        content.pack(fill=BOTH, expand=True, padx=page_pad, pady=(0, page_pad))
        self.content_root = content

        self._build_home_tab(content, button_gap, section_gap)
        self._build_programs_tab(content, button_gap, section_gap)
        # Keep the shared action references used by the launcher state machine.
        # The visible controls live only on the Programs card.
        self.install_button = self.get_mapping_button
        self.launch_button = self.open_mapping_program_button
        self._build_library_tab(content, button_gap, section_gap)
        self._build_geometry_tab(content, button_gap)
        self._build_geometry_mcp_tab(content)
        self._build_research_tab(content, section_gap)
        self._build_llm_tab(content, section_gap)
        self._build_runtime_tab(content)
        self._build_network_tab(content)
        self._build_advanced_tab(content, section_gap)
        if local_source_mode():
            self.set_pack_filter("installed")
            self.set_geometry_filter("installed")
            self.pack_filter_buttons["cloud"].configure(state=DISABLED)
            self.geometry_filter_buttons["cloud"].configure(state=DISABLED)
            for button in (self.refresh_store_button, self.refresh_geometry_button,
                           self.import_data_button, self.import_geometry_button):
                button.configure(state=DISABLED)
        self.install_button.configure(state=DISABLED)
        if not has_usable_installed_engine():
            self.launch_button.configure(state=DISABLED)

        self.reconcile_runtime_state()
        self.refresh_installed_label()
        self.kick_remote_status_refresh()
        self.show_tab("home")
        if not local_source_mode():
            cached_catalog = cached_storefront_catalog()
            if cached_catalog is not None:
                try:
                    self.apply_storefront_catalog(cached_catalog, source="saved")
                except (LauncherError, GeometryStoreError) as exc:
                    self.append_log(f"Saved Downloads list could not be read: {exc}")
            self.refresh_store_clicked()
        else:
            self.set_store_status("local")
            self.online_var.set("Local custom")
            self.online_dot_label.configure(fg=THEME["colors"]["accent"])
        self.root.after(500, self.poll_navigation)
        if not local_source_mode():
            self.root.after(250, self.kick_launcher_update_check)
        self.root.after(2000, self.periodic_refresh)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def reconcile_runtime_state(self) -> None:
        state = load_state()
        pid = int(state.runtime_pid or 0)
        port_open = is_runtime_reachable()
        if pid:
            if _pid_is_alive(pid):
                if port_open:
                    self.append_log(f"Found existing runtime PID {pid} responding on port {DEFAULT_PORT}")
                else:
                    self.append_log(
                        f"Recorded runtime PID {pid} is alive but port {DEFAULT_PORT} is not reachable yet"
                    )
                return
            self.append_log(f"Clearing stale runtime PID {pid} (process is no longer running)")
            state.runtime_pid = 0
            save_state(state)
        if port_open:
            self.append_log(
                f"WARNING: port {DEFAULT_PORT} is in use by an unknown process that this launcher did not start"
            )

    def _build_home_tab(self, parent: Frame, button_gap: int, section_gap: int) -> None:
        frame = Frame(parent, bg=THEME["colors"]["app_bg"])
        self.tab_frames["home"] = frame

        actions_panel = Frame(frame, **panel_kwargs())
        actions_panel.pack(fill="x", pady=(0, section_gap))
        Label(actions_panel, text="Start here", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 0))
        Label(
            actions_panel,
            text="Choose data packs for records and programs for local tools. Both use your shared data folder.",
            **label_kwargs("muted"),
        ).pack(anchor="w", padx=12, pady=(4, 12))

        overview = Frame(frame, **panel_kwargs())
        overview.pack(fill="x", pady=(0, section_gap))
        Label(overview, text="Your local library", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 0))
        self.runtime_overview_text = self._create_selectable_text(overview, height=4, muted=False)
        self.runtime_overview_text.pack(fill="x", padx=12, pady=(4, 12))

        home_log_panel = Frame(frame, **panel_kwargs())
        home_log_panel.pack(fill=BOTH, expand=True)
        Label(home_log_panel, text="Recent Activity", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 8))
        self.home_log_text = Text(home_log_panel, height=10, wrap="word", **text_kwargs())
        self.home_log_text.pack(fill=BOTH, expand=True, padx=12, pady=(0, 12))
        self._enable_text_copy(self.home_log_text)

    def _build_programs_tab(self, parent: Frame, button_gap: int, section_gap: int) -> None:
        frame = Frame(parent, bg=THEME["colors"]["app_bg"])
        self.tab_frames["programs"] = frame

        browser = Frame(frame, bg=THEME["colors"]["app_bg"])
        browser.pack(fill=BOTH, expand=True)
        self.program_canvas = Canvas(browser, bg=THEME["colors"]["panel_bg"],
                                     bd=0, highlightthickness=0)
        self.program_canvas.pack(side=LEFT, fill=BOTH, expand=True)
        program_scroll = Scrollbar(browser, command=self.program_canvas.yview, **scrollbar_kwargs())
        program_scroll.pack(side=RIGHT, fill=Y)
        self.program_canvas.configure(yscrollcommand=program_scroll.set)
        programs_panel = Frame(self.program_canvas, **panel_kwargs())
        self.program_window = self.program_canvas.create_window((0, 0), window=programs_panel, anchor="nw")
        programs_panel.bind("<Configure>", lambda _event: self.program_canvas.configure(
            scrollregion=self.program_canvas.bbox("all")))
        self.program_canvas.bind("<Configure>", lambda event: self.program_canvas.itemconfigure(
            self.program_window, width=event.width))
        self.program_canvas.bind("<MouseWheel>", self._scroll_programs)
        Label(programs_panel, text="Programs", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 0))
        self.programs_status = self._create_selectable_text(programs_panel, height=1, muted=False)
        self.programs_status.pack(fill="x", padx=12, pady=(4, 8))
        self.program_cards = Frame(programs_panel, bg=THEME["colors"]["panel_bg"])
        self.program_cards.pack(fill="x", padx=12, pady=(0, 8))
        self.program_cards.bind("<Configure>",
                                lambda event: self._reflow_card_grid(self.program_cards, event.width))
        self.mapping_card = self._catalog_card(self.program_cards)
        mapping_card = self.mapping_card
        mapping_card.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=8)
        Label(mapping_card, textvariable=self.mapping_badge_var, bg=THEME["colors"]["card_bg"],
              fg=THEME["colors"]["accent"], font=THEME["fonts"]["mono"]).pack(anchor="w", padx=14, pady=(10, 2))
        Label(mapping_card, text="Explore Map", bg=THEME["colors"]["card_bg"],
              fg=THEME["colors"]["text"], font=THEME["fonts"]["card"],
              wraplength=370, justify=LEFT, anchor="nw", height=2).pack(fill="x", padx=14)
        Label(mapping_card, text="Build and install Explore Map from local source", bg=THEME["colors"]["card_bg"],
              fg=THEME["colors"]["muted_text"], font=THEME["fonts"]["body"],
              wraplength=370, justify=LEFT, anchor="nw", height=2).pack(fill="x", padx=14, pady=(5, 5))
        Label(mapping_card, text=" ", bg=THEME["colors"]["card_bg"],
              font=THEME["fonts"]["mono"]).pack(anchor="w", padx=14)
        mapping_actions = Frame(mapping_card, bg=THEME["colors"]["card_bg"])
        mapping_actions.pack(fill="x", padx=14, pady=(9, 12))
        self.get_mapping_button = Button(mapping_actions, textvariable=self.install_button_var,
                                         command=self.install_clicked, **button_kwargs("accent"))
        self.get_mapping_button.pack(side=LEFT, padx=(0, 8))
        self.get_mapping_button.configure(state=DISABLED)
        self.mapping_current_label = Label(mapping_actions, text="", bg=THEME["colors"]["card_bg"],
                                           fg=THEME["colors"]["accent"], font=THEME["fonts"]["mono"])
        self.open_mapping_program_button = Button(mapping_actions, textvariable=self.open_map_button_var, command=self.launch_clicked,
                                                  **button_kwargs("primary"))
        self.open_mapping_program_button.pack(side=LEFT, padx=(0, 8))
        Button(mapping_actions, text="About", command=lambda: webbrowser.open("https://www.daedalmap.com/explore"),
               **button_kwargs("primary")).pack(side=LEFT)
        program_buttons = Frame(programs_panel, bg=THEME["colors"]["panel_bg"])
        program_buttons.pack(fill="x", padx=12, pady=(0, 8))
        self.refresh_programs_button = Button(program_buttons, text="Refresh Programs", command=self.refresh_programs_clicked,
                                              **button_kwargs("primary"))
        self.refresh_programs_button.pack(side=LEFT, padx=(0, button_gap))
        self.import_program_button = Button(program_buttons, text="Import ZIP", command=self.import_program_clicked,
                                            **button_kwargs("primary"))
        self.import_program_button.pack(side=LEFT, padx=(0, button_gap))
        Button(program_buttons, text="Downloads website", command=lambda: webbrowser.open("https://www.daedalmap.com/downloadable/"),
               **button_kwargs("primary")).pack(side=LEFT)
        self.program_drop_label = Label(programs_panel, text="Drop a DaedalMap program ZIP here, or use Import ZIP",
                                        **label_kwargs("muted"))
        self.program_drop_label.pack(anchor="w", padx=12, pady=(0, 12))
        self.update_program_list()
        self._configure_program_drop()
        self._bind_program_scroll(programs_panel)

    def _scroll_programs(self, event) -> None:
        self.program_canvas.yview_scroll(-1 if event.delta > 0 else 1, "units")

    def _bind_program_scroll(self, widget) -> None:
        widget.bind("<MouseWheel>", self._scroll_programs)
        for child in widget.winfo_children():
            self._bind_program_scroll(child)

    def _build_library_tab(self, parent: Frame, button_gap: int, section_gap: int) -> None:
        frame = Frame(parent, bg=THEME["colors"]["app_bg"])
        self.tab_frames["library"] = frame

        store = Frame(frame, **panel_kwargs())
        store.pack(fill=BOTH, expand=True)
        Label(store, text="Browse maintained data", bg=THEME["colors"]["panel_bg"],
              fg=THEME["colors"]["text"], font=THEME["fonts"]["title"]).pack(anchor="w", padx=14, pady=(12, 2))
        self.store_status_text = self._create_selectable_text(store, height=1, muted=True)
        self.store_status_text.pack(fill="x", padx=14, pady=(0, 8))

        store_buttons = Frame(store, bg=THEME["colors"]["panel_bg"])
        store_buttons.pack(fill="x", padx=14, pady=(0, 8))
        Label(store_buttons, text="Search packs", **label_kwargs("muted")).pack(side=LEFT, padx=(0, button_gap))
        self.pack_search_entry = Entry(store_buttons, textvariable=self.pack_search_var, **listbox_kwargs())
        self.pack_search_entry.pack(side=LEFT, fill="x", expand=True, padx=(0, button_gap), ipady=6)
        self.import_data_button = Button(store_buttons, text="Import Data ZIP", command=self.import_data_zip_clicked,
                                         **button_kwargs("primary"))
        self.import_data_button.pack(side=LEFT, padx=(0, button_gap))
        self.refresh_store_button = Button(store_buttons, text="Refresh", command=self.refresh_store_clicked,
                                           **button_kwargs("primary"))
        self.refresh_store_button.pack(side=LEFT)

        filter_row = Frame(store, bg=THEME["colors"]["panel_bg"])
        filter_row.pack(fill="x", padx=14, pady=(0, 7))
        self.pack_filter_buttons: dict[str, Button] = {}
        for filter_id, caption in (("cloud", "Cloud"), ("installed", "Installed")):
            button = Button(filter_row, text=caption,
                            command=lambda value=filter_id: self.set_pack_filter(value),
                            **tab_button_kwargs(filter_id == "cloud"))
            button.pack(side=LEFT, padx=(0, button_gap))
            self.pack_filter_buttons[filter_id] = button
        self.cancel_data_download_button = Button(filter_row, text="Cancel download",
                                                  command=self.cancel_pack_download_clicked,
                                                  **button_kwargs("primary"))
        self.pack_search_var.trace_add("write", lambda *_: self.update_pack_list())

        pack_browser = Frame(store, bg=THEME["colors"]["panel_bg"])
        pack_browser.pack(fill=BOTH, expand=True, padx=14, pady=(0, 12))
        self.pack_canvas = Canvas(pack_browser, bg=THEME["colors"]["panel_bg"],
                                  bd=0, highlightthickness=0)
        self.pack_canvas.pack(side=LEFT, fill=BOTH, expand=True)
        pack_scroll = Scrollbar(pack_browser, command=self.pack_canvas.yview, **scrollbar_kwargs())
        pack_scroll.pack(side=RIGHT, fill=Y)
        self.pack_canvas.configure(yscrollcommand=pack_scroll.set)
        self.pack_canvas.bind("<MouseWheel>", self._scroll_pack_cards)
        self.pack_cards = Frame(self.pack_canvas, bg=THEME["colors"]["panel_bg"])
        self.pack_window = self.pack_canvas.create_window((0, 0), window=self.pack_cards, anchor="nw")
        self.pack_cards.bind("<Configure>", lambda _event: self.pack_canvas.configure(scrollregion=self.pack_canvas.bbox("all")))
        self.pack_canvas.bind("<Configure>", self._on_pack_canvas_resize)

    def _build_geometry_tab(self, parent: Frame, button_gap: int) -> None:
        frame = Frame(parent, bg=THEME["colors"]["app_bg"])
        self.tab_frames["geometry"] = frame
        store = Frame(frame, **panel_kwargs())
        store.pack(fill=BOTH, expand=True)
        Label(store, text="Browse maintained geometry", bg=THEME["colors"]["panel_bg"],
              fg=THEME["colors"]["text"], font=THEME["fonts"]["title"]).pack(anchor="w", padx=14, pady=(12, 2))
        self.geometry_status_text = self._create_selectable_text(store, height=1, muted=True)
        self.geometry_status_text.pack(fill="x", padx=14, pady=(0, 8))
        buttons = Frame(store, bg=THEME["colors"]["panel_bg"])
        buttons.pack(fill="x", padx=14, pady=(0, 8))
        Label(buttons, text="Search geometry", **label_kwargs("muted")).pack(side=LEFT, padx=(0, button_gap))
        Entry(buttons, textvariable=self.geometry_search_var, **listbox_kwargs()).pack(
            side=LEFT, fill="x", expand=True, padx=(0, button_gap), ipady=6)
        self.import_geometry_button = Button(buttons, text="Import Geometry ZIP",
                                             command=self.import_geometry_zip_clicked, **button_kwargs("primary"))
        self.import_geometry_button.pack(side=LEFT, padx=(0, button_gap))
        self.refresh_geometry_button = Button(buttons, text="Refresh", command=self.refresh_store_clicked,
                                              **button_kwargs("primary"))
        self.refresh_geometry_button.pack(side=LEFT)
        filters = Frame(store, bg=THEME["colors"]["panel_bg"])
        filters.pack(fill="x", padx=14, pady=(0, 7))
        self.geometry_filter_buttons = {}
        for filter_id, caption in (("cloud", "Cloud"), ("installed", "Installed")):
            button = Button(filters, text=caption,
                            command=lambda value=filter_id: self.set_geometry_filter(value),
                            **tab_button_kwargs(filter_id == "cloud"))
            button.pack(side=LEFT, padx=(0, button_gap))
            self.geometry_filter_buttons[filter_id] = button
        self.cancel_geometry_download_button = Button(filters, text="Cancel download",
                                                      command=self.cancel_pack_download_clicked,
                                                      **button_kwargs("primary"))
        sections = Frame(store, bg=THEME["colors"]["panel_bg"])
        sections.pack(fill="x", padx=14, pady=(0, 7))
        self.geometry_section_buttons = {}
        for section_id, caption in (("all", "All"), ("countries", "Countries"),
                                    ("global", "Global"), ("bridges", "Bridges")):
            button = Button(sections, text=caption,
                            command=lambda value=section_id: self.set_geometry_section(value),
                            **tab_button_kwargs(section_id == "all"))
            button.pack(side=LEFT, padx=(0, button_gap))
            self.geometry_section_buttons[section_id] = button
        self.geometry_search_var.trace_add("write", lambda *_: self.update_geometry_list())
        browser = Frame(store, bg=THEME["colors"]["panel_bg"])
        browser.pack(fill=BOTH, expand=True, padx=14, pady=(0, 12))
        self.geometry_canvas = Canvas(browser, bg=THEME["colors"]["panel_bg"], bd=0, highlightthickness=0)
        self.geometry_canvas.pack(side=LEFT, fill=BOTH, expand=True)
        scrollbar = Scrollbar(browser, command=self.geometry_canvas.yview, **scrollbar_kwargs())
        scrollbar.pack(side=RIGHT, fill=Y)
        self.geometry_canvas.configure(yscrollcommand=scrollbar.set)
        self.geometry_cards = Frame(self.geometry_canvas, bg=THEME["colors"]["panel_bg"])
        self.geometry_window = self.geometry_canvas.create_window((0, 0), window=self.geometry_cards, anchor="nw")
        self.geometry_cards.bind("<Configure>", lambda _event: self.geometry_canvas.configure(
            scrollregion=self.geometry_canvas.bbox("all")))
        self.geometry_canvas.bind("<Configure>", lambda event: self._on_geometry_canvas_resize(event))
        self.update_geometry_list()

    def _build_geometry_mcp_tab(self, parent: Frame) -> None:
        frame = Frame(parent, bg=THEME["colors"]["app_bg"])
        self.tab_frames["geometry_mcp"] = frame
        service = Frame(frame, **panel_kwargs())
        service.pack(fill="x")
        Label(service, text="Geometry MCP", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 4))
        Label(service, text="Connect an MCP client to your downloaded geometry.",
              **label_kwargs("muted")).pack(anchor="w", padx=12, pady=(0, 8))
        self.geometry_mcp_button = Button(service, text="Start Geometry MCP",
                                          command=lambda: self.toggle_mcp_service("daedalmap-geometry-mcp"),
                                          **button_kwargs("primary"))
        self.geometry_mcp_button.pack(anchor="w", padx=12, pady=(0, 8))
        self.geometry_mcp_text = self._create_selectable_text(service, height=4, muted=False)
        self.geometry_mcp_text.pack(fill="x", padx=12, pady=(0, 12))

    def _on_geometry_canvas_resize(self, event) -> None:
        self.geometry_canvas.itemconfigure(self.geometry_window, width=event.width)
        self._reflow_card_grid(self.geometry_cards, event.width)

    def _on_pack_canvas_resize(self, event) -> None:
        self.pack_canvas.itemconfigure(self.pack_window, width=event.width)
        self._reflow_card_grid(self.pack_cards, event.width)

    def _catalog_card(self, parent: Frame, *, height: int = 220) -> Frame:
        card = Frame(parent, bg=THEME["colors"]["card_bg"], width=400, height=height,
                     highlightthickness=1, highlightbackground=THEME["colors"]["border"])
        card.pack_propagate(False)
        return card

    def _reflow_card_grid(self, container: Frame, width: int) -> None:
        if width <= 1:
            width = 900  # Tk reports 1 before the first visible layout pass.
        columns = max(1, min(5, (max(0, width) + 16) // 416))
        for column in range(5):
            container.grid_columnconfigure(column, weight=0,
                                           minsize=400 if column < columns else 0)
        for index, card in enumerate(container.winfo_children()):
            card.grid(row=index // columns, column=index % columns,
                      sticky="nsew", padx=(0, 16) if index % columns < columns - 1 else 0,
                      pady=8)

    def _configure_program_drop(self) -> None:
        if DND_FILES is None:
            self.program_drop_label.configure(text="Use Import ZIP to add an existing program download")
            return
        self.root.drop_target_register(DND_FILES)
        self.root.dnd_bind("<<Drop>>", self._program_zip_dropped)
        self.program_drop_label.drop_target_register(DND_FILES)
        self.program_drop_label.dnd_bind("<<Drop>>", self._program_zip_dropped)

    def _scroll_pack_cards(self, event) -> None:
        self.pack_canvas.yview_scroll(-1 if event.delta > 0 else 1, "units")

    def _bind_pack_scroll(self, widget) -> None:
        widget.bind("<MouseWheel>", self._scroll_pack_cards)
        for child in widget.winfo_children():
            self._bind_pack_scroll(child)

    def _bind_geometry_scroll(self, widget) -> None:
        widget.bind("<MouseWheel>", lambda event: self.geometry_canvas.yview_scroll(
            -1 if event.delta > 0 else 1, "units"))
        for child in widget.winfo_children():
            self._bind_geometry_scroll(child)

    def _program_zip_dropped(self, event) -> None:
        paths = self.root.tk.splitlist(event.data)
        if len(paths) != 1:
            messagebox.showinfo("Import one ZIP", "Drop one program ZIP at a time.")
            return
        now = time.monotonic()
        if self.last_program_drop[0] == paths[0] and now - self.last_program_drop[1] < 0.5:
            return
        self.last_program_drop = (paths[0], now)
        self.import_program_path(Path(paths[0]))

    def _build_research_tab(self, parent: Frame, section_gap: int) -> None:
        frame = Frame(parent, bg=THEME["colors"]["app_bg"])
        self.tab_frames["research"] = frame

        research_panel = Frame(frame, **panel_kwargs())
        research_panel.pack(fill="x", pady=(0, section_gap))
        Label(research_panel, text="Research MCP", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 0))
        Label(research_panel, text="Connect an MCP client to your local research data.",
              **label_kwargs("muted")).pack(anchor="w", padx=12, pady=(4, 8))
        research_buttons = Frame(research_panel, bg=THEME["colors"]["panel_bg"])
        research_buttons.pack(fill="x", padx=12, pady=(0, 8))
        self.research_mcp_button = Button(research_buttons, text="Start Research MCP",
                                          command=lambda: self.toggle_mcp_service("daedalmap-research-mcp"),
                                          **button_kwargs("primary"))
        self.research_mcp_button.pack(side=LEFT)

        self.research_status_text = self._create_selectable_text(research_panel, height=6, muted=False)
        self.research_status_text.pack(fill="x", padx=12, pady=(0, 12))

        corpus_panel = Frame(frame, **panel_kwargs())
        corpus_panel.pack(fill="x")
        Label(corpus_panel, text="Saved research", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 0))
        self.research_detail_text = self._create_selectable_text(corpus_panel, height=9, muted=False)
        self.research_detail_text.pack(fill="x", padx=12, pady=(0, 12))

    def _build_llm_tab(self, parent: Frame, section_gap: int) -> None:
        frame = Frame(parent, bg=THEME["colors"]["app_bg"])
        self.tab_frames["llm"] = frame

        llm_panel = Frame(frame, **panel_kwargs())
        llm_panel.pack(fill="x", pady=(0, section_gap))
        Label(llm_panel, text="AI connections", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 0))
        Label(llm_panel, text="AI provider status for local tools.",
              **label_kwargs("muted")).pack(anchor="w", padx=12, pady=(4, 8))
        self.llm_status_text = self._create_selectable_text(llm_panel, height=5, muted=False)
        self.llm_status_text.pack(fill="x", padx=12, pady=(0, 12))

    def _build_runtime_tab(self, parent: Frame) -> None:
        frame = Frame(parent, bg=THEME["colors"]["app_bg"])
        self.tab_frames["runtime"] = frame

        runtime_panel = Frame(frame, **panel_kwargs())
        runtime_panel.pack(fill="x", pady=(0, THEME["spacing"]["section_gap"]))
        Label(runtime_panel, text="Explore Map", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 0))
        Label(
            runtime_panel,
            text="Open the local map in your browser. Manage data in Data and launcher preferences in Settings.",
            **label_kwargs("muted"),
        ).pack(anchor="w", padx=12, pady=(4, 8))
        if local_source_mode():
            actions = Frame(runtime_panel, bg=THEME["colors"]["panel_bg"])
            actions.pack(anchor="w", padx=12, pady=(0, 8))
            Button(actions, text="Open Local Map", command=self.launch_clicked,
                   **button_kwargs("accent")).pack(side=LEFT, padx=(0, 8))
            Button(actions, text="Stop Local Map", command=self.stop_clicked,
                   **button_kwargs("primary")).pack(side=LEFT)
        self.update_text = self._create_selectable_text(runtime_panel, height=1, muted=True)
        self.update_text.pack(fill="x", padx=12, pady=(2, 4))
        self.runtime_detail_text = self._create_selectable_text(runtime_panel, height=4, muted=False)
        self.runtime_detail_text.pack(fill="x", padx=12, pady=(4, 12))

        map_panel = Frame(frame, **panel_kwargs())
        map_panel.pack(fill="x", pady=(0, THEME["spacing"]["section_gap"]))
        Label(map_panel, text="Map detail", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 0))
        Label(map_panel, text="Choose an offline world map. Reload the map after changing this.",
              **label_kwargs("muted")).pack(anchor="w", padx=12, pady=(4, 6))
        Label(map_panel, textvariable=self.map_tiles_status_var, **label_kwargs("muted")).pack(anchor="w", padx=12, pady=(0, 6))
        map_buttons = Frame(map_panel, bg=THEME["colors"]["panel_bg"])
        map_buttons.pack(fill="x", padx=12, pady=(0, 6))
        self.use_basic_map_button = Button(map_buttons, text="Use built-in offline map", command=self.use_basic_map_clicked,
                                           **button_kwargs("primary"))
        self.use_basic_map_button.pack(side=LEFT, padx=(0, 8))
        self.download_map_detail_button = Button(map_buttons, textvariable=self.map_tiles_download_var,
                                                 command=self.download_map_detail_clicked, **button_kwargs("accent"))
        self.download_map_detail_button.pack(side=LEFT, padx=(0, 8))
        self.cancel_map_detail_button = Button(map_buttons, text="Cancel download",
                                               command=self.cancel_map_detail_clicked,
                                               **button_kwargs("primary"))
        self.use_detail_map_button = Button(map_buttons, text="Use detailed offline map", command=self.use_detail_map_clicked,
                                            **button_kwargs("primary"))
        self.use_detail_map_button.pack(side=LEFT)
        self.refresh_map_tile_status()

        log_panel = Frame(frame, **panel_kwargs())
        log_panel.pack(fill=BOTH, expand=True)
        Label(log_panel, text="Activity Log", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 8))
        self.log_text = Text(log_panel, wrap="word", **text_kwargs())
        self.log_text.pack(fill=BOTH, expand=True, padx=12, pady=(0, 12))
        self._enable_text_copy(self.log_text)

    def _build_network_tab(self, parent: Frame) -> None:
        frame = Frame(parent, bg=THEME["colors"]["app_bg"])
        self.tab_frames["network"] = frame
        panel = Frame(frame, **panel_kwargs())
        panel.pack(fill="x")
        Label(panel, text="Network settings", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 0))
        Label(panel, text="Network runs in Mapping. You can also open it manually from the map.",
              **label_kwargs("muted")).pack(anchor="w", padx=12, pady=(4, 8))
        self.network_auto_button = Button(panel, command=self.toggle_network_auto_start,
                                          **button_kwargs("primary"))
        self.network_auto_button.pack(anchor="w", padx=12, pady=(0, 8))
        self.network_status_text = self._create_selectable_text(panel, height=2, muted=True)
        self.network_status_text.pack(fill="x", padx=12, pady=(0, 12))

    def toggle_network_auto_start(self) -> None:
        enabled = not network_start_with_mapping()
        save_network_start_with_mapping(enabled)
        self.refresh_network_settings()
        self.append_log(f"Start Network with Mapping: {'on' if enabled else 'off'}; applies next time Mapping opens")

    def refresh_network_settings(self) -> None:
        enabled = network_start_with_mapping()
        source_root = local_network_source_root()
        installed = source_root is not None or (not local_source_mode() and installed_network_root() is not None)
        mapping_installed = has_usable_installed_engine()
        self.network_auto_button.configure(text=f"Start with Mapping: {'On' if enabled else 'Off'}",
                                           state=NORMAL if installed else DISABLED)
        self._set_selectable_text(self.network_status_text,
                                  (("Local Network source is available from Open Network in Mapping."
                                    if source_root else "Installed Network is available from Open Network in Mapping.")
                                   if mapping_installed else
                                   "Network runs as a CLI without Mapping. Use Export Parquet in Programs.")
                                  if installed else ("No compatible Network source is in this Mapping checkout."
                                                     if local_source_mode() else
                                                     "Install a compatible Network expansion to use this setting."))

    def _build_advanced_tab(self, parent: Frame, section_gap: int) -> None:
        frame = Frame(parent, bg=THEME["colors"]["app_bg"])
        self.tab_frames["advanced"] = frame

        advanced_panel = Frame(frame, **panel_kwargs())
        advanced_panel.pack(fill="x", pady=(0, section_gap))
        Label(advanced_panel, text="Settings", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 0))
        Label(
            advanced_panel,
            text="A new library starts beside the launcher. Choose another folder if you need more space.",
            **label_kwargs("muted"),
        ).pack(anchor="w", padx=12, pady=(4, 8))
        advanced_buttons = Frame(advanced_panel, bg=THEME["colors"]["panel_bg"])
        advanced_buttons.pack(fill="x", padx=12, pady=(0, 8))
        self.storage_button = Button(advanced_buttons, text="Change Library Location", command=self.choose_storage_location_clicked, **button_kwargs("primary"))
        self.storage_button.pack(side=LEFT, padx=(0, THEME["spacing"]["button_gap"]))
        self.advanced_status_text = self._create_selectable_text(advanced_panel, height=8, muted=False)
        self.advanced_status_text.pack(fill="x", padx=12, pady=(0, 12))

        mode_panel = Frame(frame, **panel_kwargs())
        mode_panel.pack(fill="x", pady=(0, section_gap))
        Label(mode_panel, text="Launcher connection", **label_kwargs("section")).pack(anchor="w", padx=12, pady=(12, 2))
        Label(mode_panel, text="Local custom makes editable copies of installed Mapping and Network inside this launcher folder and skips DaedalMap release and store checks.",
              **label_kwargs("muted")).pack(anchor="w", padx=12, pady=(0, 8))
        mode_buttons = Frame(mode_panel, bg=THEME["colors"]["panel_bg"])
        mode_buttons.pack(fill="x", padx=12, pady=(0, 8))
        Button(mode_buttons, text="Connect to DaedalMap", command=self.use_connected_mode_clicked,
               **button_kwargs("primary")).pack(side=LEFT, padx=(0, 8))
        Button(mode_buttons, text="Run Local Custom", command=self.use_local_custom_mode_clicked,
               **button_kwargs("primary")).pack(side=LEFT)
        Label(mode_panel, text=f"Editable programs: {LOCAL_CUSTOM_ROOT}  |  Python: managed inside launcher-data",
              **label_kwargs("muted"), wraplength=1100, justify=LEFT).pack(anchor="w", padx=12)
        Label(mode_panel, textvariable=self.launcher_mode_var, **label_kwargs("muted"),
              wraplength=1100, justify=LEFT).pack(anchor="w", padx=12, pady=(4, 12))
        self.refresh_launcher_mode_label()

    def refresh_launcher_mode_label(self) -> None:
        active = ("Local custom" if local_custom_mode() else
                  "Local source (developer)" if local_source_mode() else "Connected to DaedalMap")
        settings = load_launcher_mode()
        saved = settings["mode"]
        pending_restart = ((saved == "local_custom") != local_custom_mode())
        pending = " Restart pending; reopen the launcher to apply it." if pending_restart else ""
        self.launcher_mode_var.set(f"Current session: {active}. Saved choice: {saved.replace('_', ' ')}.{pending}")

    def offer_restart_after_mode_change(self) -> None:
        if messagebox.askyesno("Settings changed", "Restart the launcher now to apply this setting? Running local tools will close.",
                               confirm_label="Restart now", cancel_label="Later"):
            self.on_close(restart_command=launcher_restart_command())

    def use_local_custom_mode_clicked(self) -> None:
        try:
            prepare_local_custom_sources()
        except (LauncherError, OSError) as exc:
            messagebox.showerror("Local custom", str(exc))
            return
        previous = load_launcher_mode()
        if previous["mode"] == "local_custom":
            return
        save_launcher_mode("local_custom")
        self.refresh_launcher_mode_label()
        self.append_log("Local custom mode saved")
        self.offer_restart_after_mode_change()

    def use_connected_mode_clicked(self) -> None:
        settings = load_launcher_mode()
        if settings["mode"] == "connected":
            return
        save_launcher_mode("connected")
        self.refresh_launcher_mode_label()
        self.append_log("DaedalMap connection saved")
        self.offer_restart_after_mode_change()

    def sync_navigation(self, mapping_installed: bool, installed_products: set[str]) -> None:
        visible = visible_launcher_tabs(mapping_installed, installed_products)
        if visible != self.visible_tab_ids:
            for button in self.tab_buttons.values():
                button.pack_forget()
            for tab_id, _title in NAV_TABS:
                if tab_id in visible:
                    self.tab_buttons[tab_id].pack(side=LEFT, padx=(0, THEME["spacing"]["button_gap"]))
            self.visible_tab_ids = visible
        if self.active_tab not in visible:
            self.show_tab("home")
        self.refresh_mcp_service_views(installed_products)

    def _mcp_program(self, product: str) -> AvailableProgram | None:
        return next((program for program in include_installed_programs(self.available_programs)
                     if program.product == product), None)

    def toggle_mcp_service(self, product: str) -> None:
        program = self._mcp_program(product)
        if program is None:
            messagebox.showinfo("Install MCP program", "Install this MCP program from Programs first.")
            return
        process = self.program_processes.get(product)
        if process is not None and process.poll() is None:
            self.stop_selected_program_clicked(program)
        else:
            self.run_selected_program_clicked(program)

    def refresh_mcp_service_views(self, installed_products: set[str]) -> None:
        busy = bool(self.worker and self.worker.is_alive())
        for product, button, status_text, title, port, path, data_tab in (
            ("daedalmap-research-mcp", self.research_mcp_button, self.research_status_text,
             "Research MCP", 7011, "research", "Data"),
            ("daedalmap-geometry-mcp", self.geometry_mcp_button, self.geometry_mcp_text,
             "Geometry MCP", 7010, "geometry", "Geometry"),
        ):
            installed = product in installed_products
            process = self.program_processes.get(product)
            running = process is not None and process.poll() is None
            button.configure(text=("Stop" if running else "Start") + f" {title}",
                             state=NORMAL if installed and not busy else DISABLED)
            lines = [f"Service: {'running' if running else 'installed, stopped' if installed else 'not installed'}"]
            if product == "daedalmap-research-mcp":
                lines.append(f"Data packs: {len(load_pack_state().get('installed_packs', []))} installed")
                lines.append(f"Saved research sets: {len(load_local_corpora())}")
            lines.extend((
                f"1. {'Start' if installed else 'Install'} {title} {'above or in Programs' if installed else 'in Programs'}; add local packages in {data_tab}.",
                f"2. In your MCP client, choose Streamable HTTP and enter http://127.0.0.1:{port}/mcp/{path}",
                "3. Keep the launcher open while using the service.",
            ))
            self._set_selectable_text(status_text, "\n".join(lines))

    def show_tab(self, tab_id: str) -> None:
        if tab_id not in self.visible_tab_ids:
            tab_id = "home"
        self.active_tab = tab_id
        for name, frame in self.tab_frames.items():
            if name == tab_id:
                frame.pack(fill=BOTH, expand=True)
            else:
                frame.pack_forget()
        for name, button in self.tab_buttons.items():
            button.configure(**tab_button_kwargs(name == tab_id))
        if tab_id == "research":
            self.refresh_research_views()

    def _animate_mapping_install(self) -> None:
        if self.mapping_spinner_job is None:
            return
        frames = ("◐", "◓", "◑", "◒")
        self.install_button_var.set(f"{self.mapping_spinner_verb} {frames[self.mapping_spinner_step % len(frames)]}")
        self.mapping_spinner_step += 1
        self.mapping_spinner_job = self.root.after(150, self._animate_mapping_install)

    def set_busy(self, busy: bool, *, mapping_install: bool = False, mapping_launch: bool = False,
                 map_tiles_download: bool = False, pack_download: str = "") -> None:
        if busy:
            self.install_button_idle_label = self.install_button_var.get()
        elif self.mapping_spinner_job is not None:
            self.root.after_cancel(self.mapping_spinner_job)
            self.mapping_spinner_job = None
        if not busy:
            self.mapping_launch_spinner.stop()
            if getattr(self, "map_tiles_download_spinner", None) is not None:
                self.map_tiles_download_spinner.stop()
            if getattr(self, "cancel_map_detail_button", None) is not None:
                self.cancel_map_detail_button.pack_forget()
            for name in ("cancel_data_download_button", "cancel_geometry_download_button"):
                button = getattr(self, name, None)
                if button is not None:
                    button.pack_forget()
            if self.active_pack_spinner is not None:
                self.active_pack_spinner.stop()
                self.active_pack_spinner = None
            if self.active_program_spinner is not None:
                self.active_program_spinner.stop()
                self.active_program_spinner = None
        state = DISABLED if busy else NORMAL
        for button in [*self.program_action_buttons, *self.pack_action_buttons,
                       *getattr(self, "geometry_action_buttons", [])]:
            if button.winfo_exists():
                button.configure(state=state)
        for widget_name in [
            "install_button",
            "get_mapping_button",
            "open_mapping_program_button",
            "launch_button",
            "settings_button",
            "refresh_store_button",
            "import_data_button",
            "import_geometry_button",
            "refresh_geometry_button",
            "refresh_programs_button",
            "install_program_button",
            "import_program_button",
            "run_program_button",
            "stop_program_button",
            "install_pack_button",
            "research_mcp_button",
            "geometry_mcp_button",
            "storage_button",
            "use_basic_map_button",
            "download_map_detail_button",
            "use_detail_map_button",
            "remote_map_style_entry",
            "use_remote_map_button",
        ]:
            widget = getattr(self, widget_name, None)
            if widget is not None:
                widget.configure(state=state)
        if busy:
            cancel_button = getattr(self, f"cancel_{pack_download}_download_button", None) if pack_download else None
            if cancel_button is not None:
                cancel_button.configure(text="Cancel download", state=NORMAL)
                cancel_button.pack(side=RIGHT)
            if mapping_install:
                self.mapping_spinner_verb = "Updating" if self.install_button_idle_label == "Update" else "Installing"
                self.mapping_spinner_step = 0
                self.mapping_spinner_job = True
                self._animate_mapping_install()
            elif mapping_launch:
                self.mapping_launch_spinner.start("Opening Map")
            elif map_tiles_download:
                self.map_tiles_download_spinner.start("Downloading")
                self.cancel_map_detail_button.configure(text="Cancel download", state=NORMAL)
                self.cancel_map_detail_button.pack(side=LEFT, padx=(0, 8))
            elif self.active_pack_spinner is not None:
                self.active_pack_spinner.start(self.active_pack_verb)
            elif self.active_program_spinner is not None:
                self.active_program_spinner.start(self.active_program_verb)
            else:
                self.install_button_var.set("Working...")
        elif self.install_button_var.get() == "Working..." or self.install_button_var.get().startswith(("Installing ", "Updating ")):
            self.install_button_var.set(self.install_button_idle_label)
        elif not self.mapping_install_needed:
            self.install_button.configure(state=DISABLED)
            self.get_mapping_button.configure(state=DISABLED)
        if not busy and not has_usable_installed_engine():
            self.open_mapping_program_button.configure(state=DISABLED)
            if not self.mapping_download_available:
                self.launch_button.configure(state=DISABLED)
        if not busy and getattr(self, "use_detail_map_button", None) is not None:
            self.refresh_map_tile_status()
        if not busy and hasattr(self, "research_mcp_button"):
            installed_products = {
                str(entry.get("product") or "") for entry in load_program_state()["installed_programs"]
                if entry.get("install_dir") and Path(str(entry["install_dir"])).is_dir()
            }
            self.refresh_mcp_service_views(installed_products)

    def _enable_text_copy(self, widget: Text) -> None:
        widget.bind("<Control-c>", lambda event, w=widget: self._copy_selected_text(w))
        widget.bind("<Control-C>", lambda event, w=widget: self._copy_selected_text(w))
        widget.bind("<Control-a>", lambda event, w=widget: self._select_all_text(w))
        widget.bind("<Control-A>", lambda event, w=widget: self._select_all_text(w))
        widget.bind("<Button-3>", lambda _event: None)

    def _copy_selected_text(self, widget: Text):
        try:
            text = widget.get("sel.first", "sel.last")
        except TclError:
            return "break"
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        return "break"

    def _select_all_text(self, widget: Text):
        widget.tag_add("sel", "1.0", "end-1c")
        widget.mark_set("insert", "1.0")
        widget.see("1.0")
        return "break"

    def _create_selectable_text(self, parent: Frame, *, height: int, muted: bool) -> Text:
        widget = Text(
            parent,
            height=height,
            wrap="word",
            bg=THEME["colors"]["panel_bg"],
            fg=THEME["colors"]["muted_text"] if muted else THEME["colors"]["text"],
            insertbackground=THEME["colors"]["accent_active"],
            selectbackground=THEME["colors"]["selection"],
            selectforeground=THEME["colors"]["text"],
            highlightthickness=0,
            bd=0,
            relief="flat",
            font=THEME["fonts"]["body"],
            padx=0,
            pady=0,
            cursor="xterm",
        )
        self._enable_text_copy(widget)
        return widget

    def _set_selectable_text(self, widget: Text, value: str) -> None:
        widget.configure(state=NORMAL)
        widget.delete("1.0", END)
        widget.insert("1.0", value or "")
        widget.configure(state=DISABLED)

    def append_log(self, message: str) -> None:
        if threading.get_ident() != self.ui_thread_id:
            self.root.after(0, lambda value=message: self.append_log(value))
            return
        stamp = time.strftime("%H:%M:%S")
        line = f"[{stamp}] {message}\n"
        self.log_text.insert(END, line)
        self.log_text.see(END)
        if hasattr(self, "home_log_text"):
            self.home_log_text.insert(END, line)
            self.home_log_text.see(END)
        self.root.update_idletasks()

    def set_status(self, message: str) -> None:
        self.status_var.set(message)
        self._set_selectable_text(self.status_text, message)
        self.root.update_idletasks()

    def periodic_refresh(self) -> None:
        try:
            for product, process in list(self.program_processes.items()):
                if process.poll() is not None:
                    self.append_log(f"{product} exited with code {process.returncode}")
                    self.program_processes.pop(product, None)
                    handle = self.program_log_handles.pop(product, None)
                    if handle is not None:
                        handle.close()
                    self.update_program_list()
            if self.process is not None and self.process.poll() is not None:
                exit_code = self.process.poll()
                self.append_log(f"Runtime process exited with code {exit_code}")
                self._close_runtime_log_handles()
                self.process = None
                state = load_state()
                if state.runtime_pid:
                    state.runtime_pid = 0
                    save_state(state)
            self.refresh_installed_label()
            self.scan_incoming_data()
            self.kick_remote_status_refresh(min_interval_seconds=REMOTE_STATUS_MIN_INTERVAL_SECONDS)
        finally:
            try:
                self.root.after(3000, self.periodic_refresh)
            except Exception:
                return

    def poll_navigation(self) -> None:
        try:
            self.consume_navigation_request()
        finally:
            try:
                self.root.after(500, self.poll_navigation)
            except TclError:
                return

    def consume_navigation_request(self) -> None:
        try:
            request = json.loads(LAUNCHER_NAVIGATION_PATH.read_text(encoding="utf-8"))
            LAUNCHER_NAVIGATION_PATH.unlink(missing_ok=True)
        except (OSError, ValueError):
            return
        tab = str(request.get("tab") or "") if isinstance(request, dict) else ""
        if tab not in {"advanced", "library", "geometry", "research", "geometry_mcp", "llm", "runtime", "programs"}:
            return
        self.show_tab(tab)
        try:
            self.root.deiconify()
            self.root.lift()
        except TclError:
            pass

    def scan_incoming_data(self) -> None:
        """Offer stable ZIPs copied into the shared data folder for import."""
        if self.worker and self.worker.is_alive():
            return
        candidates = [*RUNTIME_DATA_ROOT.glob("*.zip"), *(RUNTIME_DATA_ROOT / "incoming").glob("*.zip")]
        for path in candidates:
            try:
                if path.is_symlink() or not path.is_file():
                    continue
                stat = path.stat()
            except OSError:
                continue
            current = (stat.st_size, stat.st_mtime_ns)
            previous = self.incoming_snapshots.get(path)
            self.incoming_snapshots[path] = current
            if previous != current:
                continue  # Wait for a second scan so copying can finish.
            signature = (str(path), *current)
            if signature in self.incoming_reviewed:
                continue
            self.incoming_reviewed.add(signature)
            if messagebox.askyesno("New data ZIP detected",
                                   f"{path.name}\n\nCheck this ZIP and add it to your data library?"):
                self.import_data_zip_path(path, confirmed=True)
            break

    def invalidate_size_cache(self) -> None:
        self.size_cache_time = 0.0

    def kick_size_refresh(self) -> None:
        if self.size_refresh_in_flight:
            return
        if (time.time() - self.size_cache_time) < SIZE_CACHE_TTL_SECONDS:
            return
        self.size_refresh_in_flight = True

        def target() -> None:
            try:
                sizes = {
                    "data": _dir_size_bytes(RUNTIME_DATA_ROOT),
                    "packs": _dir_size_bytes(RUNTIME_PACKS_ROOT),
                    "cache": _dir_size_bytes(RUNTIME_CACHE_DIR),
                }
                self.root.after(0, lambda s=sizes: self.apply_size_cache(s))
            except Exception:
                self.size_refresh_in_flight = False

        threading.Thread(target=target, daemon=True).start()

    def apply_size_cache(self, sizes: dict[str, int]) -> None:
        self.size_cache = sizes
        self.size_cache_time = time.time()
        self.size_refresh_in_flight = False
        self.refresh_installed_label()

    def set_online_state(self, online: bool, detail: str = "") -> None:
        if online:
            self.online_var.set("Online")
            self.online_dot_label.configure(fg="#74d39a")
        else:
            suffix = f" ({detail})" if detail else ""
            self.online_var.set(f"Offline{suffix}")
            self.online_dot_label.configure(fg="#d36c6c")

    def kick_remote_status_refresh(self, min_interval_seconds: float = 0.0) -> None:
        if local_mapping_source_root() is not None:
            self.apply_remote_status(True, "", "")
            return
        if local_source_mode():
            self.apply_remote_status(False, "", "Local source unavailable")
            return
        if local_mapping_packages_root() is not None:
            if self.remote_status_in_flight:
                return
            self.remote_status_in_flight = True

            def local_target() -> None:
                try:
                    fingerprint = local_mapping_source_fingerprint()
                    self.root.after(0, lambda value=fingerprint: self.apply_remote_status(True, value, ""))
                except Exception as exc:
                    self.root.after(0, lambda msg=str(exc): self.apply_remote_status(False, "", msg))
                finally:
                    self.remote_status_in_flight = False

            self.remote_status_thread = threading.Thread(target=local_target, daemon=True)
            self.remote_status_thread.start()
            return
        if self.remote_status_in_flight:
            return
        if min_interval_seconds and (time.time() - self.last_remote_status_kick) < min_interval_seconds:
            return
        self.last_remote_status_kick = time.time()

        def target() -> None:
            self.remote_status_in_flight = True
            try:
                current_version = fetch_engine_current_version()
                self.root.after(0, lambda v=current_version: self.apply_remote_status(True, v, ""))
            except Exception as exc:
                self.root.after(0, lambda msg=str(exc): self.apply_remote_status(False, "", msg))
            finally:
                self.remote_status_in_flight = False

        self.remote_status_thread = threading.Thread(target=target, daemon=True)
        self.remote_status_thread.start()

    def apply_remote_status(self, online: bool, current_version: str, error_text: str) -> None:
        def show_mapping_action(current_label: str = "") -> None:
            if current_label:
                self.get_mapping_button.pack_forget()
                self.mapping_current_label.configure(text=current_label)
                self.mapping_current_label.pack(side=LEFT, before=self.open_mapping_program_button, padx=(0, 8))
            else:
                self.mapping_current_label.pack_forget()
                self.get_mapping_button.pack(side=LEFT, before=self.open_mapping_program_button, padx=(0, 8))

        source_root = local_mapping_source_root()
        if source_root is not None:
            show_mapping_action("Local source")
            self.mapping_badge_var.set("MAP  /  LOCAL SOURCE")
            self.mapping_download_available = False
            self.mapping_install_needed = False
            self.install_button.configure(state=DISABLED)
            self.get_mapping_button.configure(state=DISABLED)
            self.launch_button.configure(state=NORMAL)
            self.open_mapping_program_button.configure(state=NORMAL)
            self.install_button_var.set("Local source")
            text = f"Explore Map uses local source: {source_root}"
            self.update_var.set(text)
            self._set_selectable_text(self.update_text, text)
            return
        if local_source_mode():
            show_mapping_action("Source missing")
            self.mapping_badge_var.set("MAP  /  SOURCE MISSING")
            self.mapping_download_available = False
            self.mapping_install_needed = False
            self.install_button.configure(state=DISABLED)
            self.get_mapping_button.configure(state=DISABLED)
            self.launch_button.configure(state=DISABLED)
            self.open_mapping_program_button.configure(state=DISABLED)
            text = "Local custom source is unavailable. Choose Mapping Source and Python in Settings."
            self.update_var.set(text)
            self._set_selectable_text(self.update_text, text)
            return
        if local_mapping_packages_root() is not None:
            state = load_state()
            installed = has_usable_installed_engine(state)
            installed_fingerprint = local_mapping_installed_fingerprint() if installed else ""
            current = bool(installed and online and current_version and installed_fingerprint == current_version)
            show_mapping_action(f"local-dev ({installed_fingerprint[:8]})" if current else "")
            self.mapping_badge_var.set("MAP  /  INSTALLED" if installed else "MAP  /  AVAILABLE")
            self.mapping_download_available = False
            self.mapping_install_needed = not current
            self.install_button.configure(state=NORMAL if online and not current else DISABLED)
            self.get_mapping_button.configure(state=NORMAL if online and not current else DISABLED)
            self.launch_button.configure(state=NORMAL if installed else DISABLED)
            self.open_mapping_program_button.configure(state=NORMAL if installed else DISABLED)
            self.install_button_var.set("Update" if installed else "Install")
            text = (f"Installed local-dev ({installed_fingerprint[:8]}); source is current" if current else
                    f"Installed local-dev ({installed_fingerprint[:8] or 'older build'}); source has changes" if installed and online else
                    f"Installed local-dev; source check unavailable: {error_text}" if installed else
                    "Build and install Explore Map from local source")
            self.update_var.set(text)
            self._set_selectable_text(self.update_text, text)
            return
        state = load_state()
        installed = has_usable_installed_engine(state)
        show_mapping_action(f"v{state.version}" if installed and not (online and current_version and current_version != state.version) else "")
        self.mapping_badge_var.set("MAP  /  INSTALLED" if has_usable_installed_engine(state) else
                                   "MAP  /  AVAILABLE" if current_version else "MAP  /  UNAVAILABLE")
        self.mapping_download_available = bool(online and current_version)
        self.mapping_install_needed = bool(self.mapping_download_available and
                                           (state.version != current_version or not has_usable_installed_engine(state)))
        self.install_button.configure(state=NORMAL if self.mapping_install_needed else DISABLED)
        self.get_mapping_button.configure(state=NORMAL if self.mapping_install_needed else DISABLED)
        self.launch_button.configure(state=NORMAL if self.mapping_download_available or has_usable_installed_engine(state) else DISABLED)
        self.open_mapping_program_button.configure(state=NORMAL if has_usable_installed_engine(state) else DISABLED)
        if online:
            self.latest_engine_version = current_version
            self.set_online_state(True)
            if current_version and state.version == current_version and has_usable_installed_engine(state):
                text = f"Explore Map is up to date ({current_version})"
                self.update_var.set(text)
                self._set_selectable_text(self.update_text, text)
                self.install_button_var.set("Installed")
            elif current_version:
                installed = has_usable_installed_engine(state)
                text = (f"Explore Map update available ({current_version})" if installed else
                        f"Explore Map is available ({current_version})")
                self.update_var.set(text)
                self._set_selectable_text(self.update_text, text)
                self.install_button_var.set("Update" if installed else "Install")
            else:
                text = "Explore Map version is unavailable"
                self.update_var.set(text)
                self._set_selectable_text(self.update_text, text)
                self.install_button_var.set("Install" if not has_usable_installed_engine(state) else "Installed")
            return

        detail = "manifest unreachable"
        lowered = error_text.lower()
        if "timed out" in lowered:
            detail = "timeout"
        elif "404" in lowered:
            detail = "not found"
        elif "403" in lowered:
            detail = "forbidden"
        # A missing Mapping release is a store response, not a lost connection.
        store_reachable = "404" in lowered or "403" in lowered
        self.set_online_state(store_reachable, "" if store_reachable else detail)
        text = "Explore Map download is not available yet; installed tools still work" if "404" in lowered else "Explore Map download is unavailable; installed tools still work"
        self.update_var.set(text)
        self._set_selectable_text(self.update_text, text)
        self.install_button_var.set("Install" if not has_usable_installed_engine(state) else "Installed")

    def refresh_installed_label(self) -> None:
        state = load_state()
        pack_state = load_pack_state()
        installed_pack_count = len(pack_state.get("installed_packs", []))
        installed_programs = load_program_state()["installed_programs"]
        installed_program_count = len(installed_programs)
        installed_products = {
            str(entry.get("product") or "") for entry in installed_programs
            if entry.get("install_dir") and Path(str(entry["install_dir"])).is_dir()
        }
        mapping_installed = has_usable_installed_engine(state)
        self.sync_navigation(mapping_installed, installed_products)
        self.refresh_network_settings()
        openai_ready = bool(os.environ.get("OPENAI_API_KEY", "").strip())
        anthropic_ready = bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
        runtime_running = is_runtime_reachable()
        source_root = local_mapping_source_root()
        mapping_version = ("local source" if source_root else
                           "local test build" if local_mapping_packages_root() and mapping_installed else
                           state.version if mapping_installed else "not installed")
        installed_text = (f"Data packs: {installed_pack_count}  /  Programs: {installed_program_count}  /  "
                          f"Explore Map: {mapping_version}")
        self.installed_var.set(installed_text)
        self._set_selectable_text(self.installed_text, installed_text)
        self.kick_size_refresh()
        runtime_text = "\n".join(
            [
                (f"Data packs: {installed_pack_count} installed" if installed_pack_count else
                 "Data packs: none yet. Browse Data Packs to add records."),
                f"Programs: {installed_program_count} installed",
                f"Explore Map: {'running' if runtime_running else ('installed' if mapping_installed else 'not installed')}",
                f"Data folder: {RUNTIME_DATA_ROOT}",
            ]
        )
        self.runtime_var.set(runtime_text)
        self._set_selectable_text(self.runtime_overview_text, runtime_text)
        self._set_selectable_text(self.runtime_detail_text, runtime_text)
        self.refresh_saved_corpora_list()
        llm_text = "\n".join(
            [
                f"OpenAI key: {'configured' if openai_ready else 'not configured'}",
                f"Anthropic key: {'configured' if anthropic_ready else 'not configured'}",
                "Provider keys are read from the launcher environment at startup.",
            ]
        )
        self.llm_var.set(llm_text)
        self._set_selectable_text(self.llm_status_text, llm_text)
        advanced_text = "\n".join([
            "DaedalMap connection: disabled" if local_source_mode() else f"Downloads: {public_base_url()}",
            f"Mapping source: {local_mapping_source_root()}" if local_source_mode() else
            f"Mapping releases: {engine_current_url()}",
            f"Data folder: {RUNTIME_DATA_ROOT}",
            f"Launcher folder: {APP_ROOT}",
        ])
        self.advanced_var.set(advanced_text)
        self._set_selectable_text(self.advanced_status_text, advanced_text)
    def set_store_status(self, mode: str, detail: str = "") -> None:
        pack_state = load_pack_state()
        installed_pack_count = len(pack_state.get("installed_packs", []))
        if mode == "local":
            text = f"Local custom mode: DaedalMap Store is disconnected. {installed_pack_count} data packs installed."
            self.store_var.set(text)
            self._set_selectable_text(self.store_status_text, text)
            return
        if mode == "online":
            text = f"{len(self.available_packs)} data packs in Downloads  /  {installed_pack_count} installed locally"
            self.store_var.set(text)
            self._set_selectable_text(self.store_status_text, text)
            return
        if mode == "saved":
            text = f"Showing saved Downloads list  /  {installed_pack_count} installed locally"
            self.store_var.set(text)
            self._set_selectable_text(self.store_status_text, text)
            return
        if mode == "disconnected":
            suffix = f" {detail}" if detail else ""
            text = f"Downloads unavailable{suffix}. {installed_pack_count} data packs installed."
            self.store_var.set(text)
            self._set_selectable_text(self.store_status_text, text)
            return
        if mode == "offline":
            suffix = f" {detail}" if detail else ""
            text = f"Downloads offline{suffix}. {installed_pack_count} data packs installed."
            self.store_var.set(text)
            self._set_selectable_text(self.store_status_text, text)
            return
        text = "Data packs: checking..."
        self.store_var.set(text)
        self._set_selectable_text(self.store_status_text, text)

    def _pack_display_text(self, pack: AvailablePack) -> str:
        status = get_local_pack_status(pack.pack_id)
        if status.active:
            badge = f"ACTIVE v{status.installed_version or pack.version}"
        elif status.installed_version:
            badge = f"INSTALLED v{status.installed_version}"
        else:
            badge = "NOT INSTALLED"
        name = pack.source_name or pack.pack_id
        return f"{name} [{badge}]"

    def _pack_title_lookup(self) -> dict[str, str]:
        return {
            pack.pack_id: (pack.source_name or pack.pack_id)
            for pack in self.available_packs
        }

    def refresh_research_views(self) -> None:
        if hasattr(self, "research_pack_list"):
            self.refresh_research_pack_list()
        if hasattr(self, "saved_corpus_list"):
            self.refresh_saved_corpora_list()
        self.update_research_detail()

    def refresh_research_pack_list(self) -> None:
        if not hasattr(self, "research_pack_list"):
            return
        current_selection = {
            str(self.research_pack_list.get(index)).split(" (", 1)[0].strip()
            for index in self.research_pack_list.curselection()
        } if hasattr(self, "research_pack_list") else set()
        pack_state = load_pack_state()
        installed = [entry for entry in pack_state.get("installed_packs", []) if isinstance(entry, dict)]
        self.research_pack_list.delete(0, END)
        for idx, entry in enumerate(installed):
            pack_id = str(entry.get("pack_id") or "").strip()
            if not pack_id:
                continue
            title = self._pack_title_lookup().get(pack_id, pack_id)
            version = str(entry.get("version") or "").strip() or "unknown"
            label = f"{title} ({pack_id}) | v{version}"
            self.research_pack_list.insert(END, label)
            if pack_id in current_selection:
                self.research_pack_list.selection_set(idx)

    def refresh_saved_corpora_list(self) -> None:
        self.local_corpora = load_local_corpora()
        if not hasattr(self, "saved_corpus_list"):
            return
        current_id = ""
        selection = self.saved_corpus_list.curselection()
        if selection and self.local_corpora:
            selected_index = int(selection[0])
            if 0 <= selected_index < len(self.local_corpora):
                current_id = self.local_corpora[selected_index].corpus_id
        self.saved_corpus_list.delete(0, END)
        restored_index = None
        for idx, corpus in enumerate(self.local_corpora):
            self.saved_corpus_list.insert(END, f"{corpus.name} ({len(corpus.pack_ids)} pack{'s' if len(corpus.pack_ids) != 1 else ''})")
            if corpus.corpus_id == current_id:
                restored_index = idx
        if restored_index is not None:
            self.saved_corpus_list.selection_set(restored_index)

    def get_selected_research_pack_ids(self) -> list[str]:
        if not hasattr(self, "research_pack_list"):
            return []
        pack_state = load_pack_state()
        installed = [entry for entry in pack_state.get("installed_packs", []) if isinstance(entry, dict)]
        selected_ids: list[str] = []
        for index in self.research_pack_list.curselection():
            idx = int(index)
            if 0 <= idx < len(installed):
                pack_id = str(installed[idx].get("pack_id") or "").strip()
                if pack_id:
                    selected_ids.append(pack_id)
        return selected_ids

    def get_selected_local_corpus(self) -> LocalCorpus | None:
        if not hasattr(self, "saved_corpus_list"):
            return None
        selection = self.saved_corpus_list.curselection()
        if not selection:
            return None
        index = int(selection[0])
        if index < 0 or index >= len(self.local_corpora):
            return None
        return self.local_corpora[index]

    def set_pack_filter(self, value: str) -> None:
        self.pack_filter = value
        for name, button in self.pack_filter_buttons.items():
            button.configure(**tab_button_kwargs(name == value))
        self.update_pack_list()
        self.root.after_idle(lambda: self.pack_canvas.yview_moveto(0))

    def set_geometry_filter(self, value: str) -> None:
        self.geometry_filter = value
        for name, button in self.geometry_filter_buttons.items():
            button.configure(**tab_button_kwargs(name == value))
        self.update_geometry_list()
        self.root.after_idle(lambda: self.geometry_canvas.yview_moveto(0))

    def set_geometry_section(self, value: str) -> None:
        self.geometry_section = value
        for name, button in self.geometry_section_buttons.items():
            button.configure(**tab_button_kwargs(name == value))
        self.update_geometry_list()
        self.root.after_idle(lambda: self.geometry_canvas.yview_moveto(0))

    def update_geometry_list(self) -> None:
        for child in self.geometry_cards.winfo_children():
            child.destroy()
        self.geometry_action_buttons.clear()
        installed = installed_geometry(RUNTIME_DATA_ROOT)
        by_id = {item["packageId"]: item for item in self.available_geometry}
        releases = list(self.available_geometry)
        for package_id, status in installed.items():
            if package_id not in by_id:
                releases.append({"packageId": package_id, "title": package_id,
                                 "version": status["version"], "tier": status["tier"],
                                 "countryCode": status["countryCode"], "geometrySection": "countries",
                                 "description": "Installed Geometry package.", "size": 0})
        groups = group_geometry_releases(releases, self.available_geometry_families)
        if self.geometry_filter == "installed":
            groups = [group for group in groups if any(
                item["packageId"] in installed for item in group["tiers"].values())]
        if self.geometry_section != "all":
            groups = [group for group in groups if group["section"] == self.geometry_section]
        terms = self.geometry_search_var.get().strip().lower().split()
        groups = [group for group in groups if all(term in " ".join([
            group["title"], group["unit"], *group["families"], *(" ".join(str(item.get(key) or "") for key in
            ("packageId", "title", "description", "tier")) for item in group["tiers"].values())
        ]).lower() for term in terms)]
        for index, group in enumerate(groups):
            editions = group["tiers"]
            installed_count = sum(item["packageId"] in installed for item in editions.values())
            card = self._catalog_card(self.geometry_cards, height=245)
            card.grid(row=index // 2, column=index % 2, sticky="nsew",
                      padx=(0, 8) if index % 2 == 0 else (8, 0), pady=8)
            state = f"{installed_count} EDITION{'S' if installed_count != 1 else ''} INSTALLED" if installed_count else "AVAILABLE TO INSTALL"
            Label(card, text=f"{group['section'].upper()}  /  {state}", bg=THEME["colors"]["card_bg"],
                  fg=THEME["colors"]["accent"] if installed_count else "#ffbe5c",
                  font=THEME["fonts"]["mono"]).pack(anchor="w", padx=14, pady=(12, 3))
            heading = Frame(card, bg=THEME["colors"]["card_bg"])
            heading.pack(fill="x", padx=14)
            Label(heading, text=group["title"], bg=THEME["colors"]["card_bg"],
                  fg=THEME["colors"]["text"], font=THEME["fonts"]["card"],
                  wraplength=295, justify=LEFT, anchor="nw", height=1).pack(side=LEFT)
            Label(heading, text=group["unit"] if group["section"] == "countries" else group["section"].upper(),
                  bg=THEME["colors"]["card_bg"], fg=THEME["colors"]["muted_text"],
                  font=THEME["fonts"]["mono"]).pack(side=RIGHT)
            families = group["families"]
            family_text = ("Available families  " + " · ".join(families[:5])
                           + (f" · +{len(families) - 5} more" if len(families) > 5 else "")) if families else (
                           "Reference bridge" if group["section"] == "bridges" else "Available geometry editions")
            family_slot = Frame(card, bg=THEME["colors"]["card_bg"], height=76)
            family_slot.pack(fill="x", padx=14)
            family_slot.pack_propagate(False)
            Label(family_slot, text=family_text,
                  bg=THEME["colors"]["card_bg"], fg=THEME["colors"]["muted_text"],
                  font=THEME["fonts"]["body"], wraplength=370, justify=LEFT,
                  anchor="nw").pack(fill="x", pady=(7, 0))
            Button(family_slot, text="More info", command=lambda url=geometry_info_url(group): webbrowser.open(url),
                   bg=THEME["colors"]["card_bg"], fg=THEME["colors"]["accent"],
                   activebackground=THEME["colors"]["card_bg"],
                   activeforeground=THEME["colors"]["accent"],
                   relief="flat", bd=0, highlightthickness=0, cursor="hand2",
                   font=THEME["fonts"]["body"], padx=0, pady=0).pack(side="bottom", anchor="w", pady=(0, 3))
            actions = Frame(card, bg=THEME["colors"]["card_bg"])
            actions.pack(fill="x", padx=14)
            for tier in GEOMETRY_TIERS:
                item = editions.get(tier)
                column = Frame(actions, bg=THEME["colors"]["card_bg"], width=122)
                column.pack(side=LEFT, fill="x", expand=True, padx=(0, 5))
                caption = "Detail" if tier == "full" else tier.title()
                if item is None:
                    if tier == "lite" or group["section"] != "bridges":
                        Label(column, text=f"{caption} unavailable", bg=THEME["colors"]["card_bg"],
                              fg=THEME["colors"]["muted_text"], font=THEME["fonts"]["mono"]).pack(anchor="w")
                    continue
                current = installed.get(item["packageId"])
                version = item["version"]
                update_available = bool(current and current["version"] != version and item["packageId"] in by_id)
                button = Button(column, text=f"Update {caption}" if update_available else caption,
                                command=lambda selected=item: self.review_selected_geometry_clicked(selected),
                                **button_kwargs("accent" if item["packageId"] in by_id else "primary"))
                button.pack(fill="x")
                if item["packageId"] in by_id:
                    self.geometry_action_buttons.append(button)
                    if self.worker and self.worker.is_alive():
                        button.configure(state=DISABLED)
                else:
                    button.configure(state=DISABLED)
                Label(column, text=f"v{version}", bg=THEME["colors"]["card_bg"],
                      fg=THEME["colors"]["muted_text"], font=THEME["fonts"]["mono"]).pack(
                          anchor="w", pady=(5, 0))
                Label(column, text=_format_bytes(item.get("size")) if item.get("size") else "",
                      bg=THEME["colors"]["card_bg"], fg=THEME["colors"]["muted_text"],
                      font=THEME["fonts"]["mono"]).pack(anchor="w")
                if current:
                    status = "Installed" if current["version"] == version else f"Installed v{current['version']}"
                    Label(column, text=status, bg=THEME["colors"]["card_bg"],
                          fg=THEME["colors"]["accent"], font=THEME["fonts"]["mono"],
                          wraplength=118).pack(anchor="w")
            self._bind_geometry_scroll(card)
        if not groups:
            message = "No installed geometry yet." if self.geometry_filter == "installed" else "No Geometry downloads listed yet."
            Label(self.geometry_cards, text=message, **label_kwargs("muted")).grid(row=0, column=0, sticky="w", pady=12)
        self._reflow_card_grid(self.geometry_cards, self.geometry_canvas.winfo_width())
        label = f"{len(group_geometry_releases(self.available_geometry, self.available_geometry_families))} Geometry areas  /  {len(self.available_geometry)} editions  /  {len(installed)} installed locally"
        self._set_selectable_text(self.geometry_status_text, label)

    def open_pack_page(self, pack: AvailablePack) -> None:
        webbrowser.open(f"https://www.daedalmap.com/packs/{urllib.parse.quote(pack.pack_id, safe='')}")

    def update_pack_list(self) -> None:
        for child in self.pack_cards.winfo_children():
            child.destroy()
        self.pack_action_buttons.clear()
        self.pack_action_vars.clear()
        terms = self.pack_search_var.get().strip().lower().split()
        filtered = []
        for pack, status, in_cloud in packs_for_view(self.available_packs, load_pack_state(), self.pack_filter):
            haystack = " ".join((pack.source_name, pack.description, pack.pack_id,
                                 pack.topic_category, pack.package_tier)).lower()
            if not all(term in haystack for term in terms):
                continue
            filtered.append((pack, status, in_cloud))
        for index, (pack, status, in_cloud) in enumerate(filtered):
            card = self._catalog_card(self.pack_cards)
            card.grid(row=index // 2, column=index % 2, sticky="nsew",
                      padx=(0, 8) if index % 2 == 0 else (8, 0), pady=8)
            state = "READY LOCALLY" if status.active else "INSTALLED LOCALLY" if status.installed_version else "AVAILABLE TO INSTALL"
            topic = pack.topic_category.replace("_", " ").title() or "Data pack"
            Label(card, text=topic.upper() + "  /  " + state, bg=THEME["colors"]["card_bg"],
                  fg=THEME["colors"]["accent"] if status.installed_version else "#ffbe5c",
                  font=THEME["fonts"]["mono"]).pack(anchor="w", padx=14, pady=(12, 3))
            Label(card, text=pack.source_name or pack.pack_id, bg=THEME["colors"]["card_bg"],
                  fg=THEME["colors"]["text"], font=THEME["fonts"]["card"],
                  wraplength=370, justify=LEFT, anchor="nw", height=2).pack(fill="x", padx=14)
            Label(card, text=pack.description or "Maintained records for local analysis.",
                  bg=THEME["colors"]["card_bg"], fg=THEME["colors"]["muted_text"],
                  font=THEME["fonts"]["body"], wraplength=370, justify=LEFT,
                  anchor="nw", height=2).pack(fill="x", padx=14, pady=(5, 5))
            size = f"  /  {_format_bytes(pack.size_bytes)}" if pack.size_bytes else ""
            Label(card, text=f"{pack.package_tier.title()}  /  Release {pack.version}{size}",
                  bg=THEME["colors"]["card_bg"], fg=THEME["colors"]["muted_text"],
                  font=THEME["fonts"]["mono"]).pack(anchor="w", padx=14)
            actions = Frame(card, bg=THEME["colors"]["card_bg"])
            actions.pack(fill="x", padx=14, pady=(9, 12))
            if in_cloud:
                action_state = InstallActionState.compare(status.installed_version, pack.version)
                if action_state.version_label:
                    Label(actions, text=action_state.version_label, bg=THEME["colors"]["card_bg"],
                          fg=THEME["colors"]["accent"], font=THEME["fonts"]["mono"]).pack(side=LEFT, padx=(0, 8))
                else:
                    action_var = StringVar(value=action_state.action)
                    action = Button(actions, textvariable=action_var,
                                    command=lambda value=pack: self.install_selected_pack_clicked(value),
                                    **button_kwargs("accent"))
                    action.pack(side=LEFT, padx=(0, 8))
                    self.pack_action_vars[(pack.package_id, pack.version)] = action_var
                    self.pack_action_buttons.append(action)
                    if self.worker and self.worker.is_alive():
                        action.configure(state=DISABLED)
                Button(actions, text="Pack page", command=lambda value=pack: self.open_pack_page(value),
                       **button_kwargs("primary")).pack(side=LEFT)
            elif status.installed_version:
                Label(actions, text=f"v{status.installed_version}", bg=THEME["colors"]["card_bg"],
                      fg=THEME["colors"]["accent"], font=THEME["fonts"]["mono"]).pack(side=LEFT)
            self._bind_pack_scroll(card)
        if not filtered:
            message = ("No installed data packs yet." if self.pack_filter == "installed" else
                       "No packs match this view." if self.available_packs else "No cloud downloads listed yet.")
            Label(self.pack_cards, text=message,
                  **label_kwargs("muted")).grid(row=0, column=0, sticky="w", pady=12)
        self._reflow_card_grid(self.pack_cards, self.pack_canvas.winfo_width())
        self.store_loaded_once = True
        self.set_store_status(self.catalog_source)

    def update_research_detail(self) -> None:
        if not hasattr(self, "saved_corpus_list"):
            self.local_corpora = load_local_corpora()
            text = "\n".join(
                [
                    f"Saved research sets: {len(self.local_corpora)}",
                    "Select data packs to create a research set.",
                    "Install Research MCP from Programs to query local data.",
                ]
            )
            self._set_selectable_text(self.research_detail_text, text)
            return
        selected_ids = self.get_selected_research_pack_ids()
        selected_corpus = self.get_selected_local_corpus()
        title_lookup = self._pack_title_lookup()
        if selected_corpus:
            pack_lines = [
                f"- {title_lookup.get(pack_id, pack_id)} ({pack_id})"
                for pack_id in selected_corpus.pack_ids
            ] or ["- No packs saved."]
            text = "\n".join(
                [
                    f"Corpus: {selected_corpus.name}",
                    f"Corpus ID: {selected_corpus.corpus_id}",
                    f"Updated: {selected_corpus.updated_at or 'unknown'}",
                    f"Pack count: {len(selected_corpus.pack_ids)}",
                    "Packs:",
                    *pack_lines,
                ]
            )
            self._set_selectable_text(self.research_detail_text, text)
            return
        if selected_ids:
            pack_lines = [f"- {title_lookup.get(pack_id, pack_id)} ({pack_id})" for pack_id in selected_ids]
            text = "\n".join(
                [
                    "Draft local corpus:",
                    f"Selected pack count: {len(selected_ids)}",
                    "Packs:",
                    *pack_lines,
                ]
            )
            self._set_selectable_text(self.research_detail_text, text)
            return
        self._set_selectable_text(
            self.research_detail_text,
            "Select installed packs to draft a local corpus, or choose a saved corpus to inspect it.",
        )

    def save_local_corpus_clicked(self) -> None:
        name = self.research_name_entry.get().strip()
        selected_pack_ids = self.get_selected_research_pack_ids()
        if not name:
            messagebox.showinfo("Corpus Name Required", "Enter a corpus name before saving.")
            return
        if not selected_pack_ids:
            messagebox.showinfo("No Packs Selected", "Select one or more installed packs for this corpus.")
            return
        corpus_id = name.lower().replace(" ", "_")
        corpora = load_local_corpora()
        updated = False
        for corpus in corpora:
            if corpus.corpus_id == corpus_id or corpus.name.lower() == name.lower():
                corpus.corpus_id = corpus_id
                corpus.name = name
                corpus.pack_ids = selected_pack_ids
                corpus.updated_at = _now_iso()
                updated = True
                break
        if not updated:
            corpora.append(LocalCorpus(corpus_id=corpus_id, name=name, pack_ids=selected_pack_ids, updated_at=_now_iso()))
        save_local_corpora(corpora)
        self.local_corpora = corpora
        self.research_name_entry.delete(0, END)
        self.refresh_saved_corpora_list()
        self.update_research_detail()
        self.set_status(f'Saved local corpus "{name}"')
        self.append_log(f'Saved local corpus "{name}" with {len(selected_pack_ids)} pack(s)')

    def delete_selected_corpus_clicked(self) -> None:
        selected = self.get_selected_local_corpus()
        if not selected:
            messagebox.showinfo("No Corpus Selected", "Choose a saved local corpus first.")
            return
        corpora = [corpus for corpus in load_local_corpora() if corpus.corpus_id != selected.corpus_id]
        save_local_corpora(corpora)
        self.local_corpora = corpora
        self.refresh_saved_corpora_list()
        self.update_research_detail()
        self.set_status(f'Deleted local corpus "{selected.name}"')
        self.append_log(f'Deleted local corpus "{selected.name}"')

    def open_local_app_clicked(self) -> None:
        self.launch_clicked()

    def choose_storage_location_clicked(self) -> None:
        current_root = str(RUNTIME_ROOT)
        selected = filedialog.askdirectory(
            title="Choose DaedalMap local storage folder",
            initialdir=current_root if Path(current_root).exists() else str(APP_ROOT),
            mustexist=False,
        )
        if not selected:
            return
        chosen_root = Path(selected)
        apply_runtime_storage_root(chosen_root, persist=True)
        initialize_runtime_root(self.append_log)
        self.invalidate_size_cache()
        self.refresh_installed_label()
        self.refresh_map_tile_status()
        self.append_log(f"Updated local storage root to {chosen_root}")
        if is_runtime_reachable():
            message = (
                "Data location saved. Restart Mapping to use it. "
                "Existing data stays in the old folder; new downloads use the new folder."
            )
        else:
            message = (
                "Data location saved. Mapping will use it next time it opens. "
                "Existing data stays in the old folder; new downloads use the new folder."
            )
        self.set_status(message)
        messagebox.showinfo("Storage Location Saved", message)

    def refresh_map_tile_status(self) -> None:
        preferences = load_map_tile_preferences()
        labels = {"offline_basic": "Built-in offline world", "offline_detail": "Detailed offline world"}
        detail = "detailed download installed" if map_detail_available() else "detailed download available (~23 MB)"
        self.map_tiles_status_var.set(f"Current: {labels[preferences['mode']]}  /  {detail}")
        self.use_detail_map_button.configure(state=NORMAL if map_detail_available() else DISABLED)

    def use_basic_map_clicked(self) -> None:
        save_map_tile_preferences("offline_basic")
        self.refresh_map_tile_status()
        self.set_status("Built-in offline map selected. Reload Explore Map to see it.")

    def use_detail_map_clicked(self) -> None:
        if not map_detail_available():
            return
        save_map_tile_preferences("offline_detail")
        self.refresh_map_tile_status()
        self.set_status("Detailed offline map selected. Reload Explore Map to see it.")

    def download_map_detail_clicked(self) -> None:
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Launcher Busy", "The launcher is already doing work.")
            return
        if not messagebox.askyesno(
            "Download detailed world map?",
            "Download the detailed offline world map (about 23 MB)?\n\n"
            f"The two files will be saved in:\n{RUNTIME_ROOT / 'tiles'}\n\n"
            "You can cancel while the download is running.",
        ):
            return
        self.map_detail_cancel_event.clear()

        def work():
            size = download_map_detail(self.append_log, self.map_detail_cancel_event)
            save_map_tile_preferences("offline_detail")
            self.append_log(f"Detailed offline world installed ({_format_bytes(size)}). Reload Explore Map.")
            self.root.after(0, self.refresh_map_tile_status)

        self.run_worker("Downloading detailed world map", work, map_tiles_download=True)

    def cancel_map_detail_clicked(self) -> None:
        self.map_detail_cancel_event.set()
        self.cancel_map_detail_button.configure(text="Canceling...", state=DISABLED)
        self.set_status("Canceling detailed world map download...")

    def cancel_pack_download_clicked(self) -> None:
        self.pack_download_cancel_event.set()
        for name in ("cancel_data_download_button", "cancel_geometry_download_button"):
            button = getattr(self, name, None)
            if button is not None:
                button.configure(text="Canceling...", state=DISABLED)
        self.set_status("Canceling pack download...")

    def run_worker(self, label: str, fn, *, mapping_install: bool = False, mapping_launch: bool = False,
                   map_tiles_download: bool = False, pack_download: str = "") -> None:
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Launcher Busy", "The launcher is already doing work.")
            return

        def target():
            try:
                self.root.after(0, lambda: self.set_busy(True, mapping_install=mapping_install,
                                                         mapping_launch=mapping_launch,
                                                         map_tiles_download=map_tiles_download,
                                                         pack_download=pack_download))
                self.root.after(0, lambda: self.set_status(label))
                fn()
                self.root.after(0, self.refresh_installed_label)
                self.root.after(0, self.kick_remote_status_refresh)
                self.root.after(0, lambda: self.set_status("Ready"))
            except MapDetailDownloadCancelled:
                self.root.after(0, lambda: self.append_log("Detailed world map download canceled"))
                self.root.after(0, lambda: self.set_status("Detailed world map download canceled"))
            except PackDownloadCancelled:
                self.root.after(0, lambda: self.append_log("Pack download canceled; retry to resume from cached bytes"))
                self.root.after(0, lambda: self.set_status("Pack download canceled; retry to resume"))
            except Exception as exc:
                error_text = str(exc)
                self.root.after(0, lambda msg=error_text: self.append_log(f"ERROR: {msg}"))
                self.root.after(0, lambda: self.set_status("Error"))
                self.root.after(0, lambda msg=error_text: messagebox.showerror("Launcher Error", msg))
            finally:
                self.root.after(0, lambda: self.set_busy(False))

        self.worker = threading.Thread(target=target, daemon=True)
        self.worker.start()

    def install_clicked(self) -> None:
        local_build = local_mapping_packages_root() is not None
        installed = has_usable_installed_engine(load_state())
        verb = "Update" if installed else "Install"
        detail = ("The launcher will build Mapping from local source and set it up."
                  if local_build else "The launcher will verify the published release and set it up.")
        if not messagebox.askyesno(
            f"{verb} program",
            f"{verb} Explore Map?\n\n{detail} Your data stays in the shared library.",
        ):
            return

        def work():
            if local_build:
                build_and_install_local_mapping(
                    self.append_log,
                    before_replace=lambda: self.stop_runtime()
                    if (self.process is not None and self.process.poll() is None) or is_runtime_reachable() else None,
                )
                self.append_log("Explore Map is installed. Use Open Map to launch this build.")
            else:
                state = install_engine(self.append_log)
                if is_runtime_reachable():
                    self.append_log(f"Explore Map {state.version} is installed; stop and reopen the map to use it")
            self.invalidate_size_cache()

        self.run_worker("Building local Mapping test" if local_build
                        else "Installing engine", work, mapping_install=True)

    def ensure_runtime_ready(self, open_browser_on_ready: bool = False) -> EngineState:
        source_root = local_mapping_source_root()
        if local_source_mode() and source_root is None:
            raise LauncherError("Local custom source is unavailable; choose a valid Mapping source in Settings")
        if source_root is not None:
            state = EngineState(version="local-source", install_dir=str(source_root))
            if is_runtime_reachable():
                if local_source_mode() and (self.process is None or self.process.poll() is not None):
                    raise LauncherError("Port 7000 is already used outside this local-source launcher. Stop that server before opening the map.")
                self.append_log("Local Mapping runtime already reachable on localhost")
                return state
            self.append_log(f"Starting Mapping directly from source: {source_root}")
            self.start_runtime(state, open_browser_on_ready=open_browser_on_ready)
            return state
        if is_runtime_reachable():
            state = load_state()
            self.append_log("Runtime already reachable on localhost")
            return state

        state = load_state()
        if has_usable_installed_engine(state):
            self.append_log(f"Opening installed Explore Map {state.version}; updates use the Update button")
        else:
            if local_mapping_packages_root() is not None:
                raise LauncherError("Build and install a local Explore Map package from Programs first")
            self.append_log("No Explore Map install found; fetching current release")
            state = install_engine(self.append_log)

        if self.process and self.process.poll() is None:
            self.append_log("Runtime process already running")
            if not is_runtime_reachable():
                raise LauncherError("Runtime process exists but localhost is not responding yet.")
            return state

        self.start_runtime(state, open_browser_on_ready=open_browser_on_ready)
        return state

    def apply_storefront_catalog(self, catalog: dict, *, source: str) -> None:
        programs = _programs_from_catalog(catalog, self.append_log)
        packs = _packs_from_catalog(catalog, self.append_log)
        geometry = releases_from_storefront(catalog)
        self.available_programs = include_installed_programs(programs)
        self.available_packs = packs
        self.available_geometry = geometry
        self.available_geometry_families = [item for item in catalog.get("geometry_families") or [] if isinstance(item, dict)]
        self.programs_loaded_once = True
        self.store_loaded_once = True
        self.catalog_source = source
        self.update_program_list()
        self.update_pack_list()
        self.update_geometry_list()
        self.refresh_research_views()

    def refresh_store_clicked(self) -> None:
        if local_source_mode():
            self.set_store_status("local")
            return
        if self.catalog_refresh_in_flight:
            return
        self.catalog_refresh_in_flight = True

        def work() -> None:
            try:
                catalog = fetch_storefront_catalog(self.append_log)
                _programs_from_catalog(catalog, self.append_log)
                _packs_from_catalog(catalog, self.append_log)
                releases_from_storefront(catalog)
                self.root.after(0, lambda value=catalog: self.apply_storefront_catalog(value, source="online"))
            except Exception as exc:
                self.append_log(f"Downloads list unavailable: {exc}")
                self.root.after(0, lambda: self.set_store_status("disconnected" if not self.store_loaded_once else "saved"))
            finally:
                self.catalog_refresh_in_flight = False

        threading.Thread(target=work, daemon=True).start()

    def refresh_programs_clicked(self) -> None:
        self.refresh_store_clicked()

    def kick_launcher_update_check(self) -> None:
        """Check the hash-verified release pointer without delaying local startup."""
        if local_source_mode():
            return
        base_url = os.environ.get("DAEDALMAP_LAUNCHER_RELEASE_BASE_URL", "").strip() or LAUNCHER_RELEASE_BASE_URL

        def work() -> None:
            try:
                release = check_release(base_url)
                self.root.after(0, lambda value=release: self.apply_launcher_release(value))
            except Exception as exc:
                self.append_log(f"Launcher release check unavailable: {exc}")

        threading.Thread(target=work, daemon=True).start()

    def apply_launcher_release(self, release: dict) -> None:
        version = str(release.get("version") or "")
        if is_newer_version(version):
            self.launcher_available_release = release
            self.launcher_update_var.set(f"Launcher update available! v{version}")
            self.launcher_update_button.pack(side=LEFT, padx=(8, 0))
        else:
            self.launcher_available_release = None
            self.launcher_update_var.set(f"Launcher v{LAUNCHER_VERSION}")
            self.launcher_update_button.pack_forget()

    def download_launcher_update_clicked(self) -> None:
        release = self.launcher_available_release
        if not release:
            return
        if not messagebox.askyesno(
            "Download launcher update?",
            f"Download launcher v{release.get('version') or 'new'} to:\n{APP_ROOT / 'updates'}?\n\n"
            "The launcher will verify the ZIP before offering it for installation.",
        ):
            return

        def work() -> None:
            target = download_release(release, APP_ROOT / "updates")
            self.append_log(f"Verified launcher update: {target}")
            self.root.after(0, lambda: messagebox.showinfo(
                "Launcher Update Ready",
                f"The verified launcher ZIP is ready at:\n{target}\n\n"
                "Close this launcher, extract the ZIP over its current launcher folder, and run "
                "DaedalMapLauncher.exe again. Keep the existing launcher-data folder; it holds your programs and data."))

        self.run_worker("Downloading Launcher Update", work)


    def update_program_list(self) -> None:
        self.available_programs = include_installed_programs(self.available_programs)
        for child in self.program_cards.winfo_children():
            if child is not self.mapping_card:
                child.destroy()
        self.program_action_buttons.clear()
        self.program_action_vars.clear()
        installed_entries = load_program_state()["installed_programs"]
        mapping_installed = has_usable_installed_engine()
        cards = program_cards_for_view(self.available_programs, installed_entries)
        if not cards:
            Label(self.program_cards, text="Loading programs from Downloads...", **label_kwargs("muted")).grid(
                row=0, column=1, sticky="w", pady=12)
        for index, (item, installed_entry, update_available) in enumerate(cards):
            local_trial = bool(installed_entry and str(installed_entry.get("version") or "").startswith("local-dev-")
                               and os.environ.get("DAEDALMAP_LAUNCHER_DEV") == "1"
                               and item.product in {*MCP_PRODUCTS, NETWORK_PRODUCT, "daedalmap-historical-version-generator"})
            saved_catalog_mismatch = bool(update_available and self.catalog_source != "online")
            if saved_catalog_mismatch:
                item = next((candidate for candidate in self.available_programs
                             if candidate.product == item.product and candidate.version == installed_entry["version"]), item)
                update_available = False
            running = self.program_processes.get(item.product)
            status = ("running" if running is not None and running.poll() is None else
                      "local test" if local_trial else
                      "update available" if update_available else
                      "installed" if installed_entry else "available")
            card = self._catalog_card(self.program_cards)
            slot = index + 1
            card.grid(row=slot // 2, column=slot % 2, sticky="nsew", padx=(0, 8) if slot % 2 == 0 else (8, 0), pady=8)
            Label(card, text=program_kind_label(item.product) + "  /  " + status.upper(), bg=THEME["colors"]["card_bg"],
                  fg=THEME["colors"]["program_accent"], font=THEME["fonts"]["mono"]).pack(anchor="w", padx=14, pady=(12, 3))
            Label(card, text=item.title, bg=THEME["colors"]["card_bg"],
                  fg=THEME["colors"]["text"], font=THEME["fonts"]["card"],
                  wraplength=370, justify=LEFT, anchor="nw", height=2).pack(fill="x", padx=14)
            description = item.description or "Local DaedalMap tool."
            Label(card, text=description, bg=THEME["colors"]["card_bg"],
                  fg=THEME["colors"]["muted_text"], font=THEME["fonts"]["body"],
                  justify=LEFT, anchor="nw", wraplength=370, height=2).pack(fill="x", padx=14, pady=(5, 5))
            size = f"  /  {_format_bytes(item.size_bytes)}" if item.size_bytes else ""
            release_text = (f"Installed {installed_entry['version']}  /  Current {item.version}{size}"
                            if update_available else f"Release {item.version}{size}")
            Label(card, text=release_text, bg=THEME["colors"]["card_bg"],
                  fg=THEME["colors"]["muted_text"], font=THEME["fonts"]["mono"]).pack(anchor="w", padx=14)
            actions = Frame(card, bg=THEME["colors"]["card_bg"])
            actions.pack(fill="x", padx=14, pady=(9, 12))
            action_state = InstallActionState.compare(
                str(installed_entry.get("version") or "") if installed_entry else "", item.version)
            if local_trial:
                install_label = "Rebuild Local"
                install_command = lambda value=item: self.rebuild_local_program_clicked(value)
                install_enabled = True
            elif saved_catalog_mismatch:
                install_label = "Check Updates"
                install_command = self.refresh_store_clicked
                install_enabled = True
            elif update_available:
                install_label = "Update"
                install_command = lambda value=item: self.install_selected_program_clicked(value, updating=True)
                install_enabled = True
            elif installed_entry:
                install_label = action_state.version_label
                install_command = lambda: None
                install_enabled = False
            else:
                install_label = action_state.action
                install_command = lambda value=item: self.install_selected_program_clicked(value)
                install_enabled = bool(item.artifact_url and item.size_bytes > 0)
            if installed_entry and not update_available and not saved_catalog_mismatch and not local_trial:
                install_action = Label(actions, text=install_label, bg=THEME["colors"]["card_bg"],
                                       fg=THEME["colors"]["accent"], font=THEME["fonts"]["mono"])
                install_action.pack(side=LEFT, padx=(0, 8))
            else:
                install_var = StringVar(value=install_label)
                install_action = Button(actions, textvariable=install_var, command=install_command,
                                        **button_kwargs("accent"))
                install_action.pack(side=LEFT, padx=(0, 8))
                if install_enabled:
                    self.program_action_buttons.append(install_action)
                    if not saved_catalog_mismatch:
                        self.program_action_vars[(item.product, item.version)] = install_var
                else:
                    install_action.configure(state=DISABLED)
            is_running = running is not None and running.poll() is None
            use_action = Button(actions, text=program_use_label(item.product, is_running,
                                                               mapping_installed=mapping_installed),
                                command=(lambda value=item: self.stop_selected_program_clicked(value))
                                if is_running else (lambda value=item: self.run_selected_program_clicked(value)),
                                **button_kwargs("primary"))
            if installed_entry:
                use_action.pack(side=LEFT, padx=(0, 8))
                self.program_action_buttons.append(use_action)
            else:
                use_action.configure(state=DISABLED)
            if self.worker and self.worker.is_alive():
                for action in (install_action, use_action):
                    if isinstance(action, Button):
                        action.configure(state=DISABLED)
            page_path = program_page_path(item.product)
            if page_path:
                page_url = "https://www.daedalmap.com" + page_path
                Button(actions, text="About", command=lambda url=page_url: webbrowser.open(url),
                       **button_kwargs("primary")).pack(side=LEFT)
            elif item.product == "daedalmap-historical-version-generator":
                Button(actions, text="About", command=self.show_historical_generator_about,
                       **button_kwargs("primary")).pack(side=LEFT)
            else:
                Button(actions, text="About",
                       command=lambda value=item: messagebox.showinfo(value.title, value.description or "Local DaedalMap tool."),
                       **button_kwargs("primary")).pack(side=LEFT)
        source = "  /  saved Downloads list" if self.catalog_source == "saved" else ""
        updates = sum(update for _, _, update in cards) if self.catalog_source == "online" else 0
        mapping_update = bool(local_mapping_packages_root() is None and mapping_installed and
                              self.mapping_install_needed and self.mapping_download_available)
        summary = (f"{len(cards) + 1} programs  /  {len(installed_entries) + int(mapping_installed)} installed"
                   f"  /  {updates + int(mapping_update)} updates{source}")
        self._set_selectable_text(self.programs_status, summary)
        self._reflow_card_grid(self.program_cards, self.program_cards.winfo_width())
        self._bind_program_scroll(self.program_cards)

    def show_historical_generator_about(self) -> None:
        existing = getattr(self, "historical_about_dialog", None)
        if existing is not None and existing.winfo_exists():
            existing.lift()
            existing.focus_set()
            return

        dialog = Toplevel(self.root)
        self.historical_about_dialog = dialog
        dialog.title("Historical Version Generator")
        dialog.configure(**app_root_kwargs())
        dialog.resizable(False, False)
        dialog.transient(self.root)
        if self.brand_icon is not None:
            dialog.iconphoto(True, self.brand_icon)

        panel = Frame(dialog, **panel_kwargs())
        panel.pack(fill=BOTH, expand=True, padx=16, pady=16)
        Label(panel, text="ABOUT THIS TOOL", bg=THEME["colors"]["panel_bg"],
              fg=THEME["colors"]["program_accent"], font=THEME["fonts"]["mono"]).pack(
                  anchor="w", padx=18, pady=(18, 4))
        Label(panel, text="Historical Version Generator", bg=THEME["colors"]["panel_bg"],
              fg=THEME["colors"]["text"], font=THEME["fonts"]["card"]).pack(
                  anchor="w", padx=18)
        Label(panel,
              text=("For version-specific research projects and to match someone else's work precisely, "
                    "send a geometry data pack back in time to a previous version number."),
              bg=THEME["colors"]["panel_bg"], fg=THEME["colors"]["text"],
              font=THEME["fonts"]["body"], wraplength=420, justify=LEFT).pack(
                  anchor="w", padx=18, pady=(12, 8))
        Label(panel,
              text=("The tool creates a new ZIP from that release's locked manifest and saved content objects. "
                    "A current pack download alone is not enough; the changelog helps identify the version you need."),
              bg=THEME["colors"]["panel_bg"], fg=THEME["colors"]["muted_text"],
              font=THEME["fonts"]["body"], wraplength=420, justify=LEFT).pack(
                  anchor="w", padx=18, pady=(0, 18))
        Button(panel, text="Close", command=dialog.destroy, **button_kwargs("accent")).pack(
            anchor="e", padx=18, pady=(0, 18))
        dialog.bind("<Escape>", lambda _event: dialog.destroy())
        dialog.protocol("WM_DELETE_WINDOW", dialog.destroy)
        dialog.update_idletasks()
        x = self.root.winfo_rootx() + max(0, (self.root.winfo_width() - dialog.winfo_width()) // 2)
        y = self.root.winfo_rooty() + max(0, (self.root.winfo_height() - dialog.winfo_height()) // 2)
        dialog.geometry(f"+{x}+{y}")
        dialog.grab_set()
        dialog.focus_set()


    def rebuild_local_program_clicked(self, program: AvailableProgram) -> None:
        service = {"daedalmap-research-mcp": "research", "daedalmap-geometry-mcp": "geometry",
                   NETWORK_PRODUCT: "network",
                   "daedalmap-historical-version-generator": "historical"}.get(program.product)
        if not service or os.environ.get("DAEDALMAP_LAUNCHER_DEV") != "1":
            raise LauncherError("Local program rebuild requires the source launcher in development mode")

        def work() -> None:
            from local_program_trial import install_trial

            result = install_trial(service, APP_ROOT)
            self.append_log(f"Local {program.title} source installed as {result['version']}")
            self.root.after(0, self.update_program_list)

        self.run_worker(f"Rebuilding local {program.title}", work)

    def install_selected_program_clicked(self, program: AvailableProgram, *, updating: bool = False) -> None:
        verb = "Update" if updating else "Install"
        if not messagebox.askyesno(f"{verb} program", f"{verb} {program.title} to {program.version} ({_format_bytes(program.size_bytes)})?\n\nThe launcher will verify the published release and set it up. Your data stays in the shared library."):
            return
        action_var = self.program_action_vars.get((program.product, program.version))
        self.active_program_spinner = ActionSpinner(self.root, [action_var]) if action_var else None
        self.active_program_verb = "Updating" if updating else "Installing"

        def work():
            installed = install_published_program(program, self.append_log, self.prepare_program_runtime)
            self.append_log(f"{'Updated' if updating else 'Installed'} {program.title} v{program.version} at {installed}")
            running = self.program_processes.get(program.product)
            if running is not None and running.poll() is None:
                self.append_log(f"Restart {program.title} to use the new version")
            self.root.after(0, self.update_program_list)

        self.run_worker("Updating program" if updating else "Installing program", work)


    def import_program_clicked(self) -> None:
        path = filedialog.askopenfilename(title="Select a DaedalMap program ZIP", filetypes=[("ZIP archives", "*.zip")])
        if path:
            self.import_program_path(Path(path))


    def import_program_path(self, zip_path: Path) -> None:
        def work():
            program = identify_program_zip(zip_path, programs_for_import(self.append_log))
            decision = {"approved": False}
            ready = threading.Event()

            def confirm() -> None:
                try:
                    decision["approved"] = messagebox.askyesno(
                        "Install downloaded program?",
                        f"{zip_path.name}\n\nVerified as {program.title} v{program.version} by its published SHA-256.\n\nInstall under {PROGRAMS_ROOT}? The original ZIP will stay where it is.")
                finally:
                    ready.set()

            self.root.after(0, confirm)
            ready.wait()
            if not decision["approved"]:
                return
            installed = install_program_zip(zip_path, program)
            self.append_log(f"Imported {program.title} v{program.version} from {zip_path} to {installed}")
            self.root.after(0, self.update_program_list)
            if program.product != "daedalmap-historical-version-generator":
                self.prepare_program_runtime()

        self.run_worker("Checking program ZIP", work)


    def prepare_program_runtime(self) -> Path:
        try:
            return prepare_shared_environment(APP_ROOT, installed_runtime_entries(), self.append_log)
        except RuntimeSetupError as exc:
            raise LauncherError(f"Program files are installed, but runtime setup is incomplete: {exc}") from exc


    def run_selected_program_clicked(self, program: AvailableProgram) -> None:
        if program.product == NETWORK_PRODUCT:
            if installed_network_root() is None:
                messagebox.showinfo("Network unavailable", "The installed Network expansion is missing or incompatible. Reinstall it before opening Mapping.")
                return
            if has_usable_installed_engine():
                self.launch_clicked()
                return
            packs_dir = RUNTIME_DATA_ROOT / "networks" / "packs"
            if not any(packs_dir.glob("*/manifest.json")):
                messagebox.showinfo("No Network packs", "Install a Network data pack before exporting tables.")
                return
        entries = load_program_state()["installed_programs"]
        entry = next((item for item in reversed(entries) if item.get("product") == program.product), None)
        if entry is None:
            messagebox.showinfo("Install program", "Install or import this program before running it.")
            return

        def work():
            install_dir = Path(entry["install_dir"])
            if program.product == NETWORK_PRODUCT:
                python = self.prepare_program_runtime()
                output = RUNTIME_DATA_ROOT / "networks" / "exports" / "parquet"
                result = subprocess.run(
                    [str(python), "-m", "network.cli", "export-all", "--output", str(output)],
                    cwd=str(install_dir), env=build_runtime_env(), capture_output=True, text=True)
                if result.returncode:
                    raise LauncherError((result.stderr or result.stdout).strip() or "Network table export failed")
                self.append_log(result.stdout.strip())
                self.root.after(0, lambda: os.startfile(output))
                return
            if program.product not in MCP_PRODUCTS:
                self.append_log(f"Opening {program.title} files at {install_dir}")
                os.startfile(install_dir)
                return
            server = install_dir / "server.py"
            if not server.is_file():
                raise LauncherError(f"{program.title} is missing server.py; reinstall the tool")
            if program.product == "daedalmap-geometry-mcp" and not any(RUNTIME_DATA_ROOT.rglob("geometry.json")):
                raise LauncherError("Geometry MCP needs a Geometry package. Add one in the Geometry tab, then start it again.")
            running = self.program_processes.get(program.product)
            if running is not None and running.poll() is None:
                self.append_log(f"{program.title} is already running (PID {running.pid})")
                return
            python = self.prepare_program_runtime()
            initialize_runtime_root(self.append_log)
            port = 7011 if program.product == "daedalmap-research-mcp" else 7010
            if wait_for_port(DEFAULT_HOST, port, timeout_seconds=0.1):
                raise LauncherError(f"Port {port} is already in use; stop that service before starting {program.title}")
            log_path = LOGS_ROOT / f"{program.product}.log"
            handle = log_path.open("a", encoding="utf-8")
            self.program_log_handles[program.product] = handle
            env = build_runtime_env()
            env["DAEDALMAP_DISABLE_RUNTIME_ANALYTICS"] = "1"
            command = [str(python), str(server), "--data-root", str(RUNTIME_DATA_ROOT),
                       "--host", DEFAULT_HOST, "--port", str(port)]
            process = subprocess.Popen(command, cwd=str(install_dir), env=env,
                                       stdout=handle, stderr=subprocess.STDOUT,
                                       creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            self.program_processes[program.product] = process
            if not wait_for_port(DEFAULT_HOST, port, timeout_seconds=20.0):
                code = process.poll()
                if code is None:
                    process.terminate()
                self.program_processes.pop(program.product, None)
                self.program_log_handles.pop(program.product, None).close()
                raise LauncherError(f"{program.title} did not become ready on port {port}; exit={code}. See {log_path}")
            self.append_log(f"{program.title} ready at http://{DEFAULT_HOST}:{port}/mcp/{'research' if port == 7011 else 'geometry'} (PID {process.pid})")
            self.root.after(0, self.update_program_list)

        self.run_worker("Starting program", work)


    def stop_selected_program_clicked(self, program: AvailableProgram) -> None:
        product = program.product
        process = self.program_processes.pop(product, None)
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            self.append_log(f"Stopped {product} (PID {process.pid})")
        handle = self.program_log_handles.pop(product, None)
        if handle is not None:
            handle.close()
        self.update_program_list()

    def install_selected_pack_clicked(self, selected: AvailablePack) -> None:
        current = get_local_pack_status(selected.pack_id)
        verb = "Update" if current.installed_version else "Download and install"
        if not messagebox.askyesno(
            "Data download details",
            f"{selected.source_name or selected.pack_id}\n"
            f"Release: {selected.version}  /  Size: {_format_bytes(selected.size_bytes)}\n"
            f"Library: {RUNTIME_ROOT}\n\n{verb} this data pack?",
            confirm_label="Install update" if current.installed_version else "Start download",
        ):
            return
        action_var = self.pack_action_vars.get((selected.package_id, selected.version))
        self.active_pack_spinner = ActionSpinner(self.root, [action_var]) if action_var else None
        self.active_pack_verb = "Updating Pack" if action_var and action_var.get() == "Update" else "Installing Pack"
        self.pack_download_cancel_event.clear()
        self.run_worker(f"Installing {selected.source_name or selected.pack_id}",
                        lambda: self._install_data_pack_work(selected), pack_download="data")

    def install_selected_geometry_clicked(self, selected: dict) -> None:
        self.pack_download_cancel_event.clear()
        self.run_worker(f"Installing {selected.get('title') or selected['packageId']}",
                        lambda: self._install_geometry_work(selected), pack_download="geometry")

    def review_selected_geometry_clicked(self, selected: dict) -> None:
        package_id = selected["packageId"]
        current = installed_geometry(RUNTIME_DATA_ROOT).get(package_id)
        tier = "Detail" if selected.get("tier") == "full" else str(selected.get("tier") or "Geometry").title()
        details = (f"{selected.get('title') or package_id}\n"
                   f"Edition: {tier}  /  Release: {selected['version']}\n"
                   f"File: {selected.get('filename') or selected['artifactUrl'].rsplit('/', 1)[-1]}\n"
                   f"Size: {_format_bytes(selected['size'])}\n"
                   f"SHA-256: {selected['sha256']}\n\n"
                   f"Library: {RUNTIME_DATA_ROOT / 'geometry_downloads' / package_id}")
        if current and current["version"] == selected["version"]:
            if messagebox.askyesno("Geometry download details", details + "\n\nAlready installed. Open its folder?",
                                   confirm_label="Open Folder"):
                os.startfile(current["path"])
            return
        verb = "Replace the installed edition" if current else "Download and install this edition"
        if messagebox.askyesno("Geometry download details", details + f"\n\n{verb}?",
                               confirm_label="Install update" if current else "Start download"):
            self.install_selected_geometry_clicked(selected)

    def import_geometry_zip_clicked(self) -> None:
        path = filedialog.askopenfilename(title="Choose a DaedalMap Geometry ZIP",
                                          filetypes=[("ZIP archives", "*.zip")])
        if not path:
            return
        zip_path = Path(path)

        def work() -> None:
            releases = releases_from_storefront(storefront_catalog_for_import(self.append_log))
            size, checksum = zip_path.stat().st_size, sha256_file(zip_path)
            matches = [entry for entry in releases if entry["size"] == size and entry["sha256"] == checksum]
            if len(matches) != 1:
                raise LauncherError("ZIP does not match a published Geometry download. Refresh Geometry and try again.")
            selected = matches[0]
            decision = {"approved": False}
            ready = threading.Event()

            def ask() -> None:
                try:
                    decision["approved"] = messagebox.askyesno(
                        "Add Geometry package?",
                        f"{zip_path.name}\n\nVerified as {selected['title']}, release {selected['version']}.\n\n"
                        "Copy this ZIP into your library and install the Geometry package?")
                finally:
                    ready.set()

            self.root.after(0, ask)
            ready.wait()
            if decision["approved"]:
                self._install_geometry_work(selected, archive_path=zip_path)

        self.run_worker("Checking Geometry ZIP", work)

    def _install_geometry_work(self, selected: dict, *, archive_path: Path | None = None) -> None:
        initialize_runtime_root(self.append_log)
        release_url = str(selected["artifactUrl"]).rsplit("/", 1)[0] + "/version.json"
        release = read_json_url(release_url, log=self.append_log)
        artifact = release.get("artifact") if isinstance(release.get("artifact"), dict) else {}
        if (release.get("package_id") != selected["packageId"]
                or str(release.get("version") or "") != selected["version"]
                or str(artifact.get("download_url") or "") != selected["artifactUrl"]
                or str(artifact.get("sha256") or "").lower() != selected["sha256"]
                or artifact.get("size_bytes") != selected["size"]):
            raise LauncherError("Published Geometry release no longer matches Downloads; refresh Geometry")
        cache = RUNTIME_CACHE_DIR / "geometry"
        cache.mkdir(parents=True, exist_ok=True)
        saved = cache / f"{selected['packageId']}-{selected['version']}.zip"
        if archive_path is not None:
            if archive_path.resolve() != saved.resolve():
                temporary = saved.with_suffix(".zip.tmp")
                try:
                    shutil.copy2(archive_path, temporary)
                    if temporary.stat().st_size != selected["size"] or sha256_file(temporary) != selected["sha256"]:
                        raise LauncherError("Copied Geometry ZIP failed verification")
                    os.replace(temporary, saved)
                finally:
                    temporary.unlink(missing_ok=True)
        else:
            download_verified_file(selected["artifactUrl"], saved, selected["size"], selected["sha256"],
                                   log=self.append_log, progress=_make_download_progress_reporter(self.append_log),
                                   cancel_event=getattr(self, "pack_download_cancel_event", None))
        if getattr(self, "cancel_geometry_download_button", None) is not None:
            self.root.after(0, self.cancel_geometry_download_button.pack_forget)
        if getattr(self, "pack_download_cancel_event", None) is not None and self.pack_download_cancel_event.is_set():
            raise PackDownloadCancelled()
        path = install_geometry_zip(saved, RUNTIME_DATA_ROOT, selected)
        self.append_log(f"Installed {selected['title']} {selected['version']} in {path}")
        self.append_log("Geometry MCP will use this package on its next start. The extracted folder is also usable by GIS programs.")
        self.invalidate_size_cache()
        self.root.after(0, self.update_geometry_list)

    def import_data_zip_clicked(self) -> None:
        path = filedialog.askopenfilename(title="Choose a DaedalMap data pack ZIP", filetypes=[("ZIP archives", "*.zip")])
        if path:
            self.import_data_zip_path(Path(path))

    def import_data_zip_path(self, zip_path: Path, *, confirmed: bool = False) -> None:
        def work() -> None:
            release = identify_data_zip_release(zip_path, storefront_catalog_for_import(self.append_log))
            package_id = str(release.get("packageId") or "")
            version = str(release.get("version") or "")
            packs = self.available_packs or []
            selected = next((pack for pack in packs if pack.package_id == package_id and pack.version == version), None)
            if selected is None:
                parts = urllib.parse.urlparse(str(release.get("artifactUrl") or "")).path.split("/")
                pack_id = parts[parts.index("packs") + 1] if "packs" in parts and len(parts) > parts.index("packs") + 1 else ""
                if not re.fullmatch(r"[A-Za-z0-9_-]+", pack_id):
                    raise LauncherError("This ZIP matches Downloads, but its pack identity is missing")
                selected = AvailablePack(pack_id, version, package_id,
                                         source_name=str(release.get("title") or pack_id),
                                         description=str(release.get("description") or ""))
                packs = [*packs, selected]
            self.available_packs = packs
            if get_local_pack_status(selected.pack_id).installed_version == selected.version:
                self.append_log(f"{selected.source_name} {selected.version} is already in the library")
                return
            if not confirmed:
                decision = {"approved": False}
                ready = threading.Event()

                def ask() -> None:
                    try:
                        decision["approved"] = messagebox.askyesno(
                            "Add data pack?",
                            f"{zip_path.name}\n\nVerified as {selected.source_name}, release {selected.version}.\n\nCopy this ZIP into your library and install the data pack?")
                    finally:
                        ready.set()

                self.root.after(0, ask)
                ready.wait()
                if not decision["approved"]:
                    return
            self._install_data_pack_work(selected, archive_path=zip_path, release=release)

        self.run_worker("Checking data ZIP", work)

    def _install_data_pack_work(self, selected: AvailablePack, *, archive_path: Path | None = None,
                                release: dict | None = None) -> None:
        self.append_log(f"Preparing data pack: {selected.pack_id}")
        initialize_runtime_root(self.append_log)
        was_running = self.process is not None and self.process.poll() is None
        manifest_ref = selected.current_manifest_url or stable_pack_current_url(
            selected.pack_id, package_id=selected.package_id
        )
        if archive_path is not None and release is not None:
            imports_root = RUNTIME_ROOT / "imports"
            imports_root.mkdir(parents=True, exist_ok=True)
            saved_archive = imports_root / f"{selected.package_id}-{selected.version}.zip"
            if archive_path.resolve() != saved_archive.resolve():
                temporary = saved_archive.with_suffix(".zip.tmp")
                try:
                    shutil.copy2(archive_path, temporary)
                    if sha256_file(temporary) != str(release.get("sha256") or "").lower():
                        raise LauncherError("Copied ZIP failed verification")
                    os.replace(temporary, saved_archive)
                finally:
                    temporary.unlink(missing_ok=True)
            result = install_verified_data_zip(saved_archive, RUNTIME_ROOT,
                                               pack_id=selected.pack_id, release=release,
                                               log=self.append_log)
            self.append_log(f"Saved verified ZIP in the library at {saved_archive}")
        else:
            def download_pack(url: str, path: Path) -> None:
                download_file(url, path, log=self.append_log,
                              progress=_make_download_progress_reporter(self.append_log),
                              cancel_event=getattr(self, "pack_download_cancel_event", None))

            def download_complete() -> None:
                if getattr(self, "cancel_data_download_button", None) is not None:
                    self.root.after(0, self.cancel_data_download_button.pack_forget)
                if (getattr(self, "pack_download_cancel_event", None) is not None
                        and self.pack_download_cancel_event.is_set()):
                    raise PackDownloadCancelled()

            result = install_data_pack(
                manifest_ref, RUNTIME_ROOT,
                fetch_json=lambda url: read_json_url(url, log=self.append_log),
                download=download_pack,
                archive_path=archive_path,
                log=self.append_log,
                on_download_complete=download_complete,
                expected_version=selected.version,
                expected_size=selected.size_bytes,
                selected_sha=selected.sha256,
            )
        self.append_log(f"Added {selected.source_name or selected.pack_id} {result['version']} to the shared library ({result['source_count']} sources)")
        self.invalidate_size_cache()

        if was_running:
            self.append_log("Reloading Mapping with the new data pack")
            self.stop_runtime()
            self.start_runtime(load_state(), open_browser_on_ready=False)
            self.append_log("Mapping is ready; refresh your existing map tab to see the new pack")
        self.root.after(0, self.refresh_installed_label)
        self.root.after(0, self.update_pack_list)
        self.root.after(0, self.refresh_research_views)

    def launch_clicked(self) -> None:
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Launcher Busy", "The launcher is already doing work.")
            return
        if is_runtime_reachable():
            self.open_browser()
            return
        if (not has_usable_installed_engine() and local_mapping_source_root() is None
                and local_mapping_packages_root() is None
                and not messagebox.askyesno(
                    "Download Explore Map?",
                    "Explore Map is not installed. Download and install the current release before opening it?\n\n"
                    f"Launcher folder: {APP_ROOT}",
                    confirm_label="Download and open")):
            return

        if self.map_launch_page is None:
            self.map_launch_page = MapLaunchPage(f"http://{DEFAULT_HOST}:{DEFAULT_PORT}")
        launch_page = self.map_launch_page
        launch_page.error = ""
        self.append_log(f"Opening local map start page: {launch_page.url}")
        webbrowser.open(launch_page.url)

        def work():
            try:
                self.ensure_runtime_ready(open_browser_on_ready=False)
            except Exception as exc:
                launch_page.error = f"The map could not start: {exc}"
                raise

        self.run_worker("Opening Explore Map", work, mapping_launch=True)

    def start_runtime(self, state: EngineState, *, open_browser_on_ready: bool = True) -> None:
        install_dir = Path(state.install_dir)
        if not (install_dir / "app.py").exists():
            raise LauncherError(f"Installed engine is missing app.py: {install_dir}")

        initialize_runtime_root(self.append_log)
        if not (install_dir / "requirements.txt").is_file():
            raise LauncherError("Mapping engine package has no requirements.txt; this release cannot be started safely")
        if local_custom_mode():
            python = prepare_shared_environment(
                APP_ROOT, installed_runtime_entries(load_state()), self.append_log
            )
            self.append_log(f"LOCAL CUSTOM MODE: using launcher-managed Python {python}")
        elif local_source_mode():
            python = Path(os.environ["DAEDALMAP_MAPPING_TEST_PYTHON"]).resolve()
            if not python.is_file():
                raise LauncherError(f"Local source Python is missing: {python}")
            self.append_log(f"LOCAL SOURCE MODE: using workspace Python {python}; no package runtime")
        else:
            python = prepare_shared_environment(
                APP_ROOT, installed_runtime_entries(state), self.append_log
            )
        command = [
            str(python),
            "-m",
            "uvicorn",
            "app:app",
            "--host",
            DEFAULT_HOST,
            "--port",
            str(DEFAULT_PORT),
        ]
        env = build_runtime_env()
        stdout_log = LOGS_ROOT / "runtime.stdout.log"
        stderr_log = LOGS_ROOT / "runtime.stderr.log"
        self.append_log(f"Launching runtime from {install_dir}")
        self.append_log("Runtime access mode: local_wrapper")
        self.append_log(f"Runtime DATA_ROOT: {RUNTIME_DATA_ROOT}")
        self.append_log(f"Runtime PACKS_ROOT: {RUNTIME_PACKS_ROOT}")
        self._close_runtime_log_handles()
        self.runtime_stdout_handle = stdout_log.open("a", encoding="utf-8")
        self.runtime_stderr_handle = stderr_log.open("a", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                command,
                cwd=str(install_dir),
                env=env,
                stdout=self.runtime_stdout_handle,
                stderr=self.runtime_stderr_handle,
                creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
            )
        except Exception:
            self._close_runtime_log_handles()
            raise
        state.runtime_pid = int(self.process.pid)
        if local_mapping_source_root() is None:
            save_state(state)
        self.append_log(f"Runtime PID {self.process.pid}")
        self.append_log(f"stdout log: {stdout_log}")
        self.append_log(f"stderr log: {stderr_log}")

        if not wait_for_port(DEFAULT_HOST, DEFAULT_PORT, timeout_seconds=MAPPING_RUNTIME_START_TIMEOUT_SECONDS):
            exit_code = self.process.poll()
            if exit_code is not None:
                raise LauncherError(
                    "Runtime exited before becoming ready. "
                    f"Exit code: {exit_code}. Check {stderr_log}."
                )
            raise LauncherError(
                f"Runtime did not become ready on http://{DEFAULT_HOST}:{DEFAULT_PORT} within {int(MAPPING_RUNTIME_START_TIMEOUT_SECONDS)} seconds. "
                f"Check {stderr_log}."
            )

        self.append_log("Runtime is responding on localhost")
        if open_browser_on_ready:
            self.open_browser()

    def open_browser(self) -> None:
        url = f"http://{DEFAULT_HOST}:{DEFAULT_PORT}"
        self.append_log(f"Opening {url}")
        webbrowser.open(url)

    def stop_clicked(self) -> None:
        self.stop_runtime()

    def _close_runtime_log_handles(self) -> None:
        for attr in ("runtime_stdout_handle", "runtime_stderr_handle"):
            handle = getattr(self, attr, None)
            if handle is not None:
                try:
                    handle.close()
                except OSError:
                    pass
                setattr(self, attr, None)

    def stop_runtime(self) -> None:
        stopped = False
        if self.process and self.process.poll() is None:
            self.append_log("Stopping runtime")
            _terminate_pid_tree(self.process.pid)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
            stopped = True
        else:
            state = load_state()
            orphan_pid = int(state.runtime_pid or 0)
            if orphan_pid and _pid_is_alive(orphan_pid):
                self.append_log(f"Stopping orphaned runtime PID {orphan_pid} from a previous launcher session")
                _terminate_pid_tree(orphan_pid)
                stopped = True
            else:
                self.append_log("Runtime is not running")
        self.process = None
        self._close_runtime_log_handles()
        state = load_state()
        if state.runtime_pid:
            state.runtime_pid = 0
            save_state(state)
        if stopped:
            self.append_log("Runtime stopped")

    def on_close(self, *, restart_command: list[str] | None = None) -> None:
        if getattr(self, "map_detail_cancel_event", None) is not None:
            self.map_detail_cancel_event.set()
        if getattr(self, "pack_download_cancel_event", None) is not None:
            self.pack_download_cancel_event.set()
        if getattr(self, "map_launch_page", None) is not None:
            self.map_launch_page.close()
            self.map_launch_page = None
        for product, process in list(self.program_processes.items()):
            if process.poll() is None:
                _terminate_pid_tree(process.pid)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
            handle = self.program_log_handles.pop(product, None)
            if handle is not None:
                handle.close()
        self.program_processes.clear()
        self.stop_runtime()
        if restart_command is not None:
            try:
                subprocess.Popen(restart_command, env=launcher_restart_env(),
                                 cwd=str(Path(restart_command[0]).parent),
                                 creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
            except OSError as exc:
                messagebox.showerror("Restart failed", f"Could not reopen the launcher: {exc}")
                return
        self.root.destroy()


def main() -> int:
    if len(sys.argv) in (3, 6) and sys.argv[1] == "--self-test":
        initialize_runtime_root()
        report = {"frozen": bool(getattr(sys, "frozen", False)),
                  "version": LAUNCHER_VERSION,
                  "launcher_root": str(APP_ROOT),
                  "catalog_created": (RUNTIME_DATA_ROOT / "catalog.json").is_file(),
                  "drag_drop_available": TkinterDnD is not None,
                  "uv_available": bundled_uv_executable().is_file()}
        if len(sys.argv) == 6:
            current = json.loads(Path(sys.argv[3]).read_bytes())
            version = json.loads(Path(sys.argv[4]).read_bytes())
            version_url = str(current["version_manifest_url"])
            pointer_url = version_url.rsplit("/", 2)[0] + "/current.json"
            def read_release(url: str) -> dict:
                if url == pointer_url:
                    return current
                if url == version_url:
                    return version
                raise ValueError("Unexpected release URL in packaged self-test")
            installed = install_data_pack(pointer_url, RUNTIME_ROOT,
                fetch_json=read_release,
                download=lambda *_: (_ for _ in ()).throw(AssertionError("Unexpected download in packaged self-test")),
                archive_path=Path(sys.argv[5]), log=lambda *_: None)
            catalog = json.loads((RUNTIME_DATA_ROOT / "catalog.json").read_bytes())
            selected = next(pack for pack in catalog["packs"] if pack["pack_id"] == installed["pack_id"])
            geometry_path = selected.get("reference_geometry_path")
            report["packaged_data_install"] = {"pack_id": installed["pack_id"],
                "source_count": installed["source_count"],
                "reference_geometry_present": bool(geometry_path and (RUNTIME_DATA_ROOT / geometry_path).is_file())}
        receipt = Path(sys.argv[2]).resolve()
        receipt.parent.mkdir(parents=True, exist_ok=True)
        receipt.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return 0 if all((report["catalog_created"], report["drag_drop_available"], report["uv_available"],
                         report.get("packaged_data_install", {}).get("reference_geometry_present", True))) else 1
    configure_local_mapping_trial()
    apply_runtime_storage_root(load_storage_root(), persist=False)
    initialize_runtime_root()
    load_public_env()
    root = TkinterDnD.Tk() if TkinterDnD is not None else Tk()
    app = LauncherApp(root)
    app.append_log("Launcher ready")
    app.append_log(f"Engine manifest URL: {engine_current_url()}")
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
