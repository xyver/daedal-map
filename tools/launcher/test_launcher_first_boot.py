from __future__ import annotations

import sys
import os
import hashlib
import json
import io
import urllib.request
import subprocess
import threading
import zipfile
from types import SimpleNamespace
import pytest
from pathlib import Path

WRAPPER_ROOT = Path(__file__).resolve().parent
if str(WRAPPER_ROOT) not in sys.path:
    sys.path.insert(0, str(WRAPPER_ROOT))

import launcher
import runtime_manager
from map_launch_page import MapLaunchPage


def test_public_download_base_ignores_private_runtime_bucket(monkeypatch) -> None:
    monkeypatch.delenv("DAEDALMAP_PUBLIC_BASE_URL", raising=False)
    monkeypatch.delenv("DAEDALMAP_ENGINE_CURRENT_URL", raising=False)
    monkeypatch.setenv("S3_PUBLIC_BASE_URL", "https://private-runtime.example")
    assert launcher.public_base_url() == "https://downloads.daedalmap.com"
    assert launcher.engine_current_url() == (
        "https://downloads.daedalmap.com/downloadable/engine/daedalmap/stable/current.json")
    monkeypatch.setenv("DAEDALMAP_ENGINE_CURRENT_URL", launcher.engine_current_url())
    assert launcher.public_base_url() == "https://downloads.daedalmap.com"
    assert launcher.packs_index_url() == "https://downloads.daedalmap.com/downloadable/packs/index.json"


def test_map_launch_page_reports_startup_and_failure() -> None:
    page = MapLaunchPage("http://127.0.0.1:1")
    try:
        with urllib.request.urlopen(page.url, timeout=2) as response:
            assert b"Opening Explore Map" in response.read()
        with urllib.request.urlopen(page.url + "status", timeout=2) as response:
            assert json.load(response)["ready"] is False
        page.error = "Map startup failed"
        with urllib.request.urlopen(page.url + "status", timeout=2) as response:
            assert json.load(response)["error"] == "Map startup failed"
    finally:
        page.close()


def test_open_map_shows_wait_page_before_runtime_setup(monkeypatch) -> None:
    events = []
    class Page:
        url = "http://127.0.0.1:8766/"
        error = ""
        def __init__(self, _map_url):
            events.append("page")

    app = object.__new__(launcher.LauncherApp)
    app.worker = None
    app.map_launch_page = None
    app.append_log = lambda _message: None
    app.ensure_runtime_ready = lambda **kwargs: events.append(("runtime", kwargs))
    app.run_worker = lambda _label, work, **_kwargs: work()
    monkeypatch.setattr(launcher, "is_runtime_reachable", lambda: False)
    monkeypatch.setattr(launcher, "has_usable_installed_engine", lambda: True)
    monkeypatch.setattr(launcher, "MapLaunchPage", Page)
    monkeypatch.setattr(launcher.webbrowser, "open", lambda url: events.append(("browser", url)))

    app.launch_clicked()

    assert events == ["page", ("browser", Page.url), ("runtime", {"open_browser_on_ready": False})]


def test_install_action_state_matches_program_and_data_cards() -> None:
    state = launcher.InstallActionState
    assert state.compare("", "1.2") == state("Install", "")
    assert state.compare("1.1", "1.2") == state("Update", "")
    assert state.compare("1.2", "1.2") == state("", "v1.2")


def test_map_detail_download_is_atomic_and_only_complete_pair_is_available(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(launcher, "RUNTIME_ROOT", tmp_path)
    monkeypatch.setattr(launcher, "MAP_DETAIL_FILES", {"land.geojson": "https://example.test/land",
                                                    "countries.geojson": "https://example.test/countries"})
    payload = b'{"type":"FeatureCollection","features":[{"type":"Feature"}]}'
    monkeypatch.setattr(launcher.urllib.request, "urlopen", lambda *_args, **_kwargs: io.BytesIO(payload))
    size = launcher.download_map_detail(lambda _message: None)
    assert size == 2 * len(payload)
    assert launcher.map_detail_available()
    assert not list((tmp_path / "tiles").glob("*.download"))


def test_cancel_map_detail_discards_both_staged_files(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(launcher, "RUNTIME_ROOT", tmp_path)
    monkeypatch.setattr(launcher, "MAP_DETAIL_FILES", {"land.geojson": "https://example.test/land",
                                                    "countries.geojson": "https://example.test/countries"})
    payload = b'{"type":"FeatureCollection","features":[{"type":"Feature"}]}'
    cancel_event = threading.Event()

    class CancelingStream(io.BytesIO):
        def read(self, size=-1):
            chunk = super().read(size)
            cancel_event.set()
            return chunk

    def open_url(url, **_kwargs):
        return CancelingStream(payload) if url.endswith("countries") else io.BytesIO(payload)

    monkeypatch.setattr(launcher.urllib.request, "urlopen", open_url)
    with pytest.raises(launcher.MapDetailDownloadCancelled):
        launcher.download_map_detail(lambda _message: None, cancel_event)
    assert not launcher.map_detail_available()
    assert not list((tmp_path / "tiles").iterdir())


def test_map_detail_click_confirms_before_starting_download(monkeypatch, tmp_path) -> None:
    app = object.__new__(launcher.LauncherApp)
    app.worker = None
    app.map_detail_cancel_event = threading.Event()
    app.map_detail_cancel_event.set()
    started = []
    app.run_worker = lambda *args, **kwargs: started.append((args, kwargs))
    monkeypatch.setattr(launcher, "RUNTIME_ROOT", tmp_path)
    monkeypatch.setattr(launcher.messagebox, "askyesno", lambda *_args: False)
    app.download_map_detail_clicked()
    assert not started
    assert app.map_detail_cancel_event.is_set()
    monkeypatch.setattr(launcher.messagebox, "askyesno", lambda *_args: True)
    app.download_map_detail_clicked()
    assert len(started) == 1
    assert started[0][1]["map_tiles_download"] is True
    assert not app.map_detail_cancel_event.is_set()


def test_uninstalled_map_and_data_pack_do_not_download_without_confirmation(monkeypatch) -> None:
    app = object.__new__(launcher.LauncherApp)
    app.worker = None
    app.pack_action_vars = {}
    started = []
    app.run_worker = lambda *args, **kwargs: started.append((args, kwargs))
    monkeypatch.setattr(launcher, "is_runtime_reachable", lambda: False)
    monkeypatch.setattr(launcher, "has_usable_installed_engine", lambda: False)
    monkeypatch.setattr(launcher, "local_mapping_source_root", lambda: None)
    monkeypatch.setattr(launcher, "local_mapping_packages_root", lambda: None)
    monkeypatch.setattr(launcher, "get_local_pack_status", lambda _pack_id: launcher.LocalPackStatus())
    monkeypatch.setattr(launcher.messagebox, "askyesno", lambda *_args, **_kwargs: False)
    app.launch_clicked()
    app.install_selected_pack_clicked(launcher.AvailablePack("sample", "1.0", "sample-lite"))
    assert not started


def test_pack_install_restarts_map_without_opening_another_browser_window(monkeypatch) -> None:
    class Process:
        def poll(self):
            return None

    app = object.__new__(launcher.LauncherApp)
    app.process = Process()
    app.append_log = lambda _message: None
    app.invalidate_size_cache = lambda: None
    app.refresh_installed_label = lambda: None
    app.update_pack_list = lambda: None
    app.refresh_research_views = lambda: None
    class Root:
        def after(self, _delay, callback):
            callback()
    app.root = Root()
    events = []
    app.stop_runtime = lambda: events.append("stop")
    app.start_runtime = lambda _state, *, open_browser_on_ready=True: events.append(open_browser_on_ready)
    monkeypatch.setattr(launcher, "initialize_runtime_root", lambda _log: None)
    monkeypatch.setattr(launcher, "install_data_pack", lambda *_args, **_kwargs: {"source_count": 1, "version": "1"})
    monkeypatch.setattr(launcher, "load_state", lambda: launcher.EngineState(version="local-dev"))
    app._install_data_pack_work(launcher.AvailablePack("sample", "1", "sample-lite"))
    assert events == ["stop", False]


def test_closing_launcher_stops_managed_programs_and_map_without_prompt(monkeypatch) -> None:
    events = []
    class Process:
        pid = 12345
        def poll(self):
            return None
        def wait(self, timeout):
            events.append(("wait", timeout))
    class Handle:
        def close(self):
            events.append("close log")
    class Root:
        def destroy(self):
            events.append("destroy")
    app = object.__new__(launcher.LauncherApp)
    app.root = Root()
    app.program_processes = {"research": Process()}
    app.program_log_handles = {"research": Handle()}
    app.stop_runtime = lambda: events.append("stop map")
    monkeypatch.setattr(launcher, "_terminate_pid_tree", lambda pid: events.append(("stop tree", pid)))
    monkeypatch.setattr(launcher.messagebox, "askyesno", lambda *_args: pytest.fail("unexpected exit prompt"))
    app.on_close()
    assert events == [("stop tree", 12345), ("wait", 5), "close log", "stop map", "destroy"]


def test_source_launcher_selects_local_install_update_without_release_numbers(tmp_path, monkeypatch) -> None:
    # Record these variables before configure_local_mapping_trial mutates them.
    # Otherwise its direct os.environ writes leak into later launcher tests.
    for name in ("DAEDALMAP_LAUNCHER_DEV", "DAEDALMAP_MAPPING_LOCAL_PACKAGES",
                 "DAEDALMAP_MAPPING_TEST_PYTHON"):
        monkeypatch.setenv(name, os.environ.get(name, ""))
    workspace = tmp_path / "workspace"
    private = workspace / "county-map-private"
    (private / "build").mkdir(parents=True)
    (workspace / "county-map").mkdir()
    (workspace / "county-map" / "app.py").write_text("", encoding="utf-8")
    (private / "build" / "export_downloadable_engine.py").write_text("", encoding="utf-8")
    python = workspace / ".venv" / "Scripts" / "python.exe"
    python.parent.mkdir(parents=True)
    python.write_bytes(b"test")
    monkeypatch.setattr(launcher, "WRAPPER_ROOT", private)
    monkeypatch.setattr(launcher, "PUBLIC_REPO_ROOT", workspace / "county-map")
    monkeypatch.setattr(launcher, "APP_ROOT", tmp_path / "library")
    monkeypatch.delenv("DAEDALMAP_MAPPING_LOCAL_PACKAGES", raising=False)
    monkeypatch.delenv("DAEDALMAP_MAPPING_SOURCE_ROOT", raising=False)
    monkeypatch.delenv("DAEDALMAP_ENGINE_CURRENT_URL", raising=False)
    monkeypatch.delenv("DAEDALMAP_MAPPING_TEST_PYTHON", raising=False)
    launcher.configure_local_mapping_trial()
    assert launcher.local_mapping_packages_root() == tmp_path / "library" / "local-releases"
    assert Path(os.environ["DAEDALMAP_MAPPING_TEST_PYTHON"]) == python
    assert os.environ["DAEDALMAP_LAUNCHER_DEV"] == "1"
    monkeypatch.setenv("DAEDALMAP_MAPPING_LOCAL_PACKAGES", "0")
    launcher.configure_local_mapping_trial()
    assert launcher.local_mapping_packages_root() is None


def test_local_mapping_card_offers_install_update_then_version(tmp_path, monkeypatch) -> None:
    class Field:
        def __init__(self):
            self.value = None
            self.state = None

        def set(self, value):
            self.value = value

        def configure(self, **kwargs):
            self.state = kwargs.get("state", self.state)

        def pack(self, **_kwargs):
            self.packed = True

        def pack_forget(self):
            self.packed = False

    monkeypatch.setenv("DAEDALMAP_LAUNCHER_DEV", "1")
    monkeypatch.setenv("DAEDALMAP_MAPPING_LOCAL_PACKAGES", "1")
    monkeypatch.delenv("DAEDALMAP_MAPPING_SOURCE_ROOT", raising=False)
    monkeypatch.setattr(launcher, "APP_ROOT", tmp_path)
    current = launcher.EngineState()
    monkeypatch.setattr(launcher, "load_state", lambda: current)
    app = object.__new__(launcher.LauncherApp)
    for name in ("install_button", "get_mapping_button", "launch_button",
                 "open_mapping_program_button", "install_button_var", "update_var",
                 "mapping_badge_var", "runtime_current_label", "mapping_current_label"):
        setattr(app, name, Field())
    app.update_text = object()
    app._set_selectable_text = lambda _widget, _value: None
    app.apply_remote_status(True, "a" * 64, "")
    assert app.install_button_var.value == "Install"
    assert app.get_mapping_button.state == launcher.NORMAL
    assert app.open_mapping_program_button.state == launcher.DISABLED
    installed = tmp_path / "engine" / "local-dev"
    installed.mkdir(parents=True)
    (installed / "app.py").write_text("", encoding="utf-8")
    current.version = "local-dev"
    current.install_dir = str(installed)
    monkeypatch.setattr(launcher, "local_mapping_installed_fingerprint", lambda: "b" * 64)
    app.apply_remote_status(True, "a" * 64, "")
    assert app.install_button_var.value == "Update"
    assert app.open_mapping_program_button.state == launcher.NORMAL
    monkeypatch.setattr(launcher, "local_mapping_installed_fingerprint", lambda: "a" * 64)
    app.apply_remote_status(True, "a" * 64, "")
    assert app.mapping_current_label.packed
    assert app.mapping_install_needed is False


def test_browser_settings_request_selects_launcher_tab(tmp_path, monkeypatch) -> None:
    request = tmp_path / "launcher_navigation.json"
    request.write_text(json.dumps({"tab": "library"}), encoding="utf-8")
    monkeypatch.setattr(launcher, "LAUNCHER_NAVIGATION_PATH", request)
    app = object.__new__(launcher.LauncherApp)
    selected = []
    app.show_tab = selected.append
    class Root:
        def deiconify(self):
            pass
        def lift(self):
            pass
    app.root = Root()
    app.consume_navigation_request()
    assert selected == ["library"]
    assert not request.exists()


def test_mapping_install_button_animates_until_worker_finishes(monkeypatch) -> None:
    class Field:
        def __init__(self, value=""):
            self.value = value
            self.state = None

        def get(self):
            return self.value

        def set(self, value):
            self.value = value

        def configure(self, *, state):
            self.state = state

    class Root:
        def __init__(self):
            self.callbacks = []
            self.cancelled = []

        def after(self, _delay, callback):
            self.callbacks.append(callback)
            return len(self.callbacks)

        def after_cancel(self, job):
            self.cancelled.append(job)

    monkeypatch.setattr(launcher, "has_usable_installed_engine", lambda: False)
    app = object.__new__(launcher.LauncherApp)
    app.root = Root()
    app.program_action_buttons = []
    app.pack_action_buttons = []
    app.install_button_var = Field("Install")
    app.mapping_spinner_job = None
    app.mapping_spinner_step = 0
    app.mapping_launch_spinner = launcher.ActionSpinner(app.root, [])
    app.active_pack_spinner = None
    app.active_program_spinner = None
    app.mapping_install_needed = True
    app.mapping_download_available = False
    app.install_button = Field()
    app.get_mapping_button = Field()
    app.open_mapping_program_button = Field()
    app.launch_button = Field()
    app.set_busy(True, mapping_install=True)
    first = app.install_button_var.get()
    assert first.startswith("Installing ")
    assert app.get_mapping_button.state == launcher.DISABLED
    app.root.callbacks[-1]()
    assert app.install_button_var.get() != first
    app.set_busy(False)
    assert app.install_button_var.get() == "Install"
    assert app.root.cancelled


@pytest.mark.parametrize("local_build,installed", [(False, False), (False, True), (True, False), (True, True)])
def test_mapping_install_requires_confirmation_before_work(monkeypatch, local_build, installed) -> None:
    app = object.__new__(launcher.LauncherApp)
    calls = []
    app.run_worker = lambda label, work, **kwargs: calls.append((label, kwargs))
    monkeypatch.setattr(launcher, "local_mapping_packages_root", lambda: Path("local") if local_build else None)
    monkeypatch.setattr(launcher, "load_state", lambda: object())
    monkeypatch.setattr(launcher, "has_usable_installed_engine", lambda _state: installed)
    monkeypatch.setattr(launcher.messagebox, "askyesno", lambda title, message: calls.append((title, message)) or False)

    app.install_clicked()
    assert len(calls) == 1
    assert calls[0][0] == f"{'Update' if installed else 'Install'} program"
    assert ("local source" if local_build else "published release") in calls[0][1]

    calls.clear()
    monkeypatch.setattr(launcher.messagebox, "askyesno", lambda title, message: calls.append((title, message)) or True)
    app.install_clicked()
    assert calls[1][0] == ("Building local Mapping test" if local_build else "Installing engine")
    assert calls[1][1] == {"mapping_install": True}


def test_open_map_buttons_animate_until_launch_finishes(monkeypatch) -> None:
    class Field:
        def __init__(self, value=""):
            self.value = value
            self.state = None
        def get(self):
            return self.value
        def set(self, value):
            self.value = value
        def configure(self, *, state):
            self.state = state

    class Root:
        def __init__(self):
            self.callbacks = []
            self.cancelled = []
        def after(self, _delay, callback):
            self.callbacks.append(callback)
            return len(self.callbacks)
        def after_cancel(self, job):
            self.cancelled.append(job)

    monkeypatch.setattr(launcher, "has_usable_installed_engine", lambda: True)
    app = object.__new__(launcher.LauncherApp)
    app.root = Root()
    app.program_action_buttons = []
    app.pack_action_buttons = []
    app.install_button_var = Field("Install")
    app.open_map_button_var = Field("Open Map")
    app.open_explore_button_var = Field("Open Explore Map")
    app.mapping_spinner_job = None
    app.mapping_launch_spinner = launcher.ActionSpinner(
        app.root, [app.open_map_button_var, app.open_explore_button_var])
    app.active_pack_spinner = None
    app.active_program_spinner = None
    app.mapping_install_needed = False
    app.mapping_download_available = False
    for name in ("install_button", "get_mapping_button", "open_mapping_program_button", "launch_button"):
        setattr(app, name, Field())
    app.set_busy(True, mapping_launch=True)
    assert app.open_map_button_var.get().startswith("Opening Map ")
    assert app.open_mapping_program_button.state == launcher.DISABLED
    app.root.callbacks[-1]()
    app.set_busy(False)
    assert app.open_map_button_var.get() == "Open Map"
    assert app.open_explore_button_var.get() == "Open Explore Map"
    assert app.root.cancelled


def test_program_cards_name_their_actual_use_action() -> None:
    assert launcher.program_kind_label("daedalmap-research-mcp") == "MCP SERVICE"
    assert launcher.program_kind_label("daedalmap-geometry-mcp") == "MCP SERVICE"
    assert launcher.program_kind_label("daedalmap-historical-version-generator") == "CLI TOOL"
    assert launcher.program_use_label("daedalmap-research-mcp", False) == "Start"
    assert launcher.program_use_label("daedalmap-geometry-mcp", True) == "Stop"
    assert launcher.program_use_label("daedalmap-historical-version-generator", False) == "Open Files"
    assert launcher.program_kind_label("daedalmap-network") == "MAPPING EXPANSION"
    assert launcher.program_use_label("daedalmap-network", False) == "Export Parquet"
    assert launcher.program_use_label("daedalmap-network", False, mapping_installed=True) == "Open Mapping"


def test_historical_cli_installs_without_service_runtime(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(launcher, "PROGRAMS_ROOT", tmp_path / "programs")
    monkeypatch.setattr(launcher, "PROGRAMS_STATE_PATH", tmp_path / "programs.json")
    monkeypatch.setattr(launcher, "CACHE_ROOT", tmp_path / "cache")
    archive = tmp_path / "historical.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("historical_version_generator.py", "print('ready')\n")
        output.writestr("README.md", "Run from a terminal.\n")
    program = launcher.AvailableProgram(
        "daedalmap-historical-version-generator", "Historical Version Generator", "1.0.3",
        "https://example.test/manifest.json", "https://example.test/historical.zip",
        hashlib.sha256(archive.read_bytes()).hexdigest(), archive.stat().st_size)
    release = {"product": program.product, "version": program.version,
               "artifact": {"download_url": program.artifact_url, "sha256": program.sha256,
                            "size_bytes": program.size_bytes}}
    monkeypatch.setattr(launcher, "read_json_url", lambda *_args, **_kwargs: release)
    monkeypatch.setattr(launcher, "download_file", lambda _url, path, **_kwargs: path.write_bytes(archive.read_bytes()))
    installed = launcher.install_published_program(program, lambda _message: None,
                                                    lambda: pytest.fail("CLI should not prepare service runtime"))
    assert (installed / "historical_version_generator.py").is_file()
    assert not (installed / "requirements.txt").exists()
    research = tmp_path / "research"
    research.mkdir()
    (research / "requirements.txt").write_text("fastapi\n", encoding="utf-8")
    assert runtime_manager._selected_programs([
        {"product": program.product, "install_dir": str(installed)},
        {"product": "daedalmap-research-mcp", "install_dir": str(research),
         "version": "1.0.1", "sha256": "a" * 64},
    ])[0][0]["product"] == "daedalmap-research-mcp"


def test_open_files_uses_installed_cli_when_catalog_has_newer_release(tmp_path, monkeypatch) -> None:
    installed = tmp_path / "historical"
    installed.mkdir()
    monkeypatch.setattr(launcher, "load_program_state", lambda: {"installed_programs": [
        {"product": "daedalmap-historical-version-generator", "version": "1.0.2",
         "install_dir": str(installed)}]})
    opened = []
    monkeypatch.setattr(launcher.os, "startfile", opened.append)
    program = launcher.AvailableProgram("daedalmap-historical-version-generator",
                                        "Historical Version Generator", "1.0.3", "", "", "", 0)
    app = object.__new__(launcher.LauncherApp)
    app.append_log = lambda _message: None
    app.run_worker = lambda _label, work: work()
    app.prepare_program_runtime = lambda: pytest.fail("CLI should not prepare service runtime")
    app.run_selected_program_clicked(program)
    assert opened == [installed]


def test_local_trial_python_is_used_only_in_development(tmp_path, monkeypatch) -> None:
    program = tmp_path / "mapping"
    program.mkdir()
    (program / "requirements.txt").write_text("fastapi==0.122.0\n", encoding="utf-8")
    python = tmp_path / "python.exe"
    python.write_bytes(b"test")
    entries = [{"product": "daedalmap-mapping", "version": "trial", "install_dir": str(program)}]
    monkeypatch.setenv("DAEDALMAP_MAPPING_TEST_PYTHON", str(python))
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_DEV", "1")
    assert runtime_manager.prepare_shared_environment(tmp_path, entries, lambda _: None) == python
    monkeypatch.delenv("DAEDALMAP_LAUNCHER_DEV")
    monkeypatch.setattr(runtime_manager, "bundled_uv_executable", lambda: tmp_path / "missing-uv.exe")
    with pytest.raises(runtime_manager.RuntimeSetupError, match="runtime helper is missing"):
        runtime_manager.prepare_shared_environment(tmp_path, entries, lambda _: None)


def test_local_program_card_keeps_source_trial_selected_over_store_release() -> None:
    published = launcher.AvailableProgram("daedalmap-research-mcp", "Research MCP", "1.0.1",
                                          "https://example.test/manifest.json", "https://example.test/research.zip",
                                          "a" * 64, 100)
    local = launcher.AvailableProgram("daedalmap-research-mcp", "Research MCP", "local-dev-123456789abc",
                                      "local-source", "", "b" * 64, 0)
    selected = launcher.program_cards_for_view([published, local], [{
        "product": "daedalmap-research-mcp", "version": local.version,
    }])
    assert selected[0][0].version == local.version
    assert selected[0][2] is False


def test_mcp_runtime_selection_includes_installed_mapping_once(monkeypatch) -> None:
    from types import SimpleNamespace

    monkeypatch.setattr(launcher, "load_program_state", lambda: {"installed_programs": [
        {"product": "daedalmap-research-mcp", "version": "1.0.1", "install_dir": "research"},
        {"product": "daedalmap-geometry-mcp", "version": "1.0.2", "install_dir": "geometry"},
    ]})
    monkeypatch.setattr(launcher, "has_usable_installed_engine", lambda _state: True)
    mapping = SimpleNamespace(version="0.1.0", install_dir="mapping", source_commit="abc")
    selected = launcher.installed_runtime_entries(mapping)
    assert [entry["product"] for entry in selected] == [
        "daedalmap-research-mcp", "daedalmap-geometry-mcp", "daedalmap-mapping",
    ]


def test_installed_program_stays_listed_without_storefront(tmp_path, monkeypatch) -> None:
    registry = tmp_path / "programs.json"
    registry.write_text(json.dumps({"schema_version": "1.0", "installed_programs": [{
        "product": "daedalmap-research-mcp", "title": "DaedalMap Research MCP", "version": "1.0.1",
        "install_dir": str(tmp_path / "research"), "sha256": "a" * 64,
    }]}), encoding="utf-8")
    monkeypatch.setattr(launcher, "PROGRAMS_STATE_PATH", registry)
    programs = launcher.include_installed_programs([])
    assert [(item.product, item.version) for item in programs] == [("daedalmap-research-mcp", "1.0.1")]
    assert programs[0].title == "Research MCP"


def test_tool_tabs_follow_local_installs() -> None:
    assert launcher.default_launcher_window_geometry(1920, 1080) == "1400x900+260+90"
    assert launcher.default_launcher_window_geometry(2560, 1440) == "1400x900+580+270"
    assert launcher.default_launcher_window_geometry(1366, 768) == "1286x648+40+60"
    assert [name for name, _ in launcher.NAV_TABS[:5]] == [
        "home", "programs", "library", "geometry", "advanced"]
    assert dict(launcher.NAV_TABS)["advanced"] == "Settings"
    assert dict(launcher.NAV_TABS)["research"] == "Research MCP"
    assert dict(launcher.NAV_TABS)["geometry_mcp"] == "Geometry MCP"
    assert launcher.visible_launcher_tabs(False, set()) == {"home", "library", "geometry", "programs", "advanced"}
    assert "runtime" in launcher.visible_launcher_tabs(True, set())
    assert "research" in launcher.visible_launcher_tabs(False, {"daedalmap-research-mcp"})
    assert "geometry_mcp" in launcher.visible_launcher_tabs(False, {"daedalmap-geometry-mcp"})
    assert "llm" not in launcher.visible_launcher_tabs(False, {"daedalmap-ai-expansion"})
    assert "llm" in launcher.visible_launcher_tabs(True, {"daedalmap-ai-expansion"})
    assert "network" in launcher.visible_launcher_tabs(True, {"daedalmap-network"})


def test_installed_network_is_available_with_auto_start_on_or_off(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(launcher, "APP_ROOT", tmp_path)
    monkeypatch.setattr(launcher, "PROGRAMS_ROOT", tmp_path / "programs")
    monkeypatch.setattr(launcher, "PROGRAMS_STATE_PATH", tmp_path / "programs.json")
    monkeypatch.setattr(launcher, "local_mapping_source_root", lambda: None)
    root = launcher.PROGRAMS_ROOT / "daedalmap-network" / "1.0"
    (root / "network" / "app").mkdir(parents=True)
    (root / "network" / "app" / "mapping_extension.py").write_text("", encoding="utf-8")
    (root / "network" / "extension.json").write_text(
        json.dumps({"product": "daedalmap-network", "mapping_interface": 1}), encoding="utf-8")
    launcher.save_program_state({"schema_version": "1.0", "installed_programs": [
        {"product": "daedalmap-network", "version": "1.0", "install_dir": str(root)}]})
    assert launcher.installed_network_root() == root
    assert launcher.network_start_with_mapping() is True
    assert launcher.build_runtime_env()["DAEDALMAP_NETWORK_AUTO_START"] == "1"
    launcher.save_network_start_with_mapping(False)
    assert launcher.build_runtime_env()["DAEDALMAP_NETWORK_ROOT"] == str(root)
    assert launcher.build_runtime_env()["DAEDALMAP_NETWORK_AUTO_START"] == "0"
    (root / "network" / "extension.json").write_text(
        json.dumps({"product": "daedalmap-network", "mapping_interface": 2}), encoding="utf-8")
    assert launcher.installed_network_root() is None
    assert "DAEDALMAP_NETWORK_ROOT" not in launcher.build_runtime_env()


def test_packaged_launcher_always_uses_its_own_portable_library(tmp_path, monkeypatch) -> None:
    owner = tmp_path / "launcher"
    owner.mkdir()
    appdata = tmp_path / "appdata"
    monkeypatch.delenv("DAEDALMAP_LAUNCHER_ROOT", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(appdata))
    monkeypatch.setattr(launcher.sys, "frozen", True, raising=False)
    monkeypatch.setattr(launcher.sys, "executable", str(owner / "DaedalMapLauncher.exe"))
    assert launcher._default_app_root() == owner / "launcher-data"
    legacy = appdata / "DaedalMapLauncher"
    legacy.mkdir(parents=True)
    (legacy / "runtime_config.json").write_text("{}", encoding="utf-8")
    assert launcher._default_app_root() == owner / "launcher-data"
    (owner / "launcher-data").mkdir()
    assert launcher._default_app_root() == owner / "launcher-data"


def test_local_custom_setting_is_persistent_and_blocks_launcher_downloads(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(launcher, "APP_ROOT", tmp_path)
    monkeypatch.setattr(launcher, "LAUNCHER_MODE_PATH", tmp_path / "launcher_mode.json")
    monkeypatch.setattr(launcher, "LOCAL_CUSTOM_ROOT", tmp_path / "custom")
    for name in ("DAEDALMAP_LAUNCHER_LOCAL_SOURCE", "DAEDALMAP_LAUNCHER_DEV",
                 "DAEDALMAP_LAUNCHER_LOCAL_CUSTOM",
                 "DAEDALMAP_MAPPING_SOURCE_ROOT", "DAEDALMAP_NETWORK_SOURCE_ROOT",
                 "DAEDALMAP_MAPPING_TEST_PYTHON", "DAEDALMAP_MAPPING_LOCAL_PACKAGES"):
        monkeypatch.setenv(name, "")
    launcher.save_launcher_mode("local_custom")
    assert launcher.load_launcher_mode()["mode"] == "local_custom"
    launcher._configure_saved_launcher_mode()
    assert launcher.local_source_mode()
    assert launcher.local_custom_mode()
    assert os.environ["DAEDALMAP_MAPPING_SOURCE_ROOT"] == str(tmp_path / "custom" / "mapping")
    assert not os.environ.get("DAEDALMAP_MAPPING_TEST_PYTHON")
    with pytest.raises(launcher.LauncherError, match="disabled in Local custom"):
        launcher.read_json_url("https://www.daedalmap.com/downloadable/storefront_catalog.json")
    with pytest.raises(launcher.LauncherError, match="disabled in Local custom"):
        launcher.download_file("https://downloads.daedalmap.com/test.zip", tmp_path / "test.zip")
    assert not (tmp_path / "test.zip").exists()


def test_broken_launcher_mode_file_does_not_reconnect(tmp_path, monkeypatch) -> None:
    path = tmp_path / "launcher_mode.json"
    monkeypatch.setattr(launcher, "LAUNCHER_MODE_PATH", path)
    assert launcher.load_launcher_mode()["mode"] == "connected"
    path.write_text("{broken", encoding="utf-8")
    assert launcher.load_launcher_mode()["mode"] == "local_custom"


def test_launcher_restart_reopens_same_library_without_stale_source_override(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(launcher, "APP_ROOT", tmp_path / "launcher-data")
    monkeypatch.setattr(launcher.sys, "frozen", False, raising=False)
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_LOCAL_SOURCE", "1")
    monkeypatch.setenv("DAEDALMAP_MAPPING_SOURCE_ROOT", "old-source")
    command = launcher.launcher_restart_command()
    assert command == [str(Path(sys.executable).resolve()), str(Path(launcher.__file__).resolve()),
                       "--library-root", str(tmp_path / "launcher-data")]
    env = launcher.launcher_restart_env()
    assert env["DAEDALMAP_LAUNCHER_ROOT"] == str(tmp_path / "launcher-data")
    assert "DAEDALMAP_LAUNCHER_LOCAL_SOURCE" not in env
    assert "DAEDALMAP_MAPPING_SOURCE_ROOT" not in env


def test_connected_setting_prevents_source_checkout_from_reentering_local_package_trial(tmp_path, monkeypatch) -> None:
    mode_path = tmp_path / "launcher_mode.json"
    mode_path.write_text('{"mode": "connected"}', encoding="utf-8")
    monkeypatch.setattr(launcher, "LAUNCHER_MODE_PATH", mode_path)
    monkeypatch.delenv("DAEDALMAP_LAUNCHER_DEV", raising=False)
    monkeypatch.delenv("DAEDALMAP_MAPPING_LOCAL_PACKAGES", raising=False)
    launcher.configure_local_mapping_trial()
    assert "DAEDALMAP_MAPPING_LOCAL_PACKAGES" not in os.environ


def test_local_custom_editable_copy_stays_inside_launcher_and_preserves_edits(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(launcher, "LOCAL_CUSTOM_ROOT", tmp_path / "launcher-data" / "custom")
    source = tmp_path / "installed" / "mapping"
    source.mkdir(parents=True)
    (source / "app.py").write_text("released", encoding="utf-8")
    destination = launcher.LOCAL_CUSTOM_ROOT / "mapping"
    launcher._copy_local_custom_tree(source, destination)
    assert (destination / "app.py").read_text(encoding="utf-8") == "released"
    assert (destination / ".daedalmap-local-custom.json").is_file()
    (destination / "app.py").write_text("edited", encoding="utf-8")
    launcher._copy_local_custom_tree(source, destination)
    assert (destination / "app.py").read_text(encoding="utf-8") == "edited"
    assert not list(launcher.LOCAL_CUSTOM_ROOT.glob(".*-stage-*"))


def test_local_custom_copies_installed_programs_into_launcher_folder(tmp_path, monkeypatch) -> None:
    custom = tmp_path / "launcher-data" / "custom"
    monkeypatch.setattr(launcher, "LOCAL_CUSTOM_ROOT", custom)
    installed_mapping = tmp_path / "launcher-data" / "engine" / "mapping"
    installed_mapping.mkdir(parents=True)
    (installed_mapping / "app.py").write_text("map", encoding="utf-8")
    (installed_mapping / "requirements.txt").write_text("fastapi", encoding="utf-8")
    installed_network = tmp_path / "launcher-data" / "programs" / "network"
    (installed_network / "network" / "app").mkdir(parents=True)
    (installed_network / "network" / "extension.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(launcher, "load_state", lambda: launcher.EngineState(version="1", install_dir=str(installed_mapping)))
    monkeypatch.setattr(launcher, "installed_network_root", lambda: installed_network)
    assert launcher.prepare_local_custom_sources() == custom / "mapping"
    assert (custom / "mapping" / "app.py").read_text(encoding="utf-8") == "map"
    assert (custom / "network" / "network" / "extension.json").is_file()
    (custom / "mapping" / "app.py").write_text("edited", encoding="utf-8")
    launcher.prepare_local_custom_sources()
    assert (custom / "mapping" / "app.py").read_text(encoding="utf-8") == "edited"


def test_mode_change_restart_prompt_has_now_and_later(monkeypatch) -> None:
    calls = []
    app = object.__new__(launcher.LauncherApp)
    app.on_close = lambda **kwargs: calls.append(kwargs)
    monkeypatch.setattr(launcher.messagebox, "askyesno", lambda *_args, **kwargs: calls.append(kwargs) or False)
    app.offer_restart_after_mode_change()
    assert calls == [{"confirm_label": "Restart now", "cancel_label": "Later"}]
    monkeypatch.setattr(launcher.messagebox, "askyesno", lambda *_args, **kwargs: True)
    app.offer_restart_after_mode_change()
    assert calls[-1] == {"restart_command": launcher.launcher_restart_command()}


def test_restart_spawns_after_runtime_cleanup(monkeypatch) -> None:
    events = []
    app = object.__new__(launcher.LauncherApp)
    app.root = SimpleNamespace(destroy=lambda: events.append("destroy"))
    app.program_processes = {}
    app.program_log_handles = {}
    app.stop_runtime = lambda: events.append("stop runtime")
    monkeypatch.setattr(launcher.subprocess, "Popen", lambda command, **kwargs: events.append(("spawn", command, kwargs)))
    app.on_close(restart_command=["python.exe", "launcher.py", "--library-root", "library"])
    assert [event if isinstance(event, str) else event[0] for event in events] == [
        "stop runtime", "spawn", "destroy"]


def test_installed_packs_remain_visible_without_cloud_catalog() -> None:
    state = {"installed_packs": [{"pack_id": "cejst", "version": "1.0"}], "active_pack_ids": ["cejst"]}
    assert launcher.packs_for_view([], state, "cloud") == []
    installed = launcher.packs_for_view([], state, "installed")
    assert [(pack.pack_id, status.installed_version, status.active, in_cloud)
            for pack, status, in_cloud in installed] == [("cejst", "1.0", True, False)]


def test_installed_data_pack_stays_in_installed_view_with_update_action() -> None:
    state = {"installed_packs": [{"pack_id": "cejst", "version": "1.0"}], "active_pack_ids": ["cejst"]}
    current = launcher.AvailablePack("cejst", "2.0", "cejst-lite")
    installed = launcher.packs_for_view([current], state, "installed")
    assert installed[0][0] is current
    assert installed[0][1].installed_version == "1.0"
    assert launcher.InstallActionState.compare("1.0", installed[0][0].version).action == "Update"


def test_geometry_download_cancel_then_resumes_verified_partial(tmp_path, monkeypatch) -> None:
    payload = b"a full geometry ZIP transfer"
    destination = tmp_path / "geometry.zip"
    cancel = threading.Event()
    requested_ranges = []

    class Response(io.BytesIO):
        def __init__(self, body, status, headers):
            super().__init__(body)
            self.status = status
            self.headers = headers

        def read(self, size=-1):
            return super().read(min(size, 4))

    def open_request(request, timeout):
        start = int(request.headers.get("Range", "bytes=0-").split("=")[1].split("-")[0])
        requested_ranges.append(start)
        headers = {"Content-Length": str(len(payload) - start)}
        if start:
            headers["Content-Range"] = f"bytes {start}-{len(payload) - 1}/{len(payload)}"
        return Response(payload[start:], 206 if start else 200, headers)

    monkeypatch.setattr(launcher.urllib.request, "urlopen", open_request)
    kwargs = {"cancel_event": cancel, "progress": lambda done, _total: cancel.set() if done == 4 else None}
    with pytest.raises(launcher.PackDownloadCancelled):
        launcher.download_verified_file("https://example.test/geometry.zip", destination, len(payload),
                                        hashlib.sha256(payload).hexdigest(), **kwargs)
    assert not destination.exists()
    partial = destination.with_name(destination.name + f".{hashlib.sha256(payload).hexdigest()}.part")
    assert partial.stat().st_size == 4
    cancel.clear()
    launcher.download_verified_file("https://example.test/geometry.zip", destination, len(payload),
                                    hashlib.sha256(payload).hexdigest(), cancel_event=cancel)
    assert requested_ranges == [0, 4]
    assert destination.read_bytes() == payload


def test_complete_geometry_partial_is_promoted_without_network(tmp_path, monkeypatch) -> None:
    payload = b"already complete"
    digest = hashlib.sha256(payload).hexdigest()
    destination = tmp_path / "geometry.zip"
    destination.with_name(destination.name + f".{digest}.part").write_bytes(payload)
    monkeypatch.setattr(launcher.urllib.request, "urlopen",
                        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("network used")))
    launcher.download_verified_file("https://example.test/geometry.zip", destination, len(payload), digest)
    assert destination.read_bytes() == payload


def test_program_links_use_existing_site_pages_only() -> None:
    assert launcher.program_page_path("daedalmap") == "/explore"
    assert launcher.program_page_path("daedalmap-research-mcp") == "/research"
    assert launcher.program_page_path("daedalmap-geometry-mcp") == "/devs"
    assert launcher.program_page_path("daedalmap-historical-version-generator") == ""


def test_program_discovery_reads_the_website_catalog(tmp_path, monkeypatch) -> None:
    calls = []
    catalog = {"categories": {"programs": [{
        "packageId": "daedalmap-research-mcp", "title": "DaedalMap Research MCP", "version": "1.0.1",
        "manifestUrl": "https://downloads.daedalmap.com/manifest.json",
        "artifactUrl": "https://downloads.daedalmap.com/research.zip",
        "sha256": "a" * 64, "size": 992581,
    }]}}
    catalog["categories"]["programs"].insert(0, {
        "packageId": "daedalmap-launcher", "title": "DaedalMap Launcher", "version": "0.1.2",
        "manifestUrl": "https://downloads.daedalmap.com/launcher/manifest.json",
        "artifactUrl": "https://downloads.daedalmap.com/launcher.zip",
        "sha256": "b" * 64, "size": 27195114,
    })
    monkeypatch.setattr(launcher, "read_json_url", lambda url, **kwargs: (calls.append(url), catalog)[1])
    monkeypatch.setattr(launcher, "CACHE_ROOT", tmp_path / "cache")
    programs = launcher.fetch_available_programs(lambda _: None)
    assert calls == [launcher.storefront_catalog_url()]
    assert len(programs) == 1
    assert programs[0].product == "daedalmap-research-mcp"
    assert programs[0].title == "Research MCP"
    assert programs[0].artifact_url == catalog["categories"]["programs"][1]["artifactUrl"]
    assert (tmp_path / "cache" / "storefront_catalog.json").is_file()


def test_program_cards_offer_update_for_published_current_release() -> None:
    current = launcher.AvailableProgram("daedalmap-research-mcp", "Research MCP", "1.0.2",
                                        "https://example.test/1.0.2/manifest.json",
                                        "https://example.test/1.0.2/program.zip", "a" * 64, 100)
    old = launcher.AvailableProgram(current.product, current.title, "1.0.1", "", "", "b" * 64, 0)
    installed = [{"product": current.product, "version": "1.0.1"}]
    assert launcher.program_cards_for_view([current, old], installed) == [(current, installed[0], True)]
    installed[0]["version"] = "1.0.2"
    assert launcher.program_cards_for_view([current, old], installed) == [(current, installed[0], False)]


def test_program_update_rechecks_manifest_and_restores_prior_version(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(launcher, "PROGRAMS_ROOT", tmp_path / "programs")
    monkeypatch.setattr(launcher, "PROGRAMS_STATE_PATH", tmp_path / "programs.json")
    monkeypatch.setattr(launcher, "CACHE_ROOT", tmp_path / "cache")
    old_zip = tmp_path / "old.zip"
    new_zip = tmp_path / "new.zip"
    for archive_path, content in ((old_zip, "old"), (new_zip, "new")):
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("server.py", content)
            archive.writestr("requirements.txt", "fastapi==0.122.0\n")
    def program(version, archive):
        return launcher.AvailableProgram("daedalmap-research-mcp", "Research MCP", version,
                                         f"https://example.test/{version}/manifest.json",
                                         f"https://example.test/{version}/program.zip",
                                         hashlib.sha256(archive.read_bytes()).hexdigest(), archive.stat().st_size)
    old = program("1.0.1", old_zip)
    new = program("1.0.2", new_zip)
    old_dir = launcher.install_program_zip(old_zip, old)
    release = {"product": new.product, "version": new.version,
               "artifact": {"download_url": new.artifact_url, "sha256": new.sha256,
                            "size_bytes": new.size_bytes}}
    monkeypatch.setattr(launcher, "read_json_url", lambda *_args, **_kwargs: release)
    monkeypatch.setattr(launcher, "download_file", lambda _url, path, **_kwargs: path.write_bytes(new_zip.read_bytes()))
    release["artifact"]["sha256"] = "0" * 64
    with pytest.raises(launcher.LauncherError, match="no longer matches"):
        launcher.install_published_program(new, lambda _: None, lambda: None)
    assert not (tmp_path / "cache").exists()
    release["artifact"]["sha256"] = new.sha256
    with pytest.raises(RuntimeError, match="setup failed"):
        launcher.install_published_program(new, lambda _: None,
                                           lambda: (_ for _ in ()).throw(RuntimeError("setup failed")))
    assert old_dir.is_dir()
    assert not (tmp_path / "programs" / new.product / new.version).exists()
    assert [entry["version"] for entry in launcher.load_program_state()["installed_programs"]] == ["1.0.1"]
    installed = launcher.install_published_program(new, lambda _: None, lambda: None)
    assert (installed / "server.py").read_text(encoding="utf-8") == "new"
    assert launcher.load_program_state()["installed_programs"][-1]["version"] == "1.0.2"


def test_mapping_source_mode_opens_checkout_without_install(tmp_path, monkeypatch) -> None:
    source = tmp_path / "county-map"
    source.mkdir()
    (source / "app.py").write_text("", encoding="utf-8")
    (source / "requirements.txt").write_text("", encoding="utf-8")
    monkeypatch.setenv("DAEDALMAP_MAPPING_SOURCE_ROOT", str(source))
    monkeypatch.delenv("DAEDALMAP_LAUNCHER_DEV", raising=False)
    assert launcher.local_mapping_source_root() is None
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_DEV", "1")
    assert launcher.has_usable_installed_engine() is True
    monkeypatch.setattr(launcher, "is_runtime_reachable", lambda: False)
    calls = []
    app = object.__new__(launcher.LauncherApp)
    app.append_log = calls.append
    app.start_runtime = lambda state, *, open_browser_on_ready: calls.append((state, open_browser_on_ready))
    state = app.ensure_runtime_ready(open_browser_on_ready=True)
    assert state.install_dir == str(source)
    assert state.version == "local-source"
    assert calls[-1] == (state, True)


def test_explicit_local_source_uses_source_network_and_shared_library(tmp_path, monkeypatch) -> None:
    source = tmp_path / "county-map"
    (source / "network" / "app").mkdir(parents=True)
    (source / "network" / "data" / "packs").mkdir(parents=True)
    (source / "app.py").write_text("", encoding="utf-8")
    (source / "requirements.txt").write_text("", encoding="utf-8")
    (source / "network" / "app" / "mapping_extension.py").write_text("", encoding="utf-8")
    (source / "network" / "extension.json").write_text(
        json.dumps({"product": "daedalmap-network", "mapping_interface": 1}), encoding="utf-8")
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_LOCAL_SOURCE", "1")
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_DEV", "1")
    monkeypatch.setenv("DAEDALMAP_MAPPING_SOURCE_ROOT", str(source))
    monkeypatch.setenv("DAEDALMAP_NETWORK_SOURCE_ROOT", str(source))
    monkeypatch.setattr(launcher, "RUNTIME_DATA_ROOT", tmp_path / "existing-library" / "data")
    monkeypatch.setattr(launcher, "network_start_with_mapping", lambda: False)
    monkeypatch.setattr(launcher, "installed_network_root",
                        lambda: pytest.fail("local source must not select installed Network"))
    env = launcher.build_runtime_env()
    assert env["DATA_ROOT"] == str(tmp_path / "existing-library" / "data")
    assert env["DAEDALMAP_NETWORK_ROOT"] == str(source)
    assert env["NETWORK_PACKS_DIR"] == str(source / "network" / "data" / "packs")
    assert env["DAEDALMAP_NETWORK_AUTO_START"] == "0"
    assert env["DAEDALMAP_LOCAL_SOURCE_MODE"] == "1"
    assert launcher.visible_launcher_tabs(True, set()) == {
        "home", "library", "geometry", "advanced", "runtime", "network"}


def test_local_custom_mapping_runs_without_network_source(tmp_path, monkeypatch) -> None:
    source = tmp_path / "county-map"
    source.mkdir()
    (source / "app.py").write_text("", encoding="utf-8")
    (source / "requirements.txt").write_text("", encoding="utf-8")
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_LOCAL_SOURCE", "1")
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_DEV", "1")
    monkeypatch.setenv("DAEDALMAP_MAPPING_SOURCE_ROOT", str(source))
    monkeypatch.setenv("DAEDALMAP_NETWORK_SOURCE_ROOT", str(source))
    monkeypatch.setenv("DAEDALMAP_NETWORK_ROOT", "stale-installed-network")
    env = launcher.build_runtime_env()
    assert env["DAEDALMAP_LOCAL_SOURCE_MODE"] == "1"
    assert "DAEDALMAP_NETWORK_ROOT" not in env
    assert "NETWORK_PACKS_DIR" not in env


def test_local_custom_network_uses_shared_library_packs(tmp_path, monkeypatch) -> None:
    source = tmp_path / "custom" / "mapping"
    source.mkdir(parents=True)
    (source / "app.py").write_text("", encoding="utf-8")
    (source / "requirements.txt").write_text("", encoding="utf-8")
    network = tmp_path / "custom" / "network"
    (network / "network" / "app").mkdir(parents=True)
    (network / "network" / "extension.json").write_text(
        json.dumps({"product": "daedalmap-network", "mapping_interface": 1}), encoding="utf-8")
    (network / "network" / "app" / "mapping_extension.py").write_text("", encoding="utf-8")
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_LOCAL_SOURCE", "1")
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_LOCAL_CUSTOM", "1")
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_DEV", "1")
    monkeypatch.setenv("DAEDALMAP_MAPPING_SOURCE_ROOT", str(source))
    monkeypatch.setenv("DAEDALMAP_NETWORK_SOURCE_ROOT", str(network))
    env = launcher.build_runtime_env()
    assert env["DAEDALMAP_NETWORK_ROOT"] == str(network)
    assert env["DAEDALMAP_LOCAL_CUSTOM_MODE"] == "1"
    assert "NETWORK_PACKS_DIR" not in env


def test_explicit_local_source_rejects_unowned_port(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_LOCAL_SOURCE", "1")
    monkeypatch.setattr(launcher, "local_mapping_source_root", lambda: tmp_path)
    monkeypatch.setattr(launcher, "is_runtime_reachable", lambda: True)
    app = object.__new__(launcher.LauncherApp)
    app.process = None
    with pytest.raises(launcher.LauncherError, match="Port 7000 is already used"):
        app.ensure_runtime_ready()


def test_installed_mapping_launch_does_not_automatically_update(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("DAEDALMAP_MAPPING_SOURCE_ROOT", raising=False)
    installed = tmp_path / "engine"
    installed.mkdir()
    (installed / "app.py").write_text("", encoding="utf-8")
    state = launcher.EngineState(version="1.0", install_dir=str(installed))
    monkeypatch.setattr(launcher, "load_state", lambda: state)
    monkeypatch.setattr(launcher, "is_runtime_reachable", lambda: False)
    monkeypatch.setattr(launcher, "install_engine", lambda _log: pytest.fail("unexpected update"))
    calls = []
    app = object.__new__(launcher.LauncherApp)
    app.process = None
    app.append_log = calls.append
    app.start_runtime = lambda selected, *, open_browser_on_ready: calls.append(selected)
    assert app.ensure_runtime_ready() is state
    assert calls[-1] is state


def test_local_mapping_rebuild_reuses_one_install_slot(tmp_path, monkeypatch) -> None:
    if not (launcher.WRAPPER_ROOT / "build" / "export_downloadable_engine.py").is_file():
        pytest.skip("Private local package exporter is not present in a public checkout")
    source = tmp_path / "public-source"
    source.mkdir()
    subprocess.run(["git", "-C", str(source), "init"], check=True, capture_output=True)
    (source / "app.py").write_text("first", encoding="utf-8")
    (source / "requirements.txt").write_text("fastapi\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source), "add", "app.py", "requirements.txt"],
                   check=True, capture_output=True)
    subprocess.run(["git", "-C", str(source), "-c", "user.name=Test", "-c",
                    "user.email=test@example.invalid", "commit", "-m", "fixture"],
                   check=True, capture_output=True)
    monkeypatch.setattr(launcher, "APP_ROOT", tmp_path / "library")
    monkeypatch.setattr(launcher, "ENGINE_ROOT", tmp_path / "library" / "engine")
    monkeypatch.setattr(launcher, "STATE_PATH", tmp_path / "library" / "state.json")
    monkeypatch.setattr(launcher, "RUNTIME_CONFIG_PATH", tmp_path / "library" / "runtime_config.json")
    runtime = tmp_path / "library" / "runtime"
    for name, folder in (("RUNTIME_ROOT", runtime), ("LOGS_ROOT", runtime / "logs"),
                         ("RUNTIME_CONFIG_DIR", runtime / "config"),
                         ("RUNTIME_STATE_DIR", runtime / "state"),
                         ("RUNTIME_CACHE_DIR", runtime / "cache"),
                         ("RUNTIME_LOG_DIR", runtime / "logs"),
                         ("RUNTIME_DATA_ROOT", runtime / "data"),
                         ("RUNTIME_PACKS_ROOT", runtime / "packs")):
        monkeypatch.setattr(launcher, name, folder)
    monkeypatch.setattr(launcher, "PUBLIC_REPO_ROOT", source)
    monkeypatch.setenv("DAEDALMAP_LAUNCHER_DEV", "1")
    monkeypatch.setenv("DAEDALMAP_MAPPING_LOCAL_PACKAGES", "1")
    monkeypatch.setenv("DAEDALMAP_MAPPING_TEST_PYTHON", sys.executable)
    first = launcher.build_and_install_local_mapping(lambda _: None)
    assert first.version == "local-dev"
    assert (Path(first.install_dir) / "app.py").read_text(encoding="utf-8") == "first"
    (source / "app.py").write_text("second", encoding="utf-8")
    second = launcher.build_and_install_local_mapping(lambda _: None)
    assert second.version == first.version
    assert second.install_dir == first.install_dir
    assert (Path(second.install_dir) / "app.py").read_text(encoding="utf-8") == "second"
    assert (tmp_path / "library" / "engine" / ".local-dev.previous" / "app.py").read_text(encoding="utf-8") == "first"
    assert {path.name for path in (tmp_path / "library" / "engine").iterdir()} == {"local-dev", ".local-dev.previous"}
    assert (tmp_path / "library" / "local-releases" / "local-dev.zip").is_file()
    retained_manifest = json.loads((tmp_path / "library" / "local-releases" / "local-dev-release.json").read_text(encoding="utf-8"))
    assert retained_manifest["artifact"]["download_url"] == (tmp_path / "library" / "local-releases" / "local-dev.zip").as_uri()
    original_replace = launcher.os.replace
    with monkeypatch.context() as locked:
        def fail_locked_swap(source_path, destination_path):
            if Path(source_path) == Path(second.install_dir) and Path(destination_path).name == ".local-dev.previous":
                raise PermissionError("installed engine is in use")
            return original_replace(source_path, destination_path)

        locked.setattr(launcher.os, "replace", fail_locked_swap)
        with pytest.raises(PermissionError, match="installed engine is in use"):
            launcher.build_and_install_local_mapping(lambda _: None)
    assert (Path(second.install_dir) / "app.py").read_text(encoding="utf-8") == "second"
    assert launcher.has_usable_installed_engine()
    class FailedBuild:
        returncode = 1
        stderr = "builder failed"
        stdout = ""
    monkeypatch.setattr(launcher.subprocess, "run", lambda *_args, **_kwargs: FailedBuild())
    with pytest.raises(launcher.LauncherError, match="builder failed"):
        launcher.build_and_install_local_mapping(lambda _: None)
    assert (Path(second.install_dir) / "app.py").read_text(encoding="utf-8") == "second"
    assert launcher.load_state().version == "local-dev"
    assert (tmp_path / "library" / "runtime_config.json").is_file()


def test_data_discovery_uses_one_storefront_request(tmp_path, monkeypatch) -> None:
    calls = []
    catalog = {"categories": {"data": [{
        "packageId": "cejst-lite", "title": "Climate and Economic Justice Screening Tool",
        "description": "Disadvantaged community classification.", "version": "20260816.1",
        "tier": "lite",
        "manifestUrl": "https://downloads.daedalmap.com/downloadable/packs/cejst/cejst-lite/stable/20260816.1/version.json",
    }]}}
    monkeypatch.setattr(launcher, "read_json_url", lambda url, **kwargs: (calls.append(url), catalog)[1])
    monkeypatch.setattr(launcher, "CACHE_ROOT", tmp_path / "cache")
    packs = launcher.fetch_available_packs(lambda _: None)
    assert calls == [launcher.storefront_catalog_url()]
    assert [(pack.pack_id, pack.package_id, pack.version) for pack in packs] == [
        ("cejst", "cejst-lite", "20260816.1")]
    assert packs[0].current_manifest_url == (
        "https://downloads.daedalmap.com/downloadable/packs/cejst/cejst-lite/stable/current.json")
    assert packs[0].source_name == catalog["categories"]["data"][0]["title"]


def test_data_card_copy_matches_same_storefront_release() -> None:
    packs = [launcher.AvailablePack("cejst", "20260816.1", "cejst-lite", source_name="Source name")]
    launcher._apply_storefront_pack_copy(packs, {"categories": {"data": [{
        "packageId": "cejst-lite", "version": "20260816.1", "title": "Climate and Economic Justice Screening Tool",
        "description": "Disadvantaged community classification.",
    }]}})
    assert packs[0].source_name == "Climate and Economic Justice Screening Tool"
    assert packs[0].description == "Disadvantaged community classification."
    launcher._apply_storefront_pack_copy(packs, {"categories": {"data": [{
        "packageId": "cejst-lite", "version": "older", "title": "Outdated title",
    }]}})
    assert packs[0].source_name == "Climate and Economic Justice Screening Tool"


def test_data_zip_matches_downloads_release_by_size_and_hash(tmp_path) -> None:
    archive = tmp_path / "data pack.zip"
    archive.write_bytes(b"published data pack")
    item = {"packageId": "cejst-lite", "version": "20260816.1",
            "size": archive.stat().st_size, "sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}
    assert launcher.identify_data_zip_release(archive, {"categories": {"data": [item]}}) is item
    with pytest.raises(launcher.LauncherError):
        launcher.identify_data_zip_release(archive, {"categories": {"data": []}})


def test_new_data_zip_is_offered_after_copy_finishes(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    incoming = data_root / "incoming"
    incoming.mkdir(parents=True)
    archive = incoming / "data.zip"
    archive.write_bytes(b"partial")
    monkeypatch.setattr(launcher, "RUNTIME_DATA_ROOT", data_root)
    monkeypatch.setattr(launcher.messagebox, "askyesno", lambda *_: True)
    accepted = []
    monkeypatch.setattr(launcher.LauncherApp, "import_data_zip_path",
                        lambda self, path, *, confirmed=False: accepted.append((path, confirmed)))
    app = object.__new__(launcher.LauncherApp)
    app.worker = None
    app.incoming_snapshots = {}
    app.incoming_reviewed = set()
    app.scan_incoming_data()
    assert not accepted
    archive.write_bytes(b"complete download")
    app.scan_incoming_data()
    assert not accepted
    app.scan_incoming_data()
    app.scan_incoming_data()
    assert accepted == [(archive, True)]


def test_imported_data_zip_uses_local_archive(tmp_path, monkeypatch) -> None:
    archive = tmp_path / "data.zip"
    archive.write_bytes(b"published pack")
    pack = launcher.AvailablePack("cejst", "20260816.1", "cejst-lite", source_name="CEJST")
    catalog = {"categories": {"data": [{
        "packageId": pack.package_id, "version": pack.version,
        "size": archive.stat().st_size, "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
    }]}}
    monkeypatch.setattr(launcher, "storefront_catalog_for_import", lambda log: catalog)
    monkeypatch.setattr(launcher, "get_local_pack_status", lambda pack_id: launcher.LocalPackStatus())
    installed = []
    app = object.__new__(launcher.LauncherApp)
    app.available_packs = [pack]
    app.append_log = lambda message: None
    app.run_worker = lambda label, work: work()
    app._install_data_pack_work = lambda selected, *, archive_path=None, release=None: installed.append((selected, archive_path, release))
    app.import_data_zip_path(archive, confirmed=True)
    assert installed == [(pack, archive, catalog["categories"]["data"][0])]


def test_imported_program_uses_catalog_hash_and_preserves_source(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(launcher, "PROGRAMS_ROOT", tmp_path / "programs")
    monkeypatch.setattr(launcher, "PROGRAMS_STATE_PATH", tmp_path / "programs.json")
    source = tmp_path / "from usb.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("README.md", "Run this tool")
        archive.writestr("server.py", "print('ready')")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    program = launcher.AvailableProgram("daedalmap-research-mcp", "Research MCP", "1.0.1",
                                        "https://example.test/manifest.json", "https://example.test/program.zip",
                                        digest, source.stat().st_size)
    selected = launcher.identify_program_zip(source, [program])
    installed = launcher.install_program_zip(source, selected)
    assert (installed / "server.py").is_file()
    assert source.is_file()
    assert launcher.install_program_zip(source, selected) == installed
    state = json.loads((tmp_path / "programs.json").read_text(encoding="utf-8"))
    assert state["installed_programs"][0]["sha256"] == digest


def test_program_import_rejects_unknown_bytes_and_unsafe_members(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(launcher, "PROGRAMS_ROOT", tmp_path / "programs")
    monkeypatch.setattr(launcher, "PROGRAMS_STATE_PATH", tmp_path / "programs.json")
    source = tmp_path / "unknown.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("../escape.py", "bad")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    program = launcher.AvailableProgram("daedalmap-research-mcp", "Research MCP", "1.0.1",
                                        "https://example.test/manifest.json", "https://example.test/program.zip",
                                        digest, source.stat().st_size)
    try:
        launcher.identify_program_zip(source, [])
    except launcher.LauncherError:
        pass
    else:
        raise AssertionError("unknown ZIP was accepted")
    try:
        launcher.install_program_zip(source, program)
    except launcher.LauncherError:
        pass
    else:
        raise AssertionError("unsafe ZIP member was accepted")
    assert not (tmp_path / "escape.py").exists()
    assert not (tmp_path / "programs.json").exists()


def test_failed_new_program_version_preserves_prior_install(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(launcher, "PROGRAMS_ROOT", tmp_path / "programs")
    monkeypatch.setattr(launcher, "PROGRAMS_STATE_PATH", tmp_path / "programs.json")
    source = tmp_path / "program.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("server.py", "print('v1')")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    v1 = launcher.AvailableProgram("daedalmap-research-mcp", "Research MCP", "1.0.1",
                                   "https://example.test/v1.json", "https://example.test/v1.zip",
                                   digest, source.stat().st_size)
    old_dir = launcher.install_program_zip(source, v1)
    v2 = launcher.AvailableProgram(v1.product, v1.title, "1.0.2", v1.manifest_url,
                                   v1.artifact_url, "0" * 64, v1.size_bytes)
    try:
        launcher.install_program_zip(source, v2)
    except launcher.LauncherError:
        pass
    else:
        raise AssertionError("mismatched update was accepted")
    assert (old_dir / "server.py").read_text(encoding="utf-8") == "print('v1')"
    assert not (tmp_path / "programs" / v1.product / "1.0.2").exists()


def test_programs_page_is_reachable_from_navigation(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(launcher, "PROGRAMS_STATE_PATH", tmp_path / "programs.json")
    monkeypatch.setattr(launcher, "STATE_PATH", tmp_path / "mapping-state.json")
    monkeypatch.setattr(launcher, "load_pack_state", lambda: {"installed_packs": [], "active_pack_ids": []})
    monkeypatch.setattr(launcher.LauncherApp, "reconcile_runtime_state", lambda self: None)
    monkeypatch.setattr(launcher.LauncherApp, "refresh_installed_label", lambda self: None)
    monkeypatch.setattr(launcher.LauncherApp, "kick_remote_status_refresh", lambda self: None)
    monkeypatch.setattr(launcher.LauncherApp, "refresh_programs_clicked", lambda self: None)
    try:
        root = launcher.Tk()
    except launcher.TclError as exc:
        pytest.skip(f"Tk runtime unavailable in this execution context: {exc}")
    root.withdraw()
    try:
        app = launcher.LauncherApp(root)
        app.sync_navigation(False, set())
        assert app.geometry_mcp_button.master.master is app.tab_frames["geometry_mcp"]
        assert app.geometry_canvas.master.master.master is app.tab_frames["geometry"]
        assert app.tab_buttons["geometry_mcp"].winfo_manager() == ""
        app.show_tab("geometry_mcp")
        assert app.active_tab == "home"
        assert app.geometry_mcp_button.cget("state") == "disabled"
        assert "http://127.0.0.1:7010/mcp/geometry" in app.geometry_mcp_text.get("1.0", "end")
        assert "Streamable HTTP" in app.geometry_mcp_text.get("1.0", "end")
        assert app.research_mcp_button.cget("state") == "disabled"
        assert app.brand_mark is not None
        assert app.brand_mark.width() == 60
        assert app.tab_buttons["runtime"].winfo_manager() == ""
        assert app.tab_buttons["programs"].winfo_manager() == "pack"
        assert [name for name, _ in launcher.NAV_TABS if name in app.visible_tab_ids] == [
            "home", "programs", "library", "geometry", "advanced"]
        assert app.tab_buttons["advanced"].cget("text") == "Settings"
        assert app.storage_button.master.master.master is app.tab_frames["advanced"]
        assert app.download_map_detail_button.master.master.master is app.tab_frames["runtime"]
        assert not hasattr(app, "open_hosted_self_host_button")
        assert not hasattr(app, "open_advanced_hosted_account_button")
        assert set(app.pack_filter_buttons) == {"cloud", "installed"}
        app.show_tab("runtime")
        assert app.active_tab == "home"
        app.sync_navigation(True, {"daedalmap-research-mcp"})
        assert app.tab_buttons["runtime"].winfo_manager() == "pack"
        assert app.tab_buttons["research"].winfo_manager() == "pack"
        assert app.research_mcp_button.cget("text") == "Start Research MCP"
        assert app.research_mcp_button.cget("state") == "normal"
        assert "http://127.0.0.1:7011/mcp/research" in app.research_status_text.get("1.0", "end")
        assert "Open Explore Map" not in app.research_status_text.get("1.0", "end")
        app.program_processes["daedalmap-research-mcp"] = SimpleNamespace(poll=lambda: None)
        app.sync_navigation(False, {"daedalmap-research-mcp", "daedalmap-geometry-mcp"})
        assert app.tab_buttons["geometry_mcp"].winfo_manager() == "pack"
        assert app.research_mcp_button.cget("text") == "Stop Research MCP"
        assert app.geometry_mcp_button.cget("state") == "normal"
        assert app.geometry_mcp_button.cget("text") == "Start Geometry MCP"
        app.show_tab("geometry_mcp")
        assert app.active_tab == "geometry_mcp"
        app.program_processes.clear()
        app.sync_navigation(False, set())
        assert app.active_tab == "home"
        app.apply_remote_status(False, "", "HTTP Error 404")
        assert app.online_var.get() == "Online"
        assert app.install_button.cget("state") == "disabled"
        assert app.get_mapping_button.cget("state") == "disabled"
        assert app.install_button_var.get() == "Install"
        mapping_actions = app.mapping_card.winfo_children()[-1].winfo_children()
        assert [(button.getvar(button.cget("textvariable")) if button.cget("textvariable") else button.cget("text"))
                for button in mapping_actions if button.winfo_manager()] == ["Install", "Open Map", "About"]
        app.refresh_installed_label()
        assert app.install_button_var.get() == "Install"
        app.apply_remote_status(True, "1.0", "")
        assert app.get_mapping_button.cget("state") == "normal"
        assert app.install_button_var.get() == "Install"
        mapping_dir = tmp_path / "mapping"
        mapping_dir.mkdir()
        (mapping_dir / "app.py").write_text("", encoding="utf-8")
        monkeypatch.setattr(launcher, "load_state", lambda: launcher.EngineState(version="1.0", install_dir=str(mapping_dir)))
        app.apply_remote_status(True, "1.0", "")
        assert app.get_mapping_button.cget("state") == "disabled"
        assert app.install_button_var.get() == "Installed"
        assert any("Build and install Explore Map from local source" in child.cget("text")
                   for child in app.mapping_card.winfo_children() if isinstance(child, launcher.Label)
                   and not child.cget("textvariable"))
        assert "programs" in app.tab_buttons
        app.show_tab("programs")
        assert app.tab_frames["programs"].winfo_manager() == "pack"
        app.available_programs = [launcher.AvailableProgram(
            "daedalmap-research-mcp", "Research MCP", "1.0.1", "https://example.test/manifest.json",
            "https://example.test/research.zip", "a" * 64, 1024,
            "Query local data with Research MCP.",
        )]
        app.update_program_list()
        assert len(app.program_cards.winfo_children()) == 2
        assert app.mapping_card.grid_info()["column"] == 0
        assert next(child for child in app.program_cards.winfo_children() if child is not app.mapping_card).grid_info()["column"] == 1
        assert app.program_action_buttons[0].getvar(app.program_action_buttons[0].cget("textvariable")) == "Install"
        research_card = next(child for child in app.program_cards.winfo_children() if child is not app.mapping_card)
        research_actions = research_card.winfo_children()[-1].winfo_children()
        assert [(button.getvar(button.cget("textvariable")) if button.cget("textvariable") else button.cget("text"))
                for button in research_actions if button.winfo_manager()] == ["Install", "About"]
        assert research_actions[1].cget("state") == "disabled"
        historical = launcher.AvailableProgram(
            "daedalmap-historical-version-generator", "Historical Version Generator", "1.0.3",
            "https://example.test/historical-manifest.json", "https://example.test/historical.zip",
            "b" * 64, 1024, "Rebuild older geometry releases.",
        )
        app.available_programs.append(historical)
        app.update_program_list()
        historical_card = app.program_cards.winfo_children()[-1]
        app._reflow_card_grid(app.program_cards, 1300)
        assert historical_card.grid_info()["column"] == 2
        app._reflow_card_grid(app.program_cards, 850)
        assert historical_card.grid_info()["column"] == 0
        assert historical_card.grid_info()["row"] == 1
        about_button = next(button for button in historical_card.winfo_children()[-1].winfo_children()
                            if button.cget("text") == "About")
        about_button.invoke()
        dialog = app.historical_about_dialog
        assert dialog.winfo_exists()
        assert dialog.cget("bg") == launcher.THEME["colors"]["app_bg"]
        labels = [widget.cget("text") for widget in dialog.winfo_children()[0].winfo_children()
                  if isinstance(widget, launcher.Label)]
        assert "Historical Version Generator" in labels
        assert any("back in time" in label and "geometry data pack" in label for label in labels)
        assert any("current pack download alone is not enough" in label and "changelog" in label for label in labels)
        about_button.invoke()
        assert app.historical_about_dialog is dialog
        dialog.destroy()
        pack = launcher.AvailablePack("cejst", "20260816.1", "cejst-lite",
                                      source_name="Climate and Economic Justice Screening Tool",
                                      description="Disadvantaged community classification.")
        app.available_packs = [pack]
        app.update_pack_list()
        assert len(app.pack_cards.winfo_children()) == 1
        assert app.pack_action_buttons[0].getvar(app.pack_action_buttons[0].cget("textvariable")) == "Install"
        opened = []
        monkeypatch.setattr(launcher.webbrowser, "open", opened.append)
        app.open_pack_page(pack)
        assert opened == ["https://www.daedalmap.com/packs/cejst"]
        app.show_tab("library")
        root.attributes("-alpha", 0.0)
        root.deiconify()
        root.update()
        root.update_idletasks()
        assert app.pack_canvas.pack_info()["fill"] == "both"
        assert app.pack_canvas.pack_info()["expand"] == 1
        assert app.pack_canvas.winfo_height() > 250
        app.pack_search_var.set("floods")
        assert len(app.pack_action_buttons) == 0
        app.pack_search_var.set("")
        assert len(app.pack_action_buttons) == 1
        app.set_busy(True, map_tiles_download=True)
        assert app.cancel_map_detail_button.winfo_manager() == "pack"
        app.cancel_map_detail_clicked()
        assert app.map_detail_cancel_event.is_set()
        assert app.cancel_map_detail_button.cget("state") == "disabled"
        app.set_busy(False)
        assert app.cancel_map_detail_button.winfo_manager() == ""
        geometry_items = [
            {"packageId": package_id, "version": "1.2.5", "tier": "lite", "size": 48_000_000}
            for package_id in ("aus-lite", "bra-lite")
        ]
        app.available_geometry = geometry_items
        geometry_groups = [
            {"title": title, "unit": unit, "section": "countries", "families": families,
             "tiers": {"lite": item}}
            for title, unit, families, item in (
                ("Australia", "AUS", ["Admin", "Statistical", "Local government", "Cities", "Metro"], geometry_items[0]),
                ("Brazil", "BRA", ["Admin", "Neighborhoods"], geometry_items[1]),
            )
        ]
        monkeypatch.setattr(launcher, "group_geometry_releases", lambda *_args: geometry_groups)
        monkeypatch.setattr(launcher, "installed_geometry", lambda *_args: {})
        app.show_tab("geometry")
        app.update_geometry_list()
        root.update_idletasks()
        cards = app.geometry_cards.winfo_children()
        assert len(cards) == 2
        lite_buttons = [card.winfo_children()[-1].winfo_children()[0].winfo_children()[0]
                        for card in cards]
        assert lite_buttons[0].winfo_rooty() == lite_buttons[1].winfo_rooty()
        monkeypatch.setattr(launcher, "installed_geometry", lambda *_args: {
            "aus-lite": {"version": "1.2.4", "tier": "lite", "countryCode": "AUS"}})
        app.set_geometry_filter("installed")
        installed_card = app.geometry_cards.winfo_children()[0]
        installed_button = installed_card.winfo_children()[-1].winfo_children()[0].winfo_children()[0]
        assert installed_button.cget("text") == "Update Lite"
        app.available_programs = [launcher.AvailableProgram(
            f"sample-{index}", f"Sample {index}", "1.0", "", "", "", 0)
            for index in range(15)]
        app.show_tab("programs")
        app.update_program_list()
        root.update()
        assert app.program_canvas.bbox("all")[3] > app.program_canvas.winfo_height()
        app.program_canvas.yview_moveto(1)
        root.update_idletasks()
        assert app.import_program_button.winfo_rooty() >= app.program_canvas.winfo_rooty()
        assert (app.import_program_button.winfo_rooty() + app.import_program_button.winfo_height()
                <= app.program_canvas.winfo_rooty() + app.program_canvas.winfo_height())
    finally:
        root.destroy()


def test_first_boot_keeps_user_data_and_creates_shared_catalog(tmp_path, monkeypatch) -> None:
    app = tmp_path / "app"
    runtime = tmp_path / "runtime"
    values = {
        "APP_ROOT": app,
        "ENGINE_ROOT": app / "engine",
        "PROGRAMS_ROOT": app / "programs",
        "PROGRAMS_STATE_PATH": app / "programs.json",
        "CACHE_ROOT": app / "cache",
        "RUNTIME_CONFIG_PATH": app / "runtime_config.json",
        "RUNTIME_ROOT": runtime,
        "LOGS_ROOT": runtime / "logs",
        "RUNTIME_CONFIG_DIR": runtime / "config",
        "RUNTIME_STATE_DIR": runtime / "state",
        "RUNTIME_CACHE_DIR": runtime / "cache",
        "RUNTIME_LOG_DIR": runtime / "logs",
        "RUNTIME_DATA_ROOT": runtime / "data",
        "RUNTIME_PACKS_ROOT": runtime / "packs",
    }
    for name, value in values.items():
        monkeypatch.setattr(launcher, name, value)
    source = runtime / "data" / "incoming" / "user-pack.zip"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"user data")

    launcher.initialize_runtime_root()

    assert source.read_bytes() == b"user data"
    catalog = launcher.json.loads((runtime / "data" / "catalog.json").read_text(encoding="utf-8"))
    assert catalog["sources"] == []
    assert catalog["generated_by"] == "local_runtime"
    assert not (runtime / "data" / ".daedal_runtime_root.json").exists()
    assert launcher.load_pack_state()["installed_packs"] == []
    monkeypatch.setenv("INSTALL_MODE", "cloud")
    runtime_env = launcher.build_runtime_env()
    assert runtime_env["INSTALL_MODE"] == "local"
    assert runtime_env["RUNTIME_MODE"] == "local"
    assert runtime_env["DATA_ROOT"] == str(runtime / "data")
    assert runtime_env["PACKS_ROOT"] == str(runtime / "packs")
    assert runtime_env["STATE_DIR"] == str(runtime / "state")


def test_old_online_map_preference_uses_offline_map(tmp_path, monkeypatch) -> None:
    config = tmp_path / "map_tiles.json"
    config.write_text('{"mode":"remote","remote_style_url":"https://example.test/style.json"}', encoding="utf-8")
    monkeypatch.setattr(launcher, "MAP_TILES_CONFIG_PATH", config)
    assert launcher.load_map_tile_preferences()["mode"] == "offline_basic"
    with pytest.raises(launcher.LauncherError):
        launcher.save_map_tile_preferences("remote")
