from __future__ import annotations

import hashlib
import json
import sys
import zipfile
from pathlib import Path
from shutil import copy2
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from geometry_store import (GeometryStoreError, geometry_info_url, group_geometry_releases, installed_geometry,
                            install_geometry_zip, releases_from_storefront)
import launcher


def _package(root: Path, version: str) -> tuple[Path, dict]:
    member = f"identity {version}".encode()
    sidecar = {
        "profile": "downloadable_geometry_package", "package_id": "example-reference-lite",
        "version": version, "package_tier": "lite", "files": [
            {"archive_path": "reference/identity.parquet", "size_bytes": len(member),
             "sha256": hashlib.sha256(member).hexdigest()}
        ],
    }
    archive = root / f"example-{version}.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("geometry.json", json.dumps(sidecar))
        output.writestr("reference/identity.parquet", member)
    release = {"packageId": sidecar["package_id"], "version": version,
               "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(), "size": archive.stat().st_size}
    return archive, release


def test_geometry_install_update_and_failed_hash_preserves_previous(tmp_path: Path) -> None:
    data_root = tmp_path / "runtime" / "data"
    first, release_one = _package(tmp_path, "1.0.0")
    second, release_two = _package(tmp_path, "1.0.1")
    install_geometry_zip(first, data_root, release_one)
    install_geometry_zip(second, data_root, release_two)
    assert installed_geometry(data_root)["example-reference-lite"]["version"] == "1.0.1"
    assert len(list(data_root.rglob("geometry.json"))) == 1
    with pytest.raises(GeometryStoreError, match="SHA-256"):
        install_geometry_zip(first, data_root, {**release_one, "sha256": "0" * 64})
    assert installed_geometry(data_root)["example-reference-lite"]["version"] == "1.0.1"


def test_geometry_store_reads_website_category_and_ignores_unpinned_entries() -> None:
    pinned = {"packageId": "example-reference-lite", "version": "1.0.0", "size": 100,
              "sha256": "a" * 64,
              "artifactUrl": "https://downloads.daedalmap.com/downloadable/geometry/DEU/example-reference-lite/stable/1.0.0/example.zip"}
    assert releases_from_storefront({"categories": {"geometry": [pinned, {**pinned, "sha256": ""}]}}) == [pinned]


def test_launcher_geometry_download_uses_published_manifest_and_shared_root(tmp_path: Path, monkeypatch) -> None:
    archive, release = _package(tmp_path, "1.0.0")
    url = "https://downloads.daedalmap.com/downloadable/geometry/TST/example-reference-lite/stable/1.0.0/example.zip"
    item = {**release, "title": "Example Geometry Lite", "artifactUrl": url}
    manifest = {"package_id": release["packageId"], "version": release["version"],
                "artifact": {"download_url": url, "sha256": release["sha256"], "size_bytes": release["size"]}}
    data_root = tmp_path / "runtime" / "data"
    monkeypatch.setattr(launcher, "RUNTIME_DATA_ROOT", data_root)
    monkeypatch.setattr(launcher, "RUNTIME_CACHE_DIR", tmp_path / "runtime" / "cache")
    monkeypatch.setattr(launcher, "initialize_runtime_root", lambda _log: data_root.mkdir(parents=True, exist_ok=True))
    monkeypatch.setattr(launcher, "read_json_url", lambda *_args, **_kwargs: manifest)
    monkeypatch.setattr(launcher, "download_file", lambda _url, dest, **_kwargs: copy2(archive, dest))
    app = object.__new__(launcher.LauncherApp)
    app.root = SimpleNamespace(after=lambda _delay, fn: fn())
    app.append_log = lambda _message: None
    app.invalidate_size_cache = lambda: None
    app.update_geometry_list = lambda: None
    app._install_geometry_work(item)
    assert installed_geometry(data_root)["example-reference-lite"]["version"] == "1.0.0"


def test_geometry_editions_group_into_country_and_bridge_cards() -> None:
    releases = [
        {"packageId": f"germany-{tier}", "title": f"Germany Geometry {tier.title()}",
         "tier": tier, "countryCode": "DEU", "geometrySection": "countries"}
        for tier in ("lite", "display", "full")
    ] + [{"packageId": "overture-bridge", "title": "Overture GERS Bridge", "tier": "lite",
          "releaseUnitId": "BRIDGES", "geometrySection": "bridges"}]
    families = [{"country_code": "DEU", "label": "Germany", "profile_id": "germany",
                 "geometry_families": [{"family_id": "administrative", "label": "Admin"}]}]
    groups = group_geometry_releases(releases, families)
    assert [(group["title"], sorted(group["tiers"])) for group in groups] == [
        ("Germany", ["display", "full", "lite"]), ("Overture GERS Bridge", ["lite"])]
    assert groups[0]["families"] == ["Admin"]
    assert geometry_info_url(groups[0]) == "https://www.daedalmap.com/geometry/germany"
    assert geometry_info_url(groups[1]) == "https://www.daedalmap.com/docs/external-bridges"


def test_geometry_review_requires_confirmation_before_install(tmp_path: Path, monkeypatch) -> None:
    item = {"packageId": "germany-reference-lite", "title": "Germany Geometry Lite",
            "version": "1.3.0", "tier": "lite", "size": 100,
            "sha256": "a" * 64,
            "artifactUrl": "https://downloads.daedalmap.com/downloadable/geometry/DEU/germany-reference-lite/stable/1.3.0/germany.zip"}
    monkeypatch.setattr(launcher, "RUNTIME_DATA_ROOT", tmp_path)
    calls = []
    app = object.__new__(launcher.LauncherApp)
    app.install_selected_geometry_clicked = lambda selected: calls.append(selected)
    monkeypatch.setattr(launcher.messagebox, "askyesno", lambda title, details, **_kwargs: False)
    app.review_selected_geometry_clicked(item)
    assert calls == []
    monkeypatch.setattr(launcher.messagebox, "askyesno", lambda title, details, **kwargs: (
        calls.append((title, details, kwargs)) or True))
    app.review_selected_geometry_clicked(item)
    assert calls[0][0] == "Geometry download details"
    assert "SHA-256" in calls[0][1] and "Library:" in calls[0][1]
    assert calls[0][2]["confirm_label"] == "Start download"
    assert calls[1] is item


def test_geometry_tab_renders_one_country_card_with_three_editions(tmp_path: Path, monkeypatch) -> None:
    try:
        root = launcher.Tk()
    except launcher.TclError as exc:
        pytest.skip(f"Tk runtime unavailable: {exc}")
    root.withdraw()
    try:
        monkeypatch.setattr(launcher, "RUNTIME_DATA_ROOT", tmp_path / "data")
        app = object.__new__(launcher.LauncherApp)
        app.root = root
        app.tab_frames = {}
        app.geometry_search_var = launcher.StringVar(root, value="")
        app.geometry_filter = "cloud"
        app.geometry_section = "all"
        app.available_geometry = [
            {"packageId": f"germany-{tier}", "title": f"Germany Geometry {tier.title()}",
             "tier": tier, "countryCode": "DEU", "geometrySection": "countries",
             "version": "1.3.0", "size": 100, "sha256": "a" * 64,
             "artifactUrl": f"https://downloads.daedalmap.com/downloadable/geometry/DEU/germany-{tier}.zip"}
            for tier in ("lite", "display", "full")
        ]
        app.available_geometry_families = [{"country_code": "DEU", "label": "Germany", "profile_id": "germany",
                                            "geometry_families": [{"label": "Admin"}]}]
        app.geometry_action_buttons = []
        app.worker = None
        app._build_geometry_tab(root, 8)
        assert len(app.geometry_cards.winfo_children()) == 1
        assert [button.cget("text") for button in app.geometry_action_buttons] == ["Lite", "Display", "Detail"]
        card = app.geometry_cards.winfo_children()[0]
        family_slot, actions = card.winfo_children()[-2:]
        info_button = family_slot.winfo_children()[-1]
        assert info_button.cget("text") == "More info"
        assert actions.winfo_children()
        opened = []
        monkeypatch.setattr(launcher.webbrowser, "open", opened.append)
        info_button.invoke()
        assert opened == ["https://www.daedalmap.com/geometry/germany"]
    finally:
        root.destroy()
