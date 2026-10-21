from __future__ import annotations

import json
import zipfile
from pathlib import Path

from mapmover.artifact_utils import sha256_file
from mapmover import local_catalog_discovery, pack_state
from mapmover.local_pack_catalog import build_local_pack_catalog


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_first_boot_discovers_complete_manual_source_without_admitting_it(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    monkeypatch.setattr(local_catalog_discovery, "DATA_ROOT", data_root)
    monkeypatch.setattr(local_catalog_discovery, "CATALOG_PATH", data_root / "catalog.json")
    assert local_catalog_discovery.ensure_empty_catalog()
    assert not local_catalog_discovery.ensure_empty_catalog()

    source_root = data_root / "global" / "sample"
    _write_json(source_root / "metadata.json", {
        "source_id": "sample", "pack_id": "sample", "source_name": "Sample",
        "runtime_primary_file": "data.parquet", "catalog_surfaces": ["explore"],
    })
    assert local_catalog_discovery.scan_unindexed_sources() == []
    (source_root / "data.parquet").write_bytes(b"sample")
    found = local_catalog_discovery.scan_unindexed_sources()
    assert [(item["source_id"], item["path"]) for item in found] == [("sample", "global/sample")]
    fragment = local_catalog_discovery.catalog_fragment_for_candidates("sample", found)
    assert fragment["sources"][0]["path"] == "global/sample"
    assert json.loads((data_root / "catalog.json").read_text())["sources"] == []


def test_downloadable_generator_keeps_nested_source_files(tmp_path) -> None:
    extract_root = tmp_path / "extracted"
    files = [
        ("data/sources/sample/metadata.json", "global/sample/metadata.json"),
        ("data/sources/sample/admin2/yearly.parquet", "global/sample/admin2/yearly.parquet"),
        ("data/sources/sample/admin2/metadata.json", "global/sample/admin2/metadata.json"),
    ]
    _write_json(extract_root / files[0][0], {
        "source_id": "sample", "pack_id": "sample", "runtime_primary_file": "admin2/yearly.parquet",
        "catalog_surfaces": ["explore"],
    })
    (extract_root / files[1][0]).parent.mkdir(parents=True, exist_ok=True)
    (extract_root / files[1][0]).write_bytes(b"nested parquet")
    _write_json(extract_root / files[2][0], {"source_id": "sample"})
    _write_json(extract_root / "data/manifest.json", {
        "pack_id": "sample", "source_ids": ["sample"],
        "files": [{"source_id": "sample", "archive_path": archive, "source_path": source}
                  for archive, source in files],
    })
    version = {"pack_id": "sample", "version": "1.0.0", "files": [
        {"path": archive, "sha256": sha256_file(extract_root / archive)} for archive, _ in files
    ]}
    catalog = build_local_pack_catalog(extract_root, tmp_path / "installed", version)
    assert catalog["sources"][0]["path"] == "global/sample"
    assert (tmp_path / "installed/global/sample/admin2/yearly.parquet").is_file()


def test_dropped_zip_is_detected_until_its_version_is_installed(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    monkeypatch.setattr(local_catalog_discovery, "DATA_ROOT", data_root)
    archive_path = data_root / "sample.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("data/manifest.json", json.dumps({
            "pack_id": "sample", "pack_version": "1.0.0", "source_ids": ["sample"],
        }))
    monkeypatch.setattr(pack_state, "load_pack_state", lambda: {"installed_packs": []})
    assert [item["path"] for item in local_catalog_discovery.scan_local_archives()] == ["sample.zip"]
    monkeypatch.setattr(pack_state, "load_pack_state", lambda: {
        "installed_packs": [{"pack_id": "sample", "version": "1.0.0"}],
    })
    assert local_catalog_discovery.scan_local_archives() == []
    assert len(local_catalog_discovery.scan_local_archives(include_installed=True)) == 1


def test_materialization_updates_catalog_without_losing_first_pack(tmp_path, monkeypatch) -> None:
    data_root = tmp_path / "active"
    data_root.mkdir()
    _write_json(data_root / "catalog.json", {
        "generated_by": "local_runtime", "sources": [], "packs": [],
    })
    monkeypatch.setattr(pack_state, "DATA_ROOT", data_root)
    monkeypatch.setattr(pack_state, "MANAGED_DATA_ROOT_MARKER", data_root / ".daedal_runtime_root.json")
    monkeypatch.setattr(pack_state, "RUNTIME_MODE", "local")
    monkeypatch.setattr(local_catalog_discovery, "DATA_ROOT", data_root)
    incoming = data_root / "incoming"
    incoming.mkdir()
    with zipfile.ZipFile(incoming / "later.zip", "w") as archive:
        archive.writestr("data/manifest.json", json.dumps({"pack_id": "later", "pack_version": "1.0.0"}))

    entries = []
    for pack_id in ("first", "second"):
        root = tmp_path / "packs" / pack_id / "data"
        source = {"source_id": pack_id, "pack_id": pack_id, "path": f"global/{pack_id}",
                  "scope": "global", "catalog_surfaces": ["explore"]}
        _write_json(root / source["path"] / "metadata.json", source)
        _write_json(root / "catalog.json", {"sources": [source], "packs": [{"pack_id": pack_id,
                    "source_ids": [pack_id]}]})
        entries.append({"pack_id": pack_id, "data_root": str(root),
                        "catalog_path": str(root / "catalog.json"), "source_ids": [pack_id]})

    state = {"catalog_mode": "managed_packs", "installed_packs": entries[:1], "active_pack_ids": ["first"]}
    assert pack_state.materialize_active_data_root({}, state)["materialized"]
    state["installed_packs"] = entries
    state["active_pack_ids"] = ["first", "second"]
    assert pack_state.materialize_active_data_root({}, state)["materialized"]
    catalog = json.loads((data_root / "catalog.json").read_text())
    assert {source["source_id"] for source in catalog["sources"]} == {"first", "second"}
    assert (data_root / "global/first/metadata.json").is_file()
    assert (data_root / "global/second/metadata.json").is_file()
    assert (data_root / "incoming/later.zip").is_file()
