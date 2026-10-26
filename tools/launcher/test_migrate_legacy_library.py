from __future__ import annotations

import json
from pathlib import Path

import pytest

from migrate_legacy_library import migrate


def test_migration_moves_installs_into_portable_default_and_preserves_mode(tmp_path: Path) -> None:
    source = tmp_path / "AppData" / "DaedalMapLauncher"
    target = tmp_path / "Portable" / "launcher-data"
    pack = source / "runtime" / "packs" / "earthquakes" / "data"
    pack.mkdir(parents=True)
    (pack / "catalog.json").write_text("{}", encoding="utf-8")
    data = source / "runtime" / "data"
    data.mkdir()
    (data / "catalog.json").write_text(json.dumps({"sources": [{"runtime_data_root": str(pack)}]}), encoding="utf-8")
    state = source / "runtime" / "state"
    state.mkdir()
    (state / "pack_state.json").write_text(json.dumps({"installed_packs": [
        {"pack_id": "earthquakes", "catalog_path": str(pack / "catalog.json"),
         "data_root": str(pack)}]}), encoding="utf-8")
    engine = source / "engine" / "1.0"
    engine.mkdir(parents=True)
    (engine / "app.py").write_text("", encoding="utf-8")
    (source / "state.json").write_text(json.dumps({"install_dir": str(engine), "runtime_pid": 123}), encoding="utf-8")
    program = source / "programs" / "research"
    program.mkdir(parents=True)
    (source / "programs.json").write_text(json.dumps({"installed_programs": [
        {"product": "daedalmap-research-mcp", "install_dir": str(program)}]}), encoding="utf-8")
    target.mkdir(parents=True)
    (target / "launcher_mode.json").write_text('{"mode":"local_custom"}', encoding="utf-8")
    (target / "runtime" / "data").mkdir(parents=True)
    (target / "runtime" / "data" / "catalog.json").write_text(
        (data / "catalog.json").read_text(encoding="utf-8"), encoding="utf-8")

    count, removed = migrate(source, target)

    assert (count, removed) == (1, True)
    assert not source.exists()
    assert json.loads((target / "launcher_mode.json").read_text())["mode"] == "local_custom"
    assert json.loads((target / "runtime_config.json").read_text())["local_wrapper"]["storage_root"] == str(target / "runtime")
    assert json.loads((target / "state.json").read_text())["runtime_pid"] == 0
    assert json.loads((target / "state.json").read_text())["install_dir"] == str(target / "engine" / "1.0")
    assert json.loads((target / "programs.json").read_text())["installed_programs"][0]["install_dir"] == str(target / "programs" / "research")
    migrated = json.loads((target / "runtime" / "state" / "pack_state.json").read_text())
    assert migrated["installed_packs"][0]["catalog_path"] == str(target / "runtime" / "packs" / "earthquakes" / "data" / "catalog.json")
    assert json.loads((target / "runtime" / "data" / "catalog.json").read_text())["sources"][0]["runtime_data_root"] == str(target / "runtime" / "packs" / "earthquakes" / "data")


def test_migration_keeps_unique_destination_data(tmp_path: Path) -> None:
    source = tmp_path / "AppData" / "DaedalMapLauncher"
    state = source / "runtime" / "state"
    state.mkdir(parents=True)
    (state / "pack_state.json").write_text('{"installed_packs":[]}', encoding="utf-8")
    (source / "runtime" / "data").mkdir()
    target = tmp_path / "Portable" / "launcher-data"
    data = target / "runtime" / "data"
    data.mkdir(parents=True)
    (data / "my-data.parquet").write_bytes(b"unique")

    with pytest.raises(ValueError, match="absent or different"):
        migrate(source, target)

    assert source.exists()
    assert (data / "my-data.parquet").read_bytes() == b"unique"
