"""Move an older AppData launcher library into a fresh portable launcher folder.

Run this with the launchers closed, under the Windows account that owns the old
library. The old folder is removed only after the copy and path checks succeed.
"""

from __future__ import annotations

import argparse
import filecmp
import json
import os
import shutil
import sys
import uuid
from pathlib import Path


def _within_or_equal(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def _check_roots(source: Path, target: Path) -> None:
    if source.name != "DaedalMapLauncher" or target.name != "launcher-data":
        raise ValueError("Expected an AppData DaedalMapLauncher source and a launcher-data destination")
    if _within_or_equal(source, target) or _within_or_equal(target, source):
        raise ValueError("Source and destination must be separate folders")
    if not source.is_dir() or not target.is_dir():
        raise ValueError("Both launcher library folders must already exist")
    if source.is_symlink() or target.is_symlink():
        raise ValueError("Library folders must not be links")
    if not (source / "runtime" / "state" / "pack_state.json").is_file():
        raise ValueError("The old library has no installed pack state to migrate")
    if any((target / name).exists() for name in ("state.json", "programs.json")) or (
            target / "runtime" / "state" / "pack_state.json").exists():
        raise ValueError("The destination already has installs; migration requires a fresh library")


def _check_existing_data(source: Path, target: Path) -> None:
    """Allow a partial manual copy only when it contains no unique data."""
    existing = target / "runtime" / "data"
    if not existing.exists():
        return
    source_data = source / "runtime" / "data"
    for relative, file in _files(existing).items():
        original = source_data / relative
        if original.is_file() and filecmp.cmp(file, original, shallow=False):
            continue
        if relative == Path("catalog.json"):
            catalog = json.loads(file.read_text(encoding="utf-8"))
            if not catalog.get("sources") and not catalog.get("packs"):
                continue
        raise ValueError(f"The destination contains data absent or different in AppData: {file}")


def _files(root: Path) -> dict[Path, Path]:
    result: dict[Path, Path] = {}
    def fail(error: OSError) -> None:
        raise error

    for parent, directories, names in os.walk(root, followlinks=False, onerror=fail):
        for name in directories + names:
            item = Path(parent) / name
            if item.is_symlink() or (hasattr(item, "is_junction") and item.is_junction()):
                raise ValueError(f"Library contains a link or junction: {item}")
        for name in names:
            item = Path(parent) / name
            result[item.relative_to(root)] = item
    return result


def _rewrite(value: object, old: str, new: str, old_uri: str, new_uri: str) -> object:
    if isinstance(value, str):
        if value.casefold().startswith(old.casefold()):
            return new + value[len(old):]
        if value.casefold().startswith(old_uri.casefold()):
            return new_uri + value[len(old_uri):]
        return value
    if isinstance(value, list):
        return [_rewrite(item, old, new, old_uri, new_uri) for item in value]
    if isinstance(value, dict):
        return {key: _rewrite(item, old, new, old_uri, new_uri) for key, item in value.items()}
    return value


def _relocate_json(path: Path, source: Path, target: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    payload = _rewrite(payload, str(source), str(target), source.as_uri(), target.as_uri())
    if path.name == "state.json" and isinstance(payload, dict):
        payload["runtime_pid"] = 0
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _portable_config(target: Path) -> dict:
    runtime = target / "runtime"
    return {
        "install_mode": "local", "runtime_mode": "local",
        "paths": {
            "config_dir": str(runtime / "config"), "state_dir": str(runtime / "state"),
            "cache_dir": str(runtime / "cache"), "log_dir": str(runtime / "logs"),
            "data_root": str(runtime / "data"), "packs_root": str(runtime / "packs"),
            "runtime_config_path": str(target / "runtime_config.json"),
        },
        "local_wrapper": {"storage_root": str(runtime)},
    }


def migrate(source: Path, target: Path) -> tuple[int, bool]:
    source, target = source.resolve(), target.resolve()
    _check_roots(source, target)
    source_files = _files(source)
    _check_existing_data(source, target)
    stage = target.parent / f".{target.name}-migration-{uuid.uuid4().hex}"
    backup = target.parent / f".{target.name}-before-migration-{uuid.uuid4().hex}"
    if not stage.resolve().is_relative_to(target.parent.resolve()) or not backup.resolve().is_relative_to(target.parent.resolve()):
        raise ValueError("Migration staging escaped the destination parent")
    switched = False
    try:
        shutil.copytree(source, stage)
        stage_files = _files(stage)
        if source_files.keys() != stage_files.keys() or any(
                not filecmp.cmp(path, stage_files[relative], shallow=False)
                for relative, path in source_files.items()):
            raise RuntimeError("The copied library failed verification; the old library was left intact")

        for relative in source_files:
            if relative.suffix.lower() != ".json":
                continue
            if len(relative.parts) == 1 or relative.parts[:2] in {
                    ("runtime", "state"), ("runtime", "config"), ("runtime", "data")}:
                _relocate_json(stage / relative, source, target)
        (stage / "runtime_config.json").write_text(
            json.dumps(_portable_config(target), indent=2), encoding="utf-8")
        mode = target / "launcher_mode.json"
        if mode.is_file():
            shutil.copy2(mode, stage / "launcher_mode.json")
        state = stage / "runtime" / "state" / "pack_state.json"
        packs = json.loads(state.read_text(encoding="utf-8"))["installed_packs"]
        for entry in packs:
            if not Path(entry["catalog_path"]).is_relative_to(target) or not (
                    stage / Path(entry["catalog_path"]).relative_to(target)).is_file():
                raise RuntimeError(f"Migrated pack is incomplete: {entry.get('pack_id')}")
        engine_state = stage / "state.json"
        if engine_state.is_file():
            installed = json.loads(engine_state.read_text(encoding="utf-8")).get("install_dir")
            if installed and not (stage / Path(installed).relative_to(target) / "app.py").is_file():
                raise RuntimeError("Migrated Mapping install is incomplete")
        programs_state = stage / "programs.json"
        if programs_state.is_file():
            for entry in json.loads(programs_state.read_text(encoding="utf-8")).get("installed_programs", []):
                installed = Path(entry["install_dir"])
                if not installed.is_relative_to(target) or not (stage / installed.relative_to(target)).is_dir():
                    raise RuntimeError(f"Migrated program is incomplete: {entry.get('product')}")

        os.replace(target, backup)
        try:
            os.replace(stage, target)
            switched = True
        except Exception:
            os.replace(backup, target)
            raise
        shutil.rmtree(backup)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    try:
        shutil.rmtree(source)
    except OSError as exc:
        print(f"Portable library is ready, but the old copy could not be removed: {exc}", file=sys.stderr)
        return len(packs), False
    return len(packs), switched


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    args = parser.parse_args()
    try:
        count, removed = migrate(args.source, args.target)
    except (OSError, ValueError, RuntimeError, shutil.Error) as exc:
        parser.exit(1, f"Migration stopped; the old library was left intact: {exc}\n")
    print(f"Moved {count} installed data packs to {args.target.resolve()}")
    if not removed:
        print("The old library remains; remove it after checking the portable launcher.")


if __name__ == "__main__":
    main()
