"""Managed, shared Python environment for downloaded local programs."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Callable

PYTHON_VERSION = "3.13.16"
STANDALONE_CLI_PRODUCTS = {"daedalmap-historical-version-generator"}


class RuntimeSetupError(RuntimeError):
    pass


def bundled_uv_executable() -> Path:
    override = os.environ.get("DAEDALMAP_UV_EXE", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    owner = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent
    packaged = owner / "uv.exe"
    if packaged.is_file() or getattr(sys, "frozen", False):
        return packaged
    return Path(__file__).resolve().parents[2] / ".scratch" / "uv-bootstrap" / "uv.exe"


def _run(command: list[str], *, env: dict[str, str], log: Callable[[str], None], cwd: Path) -> None:
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    process = subprocess.Popen(command, cwd=str(cwd), env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                               errors="replace", creationflags=creationflags)
    assert process.stdout is not None
    tail: list[str] = []
    for raw in process.stdout:
        line = raw.strip()
        if line:
            log(line)
            tail.append(line)
            tail = tail[-8:]
    code = process.wait()
    if code:
        raise RuntimeSetupError(f"Command exited {code}: {' '.join(tail[-3:])}")


def _write_json_atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.with_suffix(path.suffix + ".tmp")
    staging.write_text(json.dumps(value, indent=2), encoding="utf-8")
    os.replace(staging, path)


def _selected_programs(programs: list[dict]) -> tuple[list[dict], list[Path], str]:
    # The registry is append ordered: the newest installed release of each tool wins.
    latest = {str(item.get("product")): item for item in programs
              if str(item.get("product")) not in STANDALONE_CLI_PRODUCTS}
    selected = [latest[product] for product in sorted(latest)]
    if not selected:
        raise RuntimeSetupError("Install a program before preparing its Python runtime")
    digest = hashlib.sha256(PYTHON_VERSION.encode("ascii"))
    requirements: list[Path] = []
    for entry in selected:
        install_dir = Path(str(entry.get("install_dir") or ""))
        if not install_dir.is_dir():
            raise RuntimeSetupError(f"Installed program folder is missing: {install_dir}")
        digest.update(str(entry.get("product")).encode("utf-8"))
        digest.update(str(entry.get("version")).encode("utf-8"))
        digest.update(str(entry.get("sha256") or entry.get("source_commit") or "").encode("utf-8"))
        req = install_dir / "requirements.txt"
        if not req.is_file():
            raise RuntimeSetupError(f"Installed program is missing requirements.txt: {install_dir}")
        digest.update(req.read_bytes())
        requirements.append(req)
    return selected, requirements, digest.hexdigest()[:20]


def prepare_shared_environment(app_root: Path, programs: list[dict],
                               log: Callable[[str], None]) -> Path:
    """Build a new union environment, smoke it, then activate it atomically."""
    selected, requirements, key = _selected_programs(programs)
    test_python = os.environ.get("DAEDALMAP_MAPPING_TEST_PYTHON", "").strip()
    if test_python and os.environ.get("DAEDALMAP_LAUNCHER_DEV", "").strip() == "1":
        python = Path(test_python).expanduser().resolve()
        if not python.is_file():
            raise RuntimeSetupError(f"Local test Python is missing: {python}")
        log(f"Using existing local test Python: {python}")
        return python
    uv = bundled_uv_executable()
    if not uv.is_file():
        raise RuntimeSetupError(
            f"Launcher runtime helper is missing: {uv}. For a source checkout, run "
            "python county-map-private/wrapper/bootstrap_source_uv.py"
        )
    app_root = Path(app_root).resolve()
    environments = app_root / "environments"
    locks = app_root / "locks"
    environments.mkdir(parents=True, exist_ok=True)
    locks.mkdir(parents=True, exist_ok=True)
    target = environments / key
    python = target / "Scripts" / "python.exe"
    active = app_root / "active_environment.json"
    if python.is_file() and (target / "environment.json").is_file():
        _write_json_atomic(active, {"key": key, "python": str(python), "programs": selected})
        return python

    env = os.environ.copy()
    env["UV_PYTHON_INSTALL_DIR"] = str(app_root / "runtimes" / "python")
    env["UV_CACHE_DIR"] = str(app_root / "cache" / "uv")
    env["UV_PYTHON_BIN_DIR"] = str(app_root / "runtimes" / "bin")
    env["UV_NO_PROGRESS"] = "1"
    lock = locks / f"{key}.txt"
    if requirements and not lock.is_file():
        pending = locks / f".{key}-{uuid.uuid4().hex}.tmp"
        try:
            log("Resolving the shared program requirements")
            _run([str(uv), "pip", "compile", *(str(req) for req in requirements),
                  "--python-version", "3.13", "--output-file", str(pending)],
                 env=env, log=log, cwd=app_root)
            os.replace(pending, lock)
        finally:
            if pending.exists():
                pending.unlink()

    stage = environments / f".build-{key}-{uuid.uuid4().hex}"
    stage_python = stage / "Scripts" / "python.exe"
    try:
        log(f"Preparing managed Python {PYTHON_VERSION}")
        _run([str(uv), "venv", str(stage), "--python", PYTHON_VERSION,
              "--managed-python"], env=env, log=log, cwd=app_root)
        if requirements:
            log("Installing the shared program dependencies")
            _run([str(uv), "pip", "sync", "--python", str(stage_python), str(lock)],
                 env=env, log=log, cwd=app_root)
        for entry in selected:
            install_dir = Path(entry["install_dir"])
            smoke = install_dir / "smoke_test.py"
            if smoke.is_file():
                log(f"Checking {entry['product']} v{entry['version']}")
                _run([str(stage_python), str(smoke)], env=env, log=log, cwd=install_dir)
            elif (install_dir / "historical_version_generator.py").is_file():
                _run([str(stage_python), str(install_dir / "historical_version_generator.py"), "--help"],
                     env=env, log=log, cwd=install_dir)
        _write_json_atomic(stage / "environment.json", {"key": key, "python_version": PYTHON_VERSION,
                                                          "programs": selected})
        if target.exists():
            raise RuntimeSetupError(f"Environment folder already exists without a valid marker: {target}")
        os.replace(stage, target)
        _write_json_atomic(active, {"key": key, "python": str(python), "programs": selected})
        log(f"Shared environment ready: {python}")
        return python
    finally:
        if stage.exists() and stage.resolve().is_relative_to(environments.resolve()):
            shutil.rmtree(stage)
