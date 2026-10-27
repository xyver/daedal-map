"""Install the pinned Windows uv helper for a source launcher checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import uuid
import zipfile
from pathlib import Path
from typing import Callable

PIN_PATH = Path(__file__).resolve().parent / "uv_pin.json"


def install_uv(library_root: Path, *, wheel: Path | None = None,
               log: Callable[[str], None] = print) -> Path:
    """Install the checked release into one source launcher's library."""
    if os.name != "nt":
        raise RuntimeError("This launcher bootstrap currently targets Windows")

    pin = json.loads(PIN_PATH.read_text(encoding="utf-8"))
    version = str(pin["version"])
    library_root = Path(library_root).expanduser().resolve()
    bootstrap_root = library_root / "cache" / "uv-bootstrap"
    bootstrap_root.mkdir(parents=True, exist_ok=True)
    if wheel is None:
        downloads = bootstrap_root / "downloads"
        downloads.mkdir(exist_ok=True)
        result = subprocess.run([
            sys.executable, "-m", "pip", "download", "--only-binary=:all:",
            "--no-deps", f"uv=={version}", "--dest", str(downloads),
        ], capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError((result.stderr or result.stdout).strip() or "Could not download the pinned uv wheel")
        matches = list(downloads.glob(f"uv-{version}-*-win_amd64.whl"))
        if len(matches) != 1:
            raise RuntimeError(f"Expected one Windows uv {version} wheel; found {len(matches)}")
        wheel = matches[0]
    wheel = wheel.expanduser().resolve()
    if f"uv-{version}-" not in wheel.name or not wheel.name.endswith("-win_amd64.whl"):
        raise RuntimeError(f"Expected a Windows uv {version} wheel: {wheel}")
    if hashlib.sha256(wheel.read_bytes()).hexdigest() != pin["wheel_sha256"]:
        raise RuntimeError("uv wheel checksum does not match the pinned Windows build")

    target = library_root / "uv.exe"
    pending = library_root / f".uv-{uuid.uuid4().hex}.exe"
    try:
        with zipfile.ZipFile(wheel) as archive:
            members = [name for name in archive.namelist() if name.endswith(".data/scripts/uv.exe")]
            if len(members) != 1:
                raise RuntimeError("uv wheel has no unique uv.exe")
            with archive.open(members[0]) as source, pending.open("wb") as output:
                while block := source.read(1024 * 1024):
                    output.write(block)
        reported = subprocess.run([str(pending), "--version"], check=True,
                                  capture_output=True, text=True).stdout.strip()
        if reported.split(maxsplit=2)[:2] != ["uv", version]:
            raise RuntimeError(f"Unexpected uv version: {reported}")
        if hashlib.sha256(pending.read_bytes()).hexdigest() != pin["exe_sha256"]:
            raise RuntimeError("uv executable checksum does not match the pinned Windows build")
        pending.replace(target)
    finally:
        pending.unlink(missing_ok=True)
    log(f"Source launcher runtime helper ready: {target} ({reported})")
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, help="Use a previously downloaded uv wheel")
    parser.add_argument("--library-root", type=Path,
                        help="Launcher library folder; defaults to launcher-data beside this source launcher")
    args = parser.parse_args()
    library_root = args.library_root or Path(__file__).resolve().parent / "launcher-data"
    install_uv(library_root, wheel=args.wheel)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
