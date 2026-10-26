"""Install the pinned Windows uv helper for a source launcher checkout."""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import uuid
import zipfile
from pathlib import Path

UV_VERSION = "0.13.5"
UV_WHEEL_SHA256 = "1fb29fc46ba12033e2ef95a95ac79d649e71bbb1358419bbbcbd19d86026ef2f"
WORKSPACE = Path(__file__).resolve().parents[2]
BOOTSTRAP_ROOT = WORKSPACE / ".scratch" / "uv-bootstrap"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, help="Use a previously downloaded uv wheel")
    args = parser.parse_args()
    if os.name != "nt":
        parser.error("This launcher bootstrap currently targets Windows")

    BOOTSTRAP_ROOT.mkdir(parents=True, exist_ok=True)
    wheel = args.wheel
    if wheel is None:
        downloads = BOOTSTRAP_ROOT / "downloads"
        downloads.mkdir(exist_ok=True)
        subprocess.run([
            sys.executable, "-m", "pip", "download", "--only-binary=:all:",
            "--no-deps", f"uv=={UV_VERSION}", "--dest", str(downloads),
        ], check=True)
        matches = list(downloads.glob(f"uv-{UV_VERSION}-*-win_amd64.whl"))
        if len(matches) != 1:
            raise RuntimeError(f"Expected one Windows uv {UV_VERSION} wheel; found {len(matches)}")
        wheel = matches[0]
    wheel = wheel.expanduser().resolve()
    if f"uv-{UV_VERSION}-" not in wheel.name or not wheel.name.endswith("-win_amd64.whl"):
        raise RuntimeError(f"Expected a Windows uv {UV_VERSION} wheel: {wheel}")
    if hashlib.sha256(wheel.read_bytes()).hexdigest() != UV_WHEEL_SHA256:
        raise RuntimeError("uv wheel checksum does not match the pinned Windows build")

    target = BOOTSTRAP_ROOT / "uv.exe"
    pending = BOOTSTRAP_ROOT / f".uv-{uuid.uuid4().hex}.exe"
    try:
        with zipfile.ZipFile(wheel) as archive:
            members = [name for name in archive.namelist() if name.endswith(".data/scripts/uv.exe")]
            if len(members) != 1:
                raise RuntimeError("uv wheel has no unique uv.exe")
            with archive.open(members[0]) as source, pending.open("wb") as output:
                while block := source.read(1024 * 1024):
                    output.write(block)
        version = subprocess.run([str(pending), "--version"], check=True,
                                 capture_output=True, text=True).stdout.strip()
        if version.split(maxsplit=2)[:2] != ["uv", UV_VERSION]:
            raise RuntimeError(f"Unexpected uv version: {version}")
        pending.replace(target)
    finally:
        pending.unlink(missing_ok=True)
    print(f"Source launcher runtime helper ready: {target} ({version})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
