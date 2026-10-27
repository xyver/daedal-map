"""Start the source launcher with pythonw and keep startup diagnostics on disk."""

from __future__ import annotations

import argparse
import contextlib
import ctypes
import runpy
import sys
import traceback
from datetime import datetime
from pathlib import Path


def show_startup_error(log_path: Path) -> None:
    ctypes.windll.user32.MessageBoxW(
        None, f"DaedalMap Launcher could not start.\n\nSee:\n{log_path}",
        "DaedalMap Launcher", 0x10)


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--library-root", type=Path)
    args, _ = parser.parse_known_args(sys.argv[1:])
    library = (args.library_root or Path(__file__).resolve().parent / "launcher-data").resolve()
    log_path = library / "runtime" / "logs" / "launcher-startup.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    launcher = Path(__file__).resolve().with_name("launcher.py")
    with log_path.open("a", encoding="utf-8") as log:
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            print(f"\n[{datetime.now().astimezone().isoformat()}] Starting source launcher", flush=True)
            try:
                runpy.run_path(str(launcher), run_name="__main__")
            except SystemExit as exc:
                if exc.code in (None, 0):
                    return 0
                print(f"Launcher exited during startup: {exc}", flush=True)
            except BaseException:
                traceback.print_exc()
            else:
                return 0
        log.flush()
    show_startup_error(log_path)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
