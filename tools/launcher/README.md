# DaedalMap Launcher

This is the public source for the Windows launcher. The portable ZIP contains
`DaedalMapLauncher.exe`, `_internal/`, and `uv.exe`. On first use it creates
`launcher-data/` beside the EXE. Installed programs, managed Python
environments, settings, logs, and data stay there by default. Settings can
move the data library to another location.

For an older launcher library in `%LOCALAPPDATA%\DaedalMapLauncher`, close the
launcher and run `python tools/launcher/migrate_legacy_library.py --source
"$env:LOCALAPPDATA\DaedalMapLauncher" --target "<portable launcher folder>\launcher-data"`
from PowerShell with access to the old files. The migration checks the copy,
updates saved paths, and removes the old folder after verification. It requires
an empty portable library.

The launcher defaults to DaedalMap release and Store checks. **Settings →
Launcher connection → Run Local Custom** copies installed Mapping and any
installed Network expansion into `launcher-data/custom/` so those files can be
edited without rebuilding program ZIPs. It stops DaedalMap release and Store
checks. Installed programs, data packs, and geometry remain visible from the
same library in both modes; Local Custom shows installed items without remote
update actions. Switching modes offers **Restart now** or **Later**. Install
Mapping while connected before entering Local Custom. A different
`--library-root` creates a separate library, regardless of connection mode.

For source development, create a Python environment with the Mapping runtime
requirements and run:

```powershell
python tools/launcher/launcher.py --local-source
```

`--local-source` is a developer override that runs the repository checkout;
the normal launcher setting uses the self-contained portable folder. The
source launcher looks for `.venv/Scripts/python.exe` in this checkout or a
parent directory. `--library-root PATH` selects an isolated launcher library.

Run the launcher tests with `python -m pytest -q tools/launcher`. Build a
Windows test ZIP with `build_launcher.ps1 -BuildPython <python.exe> -UvExe
<uv.exe> -OutputRoot <output-directory>`; the script verifies the pinned uv
helper before bundling. Publication credentials, release staging, and hosted
promotion remain separate from this public source tree.
