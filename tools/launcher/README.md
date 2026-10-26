# DaedalMap Launcher

This is the public source for the Windows launcher. The portable ZIP contains
`DaedalMapLauncher.exe`, `_internal/`, and `uv.exe`. On first use it creates
`launcher-data/` beside the EXE. Installed programs, managed Python
environments, settings, logs, and data stay there by default. Settings can
move the data library to another location.

The launcher defaults to DaedalMap release and Store checks. **Settings →
Launcher connection → Run Local Custom** copies installed Mapping and any
installed Network expansion into `launcher-data/custom/` so those files can be
edited without rebuilding program ZIPs. It stops DaedalMap release and Store
checks. Switching modes offers **Restart now** or **Later**. Install Mapping
while connected before entering Local Custom.

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
