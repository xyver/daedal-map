# DaedalMap Launcher

Program changes: [Launcher changelog](CHANGELOG.md).

For the custom program, data, geometry, and Network work still to implement,
see the [custom work requirements](../../docs/custom-work/README.md).

This is the public source for the Windows launcher. The portable ZIP contains
`DaedalMapLauncher.exe`, `_internal/`, and `uv.exe`. On first use it creates
`launcher-data/` beside the EXE. Installed programs, managed Python
environments, settings, logs, and data stay there by default. Settings can
move the data library to another location.

In Settings, **Open Data Folder** opens `launcher-data/runtime/data` by default.
Drop a folder containing Parquet files there. The launcher detects changes and
offers **Update Catalog**; the same action is always available in Settings.
Folders with `metadata.json` retain their source details, while a plain
top-level Parquet folder gets a basic local research entry. Restart Mapping if
it is already running when the catalog changes.

The launcher defaults to DaedalMap release and Store checks. **Settings →
Launcher connection → Switch to Local Custom** copies installed Mapping and any
installed Network expansion into `launcher-data/custom/` when no source link
is saved. Source links take priority in Local Custom, so edits can be tested
without rebuilding program ZIPs. It stops DaedalMap release and Store
checks. Installed programs, data packs, and geometry remain visible from the
same library in both modes; Local Custom shows installed items without remote
update actions. Switching modes offers **Restart now** or **Later**. Link an
editable Mapping checkout or install Mapping while connected before entering
Local Custom. A different
`--library-root` creates a separate library, regardless of connection mode.

Programs has **Cloud** and **Installed** views like Data and Geometry. Cloud
shows published program downloads and updates; Installed shows installed
releases and editable source links. Local Custom opens Installed and disables
Cloud, while Connected allows switching between both views.

For source development, create a Python environment with the Mapping runtime
requirements and run:

```powershell
python tools/launcher/launcher.py --local-source
```

`--local-source` is a developer override that runs the repository checkout;
the normal launcher setting uses the self-contained portable folder. The
source launcher looks for `.venv/Scripts/python.exe` in this checkout or a
parent directory. `--library-root PATH` selects an isolated launcher library.
For a source trial without a command window, point a Windows shortcut at the
same environment's `pythonw.exe` and pass
`tools/launcher/launch_gui.py --library-root "<launcher-data>"` as its arguments.
Startup failures are written to `launcher-data/runtime/logs/launcher-startup.log`
and shown in a small error dialog. Mapping server errors are kept in
`launcher-data/runtime/logs/runtime.stderr.log`. Run `launcher.py` with
`python.exe` when a live diagnostic console is useful. Packaged launcher ZIPs
already use a windowed executable.
When the source launcher runs an installed release, it looks for `uv.exe` in
that library. Connected source launchers install the pinned helper there if it
is missing. For an offline setup, run
`python tools/launcher/bootstrap_source_uv.py --library-root "<launcher-data>" --wheel "<uv-wheel>"`.
`uv_pin.json` records the tested version and checksums; the launcher build
checks the supplied executable against that pin. Upstream uv releases do not
silently change an installed launcher.

To show an editable Network checkout in **Programs** before publishing it,
create `launcher-data/local-programs/daedalmap-network/program.json`:

```json
{"product": "daedalmap-network", "source_root": "C:/path/to/checkout-containing-network"}
```

The source folder must contain `network/extension.json`,
`network/app/mapping_extension.py`, and `network/spreadsheet_editor.py`.
Select **Switch to Local Custom** in Settings and restart the launcher after
adding the manifest. Network appears as **Local source** with **Open Network
Map** and **Open Network Editor**. The map button opens Network on Mapping's
map, even when **Start with Mapping** is off. The editor button starts a
standalone, non-map spreadsheet at `http://127.0.0.1:8124/`. It reads
validated packs from the Network checkout and stores draft Parquet edits under
`launcher-data/runtime/data/networks/editor_drafts/`. Restart Mapping after
Python changes to its Network adapter; restart the editor after changes to its
server. Connected mode uses packaged releases instead.

For the Network controls to appear in Local Custom, also link a compatible
Mapping checkout at `launcher-data/local-programs/daedalmap-mapping/program.json`
using product `daedalmap-mapping` and a `source_root` containing `app.py` and
`requirements.txt`. The installed Mapping 0.1.0 release lacks the Network UI
hook. The spreadsheet editor runs without Mapping. Both links remain saved
across mode switches; neither creates a ZIP. A compatible installed Network
program also shows **Open Network Editor** in Programs → Installed and reads
packs from `launcher-data/runtime/data/networks/packs/`.

Run the launcher tests with `python -m pytest -q tools/launcher`. Build a
Windows test ZIP with `build_launcher.ps1 -BuildPython <python.exe> -UvExe
<uv.exe> -OutputRoot <output-directory>`; the script verifies the pinned uv
helper before bundling. Publication credentials, release staging, and hosted
promotion remain separate from this public source tree.
