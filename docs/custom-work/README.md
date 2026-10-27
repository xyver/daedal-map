# Custom Work in the DaedalMap Launcher

This guide records the current local workflow and the remaining general
program-catalog work for people building programs, expansions, data packs, and
geometry alongside DaedalMap. Local testing uses editable files; packaging and
publication come when the work is ready to share.

## Current workflow

1. Unzip the launcher. Its programs, managed Python environment, settings, and
   data start inside `launcher-data/` beside the launcher.
2. In **Settings**, use the single connection button to switch between **Local
   Custom** for editable source programs and **Connected** for published
   downloads and updates. The colored badge shows the active mode; a pending
   change is highlighted until restart. The launcher offers **Restart now** or
   **Later**.
3. Link an editable Network checkout with a small `program.json`, or put a
   data folder in the data folder. The launcher detects data changes and offers
   to update the local data catalog. Automatic discovery of arbitrary custom
   programs is still planned.
4. Run and inspect the local program through the same launcher UI without
   building a ZIP. Keep using the same installed data and geometry in either
   connection mode.
5. When the program and data are ready to distribute, build and verify separate
   artifacts. Publishing adds them to the maintained Store catalog; it is not
   required for local testing.

The existing **Open Data Folder** and **Update Catalog** actions cover local
data. Network has a working source link; a general local program catalog and
folder scan for arbitrary programs are still required.

## Link a program without packaging

A local program should have a stable ID and editable source folder. The
launcher library keeps a small manifest that points to that folder; the source
can live elsewhere on the computer. A source link is separate from an installed
release record in `launcher-data/programs.json`. The program should appear in
**Programs** as **Local source** in both connection modes. Editing source files
should require a program restart, not a new ZIP or Store publication. The
launcher should select local source cards in **Local Custom** and published
release cards in **Connected**. A source link stays saved when switching modes,
but Connected does not run it in place of a packaged release.

**Network is the working example today.** Create
`launcher-data/local-programs/daedalmap-network/program.json` beside the
launcher with:

```json
{
  "product": "daedalmap-network",
  "source_root": "C:/path/to/checkout-containing-network"
}
```

That folder must contain `network/extension.json` with product
`daedalmap-network` and `mapping_interface` 1, plus
`network/app/mapping_extension.py` and `network/spreadsheet_editor.py`. Choose
**Switch to Local Custom** in Settings and restart the launcher to see
**Network / Local source** in **Programs**. **Open Network Map** opens the
Network overlay in Mapping; **Open Network Editor** starts a standalone
spreadsheet on port 8124 without Mapping. It lists validated source packs from
`network/data/packs` and saves draft cell edits as Parquet in the launcher data
library under `networks/editor_drafts/<pack_id>/edits.parquet`. Pack files are
unchanged. Restart the relevant server after Python edits. The source link
works without a Network ZIP.
In Connected mode, Network appears only if a packaged release is installed or
listed in the maintained Store catalog.

Mapping also needs a compatible editable source for Network controls during
local testing. Put a `program.json` at
`launcher-data/local-programs/daedalmap-mapping/` with product
`daedalmap-mapping` and `source_root` pointing to a Mapping checkout containing
`app.py`, `requirements.txt`, and the Network UI hook. The installed Mapping
0.1.0 package predates that hook, so merely linking Network to it cannot
create the **Open Network** control. Local Custom runs the linked Mapping
source; Connected returns to the installed release. Package both compatible
versions before offering Network as a published expansion.

The general contributor workflow still needs implementation: a documented
manifest for any program type, validation, a folder picker or drop-in folder,
catalog refresh in Settings, and run/stop actions based on program type. The
Network manifest above is currently the only supported local source card;
renaming its product ID does not make an arbitrary program runnable.

## Work to complete

### Local program catalog

- [ ] Define one documented local program manifest. It needs a stable program
  ID, display name, type, version or local revision, source folder, launch
  action, runtime requirements, and optional compatibility with Mapping.
- [ ] Choose and document one folder under `launcher-data/` for editable custom
  programs. Keep the generated local program catalog separate from
  `programs.json`, which records installed releases. Contributors should be
  able to add a program by dropping in a folder and its manifest.
- [ ] Scan those folders on launch and after changes. Show a clear validation
  error for an incomplete or duplicate manifest. Offer **Update Program
  Catalog** in Settings, matching the data folder workflow.
- [ ] Merge local catalog entries with installed and maintained Store entries
  by stable program ID. Label each card **Local source**, **Installed release**,
  or **Store download**. Local Custom should show editable source; Connected
  should show packaged releases. A local program must appear in **Programs**
  before it has been published.
- [ ] Provide **Run**, **Stop**, and **Open Folder** actions appropriate to each
  program type. Changing source files should not require a ZIP or an update
  download. Document when restarting the program is necessary.
- [ ] Keep source links saved when reconnecting to DaedalMap, but show the
  packaged program cards in Connected mode. Switching modes must not move or
  erase installed programs, source files, data, or geometry.

### Connection and library settings

- [ ] Make **Connected** and **Local Custom** use the same library root. A mode
  switch changes catalog and update behavior, not file location or identity.
- [ ] In Local Custom, use local catalogs and skip automatic DaedalMap release,
  Store, and launcher update checks. Keep manual local catalog refresh actions
  available. In Connected, show maintained releases alongside local entries
  without silently replacing local source.
- [ ] Keep the default data folder inside `launcher-data/`. An external data
  location is an explicit Settings choice. Show the active folder and provide
  **Open Data Folder** so users do not need to know the internal path.
- [ ] Show the active connection mode clearly, and retain the restart prompt
  when changing it. Test that a restart reopens the same library.
- [ ] Allow Local Custom to open for a contributor who is building a standalone
  program and has not installed Mapping. Mapping source is optional for that
  workflow; the Programs and Settings tabs must remain usable.

### Data and geometry folders

- [ ] Confirm that a dropped Parquet folder is discovered in both connection
  modes and that **Update Catalog** rewrites paths to the active data folder.
  Preserve supplied `metadata.json`; a folder without metadata gets a basic
  research entry until the contributor describes its schema and map behavior.
- [ ] Apply the same discover, review, and refresh pattern to custom geometry.
  Keep downloaded geometry and local geometry distinct in the UI, with both
  visible when present.
- [ ] Check local folder data against Mapping with no managed packs, with
  managed packs, and after a maintained pack update. A pack update must not
  permanently erase local catalog entries.
- [ ] Document the minimum metadata needed for Research, map display,
  geography joins, time filtering, provenance, and licenses. Link to
  [data preparation](../DATA_PREPARATION.md),
  [data schemas](../DATA_SCHEMAS.md), and
  [pack authoring](../PACK_AUTHORING.md).

### Mapping and Network as the first integration case

- [x] Show an unpublished Network source in **Programs** through a local
  program manifest. Its absence from the maintained Store catalog does not
  prevent local launcher testing.
- [x] Keep Network optional in local source mode. **Start with Mapping** is on
  the Network settings page, and Mapping can open the overlay when that toggle
  is off. Mapping also starts without Network.
- [x] Provide a standalone Network CLI and spreadsheet editor without a
  second map. Mapping supplies the visual overlay when both are present.
- [ ] Bundle the spreadsheet editor in the separate Network program ZIP and
  expose **Open Network Editor** for installed Network, without Mapping.
- [ ] Test local source, installed release, and missing Network cases before
  packaging. Then build a separate Network artifact, validate Mapping interface
  compatibility, and add its release to the maintained program catalog.

### Verification and distribution

- [ ] Test a fresh portable launcher, a switched connection mode, a changed
  data folder, an unpublished custom program, and a published program. Test
  Mapping alone, Network alone, and both together with auto start on and off.
- [ ] Verify local edits are visible after a program restart without rebuilding
  artifacts. Verify installed data and geometry remain visible through mode
  switches and launcher updates.
- [ ] With each launcher release, verify the bundled `uv.exe` against
  `tools/launcher/uv_pin.json`. Confirm a connected source launcher can restore
  a missing helper inside its own library; a disconnected launcher must not
  fetch one unexpectedly.
- [ ] Keep source folders, disposable test output, reusable build tools, and
  release records in their intended locations. Do not fill the public repo
  with one-off test data or receipts.
- [ ] Update this README as the local catalog contract is implemented, then
  document the final manifest and folder layout with a minimal working custom
  program example.

## Current implementation boundary

The public launcher source supports a portable library, the connection
setting, local Mapping copies, and local data folder catalog refresh. Network
can appear from `launcher-data/local-programs/daedalmap-network/program.json`
without a release. Programs has Cloud and Installed views; Local Custom opens
Installed and disables Cloud, matching Data and Geometry. A general local
program catalog for other tools is still
required. Network has no maintained Store release. The launcher ZIP must be
rebuilt after source changes before a distributed launcher includes them.
