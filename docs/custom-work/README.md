# Custom Work in the DaedalMap Launcher

This is the requirements checklist for people building their own programs,
expansions, data packs, and geometry alongside DaedalMap. The launcher should
support local testing from editable files. Packaging and publication come after
the work is ready to share.

## Target workflow to implement

1. Unzip the launcher. Its programs, managed Python environment, settings, and
   data start inside `launcher-data/` beside the launcher.
2. In **Settings**, choose **Run Local Custom** to disconnect from DaedalMap
   release and Store checks. The launcher offers **Restart now** or **Later**.
3. Put an editable program in the local programs folder, or put a data folder
   in the data folder. The launcher detects changes and offers to update the
   appropriate local catalog.
4. Run and inspect the local program through the same launcher UI without
   building a ZIP. Keep using the same installed data and geometry in either
   connection mode.
5. When the program and data are ready to distribute, build and verify separate
   artifacts. Publishing adds them to the maintained Store catalog; it is not
   required for local testing.

The existing **Open Data Folder** and **Update Catalog** actions cover local
data. The equivalent local **program** catalog and UI are still required.

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
  or **Store download**. A local program must appear in **Programs** before it
  has been published.
- [ ] Provide **Run**, **Stop**, and **Open Folder** actions appropriate to each
  program type. Changing source files should not require a ZIP or an update
  download. Document when restarting the program is necessary.
- [ ] Keep local entries visible when reconnecting to DaedalMap. Disconnecting
  controls release checks and update actions; it must not hide installed or
  custom programs, data, or geometry.

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

- [ ] Show an unpublished Network source in **Programs** through the local
  program catalog. Its absence from the maintained Store catalog must not
  prevent local launcher testing.
- [ ] Keep Network a separate optional program/download. When Mapping and
  Network are both installed, offer **Start with Mapping** on Network settings;
  also provide a way to start Network from inside Mapping when that toggle is
  off. Mapping must work without Network.
- [ ] Make standalone Network a command line and Parquet table workflow, not
  another map viewer. It must run without Mapping. Mapping provides the map
  when both are present.
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

The public launcher source already supports a portable library, the connection
setting, local Mapping copies, and local data folder catalog refresh. Program
cards currently come from the maintained Store catalog or installed release
records. There is no local program catalog yet, and Network has no maintained
Store release. The launcher ZIP must be rebuilt after source changes before a
distributed launcher includes them.
