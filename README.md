# DaedalMap

DaedalMap is a map-first geographic query engine. It lets people ask place-based questions in natural language and get usable map results across disasters, demographics, economics, climate, and related public data.

Public surfaces:
- App: `https://app.daedalmap.com`
- Website/docs: `https://daedalmap.com`

This repository is the open app/runtime. It is intended to be understandable and usable on its own: you can run it locally, point it at local or hosted data, and extend it with compatible datasets and pack-style workflows.

## What It Does

DaedalMap is built around three ideas:
- ask in plain language instead of assembling GIS workflows first
- keep the map as the primary interface, not an afterthought
- separate runtime delivery from maintained data-pack delivery

Typical use cases:
- show earthquakes, floods, wildfires, storms, volcanoes, or tsunamis for a place and time window
- compare population, economic, and disaster context in the same workflow
- move between hosted demo use, logged-in workspace use, and local/self-hosted operation without changing the basic mental model

## Current Runtime Shape

DaedalMap now treats runtime behavior as a 2-axis matrix:

- `INSTALL_MODE`
  - `local` = local app/runtime install
  - `cloud` = hosted/server deployment
- `RUNTIME_MODE`
  - `local` = query local data
  - `cloud` = query managed cloud-backed data via local cache + DuckDB httpfs

Supported combinations:
- `local install + local data`
- `local install + cloud data`
- `cloud install + cloud data`

Not supported as a first-class runtime shape:
- `cloud install + local data`

The current hosted/runtime direction is:
- `Railway` for the public app runtime
- `Cloudflare R2` for canonical runtime data storage
- `Supabase` for auth and the future entitlement/control plane

In `RUNTIME_MODE=cloud`, the runtime:
- eagerly syncs only small metadata files to local cache
- queries parquet directly from object storage via DuckDB `httpfs`
- does not sync the full parquet tree at startup
- should point at the released `published/` namespace, not the mutable review lane

Admin/review surfaces may also read release markers from a separate `control/`
prefix so admin accounts can still see staging/review pack status even when the
runtime catalog in `published/` is empty.

That means the same codebase can be used in:
- full local-data mode
- hosted-style cloud-data mode

## Guest And Logged-In Behavior

Guest users can open the app and try the public workflow without logging in.

Logged-in users currently get:
- authenticated session identity
- user-scoped frontend persistence
- user-scoped backend session cache
- a dedicated `/settings` page for account/workspace settings

This is the beginning of a larger account/workspace model, not the final form. The direction is:
- stronger persistence for signed-in users
- clearer pack visibility and entitlements
- more meaningful settings/workspace behavior

## Quick Start

### 1. Install dependencies

```powershell
cd county-map
pip install -r requirements.txt
```

### 2. Add environment variables

Create a `.env` file for local development. Minimum common variables:

```env
ANTHROPIC_API_KEY=your_key_here
```

If you want to test the hosted-style setup locally, also configure:

```env
INSTALL_MODE=local
RUNTIME_MODE=cloud
S3_BUCKET=global-map-data
S3_PREFIX=published
S3_CONTROL_PREFIX=control
S3_ENDPOINT_URL=https://<account>.r2.cloudflarestorage.com
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=auto
```

For local testing against the review lane before publish, use:

```env
INSTALL_MODE=local
RUNTIME_MODE=cloud
S3_BUCKET=global-map-data
S3_PREFIX=staging
S3_CONTROL_PREFIX=control
S3_ENDPOINT_URL=https://<account>.r2.cloudflarestorage.com
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=auto
```

Most local users should leave these blank unless they intentionally want overrides:

```env
DATA_ROOT=
APP_URL=
SITE_URL=
CLOUD_CACHE_ROOT=
```

What they mean:
- `DATA_ROOT`
  only used in `RUNTIME_MODE=local`; leave blank to use the default local app-data folder
- `APP_URL`
  optional advertised app URL; leave blank for normal local runs
- `SITE_URL`
  optional website/docs/account URL override; leave blank for normal local runs
- `CLOUD_CACHE_ROOT`
  optional local cache folder for cloud metadata/support files; leave blank unless you want a custom cache location

If you are configuring a hosted deployment, set:

```env
INSTALL_MODE=cloud
RUNTIME_MODE=cloud
PORT=7000
```

If you want auth locally:

```env
SUPABASE_URL=...
SUPABASE_ANON_KEY=...
SUPABASE_SERVICE_KEY=...
```

### 3. Run the app

```powershell
python app.py
```

Open:
- `http://localhost:7000`

## Data Resolution

The runtime resolves behavior from two explicit modes:

1. `INSTALL_MODE`
   controls deployment defaults like writable directories and default URLs
2. `RUNTIME_MODE`
   controls the data plane

Data mode behavior:

1. `RUNTIME_MODE=local`
   uses `DATA_ROOT`
2. `RUNTIME_MODE=cloud`
   uses the hydrated local cloud cache as `DATA_ROOT`

Default local writable folders on Windows:
- `CONFIG_DIR=%LOCALAPPDATA%\DaedalMap\config`
- `STATE_DIR=%LOCALAPPDATA%\DaedalMap\state`
- `CACHE_DIR=%LOCALAPPDATA%\DaedalMap\cache`
- `LOG_DIR=%LOCALAPPDATA%\DaedalMap\logs`
- `PACKS_ROOT=%LOCALAPPDATA%\DaedalMap\packs`
- `DATA_ROOT=%LOCALAPPDATA%\DaedalMap\data`

In hosted/cloud mode:
- metadata is cached locally
- parquet is queried remotely from object storage

That makes local cloud-mode testing useful for reproducing hosted-runtime behavior before deploy.

Important note:
- the current public repo does not include a bundled `data/` demo tree
- a source checkout therefore needs either `DATA_ROOT` in `local` mode, or `RUNTIME_MODE=cloud` with cloud storage configured

## Data And Pack Direction

The old “demo data folder plus converters” framing is no longer the whole story.

The current direction is:
- the engine stays open
- maintained data is packaged as packs
- packs are validated and promoted with explicit release gates
- runtime catalogs eventually depend on pack availability, installation, and entitlement state

Key concepts:
- `available packs`
- `installed packs`
- `entitled packs`
- `active runtime catalog`

These are intentionally distinct.

## Settings Page

The app now has a real settings page:
- `/settings`

This page is intended for:
- account-backed workspace settings
- persistence-related behavior
- user-facing configuration that belongs in the app itself

Some local deployment/filesystem setup concerns were intentionally removed from the visible settings UI for now. Those belong more naturally in local/self-host guidance than in the everyday app settings surface.

## Useful Paths In This Repo

Important files and folders:
- `app.py` - FastAPI app entrypoint
- `mapmover/` - runtime logic, routes, path helpers, DuckDB helpers
- `static/` - frontend app modules and styles
- `templates/` - app HTML, including `/settings`
- `supabase_client.py` - auth/control-plane integration
- `docs/` - local documentation for schemas, runtime notes, and reference material

## Documentation In This Repo

Current docs in `docs/`:
- [docs/APP_OVERVIEW.md](docs/APP_OVERVIEW.md) - current runtime/app overview
- [docs/LOCAL_AND_HOSTED.md](docs/LOCAL_AND_HOSTED.md) - local, full-data, and S3-backed runtime modes
- [docs/DATA_SCHEMAS.md](docs/DATA_SCHEMAS.md) - schema and `loc_id` conventions
- [docs/public reference.md](docs/public%20reference.md) - source/licensing reference notes

## Local Development Modes

Useful local modes:

1. Full local-data mode
- points at sibling `county-map-data/`
- better for real development against fuller data

2. Hosted-style S3 mode
- local server, but object-storage-backed data path
- best for reproducing hosted runtime behavior before deploy

3. Installed/runtime-pack mode
- planned product direction beyond raw source checkout
- engine/runtime installed separately from data packs
- pack selection and updates handled outside the repo clone flow

## Contact

Questions, feedback, or self-host issues: support@daedalmap.com

## License

MIT
