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

The current hosted/runtime direction is:
- `Railway` for the public app runtime
- `Cloudflare R2` for canonical runtime data storage
- `Supabase` for auth and the future entitlement/control plane

In `STORAGE_MODE=s3`, the runtime:
- eagerly syncs only small metadata files to local cache
- queries parquet directly from object storage via DuckDB `httpfs`
- does not sync the full parquet tree at startup

That means the same codebase can be used in:
- bundled demo mode
- fuller local-data mode
- hosted-style S3 mode

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
STORAGE_MODE=s3
S3_BUCKET=global-map-data
S3_ENDPOINT_URL=https://<account>.r2.cloudflarestorage.com
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=auto
S3_LOCAL_CACHE=C:\path\to\county-map-cache
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

The runtime resolves data in this order:

1. `DATA_ROOT` if explicitly set
2. sibling `county-map-data/` folder for local/full-data development
3. bundled `data/` folder as demo fallback

In local demo mode, the app can run directly from bundled data.

In hosted/S3 mode:
- metadata is cached locally
- parquet is queried remotely from object storage

That makes local S3-mode testing useful for reproducing hosted-runtime behavior before deploy.

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

This repo still includes a small bundled `data/` fallback so the app can run as a demo without the full maintained data tree.

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
- `data/` - bundled demo fallback data
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

1. Bundled demo mode
- simplest startup
- uses local bundled `data/`

2. Full local-data mode
- points at sibling `county-map-data/`
- better for real development against fuller data

3. Hosted-style S3 mode
- local server, but object-storage-backed data path
- best for reproducing hosted runtime behavior before deploy

## License

MIT
