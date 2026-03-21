# Local And Hosted Modes

DaedalMap currently has a clean runtime matrix built from two axes:

- `INSTALL_MODE`
  - `local`
  - `cloud`
- `RUNTIME_MODE`
  - `local`
  - `cloud`

Supported runtime shapes:

1. `INSTALL_MODE=local` + `RUNTIME_MODE=local`
2. `INSTALL_MODE=local` + `RUNTIME_MODE=cloud`
3. `INSTALL_MODE=cloud` + `RUNTIME_MODE=cloud`

Unsupported as a first-class runtime shape:

- `INSTALL_MODE=cloud` + `RUNTIME_MODE=local`

## 1. Local Install + Local Data

Uses:
- explicit `DATA_ROOT`

Best for:
- local development against full datasets
- route and converter testing against real local parquet and geometry files
- self-hosting where data is managed on the same machine or volume

Notes:
- this is the default non-cloud mode
- the current public repo does not ship with a bundled demo `data/` folder
- a plain source checkout therefore still needs local data arranged separately
- if `DATA_ROOT` is left blank, the runtime uses the default local app-data folder:
  `%LOCALAPPDATA%\DaedalMap\data`

## 2. Local Install + Cloud Data

Uses:
- object storage for parquet-backed runtime data
- local cache only for metadata and selected support files

Best for:
- reproducing hosted behavior locally
- testing runtime logic before deploy
- validating object-storage-backed DuckDB query behavior

Notes:
- metadata files are cached locally
- parquet is queried remotely through DuckDB `httpfs`
- startup is faster because the full parquet tree is not mirrored locally
- for normal local testing, leave `CLOUD_CACHE_ROOT` blank unless you need a custom cache folder
- use `S3_PREFIX=staging` for review-lane QA
- use `S3_PREFIX=published` for release-lane/runtime QA

## 3. Cloud Install + Cloud Data

Best for:
- hosted web deployments
- product demos
- cloud-managed pack access

## 4. Planned Installable Product Layer

This sits on top of the runtime matrix above.

Target shape:
- install the open engine/runtime separately from the repo
- choose and download data packs after install
- keep frontend and API behavior aligned with the hosted app

Best for:
- non-developer local installs
- future installer builds
- pack-based onboarding and updates

## Why This Matters

Running the app locally does not automatically mean the app is reading local data.

If `RUNTIME_MODE=cloud`, a local server can still be exercising the hosted-style data path.

That is often the right way to catch production-like issues before deploy.

## Common Local Env Choices

Usually leave these blank:

- `DATA_ROOT`
- `APP_URL`
- `SITE_URL`
- `CLOUD_CACHE_ROOT`

Why:

- `DATA_ROOT`
  only matters in `RUNTIME_MODE=local`; blank means use the default local app-data folder
- `APP_URL`
  only needed if you want the app to advertise a specific external URL
- `SITE_URL`
  only needed if you want links to point at a non-default website/docs host
- `CLOUD_CACHE_ROOT`
  only needed if you want the cloud metadata/support cache stored somewhere custom
