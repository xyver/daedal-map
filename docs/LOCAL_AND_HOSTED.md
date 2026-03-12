# Local And Hosted Modes

DaedalMap currently supports three practical runtime styles.

## 1. Bundled Demo Mode

Uses:
- the repo's bundled `data/` folder

Best for:
- quick first run
- UI testing
- development when the full external data tree is not present

## 2. Full Local-Data Mode

Uses:
- a sibling `county-map-data/` tree or explicit `DATA_ROOT`

Best for:
- fuller local development
- working against broader datasets than the bundled demo
- converter and route testing against real local files

## 3. Hosted-Style S3 Mode

Uses:
- object storage for parquet-backed runtime data
- local cache only for small metadata files

Best for:
- reproducing hosted behavior locally
- testing runtime logic before deploy
- validating object-storage-backed queries and caching behavior

## Storage Notes

In hosted-style mode:
- metadata files are cached locally
- parquet is queried remotely through DuckDB `httpfs`
- startup is faster because the full parquet tree is not mirrored locally

## Why This Matters

Running the app locally does not always mean the app is reading local data.

If `STORAGE_MODE=s3`, a local server can still be exercising the hosted-style data path.

That is often the right way to catch production-like issues before deploy.
