"""Reusable DuckDB helpers for parquet-backed runtime queries.

In local mode, all functions accept Path objects pointing to local parquet files.
In S3 mode (STORAGE_MODE=s3), path_to_uri() converts local cache paths to s3://
URIs and the DuckDB connection is configured with httpfs + R2 credentials.
DuckDB fetches only the row groups it needs via HTTP range requests.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

try:
    import duckdb
except ImportError:
    duckdb = None


DUCKDB_EVENT_SOURCES = {
    "earthquakes",
    "floods",
    "hurricanes",
    "landslides",
    "tornadoes",
    "tsunamis",
    "volcanoes",
}


def duckdb_available() -> bool:
    return duckdb is not None


def can_query_event_source(source_id: str) -> bool:
    return duckdb_available() and source_id in DUCKDB_EVENT_SOURCES


# ---------------------------------------------------------------------------
# S3 / httpfs helpers
# ---------------------------------------------------------------------------

def is_s3_mode() -> bool:
    return os.environ.get("STORAGE_MODE", "local").strip().lower() == "s3"


def _get_s3_endpoint() -> str:
    """Return the R2/S3 endpoint without https:// prefix (as DuckDB expects)."""
    url = os.environ.get("S3_ENDPOINT_URL", "").strip()
    if url.startswith("https://"):
        url = url[len("https://"):]
    elif url.startswith("http://"):
        url = url[len("http://"):]
    return url.rstrip("/")


def _get_cache_root() -> Path | None:
    env = os.environ.get("S3_LOCAL_CACHE", "").strip()
    if env:
        return Path(env)
    # Use the same fallback as storage_mode.get_s3_cache_root() so that
    # path_to_uri() works without an explicit S3_LOCAL_CACHE env var.
    if is_s3_mode():
        return Path(__file__).parent.parent / ".data_s3_cache"
    return None


def path_to_uri(local_path: Path) -> str:
    """Convert a local cache path to an s3:// URI in S3 mode, or a local path string in local mode."""
    if not is_s3_mode():
        return str(local_path)

    bucket = os.environ.get("S3_BUCKET", "").strip()
    prefix = os.environ.get("S3_PREFIX", "").strip().strip("/")
    prefix = f"{prefix}/" if prefix else ""
    cache_root = _get_cache_root()

    if cache_root:
        try:
            rel = local_path.relative_to(cache_root)
            return f"s3://{bucket}/{prefix}{rel.as_posix()}"
        except ValueError:
            pass

    # Fallback: use the path as-is (shouldn't normally happen)
    return str(local_path)


def parquet_available(path: Path) -> bool:
    """Return True if the parquet file is accessible.
    In S3 mode, always returns True (DuckDB will raise if the file is missing on R2).
    In local mode, checks if the file exists on disk.
    """
    if is_s3_mode():
        return True
    return path.exists()


def _configure_httpfs(con) -> None:
    """Configure a DuckDB connection for R2/S3 access via httpfs."""
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    endpoint = _get_s3_endpoint()
    if endpoint:
        con.execute(f"SET s3_endpoint='{endpoint}'")
    key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
    region = os.environ.get("AWS_DEFAULT_REGION", "auto").strip() or "auto"
    if key:
        con.execute(f"SET s3_access_key_id='{key}'")
    if secret:
        con.execute(f"SET s3_secret_access_key='{secret}'")
    con.execute(f"SET s3_region='{region}'")
    con.execute("SET s3_url_style='path'")
    # Cache parquet footer/metadata globally across connections - eliminates
    # repeated HTTP HEAD+range requests for the same files on each new connection.
    con.execute("SET enable_http_metadata_cache=true")
    con.execute("SET http_keep_alive=true")


def _make_connection():
    """Create a DuckDB connection, configured for S3 if in S3 mode."""
    con = duckdb.connect()
    if is_s3_mode():
        _configure_httpfs(con)
    return con


# ---------------------------------------------------------------------------
# Core query runners
# ---------------------------------------------------------------------------

def run_df(sql: str, params: list) -> pd.DataFrame:
    if duckdb is None:
        return pd.DataFrame()
    con = _make_connection()
    try:
        return con.execute(sql, params).df()
    finally:
        con.close()


def run_rows(sql: str, params: list) -> list[tuple]:
    if duckdb is None:
        return []
    con = _make_connection()
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def _normalize_ts_for_duckdb(val: str | None) -> str | None:
    """Convert a ms-epoch timestamp string to an ISO datetime string for DuckDB.

    DuckDB's CAST(? AS TIMESTAMP) rejects raw millisecond integers like
    '1735718400000'. Detect them (>10 digits, all numeric) and convert to
    'YYYY-MM-DD HH:MM:SS' in UTC which DuckDB handles fine.
    """
    if val is None:
        return None
    s = str(val).strip()
    if s.lstrip("-").isdigit() and len(s) > 10:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(int(s) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return val


def parquet_columns(parquet_path: Path) -> set[str]:
    if duckdb is None:
        return set()
    if not is_s3_mode() and not parquet_path.exists():
        return set()
    uri = path_to_uri(parquet_path)
    rows = run_rows("DESCRIBE SELECT * FROM read_parquet(?)", [uri])
    return {row[0] for row in rows}


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

def resolve_event_parquet_path(source_dir: Path, event_file_key: str = "events") -> tuple[Path, dict]:
    meta_path = source_dir / "metadata.json"
    with open(meta_path, encoding="utf-8") as f:
        metadata = json.load(f)

    files_info = metadata.get("files", {})
    file_info = files_info.get(event_file_key)

    if not file_info:
        fallback_names = [
            f"{event_file_key}.parquet",
            "events.parquet",
            "fires.parquet",
            "positions.parquet",
            "storms.parquet",
        ]
        for name in fallback_names:
            candidate = source_dir / name
            if is_s3_mode() or candidate.exists():
                return candidate, metadata
        if not is_s3_mode():
            parquet_candidates = sorted(source_dir.glob("*.parquet"))
            for candidate in parquet_candidates:
                if candidate.name in ("all_countries.parquet", "all_regions.parquet"):
                    continue
                return candidate, metadata
        raise ValueError(f"No event file '{event_file_key}' found in {source_dir}")

    filename = file_info.get("name") or file_info.get("filename")
    if not filename:
        raise ValueError(f"No filename specified for '{event_file_key}' in {source_dir}")

    parquet_path = source_dir / filename
    if not is_s3_mode() and not parquet_path.exists():
        raise ValueError(f"Event file not found: {parquet_path}")
    return parquet_path, metadata


# ---------------------------------------------------------------------------
# Query functions (all accept Path objects; path_to_uri is applied internally)
# ---------------------------------------------------------------------------

def select_distinct_event_loc_ids(areas_path: Path, affected_loc_id: str, exact: bool = False, limit: int | None = None) -> list[str]:
    if duckdb is None or not parquet_available(areas_path):
        return []
    uri = path_to_uri(areas_path)
    comparator = "=" if exact else "LIKE"
    value = affected_loc_id if exact else f"{affected_loc_id}%"
    sql = (
        "SELECT DISTINCT event_loc_id "
        "FROM read_parquet(?) "
        f"WHERE affected_loc_id {comparator} ? "
        "ORDER BY event_loc_id"
    )
    params: list = [uri, value]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    rows = run_rows(sql, params)
    return [row[0] for row in rows if row and row[0] is not None]


def select_event_ids_by_regions(parquet_path: Path, regions: Iterable[str]) -> list[str]:
    if duckdb is None or not parquet_available(parquet_path):
        return []
    uri = path_to_uri(parquet_path)
    regions = list(regions)
    sql = "SELECT event_id FROM read_parquet(?)"
    params: list = [uri]
    if regions:
        prefixes = [f"{r}%" for r in regions]
        exacts = list(regions)
        like_parts = ['"loc_id" LIKE ?' for _ in prefixes]
        eq_parts = ['"loc_id" = ?' for _ in exacts]
        sql += " WHERE " + " OR ".join(like_parts + eq_parts)
        params.extend(prefixes + exacts)
    rows = run_rows(sql, params)
    return [row[0] for row in rows if row and row[0] is not None]


def select_filtered_event_rows(
    parquet_path: Path,
    *,
    year: int | None = None,
    start: str | None = None,
    end: str | None = None,
    min_value_filters: dict | None = None,
    exact_filters: dict | None = None,
    like_filters: dict | None = None,
    in_filters: dict | None = None,
    order_by_desc: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    if duckdb is None or not parquet_available(parquet_path):
        return pd.DataFrame()

    uri = path_to_uri(parquet_path)
    available_cols = parquet_columns(parquet_path)
    where: list[str] = []
    params: list = [uri]

    if year is not None and "year" in available_cols:
        where.append('"year" = ?')
        params.append(year)
    if start is not None and "timestamp" in available_cols:
        where.append('"timestamp" >= CAST(? AS TIMESTAMP)')
        params.append(_normalize_ts_for_duckdb(start))
    if end is not None and "timestamp" in available_cols:
        where.append('"timestamp" <= CAST(? AS TIMESTAMP)')
        params.append(_normalize_ts_for_duckdb(end))

    for col, value in (min_value_filters or {}).items():
        if col in available_cols and value is not None:
            where.append(f"{quote_ident(col)} >= ?")
            params.append(value)

    for col, value in (exact_filters or {}).items():
        if col in available_cols and value is not None:
            where.append(f"{quote_ident(col)} = ?")
            params.append(value)

    for col, value in (like_filters or {}).items():
        if col in available_cols and value is not None:
            where.append(f"{quote_ident(col)} LIKE ?")
            params.append(value)

    for col, values in (in_filters or {}).items():
        values = list(values or [])
        if col in available_cols and values:
            placeholders = ", ".join("?" for _ in values)
            where.append(f"{quote_ident(col)} IN ({placeholders})")
            params.extend(values)

    sql = "SELECT * FROM read_parquet(?)"
    if where:
        sql += " WHERE " + " AND ".join(where)
    if order_by_desc and order_by_desc in available_cols:
        sql += f" ORDER BY {quote_ident(order_by_desc)} DESC NULLS LAST"
    if limit is not None and limit > 0:
        sql += " LIMIT ?"
        params.append(limit)

    return run_df(sql, params)


def select_rows_by_exact_value(
    parquet_path: Path,
    column: str,
    value,
    *,
    order_by: str | None = None,
) -> pd.DataFrame:
    if duckdb is None or not parquet_available(parquet_path):
        return pd.DataFrame()

    uri = path_to_uri(parquet_path)
    available_cols = parquet_columns(parquet_path)
    if column not in available_cols:
        return pd.DataFrame()

    sql = f"SELECT * FROM read_parquet(?) WHERE {quote_ident(column)} = ?"
    params: list = [uri, value]
    if order_by and order_by in available_cols:
        sql += f" ORDER BY {quote_ident(order_by)} ASC NULLS LAST"
    return run_df(sql, params)


def select_rows(
    parquet_path: Path,
    *,
    columns: Iterable[str] | None = None,
    exact_filters: dict | None = None,
    in_filters: dict | None = None,
    order_by: str | None = None,
) -> pd.DataFrame:
    if duckdb is None or not parquet_available(parquet_path):
        return pd.DataFrame()

    uri = path_to_uri(parquet_path)
    available_cols = parquet_columns(parquet_path)
    selected = [c for c in (columns or []) if c in available_cols]
    select_expr = ", ".join(quote_ident(c) for c in selected) if selected else "*"

    where: list[str] = []
    params: list = [uri]

    for col, value in (exact_filters or {}).items():
        if col in available_cols and value is not None:
            where.append(f"{quote_ident(col)} = ?")
            params.append(value)

    for col, values in (in_filters or {}).items():
        values = [v for v in (values or []) if v is not None]
        if col in available_cols and values:
            placeholders = ", ".join("?" for _ in values)
            where.append(f"{quote_ident(col)} IN ({placeholders})")
            params.extend(values)

    sql = f"SELECT {select_expr} FROM read_parquet(?)"
    if where:
        sql += " WHERE " + " AND ".join(where)
    if order_by and order_by in available_cols:
        sql += f" ORDER BY {quote_ident(order_by)} ASC NULLS LAST"
    return run_df(sql, params)


def select_linked_loc_ids(
    links_path: Path,
    *,
    source_column: str,
    source_loc_id: str,
    target_column: str,
    link_type: str | None = None,
) -> list[str]:
    if duckdb is None or not parquet_available(links_path):
        return []

    uri = path_to_uri(links_path)
    available_cols = parquet_columns(links_path)
    if source_column not in available_cols or target_column not in available_cols:
        return []

    sql = (
        f"SELECT DISTINCT {quote_ident(target_column)} "
        f"FROM read_parquet(?) "
        f"WHERE {quote_ident(source_column)} = ?"
    )
    params: list = [uri, source_loc_id]
    if link_type is not None and "link_type" in available_cols:
        sql += ' AND "link_type" = ?'
        params.append(link_type)
    sql += f" ORDER BY {quote_ident(target_column)}"
    rows = run_rows(sql, params)
    return [row[0] for row in rows if row and row[0] is not None]


def select_peak_positions_by_storm_ids(positions_path: Path, storm_ids: Iterable[str]) -> pd.DataFrame:
    if duckdb is None or not parquet_available(positions_path):
        return pd.DataFrame()

    storm_ids = [s for s in storm_ids if s]
    if not storm_ids:
        return pd.DataFrame()

    df = select_filtered_event_rows(
        positions_path,
        in_filters={"storm_id": storm_ids},
    )
    if df.empty:
        return df

    df = df.dropna(subset=["latitude", "longitude"])
    if df.empty or "storm_id" not in df.columns:
        return pd.DataFrame()

    if "wind_kt" in df.columns:
        df["wind_sort"] = df["wind_kt"].fillna(-1)
        idx = df.groupby("storm_id")["wind_sort"].idxmax()
        df = df.loc[idx].drop(columns=["wind_sort"], errors="ignore")
    else:
        df = df.sort_values(["storm_id", "timestamp"] if "timestamp" in df.columns else ["storm_id"])
        df = df.groupby("storm_id").head(1)

    return df


def select_filtered_partitioned_rows(
    parquet_paths: Iterable[Path],
    *,
    year: int | None = None,
    start: str | None = None,
    end: str | None = None,
    min_value_filters: dict | None = None,
    exact_filters: dict | None = None,
    like_filters: dict | None = None,
    in_filters: dict | None = None,
) -> pd.DataFrame:
    if duckdb is None:
        return pd.DataFrame()

    if is_s3_mode():
        # In S3 mode, convert paths to s3:// URIs - skip local exists check
        uris = [path_to_uri(Path(p)) for p in parquet_paths]
    else:
        uris = [str(Path(p)) for p in parquet_paths if Path(p).exists()]

    if not uris:
        return pd.DataFrame()

    # Get columns from first reachable file to build WHERE clause
    available_cols: set[str] = set()
    if is_s3_mode():
        for uri in uris:
            try:
                rows = run_rows("DESCRIBE SELECT * FROM read_parquet(?)", [uri])
                available_cols = {row[0] for row in rows}
                break
            except Exception:
                continue
    else:
        available_cols = parquet_columns(Path(uris[0]))

    # Build WHERE clause and filter params (not including the URI placeholder)
    where: list[str] = []
    filter_params: list = []

    if year is not None and "year" in available_cols:
        where.append('"year" = ?')
        filter_params.append(year)
    if start is not None and "timestamp" in available_cols:
        where.append('"timestamp" >= CAST(? AS TIMESTAMP)')
        filter_params.append(_normalize_ts_for_duckdb(start))
    if end is not None and "timestamp" in available_cols:
        where.append('"timestamp" <= CAST(? AS TIMESTAMP)')
        filter_params.append(_normalize_ts_for_duckdb(end))

    for col, value in (min_value_filters or {}).items():
        if col in available_cols and value is not None:
            where.append(f"{quote_ident(col)} >= ?")
            filter_params.append(value)

    for col, value in (exact_filters or {}).items():
        if col in available_cols and value is not None:
            where.append(f"{quote_ident(col)} = ?")
            filter_params.append(value)

    for col, value in (like_filters or {}).items():
        if col in available_cols and value is not None:
            where.append(f"{quote_ident(col)} LIKE ?")
            filter_params.append(value)

    for col, values in (in_filters or {}).items():
        values = list(values or [])
        if col in available_cols and values:
            placeholders_w = ", ".join("?" for _ in values)
            where.append(f"{quote_ident(col)} IN ({placeholders_w})")
            filter_params.extend(values)

    where_clause = " WHERE " + " AND ".join(where) if where else ""

    if is_s3_mode():
        # Query each file individually so missing S3 files are silently skipped
        dfs = []
        for uri in uris:
            try:
                df = run_df(f"SELECT * FROM read_parquet(?){where_clause}", [uri] + filter_params)
                if not df.empty:
                    dfs.append(df)
            except Exception as e:
                err = str(e)
                if "No files found" in err or "404" in err or "HTTP" in err:
                    continue
                raise
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    else:
        placeholders = ", ".join("?" for _ in uris)
        sql = f"SELECT * FROM read_parquet([{placeholders}]){where_clause}"
        return run_df(sql, list(uris) + filter_params)


def select_columns_from_parquet(parquet_path: Path, columns: Iterable[str]) -> pd.DataFrame:
    if duckdb is None or not parquet_available(parquet_path):
        return pd.DataFrame()

    uri = path_to_uri(parquet_path)
    available_cols = parquet_columns(parquet_path)
    selected = [c for c in columns if c in available_cols]
    if not selected:
        return pd.DataFrame()

    sql = "SELECT " + ", ".join(quote_ident(c) for c in selected) + " FROM read_parquet(?)"
    return run_df(sql, [uri])


# ---------------------------------------------------------------------------
# In-memory TTL response cache
# Caches DataFrames from slow default GeoJSON queries so cold-start fetches
# from R2 do not block every incoming request.
# ---------------------------------------------------------------------------

_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, tuple[pd.DataFrame, float]] = {}  # key -> (df, expires_at)

DEFAULT_CACHE_TTL = int(os.environ.get("DUCKDB_CACHE_TTL", "300"))  # seconds


def cache_get(key: str) -> pd.DataFrame | None:
    """Return cached DataFrame if still valid, else None."""
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
    if entry is None:
        return None
    df, expires_at = entry
    if time.monotonic() > expires_at:
        with _CACHE_LOCK:
            _CACHE.pop(key, None)
        return None
    return df


def cache_set(key: str, df: pd.DataFrame, ttl: int | None = None) -> None:
    """Store a DataFrame in the cache with a TTL (default DEFAULT_CACHE_TTL)."""
    ttl = ttl if ttl is not None else DEFAULT_CACHE_TTL
    expires_at = time.monotonic() + ttl
    with _CACHE_LOCK:
        _CACHE[key] = (df, expires_at)


def cache_clear(prefix: str | None = None) -> None:
    """Clear all cache entries, or only those whose key starts with prefix."""
    with _CACHE_LOCK:
        if prefix is None:
            _CACHE.clear()
        else:
            for k in list(_CACHE):
                if k.startswith(prefix):
                    del _CACHE[k]


def make_cache_key(source: str, **params) -> str:
    """Build a cache key from a source name and request params.

    Only non-None values are included. Sorted so key is stable regardless of
    argument order. Use the same helper in both the pre-warmer and route
    handlers so keys always match.

    Example:
        make_cache_key("floods", year=2021, include_geometry=True)
        -> "floods:include_geometry:True:year:2021"
    """
    relevant = {k: v for k, v in params.items() if v is not None and v is not False}
    parts = [source] + [f"{k}:{v}" for k, v in sorted(relevant.items())]
    return ":".join(parts)


def select_filtered_event_rows_cached(
    parquet_path: Path,
    cache_key: str,
    ttl: int | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Like select_filtered_event_rows but checks/stores results in the TTL cache.

    Use this for default (no user-specific filters) queries to avoid cold R2
    fetches on every request. cached DataFrame is returned for cache_ttl seconds.
    """
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    df = select_filtered_event_rows(parquet_path, **kwargs)
    if not df.empty:
        cache_set(cache_key, df, ttl)
    return df


# ---------------------------------------------------------------------------
# Startup pre-warmer
# Runs the expensive default queries for each disaster source so the DuckDB
# http metadata cache and our in-memory DataFrame cache are both populated
# before the first user request arrives.
# ---------------------------------------------------------------------------

def _prewarm_source(source_id: str, parquet_path: Path, min_year_filter: dict | None) -> None:
    """Run the default query for one source and populate the cache."""
    import logging
    log = logging.getLogger(__name__)
    if not parquet_available(parquet_path):
        return
    cache_key = f"{source_id}:default:{min_year_filter}"
    if cache_get(cache_key) is not None:
        return  # already warm
    try:
        t0 = time.monotonic()
        df = select_filtered_event_rows(
            parquet_path,
            min_value_filters=min_year_filter,
        )
        elapsed = time.monotonic() - t0
        if not df.empty:
            cache_set(cache_key, df)
        log.info("prewarm %s: %d rows in %.1fs", source_id, len(df), elapsed)
    except Exception as exc:
        log.warning("prewarm %s failed: %s", source_id, exc)


def prewarm_disaster_sources(global_dir: Path) -> None:
    """Pre-warm the default queries for all disaster sources.

    Call this in a background thread from the app lifespan. It populates both
    the DuckDB http metadata cache and our in-memory DataFrame cache so that
    the first user request does not incur cold R2 latency.

    Animation years 2020-2025 are pre-warmed with the exact filter params that
    the frontend overlay-controller.js uses (min_magnitude, min_area_km2, etc.)
    so that animation playback hits the cache on the first pass.
    """
    if not is_s3_mode():
        return  # pre-warming only needed for R2 mode

    import logging
    log = logging.getLogger(__name__)

    # Animation years the user typically plays through.
    animation_years = list(range(2020, 2026))

    # --- earthquakes (min_magnitude 5.5 from overlay-controller.js) ----------
    eq_path = global_dir / "disasters/earthquakes/events.parquet"
    for yr in animation_years:
        ck = make_cache_key("earthquakes", year=yr, min_magnitude=5.5)
        if cache_get(ck) is None:
            try:
                t0 = time.monotonic()
                df = select_filtered_event_rows(eq_path, year=yr, min_value_filters={"magnitude": 5.5})
                if not df.empty:
                    cache_set(ck, df)
                log.info("prewarm earthquakes year=%d: %d rows in %.1fs", yr, len(df), time.monotonic() - t0)
            except Exception as exc:
                log.warning("prewarm earthquakes year=%d failed: %s", yr, exc)

    # --- tsunamis (no extra filters) -----------------------------------------
    ts_path = global_dir / "disasters/tsunamis/events.parquet"
    for yr in animation_years:
        ck = make_cache_key("tsunamis", year=yr)
        if cache_get(ck) is None:
            try:
                t0 = time.monotonic()
                df = select_filtered_event_rows(ts_path, year=yr)
                if not df.empty:
                    cache_set(ck, df)
                log.info("prewarm tsunamis year=%d: %d rows in %.1fs", yr, len(df), time.monotonic() - t0)
            except Exception as exc:
                log.warning("prewarm tsunamis year=%d failed: %s", yr, exc)

    # --- floods (max year is 2019, no animation years qualify) ----------------
    fl_path = global_dir / "disasters/floods/events_enriched.parquet"
    if not parquet_available(fl_path):
        fl_path = global_dir / "disasters/floods/events.parquet"
    for yr in [y for y in animation_years if y <= 2019]:
        ck = make_cache_key("floods", year=yr)
        if cache_get(ck) is None:
            try:
                t0 = time.monotonic()
                df = select_filtered_event_rows(fl_path, year=yr)
                if not df.empty:
                    cache_set(ck, df)
                log.info("prewarm floods year=%d: %d rows in %.1fs", yr, len(df), time.monotonic() - t0)
            except Exception as exc:
                log.warning("prewarm floods year=%d failed: %s", yr, exc)

    # --- volcanoes/eruptions (exclude_ongoing is a post-fetch pandas filter) --
    vol_path = global_dir / "disasters/volcanoes/events.parquet"
    for yr in animation_years:
        ck = make_cache_key("volcanoes", year=yr)
        if cache_get(ck) is None:
            try:
                t0 = time.monotonic()
                df = select_filtered_event_rows(vol_path, year=yr)
                if not df.empty:
                    cache_set(ck, df)
                log.info("prewarm volcanoes year=%d: %d rows in %.1fs", yr, len(df), time.monotonic() - t0)
            except Exception as exc:
                log.warning("prewarm volcanoes year=%d failed: %s", yr, exc)

    # --- tornadoes (min_scale=EF2 is a post-fetch filter; cache raw year slice)
    tor_path = global_dir / "disasters/tornadoes/events.parquet"
    for yr in animation_years:
        ck = make_cache_key("tornadoes", year=yr)
        if cache_get(ck) is None:
            try:
                t0 = time.monotonic()
                df = select_filtered_event_rows(tor_path, year=yr)
                if not df.empty:
                    cache_set(ck, df)
                log.info("prewarm tornadoes year=%d: %d rows in %.1fs", yr, len(df), time.monotonic() - t0)
            except Exception as exc:
                log.warning("prewarm tornadoes year=%d failed: %s", yr, exc)

    # --- hurricanes (storms.parquet + positions.parquet; route assembles join)
    # Warm DuckDB metadata cache for both files; route handler caches the join.
    hur_storms_path = global_dir / "disasters/hurricanes/storms.parquet"
    hur_positions_path = global_dir / "disasters/hurricanes/positions.parquet"
    for yr in animation_years:
        ck = make_cache_key("hurricanes", year=yr, min_category="Cat1")
        if cache_get(ck) is None:
            try:
                t0 = time.monotonic()
                storms_df = select_filtered_event_rows(hur_storms_path, year=yr)
                if not storms_df.empty:
                    # Filter Cat1+ (matches overlay-controller.js default)
                    cat_order = {"TD": 0, "TS": 1, "Cat1": 2, "Cat2": 3, "Cat3": 4, "Cat4": 5, "Cat5": 6}
                    storms_df = storms_df[storms_df["max_category"].map(lambda x: cat_order.get(x, 0) >= 2)]
                    if not storms_df.empty:
                        storm_ids = storms_df["storm_id"].tolist()
                        pos_df = select_filtered_event_rows(hur_positions_path, in_filters={"storm_id": storm_ids})
                        pos_df = pos_df.dropna(subset=["latitude", "longitude"])
                        if not pos_df.empty:
                            pos_df["wind_sort"] = pos_df["wind_kt"].fillna(-1)
                            max_pos = pos_df.loc[pos_df.groupby("storm_id")["wind_sort"].idxmax()]
                            joined = storms_df.merge(max_pos[["storm_id", "latitude", "longitude"]], on="storm_id", how="inner", suffixes=("", "_pos"))
                            if not joined.empty:
                                cache_set(ck, joined)
                log.info("prewarm hurricanes year=%d: %d storms in %.1fs", yr, len(storms_df), time.monotonic() - t0)
            except Exception as exc:
                log.warning("prewarm hurricanes year=%d failed: %s", yr, exc)

    # --- wildfires (per-year global files 2020-2024; route caches assembled df)
    # Warm DuckDB metadata cache for the per-year parquet files.
    wf_base = global_dir / "disasters/wildfires/by_year_enriched"
    for yr in animation_years:
        if yr > 2024:
            continue  # global wildfire data only goes to 2024
        ck = make_cache_key("wildfires", year=yr, min_area_km2=500)
        if cache_get(ck) is None:
            try:
                t0 = time.monotonic()
                wf_path = wf_base / f"fires_{yr}_enriched.parquet"
                df = select_filtered_event_rows(wf_path, min_value_filters={"area_km2": 500})
                if not df.empty:
                    import pandas as _pd
                    df["timestamp"] = _pd.to_datetime(df["timestamp"], errors="coerce")
                    df["year"] = df["timestamp"].dt.year
                    df = df[df["year"] == yr]
                    if "land_cover" not in df.columns:
                        df["land_cover"] = ""
                    if not df.empty:
                        cache_set(ck, df)
                log.info("prewarm wildfires year=%d: %d rows in %.1fs", yr, len(df), time.monotonic() - t0)
            except Exception as exc:
                log.warning("prewarm wildfires year=%d failed: %s", yr, exc)

    log.info("Pre-warmer complete")
