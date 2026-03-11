"""Reusable DuckDB helpers for parquet-backed runtime queries.

In local mode, all functions accept Path objects pointing to local parquet files.
In S3 mode (STORAGE_MODE=s3), path_to_uri() converts local cache paths to s3://
URIs and the DuckDB connection is configured with httpfs + R2 credentials.
DuckDB fetches only the row groups it needs via HTTP range requests.
"""

from __future__ import annotations

import json
import os
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
