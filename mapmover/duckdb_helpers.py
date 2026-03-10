"""Reusable DuckDB helpers for parquet-backed runtime queries."""

from __future__ import annotations

import json
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


def run_df(sql: str, params: list) -> pd.DataFrame:
    if duckdb is None:
        return pd.DataFrame()
    con = duckdb.connect()
    try:
        return con.execute(sql, params).df()
    finally:
        con.close()


def run_rows(sql: str, params: list) -> list[tuple]:
    if duckdb is None:
        return []
    con = duckdb.connect()
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def parquet_columns(parquet_path: Path) -> set[str]:
    if duckdb is None or not parquet_path.exists():
        return set()
    rows = run_rows("DESCRIBE SELECT * FROM read_parquet(?)", [str(parquet_path)])
    return {row[0] for row in rows}


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


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
            if candidate.exists():
                return candidate, metadata
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
    if not parquet_path.exists():
        raise ValueError(f"Event file not found: {parquet_path}")
    return parquet_path, metadata


def select_distinct_event_loc_ids(areas_path: Path, affected_loc_id: str, exact: bool = False, limit: int | None = None) -> list[str]:
    if duckdb is None or not areas_path.exists():
        return []
    comparator = "=" if exact else "LIKE"
    value = affected_loc_id if exact else f"{affected_loc_id}%"
    sql = (
        "SELECT DISTINCT event_loc_id "
        "FROM read_parquet(?) "
        f"WHERE affected_loc_id {comparator} ? "
        "ORDER BY event_loc_id"
    )
    params: list = [str(areas_path), value]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    rows = run_rows(sql, params)
    return [row[0] for row in rows if row and row[0] is not None]


def select_event_ids_by_regions(parquet_path: Path, regions: Iterable[str]) -> list[str]:
    if duckdb is None or not parquet_path.exists():
        return []
    regions = list(regions)
    sql = "SELECT event_id FROM read_parquet(?)"
    params: list = [str(parquet_path)]
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
    if duckdb is None or not parquet_path.exists():
        return pd.DataFrame()

    available_cols = parquet_columns(parquet_path)
    where: list[str] = []
    params: list = [str(parquet_path)]

    if year is not None and "year" in available_cols:
        where.append('"year" = ?')
        params.append(year)
    if start is not None and "timestamp" in available_cols:
        where.append('"timestamp" >= CAST(? AS TIMESTAMP)')
        params.append(start)
    if end is not None and "timestamp" in available_cols:
        where.append('"timestamp" <= CAST(? AS TIMESTAMP)')
        params.append(end)

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
    if duckdb is None or not parquet_path.exists():
        return pd.DataFrame()

    available_cols = parquet_columns(parquet_path)
    if column not in available_cols:
        return pd.DataFrame()

    sql = (
        f"SELECT * FROM read_parquet(?) WHERE {quote_ident(column)} = ?"
    )
    params: list = [str(parquet_path), value]
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
    if duckdb is None or not parquet_path.exists():
        return pd.DataFrame()

    available_cols = parquet_columns(parquet_path)
    selected = [c for c in (columns or []) if c in available_cols]
    select_expr = ", ".join(quote_ident(c) for c in selected) if selected else "*"

    where: list[str] = []
    params: list = [str(parquet_path)]

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
    if duckdb is None or not links_path.exists():
        return []

    available_cols = parquet_columns(links_path)
    if source_column not in available_cols or target_column not in available_cols:
        return []

    sql = (
        f"SELECT DISTINCT {quote_ident(target_column)} "
        f"FROM read_parquet(?) "
        f"WHERE {quote_ident(source_column)} = ?"
    )
    params: list = [str(links_path), source_loc_id]
    if link_type is not None and "link_type" in available_cols:
        sql += ' AND "link_type" = ?'
        params.append(link_type)
    sql += f" ORDER BY {quote_ident(target_column)}"
    rows = run_rows(sql, params)
    return [row[0] for row in rows if row and row[0] is not None]


def select_peak_positions_by_storm_ids(positions_path: Path, storm_ids: Iterable[str]) -> pd.DataFrame:
    if duckdb is None or not positions_path.exists():
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

    paths = [str(Path(p)) for p in parquet_paths if Path(p).exists()]
    if not paths:
        return pd.DataFrame()

    placeholders = ", ".join("?" for _ in paths)
    sql = f"SELECT * FROM read_parquet([{placeholders}])"
    params: list = list(paths)

    available_cols = parquet_columns(Path(paths[0]))
    where: list[str] = []

    if year is not None and "year" in available_cols:
        where.append('"year" = ?')
        params.append(year)
    if start is not None and "timestamp" in available_cols:
        where.append('"timestamp" >= CAST(? AS TIMESTAMP)')
        params.append(start)
    if end is not None and "timestamp" in available_cols:
        where.append('"timestamp" <= CAST(? AS TIMESTAMP)')
        params.append(end)

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

    if where:
        sql += " WHERE " + " AND ".join(where)

    return run_df(sql, params)


def select_columns_from_parquet(parquet_path: Path, columns: Iterable[str]) -> pd.DataFrame:
    if duckdb is None or not parquet_path.exists():
        return pd.DataFrame()

    available_cols = parquet_columns(parquet_path)
    selected = [c for c in columns if c in available_cols]
    if not selected:
        return pd.DataFrame()

    sql = "SELECT " + ", ".join(quote_ident(c) for c in selected) + " FROM read_parquet(?)"
    return run_df(sql, [str(parquet_path)])
