"""Resolve admin and place names through each country's selected geography.

Every country uses the same two sources, both selected by the geometry
catalog and read the same way in local and cloud mode:

- spine names (Admin 0 and below): the country's admitted Admin 0-3 query
  bank, or the Global Baseline graph's Admin 0-2 identities where no country
  spine is adopted;
- place names: the country reference graph's name aliases in its city and
  urban-area alias partitions, selected by the partition index's family.

Names are matched on normalized keys computed by one SQL expression, so stored
names and query text can never be normalized differently. Each name yields a
plain key (case and accents removed) and a transliterated key (German umlauts
written as ae/oe/ue), so "Koeln", "Koln", and the umlaut spelling all match.
"""

from __future__ import annotations

import threading
from functools import lru_cache
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from ..duckdb_helpers import lease_query_connection, path_to_uri, select_rows
from . import reference_graph as graph
from .admin_spine_query import layout_available, layout_root
from .geography_reference import translate_geometry_id_to_local_id

PLACE_FAMILIES = ("place_or_municipality", "urban_or_metro_area")
PLACE_FAMILY_RANK = {family: rank for rank, family in enumerate(PLACE_FAMILIES)}
SPINE_MAX_LEVEL_ADOPTED = 3
SPINE_MAX_LEVEL_GLOBAL = 2

# Letters NFKD accent stripping cannot reduce to ASCII.
_FOLDS = (
    ("\u00df", "ss"), ("\u00f8", "o"), ("\u00e6", "ae"), ("\u0153", "oe"),
    ("\u0142", "l"), ("\u0111", "d"), ("\u00fe", "th"), ("\u0131", "i"),
)
_TRANSLITERATIONS = (("\u00e4", "ae"), ("\u00f6", "oe"), ("\u00fc", "ue"))
_ADMIN_SUFFIX_PATTERN = (
    r"\b(county of|county|parish|borough|municipality|municipio|district|region|"
    r"province|state|prefecture|department|departement|oblast)\b"
)
KEY_COLUMNS = ("k_plain", "k_translit", "k_plain_stripped", "k_translit_stripped")

_SPINE_LOCK = threading.Lock()
_SPINE_CACHE: dict[tuple[str, str], pa.Table] = {}


def name_key_sql(column: str, *, transliterate: bool = False, strip_suffixes: bool = False) -> str:
    """SQL expression that normalizes ``column`` to one match key."""
    expression = f"replace(lower(CAST({column} AS VARCHAR)), '&', ' and ')"
    for source, target in (_TRANSLITERATIONS if transliterate else ()) + _FOLDS:
        expression = f"replace({expression}, '{source}', '{target}')"
    expression = f"strip_accents({expression})"
    if strip_suffixes:
        expression = f"regexp_replace({expression}, '{_ADMIN_SUFFIX_PATTERN}', ' ', 'g')"
    return f"trim(regexp_replace({expression}, '[^a-z0-9]+', ' ', 'g'))"


def _key_select(column: str) -> str:
    return ", ".join((
        f"{name_key_sql(column)} AS k_plain",
        f"{name_key_sql(column, transliterate=True)} AS k_translit",
        f"{name_key_sql(column, strip_suffixes=True)} AS k_plain_stripped",
        f"{name_key_sql(column, transliterate=True, strip_suffixes=True)} AS k_translit_stripped",
    ))


@lru_cache(maxsize=4096)
def name_keys(text: str) -> dict[str, tuple[str, ...]]:
    """Return the exact and suffix-stripped match keys for one query text."""
    connection = lease_query_connection()
    try:
        row = connection.execute(
            f"SELECT {_key_select('v')} FROM (SELECT CAST(? AS VARCHAR) AS v)", [str(text or "")]
        ).fetchone()
    finally:
        connection.close()
    plain, translit, plain_stripped, translit_stripped = row or ("", "", "", "")
    return {
        "exact": tuple(dict.fromkeys(key for key in (plain, translit) if key)),
        "stripped": tuple(dict.fromkeys(key for key in (plain_stripped, translit_stripped) if key)),
    }


def _spine_source(country: str) -> tuple[str, list[Any], int] | None:
    """Return the catalog-selected shallow name source for one country."""
    if layout_available(country):
        bank = layout_root(country) / "admin_0_3.parquet"
        return (
            "SELECT loc_id, admin_level, name FROM read_parquet(?) WHERE admin_level BETWEEN 0 AND ?",
            [path_to_uri(bank), SPINE_MAX_LEVEL_ADOPTED],
            SPINE_MAX_LEVEL_ADOPTED,
        )
    root = graph.global_reference_graph_root()
    source = graph._table_source_for_roots("identities", [root]) if root else ""
    if not source:
        return None
    return (
        f"SELECT loc_id, CAST(admin_level AS INTEGER) AS admin_level, name "
        f"FROM read_parquet({source}, union_by_name=True) "
        f"WHERE (loc_id = ? OR loc_id LIKE ?) AND admin_level BETWEEN 0 AND ?",
        [country, f"{country}-%", SPINE_MAX_LEVEL_GLOBAL],
        SPINE_MAX_LEVEL_GLOBAL,
    )


def spine_name_table(country: str) -> tuple[pa.Table, int, frozenset[int]] | None:
    """Keyed Admin 1+ names for one country, cached per selected source.

    Returns the named rows, the deepest level the source serves, and the
    levels that have rows but no names. The table holds only identity text
    and keys (no geometry), the bounded reusable slice that name resolution
    reads on every call.
    """
    country = str(country or "").strip().upper()
    source = _spine_source(country) if country else None
    if source is None:
        return None
    sql, params, max_level = source
    cache_key = (country, repr(params))
    with _SPINE_LOCK:
        cached = _SPINE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    connection = lease_query_connection()
    try:
        table = connection.execute(
            f"""SELECT loc_id, admin_level, name, {_key_select('name')},
                       CASE WHEN admin_level = 1 THEN lower(split_part(loc_id, '-', 2)) END AS k_code
                FROM ({sql})""",
            params,
        ).arrow()
    finally:
        connection.close()
    if isinstance(table, pa.RecordBatchReader):
        table = table.read_all()
    named = pc.fill_null(pc.not_equal(table["k_plain"], ""), False)
    levels = set(table["admin_level"].to_pylist())
    named_levels = set(table.filter(named)["admin_level"].to_pylist())
    loaded = (table.filter(named), max_level, frozenset(int(level) for level in levels - named_levels))
    with _SPINE_LOCK:
        _SPINE_CACHE[cache_key] = loaded
    return loaded


def clear_name_lookup_cache() -> None:
    with _SPINE_LOCK:
        _SPINE_CACHE.clear()
    _place_alias_partitions.cache_clear()
    name_keys.cache_clear()


def _in_scope(loc_id: Any, parent: str | None, anchor: Any = None) -> bool:
    if not parent:
        return True
    for value in (loc_id, anchor):
        text = str(value or "").upper()
        if text == parent or text.startswith(parent + "-"):
            return True
    return False


def spine_candidates(
    country: str,
    query: str,
    *,
    levels: list[int] | None = None,
    parent_loc_id: str | None = None,
) -> dict[str, Any]:
    """Match a name against one country's shallow spine.

    Exact keys (and Admin 1 codes such as ``OR`` or ``BC``) win over
    suffix-stripped keys, so "Orleans Parish" never ties with Orleans County.
    Within the winning strength the shallowest level wins; several matches at
    that level are returned as ``ambiguous`` candidates. Global Baseline
    G-IDs are returned through the shared local-id crosswalk. A miss on a
    level that the source serves without names reports ``names_missing``.
    """
    loaded = spine_name_table(country)
    if loaded is None:
        return {"status": "no_source"}
    table, max_level, unnamed_levels = loaded
    parent = str(parent_loc_id or "").strip().upper() or None
    keys = name_keys(query)
    wanted_levels = [level for level in (levels or range(0, max_level + 1)) if 0 <= level <= max_level]
    if not wanted_levels:
        return {"status": "level_not_served", "max_level": max_level}
    level_mask = pc.is_in(table["admin_level"], value_set=pa.array(wanted_levels, type=table["admin_level"].type))
    for strength, columns, values in (
        ("exact", ("k_plain", "k_translit", "k_code"), keys["exact"]),
        ("stripped", ("k_plain_stripped", "k_translit_stripped"), keys["stripped"]),
    ):
        if not values:
            continue
        value_set = pa.array(values, type=pa.string())
        mask = None
        for column in columns:
            hit = pc.fill_null(pc.is_in(table[column], value_set=value_set), False)
            mask = hit if mask is None else pc.or_(mask, hit)
        rows = []
        for row in table.filter(pc.and_(mask, level_mask)).select(["loc_id", "admin_level", "name"]).to_pylist():
            row["loc_id"] = translate_geometry_id_to_local_id(str(row["loc_id"]))
            if _in_scope(row["loc_id"], parent) and str(row["loc_id"]).upper() != parent:
                rows.append(row)
        if not rows:
            continue
        unique = list({row["loc_id"]: row for row in rows}.values())
        shallowest = min(int(row["admin_level"]) for row in unique)
        best = sorted((row for row in unique if int(row["admin_level"]) == shallowest), key=lambda row: row["loc_id"])
        if len(best) == 1:
            return {"status": "matched", "match": best[0], "strength": strength}
        return {"status": "ambiguous", "candidates": best, "strength": strength}
    if unnamed_levels.intersection(wanted_levels):
        return {"status": "names_missing", "levels": sorted(unnamed_levels.intersection(wanted_levels))}
    return {"status": "not_found"}


@lru_cache(maxsize=32)
def _place_alias_partitions(root_text: str) -> tuple[str, ...]:
    """Alias partitions holding city and urban-area names for one graph."""
    root = Path(root_text)
    frame = select_rows(root / graph.PARTITION_INDEXES["aliases"], columns=["family", "path"])
    if frame is None or frame.empty or "family" not in frame.columns:
        return ()
    selected = frame[frame["family"].isin(PLACE_FAMILIES)]["path"].dropna().astype(str).tolist()
    path_map = graph._partition_source_map(root_text, "aliases")
    return tuple(dict.fromkeys(graph._sql_path(path_map.get(path, graph.DATA_ROOT / path)) for path in selected))


def place_candidates(country: str, query: str, *, parent_loc_id: str | None = None) -> dict[str, Any]:
    """Match a name against one country graph's city and urban-area names.

    Only exact keys are used; suffix stripping is an admin-spine convention.
    City names rank above urban areas; equal-ranked matches are ambiguous.
    """
    root = graph.reference_graph_roots().get(str(country or "").strip().upper())
    partitions = _place_alias_partitions(str(root)) if root else ()
    if not partitions:
        return {"status": "no_source"}
    keys = name_keys(query)["exact"]
    if not keys:
        return {"status": "not_found"}
    source = "[" + ", ".join(f"'{path}'" for path in partitions) + "]"
    connection = lease_query_connection()
    try:
        alias_rows = connection.execute(
            f"""SELECT DISTINCT loc_id, external_id, reference_system
                FROM read_parquet({source}, union_by_name=True)
                WHERE lower(alias_type) LIKE '%name%' AND lower(alias_type) NOT LIKE '%\\_id' ESCAPE '\\'
                  AND (list_contains(?, {name_key_sql('external_id')})
                       OR list_contains(?, {name_key_sql('external_id', transliterate=True)}))""",
            [list(keys), list(keys)],
        ).fetchall()
    finally:
        connection.close()
    if not alias_rows:
        return {"status": "not_found"}
    parent = str(parent_loc_id or "").strip().upper() or None
    matched_names = {str(loc_id): str(name) for loc_id, name, _system in alias_rows}
    candidates = []
    for identity in graph.identities(list(matched_names)):
        family = str(identity.get("geography_family") or identity.get("family") or "")
        if family not in PLACE_FAMILY_RANK:
            continue
        if not _in_scope(identity.get("loc_id"), parent, identity.get("sibling_anchor_loc_id")):
            continue
        candidates.append({
            "loc_id": identity["loc_id"],
            "name": identity.get("name") or matched_names.get(str(identity["loc_id"])),
            "family": family,
            "anchor_loc_id": identity.get("sibling_anchor_loc_id"),
        })
    if not candidates:
        return {"status": "not_found"}
    best_rank = min(PLACE_FAMILY_RANK[c["family"]] for c in candidates)
    best = sorted((c for c in candidates if PLACE_FAMILY_RANK[c["family"]] == best_rank), key=lambda c: c["loc_id"])
    if len(best) == 1:
        return {"status": "matched", "match": best[0]}
    return {"status": "ambiguous", "candidates": best}
