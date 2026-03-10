"""
Preprocessor - extracts hints from user queries before LLM call.

Part of the tiered context system:
- Tier 1: System prompt (cached)
- Tier 2: Preprocessor (this file, 0 LLM tokens)
- Tier 3: Just-in-time context (preprocessor hints)
- Tier 4: Reference documents (on-demand)

The preprocessor runs BEFORE the LLM call and:
1. Extracts topics from keywords
2. Resolves regions ("Europe" -> country codes)
3. Detects time patterns ("trend", "from X to Y")
4. Detects reference lookups (SDG, capitals, languages)

Output is a hints dict that can be injected into LLM context.
"""

import re
import json
from pathlib import Path
from typing import Optional
import logging

from .data_loading import load_catalog, load_source_metadata, get_source_path
from .paths import DATA_ROOT, GEOMETRY_DIR as GEOM_DIR, COUNTRIES_DIR

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIDENCE SCORING CONFIGURATION
# Adjust these values to tune the LLM candidate selection behavior
# =============================================================================

# Intent detection - data_request scoring
SCORE_DATA_KEYWORDS = 0.4       # "data", "statistics", "metrics"
SCORE_DATA_FROM = 0.2           # "from the", "from", "dataset"
SCORE_SOURCE_MENTIONED = 0.3    # Source name detected in query
SCORE_METRIC_KEYWORDS = 0.2     # "population", "gdp", "births", etc.

# Intent detection - navigation scoring
SCORE_NAV_PATTERN = 0.5         # Matches "show me", "go to", etc.
SCORE_NAV_PENALTY_DATA = -0.3   # Penalty if "data" keyword or source present
SCORE_NAV_LOCATION_ONLY = 0.3   # Location mentioned but no nav pattern

# Cross-reference adjustments (adjust_scores_with_context)
PENALTY_LOCATION_IN_SOURCE = -0.5   # Penalize location if it's part of source name (e.g., "bureau" in "Bureau of Statistics")
PENALTY_NAV_SOURCE_DETECTED = -0.3  # Reduce navigation confidence when source detected

# Source detection
SCORE_SOURCE_FULL_MATCH = 1.0   # Full source_name match
SCORE_SOURCE_ID_MATCH = 0.9     # source_id match
SCORE_SOURCE_PARTIAL_8 = 0.7    # Partial name match (>8 chars)
SCORE_SOURCE_PARTIAL_4 = 0.5    # Partial name match (4-8 chars)

# Location detection
SCORE_LOCATION_EXACT_COUNTRY = 1.0   # Exact country name
SCORE_LOCATION_CAPITAL = 0.9         # Capital city
SCORE_LOCATION_ADMIN1 = 0.8          # State/province
SCORE_LOCATION_ADMIN2_VIEWPORT = 0.5 # County in viewport
SCORE_LOCATION_PARTIAL = 0.3         # Partial word match

# Overlay intent detection - loaded from reference/disasters.json
# Use _load_disaster_overlays() to access

# =============================================================================

# Paths
CONVERSIONS_PATH = Path(__file__).parent / "conversions.json"
REFERENCE_DIR = Path(__file__).parent / "reference"
DATA_DIR = DATA_ROOT / "data"
GEOMETRY_DIR = GEOM_DIR

# Parquet cache for location lookups
_PARQUET_NAMES_CACHE = {}  # iso3 -> {name_lower: loc_id}
_PARQUET_SORTED_NAMES_CACHE = {}  # iso3 -> sorted list of names (pre-filtered, longest first)

# Reference file cache (loaded once per file)
_REFERENCE_FILE_CACHE = {}  # filepath_str -> dict

# Global.csv cache for viewport lookups
_GLOBAL_CSV_CACHE = None  # DataFrame, loaded once

# Conversions.json cache
_CONVERSIONS_CACHE = None

# Country index.json cache
_COUNTRY_INDEX_CACHE = {}  # iso3 -> dict

# Caches for reference files
_TOPICS_CACHE = None
_DISASTERS_CACHE = None


def _load_topics() -> dict:
    """
    Load topic keywords by aggregating from catalog.

    Each source in the catalog has:
    - category: broad topic (economic, health, etc.)
    - topic_tags: specific topic tags
    - keywords: search terms

    Returns dict mapping category -> list of all keywords for that category.
    """
    global _TOPICS_CACHE
    if _TOPICS_CACHE is not None:
        return _TOPICS_CACHE

    try:
        catalog = load_catalog()
        sources = catalog.get("sources", [])

        # Aggregate keywords by category
        topics_dict = {}
        for source in sources:
            category = source.get("category") or ""
            category = category.lower()
            if not category:
                continue

            if category not in topics_dict:
                topics_dict[category] = set()

            # Add topic_tags as keywords
            for tag in source.get("topic_tags", []):
                topics_dict[category].add(tag.lower())

            # Add keywords
            for kw in source.get("keywords", []):
                topics_dict[category].add(kw.lower())

        # Convert sets to lists
        _TOPICS_CACHE = {cat: list(kws) for cat, kws in topics_dict.items()}
        logger.debug(f"Loaded {len(_TOPICS_CACHE)} topic categories from catalog")
        return _TOPICS_CACHE
    except Exception as e:
        logger.warning(f"Error loading topics from catalog: {e}")
        _TOPICS_CACHE = {}
        return _TOPICS_CACHE


def _load_disaster_overlays() -> dict:
    """Load disaster overlay keywords from reference file."""
    global _DISASTERS_CACHE
    if _DISASTERS_CACHE is not None:
        return _DISASTERS_CACHE

    ref_path = REFERENCE_DIR / "disasters.json"
    try:
        with open(ref_path, encoding='utf-8') as f:
            data = json.load(f)
            overlays = data.get("overlays", {})
            # Extract just the keywords for each overlay
            _DISASTERS_CACHE = {
                overlay: info.get("keywords", [])
                for overlay, info in overlays.items()
                if not overlay.startswith("_")
            }
            logger.debug(f"Loaded {len(_DISASTERS_CACHE)} disaster overlays from reference file")
            return _DISASTERS_CACHE
    except Exception as e:
        logger.warning(f"Error loading disasters.json: {e}")
        _DISASTERS_CACHE = {}
        return _DISASTERS_CACHE

def normalize_query_for_location_matching(query: str) -> str:
    """
    Normalize query to improve location matching.

    Handles:
    - Possessive forms: "australia's" -> "australia", "australias" -> "australia"
    - Trailing apostrophe: "texas'" -> "texas"
    """
    # Remove apostrophe-s possessive
    query = re.sub(r"'s\b", "", query)
    # Handle trailing apostrophe (e.g., "texas'")
    query = re.sub(r"'\b", "", query)
    # Handle informal possessive without apostrophe (e.g., "australias" -> "australia")
    # Only for words that look like country names (capitalized or at word boundary)
    # This pattern matches word + trailing 's' when followed by common words
    query = re.sub(r'\b(\w+?)s\s+(population|gdp|economy|data|capital|government|president|leader)',
                   r'\1 \2', query, flags=re.IGNORECASE)
    return query


def load_conversions() -> dict:
    """Load conversions.json for region resolution. Cached after first load."""
    global _CONVERSIONS_CACHE

    if _CONVERSIONS_CACHE is not None:
        return _CONVERSIONS_CACHE

    if CONVERSIONS_PATH.exists():
        with open(CONVERSIONS_PATH, encoding='utf-8') as f:
            _CONVERSIONS_CACHE = json.load(f)
            logger.debug("Cached conversions.json")
            return _CONVERSIONS_CACHE

    _CONVERSIONS_CACHE = {}
    return {}


def load_reference_file(filepath: Path) -> Optional[dict]:
    """Load a reference JSON file if it exists. Cached after first load."""
    global _REFERENCE_FILE_CACHE

    cache_key = str(filepath)
    if cache_key in _REFERENCE_FILE_CACHE:
        return _REFERENCE_FILE_CACHE[cache_key]

    if filepath.exists():
        with open(filepath, encoding='utf-8') as f:
            data = json.load(f)
            _REFERENCE_FILE_CACHE[cache_key] = data
            logger.debug(f"Cached reference file: {filepath.name}")
            return data

    _REFERENCE_FILE_CACHE[cache_key] = None
    return None


# =============================================================================
# Viewport-based Location Lookup
# =============================================================================

def get_countries_in_viewport(bounds: dict) -> list:
    """
    Get list of ISO3 codes for countries visible in viewport.
    Uses global.csv bounding boxes for fast filtering.
    DataFrame is cached after first load.
    """
    global _GLOBAL_CSV_CACHE

    if not bounds:
        return []

    # Load and cache global.csv DataFrame
    if _GLOBAL_CSV_CACHE is None:
        global_csv = GEOMETRY_DIR / "global.csv"
        if not global_csv.exists():
            return []
        try:
            import pandas as pd
            _GLOBAL_CSV_CACHE = pd.read_csv(global_csv)
            logger.debug(f"Cached global.csv with {len(_GLOBAL_CSV_CACHE)} countries")
        except Exception as e:
            logger.warning(f"Error loading global.csv: {e}")
            return []

    try:
        df = _GLOBAL_CSV_CACHE

        # Viewport bounds
        v_west = bounds.get("west", -180)
        v_south = bounds.get("south", -90)
        v_east = bounds.get("east", 180)
        v_north = bounds.get("north", 90)

        # Filter by bounding box intersection
        if 'bbox_min_lon' in df.columns:
            mask = (
                (df['bbox_max_lon'] >= v_west) &
                (df['bbox_min_lon'] <= v_east) &
                (df['bbox_max_lat'] >= v_south) &
                (df['bbox_min_lat'] <= v_north)
            )
            df = df[mask]

        return df['loc_id'].tolist() if 'loc_id' in df.columns else []
    except Exception as e:
        logger.warning(f"Error getting countries in viewport: {e}")
        return []


def load_parquet_names(iso3: str) -> dict:
    """
    Load location names from a country's parquet file.
    Returns dict of {name_lower: [list of location dicts]}
    Multiple locations can share the same name (e.g., 30+ Washington Counties).
    Cached per ISO3 code.
    """
    global _PARQUET_NAMES_CACHE

    if iso3 in _PARQUET_NAMES_CACHE:
        return _PARQUET_NAMES_CACHE[iso3]

    parquet_file = GEOMETRY_DIR / f"{iso3}.parquet"
    if not parquet_file.exists():
        _PARQUET_NAMES_CACHE[iso3] = {}
        return {}

    try:
        import pandas as pd
        from .duckdb_helpers import duckdb_available, select_columns_from_parquet
        # Only load name columns, not geometry (much faster)
        columns = ['loc_id', 'name', 'parent_id', 'admin_level']
        if duckdb_available():
            df = select_columns_from_parquet(parquet_file, columns)
            if df.empty:
                df = pd.read_parquet(parquet_file, columns=columns)
        else:
            df = pd.read_parquet(parquet_file, columns=columns)

        names_dict = {}
        for _, row in df.iterrows():
            name = row.get('name')
            if name and isinstance(name, str):
                name_lower = name.lower()
                location_info = {
                    "loc_id": row.get('loc_id'),
                    "parent_id": row.get('parent_id'),
                    "admin_level": row.get('admin_level')
                }
                # Store as list to handle duplicate names (e.g., Washington County in 30+ states)
                if name_lower not in names_dict:
                    names_dict[name_lower] = []
                names_dict[name_lower].append(location_info)

        _PARQUET_NAMES_CACHE[iso3] = names_dict
        logger.debug(f"Loaded {len(names_dict)} unique location names from {iso3}.parquet")
        return names_dict
    except Exception as e:
        logger.warning(f"Error loading parquet names for {iso3}: {e}")
        _PARQUET_NAMES_CACHE[iso3] = {}
        return {}


def get_sorted_location_names(iso3: str) -> list:
    """
    Get pre-sorted list of location names for a country (cached).
    Names are sorted by length (longest first) and filtered to remove
    numbers and single characters.
    """
    global _PARQUET_SORTED_NAMES_CACHE

    if iso3 in _PARQUET_SORTED_NAMES_CACHE:
        return _PARQUET_SORTED_NAMES_CACHE[iso3]

    # Get the names dict first (also cached)
    names = load_parquet_names(iso3)
    if not names:
        _PARQUET_SORTED_NAMES_CACHE[iso3] = []
        return []

    # Build sorted list once and cache it
    sorted_names = sorted(
        [n for n in names.keys()
         if not n.isdigit()
         and len(n) >= 2],
        key=len, reverse=True
    )

    _PARQUET_SORTED_NAMES_CACHE[iso3] = sorted_names
    logger.debug(f"Cached {len(sorted_names)} sorted location names for {iso3}")
    return sorted_names


def search_locations_globally(name: str, admin_level: int = None, limit_countries: list = None) -> list:
    """
    Search for locations by name across all parquet files globally.
    Used when viewport-based search fails or isn't available.

    Args:
        name: Location name to search for (case-insensitive)
        admin_level: Optional admin level to filter by (1=state, 2=county, 3=city)
        limit_countries: Optional list of ISO3 codes to limit search to

    Returns:
        List of match dicts with loc_id, matched_term, iso3, admin_level, etc.
    """
    name_lower = name.lower().strip()
    all_matches = []

    # Get list of countries to search
    if limit_countries:
        countries = limit_countries
    else:
        # Search common large countries first, then others
        priority_countries = ["USA", "CAN", "GBR", "AUS", "DEU", "FRA", "IND", "BRA", "MEX"]
        other_countries = []

        # Get all available parquet files
        if GEOMETRY_DIR.exists():
            for f in GEOMETRY_DIR.glob("*.parquet"):
                iso3 = f.stem
                if iso3 not in priority_countries:
                    other_countries.append(iso3)

        countries = priority_countries + other_countries

    # Search each country's parquet
    iso_data = load_reference_file(REFERENCE_DIR / "iso_codes.json")

    for iso3 in countries:
        names = load_parquet_names(iso3)
        if not names:
            continue

        # Look for exact name match - names[name_lower] is now a LIST of locations
        if name_lower in names:
            locations_list = names[name_lower]
            country_name = iso_data.get("iso3_to_name", {}).get(iso3, iso3) if iso_data else iso3

            for info in locations_list:
                # Check admin level filter
                if admin_level is not None and info.get("admin_level") != admin_level:
                    continue

                all_matches.append({
                    "matched_term": name_lower,
                    "iso3": iso3,
                    "country_name": country_name,
                    "loc_id": info.get("loc_id"),
                    "parent_id": info.get("parent_id"),
                    "admin_level": info.get("admin_level", 0),
                    "is_subregion": info.get("admin_level", 0) > 0
                })

    return all_matches


def lookup_location_in_viewport(query: str, viewport: dict = None) -> dict:
    """
    Search for a location name in parquet files, scoped by viewport.

    Args:
        query: User query text
        viewport: Optional viewport dict with bounds and adminLevel

    Returns:
        Dict with:
        - "match": (matched_term, iso3, is_subregion) if single match
        - "matches": list of all matches if multiple found
        - "ambiguous": True if multiple matches need disambiguation
        - None values if no match found
    """
    query_lower = query.lower()
    result = {"match": None, "matches": [], "ambiguous": False}

    # Determine which countries to search
    countries_to_search = []

    if viewport and viewport.get("bounds"):
        # Use viewport to scope search
        countries_to_search = get_countries_in_viewport(viewport["bounds"])
        if not countries_to_search:
            # Viewport might be too small or no global.csv
            logger.debug("No countries in viewport, falling back to global search")
            return result
    else:
        # No viewport - this is handled by existing extract_country_from_query
        return result

    all_matches = []

    # Search for location names in visible countries' parquets
    for iso3 in countries_to_search:
        names = load_parquet_names(iso3)
        if not names:
            continue

        # Get country name for display (once per country)
        iso_data = load_reference_file(REFERENCE_DIR / "iso_codes.json")
        country_name = iso_data.get("iso3_to_name", {}).get(iso3, iso3) if iso_data else iso3

        # Use cached sorted names (filtered and sorted by length, longest first)
        sorted_names = get_sorted_location_names(iso3)

        for name in sorted_names:
            # Use word boundary matching
            pattern = r'\b' + re.escape(name) + r'\b'
            if re.search(pattern, query_lower):
                # names[name] is now a LIST of locations
                locations_list = names[name]
                for info in locations_list:
                    is_subregion = info.get("admin_level", 0) > 0

                    all_matches.append({
                        "matched_term": name,
                        "iso3": iso3,
                        "country_name": country_name,
                        "loc_id": info.get("loc_id"),
                        "admin_level": info.get("admin_level", 0),
                        "is_subregion": is_subregion
                    })

    if len(all_matches) == 0:
        return result
    elif len(all_matches) == 1:
        m = all_matches[0]
        result["match"] = (m["matched_term"], m["iso3"], m["is_subregion"])
        result["matches"] = all_matches
    else:
        # Multiple matches - need disambiguation
        result["matches"] = all_matches
        result["ambiguous"] = True
        # Still provide first match as default, but flag ambiguity
        m = all_matches[0]
        result["match"] = (m["matched_term"], m["iso3"], m["is_subregion"])

    return result


# =============================================================================
# Country Name Extraction
# =============================================================================

# Caches for lookups (built once on first use)
_NAME_TO_ISO3_CACHE = None
_SUBREGION_TO_ISO3_CACHE = None


def build_name_to_iso3() -> dict:
    """Build reverse lookup from country name to ISO3 code."""
    global _NAME_TO_ISO3_CACHE
    if _NAME_TO_ISO3_CACHE is not None:
        return _NAME_TO_ISO3_CACHE

    iso_path = REFERENCE_DIR / "iso_codes.json"
    name_to_iso3 = {}

    if iso_path.exists():
        data = load_reference_file(iso_path)
        iso3_to_name = data.get("iso3_to_name", {})

        for iso3, name in iso3_to_name.items():
            # Primary name (lowercase for matching)
            name_to_iso3[name.lower()] = iso3

            # Also add without common suffixes/prefixes for fuzzy matching
            clean_name = name.lower()
            for suffix in [" islands", " island", " republic", " federation"]:
                if clean_name.endswith(suffix):
                    name_to_iso3[clean_name.replace(suffix, "").strip()] = iso3

    # Add common aliases
    aliases = {
        "usa": "USA", "us": "USA", "united states": "USA", "america": "USA",
        "uk": "GBR", "britain": "GBR", "england": "GBR",
        "russia": "RUS", "ussr": "RUS",
        "korea": "KOR", "south korea": "KOR",
        "north korea": "PRK", "dprk": "PRK",
        "taiwan": "TWN", "republic of china": "TWN",
        "iran": "IRN", "persia": "IRN",
        "syria": "SYR",
        "uae": "ARE", "emirates": "ARE",
        "vietnam": "VNM", "viet nam": "VNM",
        "congo": "COD", "drc": "COD",
        "ivory coast": "CIV", "cote d'ivoire": "CIV",
        "turkey": "TUR", "turkiye": "TUR",
        "holland": "NLD", "netherlands": "NLD",
        "czech republic": "CZE", "czechia": "CZE",
    }
    name_to_iso3.update(aliases)

    _NAME_TO_ISO3_CACHE = name_to_iso3
    return name_to_iso3


def build_subregion_to_iso3() -> dict:
    """
    Build lookup from capitals to parent country ISO3.

    Capitals are loaded from reference file.
    Other locations are recognized by the LLM (no preprocessing lookup needed).
    """
    global _SUBREGION_TO_ISO3_CACHE
    if _SUBREGION_TO_ISO3_CACHE is not None:
        return _SUBREGION_TO_ISO3_CACHE

    subregion_to_iso3 = {}

    # Load capitals from country_metadata.json
    metadata_path = REFERENCE_DIR / "country_metadata.json"
    if metadata_path.exists():
        data = load_reference_file(metadata_path)
        capitals = data.get("capitals", {})
        for iso3, capital in capitals.items():
            if isinstance(capital, str) and capital and not capital.startswith("_"):
                subregion_to_iso3[capital.lower()] = iso3

    # Major cities are no longer hardcoded - resolved dynamically from
    # geometry parquet files using viewport context

    _SUBREGION_TO_ISO3_CACHE = subregion_to_iso3
    return subregion_to_iso3


def extract_country_from_query(query: str, viewport: dict = None) -> dict:
    """
    Extract country from query using hierarchical resolution.

    Resolution order:
    1. Direct country name match (from reference file)
    2. Capital city match (from reference file)
    3. Viewport-based location match (from geometry parquet files)

    Args:
        query: User query text
        viewport: Optional viewport dict with bounds and adminLevel

    Returns dict with:
        - match: (matched_term, ISO3, is_subregion) tuple if found
        - ambiguous: True if multiple matches need disambiguation
        - matches: list of all matches (for disambiguation display)
        - source: "country", "capital", or "viewport" indicating match source
    """
    result = {"match": None, "ambiguous": False, "matches": [], "source": None}

    # Normalize query to handle possessive forms (australias -> australia)
    normalized_query = normalize_query_for_location_matching(query)
    query_lower = normalized_query.lower()

    # First try direct country match
    name_to_iso3 = build_name_to_iso3()
    sorted_names = sorted(name_to_iso3.keys(), key=len, reverse=True)

    for name in sorted_names:
        pattern = r'\b' + re.escape(name) + r'\b'
        if re.search(pattern, query_lower):
            iso3 = name_to_iso3[name]
            result["match"] = (name, iso3, False)
            result["source"] = "country"
            return result

    # Try capital cities from reference file
    subregion_to_iso3 = build_subregion_to_iso3()
    sorted_subregions = sorted(subregion_to_iso3.keys(), key=len, reverse=True)

    for subregion in sorted_subregions:
        pattern = r'\b' + re.escape(subregion) + r'\b'
        if re.search(pattern, query_lower):
            iso3 = subregion_to_iso3[subregion]
            result["match"] = (subregion, iso3, True)
            result["source"] = "capital"
            return result

    # Viewport-based lookup DISABLED for performance
    # Country/capital matching above handles common cases.
    # The LLM handles location recognition; validation in postprocessor.

    return result


# =============================================================================
# Source/Metric Hints for Context Injection
# =============================================================================

def load_country_index(iso3: str) -> Optional[dict]:
    """
    Load a country's index.json file for context injection.
    Contains llm_summary and dataset categories. Cached per country.
    """
    global _COUNTRY_INDEX_CACHE

    iso3_upper = iso3.upper()
    if iso3_upper in _COUNTRY_INDEX_CACHE:
        return _COUNTRY_INDEX_CACHE[iso3_upper]

    # Try country index path
    index_path = COUNTRIES_DIR / iso3_upper / "index.json"

    if index_path.exists():
        try:
            with open(index_path, encoding='utf-8') as f:
                data = json.load(f)
                _COUNTRY_INDEX_CACHE[iso3_upper] = data
                logger.debug(f"Cached country index: {iso3_upper}")
                return data
        except Exception:
            pass

    _COUNTRY_INDEX_CACHE[iso3_upper] = None
    return None


def get_relevant_sources_with_metrics(topics: list, iso3: str = None) -> dict:
    """
    Find relevant sources based on detected topics and location.
    Returns dict with sources list and optional country context.

    Args:
        topics: List of detected topic names (e.g., ["demographics", "economy"])
        iso3: ISO3 country code if location was detected (e.g., "AUS")

    Returns:
        Dict with:
        - sources: List of relevant sources with metric names (FULL list for country sources)
        - country_summary: llm_summary from country index.json (if available)
        - country_index: Full country index data (datasets, admin_levels, etc.)
    """
    result = {"sources": [], "country_summary": None, "country_index": None}

    # Load country index.json for full context if location specified
    if iso3:
        country_index = load_country_index(iso3)
        if country_index:
            result["country_summary"] = country_index.get("llm_summary")
            # Include full index data for LLM context
            result["country_index"] = {
                "datasets": country_index.get("datasets", []),
                "admin_levels": country_index.get("admin_levels", []),
                "admin_counts": country_index.get("admin_counts", {}),
            }

    catalog = load_catalog()
    sources = catalog.get("sources", [])
    relevant = []

    # Map topics to common keywords for matching source topic_tags/keywords
    topic_keywords = {
        "demographics": ["population", "demographics", "census", "age", "birth", "death"],
        "economy": ["economic", "economy", "gdp", "income", "trade"],
        "health": ["health", "disease", "mortality", "medical"],
        "environment": ["environment", "climate", "emissions", "co2", "energy"],
        "education": ["education", "literacy", "school"],
        "development": ["sdg", "development", "sustainable"],
        "hazard": ["earthquake", "volcano", "hurricane", "cyclone", "wildfire", "fire", "flood", "tsunami", "storm", "disaster", "hazard"],
    }

    # Build list of keywords to match from detected topics
    keywords_to_match = []
    for topic in topics:
        keywords_to_match.extend(topic_keywords.get(topic, [topic]))

    for source in sources:
        source_id = source.get("source_id", "")
        scope = source.get("scope", "global")
        topic_tags = source.get("topic_tags", [])
        source_keywords = source.get("keywords", [])
        metrics = source.get("metrics", {})

        # Determine if this source should be included
        # Logic: include if (scope matches location) OR (topics match source keywords)
        include_source = False
        is_country_source = (iso3 and scope.lower() == iso3.lower())
        is_global_source = (scope == "global")

        # Check topic match (for any source type)
        topic_matches = False
        if keywords_to_match:
            all_source_keywords = [t.lower() for t in topic_tags + source_keywords]
            for kw in keywords_to_match:
                if any(kw.lower() in sk for sk in all_source_keywords):
                    topic_matches = True
                    break

        if iso3:
            # Location specified: include ALL country sources + topic-matched global
            if is_country_source:
                include_source = True  # All sources for this country
            elif is_global_source and topic_matches:
                include_source = True  # Global sources only if topic matches
        else:
            # No location: include ANY source that matches topics (country or global)
            if topic_matches:
                include_source = True

        if not include_source:
            continue

        # For country-specific sources, load FULL metadata to get ALL metrics
        # For global sources, use catalog (trimmed) to avoid loading too many files
        metric_list = []
        if is_country_source:
            # Load full metadata.json for complete metric list
            full_metadata = load_source_metadata(source_id)
            if full_metadata:
                full_metrics = full_metadata.get("metrics", {})
                for metric_key, metric_info in full_metrics.items():
                    metric_name = metric_info.get("name", metric_key)
                    metric_list.append({
                        "column": metric_key,
                        "name": metric_name,
                        "unit": metric_info.get("unit", "")
                    })
            else:
                # Fallback to catalog if metadata not found
                for metric_key, metric_info in metrics.items():
                    metric_list.append({
                        "column": metric_key,
                        "name": metric_info.get("name", metric_key),
                        "unit": metric_info.get("unit", "")
                    })
        else:
            # Global sources - use catalog (trimmed to avoid bloat)
            for metric_key, metric_info in metrics.items():
                metric_name = metric_info.get("name", metric_key)
                metric_list.append({
                    "column": metric_key,
                    "name": metric_name,
                    "unit": metric_info.get("unit", "")
                })

        if metric_list:
            relevant.append({
                "source_id": source_id,
                "source_name": source.get("source_name", source_id),
                "scope": scope,
                "metrics": metric_list,
                "is_country_source": is_country_source  # Flag for tier3 context
            })

    result["sources"] = relevant
    return result


# =============================================================================
# Topic Extraction
# =============================================================================

# Topic keywords - derived from catalog.json (category, topic_tags, keywords)
# Use _load_topics() to access


def extract_topics(query: str) -> list:
    """
    Extract topic categories from query based on keywords.

    Returns list of topic names that match.
    """
    query_lower = query.lower()
    matched_topics = []

    topics = _load_topics()
    for topic, keywords in topics.items():
        if any(kw in query_lower for kw in keywords):
            matched_topics.append(topic)

    return matched_topics


# =============================================================================
# Region Resolution
# =============================================================================

# Region aliases - loaded from conversions.json
# Use _get_region_aliases() to access

_REGION_ALIASES_CACHE = None


def _get_region_aliases() -> dict:
    """Load region aliases from conversions.json."""
    global _REGION_ALIASES_CACHE
    if _REGION_ALIASES_CACHE is not None:
        return _REGION_ALIASES_CACHE

    conversions = load_conversions()
    regions = conversions.get("regions", {})

    # Build alias lookup from region names to their keys
    aliases = {}
    for region_key, region_data in regions.items():
        if isinstance(region_data, dict):
            # Add the display name as an alias
            display_name = region_data.get("name", region_key).lower()
            aliases[display_name] = region_key
            # Add synonyms if present
            for synonym in region_data.get("synonyms", []):
                aliases[synonym.lower()] = region_key

    # Add common shortcuts
    aliases.update({
        "europe": "WHO_European_Region",
        "european": "WHO_European_Region",
        "africa": "WHO_African_Region",
        "african": "WHO_African_Region",
        "sub-saharan africa": "Sub_Saharan_Africa",
        "asia": "WHO_South_East_Asia_Region",
        "middle east": "WHO_Eastern_Mediterranean_Region",
        "americas": "WHO_Region_of_the_Americas",
        "latin america": "Latin_America",
        "south america": "South_America",
        "north america": "North_America",
        "g7": "G7",
        "g20": "G20",
        "oecd": "OECD",
        "eu": "European_Union",
        "european union": "European_Union",
        "nordic": "Nordic_Countries",
        "brics": "BRICS",
        "developed": "High_Income",
        "developing": "Lower_Middle_Income",
        "high income": "High_Income",
        "low income": "Low_Income",
    })

    _REGION_ALIASES_CACHE = aliases
    return _REGION_ALIASES_CACHE


def resolve_regions(query: str) -> list:
    """
    Detect region mentions in query and resolve to grouping names.

    Returns list of dicts with region info.
    Uses word boundaries to avoid false positives.
    """
    query_lower = query.lower()
    conversions = load_conversions()
    groupings = conversions.get("regional_groupings", {})

    resolved = []

    # Check aliases first - use word boundaries to avoid partial matches
    region_aliases = _get_region_aliases()
    for alias, grouping_name in region_aliases.items():
        # Use regex with word boundaries
        pattern = r'\b' + re.escape(alias) + r'\b'
        if re.search(pattern, query_lower):
            if grouping_name in groupings:
                group_data = groupings[grouping_name]
                resolved.append({
                    "match": alias,
                    "grouping": grouping_name,
                    "code": group_data.get("code"),
                    "countries": group_data.get("countries", []),
                    "count": len(group_data.get("countries", []))
                })

    # Also check for grouping names directly (e.g., "WHO_African_Region")
    for grouping_name, group_data in groupings.items():
        # Check if grouping name is mentioned (with word boundaries)
        name_lower = grouping_name.lower().replace("_", " ")
        name_pattern = r'\b' + re.escape(name_lower) + r'\b'

        # Check if code is mentioned (only for codes 3+ chars, with word boundaries)
        code = group_data.get("code", "").lower()
        code_matched = False
        if code and len(code) >= 3:
            code_pattern = r'\b' + re.escape(code) + r'\b'
            code_matched = bool(re.search(code_pattern, query_lower))

        if re.search(name_pattern, query_lower) or code_matched:
            # Avoid duplicates from alias resolution
            if not any(r["grouping"] == grouping_name for r in resolved):
                resolved.append({
                    "match": grouping_name,
                    "grouping": grouping_name,
                    "code": group_data.get("code"),
                    "countries": group_data.get("countries", []),
                    "count": len(group_data.get("countries", []))
                })

    return resolved


# =============================================================================
# Time Pattern Detection
# =============================================================================

TIME_PATTERNS = {
    "year_range": [
        r"from\s+(\d{4})\s+to\s+(\d{4})",
        r"between\s+(\d{4})\s+and\s+(\d{4})",
        r"(\d{4})\s*[-to]+\s*(\d{4})",
    ],
    "year_to_now": [
        # "from 2010 to now", "2015 to present", "from 2000 until now"
        r"from\s+(\d{4})\s+(?:to|until)\s+(?:now|present|today|current)",
        r"(\d{4})\s+(?:to|until)\s+(?:now|present|today|current)",
    ],
    "trend_indicators": [
        r"\btrend\b",
        r"\bover time\b",
        r"\bhistor(?:y|ical)\b",
        r"\bchange\b",
        r"\bgrowth\b",
        r"\bdecline\b",
        r"\ball\s+(?:the\s+)?years?\b",
        r"\bevery\s+year\b",
        r"\bacross\s+(?:all\s+)?years?\b",
    ],
    "last_n_years": [
        r"last\s+(\d+)\s+years?",
        r"past\s+(\d+)\s+years?",
    ],
    "since_year": [
        r"since\s+(\d{4})",
        r"from\s+(\d{4})",
    ],
    "single_year": [
        r"\bin\s+(\d{4})\b",
        r"\bfor\s+(\d{4})\b",
        r"\b(\d{4})\s+data\b",
    ],
}


def detect_time_patterns(query: str) -> dict:
    """
    Detect time-related patterns in query.

    Returns dict with:
    - is_time_series: bool
    - year_start: int or None
    - year_end: int or None
    - pattern_type: str describing what was detected
    """
    result = {
        "is_time_series": False,
        "year_start": None,
        "year_end": None,
        "pattern_type": None,
    }

    query_lower = query.lower()

    # Check for explicit year ranges
    for pattern in TIME_PATTERNS["year_range"]:
        match = re.search(pattern, query_lower)
        if match:
            result["is_time_series"] = True
            result["year_start"] = int(match.group(1))
            result["year_end"] = int(match.group(2))
            result["pattern_type"] = "year_range"
            return result

    # Check for "year to now" patterns (e.g., "from 2010 to now")
    for pattern in TIME_PATTERNS["year_to_now"]:
        match = re.search(pattern, query_lower)
        if match:
            result["is_time_series"] = True
            result["year_start"] = int(match.group(1))
            result["year_end"] = 2024  # Current year
            result["pattern_type"] = "year_to_now"
            return result

    # Check for trend indicators
    for pattern in TIME_PATTERNS["trend_indicators"]:
        if re.search(pattern, query_lower):
            result["is_time_series"] = True
            result["pattern_type"] = "trend"
            # Could set default range here, or let LLM decide
            break

    # Check for "last N years"
    for pattern in TIME_PATTERNS["last_n_years"]:
        match = re.search(pattern, query_lower)
        if match:
            result["is_time_series"] = True
            n_years = int(match.group(1))
            result["year_end"] = 2024  # Current year
            result["year_start"] = 2024 - n_years
            result["pattern_type"] = "last_n_years"
            return result

    # Check for "since YYYY"
    for pattern in TIME_PATTERNS["since_year"]:
        match = re.search(pattern, query_lower)
        if match:
            result["is_time_series"] = True
            result["year_start"] = int(match.group(1))
            result["year_end"] = 2024
            result["pattern_type"] = "since_year"
            return result

    # Check for single year (not time series)
    for pattern in TIME_PATTERNS["single_year"]:
        match = re.search(pattern, query_lower)
        if match:
            year = int(match.group(1))
            if 1900 < year < 2100:  # Sanity check
                result["year_start"] = year
                result["year_end"] = year
                result["pattern_type"] = "single_year"
                return result

    return result


# =============================================================================
# Reference Lookup Detection
# =============================================================================

def lookup_country_specific_data(ref_type: str, iso3: str, country_name: str) -> Optional[dict]:
    """
    Look up specific country data from reference files.

    Returns dict with the specific data, or None if not found.
    """
    if ref_type == "currency":
        ref_path = REFERENCE_DIR / "currencies_scraped.json"
        if ref_path.exists():
            data = load_reference_file(ref_path)
            currencies = data.get("currencies", {})
            if iso3 in currencies:
                currency = currencies[iso3]
                return {
                    "country": country_name,
                    "iso3": iso3,
                    "currency_code": currency.get("code"),
                    "currency_name": currency.get("name"),
                    "formatted": f"{country_name} uses {currency.get('name')} ({currency.get('code')})"
                }

    elif ref_type == "language":
        ref_path = REFERENCE_DIR / "languages_scraped.json"
        if ref_path.exists():
            data = load_reference_file(ref_path)
            languages = data.get("languages", {})
            if iso3 in languages:
                lang_data = languages[iso3]
                official = lang_data.get("official", [])
                all_langs = lang_data.get("languages", [])
                return {
                    "country": country_name,
                    "iso3": iso3,
                    "official_languages": official,
                    "all_languages": all_langs,
                    "formatted": f"{country_name}: Official language(s): {', '.join(official) if official else 'N/A'}. All languages: {', '.join(all_langs[:5]) if all_langs else 'N/A'}"
                }

    elif ref_type == "timezone":
        ref_path = REFERENCE_DIR / "timezones_scraped.json"
        if ref_path.exists():
            data = load_reference_file(ref_path)
            timezones = data.get("timezones", {})
            if iso3 in timezones:
                tz_data = timezones[iso3]
                return {
                    "country": country_name,
                    "iso3": iso3,
                    "utc_offset": tz_data.get("utc_offset"),
                    "has_dst": tz_data.get("has_dst"),
                    "num_timezones": tz_data.get("num_timezones"),
                    "formatted": f"{country_name}: {tz_data.get('utc_offset')}" + (f" (DST observed)" if tz_data.get("has_dst") else "") + (f" ({tz_data.get('num_timezones')} time zones)" if tz_data.get("num_timezones", 1) > 1 else "")
                }

    elif ref_type == "capital":
        ref_path = REFERENCE_DIR / "country_metadata.json"
        if ref_path.exists():
            data = load_reference_file(ref_path)
            capitals = data.get("capitals", {})
            if iso3 in capitals:
                capital = capitals[iso3]
                return {
                    "country": country_name,
                    "iso3": iso3,
                    "capital": capital,
                    "formatted": f"The capital of {country_name} is {capital}" if capital else f"Capital not found for {country_name}"
                }

    return None


def detect_reference_lookup(query: str) -> Optional[dict]:
    """
    Detect if query is asking for reference information.

    Returns dict with reference file path, type, and specific country data if found.
    """
    query_lower = query.lower()

    # Currency analytics should route to data orders, not reference lookup.
    currency_analytics_terms = [
        "against usd", "vs usd", "drop", "depreciat", "appreciat", "volatility",
        "single year", "over the last", "trend", "time series", "change", "percent",
        "over time", "since ", "between ", "compare"
    ]
    is_currency_analytics = ("currency" in query_lower or "fx" in query_lower or "exchange rate" in query_lower) and any(
        term in query_lower for term in currency_analytics_terms
    )

    # System help pattern - "how do you work?", "what can you do?", "help", etc.
    help_keywords = [
        "how do you work", "how does this work", "what can you do",
        "what can i do", "what can i ask", "how do i use",
        "how to use", "what is this", "what are you",
        "tell me about yourself", "help me", "what do you do",
        "how do i ask", "what questions can i",
    ]
    # Match exact "help" but not "help me find earthquakes" (only short help queries)
    is_short_help = query_lower.strip() in ["help", "?", "help me", "how"]
    if is_short_help or any(kw in query_lower for kw in help_keywords):
        ref_path = REFERENCE_DIR / "system_help.json"
        if ref_path.exists():
            return {
                "type": "system_help",
                "file": str(ref_path),
                "content": load_reference_file(ref_path)
            }

    # SDG pattern - "What is SDG 7?" or "goal 7" or "sustainable development goal 7"
    sdg_match = re.search(r'sdg\s*(\d+)|goal\s*(\d+)|sustainable development goal\s*(\d+)', query_lower)
    if sdg_match:
        num = sdg_match.group(1) or sdg_match.group(2) or sdg_match.group(3)
        goal_num = int(num)
        # Dynamically find SDG source path from catalog
        goal_tag = f"goal{goal_num}"
        catalog = load_catalog()
        if catalog:
            for source in catalog.get("sources", []):
                topic_tags = source.get("topic_tags", [])
                if goal_tag in topic_tags:
                    source_path = get_source_path(source.get("source_id"))
                    if source_path:
                        ref_path = source_path / "reference.json"
                        if ref_path.exists():
                            return {
                                "type": "sdg",
                                "sdg_number": goal_num,
                                "file": str(ref_path),
                                "content": load_reference_file(ref_path)
                            }
                    break

    # Extract country from query for country-specific lookups
    # Returns dict with match tuple and disambiguation info
    country_result = extract_country_from_query(query)
    if country_result.get("match"):
        matched_term, iso3, is_subregion = country_result["match"]
        # Get proper country name from ISO3 for display
        iso_data = load_reference_file(REFERENCE_DIR / "iso_codes.json")
        country_name = iso_data.get("iso3_to_name", {}).get(iso3, matched_term.title()) if iso_data else matched_term.title()
    else:
        matched_term = None
        iso3 = None
        is_subregion = False
        country_name = None

    # Capital pattern - "What is the capital of X?"
    if any(kw in query_lower for kw in ["capital of", "capital city"]):
        result = {
            "type": "capital",
            "file": str(REFERENCE_DIR / "country_metadata.json"),
        }
        if iso3:
            specific = lookup_country_specific_data("capital", iso3, country_name)
            if specific:
                result["country_data"] = specific
        return result

    # Currency pattern - use scraped World Factbook data
    if any(kw in query_lower for kw in ["currency", "money in", "monetary unit"]) and not is_currency_analytics:
        result = {
            "type": "currency",
            "file": str(REFERENCE_DIR / "currencies_scraped.json"),
        }
        if iso3:
            specific = lookup_country_specific_data("currency", iso3, country_name)
            if specific:
                result["country_data"] = specific
        return result

    # Language pattern - use scraped World Factbook data
    if any(kw in query_lower for kw in ["language", "speak", "spoken", "official language"]):
        result = {
            "type": "language",
            "file": str(REFERENCE_DIR / "languages_scraped.json"),
        }
        if iso3:
            specific = lookup_country_specific_data("language", iso3, country_name)
            if specific:
                result["country_data"] = specific
        return result

    # Timezone pattern - use scraped World Factbook data
    if any(kw in query_lower for kw in ["timezone", "time zone", "what time"]):
        result = {
            "type": "timezone",
            "file": str(REFERENCE_DIR / "timezones_scraped.json"),
        }
        if iso3:
            specific = lookup_country_specific_data("timezone", iso3, country_name)
            if specific:
                result["country_data"] = specific
        return result

    # Country background/overview pattern - "tell me about France" or "history of Germany"
    background_keywords = ["background", "history of", "tell me about", "overview of", "about the country"]
    if iso3 and any(kw in query_lower for kw in background_keywords):
        ref_path = REFERENCE_DIR / "world_factbook_text.json"
        if ref_path.exists():
            data = load_reference_file(ref_path)
            countries = data.get("countries", {})
            if iso3 in countries:
                country_data = countries[iso3]
                background = country_data.get("background", "")
                if background:
                    # Truncate long backgrounds for context injection
                    summary = background[:800] + "..." if len(background) > 800 else background
                    return {
                        "type": "country_info",
                        "file": str(ref_path),
                        "country_data": {
                            "country": country_name,
                            "iso3": iso3,
                            "background": background,
                            "formatted": f"{country_name} Background: {summary}"
                        }
                    }

    # Economy pattern - "economy of France" or "economic overview of Germany"
    if iso3 and any(kw in query_lower for kw in ["economy", "economic", "industries", "gdp"]):
        ref_path = REFERENCE_DIR / "world_factbook_text.json"
        if ref_path.exists():
            data = load_reference_file(ref_path)
            countries = data.get("countries", {})
            if iso3 in countries:
                country_data = countries[iso3]
                econ = country_data.get("economic_overview", "")
                industries = country_data.get("industries", "")
                if econ or industries:
                    parts = []
                    if econ:
                        parts.append(f"Economic Overview: {econ[:400]}")
                    if industries:
                        parts.append(f"Industries: {industries}")
                    return {
                        "type": "economy_info",
                        "file": str(ref_path),
                        "country_data": {
                            "country": country_name,
                            "iso3": iso3,
                            "economic_overview": econ,
                            "industries": industries,
                            "formatted": f"{country_name} - " + "; ".join(parts)
                        }
                    }

    # Trade pattern - "trade partners of Peru" or "exports of Japan"
    trade_keywords = ["trade partner", "trading partner", "export partner", "import partner",
                      "exports of", "imports of", "main export", "main import", "top export", "top import",
                      "trade with", "trades with", "trading with", "who export", "who import"]
    if iso3 and any(kw in query_lower for kw in trade_keywords):
        ref_path = REFERENCE_DIR / "world_factbook_text.json"
        if ref_path.exists():
            data = load_reference_file(ref_path)
            countries = data.get("countries", {})
            if iso3 in countries:
                country_data = countries[iso3]
                exports_commodities = country_data.get("exports_commodities", "")
                exports_partners = country_data.get("exports_partners", "")
                imports_commodities = country_data.get("imports_commodities", "")
                imports_partners = country_data.get("imports_partners", "")
                if exports_partners or imports_partners or exports_commodities or imports_commodities:
                    parts = []
                    if exports_partners:
                        parts.append(f"Export partners: {exports_partners}")
                    if exports_commodities:
                        parts.append(f"Main exports: {exports_commodities}")
                    if imports_partners:
                        parts.append(f"Import partners: {imports_partners}")
                    if imports_commodities:
                        parts.append(f"Main imports: {imports_commodities}")
                    return {
                        "type": "trade_info",
                        "file": str(ref_path),
                        "country_data": {
                            "country": country_name,
                            "iso3": iso3,
                            "exports_partners": exports_partners,
                            "exports_commodities": exports_commodities,
                            "imports_partners": imports_partners,
                            "imports_commodities": imports_commodities,
                            "formatted": f"{country_name} Trade - " + "; ".join(parts)
                        }
                    }

    # Government pattern - "government of France" or "political system"
    if iso3 and any(kw in query_lower for kw in ["government", "political", "constitution", "president", "parliament", "legislature"]):
        ref_path = REFERENCE_DIR / "world_factbook_text.json"
        if ref_path.exists():
            data = load_reference_file(ref_path)
            countries = data.get("countries", {})
            if iso3 in countries:
                country_data = countries[iso3]
                executive = country_data.get("executive_branch", "")
                legislative = country_data.get("legislative_branch", "")
                constitution = country_data.get("constitution", "")
                if executive or legislative or constitution:
                    parts = []
                    if executive:
                        parts.append(f"Executive: {executive[:300]}")
                    if legislative:
                        parts.append(f"Legislature: {legislative[:200]}")
                    return {
                        "type": "government_info",
                        "file": str(ref_path),
                        "country_data": {
                            "country": country_name,
                            "iso3": iso3,
                            "executive": executive,
                            "legislative": legislative,
                            "constitution": constitution,
                            "formatted": f"{country_name} Government - " + "; ".join(parts)
                        }
                    }

    # Data source reference pattern - dynamically search catalog for matching sources
    # Matches query against source_id, source_name, keywords, and topic_tags
    catalog = load_catalog()
    if catalog:
        for source in catalog.get("sources", []):
            source_id = source.get("source_id", "")
            source_name = source.get("source_name", "").lower()
            keywords = [k.lower() for k in source.get("keywords", [])]
            topic_tags = [t.lower() for t in source.get("topic_tags", [])]

            # Check if query mentions this source by id, name, or keywords
            if source_id.lower() in query_lower or source_name in query_lower:
                source_path = get_source_path(source_id)
                if source_path:
                    ref_path = source_path / "reference.json"
                    if ref_path.exists():
                        return {
                            "type": "data_source",
                            "source_id": source_id,
                            "file": str(ref_path),
                            "content": load_reference_file(ref_path)
                        }

            # Check keywords and topic_tags
            for kw in keywords + topic_tags:
                if kw and len(kw) > 2 and kw in query_lower:
                    source_path = get_source_path(source_id)
                    if source_path:
                        ref_path = source_path / "reference.json"
                        if ref_path.exists():
                            return {
                                "type": "data_source",
                                "source_id": source_id,
                                "file": str(ref_path),
                                "content": load_reference_file(ref_path)
                            }

    return None


# =============================================================================
# Derived Field Detection
# =============================================================================

DERIVED_PATTERNS = {
    "per_capita": [
        r"per capita",
        r"per person",
        r"per head",
        r"per inhabitant",
    ],
    "density": [
        r"density",
        r"per square",
        r"per km",
        r"per sq",
    ],
    "per_1000": [
        r"per 1000",
        r"per thousand",
    ],
    "ratio": [
        r"ratio of",
        r"(\w+)\s*/\s*(\w+)",  # GDP/CO2 style
        r"(\w+)\s+to\s+(\w+)\s+ratio",
    ],
}


def detect_derived_intent(query: str) -> Optional[dict]:
    """
    Detect if query is asking for derived/calculated fields.

    Returns dict with derived type and any detected specifics.
    """
    query_lower = query.lower()

    for derived_type, patterns in DERIVED_PATTERNS.items():
        for pattern in patterns:
            match = re.search(pattern, query_lower)
            if match:
                result = {
                    "type": derived_type,
                    "match": match.group(0),
                }
                # For ratio patterns, try to extract numerator/denominator
                if derived_type == "ratio" and len(match.groups()) >= 2:
                    result["numerator_hint"] = match.group(1)
                    result["denominator_hint"] = match.group(2)
                return result

    return None


# =============================================================================
# Filter Intent Detection (Overlay Integration)
# =============================================================================

# Patterns for reading current filter state
FILTER_READ_PATTERNS = [
    r"what.*(magnitude|power|size|strength|category|scale).*(?:displayed|showing|on|visible)",
    r"what.*filters?",
    r"what.*(earthquakes?|volcanoes?|fires?|storms?|hurricanes?|tornadoes?|floods?).*showing",
    r"current filters?",
    r"how many.*(earthquakes?|events?|fires?|storms?).*showing",
    r"what.*range.*(magnitude|power|size)",
]

# Patterns for changing filters
FILTER_CHANGE_PATTERNS = [
    (r"(?:show|display).*mag(?:nitude)?[\s:]*(\d+\.?\d*)[\s-]+(?:to|-)[\s]*(\d+\.?\d*)", "magnitude_range"),
    (r"(?:show|display).*mag(?:nitude)?[\s:]*(\d+\.?\d*)\s*\+", "magnitude_min"),
    (r"(?:show|display).*(?:>=?|above|over)\s*(\d+\.?\d*)\s*mag", "magnitude_min"),
    (r"(?:show|display).*(?:<=?|under|below)\s*(\d+\.?\d*)\s*mag", "magnitude_max"),
    (r"(?:all|any|clear|reset|remove)\s*(?:earthquakes?|filters?|magnitude)", "clear"),
    (r"(?:show|display).*vei\s*(\d+)\s*(?:\+|and\s*above|or\s*higher)", "vei_min"),
    (r"(?:show|display).*category\s*(\d+)\s*(?:\+|and\s*above|or\s*higher)", "category_min"),
    (r"(?:show|display).*(?:ef|scale)\s*(\d+)\s*(?:\+|and\s*above|or\s*higher)", "scale_min"),
    (r"(?:show|display).*(?:over|above|>=?)\s*(\d+)\s*(?:km2|sq\s*km|square\s*km)", "area_min"),
    (r"(?:show|display).*(?:over|above|>=?)\s*(\d+)\s*acres?", "acres_min"),
]


def detect_filter_intent(query: str, active_overlays: dict) -> Optional[dict]:
    """
    Detect if user is asking about or changing overlay filters.

    Args:
        query: User query text
        active_overlays: Current overlay state {type, filters, allActive}

    Returns:
        dict with filter intent info, or None if no filter intent detected
    """
    if not active_overlays:
        return None

    query_lower = query.lower()
    overlay_type = active_overlays.get("type")

    # Check for read patterns
    for pattern in FILTER_READ_PATTERNS:
        if re.search(pattern, query_lower):
            return {
                "type": "read_filters",
                "overlay": overlay_type,
                "pattern": pattern
            }

    # Check for change patterns
    for pattern, filter_type in FILTER_CHANGE_PATTERNS:
        match = re.search(pattern, query_lower)
        if match:
            result = {
                "type": "change_filters",
                "overlay": overlay_type,
                "filter_type": filter_type,
                "raw_match": match.group(0)
            }

            # Extract values based on filter type
            if filter_type == "magnitude_range":
                result["minMagnitude"] = float(match.group(1))
                result["maxMagnitude"] = float(match.group(2))
            elif filter_type == "magnitude_min":
                result["minMagnitude"] = float(match.group(1))
            elif filter_type == "magnitude_max":
                result["maxMagnitude"] = float(match.group(1))
            elif filter_type == "vei_min":
                result["minVei"] = int(match.group(1))
            elif filter_type == "category_min":
                result["minCategory"] = int(match.group(1))
            elif filter_type == "scale_min":
                result["minScale"] = int(match.group(1))
            elif filter_type == "area_min":
                result["minAreaKm2"] = float(match.group(1))
            elif filter_type == "acres_min":
                # Convert acres to km2 (1 acre = 0.00404686 km2)
                result["minAreaKm2"] = float(match.group(1)) * 0.00404686
            elif filter_type == "clear":
                result["clear"] = True

            return result

    return None


def detect_overlay_intent(query: str, active_overlays: dict = None) -> Optional[dict]:
    """
    Detect if user is asking about a disaster overlay, even if no overlay is active.

    This allows the chat to recognize queries like "show me earthquakes in California"
    and enable the appropriate overlay.

    Args:
        query: User query text
        active_overlays: Current overlay state (optional)

    Returns:
        dict with overlay intent info:
        {
            "overlay": "earthquakes",
            "action": "enable" | "filter" | "query",
            "location": {"loc_prefix": "USA-CA"},  # if location detected
            "severity": {"minMagnitude": 7.0},     # if severity detected
        }
    """
    query_lower = query.lower()

    # Check if any disaster keywords are mentioned
    detected_overlay = None
    disaster_overlays = _load_disaster_overlays()
    for overlay_id, keywords in disaster_overlays.items():
        for keyword in keywords:
            if keyword in query_lower:
                detected_overlay = overlay_id
                break
        if detected_overlay:
            break

    if not detected_overlay:
        return None

    # Determine action based on context
    is_overlay_active = False
    if active_overlays:
        active_type = active_overlays.get("type", "")
        all_active = active_overlays.get("allActive", [])
        is_overlay_active = (
            active_type == detected_overlay or
            detected_overlay in all_active
        )

    # Extract any severity filters mentioned
    severity = {}
    filter_result = detect_filter_intent(query, {"type": detected_overlay})
    if filter_result and filter_result.get("type") == "change_filters":
        # Extract severity fields
        for key in ["minMagnitude", "maxMagnitude", "minCategory", "minVei", "minScale", "minAreaKm2"]:
            if key in filter_result:
                severity[key] = filter_result[key]

    # Determine action
    if is_overlay_active:
        if severity:
            action = "filter"  # Modify existing overlay filters
        else:
            action = "query"   # Query about existing overlay
    else:
        action = "enable"      # Need to turn on the overlay

    return {
        "overlay": detected_overlay,
        "action": action,
        "severity": severity if severity else None,
        "is_active": is_overlay_active
    }


# =============================================================================
# Navigation Intent Detection
# =============================================================================

# Patterns that indicate user wants to navigate/view locations, not request data
# Data/event keywords that should NOT trigger navigation (these are data queries)
_DATA_KEYWORDS = r"data|from|gdp|population|earthquake|volcano|hurricane|storm|wildfire|fire|flood|drought|tornado|tsunami|emission|income|health|mortality"

NAVIGATION_PATTERNS = [
    rf"^show me\b(?!.*(?:{_DATA_KEYWORDS}))",  # "show me X" but not data/event queries
    r"^where is\b",
    r"^where are\b",
    r"^locate\b",
    rf"^find\b(?!.*(?:{_DATA_KEYWORDS}))",  # "find X" but not data queries
    r"^zoom to\b",
    r"^go to\b",
    r"^take me to\b",
    rf"^show\b(?!.*(?:{_DATA_KEYWORDS}))",  # "show X" but not data/event queries
]

# Patterns for "show borders/geometry" follow-up requests (no data, just display)
SHOW_BORDERS_PATTERNS = [
    r"^(?:just\s+)?show\s+(?:me\s+)?(?:them|all|all\s+of\s+them)\b",
    r"^display\s+(?:them|all|all\s+of\s+them)\b",
    r"^(?:just\s+)?show\s+(?:me\s+)?(?:the\s+)?(?:borders?|geometr(?:y|ies)|outlines?|boundaries?)\b",
    r"^(?:put|display|show)\s+(?:them\s+)?(?:all\s+)?on\s+(?:the\s+)?map\b",
    r"^(?:just\s+)?the\s+(?:borders?|geometr(?:y|ies)|locations?)\b",
]


# =============================================================================
# Candidate-Based Detection
# =============================================================================

def detect_source_candidates(query: str) -> dict:
    """
    Detect all possible source matches in query with confidence scores.

    Returns dict with:
    - candidates: List of all matches sorted by confidence (highest first)
    - best: The highest confidence match (or None)

    Confidence scoring:
    - Full source_name match: 1.0
    - source_id match: 0.9
    - Partial name (>8 chars): 0.7
    - Partial name (4-8 chars): 0.5
    - Boost if query contains "data", "statistics", "source": +0.1
    """
    query_lower = query.lower()
    catalog = load_catalog()

    if not catalog:
        return {"candidates": [], "best": None}

    sources = catalog.get("sources", [])
    candidates = []

    def add_candidate(source_id: str, source_name: str, confidence: float, match_type: str, matched_text: str) -> None:
        candidates.append({
            "source_id": source_id,
            "source_name": source_name or source_id,
            "confidence": min(1.0, confidence),
            "match_type": match_type,
            "matched_text": matched_text
        })

    # Check for data-related keywords that boost source interpretation
    data_keywords = ["data", "statistics", "dataset", "source", "metrics", "from the"]
    has_data_context = any(kw in query_lower for kw in data_keywords)
    data_boost = 0.1 if has_data_context else 0.0

    # Handle SDG aliases (e.g., "SDG 8", "sdg-8", "sustainable development goal 8")
    sdg_pattern = re.search(r'\b(?:sdg|sustainable\s+development\s+goal)[\s\-_]*(\d{1,2})\b', query_lower)
    if sdg_pattern:
        goal_num = int(sdg_pattern.group(1))
        if 1 <= goal_num <= 17:
            goal_tag = f"goal{goal_num}"
            for source in sources:
                source_id = source.get("source_id", "")
                source_name = source.get("source_name", f"UN SDG Goal {goal_num}")
                topic_tags = source.get("topic_tags", [])
                source_name_lower = source_name.lower()

                if goal_tag in topic_tags or f"goal {goal_num}:" in source_name_lower or f"goal {goal_num} " in source_name_lower:
                    add_candidate(
                        source_id=source_id,
                        source_name=source_name,
                        confidence=1.0,
                        match_type="sdg_alias",
                        matched_text=sdg_pattern.group(0)
                    )

    for source in sources:
        source_id = source.get("source_id", "")
        source_name = source.get("source_name", "")
        source_name_lower = source_name.lower() if source_name else ""

        # Check if full source_name appears in query
        if source_name and source_name_lower in query_lower:
            add_candidate(source_id, source_name, 1.0 + data_boost, "full_name", source_name)
        # Check partial name matches
        elif source_name:
            name_parts = [p.strip() for p in source_name.replace(' - ', '|').replace(': ', '|').split('|')]
            for part in name_parts:
                part_lower = part.lower()
                if len(part) >= 4 and part_lower in query_lower:
                    # Score based on match length
                    if len(part) >= 8:
                        base_score = 0.7
                    else:
                        base_score = 0.5
                    add_candidate(source_id, source_name, base_score + data_boost, "partial_name", part)
                    break  # Only add once per source

        # Check if source_id appears (for power users)
        if source_id and source_id.lower() in query_lower:
            add_candidate(source_id, source_name, 0.9 + data_boost, "source_id", source_id)

    # Sort by confidence (highest first)
    candidates = sorted(candidates, key=lambda x: -x["confidence"])

    # Remove duplicates (keep highest confidence per source_id)
    seen = set()
    unique_candidates = []
    for c in candidates:
        if c["source_id"] not in seen:
            seen.add(c["source_id"])
            unique_candidates.append(c)

    return {
        "candidates": unique_candidates,
        "best": unique_candidates[0] if unique_candidates else None
    }


def detect_location_candidates(query: str, viewport: dict = None) -> dict:
    """
    Detect all possible location matches in query with confidence scores.

    Returns dict with:
    - candidates: List of all matches sorted by confidence (highest first)
    - best: The highest confidence match (or None)

    Confidence scoring:
    - Exact country name: 1.0
    - Capital city: 0.9
    - Admin1 (state/province): 0.8
    - Admin2 (county) in viewport: 0.6
    - Partial word match: 0.4
    - Stop word match: 0.1 (very low - likely false positive)
    """
    # Normalize query to handle possessive forms
    normalized_query = normalize_query_for_location_matching(query)
    query_lower = normalized_query.lower()

    candidates = []

    # 1. Check country names (highest priority)
    name_to_iso3 = build_name_to_iso3()
    sorted_names = sorted(name_to_iso3.keys(), key=len, reverse=True)

    for name in sorted_names:
        pattern = r'\b' + re.escape(name) + r'\b'
        if re.search(pattern, query_lower):
            iso3 = name_to_iso3[name]
            # Get proper country name
            iso_data = load_reference_file(REFERENCE_DIR / "iso_codes.json")
            country_name = iso_data.get("iso3_to_name", {}).get(iso3, name.title()) if iso_data else name.title()

            candidates.append({
                "matched_term": name,
                "iso3": iso3,
                "loc_id": iso3,
                "country_name": country_name,
                "confidence": 1.0,
                "match_type": "country",
                "is_subregion": False
            })

    # 2. Check capital cities
    subregion_to_iso3 = build_subregion_to_iso3()
    sorted_subregions = sorted(subregion_to_iso3.keys(), key=len, reverse=True)

    for subregion in sorted_subregions:
        pattern = r'\b' + re.escape(subregion) + r'\b'
        if re.search(pattern, query_lower):
            iso3 = subregion_to_iso3[subregion]
            iso_data = load_reference_file(REFERENCE_DIR / "iso_codes.json")
            country_name = iso_data.get("iso3_to_name", {}).get(iso3, subregion.title()) if iso_data else subregion.title()

            candidates.append({
                "matched_term": subregion,
                "iso3": iso3,
                "loc_id": iso3,
                "country_name": country_name,
                "confidence": 0.9,
                "match_type": "capital",
                "is_subregion": True
            })

    # 3. Viewport location lookup DISABLED for performance
    # The LLM is smart enough to recognize locations like "California" or "Texas"
    # without us searching through 50,000+ parquet names on every query.
    # Validation/disambiguation happens in postprocessor if needed.
    # Original code searched all location names which took 8-28 seconds per query.

    # Sort by confidence (highest first)
    candidates = sorted(candidates, key=lambda x: -x["confidence"])

    # Remove duplicates (keep highest confidence per loc_id)
    seen = set()
    unique_candidates = []
    for c in candidates:
        loc_key = c.get("loc_id") or c.get("iso3")
        if loc_key and loc_key not in seen:
            seen.add(loc_key)
            unique_candidates.append(c)

    return {
        "candidates": unique_candidates,
        "best": unique_candidates[0] if unique_candidates else None
    }


def detect_intent_candidates(query: str, source_candidates: dict, location_candidates: dict) -> dict:
    """
    Detect possible user intents with confidence scores.

    Intents:
    - data_request: User wants to see data
    - navigation: User wants to zoom/navigate to a location
    - reference_lookup: User asking about facts (capital, currency, etc.)
    - filter_change: User wants to adjust filters
    - show_borders: User wants to see geometry without data

    Returns dict with:
    - candidates: List of intents sorted by confidence
    - best: The highest confidence intent
    """
    query_lower = query.lower().strip()
    candidates = []

    # Check for data request signals (uses config from top of file)
    data_score = 0.0
    if any(kw in query_lower for kw in ["data", "statistics", "metrics", "show me data"]):
        data_score += SCORE_DATA_KEYWORDS
    if any(kw in query_lower for kw in ["from the", "from", "dataset"]):
        data_score += SCORE_DATA_FROM
    if source_candidates.get("best"):
        data_score += SCORE_SOURCE_MENTIONED  # Source mentioned = likely data request
    if any(kw in query_lower for kw in ["population", "gdp", "births", "deaths", "economy"]):
        data_score += SCORE_METRIC_KEYWORDS  # Metric keywords

    if data_score > 0:
        candidates.append({
            "type": "data_request",
            "confidence": min(1.0, data_score),
            "signals": ["source_mentioned"] if source_candidates.get("best") else []
        })

    # Check for navigation signals (uses config from top of file)
    nav_score = 0.0
    nav_result = detect_navigation_intent(query)
    if nav_result.get("is_navigation"):
        nav_score += SCORE_NAV_PATTERN
        # But reduce if data keywords present
        if "data" in query_lower or source_candidates.get("best"):
            nav_score += SCORE_NAV_PENALTY_DATA  # Negative value

    if location_candidates.get("best") and nav_score == 0:
        # Location mentioned but no explicit nav pattern - could be either
        nav_score += SCORE_NAV_LOCATION_ONLY

    if nav_score > 0:
        candidates.append({
            "type": "navigation",
            "confidence": max(0.0, min(1.0, nav_score)),
            "pattern": nav_result.get("pattern"),
            "location_text": nav_result.get("location_text")
        })

    # Check for reference lookup (capital, currency, language, etc.)
    ref_score = 0.0
    # Don't classify analytical FX/currency requests as reference lookup.
    currency_analytics_terms = [
        "against usd", "vs usd", "drop", "depreciat", "appreciat", "volatility",
        "single year", "over the last", "trend", "time series", "change", "percent",
        "over time", "since ", "between ", "compare"
    ]
    is_currency_analytics = ("currency" in query_lower or "fx" in query_lower or "exchange rate" in query_lower) and any(
        term in query_lower for term in currency_analytics_terms
    )

    ref_patterns = [
        (r"capital of", 0.9),
        (r"what.+capital", 0.9),
        (r"currency", 0.8),
        (r"language.+spoken", 0.8),
        (r"what language", 0.8),
        (r"sdg\s*\d+", 0.9),
        (r"goal\s*\d+", 0.8),
    ]
    for pattern, score in ref_patterns:
        if pattern == r"currency" and is_currency_analytics:
            continue
        if re.search(pattern, query_lower):
            ref_score = max(ref_score, score)

    if ref_score > 0:
        candidates.append({
            "type": "reference_lookup",
            "confidence": ref_score,
            "signals": []
        })

    # Check for show borders intent
    show_borders = detect_show_borders_intent(query)
    if show_borders.get("is_show_borders"):
        candidates.append({
            "type": "show_borders",
            "confidence": 0.9,
            "pattern": show_borders.get("pattern")
        })

    # Check for filter change intent
    filter_result = detect_filter_intent(query, {}) or {}
    if filter_result.get("is_filter_intent"):
        candidates.append({
            "type": "filter_change",
            "confidence": 0.85,
            "filter_type": filter_result.get("filter_type"),
            "parsed_values": filter_result.get("parsed_values", {})
        })

    # Sort by confidence
    candidates = sorted(candidates, key=lambda x: -x["confidence"])

    return {
        "candidates": candidates,
        "best": candidates[0] if candidates else {"type": "data_request", "confidence": 0.5}
    }


def adjust_scores_with_context(source_candidates: dict, location_candidates: dict, intent_candidates: dict) -> dict:
    """
    Cross-reference candidates to adjust confidence scores.

    Key adjustments:
    - Penalize location if it's a substring of a detected source name
    - Boost source if query has data-related keywords
    - Reduce navigation confidence if source is mentioned
    """
    adjusted_locations = []

    # Get matched source text for comparison
    source_texts = set()
    for sc in source_candidates.get("candidates", []):
        source_name = sc.get("source_name", "").lower()
        source_texts.add(source_name)
        # Also add individual words from source name
        for word in source_name.split():
            if len(word) >= 4:
                source_texts.add(word)

    # Adjust location scores
    for loc in location_candidates.get("candidates", []):
        matched_term = loc.get("matched_term", "").lower()

        # Check if location term appears in any source name
        term_in_source = any(matched_term in st for st in source_texts)

        if term_in_source:
            # Heavily penalize - this is likely a false positive (uses config from top of file)
            loc["confidence"] = max(0.0, loc["confidence"] + PENALTY_LOCATION_IN_SOURCE)
            loc["penalized_reason"] = "term_in_source_name"

        adjusted_locations.append(loc)

    # Re-sort after adjustments
    adjusted_locations = sorted(adjusted_locations, key=lambda x: -x["confidence"])

    # Update best
    location_candidates["candidates"] = adjusted_locations
    location_candidates["best"] = adjusted_locations[0] if adjusted_locations else None

    # Adjust intent scores based on source detection (uses config from top of file)
    adjusted_intents = []
    for intent in intent_candidates.get("candidates", []):
        if intent["type"] == "navigation" and source_candidates.get("best"):
            # Source mentioned - reduce navigation confidence
            if source_candidates["best"]["confidence"] > 0.7:
                intent["confidence"] = max(0.0, intent["confidence"] + PENALTY_NAV_SOURCE_DETECTED)
                intent["adjusted_reason"] = "source_detected"

        adjusted_intents.append(intent)

    adjusted_intents = sorted(adjusted_intents, key=lambda x: -x["confidence"])
    intent_candidates["candidates"] = adjusted_intents
    intent_candidates["best"] = adjusted_intents[0] if adjusted_intents else None

    return {
        "sources": source_candidates,
        "locations": location_candidates,
        "intents": intent_candidates
    }


def detect_show_borders_intent(query: str) -> dict:
    """
    Detect if query is asking to display geometry/borders without data.
    Typically used as a follow-up after disambiguation lists locations.

    Patterns:
    - "just show me them"
    - "display them all"
    - "show all of them on the map"
    - "just the borders"

    Returns dict with:
    - is_show_borders: True if this is a show-borders request
    - pattern: The matched pattern
    """
    result = {
        "is_show_borders": False,
        "pattern": None,
    }

    query_lower = query.lower().strip()

    for pattern in SHOW_BORDERS_PATTERNS:
        if re.match(pattern, query_lower):
            result["is_show_borders"] = True
            result["pattern"] = pattern
            return result

    return result


def detect_navigation_intent(query: str) -> dict:
    """
    Detect if query is asking to navigate to/view locations.

    Returns dict with:
    - is_navigation: True if this is a navigation request
    - pattern: The matched pattern
    - location_text: The text after the navigation verb (potential location names)
    """
    result = {
        "is_navigation": False,
        "pattern": None,
        "location_text": None,
    }

    query_lower = query.lower().strip()

    for pattern in NAVIGATION_PATTERNS:
        match = re.match(pattern, query_lower)
        if match:
            result["is_navigation"] = True
            result["pattern"] = pattern
            # Extract everything after the navigation verb as potential location(s)
            after_match = query_lower[match.end():].strip()
            result["location_text"] = after_match
            return result

    return result


def detect_drilldown_pattern(query: str, viewport: dict = None) -> dict:
    """
    Detect "drill-down" patterns like:
    - "texas counties" or "california cities" ([location] [level])
    - "counties in texas" or "cities of california" ([level] in/of [location])
    - "all the texas counties" (with "all the" prefix)

    Returns dict with:
    - is_drilldown: True if this is a drill-down pattern
    - parent_location: The parent location dict (e.g., Texas)
    - child_level_name: The child level name (e.g., "counties")
    """
    query_lower = query.lower().strip()

    # Remove common prefixes: "all", "the", "show me", etc.
    query_lower = re.sub(r'^(?:show\s+me\s+)?(?:all\s+)?(?:the\s+)?', '', query_lower)

    # Admin level names to check (plural forms)
    level_names = ["counties", "states", "cities", "districts", "regions",
                   "provinces", "municipalities", "departments", "prefectures",
                   "parishes", "boroughs"]

    # Pattern 1: "[level] in/of [location]" (e.g., "counties in texas")
    for level in level_names:
        # Match patterns like "counties in texas" or "cities of california"
        pattern = rf'^{level}\s+(?:in|of)\s+(.+)$'
        match = re.match(pattern, query_lower)
        if match:
            location_part = match.group(1).strip()
            if location_part:
                # Try to find this as a location
                result = extract_country_from_query(location_part)
                if result.get("match"):
                    matched_term, iso3, is_subregion = result["match"]
                    return {
                        "is_drilldown": True,
                        "parent_location": {
                            "matched_term": matched_term,
                            "iso3": iso3,
                            "loc_id": result.get("loc_id", iso3),
                            "country_name": result.get("country_name", matched_term),
                            "is_subregion": is_subregion
                        },
                        "child_level_name": level
                    }

                # Viewport lookup DISABLED - LLM handles location recognition

    # Pattern 2: "[location] [level]" (e.g., "texas counties")
    for level in level_names:
        if query_lower.endswith(level):
            # Extract what comes before the level
            location_part = query_lower[:-len(level)].strip()

            if not location_part:
                continue

            # Try to find this as a location (state, country, etc.)
            result = extract_country_from_query(location_part)
            if result.get("match"):
                matched_term, iso3, is_subregion = result["match"]
                return {
                    "is_drilldown": True,
                    "parent_location": {
                        "matched_term": matched_term,
                        "iso3": iso3,
                        "loc_id": result.get("loc_id", iso3),
                        "country_name": result.get("country_name", matched_term),
                        "is_subregion": is_subregion
                    },
                    "child_level_name": level
                }

            # Viewport lookup DISABLED - LLM handles location recognition

    return {"is_drilldown": False}


def extract_multiple_locations(query: str, viewport: dict = None) -> dict:
    """
    Extract multiple location names from a query.
    Handles patterns like "Simpson and Woodford counties" or
    "Butler, Franklin, Knox, Laurel, Lawrence and Whitley".

    Returns dict with:
    - locations: list of location match dicts
    - needs_disambiguation: True if singular suffix with multiple matches (user wants ONE)
    - suffix_type: 'singular', 'plural', or None
    """
    # First, try to find comma-separated or "and"-separated location names
    # Common patterns: "X, Y, and Z" or "X and Y" or "X, Y, Z"
    query_lower = query.lower()

    # Check for drill-down pattern first (e.g., "texas counties" -> drill into Texas)
    drilldown = detect_drilldown_pattern(query, viewport)
    if drilldown.get("is_drilldown"):
        # Return the parent location with a drill-down flag
        parent = drilldown["parent_location"]
        parent["drill_to_level"] = drilldown["child_level_name"]
        return {"locations": [parent], "needs_disambiguation": False, "suffix_type": "plural"}

    # Track admin level AND whether suffix was singular (disambiguation) or plural (show all)
    singular_suffixes = {
        "county": 2, "parish": 2, "borough": 2,
        "state": 1, "province": 1, "region": 1,
        "city": 3, "town": 3, "place": 3,
        "district": 2,
    }
    plural_suffixes = {
        "counties": 2, "parishes": 2, "boroughs": 2,
        "states": 1, "provinces": 1, "regions": 1,
        "cities": 3, "towns": 3, "places": 3,
        "districts": 2,
    }

    suffix_found = None
    expected_admin_level = None
    suffix_type = None  # 'singular' = user wants one (disambiguate), 'plural' = show all

    # Check singular suffixes first (more specific)
    for suffix, level in singular_suffixes.items():
        if query_lower.endswith(suffix):
            suffix_found = suffix
            expected_admin_level = level
            suffix_type = "singular"
            query_lower = query_lower[:-len(suffix)].strip()
            break

    # Check plural suffixes if no singular match
    if not suffix_found:
        for suffix, level in plural_suffixes.items():
            if query_lower.endswith(suffix):
                suffix_found = suffix
                expected_admin_level = level
                suffix_type = "plural"
                query_lower = query_lower[:-len(suffix)].strip()
                break

    # Split by comma and "and"
    # Replace "and" with comma, then split
    normalized = re.sub(r'\s+and\s+', ', ', query_lower)
    normalized = re.sub(r'\s*,\s*', ',', normalized)
    parts = [p.strip() for p in normalized.split(',') if p.strip()]

    # Don't append suffix back - search by name only, then filter by admin level
    # This prevents matching "washington" in "washington county" to Washington state

    all_matches = []

    # Look up each part
    for part in parts:
        part_matches = []

        # Viewport lookup DISABLED for performance - was scanning all 50,000+ names
        # Use targeted global search instead (exact name match, much faster)

        # Do a targeted search for this specific name at the expected admin level
        if expected_admin_level is not None:
            logger.debug(f"Viewport lookup empty for '{part}', doing global search at admin_level={expected_admin_level}")
            global_matches = search_locations_globally(part, admin_level=expected_admin_level)
            if global_matches:
                part_matches.extend(global_matches)
                logger.debug(f"Global search found {len(global_matches)} matches for '{part}'")

        # If still no matches and no specific admin level, try country lookup
        if not part_matches and expected_admin_level is None:
            result = extract_country_from_query(part)
            if result.get("match"):
                matched_term, iso3, is_subregion = result["match"]
                part_matches.append({
                    "matched_term": matched_term,
                    "iso3": iso3,
                    "is_subregion": is_subregion,
                    "source": result.get("source", "country")
                })

        all_matches.extend(part_matches)

    # Determine if disambiguation is needed:
    # - Singular suffix (county, city) with multiple matches = user wants ONE, needs to pick
    # - Plural suffix (counties, cities) with multiple matches = user wants ALL, show them
    needs_disambiguation = (suffix_type == "singular" and len(all_matches) > 1)

    return {
        "locations": all_matches,
        "needs_disambiguation": needs_disambiguation,
        "suffix_type": suffix_type,
        "query_term": query.strip()  # Original query for disambiguation message
    }


# =============================================================================
# Main Preprocessor Function
# =============================================================================

def preprocess_query(query: str, viewport: dict = None, active_overlays: dict = None, cache_stats: dict = None, saved_order_names: list = None, time_state: dict = None, loaded_data: list = None) -> dict:
    """
    Main preprocessor function - extracts all hints from query.

    Args:
        query: User query text
        viewport: Optional viewport dict with {center, zoom, bounds, adminLevel}
        active_overlays: Optional dict with {type, filters, allActive} from frontend
        cache_stats: Optional dict with per-overlay stats {overlayId: {count, years, ...}}
        saved_order_names: Optional list of saved order names from frontend
        time_state: Optional dict with time slider state {isLiveLocked, currentTime, etc.}
        loaded_data: Optional list of loaded data entries {source_id, region, metric, years, data_type}

    Returns a hints dict that can be injected into LLM context.
    """
    show_borders = detect_show_borders_intent(query)
    nav_intent = detect_navigation_intent(query)

    navigation = None
    disambiguation = None

    if nav_intent.get("is_navigation") and nav_intent.get("location_text"):
        location_result = extract_multiple_locations(nav_intent["location_text"], viewport)
        locations = location_result.get("locations", [])
        if locations:
            if location_result.get("needs_disambiguation"):
                disambiguation = {
                    "needed": True,
                    "query_term": location_result.get("query_term", "location"),
                    "options": locations,
                    "count": len(locations)
                }
            else:
                navigation = {
                    "is_navigation": True,
                    "pattern": nav_intent.get("pattern"),
                    "locations": locations,
                    "count": len(locations)
                }

    # Canonical source detection path: candidate-based only.
    source_candidates = detect_source_candidates(query)
    detected_source = source_candidates.get("best")

    # Resolve location for non-navigation queries (data orders, etc.)
    # Pass viewport to enable parquet-based city/location lookups
    location = None

    if not navigation and not disambiguation:
        location_result = extract_country_from_query(query, viewport=viewport)

        # Filter likely false positives where location token is part of the source name.
        if detected_source and location_result.get("match"):
            source_name_lower = detected_source.get("source_name", "").lower()
            matched_term = location_result["match"][0].lower()
            if matched_term in source_name_lower:
                location_result = {}

        if location_result.get("match"):
            matched_term, iso3, is_subregion = location_result["match"]
            iso_data = load_reference_file(REFERENCE_DIR / "iso_codes.json")
            country_name = iso_data.get("iso3_to_name", {}).get(iso3, matched_term.title()) if iso_data else matched_term.title()
            location = {
                "matched_term": matched_term,
                "iso3": iso3,
                "country_name": country_name,
                "is_subregion": is_subregion,
                "source": location_result.get("source"),
            }

            if location_result.get("ambiguous") and location_result.get("matches"):
                matches = location_result["matches"]

                resolved_by_viewport = False
                if viewport:
                    filtered_matches = matches

                    current_admin_level = viewport.get("adminLevel")
                    if current_admin_level is not None and current_admin_level >= 0:
                        for check_level in range(current_admin_level, -1, -1):
                            level_matches = [
                                m for m in filtered_matches
                                if m.get("admin_level", 0) == check_level
                            ]
                            if level_matches:
                                filtered_matches = level_matches
                                logger.debug(f"Admin level filter: {len(level_matches)} matches at level {check_level} (viewing level {current_admin_level})")
                                break

                    if len(filtered_matches) > 1 and viewport.get("bounds"):
                        countries_in_view = get_countries_in_viewport(viewport["bounds"])
                        if countries_in_view:
                            country_matches = [
                                m for m in filtered_matches
                                if m.get("iso3", "").split("-")[0] in countries_in_view
                            ]
                            if country_matches:
                                filtered_matches = country_matches

                    if len(filtered_matches) == 1:
                        m = filtered_matches[0]
                        location = {
                            "matched_term": m.get("matched_term", matched_term),
                            "iso3": m.get("iso3", iso3),
                            "loc_id": m.get("loc_id"),
                            "country_name": m.get("country_name", country_name),
                            "is_subregion": m.get("is_subregion", is_subregion),
                            "source": "viewport_resolved",
                        }
                        resolved_by_viewport = True
                        logger.info(f"Viewport auto-resolved '{matched_term}' to {m.get('loc_id')}")
                    elif len(filtered_matches) > 1:
                        disambiguation = {
                            "needed": True,
                            "query_term": matched_term,
                            "options": filtered_matches,
                            "count": len(filtered_matches)
                        }
                        resolved_by_viewport = True

                if not resolved_by_viewport:
                    disambiguation = {
                        "needed": True,
                        "query_term": matched_term,
                        "options": matches,
                        "count": len(matches)
                    }

    filter_intent = detect_filter_intent(query, active_overlays) if active_overlays else None

    overlay_intent = detect_overlay_intent(query, active_overlays)

    location_candidates = detect_location_candidates(query, viewport)
    intent_candidates = detect_intent_candidates(query, source_candidates, location_candidates)

    # Cross-reference to adjust scores (e.g., penalize "bureau" location if "Bureau of Statistics" source detected)
    adjusted = adjust_scores_with_context(source_candidates, location_candidates, intent_candidates)

    candidates = {
        "sources": adjusted["sources"],
        "locations": adjusted["locations"],
        "intents": adjusted["intents"],
    }

    hints = {
        "original_query": query,
        "viewport": viewport,
        "show_borders": show_borders if show_borders.get("is_show_borders") else None,
        "navigation": navigation,
        "topics": extract_topics(query),
        "regions": resolve_regions(query),
        "location": location,
        "disambiguation": disambiguation,
        "time": detect_time_patterns(query),
        "reference_lookup": detect_reference_lookup(query),
        "derived_intent": detect_derived_intent(query),
        "detected_source": detected_source,
        "active_overlays": active_overlays,
        "cache_stats": cache_stats,
        "filter_intent": filter_intent,
        "overlay_intent": overlay_intent,
        "candidates": candidates,
        "saved_order_names": saved_order_names or [],
        "time_state": time_state,
        "loaded_data": loaded_data or [],
    }

    # Build summary for LLM context injection
    summary_parts = []

    # Navigation intent takes priority
    if navigation:
        loc_names = [loc.get("matched_term", loc.get("loc_id", "?")) for loc in navigation["locations"]]
        summary_parts.append(f"NAVIGATION: Show {navigation['count']} locations: {', '.join(loc_names[:5])}")

    if hints["topics"]:
        summary_parts.append(f"Topics detected: {', '.join(hints['topics'])}")

    if hints["regions"]:
        region_names = [r["match"] for r in hints["regions"]]
        summary_parts.append(f"Regions mentioned: {', '.join(region_names)}")

    # Add location resolution to summary - critical for city->country resolution
    if location and not navigation:
        if disambiguation:
            # Multiple matches - note ambiguity in summary
            summary_parts.append(f"AMBIGUOUS: '{location['matched_term']}' matches {disambiguation['count']} locations")
        elif location["is_subregion"]:
            summary_parts.append(f"Location: '{location['matched_term']}' -> {location['country_name']} ({location['iso3']})")
        else:
            summary_parts.append(f"Location: {location['country_name']} ({location['iso3']})")

    if hints["time"]["is_time_series"]:
        if hints["time"]["year_start"] and hints["time"]["year_end"]:
            summary_parts.append(f"Time range: {hints['time']['year_start']}-{hints['time']['year_end']}")
        else:
            summary_parts.append("Time series requested (trend/historical)")

    if hints["reference_lookup"]:
        summary_parts.append(f"Reference lookup: {hints['reference_lookup']['type']}")

    if hints["derived_intent"]:
        summary_parts.append(f"Derived calculation: {hints['derived_intent']['type']}")

    if hints["detected_source"]:
        summary_parts.append(f"Source specified: {hints['detected_source']['source_name']}")

    # Add active overlay context
    if active_overlays and active_overlays.get("type"):
        overlay_type = active_overlays["type"]
        filters = active_overlays.get("filters", {})
        filter_desc = format_filter_description(filters, overlay_type)
        summary_parts.append(f"OVERLAY: {overlay_type} ({filter_desc})")

    # Add filter intent
    if filter_intent:
        if filter_intent["type"] == "read_filters":
            summary_parts.append("INTENT: Query about current filters")
        elif filter_intent["type"] == "change_filters":
            summary_parts.append(f"INTENT: Change filters ({filter_intent.get('filter_type', 'unknown')})")

    # Add overlay intent (detects disaster keywords even without active overlay)
    if overlay_intent:
        action = overlay_intent.get("action", "unknown")
        overlay = overlay_intent.get("overlay", "unknown")
        severity = overlay_intent.get("severity")
        if action == "enable":
            summary_parts.append(f"OVERLAY_INTENT: Enable {overlay} overlay")
        elif action == "filter":
            summary_parts.append(f"OVERLAY_INTENT: Filter {overlay} ({severity})")
        elif action == "query":
            summary_parts.append(f"OVERLAY_INTENT: Query about {overlay}")

    # Add time state (live mode) info
    if time_state and time_state.get("available"):
        if time_state.get("isLiveLocked"):
            tz = time_state.get("timezone", "local")
            summary_parts.append(f"TIME: LIVE MODE (locked to current time, timezone: {tz})")
        elif time_state.get("currentTimeFormatted"):
            summary_parts.append(f"TIME: Viewing {time_state['currentTimeFormatted']}")

    # Add saved orders info if any exist
    if saved_order_names:
        summary_parts.append(f"SAVED_ORDERS: {', '.join(saved_order_names)}")

    # Add loaded data info for removal context
    if loaded_data:
        loaded_strs = []
        for entry in loaded_data:
            src = entry.get("source_id", "?")
            region = entry.get("region", "global")
            metric = entry.get("metric")
            years = entry.get("years", "")
            dtype = entry.get("data_type", "metrics")
            if metric:
                loaded_strs.append(f"{src}: {metric} in {region} ({years})")
            else:
                loaded_strs.append(f"{src}: {region} ({years})")
        summary_parts.append(f"LOADED_DATA: {'; '.join(loaded_strs)}")

    hints["summary"] = "; ".join(summary_parts) if summary_parts else None

    return hints


def format_filter_description(filters: dict, overlay_type: str) -> str:
    """Format filter settings as human-readable description."""
    if not filters:
        return "no filters"

    parts = []

    if overlay_type == "earthquakes":
        if filters.get("minMagnitude"):
            parts.append(f"mag >= {filters['minMagnitude']}")
        if filters.get("maxMagnitude"):
            parts.append(f"mag <= {filters['maxMagnitude']}")
    elif overlay_type == "hurricanes":
        if filters.get("minCategory"):
            parts.append(f"cat >= {filters['minCategory']}")
    elif overlay_type == "volcanoes":
        if filters.get("minVei"):
            parts.append(f"VEI >= {filters['minVei']}")
    elif overlay_type == "wildfires":
        if filters.get("minAreaKm2"):
            parts.append(f"area >= {filters['minAreaKm2']} km2")
    elif overlay_type == "tornadoes":
        if filters.get("minScale"):
            parts.append(f"scale >= EF{filters['minScale']}")

    return ", ".join(parts) if parts else "no filters"


def build_tier3_context(hints: dict) -> str:
    """
    Build Tier 3 (Just-in-Time) context string from preprocessor hints.

    This is injected into the LLM messages as additional context.
    """
    context_parts = []

    # Add summary if present
    if hints.get("summary"):
        context_parts.append(f"[Preprocessor hints: {hints['summary']}]")

    # ==========================================================================
    # CANDIDATE-BASED CONTEXT (Phase 1 Refactor)
    # Present ALL interpretation candidates to LLM with confidence scores
    # ==========================================================================
    candidates = hints.get("candidates")
    if candidates:
        candidate_lines = ["[INTERPRETATION CANDIDATES - Review all options before deciding:]"]

        # Intent candidates
        intents = candidates.get("intents", {}).get("candidates", [])
        if intents:
            candidate_lines.append("\nPossible intents (pick most likely based on full query):")
            for intent in intents[:3]:
                conf = intent.get("confidence", 0)
                itype = intent.get("type", "unknown")
                reason = f" [{intent.get('adjusted_reason')}]" if intent.get("adjusted_reason") else ""
                candidate_lines.append(f"  - {itype}: {conf:.2f}{reason}")

        # Source candidates
        sources = candidates.get("sources", {}).get("candidates", [])
        if sources:
            candidate_lines.append("\nPossible data sources mentioned:")
            for src in sources[:3]:
                conf = src.get("confidence", 0)
                name = src.get("source_name", "?")
                matched = src.get("matched_text", "")
                candidate_lines.append(f"  - {name}: {conf:.2f} (matched: '{matched}')")

        # Location candidates (with penalty notes)
        locations = candidates.get("locations", {}).get("candidates", [])
        if locations:
            candidate_lines.append("\nPossible locations:")
            for loc in locations[:5]:
                conf = loc.get("confidence", 0)
                term = loc.get("matched_term", "?")
                loc_id = loc.get("loc_id", loc.get("iso3", "?"))
                mtype = loc.get("match_type", "")
                penalty = ""
                if loc.get("penalized_reason") == "term_in_source_name":
                    penalty = " [LIKELY FALSE POSITIVE - term appears in source name]"
                candidate_lines.append(f"  - '{term}' -> {loc_id}: {conf:.2f} ({mtype}){penalty}")

        # Only add if we have actual candidates to show
        if len(candidate_lines) > 1:
            candidate_lines.append("\nBased on the full query context, determine which interpretation is correct.")
            candidate_lines.append("If the query is about data/statistics, prefer data_request intent even if it starts with 'show me'.")
            context_parts.append("\n".join(candidate_lines))

    # Add active overlay context for LLM
    active_overlays = hints.get("active_overlays")
    if active_overlays and active_overlays.get("type"):
        overlay_type = active_overlays["type"]
        filters = active_overlays.get("filters", {})
        filter_desc = format_filter_description(filters, overlay_type)
        context_parts.append(f"[ACTIVE OVERLAY: {overlay_type}]")
        if filters:
            context_parts.append(f"[CURRENT FILTERS: {filter_desc}]")

    # Add overlay intent context - detects disaster keywords even without active overlay
    overlay_intent = hints.get("overlay_intent")
    if overlay_intent:
        overlay = overlay_intent.get("overlay", "unknown")
        action = overlay_intent.get("action", "unknown")
        is_active = overlay_intent.get("is_active", False)
        severity = overlay_intent.get("severity")

        if action == "enable" and not is_active:
            context_parts.append(
                f"[OVERLAY INTENT: User mentioned '{overlay}' but overlay is NOT active. "
                f"Consider responding with overlay_toggle to enable {overlay} overlay.]"
            )
        elif action == "filter" and severity:
            severity_str = ", ".join(f"{k}={v}" for k, v in severity.items())
            context_parts.append(
                f"[OVERLAY INTENT: User wants to filter {overlay} with: {severity_str}. "
                f"Respond with filter_update or overlay_toggle with filters.]"
            )

    # Add cache stats context
    cache_stats = hints.get("cache_stats")
    if cache_stats:
        for overlay_id, stats in cache_stats.items():
            count = stats.get("count", 0)
            if count > 0:
                extra_info = []
                if stats.get("minMag") is not None:
                    extra_info.append(f"mag {stats['minMag']}-{stats.get('maxMag', '?')}")
                if stats.get("years"):
                    years = stats["years"]
                    if len(years) > 0:
                        extra_info.append(f"years {years[0]}-{years[-1]}")
                info_str = f" ({', '.join(extra_info)})" if extra_info else ""
                context_parts.append(f"[CACHE: {count} {overlay_id} loaded{info_str}]")

    # Add loaded data context (for removal operations - tells LLM what regions are loaded)
    loaded_data = hints.get("loaded_data")
    if loaded_data:
        loaded_lines = []
        for entry in loaded_data:
            src = entry.get("source_id", "unknown")
            region = entry.get("region", "global")
            metric = entry.get("metric")
            years = entry.get("years", "")
            overlay_type = entry.get("overlay_type")
            if overlay_type:
                loaded_lines.append(f"- {src}: {overlay_type} overlay for {region}")
            elif metric:
                loaded_lines.append(f"- {src}: {metric} for {region} ({years})")
            else:
                loaded_lines.append(f"- {src}: {region} ({years})")
        if loaded_lines:
            context_parts.append(
                "[LOADED DATA - use this to match regions for removal requests]\n" +
                "\n".join(loaded_lines)
            )

    # Check if user explicitly mentioned a location in their query
    explicit_location = hints.get("location")

    # Add viewport context - but ONLY for admin level, NOT country inference
    # Country inference only happens when user doesn't mention a location explicitly
    viewport = hints.get("viewport")
    if viewport:
        admin_level = viewport.get("adminLevel", 0)
        level_names = {0: "countries", 1: "states/provinces", 2: "counties/districts", 3: "subdivisions"}
        level_name = level_names.get(admin_level, f"level {admin_level}")
        context_parts.append(f"[VIEWPORT: User is viewing at {level_name} level]")

        # ONLY infer country from viewport if:
        # 1. User did NOT explicitly mention a location
        # 2. Zoom level >= 3 (zoomed in enough to be focused on a region)
        # 3. Single country visible in viewport
        zoom_level = viewport.get("zoom", 0)
        if not explicit_location and viewport.get("bounds") and zoom_level >= 3:
            countries_in_view = get_countries_in_viewport(viewport["bounds"])

            if len(countries_in_view) == 1:
                # Single country in view - infer as user's focus
                iso3 = countries_in_view[0]
                iso_data = load_reference_file(REFERENCE_DIR / "iso_codes.json")
                country_name = iso_data.get("iso3_to_name", {}).get(iso3, iso3) if iso_data else iso3
                context_parts.append(
                    f"[INFERRED LOCATION: User appears to be viewing {country_name} ({iso3}). "
                    f"Use this country's data sources unless they specify otherwise.]"
                )

    # Check for disambiguation needed FIRST - if ambiguous, LLM should ask for clarification
    disambiguation = hints.get("disambiguation")
    if disambiguation and disambiguation.get("needed"):
        options = disambiguation.get("options", [])
        term = disambiguation.get("query_term", "location")

        # Format options for LLM to present to user
        option_strs = []
        for i, opt in enumerate(options[:5], 1):  # Limit to 5 options
            loc_id = opt.get("loc_id", "")
            country = opt.get("country_name", opt.get("iso3", ""))
            option_strs.append(f"{i}. {opt.get('matched_term', term).title()} in {country} ({loc_id})")

        context_parts.append(
            f"[DISAMBIGUATION REQUIRED: '{term}' matches {len(options)} locations. "
            f"Ask user to clarify which one:\n" + "\n".join(option_strs) + "]"
        )
        # When disambiguation needed, don't add location context - let LLM ask first
        return "\n".join(context_parts)

    # Add location resolution - critical for city->country data queries
    location = hints.get("location")
    if location:
        if location.get("is_subregion"):
            context_parts.append(
                f"[LOCATION RESOLUTION: '{location['matched_term']}' is in {location['country_name']}. "
                f"Use loc_id={location['iso3']} for data queries about {location['matched_term']}]"
            )
        else:
            context_parts.append(
                f"[LOCATION: {location['country_name']} (loc_id={location['iso3']})]"
            )

    # If user mentioned a specific data source, inject FULL metadata
    # LLM needs all metrics with both column names (for JSON orders) and human names (for replies)
    detected_source = hints.get("detected_source")
    if detected_source:
        source_id = detected_source.get("source_id")
        source_name = detected_source.get("source_name")
        metadata = load_source_metadata(source_id)
        if metadata:
            metrics = metadata.get("metrics", {})
            temporal = metadata.get("temporal_coverage", {})
            year_range = f"{temporal.get('start', '?')}-{temporal.get('end', '?')}"

            # Build full metrics mapping: column_name -> human_name
            # LLM needs column names for JSON orders, human names for user replies
            metrics_mapping = {}
            for col, info in metrics.items():
                human_name = info.get("name", col)
                # Truncate very long names but keep them useful
                if len(human_name) > 80:
                    human_name = human_name[:77] + "..."
                metrics_mapping[col] = human_name

            # Try to load reference.json for additional context (goal titles, descriptions)
            reference_context = ""
            try:
                source_path = get_source_path(source_id)
                ref_path = source_path / "reference.json"
                if ref_path.exists():
                    import json as ref_json
                    with open(ref_path, encoding='utf-8') as f:
                        ref_data = ref_json.load(f)
                    # Extract goal info for SDGs
                    if ref_data.get("goal"):
                        goal = ref_data["goal"]
                        reference_context = f"\nGoal: {goal.get('name', '')}\nDescription: {goal.get('description', '')}"
            except Exception:
                pass  # Reference file is optional

            msg = f"[SOURCE DETECTED: {source_name}]"
            msg += f"\nYears: {year_range}. Total metrics: {len(metrics)}."
            if reference_context:
                msg += reference_context
            msg += f"\n\nALL METRICS (use column name in JSON 'metric' field, human name when talking to user):\n"
            for col, human in metrics_mapping.items():
                msg += f'  "{col}": {human}\n'
            msg += "\n(REPLY RULES: When listing metrics to user, show max 10 and use human names only. Say 'I can get them all' not '*'.)"
            context_parts.append(msg)

    # Add resolved region details
    if hints.get("regions"):
        for region in hints["regions"][:3]:  # Limit to 3 regions
            context_parts.append(
                f"Region '{region['match']}' = {region['grouping']} "
                f"({region['count']} countries)"
            )

    # Add time context
    if hints.get("time", {}).get("is_time_series"):
        time = hints["time"]
        if time.get("year_start") and time.get("year_end"):
            context_parts.append(
                f"User wants data from {time['year_start']} to {time['year_end']}"
            )

    # Inject relevant source/metric hints when topics + location detected
    # This gives the LLM the actual column names to use
    topics = hints.get("topics", [])

    # Get ISO3 from location OR navigation OR viewport inference
    # Priority: explicit location > navigation > viewport-inferred
    location = hints.get("location")
    iso3 = None
    if location:
        iso3 = location.get("iso3")
    elif hints.get("navigation") and hints["navigation"].get("locations"):
        # For navigation queries, extract iso3 from first location
        nav_locs = hints["navigation"]["locations"]
        if nav_locs and nav_locs[0].get("iso3"):
            iso3 = nav_locs[0]["iso3"]
    elif viewport and not explicit_location:
        # Fallback: use viewport-inferred country if zoomed into single country
        zoom_level = viewport.get("zoom", 0)
        if viewport.get("bounds") and zoom_level >= 3:
            countries_in_view = get_countries_in_viewport(viewport["bounds"])
            if len(countries_in_view) == 1:
                iso3 = countries_in_view[0]
                logger.debug(f"Using viewport-inferred country: {iso3}")

    if topics or iso3:
        source_hints = get_relevant_sources_with_metrics(topics, iso3)
        relevant_sources = source_hints.get("sources", [])
        country_summary = source_hints.get("country_summary")
        country_index = source_hints.get("country_index")

        # Add country index data (datasets, admin levels) if available
        if country_index:
            datasets = country_index.get("datasets", [])
            admin_levels = country_index.get("admin_levels", [])
            admin_counts = country_index.get("admin_counts", {})
            if datasets:
                level_info = []
                for level in admin_levels:
                    count = admin_counts.get(str(level))
                    if count:
                        level_info.append(f"admin_{level}={count}")
                admin_str = f" ({', '.join(level_info)})" if level_info else ""
                context_parts.append(
                    f"[COUNTRY DATASETS: {', '.join(datasets)}{admin_str}]"
                )

        # Add country summary from index.json if available
        if country_summary:
            context_parts.append(f"[COUNTRY DATA SUMMARY: {country_summary}]")

        if relevant_sources:
            # Prioritize country-specific sources first
            country_sources = [s for s in relevant_sources if s.get("is_country_source")]
            global_sources = [s for s in relevant_sources if not s.get("is_country_source")]

            hints_lines = [
                "[CRITICAL - EXACT METRIC NAMES FOR ORDER JSON ONLY:]",
                "Use these EXACT column names in your JSON orders. But NEVER show column names to the user - only show the human-readable name in parentheses:"
            ]

            # Country-specific sources - show ALL metrics with both column AND name
            for src in country_sources[:3]:  # Limit sources but not metrics
                # Format: "column_name" (Human Readable Name) so LLM knows what to use
                metrics_list = [f'"{m["column"]}" ({m["name"]})' for m in src["metrics"]]
                metrics_str = ", ".join(metrics_list)
                hints_lines.append(f"- {src['source_id']}: [{metrics_str}]")

            # Global sources - show limited metrics with both column AND name
            for src in global_sources[:2]:  # Limit global to 2 sources
                metrics_list = [f'"{m["column"]}" ({m["name"]})' for m in src["metrics"][:5]]
                metrics_str = ", ".join(metrics_list)
                hints_lines.append(f"- {src['source_id']}: [{metrics_str}]")

            if len(hints_lines) > 2:  # More than just the headers
                context_parts.append("\n".join(hints_lines))

    return "\n".join(context_parts) if context_parts else ""


def build_tier4_context(hints: dict) -> str:
    """
    Build Tier 4 (Reference) context string from preprocessor hints.

    This is injected when reference lookups are detected.
    If country_data is available, returns the specific answer directly.
    """
    ref_lookup = hints.get("reference_lookup")
    if not ref_lookup:
        return ""

    ref_type = ref_lookup["type"]

    # If we have specific country data, return it directly for the LLM to use
    country_data = ref_lookup.get("country_data")
    if country_data:
        formatted = country_data.get("formatted", "")
        return f"[REFERENCE ANSWER: {formatted}]"

    # Otherwise, provide general reference info
    content = ref_lookup.get("content")

    if ref_type == "sdg" and content:
        # Format SDG reference
        goal = content.get("goal", {})
        parts = [
            f"SDG {goal.get('number')}: {goal.get('name')}",
            f"Full title: {goal.get('full_title')}",
            f"Description: {goal.get('description')}",
        ]
        if goal.get("targets"):
            parts.append("Targets:")
            for target in goal["targets"][:5]:  # Limit targets
                parts.append(f"  {target['id']}: {target['text']}")
        return "\n".join(parts)

    elif ref_type == "data_source" and content:
        # Format data source reference
        about = content.get("about", {})
        this_dataset = content.get("this_dataset", {})
        parts = [
            f"Data Source: {about.get('name', 'Unknown')}",
            f"Publisher: {about.get('publisher', 'Unknown')}",
            f"URL: {about.get('url', 'N/A')}",
            f"License: {about.get('license', 'Unknown')}",
        ]
        if about.get("history"):
            parts.append(f"Background: {about['history'][:200]}...")
        if this_dataset.get("focus"):
            parts.append(f"Focus: {this_dataset['focus']}")
        return "\n".join(parts)

    elif ref_type == "capital":
        return "[Reference: Country capital data available. Ask about a specific country.]"

    elif ref_type == "currency":
        return "[Reference: Currency data for 215 countries available from World Factbook. Ask about a specific country.]"

    elif ref_type == "language":
        return "[Reference: Language data for 200+ countries available from World Factbook. Ask about a specific country.]"

    elif ref_type == "timezone":
        return "[Reference: Timezone data for 200+ countries available from World Factbook. Ask about a specific country.]"

    elif ref_type in ["country_info", "economy_info", "government_info", "trade_info"]:
        # These already have country_data with formatted output
        return "[Reference: Detailed country information available from World Factbook.]"

    elif ref_type == "system_help" and content:
        # Format system help reference
        about = content.get("about", {})
        how = content.get("how_it_works", {})
        capabilities = content.get("capabilities", [])
        examples = content.get("example_queries", {})
        tips = content.get("tips", [])

        parts = [
            f"[SYSTEM HELP REFERENCE]",
            f"Name: {about.get('name', 'Geographic Data Explorer')}",
            f"Description: {about.get('description', '')}",
            "",
            f"How it works: {how.get('summary', '')}",
            "",
            "Capabilities:",
        ]
        for cap in capabilities:
            parts.append(f"  - {cap}")

        parts.append("")
        parts.append("Example queries the user can try:")
        for category, queries in examples.items():
            parts.append(f"  {category}:")
            for q in queries[:2]:  # Show 2 per category to keep concise
                parts.append(f"    - \"{q}\"")

        parts.append("")
        parts.append("Tips:")
        for tip in tips[:3]:  # Top 3 tips
            parts.append(f"  - {tip}")

        return "\n".join(parts)

    return ""
