"""Keep reference/admin_levels.json in step with the released admin spines.

The app's level labels ("3,143 counties" in map popups) are English plurals
kept in admin_levels.json. The geometry catalog's native_tier_names are
native-language singular terms for the site's geometry pages, so they are not
a drop-in replacement. What must not drift is the level structure: every
released spine level needs a label, and no label may name a level the spine
no longer has.
"""

import json
from pathlib import Path

import pytest

from mapmover.runtime.geometry_catalog import load_geometry_catalog

ADMIN_LEVELS_PATH = Path(__file__).resolve().parents[1] / "mapmover" / "reference" / "admin_levels.json"


def _released_spine_levels() -> dict[str, set[int]]:
    catalog = load_geometry_catalog()
    spines: dict[str, set[int]] = {}
    for row in catalog.get("country_family_coverage") or []:
        if not isinstance(row, dict):
            continue
        code = str(row.get("country_code") or "").strip().upper()
        levels = {
            int(item["admin_level"])
            for item in row.get("admin_hierarchy_levels") or []
            if isinstance(item, dict) and item.get("admin_level") is not None
        }
        levels.discard(0)
        if code and levels:
            spines[code] = levels
    return spines


def test_admin_level_labels_match_released_spines() -> None:
    spines = _released_spine_levels()
    if not spines:
        pytest.skip("geometry catalog with admin spines is not available locally")
    labels = json.loads(ADMIN_LEVELS_PATH.read_text(encoding="utf-8"))
    mismatches = {}
    for code, levels in sorted(spines.items()):
        labelled = {int(key) for key in (labels.get(code) or {}) if not key.startswith("_")}
        if labelled != levels:
            mismatches[code] = {
                "missing_labels": sorted(levels - labelled),
                "labels_without_level": sorted(labelled - levels),
            }
    assert not mismatches, f"admin_levels.json disagrees with released spines: {mismatches}"
