"""Contract regression for installed Marine and external Bridge packages."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from downloaded_geometry_backend import DownloadedGeometryBackend


def _package(root: Path, name: str, profile: str, unit: str, files: dict[str, tuple[str, list[dict]]],
             tier: str | None = None):
    base = root / name
    members = []
    for archive, (component, rows) in files.items():
        target = base / archive
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), target)
        members.append({"archive_path": archive, "component": component,
                        "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
                        "size_bytes": target.stat().st_size})
    sidecar = {"profile": profile, "release_unit_id": unit, "package_id": name,
               "package_tier": tier or ("display" if unit == "MARINE" else "lite"),
               "external_system": "overture_gers" if unit == "BRIDGES" else None,
               "files": members}
    (base / "geometry.json").write_text(json.dumps(sidecar), encoding="utf-8")


class InstalledSideFamilyTest(unittest.TestCase):
    def test_point_lookup_selects_one_installed_scope_and_bank(self):
        polygon = '{"type":"Polygon","coordinates":[[[-1,-1],[1,-1],[1,1],[-1,1],[-1,-1]]]}'
        def row(loc_id):
            return {"loc_id": loc_id, "name": loc_id, "geometry": polygon,
                    "bbox_min_lon": -1.0, "bbox_max_lon": 1.0,
                    "bbox_min_lat": -1.0, "bbox_max_lat": 1.0}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _package(root, "global", "downloadable_geometry_package", "GLOBAL", {
                "display/admin_0.parquet": ("display", [row("AAA")]),
                "display/admin_1.parquet": ("display", [row("AAA-1")]),
            }, tier="display")
            _package(root, "marine", "downloadable_geometry_package", "MARINE", {
                "reference/identities.parquet": ("reference", [
                    {"loc_id": "AAA-EEZ", "family": "marine_jurisdiction", "subtype": "exclusive_economic_zone"},
                    {"loc_id": "XOP-HS", "family": "marine_jurisdiction", "subtype": "high_seas"},
                    {"loc_id": "IHO-1", "family": "water_body", "subtype": "named_water_area"},
                ]),
                "display/jurisdictions.parquet": ("display", [
                    {**row("AAA-EEZ"), "family": "marine_jurisdiction",
                     "subtype": "exclusive_economic_zone"},
                    {**row("XOP-HS"), "family": "marine_jurisdiction",
                     "subtype": "high_seas"},
                ]),
                "display/water_bodies.parquet": ("display", [row("XZONE")]),
                "display/named_water_areas.parquet": ("display", [row("IHO-1")]),
            })
            country_row = {**row("USA-CA"), "admin_level": 1, "iso_a3": "USA"}
            _package(root, "usa", "downloadable_geometry_package", "USA", {
                "display/admin_0_3.parquet": ("display", [country_row]),
                "display/families/watershed.parquet": ("display", [row("WATERSHED-1")]),
            }, tier="display")
            backend = DownloadedGeometryBackend(root)
            self.assertEqual(backend.call("resolve_point", {
                "lat": 0, "lon": 0, "family": "administrative",
            })["error"]["code"], "point_scope_required")
            self.assertFalse(backend.verified)
            global_result = backend.call("resolve_point", {
                "lat": 0, "lon": 0, "scope_id": "GLOBAL", "family": "administrative",
            })
            self.assertEqual(global_result["matched"]["loc_id"], "AAA")
            self.assertEqual({p.name for p in backend.verified}, {"admin_0.parquet"})
            backend.verified.clear()
            named_result = backend.call("resolve_point", {
                "lat": 0, "lon": 0, "scope_id": "MARINE", "family": "water_body",
                "water_kind": "named",
            })
            self.assertEqual(named_result["family_result"]["matches"][0]["loc_id"], "IHO-1")
            self.assertEqual({p.name for p in backend.verified}, {"named_water_areas.parquet"})
            backend.verified.clear()
            high_seas = backend.call("resolve_point", {
                "lat": 0, "lon": 0, "scope_id": "MARINE", "family": "high_seas",
            })
            self.assertEqual([row["loc_id"] for row in high_seas["family_result"]["matches"]], ["XOP-HS"])
            self.assertEqual({p.name for p in backend.verified}, {"jurisdictions.parquet"})
            backend.verified.clear()
            missing_family = backend.call("resolve_point", {
                "lat": 0, "lon": 0, "scope_id": "MARINE", "family": "territorial_seas",
            })
            self.assertEqual(missing_family["error"]["code"], "selected_scope_unavailable")
            self.assertEqual({row["family_id"] for row in backend.call("get_geography", {"scope_id": "MARINE"})["coverage"]},
                             {"exclusive_economic_zone", "high_seas", "water_body"})
            backend.verified.clear()
            country_result = backend.call("resolve_point", {
                "lat": 0, "lon": 0, "scope_id": "USA", "family": "administrative",
            })
            self.assertEqual(country_result["matched"]["loc_id"], "USA-CA")
            self.assertEqual({p.name for p in backend.verified}, {"admin_0_3.parquet"})

    def test_marine_identity_and_bridge_relationships(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _package(root, "marine", "downloadable_geometry_package", "MARINE", {
                "reference/identities.parquet": ("reference", [
                    {"loc_id": "AAA-EEZ", "family": "marine_jurisdiction", "subtype": "exclusive_economic_zone", "name": "Example EEZ"},
                    {"loc_id": "XLAKE", "family": "water_body", "subtype": "water_body", "name": "Example lake"},
                ]),
                "display/water_bodies.parquet": ("display", [
                    {"loc_id": "XLAKE", "level": "water_body", "name": "Example lake",
                     "geometry": '{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}'},
                ]),
            })
            _package(root, "gers", "downloadable_external_reference_bridge", "BRIDGES", {
                "reference/overture_gers_loc_id_bridge.parquet": ("reference", [
                    {"loc_id": "AAA", "gers_division_id": "gers-exact", "iso3": "AAA",
                     "relationship_type": "equivalent_identity"},
                    {"loc_id": "AAA-EEZ", "gers_division_id": "gers-overlap", "iso3": "AAA",
                     "relationship_type": "overlaps"},
                ]),
            })
            backend = DownloadedGeometryBackend(root)
            catalog = backend.call("get_catalog", {"catalog": "geometry"})
            self.assertEqual({row["family_id"] for row in catalog["families"]},
                             {"exclusive_economic_zone", "water_body"})
            self.assertEqual(catalog["external_reference_bridges"][0]["external_system"], "overture_gers")
            self.assertEqual(backend.call("get_loc_id_info", {"loc_id": "AAA-EEZ"})["family"], "exclusive_economic_zone")
            self.assertEqual(backend.call("get_shape", {"loc_id": "AAA-EEZ"})["error"]["code"], "shape_unavailable")
            self.assertTrue(backend.call("get_shape", {"loc_id": "XLAKE"})["has_shape"])
            exact = backend.call("convert_reference", {"iso3": "AAA", "from_system": "overture_gers", "value": "gers-exact"})
            related = backend.call("convert_reference", {"iso3": "AAA", "from_system": "overture_gers", "value": "gers-overlap"})
            self.assertEqual(exact["resolved_loc_id"], "AAA")
            self.assertIsNone(related["resolved_loc_id"])
            self.assertEqual(related["matches"][0]["relationship_type"], "overlaps")


if __name__ == "__main__":
    unittest.main()
