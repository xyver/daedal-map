"""Contract regression for installed Marine and external Bridge packages."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Polygon, box

from downloaded_geometry_backend import DownloadedGeometryBackend
import downloaded_geometry_backend as backend_module


def _package(root: Path, name: str, profile: str, unit: str, files: dict[str, tuple[str, list[dict]]],
             tier: str | None = None):
    base = root / name
    base.mkdir(parents=True, exist_ok=True)
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


class InstalledBBoxTest(unittest.TestCase):
    corners = [{"lat": 0, "lon": 0}, {"lat": 0, "lon": 2},
               {"lat": 2, "lon": 0}, {"lat": 2, "lon": 2}]

    @staticmethod
    def row(loc_id, level, parent="", bounds=(0, 0, 2, 2), *, indexed=True, owner=None):
        row = {"loc_id": loc_id, "name": loc_id, "admin_level": level,
               "parent_loc_id": parent, "geometry": box(*bounds).wkb}
        if indexed:
            row.update(dict(zip(("bbox_min_lon", "bbox_min_lat", "bbox_max_lon", "bbox_max_lat"), bounds)))
        if owner is not None:
            row["admin_1_loc_id"] = owner
        return row

    def global_package(self, root, *, indexed=True, extra=None, tier="display"):
        component = "exact" if tier == "detail" else "display"
        rows = [self.row("CAN", 0, bounds=(-5, -5, 5, 5), indexed=indexed),
                self.row("CAN-GNT", 1, "CAN", bounds=(0, 0, 1, 2), indexed=indexed),
                self.row("CAN-GNT-GER", 2, "CAN-GNT", indexed=indexed)]
        files = {f"{component}/admin_{level}.parquet": (component, [row]) for level, row in enumerate(rows)}
        files["reference/identities.parquet"] = ("reference", [
            {"loc_id": r["loc_id"], "admin_level": r["admin_level"], "parent_loc_id": r["parent_loc_id"],
             "name": "Northwest Territories" if r["admin_level"] == 1 else r["name"]} for r in rows])
        files.update(extra or {})
        _package(root, "global", "downloadable_geometry_package", "GLOBAL", files, tier=tier)

    def country_package(self, root, *, tier="detail"):
        component = "exact" if tier == "detail" else "display"
        rows = [self.row("CAN", 0, bounds=(-5, -5, 5, 5), owner=""),
                self.row("CAN-NT", 1, "CAN", owner="CAN-NT"),
                self.row("CAN-NT-1", 2, "CAN-NT", bounds=(0, 0, 1, 2), owner="CAN-NT"),
                self.row("CAN-AB", 1, "CAN", owner="CAN-AB")]
        deep_rows = [self.row("CAN-NT-1-4", 4, "CAN-NT-1", owner="CAN-NT"),
                     self.row("CAN-NT-1-4-5", 5, "CAN-NT-1-4", owner="CAN-NT")]
        files = {f"{component}/admin/admin_0/0000_admin_0_3.parquet": (component, rows),
                 "reference/identities.parquet": ("reference", [
                     {k: v for k, v in r.items() if k != "geometry"} for r in rows + deep_rows])}
        if tier == "detail":
            for r in deep_rows:
                files[f"exact/admin/admin_{r['admin_level']}/0001_CAN-NT.parquet"] = ("exact", [r])
            files["exact/admin/admin_4/0002_CAN-AB.parquet"] = ("exact", [self.row("CAN-AB-4", 4, "CAN-AB")])
        _package(root, "country", "downloadable_geometry_package", "CAN", files, tier=tier)

    def test_global_identity_parent_ranking_and_country_scope_are_distinct(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.global_package(root)
            self.country_package(root)
            backend = DownloadedGeometryBackend(root)
            global_result = backend.call("resolve_bbox", {"corners": self.corners})
            best = global_result["best_match"]
            self.assertEqual(best["loc_id"], "CAN-GNT")
            self.assertEqual(best["name"], "Northwest Territories")
            self.assertEqual(best["parent_baseline_loc_id"], "CAN")
            self.assertEqual(global_result["matches_by_admin_level"]["0"][0]["parent_baseline_loc_id"], "WORLD")
            self.assertEqual(best["view_coverage_pct"], 50)
            self.assertEqual(best["loc_id_coverage_pct"], 100)
            self.assertEqual(best["intersection_over_union_pct"], 50)
            self.assertEqual(global_result["matches_by_admin_level"]["2"], [])
            self.assertTrue(all("global" in str(p) for p in backend.verified))
            country = backend.call("resolve_bbox", {"corners": self.corners, "scope_id": "can"})
            self.assertEqual(country["scope_id"], "CAN")
            self.assertEqual(country["matches_by_admin_level"]["0"][0]["parent_loc_id"], "")
            self.assertEqual(country["matches_by_admin_level"]["2"][0]["parent_loc_id"], "CAN-NT")
            self.assertEqual(country["matches_by_admin_level"]["2"][0]["deep_scope_loc_id"], "CAN-NT")
            self.assertNotIn("CAN-GNT", {r["loc_id"] for rs in country["matches_by_admin_level"].values() for r in rs})

    def test_global_bbox_preserves_explicit_root_parent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.global_package(root, extra={"reference/identities.parquet": ("reference", [
                {"loc_id": "CAN", "admin_level": 0, "parent_loc_id": "EXPLICIT-ROOT", "name": "Canada"}])})
            result = DownloadedGeometryBackend(root).call("resolve_bbox", {"corners": self.corners})
            self.assertEqual(result["matches_by_admin_level"]["0"][0]["parent_baseline_loc_id"], "EXPLICIT-ROOT")
            self.assertEqual(result["matches_by_admin_level"]["1"][0]["parent_baseline_loc_id"], "CAN")

    def test_deep_bbox_uses_admin1_owner_and_all_declared_levels(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.global_package(root)
            self.country_package(root)
            backend = DownloadedGeometryBackend(root)
            result = backend.call("resolve_deep_bbox", {"corners": self.corners, "shallow_loc_id": "can-nt-1"})
            self.assertEqual(result["admin_1_scope"], "CAN-NT")
            self.assertEqual([len(result["matches_by_admin_level"][str(level)]) for level in range(1, 7)], [1, 1, 0, 1, 1, 0])
            self.assertEqual(result["matches_by_admin_level"]["5"][0]["parent_loc_id"], "CAN-NT-1-4")
            self.assertFalse(any("global" in str(p) or "0002_CAN-AB" in str(p) for p in backend.verified))
            for bad_id in ("CAN", "CAN-GNT", "CAN-NT-1-4"):
                self.assertEqual(backend.call("resolve_deep_bbox", {"corners": self.corners, "shallow_loc_id": bad_id})["error"]["code"], "invalid_shallow_loc_id")

    def test_display_deep_bbox_keeps_shallow_matches_without_inventing_deep_data(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.country_package(root, tier="display")
            result = DownloadedGeometryBackend(root).call("resolve_deep_bbox", {"corners": self.corners, "shallow_loc_id": "CAN-NT"})
            self.assertTrue(result["matches_by_admin_level"]["1"])
            self.assertEqual(result["matches_by_admin_level"]["4"], [])

    def test_portable_display_bounds_are_cached_and_unmatched_shapes_are_not_read_again(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.global_package(root, indexed=False)
            backend = DownloadedGeometryBackend(root)
            first = backend.call("resolve_bbox", {"corners": self.corners})
            self.assertEqual(len(backend.bbox_indexes), 3)
            indexes = dict(backend.bbox_indexes)
            second = backend.call("resolve_bbox", {"corners": list(reversed(self.corners))})
            self.assertEqual(first, second)
            self.assertTrue(all(backend.bbox_indexes[p] is t for p, t in indexes.items()))
            with mock.patch.object(backend_module, "_shape", side_effect=AssertionError("unmatched shape decoded")):
                empty = backend.call("resolve_bbox", {"corners": [{"lat": lat, "lon": lon} for lat in (20, 21) for lon in (20, 21)]})
            self.assertIsNone(empty["best_match"])

    def test_candidate_limit_applies_before_geometry_and_across_admin_levels(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            candidates = [{"loc_id": f"AAA-{i}", "admin_level": 0 if i < 300 else 1,
                           "bbox_min_lon": 0., "bbox_min_lat": 0., "bbox_max_lon": 2., "bbox_max_lat": 2.} for i in range(501)]
            self.global_package(root, tier="detail", extra={"predicate/admin_bbox_index.parquet": ("predicate", candidates)})
            backend = DownloadedGeometryBackend(root)
            with mock.patch.object(backend, "_bbox_shape_rows", side_effect=AssertionError("geometry opened")):
                result = backend.call("resolve_bbox", {"corners": self.corners})
            self.assertEqual(result["error"]["code"], "bbox_too_broad")
            self.assertEqual(result["candidate_limit"], 500)
            self.assertEqual({p.name for p in backend.verified}, {"admin_bbox_index.parquet"})

    def test_cached_shapes_skip_parquet_but_still_reject_changed_member(self):
        with tempfile.TemporaryDirectory() as temp:
            self.global_package(Path(temp))
            backend = DownloadedGeometryBackend(temp)
            file = next(f for f in backend.packages[0]["files"]
                        if f["archive_path"] == "display/admin_0.parquet")
            first = list(backend._bbox_shape_rows(file, {"CAN"}))
            with mock.patch.object(backend_module.ds, "dataset", side_effect=AssertionError("Parquet reread")):
                self.assertEqual(first, list(backend._bbox_shape_rows(file, {"CAN"})))
            # A response consumer cannot mutate the retained metadata.
            first[0]["name"] = "changed"
            self.assertEqual(list(backend._bbox_shape_rows(file, {"CAN"}))[0]["name"], "CAN")
            with file["local_path"].open("ab") as stream:
                stream.write(b"changed")
            with self.assertRaisesRegex(ValueError, "hash/size mismatch"):
                list(backend._bbox_shape_rows(file, {"CAN"}))

    def test_cache_byte_and_row_limits_preserve_results_after_eviction(self):
        with tempfile.TemporaryDirectory() as temp:
            self.country_package(Path(temp))
            for byte_limit, row_limit in ((1, 128), (240, 128), (10000, 1)):
                with self.subTest(bytes=byte_limit, rows=row_limit), \
                        mock.patch.object(backend_module, "MAX_BBOX_CACHE_BYTES", byte_limit), \
                        mock.patch.object(backend_module, "MAX_BBOX_CACHE_ROWS", row_limit):
                    backend = DownloadedGeometryBackend(temp)
                    file = next(f for f in backend.packages[0]["files"]
                                if f["archive_path"].endswith("0000_admin_0_3.parquet"))
                    first = {}
                    for loc_id in ("CAN", "CAN-NT", "CAN-NT-1", "CAN-AB", "CAN"):
                        row = list(backend._bbox_shape_rows(file, {loc_id}))[0]
                        self.assertEqual(row, first.setdefault(loc_id, row))
                        self.assertLessEqual(backend.bbox_cache_bytes, byte_limit)
                        self.assertLessEqual(len(backend.bbox_shape_cache), row_limit)
                    if byte_limit == 1:
                        self.assertFalse(backend.bbox_shape_cache)
                    else:
                        self.assertEqual(next(reversed(backend.bbox_shape_cache))[-1], "CAN")

    def test_invalid_bbox_contracts_fail_before_any_file_read(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.global_package(root)
            backend = DownloadedGeometryBackend(root)
            bad_corners = [self.corners[:3], [self.corners[0]] * 4,
                           [{"lat": lat, "lon": lon} for lat in (0, 2) for lon in (-179, 179)]]
            for value in (True, float("nan"), float("inf"), "1", 86):
                bad_corners.append([{**self.corners[0], "lat": value}, *self.corners[1:]])
            bad_corners.append([{**self.corners[0], "extra": 1}, *self.corners[1:]])
            for corners in bad_corners:
                with self.subTest(corners=corners):
                    self.assertEqual(backend.call("resolve_bbox", {"corners": corners})["error"]["code"], "invalid_bbox")
            self.assertEqual(backend.call("resolve_bbox", {"corners": self.corners, "scope_id": "MARINE"})["error"]["code"], "invalid_bbox_scope")
            self.assertEqual(backend.call("resolve_bbox", {"corners": self.corners, "shallow_loc_id": "CAN-NT"})["error"]["code"], "bbox_contract_violation")
            self.assertEqual(backend.call("resolve_deep_bbox", {"corners": self.corners})["error"]["code"], "shallow_loc_id_required")
            self.assertFalse(backend.verified)

    def test_lite_has_no_bbox_geometry_and_does_not_borrow_global(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.global_package(root)
            _package(root, "lite", "downloadable_geometry_package", "CAN", {
                "reference/identities.parquet": ("reference", [{"loc_id": "CAN-NT", "admin_level": 1}]),
            }, tier="lite")
            backend = DownloadedGeometryBackend(root)
            self.assertEqual(backend.call("resolve_bbox", {"corners": self.corners, "scope_id": "CAN"})["error"]["code"], "selected_scope_unavailable")
            self.assertFalse(backend.verified)

    def test_stale_bbox_index_does_not_silently_drop_missing_shapes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            index = [{"loc_id": "AAA-MISSING", "admin_level": 1,
                      "bbox_min_lon": 0., "bbox_min_lat": 0., "bbox_max_lon": 2., "bbox_max_lat": 2.}]
            self.global_package(root, tier="detail", extra={"predicate/admin_bbox_index.parquet": ("predicate", index)})
            backend = DownloadedGeometryBackend(root)
            self.assertEqual(backend.call("resolve_bbox", {"corners": self.corners})["error"]["code"], "selected_scope_unavailable")

    def test_border_touch_and_polygon_hole_do_not_count_as_area_overlap(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            touch = self.row("AAA-TOUCH", 1, "AAA", bounds=(2, 0, 3, 2))
            hole = self.row("AAA-HOLE", 1, "AAA", bounds=(-2, -2, 4, 4))
            hole["geometry"] = Polygon(box(-2, -2, 4, 4).exterior.coords,
                                       [box(-1, -1, 3, 3).exterior.coords]).wkb
            self.global_package(root, extra={"display/admin_1.parquet": ("display", [touch, hole])})
            result = DownloadedGeometryBackend(root).call("resolve_bbox", {"corners": self.corners})
            self.assertEqual(result["matches_by_admin_level"]["1"], [])

    def test_same_tier_duplicates_are_rejected_instead_of_mixing_releases(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.global_package(root)
            _package(root, "duplicate", "downloadable_geometry_package", "GLOBAL", {}, tier="display")
            with self.assertRaisesRegex(ValueError, "Ambiguous installed bbox package"):
                DownloadedGeometryBackend(root).call("resolve_bbox", {"corners": self.corners})


class InstalledSideFamilyTest(unittest.TestCase):
    def test_country_reference_index_lists_installed_side_families(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _package(root, "country", "downloadable_geometry_package", "USA", {
                "reference/identities.parquet": ("reference", [
                    {"partition_id": "admin", "family": "admin_1"},
                    {"partition_id": "watershed", "family": "watershed"},
                    {"partition_id": "postal", "family": "postal_area"},
                ]),
            }, tier="lite")
            backend = DownloadedGeometryBackend(root)
            catalog = backend.call("get_catalog", {"catalog": "geometry"})
            self.assertEqual({row["family_id"] for row in catalog["families"]},
                             {"administrative", "watershed", "postal_area"})
            geography = backend.call("get_geography", {"family_id": "watershed"})
            self.assertEqual({row["scope_id"] for row in geography["coverage"]}, {"USA"})

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
                "display/admin/admin_0/0000_admin_0_3.parquet": ("display", [country_row]),
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
            self.assertEqual({p.name for p in backend.verified}, {"0000_admin_0_3.parquet"})

    def test_deep_point_does_not_borrow_global_shape_bank(self):
        polygon = '{"type":"Polygon","coordinates":[[[-1,-1],[1,-1],[1,1],[-1,1],[-1,-1]]]}'
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _package(root, "global", "downloadable_geometry_package", "GLOBAL", {
                "display/admin_0.parquet": ("display", [{"loc_id": "AAA", "geometry": polygon,
                    "bbox_min_lon": -1.0, "bbox_max_lon": 1.0, "bbox_min_lat": -1.0, "bbox_max_lat": 1.0}]),
                "display/admin_1.parquet": ("display", [{"loc_id": "AAA-1", "parent_id": "AAA",
                    "admin_level": 1, "geometry": polygon, "bbox_min_lon": -1.0,
                    "bbox_max_lon": 1.0, "bbox_min_lat": -1.0, "bbox_max_lat": 1.0}]),
            }, tier="display")
            _package(root, "country", "downloadable_geometry_package", "AAA", {
                "reference/identities.parquet": ("reference", [{"loc_id": "AAA", "admin_level": 0,
                    "iso_a3": "AAA"}]),
            }, tier="lite")
            backend = DownloadedGeometryBackend(root)
            result = backend.call("resolve_deep_point", {
                "lat": 0, "lon": 0, "shallow_loc_id": "AAA",
            })
            self.assertEqual(result["error"]["code"], "selected_scope_unavailable")

    def test_deep_point_selects_one_country_detail_partition(self):
        polygon = '{"type":"Polygon","coordinates":[[[-1,-1],[1,-1],[1,1],[-1,1],[-1,-1]]]}'
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _package(root, "global", "downloadable_geometry_package", "GLOBAL", {
                "display/admin_1.parquet": ("display", [{"loc_id": "AAA-X", "parent_id": "AAA",
                    "admin_level": 1, "geometry": polygon}]),
            }, tier="display")
            _package(root, "country", "downloadable_geometry_package", "AAA", {
                "reference/identities.parquet": ("reference", [
                    {"loc_id": "AAA", "admin_level": 0, "iso_a3": "AAA"},
                    {"loc_id": "AAA-X", "parent_id": "AAA", "admin_level": 1, "iso_a3": "AAA"},
                    {"loc_id": "AAA-X-4", "parent_id": "AAA-X", "admin_level": 4, "iso_a3": "AAA"},
                ]),
                "exact/admin/admin_4/0001_AAA-X.parquet": ("exact", [{
                    "loc_id": "AAA-X-4", "parent_id": "AAA-X", "admin_level": 4,
                    "iso_a3": "AAA", "geometry": polygon,
                    "bbox_min_lon": -1.0, "bbox_max_lon": 1.0,
                    "bbox_min_lat": -1.0, "bbox_max_lat": 1.0,
                }]),
            }, tier="detail")
            backend = DownloadedGeometryBackend(root)
            result = backend.call("resolve_deep_point", {
                "lat": 0, "lon": 0, "shallow_loc_id": "AAA-X", "target_admin_level": 4,
            })
            self.assertEqual(result["matched"]["loc_id"], "AAA-X-4")
            self.assertEqual([path.name for path in backend.verified if "exact" in path.parts or "display" in path.parts],
                             ["0001_AAA-X.parquet"])

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


    def test_bridge_mcp_tools_preserve_typed_relationships(self):
        external_id = "d5654a87-5adf-42b3-9ff7-c656797a1776"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _package(root, "gers", "downloadable_external_reference_bridge", "BRIDGES", {
                "reference/overture_gers_loc_id_bridge.parquet": ("reference", [
                    {"loc_id": "CAN", "gers_division_id": external_id, "iso3": "CAN",
                     "relationship_type": "equivalent_identity"},
                    {"loc_id": "CAN-BC", "gers_division_id": external_id, "iso3": "CAN",
                     "relationship_type": "overlaps"},
                ]),
            })
            backend = DownloadedGeometryBackend(root)
            self.assertEqual(backend.call("list_bridges", {})["bridges"][0]["system"], "overture_gers")
            self.assertEqual(backend.call("get_bridge", {"system": "overture_gers"})["bridge"]["system"], "overture_gers")
            result = backend.call("query_bridge", {"system": "overture_gers", "country_scope": "CAN",
                                                   "external_id": external_id})
            self.assertEqual(result["identity_bindings"], ["CAN"])
            self.assertEqual(result["edge_count"], 2)
            self.assertFalse(result["edges"][1]["identity_binding"])
            reverse = backend.call("query_bridge", {"system": "overture_gers", "country_scope": "CAN",
                                                    "loc_id": "CAN"})
            self.assertEqual(reverse["identity_bindings"], [external_id])


if __name__ == "__main__":
    unittest.main()
