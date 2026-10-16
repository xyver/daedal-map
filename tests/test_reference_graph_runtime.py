from __future__ import annotations

import json
import hashlib
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

import pandas as pd

from mapmover.geometry_handlers import get_location_info
from mapmover.runtime.reference_exchange import (
    get_geometry_availability,
    get_geometry_references,
    list_reference_systems,
    loc_id_references,
    resolve_reference,
)
from mapmover.runtime.reference_graph import (
    aliases_for_loc_id,
    identity,
    identity_at,
    identities,
    identify_aliases,
    public_alias_reference_systems,
    relationships_between_loc_ids,
    relationships_for_loc_id,
    resolve_public_loc_id,
    resolve_public_loc_ids,
    where_is_geography_data,
)
from mapmover.runtime import reference_graph


class ReferenceGraphRuntimeTests(unittest.TestCase):
    def test_health_source_windows_are_hash_pinned_and_month_precise(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory)
            root = (data_root / "geometry/countries/CAN/releases/geometry/"
                    "can_geometry_1_3_5/runtime/reference_graph")
            root.mkdir(parents=True)
            source = root.parent / "source_releases.json"
            source.write_text(json.dumps({"source_releases": {
                "can_national_health_region_2023": {
                    "source_release_id": "can_national_health_region_2023",
                    "canonical_family": "health_region",
                    "source_native_subtype": "statistics_canada_health_region",
                    "selection_from": "2023-09-01",
                    "selection_to_exclusive": "2024-09-01",
                },
            }}), encoding="utf-8")
            package = root.parent.parent / "full_package_manifest.json"
            package.write_text(json.dumps({
                "status": "PASS_LOCAL_FULL", "release_id": "can_geometry_1_3_5",
                "source_release_metadata_path": source.relative_to(data_root).as_posix(),
                "source_release_metadata_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                "graph_temporal_source_bindings": {
                    "statcan_health_regions_2023": "can_national_health_region_2023",
                },
            }), encoding="utf-8")
            with mock.patch.object(reference_graph, "DATA_ROOT", data_root):
                reference_graph._source_selection_bindings.cache_clear()
                bindings = reference_graph._source_selection_bindings(str(root), False)
                start, end, source_id = bindings["statcan_health_regions_2023"]
                self.assertEqual((start, end, source_id), (
                    "2023-09-01", "2024-09-01", "can_national_health_region_2023"))
                self.assertFalse(reference_graph._identity_row_covers_date(
                    {"valid_from": start, "valid_to": end}, date(2023, 8, 31)))
                self.assertTrue(reference_graph._identity_row_covers_date(
                    {"valid_from": start, "valid_to": end}, date(2023, 9, 1)))
                self.assertFalse(reference_graph._identity_row_covers_date(
                    {"valid_from": start, "valid_to": end}, date(2024, 9, 1)))
                reference_graph._source_selection_bindings.cache_clear()
                source.write_text(source.read_text(encoding="utf-8") + " ", encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "hash drift"):
                    reference_graph._source_selection_bindings(str(root), False)
                reference_graph._source_selection_bindings.cache_clear()
                package.unlink()
                with self.assertRaisesRegex(ValueError, "binding package is missing"):
                    reference_graph._source_selection_bindings(str(root), False)
                reference_graph._source_selection_bindings.cache_clear()

    def test_local_override_selects_two_candidate_graphs_without_catalog_promotion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory)
            roots = []
            for country in ("GBR", "MEX"):
                root = data_root / "geometry" / "countries" / country / "reference_graph"
                root.mkdir(parents=True)
                (root / "manifest.json").write_text(json.dumps({
                    "country": country, "status": "PASS", "release_id": f"{country.lower()}_candidate",
                }), encoding="utf-8")
                for name in ("identity_partitions.parquet", "endpoint_families.parquet"):
                    (root / name).write_bytes(b"test")
                roots.append(root)
            selected = reference_graph._discover_roots(
                str(data_root), json.dumps([str(root) for root in roots]), False,
            )
            self.assertEqual([country for country, _ in selected], ["GBR", "MEX"])
            with self.assertRaisesRegex(ValueError, "duplicate countries"):
                reference_graph._discover_roots(
                    str(data_root), json.dumps([str(roots[0]), str(roots[0])]), False,
                )

    def test_country_discovery_index_uses_only_its_own_release(self) -> None:
        manifest = {
            "status": "PASS",
            "schema_version": "reference_discovery_index_v1",
            "artifact": "identifier_index.parquet",
            "source_graphs": {
                "BRA": {"release_id": "bra_reference_graph_1_2_5"},
            },
        }
        bra_root = Path("geometry/countries/BRA/reference_graph")

        def graph_json(path: Path):
            return (
                manifest
                if path == reference_graph._country_discovery_paths("BRA")[1]
                else {"release_id": "bra_reference_graph_1_2_5"}
            )

        reference_graph._country_discovery_index_current.cache_clear()
        with (
            mock.patch.object(reference_graph, "_graph_json", side_effect=graph_json),
            mock.patch.object(reference_graph, "parquet_columns", return_value=("external_id",)),
            mock.patch.object(reference_graph, "reference_graph_roots", return_value={"BRA": bra_root}),
            mock.patch.object(reference_graph, "global_reference_graph_root", return_value=None),
        ):
            self.assertTrue(reference_graph._country_discovery_index_current("BRA", True, str(bra_root)))
        reference_graph._country_discovery_index_current.cache_clear()

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        pd.DataFrame([{
            "loc_id": "TST-A-001", "family": "test_sidechain", "native_id": "001",
            "name": "Test Area", "parent_loc_id": "", "admin_level": None,
            "namespace_release": "test_2026", "valid_from": "2026-01-01", "valid_to": "",
            "has_shape": True, "geometry_bank": "test/shapes.parquet",
            "geometry_status": "approved", "source_system": "Test Authority",
            "source_vintage": "2026",
        }, {
            "loc_id": "TST-B-002", "family": "test_sidechain", "native_id": "002",
            "name": "Related Area", "parent_loc_id": "", "admin_level": None,
            "namespace_release": "test_2026", "valid_from": "2026-01-01", "valid_to": "",
            "has_shape": False, "geometry_bank": "", "geometry_status": "identity_only",
            "source_system": "Test Authority", "source_vintage": "2026",
        }]).to_parquet(self.root / "identities.parquet", index=False)
        pd.read_parquet(self.root / "identities.parquet").to_parquet(
            self.root / "identity_versions.parquet", index=False
        )
        pd.DataFrame([{
            "reference_system": "test.code", "external_id": "001",
            "loc_id": "TST-A-001", "alias_type": "official_code",
            "source_system": "Test Authority", "source_vintage": "2026",
        }, {
            "reference_system": "public.tst.test_sidechain.v2", "external_id": "TST-PUBLIC-A",
            "loc_id": "TST-A-001", "alias_type": "preferred_public_loc_id",
            "source_system": "DaedalMap", "source_vintage": "2026",
        }, {
            "reference_system": "public.tst.test_sidechain.v2", "external_id": "TST-PUBLIC-AMBIG",
            "loc_id": "TST-A-001", "alias_type": "preferred_public_loc_id",
            "source_system": "DaedalMap", "source_vintage": "2026",
        }, {
            "reference_system": "public.tst.test_sidechain.v2", "external_id": "TST-PUBLIC-AMBIG",
            "loc_id": "TST-B-002", "alias_type": "preferred_public_loc_id",
            "source_system": "DaedalMap", "source_vintage": "2026",
        }, {
            "reference_system": "daedalmap.public.tst.test_sidechain.v1", "external_id": "TST-LEGACY-A",
            "loc_id": "TST-A-001", "alias_type": "preferred_public_loc_id",
            "source_system": "DaedalMap", "source_vintage": "2025",
        }]).to_parquet(self.root / "aliases.parquet", index=False)
        pd.DataFrame([{
            "relationship_id": "TST-REL-1", "source_family": "test_sidechain",
            "source_id": "001", "source_loc_id": "TST-A-001", "source_name": "Test Area",
            "target_family": "test_sidechain", "target_id": "002",
            "target_loc_id": "TST-B-002", "target_name": "Related Area",
            "relationship_type": "spatial_overlap", "relationship_subtype": "test_overlap",
            "method": "measured_polygon_intersection", "authority": "Test Authority",
            "relationship_vintage": "2026", "valid_from": None, "valid_to": None,
            "intersection_area": 1.0, "source_area": 2.0, "target_area": 4.0,
            "source_area_share": 0.5, "target_area_share": 0.25,
            "rank_by_source_area": 1, "rank_by_target_area": 1, "is_primary": True,
            "primary_policy": "largest_overlap", "source_centroid_target_loc_id": "TST-B-002",
            "evidence_member_count": None, "area_crs": "EPSG:6933",
            "source_artifact": "test.parquet", "source_release": "test_2026",
            "target_release": "test_2026", "has_source_shape": True,
            "has_target_shape": False, "review_status": "generated_verified",
        }]).to_parquet(self.root / "relationships.parquet", index=False)
        pd.DataFrame([{
            "partition_id": "test_sidechain", "family": "test_sidechain",
            "kind": "sidechain_identity", "path": str(self.root / "identities.parquet"),
            "sha256": "test", "row_count": 2, "selector": "",
            "strict_parent_source": False,
        }]).to_parquet(self.root / "identity_partitions.parquet", index=False)
        pd.DataFrame([{
            "partition_id": "test_sidechain", "family": "test_sidechain",
            "path": str(self.root / "aliases.parquet"), "sha256": "test", "row_count": 4,
        }]).to_parquet(self.root / "alias_partitions.parquet", index=False)
        pd.DataFrame([{
            "partition_id": "test_relationships", "semantic_type": "measured_spatial_overlap",
            "evidence_class": "test", "source_family": "test_sidechain",
            "target_family": "test_sidechain", "path": str(self.root / "relationships.parquet"),
            "sha256": "test", "index_path": "", "index_sha256": "", "row_count": 1,
            "payload_mode": "materialized_partition", "strict_parent": False, "succession": False,
        }]).to_parquet(self.root / "relationship_partitions.parquet", index=False)
        pd.DataFrame(columns=["partition_id", "family", "path", "sha256", "row_count"]).to_parquet(
            self.root / "shape_partitions.parquet", index=False
        )
        pd.DataFrame([{
            "family": "test_sidechain", "status": "identity_admitted", "identity_admitted": True,
        }]).to_parquet(self.root / "endpoint_families.parquet", index=False)
        pd.DataFrame([{
            "role": "test", "package_id": "test", "path": "test",
            "sha256": "test", "bytes": 0,
        }]).to_parquet(self.root / "source_manifests.parquet", index=False)
        (self.root / "manifest.json").write_text(json.dumps({
            "schema_version": "0.2.0", "graph_scope": "country", "country": "TST",
            "release_id": "test_candidate", "status": "PASS",
            "publication_status": "test", "completeness_status": "complete_declared_scope",
            "metrics": {"identities": 2, "relationships": 1},
        }), encoding="utf-8")
        self.env = mock.patch.dict(os.environ, {
            "GEOGRAPHY_REFERENCE_GRAPH_ROOT": str(self.root),
            "DEPLOYMENT": "local", "STORAGE_MODE": "local",
        })
        self.env.start()

    def tearDown(self) -> None:
        self.env.stop()
        self.temp.cleanup()

    def test_reports_explicit_local_candidate(self) -> None:
        report = where_is_geography_data()
        self.assertTrue(report["ok"])
        self.assertEqual(report["mode"], "explicit_runtime_selection")
        self.assertEqual(report["release_id"], "test_candidate")
        self.assertFalse(report["local_data_uploaded"])

    def test_identity_alias_and_relationship_queries(self) -> None:
        self.assertEqual(identity("TST-A-001")["family"], "test_sidechain")
        self.assertTrue(any(row["external_id"] == "001" for row in aliases_for_loc_id("TST-A-001")))
        self.assertEqual(relationships_for_loc_id("TST-A-001")[0]["target_loc_id"], "TST-B-002")

    def test_exact_pair_reads_declared_graph_route_without_shape_columns(self) -> None:
        statements: list[str] = []
        real_connection = reference_graph._connection

        class RecordingConnection:
            def __init__(self):
                self.connection = real_connection()

            def execute(self, statement, parameters=None):
                statements.append(statement)
                return self.connection.execute(statement, parameters or [])

            def close(self):
                self.connection.close()

        with mock.patch.object(reference_graph, "_connection", side_effect=RecordingConnection):
            rows = relationships_between_loc_ids("TST-A-001", "TST-B-002")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["relationship_type"], "spatial_overlap")
        pair_queries = [statement for statement in statements if "source_loc_id = ? AND target_loc_id = ?" in statement]
        self.assertTrue(pair_queries)
        self.assertTrue(all("SELECT *" not in statement for statement in pair_queries))
        self.assertTrue(all("geometry" not in statement.lower() for statement in pair_queries))
        self.assertEqual(relationships_between_loc_ids("TST-A-001", "TST-MISSING"), [])

    def test_referenced_parent_partition_exposes_direct_graph_edges(self) -> None:
        spine = self.root / "admin_spine.parquet"
        pd.DataFrame([
            {"loc_id": "TST-A-001", "parent_id": "TST-B-002", "admin_level": 2,
             "parent_admin_level": 1, "source_system": "test_admin", "valid_from": "2020-01-01"},
            {"loc_id": "TST-C-003", "parent_id": "TST-A-001", "admin_level": 3,
             "parent_admin_level": 2, "source_system": "test_admin", "valid_from": "2020-01-01"},
        ]).to_parquet(spine, index=False)
        pd.DataFrame([{
            "partition_id": "strict_parent:test", "semantic_type": "strict_parent",
            "evidence_class": "admin_spine_parent_column", "source_family": "test_sidechain",
            "target_family": "test_sidechain", "path": str(spine), "sha256": "test",
            "index_path": "", "index_sha256": "", "row_count": 2,
            "payload_mode": "referenced_partition", "strict_parent": True, "succession": False,
        }]).to_parquet(self.root / "relationship_partitions.parquet", index=False)
        edges = relationships_for_loc_id("TST-A-001")
        self.assertEqual({(row["source_loc_id"], row["target_loc_id"]) for row in edges}, {
            ("TST-A-001", "TST-B-002"), ("TST-C-003", "TST-A-001"),
        })
        self.assertTrue(all(row["relationship_type"] == "strict_parent" for row in edges))
        pair = relationships_between_loc_ids("TST-A-001", "TST-B-002")
        self.assertEqual(pair[0]["relationship_type"], "strict_parent")

    def test_exact_identity_route_miss_does_not_scan_all_partitions(self) -> None:
        pd.DataFrame([{
            "loc_id": "TST-A-001", "path": str(self.root / "identities.parquet"),
        }]).to_parquet(self.root / "identity_routes.parquet", index=False)
        self.assertEqual(identity("TST-A-001")["name"], "Test Area")
        self.assertEqual(reference_graph._route_paths(
            self.root, reference_graph.IDENTITY_ROUTE_INDEX, "loc_id", ["TST-MISSING"],
        ), [])
        self.assertEqual(reference_graph._table_source_for_roots(
            "identities", [self.root], loc_ids=["TST-MISSING"],
        ), "")
        self.assertIsNone(identity("TST-MISSING"))

    def test_required_identity_route_miss_does_not_open_partition(self) -> None:
        from mapmover.geometry_handlers import get_location_infos

        pd.DataFrame([{
            "loc_id": "TST-A-001", "path": str(self.root / "identities.parquet"),
        }]).to_parquet(self.root / "identity_routes.parquet", index=False)
        with (
            mock.patch.object(reference_graph, "_connection") as connection,
            mock.patch.object(reference_graph, "global_reference_graph_root", return_value=None),
            mock.patch("mapmover.geometry_handlers.get_selection_geometry_metadata") as geometry,
        ):
            infos = get_location_infos(["TST-MISSING"], graph_authoritative=True)

        self.assertEqual(infos[0]["error"], "no identity in the active reference graph")
        connection.assert_not_called()
        geometry.assert_not_called()

    def test_required_identity_route_hit_uses_pinned_partition(self) -> None:
        pd.DataFrame([{
            "loc_id": "TST-A-001", "path": str(self.root / "identities.parquet"),
        }]).to_parquet(self.root / "identity_routes.parquet", index=False)
        with mock.patch.object(reference_graph, "_partition_paths") as broad_partitions:
            rows = identities(["TST-A-001"], require_route=True)

        self.assertEqual(rows[0]["name"], "Test Area")
        broad_partitions.assert_not_called()

    def test_required_identity_route_missing_fails_without_partition_scan(self) -> None:
        from mapmover.geometry_handlers import get_location_infos

        with (
            mock.patch.object(reference_graph, "_partition_paths") as partitions,
            mock.patch("mapmover.geometry_handlers.get_selection_geometry_metadata") as geometry,
        ):
            with self.assertRaisesRegex(reference_graph.ReferenceRouteUnavailable, "identity_routes.parquet"):
                get_location_infos(["TST-A-001"], graph_authoritative=True)

        partitions.assert_not_called()
        geometry.assert_not_called()

    def test_batch_location_info_reads_sidechain_identity_without_geometry(self) -> None:
        from mapmover.geometry_handlers import get_location_infos

        with (
            mock.patch("mapmover.runtime.reference_graph.identities", return_value=[{
                "loc_id": "TST-A-001", "name": "Test Area", "family": "test_sidechain",
                "parent_loc_id": "TST", "has_shape": True,
            }, {
                "loc_id": "TST-B-002", "name": "Related Area", "family": "test_sidechain",
                "parent_loc_id": "TST", "has_shape": False,
            }]) as identities_reader,
            mock.patch("mapmover.runtime.reference_graph.where_is_geography_data", return_value={
                "release_id": "test_candidate", "status": "PASS",
            }),
            mock.patch("mapmover.geometry_handlers.get_selection_geometry_metadata") as geometry,
        ):
            infos = get_location_infos(["TST-A-001", "TST-B-002", "TST-A-001"], fallback=False)

        self.assertEqual(infos[0]["name"], "Test Area")
        self.assertEqual(infos[0]["family"], "test_sidechain")
        self.assertTrue(infos[0]["has_polygon"])
        self.assertEqual([item["name"] for item in infos], ["Test Area", "Related Area", "Test Area"])
        identities_reader.assert_called_once_with(["TST-A-001", "TST-B-002"])
        geometry.assert_not_called()

    def test_alias_identification_fails_when_country_index_is_missing(self) -> None:
        second = self.root / "aliases_second.parquet"
        pd.DataFrame([{
            "reference_system": "test.other", "external_id": "002",
            "loc_id": "TST-B-002", "alias_type": "official_code",
        }]).to_parquet(second, index=False)
        partitions = pd.read_parquet(self.root / "alias_partitions.parquet")
        extra = partitions.iloc[[0]].copy()
        extra["partition_id"] = "test_other"
        extra["path"] = str(second)
        pd.concat([partitions, extra], ignore_index=True).to_parquet(
            self.root / "alias_partitions.parquet", index=False,
        )
        opened: list[str] = []
        real_connection = reference_graph._connection

        class RecordingConnection:
            def __init__(self):
                self.connection = real_connection()

            def execute(self, statement, parameters=None):
                if parameters:
                    opened.append(str(parameters[0]))
                return self.connection.execute(statement, parameters or [])

            def close(self):
                self.connection.close()

        with (
            mock.patch.object(reference_graph, "_country_discovery_index_current", return_value=False),
            mock.patch.object(reference_graph, "_connection", side_effect=RecordingConnection),
        ):
            with self.assertRaisesRegex(reference_graph.ReferenceRouteUnavailable, "index unavailable"):
                identify_aliases(["001", "002"], iso3="TST")
        self.assertEqual(opened, [])

    def test_country_alias_identification_uses_current_discovery_index(self) -> None:
        data_root = self.root / "data"
        discovery = data_root / "geometry" / "countries" / "TST" / "reference_discovery" / "identifier_index.parquet"
        discovery.parent.mkdir(parents=True)
        (discovery.parent / "manifest.json").write_text(json.dumps({
            "status": "PASS",
            "schema_version": "reference_discovery_index_v1",
            "artifact": "identifier_index.parquet",
            "source_graphs": {
                "TST": {"release_id": "test_candidate"},
            },
        }), encoding="utf-8")
        pd.DataFrame([
            {"external_id": "001", "reference_system": "test.code", "loc_id": "TST-A-001", "alias_type": "official_code", "country_scope": "TST"},
        ]).to_parquet(discovery, index=False)
        opened: list[str] = []
        real_connection = reference_graph._connection

        class RecordingConnection:
            def __init__(self):
                self.connection = real_connection()

            def execute(self, statement, parameters=None):
                opened.append(str(parameters[0]))
                return self.connection.execute(statement, parameters or [])

            def close(self):
                self.connection.close()

        with (
            mock.patch.object(reference_graph, "DATA_ROOT", data_root),
            mock.patch.object(reference_graph, "_connection", side_effect=RecordingConnection),
        ):
            reference_graph._country_discovery_index_current.cache_clear()
            self.assertTrue(reference_graph._country_discovery_index_current("TST", False, str(self.root)))
            rows = identify_aliases(["001"], iso3="TST", reference_system="test.code")
            reference_graph._country_discovery_index_current.cache_clear()

        self.assertEqual([row["loc_id"] for row in rows], ["TST-A-001"])
        self.assertEqual(opened, [str(discovery)])

    def test_unscoped_alias_identification_reads_country_indexes_sequentially(self) -> None:
        data_root = self.root / "data"
        for country in ("AAA", "BBB"):
            index = data_root / "geometry" / "countries" / country / "reference_discovery" / "identifier_index.parquet"
            index.parent.mkdir(parents=True)
            pd.DataFrame([{
                "external_id": "001", "reference_system": "test.code",
                "loc_id": f"{country}-A-001", "alias_type": "official_code",
                "country_scope": country,
            }]).to_parquet(index, index=False)
        opened: list[str] = []
        real_connection = reference_graph._connection

        class RecordingConnection:
            def __init__(self):
                self.connection = real_connection()

            def execute(self, statement, parameters=None):
                opened.append(str(parameters[0]))
                return self.connection.execute(statement, parameters or [])

            def close(self):
                self.connection.close()

        with (
            mock.patch.object(reference_graph, "DATA_ROOT", data_root),
            mock.patch.object(reference_graph, "reference_graph_roots", return_value={"AAA": self.root, "BBB": self.root}),
            mock.patch.object(reference_graph, "_country_discovery_index_current", return_value=True),
            mock.patch.object(reference_graph, "reference_graph_available", return_value=True),
            mock.patch.object(reference_graph, "_connection", side_effect=RecordingConnection),
        ):
            rows = identify_aliases(["001"])
        self.assertEqual({row["loc_id"] for row in rows}, {"AAA-A-001", "BBB-A-001"})
        self.assertEqual(len(opened), 2)
        self.assertIn("AAA", opened[0])
        self.assertIn("BBB", opened[1])

    def test_alias_identification_honors_cancellation_between_files(self) -> None:
        with mock.patch.object(reference_graph, "_country_discovery_index_current", return_value=True):
            with self.assertRaisesRegex(RuntimeError, "cancelled"):
                identify_aliases(
                    ["001"], iso3="TST",
                    cancelled=lambda: (_ for _ in ()).throw(RuntimeError("cancelled")),
                )

    def test_undated_single_and_batch_identity_queries_select_newest_version(self) -> None:
        current = pd.read_parquet(self.root / "identities.parquet")
        latest = current.loc[current.loc_id.eq("TST-A-001")].copy()
        latest["name"] = "Current Area"
        latest["namespace_release"] = "test_2026"
        latest["source_vintage"] = "2026"
        latest["valid_from"] = "2026-01-01"
        older = latest.copy()
        older["name"] = "Earlier Area"
        older["namespace_release"] = "test_2025"
        older["source_vintage"] = "2025"
        older["valid_from"] = "2025-01-01"
        others = current.loc[~current.loc_id.eq("TST-A-001")]
        # Put the newest row first so a physical first/last-row accident cannot
        # make both the single and batch APIs agree with the expected result.
        pd.concat([latest, older, others], ignore_index=True).to_parquet(
            self.root / "identities.parquet", index=False,
        )

        self.assertEqual(identity("TST-A-001")["name"], "Current Area")
        self.assertEqual(identities(["TST-A-001"])[0]["name"], "Current Area")
        self.assertEqual(identity_at("TST-A-001", date(2025, 6, 1))["name"], "Earlier Area")
        self.assertIsNone(identity_at("TST-A-001", date(2024, 6, 1)))

    def test_dated_identity_outranks_shared_undated_spine_row(self) -> None:
        current = pd.read_parquet(self.root / "identities.parquet")
        base = current.loc[current.loc_id.eq("TST-A-001")].iloc[0].to_dict()
        undated = {**base, "name": "Undated spine", "valid_from": "", "valid_to": ""}
        historical = {
            **base, "name": "Historical frame", "frame_id": "TST-A-001@1990",
            "valid_from": "1990-01-01", "valid_to": "2000-01-01",
        }
        pd.DataFrame([undated, historical]).to_parquet(
            self.root / "identities.parquet", index=False,
        )

        selected = identity_at("TST-A-001", date(1995, 6, 1))

        self.assertEqual(selected["name"], "Historical frame")
        self.assertEqual(selected["frame_id"], "TST-A-001@1990")

    def test_preferred_public_loc_id_resolves_and_is_discoverable(self) -> None:
        resolved = resolve_public_loc_id("tst-public-a")
        self.assertTrue(resolved["ok"])
        self.assertEqual(resolved["loc_id"], "TST-A-001")
        self.assertEqual(resolved["reference_system"], "public.tst.test_sidechain.v2")
        systems = {row["system"]: row for row in public_alias_reference_systems(iso3="TST")}
        self.assertEqual(systems["public.tst.test_sidechain.v2"]["public_id_count"], 2)
        legacy = resolve_public_loc_id("tst-legacy-a")
        self.assertTrue(legacy["ok"])
        self.assertEqual(legacy["loc_id"], "TST-A-001")
        listed = {row["system"]: row for row in list_reference_systems(country_scope="TST")["systems"]}
        public = listed["public.tst.test_sidechain.v2"]
        self.assertTrue(public["exchangeable"])
        self.assertEqual(public["exchange_via"], "preferred_public_loc_id")

    def test_preferred_public_loc_id_ambiguity_fails_closed(self) -> None:
        resolved = resolve_public_loc_id("TST-PUBLIC-AMBIG")
        self.assertFalse(resolved["ok"])
        self.assertEqual(resolved["error"]["code"], "ambiguous_public_loc_id")
        self.assertEqual(resolved["candidate_loc_ids"], ["TST-A-001", "TST-B-002"])
        direct = resolve_reference(
            from_system="public.tst.test_sidechain.v2",
            value="TST-PUBLIC-AMBIG",
            iso3="TST",
        )
        self.assertFalse(direct["ok"])
        self.assertIsNone(direct["resolved_loc_id"])

    def test_public_loc_id_batch_matches_single_resolution_semantics(self) -> None:
        requested = ["TST-A-001", "TST-PUBLIC-A", "TST-PUBLIC-AMBIG", "TST-MISSING", "TST-PUBLIC-A"]

        batched = resolve_public_loc_ids(requested)
        singles = [resolve_public_loc_id(value) for value in requested]

        self.assertEqual(batched, singles)

    def test_indexed_public_alias_candidates_uses_exact_current_country_rows(self) -> None:
        index = self.root / "geometry" / "countries" / "TST" / "reference_discovery" / "identifier_index.parquet"
        index.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([
            {"external_id": "TST-PUBLIC-A", "reference_system": "public.tst.test_sidechain.v2",
             "loc_id": "TST-A-001", "alias_type": "preferred_public_loc_id", "country_scope": "TST"},
            {"external_id": "TST-PUBLIC-A", "reference_system": "public.tst.test_sidechain.v2",
             "loc_id": "OTHER-A-001", "alias_type": "preferred_public_loc_id", "country_scope": "OTHER"},
            {"external_id": "TST-PUBLIC-A", "reference_system": "test.code",
             "loc_id": "TST-B-002", "alias_type": "official_code", "country_scope": "TST"},
        ]).to_parquet(index, index=False)
        with (
            mock.patch.object(reference_graph, "DATA_ROOT", self.root),
            mock.patch.object(reference_graph, "reference_graph_roots", return_value={"TST": self.root}),
            mock.patch.object(reference_graph, "_country_discovery_index_current", return_value=True),
            mock.patch.object(reference_graph, "reference_graph_available", return_value=True),
        ):
            rows = reference_graph.indexed_public_alias_candidates(["tst-public-a", "TST-MISSING"])
        self.assertEqual([row["loc_id"] for row in rows["TST-PUBLIC-A"]], ["TST-A-001"])
        self.assertEqual(rows["TST-MISSING"], [])
        with (
            mock.patch.object(reference_graph, "reference_graph_roots", return_value={"TST": self.root}),
            mock.patch.object(reference_graph, "_country_discovery_index_current", return_value=False),
            mock.patch.object(reference_graph, "reference_graph_available", return_value=True),
        ):
            self.assertIsNone(reference_graph.indexed_public_alias_candidates(["TST-PUBLIC-A"]))

    def test_indexed_legacy_usa_aliases_use_official_codes_in_same_file(self) -> None:
        index = self.root / "geometry" / "countries" / "USA" / "reference_discovery" / "identifier_index.parquet"
        index.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([
            {"external_id": "42223", "reference_system": "usa.census.2020.zcta5.geoid",
             "loc_id": "USA-TN-125-101400-POSTAL-42223", "alias_type": "official_identifier", "country_scope": "USA"},
            {"external_id": "42223", "reference_system": "usa.census.2020.zcta5.geoid",
             "loc_id": "OTHER-POSTAL-42223", "alias_type": "official_identifier", "country_scope": "OTHER"},
            {"external_id": "1234", "reference_system": "usa.census.2025.aiannhce",
             "loc_id": "USA-TRIBAL-EXAMPLE", "alias_type": "official_identifier", "country_scope": "USA"},
        ]).to_parquet(index, index=False)
        with (
            mock.patch.object(reference_graph, "DATA_ROOT", self.root),
            mock.patch.object(reference_graph, "reference_graph_roots", return_value={"USA": self.root}),
            mock.patch.object(reference_graph, "_country_discovery_index_current", return_value=True),
            mock.patch.object(reference_graph, "reference_graph_available", return_value=True),
        ):
            rows = reference_graph.indexed_public_alias_candidates([
                "USA-Z-42223", "USA-TRIBAL-1234", "USA-Z-99999",
            ])

        self.assertEqual([row["loc_id"] for row in rows["USA-Z-42223"]], ["USA-TN-125-101400-POSTAL-42223"])
        self.assertEqual([row["loc_id"] for row in rows["USA-TRIBAL-1234"]], ["USA-TRIBAL-EXAMPLE"])
        self.assertEqual(rows["USA-Z-99999"], [])

    def test_geometry_calls_preserve_canonical_output_and_requested_public_alias(self) -> None:
        metadata = [{
            "loc_id": "TST-A-001", "name": "Test Area", "admin_level": None,
            "centroid_lon": 1.0, "centroid_lat": 2.0,
            "bbox_min_lon": 0.0, "bbox_min_lat": 1.0,
            "bbox_max_lon": 2.0, "bbox_max_lat": 3.0,
        }]
        with mock.patch(
            "mapmover.runtime.reference_exchange.get_selection_geometry_metadata",
            return_value=metadata,
        ):
            checked = get_geometry_availability(["TST-PUBLIC-A", "TST-PUBLIC-AMBIG"])
            fetched = get_geometry_references(["TST-PUBLIC-A"], include_polygon=False, include_info=False)
        self.assertEqual(checked["items"][0]["loc_id"], "TST-A-001")
        self.assertEqual(checked["items"][0]["requested_loc_id"], "TST-PUBLIC-A")
        self.assertEqual(checked["items"][1]["error"]["code"], "ambiguous_public_loc_id")
        self.assertEqual(fetched["results"][0]["loc_id"], "TST-A-001")
        self.assertEqual(fetched["results"][0]["public_alias"], "TST-PUBLIC-A")

    def test_loc_id_references_accepts_preferred_public_alias(self) -> None:
        result = loc_id_references("TST-PUBLIC-A", limit_per_system=5)
        self.assertTrue(result["ok"])
        self.assertEqual(result["loc_id"], "TST-A-001")
        self.assertEqual(result["requested_loc_id"], "TST-PUBLIC-A")

    def test_relationship_query_avoids_global_order_by(self) -> None:
        statements: list[str] = []
        real_connection = reference_graph._connection

        class RecordingConnection:
            def __init__(self):
                self.connection = real_connection()

            def execute(self, statement, parameters=None):
                statements.append(statement)
                return self.connection.execute(statement, parameters or [])

            def close(self):
                self.connection.close()

        with mock.patch.object(reference_graph, "_connection", side_effect=RecordingConnection):
            rows = relationships_for_loc_id("TST-A-001")

        self.assertEqual(len(rows), 1)
        # One exact identity lookup discovers the endpoint family, after which
        # outgoing and incoming scans open only matching relationship partitions.
        self.assertEqual(len(statements), 3)
        relationship_statements = statements[-2:]
        self.assertTrue(all("ORDER BY" not in statement.upper() for statement in relationship_statements))
        self.assertTrue(all("LIMIT ?" in statement.upper() for statement in relationship_statements))

    def test_existing_reference_tools_use_graph_without_new_contract(self) -> None:
        resolved = resolve_reference(from_system="test.code", value="001", iso3="TST")
        self.assertTrue(resolved["ok"])
        self.assertEqual(resolved["resolved_loc_id"], "TST-A-001")
        self.assertEqual(resolved["resolved_family"], "test_sidechain")
        references = loc_id_references("TST-A-001", limit_per_system=5)
        self.assertEqual(references["family"], "test_sidechain")
        self.assertTrue(any(item.get("relationship_id") == "TST-REL-1" for item in references["references"]))

    def test_reference_resolution_scopes_graph_aliases_to_requested_country(self) -> None:
        with mock.patch(
            "mapmover.runtime.reference_graph.resolve_alias",
            return_value=[{"loc_id": "TST-A-001"}],
        ) as resolver:
            resolved = resolve_reference(from_system="test.code", value="001", iso3="TST")

        self.assertTrue(resolved["ok"])
        resolver.assert_called_once_with("test.code", "001", limit=10, iso3="TST")

    def test_loc_id_info_falls_back_to_graph_identity(self) -> None:
        with mock.patch(
            "mapmover.geometry_handlers._get_selection_metadata_for_loc_id",
            side_effect=AssertionError("identity metadata must not hydrate geometry"),
        ):
            info = get_location_info("TST-A-001")
        self.assertEqual(info["name"], "Test Area")
        self.assertEqual(info["family"], "test_sidechain")
        self.assertEqual(info["release_id"], "test_candidate")

    def test_graph_sql_path_uses_cloud_path_translation(self) -> None:
        with mock.patch.object(reference_graph, "path_to_uri", return_value="s3://bucket/published/graph.parquet"):
            self.assertEqual(
                reference_graph._sql_path(self.root / "identities.parquet"),
                "s3://bucket/published/graph.parquet",
            )

    def test_country_identity_stays_in_its_graph(self) -> None:
        country = self.root / "country"
        global_root = self.root / "global"
        with mock.patch.object(reference_graph, "reference_graph_roots", return_value={"TST": country}), \
             mock.patch.object(reference_graph, "global_reference_graph_root", return_value=global_root):
            self.assertEqual(
                reference_graph.graph_roots_for_loc_id("TST-A-001"),
                [country],
            )
            self.assertEqual(
                reference_graph.graph_roots_for_loc_id("ZZZ-G1"),
                [global_root],
            )

    def test_country_identity_does_not_probe_global_graph(self) -> None:
        country = self.root / "country"
        with mock.patch.object(reference_graph, "reference_graph_roots", return_value={"TST": country}), \
             mock.patch.object(reference_graph, "global_reference_graph_root") as global_root:
            self.assertEqual(reference_graph.graph_roots_for_loc_id("TST-A-001"), [country])
            self.assertEqual(reference_graph.active_reference_graph_root(), country)
            global_root.assert_not_called()

    def test_cloud_country_identity_uses_selected_catalog_without_graph_discovery(self) -> None:
        profile = {
            "country_code": "CAN", "release_status": "published",
            "active_release": {"release_id": "can_geometry_1_3_5"},
        }
        expected = (
            self.root / "geometry/countries/CAN/reference/can_geometry_1_3_5"
        ).resolve()
        with (
            mock.patch.object(reference_graph, "DATA_ROOT", self.root),
            mock.patch.object(reference_graph, "is_cloud_mode", return_value=True),
            mock.patch.object(reference_graph, "load_geometry_catalog", return_value={
                "country_profiles": [profile],
            }),
            mock.patch.object(reference_graph, "reference_graph_roots") as discovery,
            mock.patch.object(reference_graph, "global_reference_graph_root") as global_root,
        ):
            self.assertEqual(
                reference_graph.graph_roots_for_loc_id("CAN-ON-3520-HEALTH-3503"),
                [expected],
            )
            discovery.assert_not_called()
            global_root.assert_not_called()

    def test_missing_global_manifest_does_not_probe_graph_parquets(self) -> None:
        with mock.patch.object(reference_graph, "_graph_json", return_value=None), \
             mock.patch.object(reference_graph, "_missing_graph_files") as missing:
            self.assertIsNone(reference_graph.global_reference_graph_root())
            missing.assert_not_called()


if __name__ == "__main__":
    unittest.main()
