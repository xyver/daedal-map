"""Release pin checks for the bounded hosted country routing prewarm."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from mapmover.runtime import geometry_catalog, published_artifacts, reference_prewarm


class CountryRoutingPrewarmTests(unittest.TestCase):
    def test_failed_routing_validation_does_not_stage_new_catalog(self) -> None:
        previous = geometry_catalog._geometry_catalog_prefetch
        try:
            geometry_catalog._geometry_catalog_prefetch = (8, {"country_profiles": [{"country_code": "USA"}]})
            with mock.patch.object(geometry_catalog, "_is_cloud_mode", return_value=True), \
                    mock.patch.object(geometry_catalog, "_fetch_geometry_catalog_from_s3",
                                      return_value={"country_profiles": [{"country_code": "CAN"}]}), \
                    mock.patch.object(geometry_catalog, "_merge_crosswalks", side_effect=lambda payload: payload):
                with self.assertRaisesRegex(RuntimeError, "index unavailable"):
                    geometry_catalog.refresh_geometry_catalog(
                        validate=lambda _: (_ for _ in ()).throw(RuntimeError("index unavailable"))
                    )
            self.assertEqual(geometry_catalog._geometry_catalog_prefetch[0], 8)
        finally:
            geometry_catalog._geometry_catalog_prefetch = previous

    def test_selected_country_indexes_are_hydrated_and_hash_checked(self) -> None:
        country = "CAN"
        release_id = "can_geometry_1_0_0"
        base = f"geometry/countries/{country}"
        exact = f"{base}/admin_spine/exact/{release_id}/"
        graph = f"{base}/reference/{release_id}/"
        discovery = f"{base}/reference_discovery/"
        manifest_path = f"{base}/releases/1.0.0/manifest.json"
        payloads = {
            exact + "loc_id_routes.parquet": b"admin routes",
            graph + "identity_routes.parquet": b"identity routes",
            discovery + "identifier_index.parquet": b"discovery routes",
        }
        digest = lambda value: hashlib.sha256(value).hexdigest()
        manifest = {
            "release_unit": {"id": country},
            "runtime": {"exact": exact, "reference": f"{base}/reference/"},
            "objects": [
                {"source_paths": [path], "sha256": digest(payloads[path])}
                for path in (exact + "loc_id_routes.parquet", graph + "identity_routes.parquet")
            ],
        }
        controls = {
            manifest_path: manifest,
            graph + "manifest.json": {"release_id": "graph_v1"},
            discovery + "manifest.json": {
                "status": "PASS", "artifact": "identifier_index.parquet",
                "source_graphs": {country: {"release_id": "graph_v1"}},
                "sha256": digest(payloads[discovery + "identifier_index.parquet"]),
            },
        }
        manifest_bytes = json.dumps(manifest).encode()
        controls[f"{base}/index.json"] = {
            "release_unit": {"id": country},
            "current": {"release_id": release_id, "version": "1.0.0",
                        "manifest": {"path": manifest_path, "sha256": digest(manifest_bytes)}},
        }
        catalog = {"country_profiles": [{
            "country_code": country, "release_status": "published",
            "release_id": release_id, "release_version": "1.0.0",
        }]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for path, content in payloads.items():
                local = root / path
                local.parent.mkdir(parents=True, exist_ok=True)
                local.write_bytes(content)

            def read(relative, *, lane):
                return json.dumps(controls[relative]).encode()

            def resolve(relative, *, lane, max_bytes):
                return root / relative

            with mock.patch.object(published_artifacts, "artifact_cache_enabled", return_value=True), \
                    mock.patch.object(published_artifacts, "artifact_ref", return_value=SimpleNamespace(prefix="published/")), \
                    mock.patch.object(published_artifacts, "artifact_prefix", return_value="published/"), \
                    mock.patch.object(published_artifacts, "read_artifact_bytes", side_effect=read), \
                    mock.patch.object(published_artifacts, "resolve_artifact_path", side_effect=resolve), \
                    mock.patch.object(published_artifacts, "pin_artifact_paths", return_value=43) as pin, \
                    mock.patch.object(geometry_catalog, "load_geometry_catalog", return_value=catalog):
                result = reference_prewarm.prewarm_country_routing_indexes()
                self.assertEqual(result["countries"], [country])
                self.assertEqual(result["index_bytes"], sum(map(len, payloads.values())))
                self.assertEqual(result["pinned_bytes"], 43)
                self.assertEqual(len(pin.call_args.args[0]), 8)
                self.assertTrue(reference_prewarm.country_routing_selection_is_warm(catalog))

                (root / (graph + "identity_routes.parquet")).write_bytes(b"wrong bytes")
                with self.assertRaisesRegex(ValueError, "hash mismatch"):
                    reference_prewarm.prewarm_country_routing_indexes()
