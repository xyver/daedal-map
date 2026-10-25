import unittest
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from mapmover import data_loading


class SourceMetadataFallbackTests(unittest.TestCase):
    def test_local_catalog_only_loads_overlay_metadata_without_recursing(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            source_dir = root / "global" / "sample"
            source_dir.mkdir(parents=True)
            (source_dir / "metadata.json").write_text(
                json.dumps({"source_id": "sample", "data_version": "1.0"}), encoding="utf-8"
            )
            catalog_path = root / "catalog.json"
            catalog_path.write_text(
                json.dumps({"sources": [{"source_id": "sample", "pack_id": "sample_pack",
                                       "path": "global/sample", "overlay": "Tests/Sample"}],
                            "packs": [{"pack_id": "sample_pack"}]}),
                encoding="utf-8",
            )
            with patch("mapmover.data_loading.DATA_ROOT", root), patch(
                "mapmover.data_loading.get_data_plane_mode", return_value="local"
            ), patch("mapmover.data_loading._use_wip_catalog_for_local_runtime", return_value=False), patch(
                "mapmover.data_loading.get_catalog_path", return_value=catalog_path
            ), patch("mapmover.data_loading._catalog_cache", None), patch(
                "mapmover.data_loading._full_catalog_cache", None
            ), patch("mapmover.data_loading._metadata_cache", {}), patch(
                "mapmover.data_loading.load_full_catalog", side_effect=AssertionError("recursive full catalog load")
            ), patch("mapmover.pack_state.load_pack_state", return_value={
                "catalog_mode": "unmanaged_data_root", "installed_packs": []
            }):
                catalog = data_loading.load_catalog(force_refresh=True)
            self.assertEqual(catalog["total_sources"], 1)
            self.assertIn("Tests", catalog["overlay_tree"])

    def test_cloud_metadata_path_uses_active_catalog_before_wip_catalog(self):
        source_contract = {
            "source_id": "example_monthly_source",
            "path": "global/climate/example_monthly_source",
        }
        data_loading._metadata_cache.pop("example_monthly_source", None)

        with patch("mapmover.data_loading.get_data_plane_mode", return_value="cloud"), patch(
            "mapmover.data_loading._catalog_cache", {"sources": [source_contract]}
        ), patch(
            "mapmover.data_loading.load_catalog", side_effect=AssertionError("metadata lookup must not build active catalog")
        ), patch("mapmover.data_loading.load_full_catalog") as load_full_catalog, patch(
            "mapmover.data_loading._fetch_json_from_s3", return_value={"source_id": "example_monthly_source"}
        ) as fetch_json:
            result = data_loading.load_source_metadata("example_monthly_source")

        self.assertEqual(result["source_id"], "example_monthly_source")
        fetch_json.assert_called_once_with("global/climate/example_monthly_source/metadata.json")
        load_full_catalog.assert_not_called()
        data_loading._metadata_cache.pop("example_monthly_source", None)

    def test_cloud_metadata_failure_uses_embedded_catalog_contract(self):
        source_contract = {
            "source_id": "example_monthly_source",
            "path": "global/climate/example_monthly_source",
            "metrics": {"temperature_c": {"name": "Temperature"}},
            "routing_hints": {"query_aliases": ["example temperature"]},
        }
        data_loading._metadata_cache.pop("example_monthly_source", None)

        with patch("mapmover.data_loading.get_data_plane_mode", return_value="cloud"), patch(
            "mapmover.data_loading._catalog_cache", {"sources": [source_contract]}
        ), patch(
            "mapmover.data_loading.load_full_catalog", return_value={"sources": []}
        ), patch(
            "mapmover.data_loading._fetch_json_from_s3", side_effect=OSError("temporary R2 failure")
        ), patch("mapmover.data_loading.get_source_path"), patch(
            "mapmover.data_loading._allow_local_source_fallback", return_value=False
        ):
            result = data_loading.load_source_metadata("example_monthly_source")

        self.assertEqual(result["source_id"], "example_monthly_source")
        self.assertEqual(result["metrics"]["temperature_c"]["name"], "Temperature")
        self.assertEqual(result["routing_hints"]["query_aliases"], ["example temperature"])
        data_loading._metadata_cache.pop("example_monthly_source", None)


if __name__ == "__main__":
    unittest.main()
