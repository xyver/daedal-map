from __future__ import annotations

from types import SimpleNamespace
import asyncio
import json
import unittest
from unittest.mock import patch

from mapmover.routes.system import (
    _local_wrapper_state_allowed,
    _refresh_runtime_pack_catalog,
    _select_downloadable_pack_current_url,
    _storefront_pack_entries,
    _render_app_shell,
    get_local_map_tiles_config,
    get_local_map_detail_file,
    serve_settings_page,
)


class LocalWrapperAuthStateTests(unittest.TestCase):
    def test_offline_admin0_supplies_local_display_without_geometry_pack(self) -> None:
        from mapmover import geometry_handlers

        with patch.dict("os.environ", {"DAEDALMAP_ACCESS_MODE": "local_wrapper"}), \
                patch.object(geometry_handlers, "load_global_country_display_frame", return_value=None):
            index = geometry_handlers.get_geometry_index(admin_level=0)
            self.assertGreater(index["count"], 200)
            country = index["rows"][0]
            display = geometry_handlers.get_display_geometries([country["loc_id"]])
            self.assertEqual(display["features"][0]["properties"]["loc_id"], country["loc_id"])
            self.assertEqual(geometry_handlers.get_countries_geometry()["count"], index["count"])
            local_index = geometry_handlers.get_geometry_index(admin_level=0, bbox=(-125, 30, -65, 50))
            self.assertGreater(local_index["count"], 0)
            self.assertLess(local_index["count"], index["count"])

    def test_local_map_detail_requires_both_files_and_stays_loopback_only(self) -> None:
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = root / "map_tiles.json"
            config.write_text(json.dumps({"mode": "offline_detail"}), encoding="utf-8")
            request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))
            env = {"DAEDALMAP_MAP_TILES_CONFIG": str(config), "DAEDALMAP_MAP_TILES_ROOT": str(root)}
            with patch.dict("os.environ", env), patch("mapmover.paths.INSTALL_MODE", "local"), \
                    patch("mapmover.paths.RUNTIME_MODE", "local"):
                self.assertEqual(asyncio.run(get_local_map_tiles_config(request))["mode"], "offline_basic")
                for name in ("ne_10m_land.geojson", "ne_10m_admin_0_countries.geojson"):
                    (root / name).write_text("{}", encoding="utf-8")
                self.assertEqual(asyncio.run(get_local_map_tiles_config(request))["mode"], "offline_detail")
                config.write_text(json.dumps({"mode": "remote", "remote_style_url": "https://example.test/style.json"}), encoding="utf-8")
                online = asyncio.run(get_local_map_tiles_config(request))
                self.assertEqual(online["mode"], "online")
                self.assertEqual(online["remote_style_url"], "https://tiles.openfreemap.org/styles/dark")
                response = asyncio.run(get_local_map_detail_file(request, "ne_10m_land.geojson"))
                self.assertEqual(response.path, root / "ne_10m_land.geojson")
                remote = SimpleNamespace(client=SimpleNamespace(host="10.0.0.5"))
                self.assertEqual(asyncio.run(get_local_map_detail_file(remote, "ne_10m_land.geojson")).status_code, 404)

    def test_wrapper_shell_marks_local_only_map_interactions(self) -> None:
        with patch.dict("os.environ", {"DAEDALMAP_ACCESS_MODE": "local_wrapper"}):
            shell = _render_app_shell()
            self.assertIn('<body data-local-wrapper="true">', shell)
            self.assertIn('window.DAEDALMAP_LOCAL_WRAPPER = true;', shell)
            self.assertNotIn('<link rel="preconnect" href="https://unpkg.com"', shell)
            self.assertNotIn('<script src="/static/js/dm-visitor.js"></script>', shell)
            self.assertIn('data-tutorial-title="Explore Map"', shell)
            self.assertNotIn('data-tutorial-title="Account and Session"', shell)

    def test_wrapper_settings_hands_requested_tab_to_launcher(self) -> None:
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as folder, \
                patch.dict("os.environ", {"DAEDALMAP_ACCESS_MODE": "local_wrapper"}), \
                patch("mapmover.paths.STATE_DIR", Path(folder)), \
                patch("mapmover.runtime_config.get_runtime_config", return_value={"mode": "local"}):
            request = SimpleNamespace(query_params={"tab": "library"})
            response = asyncio.run(serve_settings_page(request))
            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Open Data in DaedalMap Launcher", response.body)
            self.assertEqual(json.loads((Path(folder) / "launcher_navigation.json").read_text()),
                             {"tab": "library"})

    def test_local_library_projects_same_storefront_pack_as_launcher(self) -> None:
        manifest = "https://downloads.daedalmap.com/downloadable/packs/un_wpp/un_wpp-lite/stable/20261021/manifest.json"
        catalog = {"categories": {"data": [{
            "packageId": "un_wpp-lite", "manifestUrl": manifest, "version": "20261021",
            "title": "World Population", "description": "Population data", "tier": "lite",
            "size": 12345, "badges": ["Lite"],
        }]}}
        packs = _storefront_pack_entries(catalog)
        self.assertEqual(len(packs), 1)
        self.assertEqual(packs[0]["pack_id"], "un_wpp")
        self.assertEqual(packs[0]["current_manifest_url"],
                         "https://downloads.daedalmap.com/downloadable/packs/un_wpp/un_wpp-lite/stable/current.json")
        self.assertEqual(packs[0]["size_bytes"], 12345)
        self.assertEqual(packs[0]["source_name"], "World Population")

    def test_local_loopback_runtime_accepts_browser_auth_sync(self) -> None:
        request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))
        with patch("mapmover.paths.INSTALL_MODE", "local"), patch("mapmover.paths.RUNTIME_MODE", "local"):
            self.assertTrue(_local_wrapper_state_allowed(request))

    def test_non_loopback_request_cannot_read_or_write_auth_sync(self) -> None:
        request = SimpleNamespace(client=SimpleNamespace(host="10.0.0.5"))
        with patch("mapmover.paths.INSTALL_MODE", "local"), patch("mapmover.paths.RUNTIME_MODE", "local"):
            self.assertFalse(_local_wrapper_state_allowed(request))

    def test_install_selects_advertised_package_edition(self) -> None:
        base = "https://downloads.daedalmap.com"
        index = {"packs": [
            {"pack_id": "un_wpp", "package_id": "un_wpp-lite",
             "current_manifest_url": f"{base}/downloadable/packs/un_wpp/un_wpp-lite/stable/current.json"},
            {"pack_id": "un_wpp", "package_id": "un_wpp-detail",
             "current_manifest_url": f"{base}/downloadable/packs/un_wpp/un_wpp-detail/stable/current.json"},
        ]}
        self.assertEqual(
            _select_downloadable_pack_current_url(index, "un_wpp", "", base),
            index["packs"][0]["current_manifest_url"],
        )
        self.assertEqual(
            _select_downloadable_pack_current_url(index, "un_wpp", "un_wpp-detail", base),
            index["packs"][1]["current_manifest_url"],
        )
        with self.assertRaisesRegex(ValueError, "not in the downloadable catalog"):
            _select_downloadable_pack_current_url(index, "un_wpp", "un_wpp-missing", base)
        index["packs"][0]["current_manifest_url"] = "https://elsewhere.example/current.json"
        with self.assertRaisesRegex(ValueError, "does not match the download origin"):
            _select_downloadable_pack_current_url(index, "un_wpp", "un_wpp-lite", base)

    def test_catalog_refresh_invalidates_old_catalog_before_reloading(self) -> None:
        calls = []
        with (
            patch("mapmover.data_loading.clear_catalog_cache", side_effect=lambda: calls.append("catalog")),
            patch("mapmover.routes.system.clear_metadata_cache", side_effect=lambda: calls.append("metadata")),
            patch("mapmover.routes.system.clear_public_pack_catalog_cache", side_effect=lambda: calls.append("public")),
            patch("mapmover.routes.system.initialize_catalog", side_effect=lambda: calls.append("reload")),
        ):
            _refresh_runtime_pack_catalog()
        self.assertEqual(calls, ["catalog", "metadata", "public", "reload"])


if __name__ == "__main__":
    unittest.main()
