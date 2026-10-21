from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mapmover.routes.system import (
    _local_wrapper_state_allowed,
    _refresh_runtime_pack_catalog,
    _select_downloadable_pack_current_url,
)


class LocalWrapperAuthStateTests(unittest.TestCase):
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
