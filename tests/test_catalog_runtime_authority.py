from __future__ import annotations

from unittest import mock
from pathlib import Path
import asyncio

import pytest

import mapmover.data_loading as data_loading
from mapmover import api_query_commercial, api_query_runtime
from mapmover.catalog_cache_policy import (
    CONTROL_CATALOG_CACHE_TTL_SECONDS,
    control_catalog_cache_epoch,
)
from mapmover.ops_feed_registry import OPS_FEED_REGISTRY_CACHE_TTL_SECONDS
from mapmover.routes import mcp, system
from mapmover.pack_state import build_active_catalog
from mapmover.runtime import geometry_catalog
from mapmover import control_catalog_prewarm


@pytest.fixture(autouse=True)
def _clear_control_catalog_test_caches():
    yield
    data_loading.clear_api_discovery_cache()
    api_query_runtime.clear_api_source_spec_cache()
    mcp.clear_catalog_derived_mcp_caches()


def _published_pack(pack_id: str, permission: str = "free") -> dict:
    return {
        "pack_id": pack_id,
        "pack_name": pack_id.replace("_", " ").title(),
        "catalog_surfaces": ["api", "mcp"],
        "source_ids": [f"{pack_id}_source"],
        "material_policy": {
            "permission": permission,
            "hosted_access": {
                "maximum_lane": permission,
                "publication_ready": True,
            },
        },
    }


def test_all_runtime_control_catalogs_share_one_ttl() -> None:
    assert data_loading._CATALOG_TTL_SECONDS == CONTROL_CATALOG_CACHE_TTL_SECONDS
    assert data_loading._API_CATALOG_TTL_SECONDS == CONTROL_CATALOG_CACHE_TTL_SECONDS
    assert geometry_catalog.control_catalog_cache_epoch is control_catalog_cache_epoch
    assert OPS_FEED_REGISTRY_CACHE_TTL_SECONDS == CONTROL_CATALOG_CACHE_TTL_SECONDS
    assert system._PUBLIC_PACK_CATALOG_TTL_SECONDS == CONTROL_CATALOG_CACHE_TTL_SECONDS


def test_api_catalog_build_does_not_fetch_explore_overlay_metadata() -> None:
    catalog = {"sources": [{
        "source_id": "earthquakes_events", "pack_id": "earthquakes",
        "catalog_surfaces": ["api", "explore"],
        "overlay": {"path": "disasters/earthquakes"},
    }], "packs": []}
    state = {"catalog_mode": "unmanaged_data_root", "installed_packs": []}
    with (
        mock.patch("mapmover.pack_state.load_pack_state", return_value=state),
        mock.patch.object(data_loading, "load_source_metadata", return_value={}) as metadata,
    ):
        api_catalog = build_active_catalog(catalog, catalog_surface="api")
        assert api_catalog["sources"][0]["source_id"] == "earthquakes_events"
        assert api_catalog["overlay_tree"] == {}
        metadata.assert_not_called()
        explore_catalog = build_active_catalog(catalog, catalog_surface="explore")
        assert explore_catalog["overlay_tree"]
        metadata.assert_called_once_with("earthquakes_events")


def test_local_folder_remains_visible_with_managed_packs(tmp_path: Path) -> None:
    import mapmover.pack_state as pack_state

    folder = tmp_path / "my_tables"
    folder.mkdir()
    catalog = {"sources": [{"source_id": "my_tables", "pack_id": "local_my_tables",
                            "path": "my_tables", "local_folder": True,
                            "catalog_surfaces": ["research"]}],
               "packs": [{"pack_id": "local_my_tables", "source_ids": ["my_tables"]}]}
    state = {"catalog_mode": "managed_packs", "installed_packs": [], "active_pack_ids": []}
    with (mock.patch.object(pack_state, "DATA_ROOT", tmp_path),
          mock.patch.object(pack_state, "RUNTIME_MODE", "local")):
        result = build_active_catalog(catalog, state, catalog_surface="research")
    assert [source["source_id"] for source in result["sources"]] == ["my_tables"]
    assert result["sources"][0]["runtime_data_root"] == str(tmp_path)
    state["installed_packs"] = [{"pack_id": "local_my_tables", "data_root": "C:/old/pack"}]
    with (mock.patch.object(pack_state, "DATA_ROOT", tmp_path),
          mock.patch.object(pack_state, "RUNTIME_MODE", "local")):
        assert build_active_catalog(catalog, state, catalog_surface="research")["sources"][0]["runtime_data_root"] == str(tmp_path)
    state["installed_packs"] = []
    with (mock.patch.object(pack_state, "DATA_ROOT", tmp_path),
          mock.patch.object(pack_state, "RUNTIME_MODE", "cloud")):
        assert build_active_catalog(catalog, state, catalog_surface="research")["sources"] == []


def test_normal_catalog_owns_agent_pack_admission_and_current_facts() -> None:
    published = {
        "packs": [
            {**_published_pack("new_pack", "paid"), "description": "current"},
        ],
        "sources": [],
    }
    generated = {
        "catalog_version": "1.0",
        "packs": [
            {"pack_id": "retired_pack", "description": "stale"},
            {"pack_id": "new_pack", "description": "old", "examples": ["kept"]},
        ],
    }
    data_loading.clear_api_discovery_cache()
    with (
        mock.patch.object(data_loading, "load_catalog", return_value=published),
        mock.patch.object(data_loading, "_load_json_from_runtime_or_s3", return_value=generated),
        mock.patch.object(data_loading, "_hydrate_api_catalog_payload", side_effect=lambda payload: payload),
    ):
        result = data_loading.load_api_catalog()

    assert [pack["pack_id"] for pack in result["packs"]] == ["new_pack"]
    assert result["packs"][0]["description"] == "current"
    assert result["packs"][0]["examples"] == ["kept"]
    assert result["packs"][0]["material_policy"]["permission"] == "paid"


def test_full_catalog_metric_ids_come_from_published_sources() -> None:
    published = {
        "packs": [_published_pack("new_pack")],
        "sources": [
            {"source_id": "new_pack_source", "pack_id": "new_pack", "data_type": "events", "metrics": {"magnitude": {}}},
            {"source_id": "retired_source", "pack_id": "retired_pack", "metrics": {"stale": {}}},
        ],
    }
    with (
        mock.patch.object(data_loading, "load_catalog", return_value=published),
        mock.patch.object(data_loading, "_load_json_from_runtime_or_s3", return_value={"packs": []}),
    ):
        result = data_loading.load_api_catalog()

    assert result["packs"][0]["metric_ids"] == ["event_count", "magnitude"]
    assert len(result["packs"]) == 1


def test_lite_pack_load_skips_source_metadata_and_has_separate_cache() -> None:
    published = {
        "packs": [_published_pack("new_pack")],
        "sources": [{"source_id": "new_pack_source", "pack_id": "new_pack", "temporal_coverage": {"end": "2025"}}],
    }
    with (
        mock.patch.object(data_loading, "load_catalog", return_value=published),
        mock.patch.object(data_loading, "_load_json_from_runtime_or_s3", return_value=None),
        mock.patch.object(data_loading, "load_source_metadata", return_value={"temporal_coverage": {"end": "2026"}}) as metadata,
    ):
        lite = data_loading.load_api_pack_detail("new_pack", hydrate_source_metadata=False)
        metadata.assert_not_called()
        full = data_loading.load_api_pack_detail("new_pack")
        metadata.assert_called_once_with("new_pack_source")

    assert lite["sources"][0]["temporal_coverage"]["end"] == "2025"
    assert full["sources"][0]["temporal_coverage"]["end"] == "2026"


def test_forced_pack_refresh_keeps_lite_payload_and_accepts_missing_generated_detail() -> None:
    published = {
        "packs": [_published_pack("new_pack")],
        "sources": [{"source_id": "new_pack_source", "pack_id": "new_pack", "temporal_coverage": {"end": "2025"}}],
    }
    with (
        mock.patch.object(data_loading, "load_catalog", return_value=published),
        mock.patch.object(data_loading, "_load_json_from_runtime_or_s3", return_value=None),
        mock.patch.object(data_loading, "load_source_metadata", return_value={"temporal_coverage": {"end": "2026"}}) as metadata,
    ):
        full = data_loading.load_api_pack_detail("new_pack", force_refresh=True)
        lite = data_loading.load_api_pack_detail("new_pack", hydrate_source_metadata=False)
        metadata.assert_called_once_with("new_pack_source")

    assert full["sources"][0]["temporal_coverage"]["end"] == "2026"
    assert lite["sources"][0]["temporal_coverage"]["end"] == "2025"


def test_agent_catalog_prewarm_reports_missing_required_metadata() -> None:
    with (
        mock.patch.object(data_loading, "load_api_catalog", return_value={"packs": [{"pack_id": "demo"}]}),
        mock.patch.object(data_loading, "load_api_guide", return_value={}),
    ):
        with pytest.raises(RuntimeError, match="no guide"):
            data_loading.prewarm_api_catalog()


def test_forced_published_refresh_keeps_old_cache_on_fetch_failure() -> None:
    old = {"sources": [{"source_id": "existing"}], "packs": []}
    with (
        mock.patch.object(data_loading, "_catalog_cache", old),
        mock.patch.object(data_loading, "_catalog_cache_time", 1.0e12),
        mock.patch.object(data_loading, "get_data_plane_mode", return_value="cloud"),
        mock.patch.object(data_loading, "_fetch_json_from_s3", side_effect=OSError("R2 unavailable")),
    ):
        with pytest.raises(RuntimeError, match="refresh failed"):
            data_loading.load_catalog(force_refresh=True)
        assert data_loading._catalog_cache is old


def test_app_refresh_uses_force_reads_without_clearing_serving_caches() -> None:
    with (
        mock.patch.object(control_catalog_prewarm, "load_catalog", return_value={"sources": [{}]}) as published,
        mock.patch.object(control_catalog_prewarm, "load_full_catalog", return_value={"sources": [{}]}) as wip,
        mock.patch.object(control_catalog_prewarm, "refresh_geometry_catalog", return_value={"geometry_banks": [{}]}) as geometry,
        mock.patch.object(control_catalog_prewarm, "load_ops_feed_records", return_value=[{}]) as feeds,
        mock.patch.object(control_catalog_prewarm, "load_api_catalog", return_value={"packs": [{"pack_id": "demo"}]}) as agent,
        mock.patch.object(control_catalog_prewarm, "load_api_guide", return_value={"title": "Guide"}) as guide,
        mock.patch.object(control_catalog_prewarm, "load_api_pack_detail", return_value={"pack_id": "demo"}) as detail,
    ):
        counts = control_catalog_prewarm.refresh_app_catalogs()
    assert counts == {"published": 1, "wip": 1, "geometry": 1, "ops_feeds": 1, "agent_packs": 1}
    published.assert_called_once_with(force_refresh=True)
    wip.assert_called_once_with(force_refresh=True)
    geometry.assert_called_once_with()
    feeds.assert_called_once_with(force_refresh=True)
    agent.assert_called_once_with(force_refresh=True)
    guide.assert_called_once_with(force_refresh=True)
    detail.assert_called_once_with("demo", force_refresh=True)


def test_geometry_prefetch_is_visible_only_in_next_epoch() -> None:
    geometry_catalog.clear_geometry_catalog_cache()
    current = {"geometry_banks": [{"bank_id": "old"}]}
    upcoming = {"geometry_banks": [{"bank_id": "new"}]}
    with (
        mock.patch.object(geometry_catalog, "_catalog_cache_epoch", return_value=10) as epoch,
        mock.patch.object(geometry_catalog, "_load_geometry_catalog_cached", return_value=current),
        mock.patch.object(geometry_catalog, "_is_cloud_mode", return_value=True),
        mock.patch.object(geometry_catalog, "_fetch_geometry_catalog_from_s3", return_value=upcoming),
    ):
        geometry_catalog.refresh_geometry_catalog()
        assert geometry_catalog.load_geometry_catalog() is current
        epoch.return_value = 11
        assert geometry_catalog.load_geometry_catalog()["geometry_banks"][0]["bank_id"] == "new"
    geometry_catalog.clear_geometry_catalog_cache()


def test_readiness_reports_stale_app_catalog_generation() -> None:
    with (
        mock.patch.object(system, "prewarm_readiness", return_value={"state": "ready"}),
        mock.patch.object(control_catalog_prewarm, "app_catalog_refresh_status", return_value={"state": "error", "age_seconds": 301}),
    ):
        response = asyncio.run(system.readiness_check())
    assert response.status_code == 503


def test_readiness_waits_for_catalog_refresh_monitor() -> None:
    with (
        mock.patch.object(system, "prewarm_readiness", return_value={"state": "ready"}),
        mock.patch.object(control_catalog_prewarm, "app_catalog_refresh_status", return_value={"state": "not_started", "age_seconds": None}),
    ):
        response = asyncio.run(system.readiness_check())
    assert response.status_code == 503


def test_new_catalog_pack_detail_does_not_require_generated_pack_file() -> None:
    pack = _published_pack("new_pack")
    published = {
        "packs": [pack],
        "sources": [{"source_id": "new_pack_source", "description": "catalog source"}],
    }
    data_loading.clear_api_discovery_cache()
    with (
        mock.patch.object(data_loading, "load_catalog", return_value=published),
        mock.patch.object(data_loading, "_load_json_from_runtime_or_s3", return_value=None),
        mock.patch.object(data_loading, "_hydrate_api_pack_detail_from_source_metadata", side_effect=lambda payload: payload),
    ):
        result = data_loading.load_api_pack_detail("new_pack")

    assert result is not None
    assert result["pack_id"] == "new_pack"
    assert result["sources"][0]["source_id"] == "new_pack_source"


def test_normal_catalog_owns_agent_pack_source_membership() -> None:
    pack = _published_pack("new_pack")
    published = {
        "packs": [pack],
        "sources": [{"source_id": "new_pack_source", "description": "current"}],
    }
    generated = {
        "pack_id": "new_pack",
        "sources": [
            {"source_id": "new_pack_source", "description": "stale", "example": "kept"},
            {"source_id": "retired_source", "description": "must disappear"},
        ],
    }
    data_loading.clear_api_discovery_cache()
    with (
        mock.patch.object(data_loading, "load_catalog", return_value=published),
        mock.patch.object(data_loading, "_load_json_from_runtime_or_s3", return_value=generated),
        mock.patch.object(data_loading, "_hydrate_api_pack_detail_from_source_metadata", side_effect=lambda payload: payload),
    ):
        result = data_loading.load_api_pack_detail("new_pack")

    assert result is not None
    assert result["sources"] == [{
        "source_id": "new_pack_source",
        "description": "current",
        "example": "kept",
    }]


def test_normal_catalog_still_drives_discovery_when_agent_catalog_is_unavailable() -> None:
    published = {"packs": [_published_pack("new_pack")], "sources": []}
    data_loading.clear_api_discovery_cache()
    with (
        mock.patch.object(data_loading, "load_catalog", return_value=published),
        mock.patch.object(data_loading, "_load_json_from_runtime_or_s3", return_value=None),
        mock.patch.object(data_loading, "_hydrate_api_catalog_payload", side_effect=lambda payload: payload),
    ):
        result = data_loading.load_api_catalog()

    assert [pack["pack_id"] for pack in result["packs"]] == ["new_pack"]
    assert result["source_mode"] == "catalog.json+agent_catalog"


def test_pack_access_reads_material_policy_from_normal_catalog() -> None:
    published = {"packs": [_published_pack("paid_pack", "paid")], "sources": []}
    with (
        mock.patch.object(data_loading, "load_catalog", return_value=published),
        mock.patch.object(api_query_commercial, "pack_profile", return_value={"pricing": "paid_x402_base_usdc"}),
        mock.patch.object(api_query_commercial, "resolve_effective_access", side_effect=lambda **kwargs: kwargs),
    ):
        result = api_query_commercial.pack_effective_access("paid_pack")

    assert result["authored_pricing"] == "paid_x402_base_usdc"
    assert result["license_permissions"] == {"paid"}
    assert result["publication_cleared"] is True


def test_pack_access_keeps_free_registry_pricing_with_paid_eligible_material() -> None:
    published = {"packs": [_published_pack("free_pack", "paid")], "sources": []}
    with (
        mock.patch.object(data_loading, "load_catalog", return_value=published),
        mock.patch.object(api_query_commercial, "pack_profile", return_value={"pricing": "free"}),
        mock.patch.object(api_query_commercial, "resolve_effective_access", side_effect=lambda **kwargs: kwargs),
    ):
        result = api_query_commercial.pack_effective_access("free_pack")

    assert result["authored_pricing"] == "free"
    assert result["license_permissions"] == {"paid"}


@pytest.mark.parametrize(
    ("pricing", "expected"),
    [
        ("free", False),
        ("paid_x402_base_usdc", True),
        ("mixed", False),
    ],
)
def test_discovery_pack_pricing_is_one_registry_switch(pricing: str, expected: bool) -> None:
    with mock.patch.object(data_loading, "pack_profile", return_value={"pricing": pricing}):
        assert data_loading._pack_is_paid("toggle_pack") is expected


def test_mcp_pack_pricing_lists_come_from_registry_not_material_catalog() -> None:
    with (
        mock.patch("pack_registry_shared.free_pack_ids", return_value=("free_pack",)),
        mock.patch("pack_registry_shared.paid_pack_ids", return_value=("paid_pack",)),
        mock.patch.object(mcp, "load_api_catalog", return_value={"packs": []}),
    ):
        assert mcp._free_pack_ids() == frozenset({"free_pack"})
        assert mcp._paid_pack_ids() == frozenset({"paid_pack"})


def test_mcp_catalog_refresh_helper_clears_derived_views() -> None:
    with mock.patch.object(mcp, "load_api_catalog", return_value={"packs": []}):
        mcp._facade_tools(None)
    with mock.patch.object(api_query_runtime, "_build_dynamic_source_spec", return_value=None):
        api_query_runtime._get_dynamic_api_source_spec("test_source", 0)
    mcp._tool_definitions()
    assert mcp._facade_tools_cached.cache_info().currsize > 0
    assert mcp._tool_definitions_cached.cache_info().currsize > 0
    assert api_query_runtime._get_dynamic_api_source_spec.cache_info().currsize > 0

    mcp.clear_catalog_derived_mcp_caches()

    assert mcp._facade_tools_cached.cache_info().currsize == 0
    assert mcp._tool_definitions_cached.cache_info().currsize == 0
    assert api_query_runtime._get_dynamic_api_source_spec.cache_info().currsize == 0


def test_api_published_source_does_not_require_python_registry_entry() -> None:
    source = {
        "source_id": "future_source",
        "pack_id": "future_pack",
        "catalog_surfaces": ["api", "mcp"],
        "runtime_primary_file": "yearly.parquet",
        "location_field": "loc_id",
        "temporal_coverage": {"field": "year", "granularity": "yearly"},
        "metrics": {"value": {"name": "Value"}},
    }
    api_query_runtime.clear_api_source_spec_cache()
    with (
        mock.patch.object(api_query_runtime, "load_catalog", return_value={"sources": [source]}),
        mock.patch.object(api_query_runtime, "load_source_metadata", return_value=source),
        mock.patch.object(api_query_runtime, "get_source_path", return_value=Path("missing/future_source")),
        mock.patch.object(api_query_runtime, "parquet_available", return_value=True),
        mock.patch.object(api_query_runtime, "parquet_columns", return_value={"loc_id", "year", "value"}),
    ):
        result = api_query_runtime.get_api_source_spec("future_source")

    assert result is not None
    assert result.pack_id == "future_pack"
    assert result.parquet_name == "yearly.parquet"
    assert result.query_mode == "single_source"
    assert result.time_field == "year"


def test_published_aggregate_primary_file_does_not_repeat_admin2_folder() -> None:
    source = {
        "source_id": "earthquake_aggregates",
        "pack_id": "earthquakes",
        "path": "global/disasters/earthquakes/sources/aggregates",
        "catalog_surfaces": ["api", "mcp"],
        "runtime_primary_file": "admin2/yearly.parquet",
        "location_field": "loc_id",
        "temporal_coverage": {"field": "year", "granularity": "yearly"},
        "metrics": {"event_count": {"name": "Event count"}},
    }
    source_path = Path("published/global/disasters/earthquakes/sources/aggregates")
    column_paths = []

    def columns(path):
        column_paths.append(path)
        return {"loc_id", "year", "event_count"}

    api_query_runtime.clear_api_source_spec_cache()
    with (
        mock.patch.object(api_query_runtime, "load_catalog", return_value={"sources": [source]}),
        mock.patch.object(api_query_runtime, "load_source_metadata", return_value=source),
        mock.patch.object(api_query_runtime, "get_source_path", return_value=source_path),
        mock.patch.object(api_query_runtime, "parquet_available", return_value=True),
        mock.patch.object(api_query_runtime, "parquet_columns", side_effect=columns),
    ):
        spec = api_query_runtime.get_api_source_spec("earthquake_aggregates")

    assert spec is not None
    assert column_paths == [source_path / "admin2/yearly.parquet"]


def test_python_source_specializer_cannot_override_catalog_api_admission() -> None:
    api_query_runtime.clear_api_source_spec_cache()
    with mock.patch.object(api_query_runtime, "load_catalog", return_value={"sources": []}):
        assert api_query_runtime.get_api_source_spec("fx_usd_historical") is None


def test_api_source_lookup_requests_api_catalog_surface() -> None:
    from mapmover.catalog_surface import get_catalog_surface_override

    source = {"source_id": "01", "pack_id": "un_sdg", "catalog_surfaces": ["api"]}
    seen = []

    def scoped_catalog():
        surface = get_catalog_surface_override()
        seen.append(surface)
        return {"sources": [source] if surface == "api" else []}

    with mock.patch.object(api_query_runtime, "load_catalog", side_effect=scoped_catalog):
        assert api_query_runtime._catalog_api_source("01") == source
    assert seen == ["api"]


def test_api_source_access_reads_api_catalog_surface() -> None:
    from mapmover.catalog_surface import get_catalog_surface_override
    from mapmover.routes import api_query

    source = {"source_id": "01", "pack_id": "un_sdg", "material_policy": {}}
    seen = []

    def scoped_catalog():
        surface = get_catalog_surface_override()
        seen.append(surface)
        return {"sources": [source] if surface == "api" else []}

    with mock.patch.object(api_query, "load_catalog", side_effect=scoped_catalog):
        assert api_query._source_material_record("01") == source
    assert seen == ["api"]


def test_api_pack_access_reads_api_catalog_surface() -> None:
    from mapmover import api_query_commercial, data_loading
    from mapmover.catalog_surface import get_catalog_surface_override

    pack = {"pack_id": "un_sdg", "material_policy": {
        "permission": "free", "hosted_access": {"publication_ready": True, "maximum_lane": "free"}}}
    seen = []

    def scoped_catalog():
        surface = get_catalog_surface_override()
        seen.append(surface)
        return {"packs": [pack] if surface == "api" else []}

    with mock.patch.object(data_loading, "load_catalog", side_effect=scoped_catalog):
        access = api_query_commercial.pack_effective_access("un_sdg")
    assert seen == ["api"]
    assert access["allow"] is True
