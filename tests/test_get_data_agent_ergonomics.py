"""get_data behaviours that let a blind agent finish an event query in few calls.

Covers the scope gate (field filters narrow, real row counts admit, errors
state passing shapes), event label fields, time-ordered default sort, the
sparse-metric warning, the country-attribution warning for offshore events,
and the get_catalog word-boundary truncation.
"""
from __future__ import annotations

import asyncio
import json
import unittest
from unittest.mock import patch

from starlette.requests import Request

from mapmover.api_query_runtime import ApiMetricSpec, ApiSourceSpec
from mapmover.api_query_scope import build_query_scope, query_scope_rejection
from mapmover.runtime.source_response_semantics import collect_sparse_metric_warnings


class _Spec:
    pack_id = "volcanoes"
    source_id = "volcanoes_events"
    query_mode = "single_source_events"
    time_field = "year"
    max_limit = 500


def _scope(*, region_ids=(), field_filter_count=0, start=1880, end=1979, limit=100):
    return build_query_scope(
        _Spec(),
        normalized_region_ids=list(region_ids),
        normalized_time={"start": start, "end": end},
        raw_compare_filters=[],
        normalized_sort=[],
        requested_sort_count=0,
        metrics=["VEI"],
        limit=limit,
        output_format="rows",
        field_filter_count=field_filter_count,
    )


class ScopeGateTests(unittest.TestCase):
    def test_exact_field_filter_narrows_like_region_ids(self):
        scope = _scope(field_filter_count=1, start=1880, end=1889)
        self.assertTrue(scope["has_field_filter"])
        self.assertIsNone(query_scope_rejection(scope))

    def test_rejection_states_limits_and_never_reports_score_as_rows(self):
        rejection = query_scope_rejection(_scope(start=1880, end=1889))
        self.assertEqual(rejection["code"], "query_too_broad")
        self.assertEqual(rejection["rule"], "unscoped_time_window")
        self.assertIn("10 years", rejection["message"])
        self.assertIn("5 years", rejection["message"])
        details = rejection["details"]
        self.assertEqual(details["requested"]["time_span_years"], 10)
        self.assertTrue(any("filters.equals" in line for line in details["passing_shapes"]))
        self.assertNotIn("available_rows", details["cap_info"])

    def test_rejection_with_count_reports_matching_rows(self):
        rejection = query_scope_rejection(_scope(start=1880, end=1889, limit=5), matching_rows=45)
        self.assertEqual(rejection["details"]["matching_rows"], 45)
        self.assertEqual(rejection["details"]["cap_info"]["available_rows"], 45)
        self.assertIn("45 matching rows", rejection["message"])


class SparseMetricWarningTests(unittest.TestCase):
    def test_sorting_on_sparse_metric_warns(self):
        warnings = collect_sparse_metric_warnings(
            {"deaths": {"density": 0.009}, "VEI": {"density": 0.76}},
            metric_uses={"deaths": ["sort"], "VEI": ["sort"]},
        )
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["code"], "sparse_metric")
        self.assertEqual([item["metric"] for item in warnings[0]["metrics"]], ["deaths"])

    def test_unused_sparse_metric_is_silent(self):
        self.assertEqual(collect_sparse_metric_warnings({"deaths": {"density": 0.009}}, metric_uses={}), [])


class CatalogTruncationTests(unittest.TestCase):
    def test_truncates_at_word_boundary(self):
        from mapmover.routes.mcp import _truncate_words

        text = "Global volcanic eruption records from the Smithsonian GVP including VEI"
        short = _truncate_words(text, 40)
        self.assertTrue(short.endswith("..."))
        self.assertLessEqual(len(short), 40)
        self.assertTrue(text.startswith(short[:-3]))
        self.assertEqual(text[len(short) - 3], " ")
        self.assertEqual(_truncate_words("short", 40), "short")


class RegionAwareSourceRoutingTests(unittest.TestCase):
    def test_metadata_metric_can_name_an_existing_physical_column(self):
        from mapmover.api_query_runtime import _build_metric_specs_from_metadata

        metrics = _build_metric_specs_from_metadata({"metrics": {
            "msha_mines_record_count": {"column": "record_count", "description": "MSHA mine records"},
        }})
        self.assertEqual(metrics["msha_mines_record_count"].column, "record_count")

    def test_distinct_pack_metrics_select_their_declared_sources(self):
        from mapmover import api_query_runtime as runtime

        metadata = {
            "msha_mines": {"scope": "USA", "metrics": {"msha_mines_record_count": {"column": "record_count"}}},
            "usgs_mrds": {"scope": "global", "metrics": {"usgs_mrds_record_count": {"column": "record_count"}}},
        }
        with patch.object(runtime, "_get_mcp_pack_source_ids", return_value=list(metadata)), \
                patch.object(runtime, "load_source_metadata", side_effect=lambda source_id: metadata[source_id]):
            msha = runtime.resolve_pack_sources_for_metrics("global_resources", ["msha_mines_record_count"])
            mrds = runtime.resolve_pack_sources_for_metrics("global_resources", ["usgs_mrds_record_count"])
        self.assertEqual(msha["selected_source_id"], "msha_mines")
        self.assertEqual(mrds["selected_source_id"], "usgs_mrds")

    def test_home_country_source_wins_and_foreign_source_drops(self):
        from mapmover.api_query_runtime import _region_countries, _source_region_fit

        countries = _region_countries(["USA-CA-007"])
        self.assertEqual(countries, {"USA"})
        self.assertEqual(_source_region_fit({"scope": "usa"}, countries), 1)
        self.assertEqual(_source_region_fit({"scope": "CAN"}, countries), -1)
        self.assertEqual(_source_region_fit({"scope": "global"}, countries), 0)
        self.assertEqual(_source_region_fit({}, countries), 0)
        self.assertEqual(_source_region_fit({"scope": "CAN"}, set()), 0)

    def test_pack_metric_routing_uses_region(self):
        from mapmover import api_query_runtime as runtime

        metadata = {
            "can_wildfires": {"scope": "CAN", "metrics": {"area_km2": {"density": 0.99}}},
            "wildfires_usa": {"scope": "USA", "metrics": {"area_km2": {"density": 0.5}}},
        }
        spec = ApiSourceSpec(
            source_id="x", pack_id="wildfires", parquet_name="x.parquet", query_mode="single_source_events",
            location_field="loc_id", time_field="timestamp", time_granularity="event",
            metrics={"area_km2": ApiMetricSpec(metric_id="area_km2", column="area_km2", description="")},
            filterable_fields={"loc_id"}, sortable_fields={"area_km2"},
        )
        with patch.object(runtime, "_get_mcp_pack_source_ids", return_value=list(metadata)), \
                patch.object(runtime, "load_source_metadata", side_effect=lambda s: metadata[s]), \
                patch.object(runtime, "get_api_source_spec", return_value=spec):
            unscoped = runtime.resolve_pack_sources_for_metrics("wildfires", ["area_km2"])
            scoped = runtime.resolve_pack_sources_for_metrics("wildfires", ["area_km2"], region_ids=["USA-CA-007"])
        self.assertEqual(unscoped["selected_source_id"], "can_wildfires")
        self.assertEqual(scoped["selected_source_id"], "wildfires_usa")


VOLCANO_SPEC = ApiSourceSpec(
    source_id="volcanoes_events",
    pack_id="volcanoes",
    parquet_name="events.parquet",
    query_mode="single_source_events",
    location_field="loc_id",
    time_field="year",
    time_granularity="yearly",
    metrics={
        "VEI": ApiMetricSpec(metric_id="VEI", column="VEI", description="Volcanic Explosivity Index"),
        "deaths": ApiMetricSpec(metric_id="deaths", column="deaths", description="Deaths"),
    },
    filterable_fields={"loc_id", "year", "event_id", "volcano_name", "country", "region", "activity_type", "is_ongoing"},
    sortable_fields={"loc_id", "year", "VEI", "deaths", "volcano_name"},
)
COLUMN_TYPES = {
    "loc_id": "VARCHAR", "event_id": "VARCHAR", "year": "BIGINT", "VEI": "DOUBLE", "deaths": "DOUBLE",
    "volcano_name": "VARCHAR", "country": "VARCHAR", "region": "VARCHAR", "activity_type": "VARCHAR",
    "is_ongoing": "BOOLEAN",
}
METADATA = {"metrics": {"VEI": {"density": 0.757}, "deaths": {"density": 0.009}}}


def _request() -> Request:
    return Request({
        "type": "http", "method": "POST", "path": "/api/v1/query/dataset",
        "headers": [(b"accept", b"application/json")], "client": ("test", 0),
        "server": ("test", 0), "scheme": "http", "query_string": b"",
    })


def _run(payload, query_side_effect):
    from mapmover.routes.api_query import execute_query_dataset_payload

    calls: list[dict] = []

    def fake_query(spec, **kwargs):
        calls.append(kwargs)
        return query_side_effect(kwargs)

    with patch("mapmover.routes.api_query.resolve_pack_source_for_query", return_value=("volcanoes", "volcanoes_events")), \
            patch("mapmover.routes.api_query.get_api_source_spec", return_value=VOLCANO_SPEC), \
            patch("mapmover.routes.api_query.api_source_ready", return_value=True), \
            patch("mapmover.routes.api_query.get_api_source_columns", return_value=list(COLUMN_TYPES)), \
            patch("mapmover.routes.api_query._text_column_types", return_value=COLUMN_TYPES), \
            patch("mapmover.routes.api_query.resolve_effective_time_spec", side_effect=lambda s, _t: s), \
            patch("mapmover.routes.api_query.get_api_source_time_bounds", return_value=(-55500, 2026)), \
            patch("mapmover.routes.api_query.execute_dataset_query", side_effect=fake_query), \
            patch("mapmover.routes.api_query.load_source_metadata", return_value=METADATA), \
            patch("mapmover.routes.api_query.get_country_names_from_codes", return_value=["Indonesia"]), \
            patch("mapmover.routes.api_query.pack_requires_commercial_access", return_value=False), \
            patch("mapmover.routes.api_query.is_local_loopback_request", return_value=True), \
            patch("mapmover.routes.api_query.log_api_query_event"):
        response = asyncio.run(execute_query_dataset_payload(_request(), payload))
    return response, calls


class EventRowShapeTests(unittest.TestCase):
    def test_narrow_long_query_is_admitted_by_row_count_with_labels_and_time_sort(self):
        def side_effect(kwargs):
            if kwargs.get("count_only"):
                return [{"row_count": 3}]
            return [{"loc_id": "IHO1953-260002028", "event_id": "IDN-VOLC-VE015589", "year": 1883,
                     "deaths": None, "VEI": 6.0, "volcano_name": "Krakatau", "country": "Indonesia",
                     "region": "Sunda-Banda", "activity_type": "Confirmed Eruption"}]

        response, calls = _run({
            "source_id": "volcanoes_events",
            "metrics": ["VEI", "deaths"],
            "filters": {"equals": {"volcano_name": "Krakatau"}, "time": {"start": 1800, "end": 1999}},
            "limit": 10,
        }, side_effect)

        self.assertEqual(response.status_code, 200, response.body)
        payload = json.loads(response.body)
        self.assertTrue(any(call.get("count_only") for call in calls))
        self.assertIn("scope_note", payload)
        self.assertEqual(payload["sort"], [{"field": "year", "direction": "asc"}])
        self.assertTrue(payload["sort_defaulted"])
        self.assertEqual(payload["label_fields"][0], "volcano_name")
        self.assertNotIn("is_ongoing", payload["label_fields"])
        self.assertEqual(payload["rows"][0]["volcano_name"], "Krakatau")
        self.assertEqual(payload["rows"][0]["country"], "Indonesia")

    def test_count_over_limit_keeps_rejection_with_matching_rows(self):
        response, _calls = _run({
            "source_id": "volcanoes_events",
            "metrics": ["VEI"],
            "filters": {"time": {"start": 1800, "end": 1999}},
            "limit": 10,
        }, lambda kwargs: [{"row_count": 900}] if kwargs.get("count_only") else [])
        self.assertEqual(response.status_code, 400)
        error = json.loads(response.body)["error"]
        self.assertEqual(error["code"], "query_too_broad")
        self.assertEqual(error["details"]["matching_rows"], 900)

    def test_country_filter_reports_offshore_events_and_sparse_sort(self):
        def side_effect(kwargs):
            if kwargs.get("count_only"):
                return [{"row_count": 1}]
            if "country" in (kwargs.get("in_filters") or {}):
                return [{"loc_id": "IHO1953-260002028"}, {"loc_id": "IDN-G1-G2"}]
            return [{"loc_id": "IDN-G1-G2", "event_id": "IDN-G1-G2-VOLC-VE1", "year": 1883, "deaths": 5.0}]

        response, _calls = _run({
            "source_id": "volcanoes_events",
            "metrics": ["deaths"],
            "filters": {"region_ids": ["IDN"], "time": {"value": 1883}},
            "sort": {"field": "deaths", "direction": "desc"},
            "limit": 10,
        }, side_effect)

        self.assertEqual(response.status_code, 200, response.body)
        payload = json.loads(response.body)
        self.assertFalse(payload["sort_defaulted"])
        codes = {warning["code"]: warning for warning in payload["warnings"]}
        outside = codes["country_events_outside_region_ids"]
        self.assertEqual(outside["count"], 1)
        self.assertEqual(outside["loc_ids"], [{"loc_id": "IHO1953-260002028", "events": 1}])
        self.assertEqual(codes["sparse_metric"]["metrics"][0]["used_for"], ["only_metric", "sort"])


if __name__ == "__main__":
    unittest.main()
