import asyncio
import time
from datetime import date, datetime
from unittest.mock import patch

import msgpack
import pandas as pd
import pytest

from mapmover.geometry_handlers import df_to_geojson
from mapmover.routes import geometry


@pytest.mark.parametrize(
    "endpoint",
    (
        geometry.get_selection_geometry_endpoint,
        geometry.get_geometry_features_endpoint,
        geometry.get_display_geometry_features_endpoint,
    ),
)
def test_geometry_loading_does_not_block_event_loop(endpoint) -> None:
    async def decode_request_body(_request):
        return {"loc_ids": ["USA-VA-600"]}

    def slow_geometry_load(_loc_ids):
        time.sleep(0.2)
        return {"type": "FeatureCollection", "features": []}

    async def run_check() -> None:
        started = time.monotonic()
        task = asyncio.create_task(endpoint(object()))
        await asyncio.sleep(0.02)

        assert time.monotonic() - started < 0.1
        assert not task.done()
        await task

    with (
        patch.object(geometry, "decode_request_body", decode_request_body),
        patch.object(geometry, "get_selection_geometries_handler", slow_geometry_load),
        patch.object(geometry, "get_display_geometries_handler", slow_geometry_load),
    ):
        asyncio.run(run_check())


@pytest.mark.parametrize(
    "endpoint",
    (geometry.get_selection_geometry_endpoint, geometry.get_geometry_features_endpoint),
)
def test_exact_geometry_routes_serialize_validity_dates(endpoint) -> None:
    async def decode_request_body(_request):
        return {"loc_ids": ["USA-VA-600"]}

    frame = pd.DataFrame([{
        "loc_id": "USA-VA-600",
        "geometry": '{"type":"Polygon","coordinates":[]}',
        "observed_at": date(2026, 9, 1),
        "valid_from": datetime(2026, 9, 1, 12, 30),
        "valid_to": pd.NaT,
    }])

    with (
        patch.object(geometry, "decode_request_body", decode_request_body),
        patch.object(geometry, "get_selection_geometries_handler", lambda _ids: df_to_geojson(frame)),
    ):
        response = asyncio.run(endpoint(object()))

    assert response.status_code == 200
    properties = msgpack.unpackb(response.body)["features"][0]["properties"]
    assert properties["observed_at"] == "2026-09-01"
    assert properties["valid_from"] == "2026-09-01T12:30:00"
    assert "valid_to" not in properties
