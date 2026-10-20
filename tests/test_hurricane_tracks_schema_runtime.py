import asyncio
import unittest
from unittest.mock import patch

import msgpack
import pandas as pd

from mapmover.routes.disasters import hurricanes


class HurricaneTracksSchemaRuntimeTests(unittest.TestCase):
    def test_preload_range_serves_published_storms_with_or_without_display_fields(self):
        base_storm = {
            "storm_id": "storm-1",
            "name": "Example",
            "year": 2024,
            "basin": "NA",
            "max_wind_kt": 80,
            "min_pressure_mb": 960,
            "max_category": "Cat1",
            "num_positions": 2,
            "start_date": "2024-08-01",
            "end_date": "2024-08-03",
            "made_landfall": False,
        }
        positions = pd.DataFrame([
            {"storm_id": "storm-1", "timestamp": "2024-08-01", "latitude": 20.0, "longitude": -60.0},
            {"storm_id": "storm-1", "timestamp": "2024-08-02", "latitude": 21.0, "longitude": -61.0},
        ])

        for prepared_lifecycle in (False, True):
            with self.subTest(prepared_lifecycle=prepared_lifecycle):
                storm = dict(base_storm)
                if prepared_lifecycle:
                    storm.update({
                        "display_start_timestamp": "2024-08-01T00:00:00Z",
                        "display_end_timestamp": "2024-08-03T00:00:00Z",
                        "display_animation_kind": "progressive_track",
                    })
                storms = pd.DataFrame([storm])

                def select_rows(path, **_kwargs):
                    return storms.copy() if path.name == "storms.parquet" else positions.copy()

                with (
                    patch.object(hurricanes, "parquet_available", return_value=True),
                    patch.object(hurricanes, "duckdb_available", return_value=True),
                    patch.object(hurricanes, "is_default_preload_range", return_value=True),
                    patch.object(hurricanes, "cache_get", return_value=None),
                    patch.object(hurricanes, "cache_set") as cache_set,
                    patch.object(hurricanes, "select_filtered_event_rows", side_effect=select_rows),
                ):
                    response = asyncio.run(hurricanes.get_storm_tracks_geojson(
                        start="2017-01-01T00:00:00Z",
                        end="2026-12-31T23:59:59Z",
                        min_category="Cat1",
                    ))

                self.assertEqual(response.status_code, 200)
                payload = msgpack.unpackb(response.body, raw=False)
                self.assertEqual(payload["count"], 1)
                feature = payload["features"][0]
                self.assertEqual(len(feature["geometry"]["coordinates"]), 2)
                self.assertEqual(
                    "display_animation_kind" in feature["properties"],
                    prepared_lifecycle,
                )
                cache_set.assert_called_once()


if __name__ == "__main__":
    unittest.main()
