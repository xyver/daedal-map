import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import asyncio

import msgpack
import pandas as pd

from mapmover import disaster_filters
from mapmover.routes.disasters import earthquakes, tsunamis, wildfires


class DisasterFilterLayoutRuntimeTests(unittest.TestCase):
    def test_installed_earthquake_source_serves_map_overlay(self):
        with tempfile.TemporaryDirectory() as directory:
            source_dir = Path(directory) / "global/disasters/earthquakes/sources/earthquakes_events"
            source_dir.mkdir(parents=True)
            pd.DataFrame([{
                "event_id": "eq-2014", "magnitude": 6.0, "depth_km": 10.0,
                "timestamp": pd.Timestamp("2014-03-01T00:00:00"), "year": 2014,
                "latitude": 1.0, "longitude": 2.0, "loc_id": "USA-CA",
            }]).to_parquet(source_dir / "events.parquet")
            with patch.object(earthquakes, "get_source_path", return_value=source_dir):
                response = asyncio.run(earthquakes.get_earthquakes_geojson(year=2014, min_magnitude=5.5))

        self.assertEqual(response.status_code, 200)
        payload = msgpack.unpackb(response.body, raw=False)
        self.assertEqual(len(payload["features"]), 1)
        self.assertEqual(payload["features"][0]["properties"]["event_id"], "eq-2014")

    def test_installed_earthquake_source_serves_popup_aftershocks(self):
        with tempfile.TemporaryDirectory() as directory:
            source_dir = Path(directory) / "global/disasters/earthquakes/sources/earthquakes_events"
            source_dir.mkdir(parents=True)
            pd.DataFrame([
                {"event_id": "main", "mainshock_id": None, "magnitude": 6.0,
                 "timestamp": pd.Timestamp("2014-03-01T00:00:00"), "year": 2014,
                 "latitude": 1.0, "longitude": 2.0},
                {"event_id": "child", "mainshock_id": "main", "magnitude": 5.0,
                 "timestamp": pd.Timestamp("2014-03-02T00:00:00"), "year": 2014,
                 "latitude": 1.1, "longitude": 2.1},
            ]).to_parquet(source_dir / "events.parquet")
            with patch.object(earthquakes, "get_source_path", return_value=source_dir):
                response = asyncio.run(earthquakes.get_earthquake_aftershocks("child"))

        self.assertEqual(response.status_code, 200)
        payload = msgpack.unpackb(response.body, raw=False)
        self.assertEqual(payload["metadata"]["event_id"], "main")
        self.assertEqual(payload["metadata"]["aftershock_count"], 1)

    def test_missing_local_wildfire_pack_is_not_reported_as_loaded(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(wildfires, "GLOBAL_DIR", Path(directory) / "global"), \
             patch.object(wildfires, "COUNTRIES_DIR", Path(directory) / "countries"), \
             patch.object(wildfires, "is_cloud_mode", return_value=False):
            response = asyncio.run(wildfires.get_wildfires_geojson(year=2020))

        self.assertEqual(response.status_code, 404)
        self.assertIn(b"Wildfire data not available", response.body)

    def test_canonical_mainshock_resolves_source_id_aftershocks(self):
        with tempfile.TemporaryDirectory() as directory:
            source_dir = Path(directory) / "global/disasters/earthquakes/sources/earthquakes_events"
            source_dir.mkdir(parents=True)
            pd.DataFrame([
                {"event_id": "canonical-main", "source_event_id": "us-main",
                 "mainshock_id": None, "magnitude": 7.0,
                 "timestamp": pd.Timestamp("2020-01-01"), "year": 2020,
                 "latitude": 1.0, "longitude": 2.0},
                {"event_id": "canonical-child", "source_event_id": "us-child",
                 "mainshock_id": "us-main", "magnitude": 5.8,
                 "timestamp": pd.Timestamp("2020-01-02"), "year": 2020,
                 "latitude": 1.1, "longitude": 2.1},
            ]).to_parquet(source_dir / "events.parquet")
            with patch.object(earthquakes, "get_source_path", return_value=source_dir):
                response = asyncio.run(earthquakes.get_earthquake_aftershocks("canonical-child"))

        self.assertEqual(response.status_code, 200)
        payload = msgpack.unpackb(response.body, raw=False)
        self.assertEqual(payload["metadata"]["event_id"], "canonical-main")
        self.assertEqual(payload["metadata"]["aftershock_count"], 1)

    def test_installed_tsunami_source_serves_runup_animation(self):
        with tempfile.TemporaryDirectory() as directory:
            source_dir = Path(directory) / "global/disasters/tsunamis/sources/tsunamis_events"
            source_dir.mkdir(parents=True)
            pd.DataFrame([{
                "event_id": "ts-1", "latitude": 1.0, "longitude": 2.0,
                "year": 2020, "timestamp": pd.Timestamp("2020-01-01"),
                "cause": "Earthquake", "eq_magnitude": 7.0, "deaths": 0,
            }]).to_parquet(source_dir / "events.parquet")
            pd.DataFrame([{
                "event_id": "ts-1", "runup_id": "runup-1", "latitude": 1.1,
                "longitude": 2.1, "country": "Example", "location": "Bay",
                "water_height_m": 2.5, "dist_from_source_km": 15.0,
                "arrival_travel_time_min": 20.0,
                "timestamp": pd.Timestamp("2020-01-01T00:20:00"), "deaths": 0,
            }]).to_parquet(source_dir / "runups.parquet")
            with patch.object(tsunamis, "get_source_path", return_value=source_dir):
                response = asyncio.run(tsunamis.get_tsunami_animation_data("ts-1"))

        self.assertEqual(response.status_code, 200)
        payload = msgpack.unpackb(response.body, raw=False)
        self.assertEqual(payload["metadata"]["runup_count"], 1)
        self.assertEqual(len(payload["features"]), 2)

    def test_wildfire_locations_use_canonical_source_event_areas(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(disaster_filters, "GLOBAL_DIR", Path(directory)), \
             patch.object(disaster_filters, "is_cloud_mode", return_value=True):
            usa = disaster_filters._resolve_event_areas_path("wildfires", "USA-CA")
            can = disaster_filters._resolve_event_areas_path("wildfires", "CAN-ON")
            global_path = disaster_filters._resolve_event_areas_path("wildfires", "BRA-SP")

        self.assertEqual(Path(directory) / "disasters/wildfires/sources/usa/event_areas.parquet", usa)
        self.assertEqual(Path(directory) / "disasters/wildfires/sources/can/event_areas.parquet", can)
        self.assertEqual(Path(directory) / "disasters/event_areas/wildfires.parquet", global_path)

    def test_earthquake_affected_area_ids_filter_event_id_column(self):
        with patch.object(earthquakes, "duckdb_available", return_value=True), \
             patch.object(earthquakes, "parquet_available", return_value=True), \
             patch.object(earthquakes, "path_to_uri", return_value="events.parquet"), \
             patch.object(earthquakes, "parquet_columns", return_value={"event_id", "loc_id"}), \
             patch.object(earthquakes, "get_affected_event_ids", return_value={"eq-1", "eq-2"}), \
             patch.object(earthquakes, "run_df", return_value=None) as run_df:
            earthquakes._load_earthquakes_duckdb(affected_loc_id="USA-CA")

        sql = run_df.call_args.args[0]
        self.assertIn('"event_id" IN (?, ?)', sql)
        self.assertNotIn('"loc_id" IN (?, ?)', sql)


if __name__ == "__main__":
    unittest.main()
