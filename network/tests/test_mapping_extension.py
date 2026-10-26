"""Contract check for Network packs served inside the local Mapping map."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.mapping_extension import router
from app.network_pack import write_pack


class MappingExpansionTests(unittest.TestCase):
    def test_all_typed_packs_open_on_one_global_map_and_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("road", "rail", "power", "river"):
                write_pack(
                    root / name, pack_id=name, version="1", network_type=name,
                    bbox=[-122, 48, -121, 49], capabilities=["connected_component"],
                    source={"name": "fixture"}, nodes={"a": [-122, 48], "b": [-121, 49]},
                    links=[{"id": "edge", "feature_id": "edge", "from": "a", "to": "b", "directed": False}],
                    features=[{"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[-122, 48], [-121, 49]]},
                               "properties": {"feature_id": "edge"}}],
                )
            app = FastAPI()
            app.include_router(router)
            with patch.dict("os.environ", {"NETWORK_PACKS_DIR": str(root)}), TestClient(app) as client:
                listed = client.get("/api/local/network/packs")
                self.assertEqual(listed.status_code, 200)
                self.assertEqual({item["network_type"] for item in listed.json()["packs"]},
                                 {"road", "rail", "power", "river"})
                for name in ("road", "rail", "power", "river"):
                    visible = client.get(f"/api/local/network/packs/{name}/features",
                                         params={"bbox": "-123,47,-120,50"})
                    self.assertEqual(visible.status_code, 200)
                    self.assertEqual(len(visible.json()["features"]), 3)
                    self.assertEqual(client.get(f"/api/local/network/packs/{name}/feature", params={"feature_id": "edge"}).json()["links"][0]["id"], "edge")
                    self.assertEqual(client.get(f"/api/local/network/packs/{name}/node", params={"node_id": "a"}).json()["links"][0]["id"], "edge")
                self.assertEqual(client.get("/api/local/network/packs/road/features",
                                            params={"bbox": "10,10,20,20"}).json()["features"], [])
                self.assertEqual(client.get("/api/local/network/packs/road/features",
                                            params={"bbox": "120,10,-120,20"}).status_code, 422)
                payload = root / "power" / "features.geojson"
                payload.write_text(json.dumps({"type": "FeatureCollection", "features": []}), encoding="utf-8")
                self.assertEqual(client.get("/api/local/network/packs/power/features").status_code, 422)
                self.assertNotIn("power", {item["pack_id"] for item in client.get("/api/local/network/packs").json()["packs"]})


if __name__ == "__main__":
    unittest.main()
