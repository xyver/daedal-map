"""Standalone spreadsheet drafts must survive reload without changing a pack."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from network.app.network_pack import write_pack
from network.spreadsheet_editor import app


class SpreadsheetEditorTests(unittest.TestCase):
    def test_draft_cells_are_persistent_and_pack_is_immutable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pack_dir = root / "packs" / "road"
            write_pack(pack_dir, pack_id="road", version="1", network_type="road",
                       bbox=[-122, 48, -121, 49], capabilities=["inspect"],
                       source={"name": "fixture"}, nodes={"a": [-122, 48], "b": [-121, 49]},
                       links=[{"id": "edge", "feature_id": "feature", "from": "a", "to": "b",
                               "directed": False, "distance_km": 2.5}],
                       features=[{"type": "Feature", "geometry": {"type": "LineString",
                                  "coordinates": [[-122, 48], [-121, 49]]},
                                  "properties": {"feature_id": "feature"}}])
            source = (pack_dir / "nodes.json").read_bytes()
            with patch.dict("os.environ", {"NETWORK_PACKS_DIR": str(root / "packs"),
                                        "NETWORK_EDITOR_DRAFTS_DIR": str(root / "drafts")}):
                client = TestClient(app)
                self.assertEqual(client.get("/api/packs").json()[0]["pack_id"], "road")
                self.assertEqual(client.put("/api/packs/road/nodes/a/longitude",
                                            json={"value": -121.5}).status_code, 200)
                self.assertEqual(client.get("/api/packs/road/nodes?q=a").json()["rows"][0]["longitude"], -121.5)
                self.assertEqual(pq.read_table(root / "drafts" / "road" / "edits.parquet").num_rows, 1)
                self.assertEqual(client.put("/api/packs/road/features/feature/properties_json",
                                            json={"value": json.dumps({"feature_id": "changed"})}).status_code, 422)
                self.assertEqual(client.put("/api/packs/road/links/edge/to_node",
                                            json={"value": "missing"}).status_code, 422)
                self.assertEqual((pack_dir / "nodes.json").read_bytes(), source)


if __name__ == "__main__":
    unittest.main()
