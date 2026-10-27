"""Standalone Network Parquet exports preserve graph IDs and spatial metadata."""

import json
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq
from shapely import wkb

from network.cli import validated_packs, write_tables
from network.app.network_pack import write_pack


class NetworkCliTests(unittest.TestCase):
    def test_exported_tables_preserve_graph_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_pack(root / "road", pack_id="road", version="1", network_type="road",
                       bbox=[-122, 48, -121, 49], capabilities=["connected_component"],
                       source={"name": "fixture"}, nodes={"a": [-122, 48], "b": [-121, 49]},
                       links=[{"id": "edge", "feature_id": "feature", "from": "a", "to": "b",
                               "directed": False, "distance_km": 2.5}],
                       features=[{"type": "Feature", "geometry": {"type": "LineString",
                                  "coordinates": [[-122, 48], [-121, 49]]},
                                  "properties": {"feature_id": "feature"}}])
            pack, = validated_packs(root)
            write_tables(pack, root / "tables")
            nodes = pq.read_table(root / "tables" / "nodes.parquet").to_pylist()
            links = pq.read_table(root / "tables" / "links.parquet").to_pylist()
            features = pq.read_table(root / "tables" / "features.parquet").to_pylist()
            self.assertEqual([row["node_id"] for row in nodes], ["a", "b"])
            self.assertEqual({row["geography_relation_status"] for row in nodes}, {"not_enriched"})
            self.assertTrue(all(row["containing_loc_id"] is None for row in nodes))
            self.assertIsNone(links[0]["containing_loc_id"])
            self.assertIsNone(features[0]["containing_loc_id"])
            self.assertEqual((links[0]["link_id"], links[0]["from_node"], links[0]["to_node"]),
                             ("edge", "a", "b"))
            self.assertEqual(links[0]["distance_km"], 2.5)
            self.assertEqual(wkb.loads(nodes[0]["geometry"]).geom_type, "Point")
            self.assertEqual(wkb.loads(features[0]["geometry"]).geom_type, "LineString")
            geo = json.loads(pq.read_metadata(root / "tables" / "features.parquet").metadata[b"geo"])
            self.assertEqual(geo["primary_column"], "geometry")
            self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["LineString"])
            manifest = json.loads((root / "tables" / "export_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["files"]["links.parquet"], 1)
            self.assertEqual(manifest["reference_loc_id"]["status"], "not_assigned")
            self.assertEqual(manifest["containing_loc_id"]["rule"],
                             "smallest_admin_containing_full_geometry")
            self.assertEqual(manifest["containing_loc_id"]["status"], "not_enriched")
            self.assertEqual(manifest["geography_relations"]["status"], "not_enriched")
            self.assertEqual(pq.read_table(root / "tables" / "geography_relations.parquet").num_rows, 0)


if __name__ == "__main__":
    unittest.main()
