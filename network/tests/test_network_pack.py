import json
import tempfile
import unittest
from pathlib import Path

from app.network_pack import Pack, write_pack
from app.pack_scenarios import PackScenarioStore


def feature(feature_id):
    return {"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]},
            "properties": {"feature_id": feature_id}}


class NetworkPackContractTests(unittest.TestCase):
    def test_rail_closure_changes_distance_and_is_not_saved_in_pack(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "rail"
            nodes = {"a": [0, 0], "b": [1, 0], "c": [2, 0]}
            links = [
                {"id": "ab", "feature_id": "ab", "from": "a", "to": "b", "directed": False, "distance_km": 1},
                {"id": "bc", "feature_id": "bc", "from": "b", "to": "c", "directed": False, "distance_km": 1},
                {"id": "ac", "feature_id": "ac", "from": "a", "to": "c", "directed": False, "distance_km": 5},
            ]
            write_pack(directory, pack_id="rail", version="1", network_type="rail", bbox=[0, 0, 2, 1],
                       capabilities=["route_distance", "connected_component", "close_feature"],
                       source={"name": "fixture"}, nodes=nodes, links=links,
                       features=[feature(item["id"]) for item in links])
            pack = Pack.open(directory)
            self.assertEqual(pack.shortest_distance("a", "c")["distance_km"], 2)
            self.assertEqual(pack.shortest_distance("a", "c", disabled_features={"bc"})["distance_km"], 5)
            self.assertEqual(pack.shortest_distance("c", "a")["distance_km"], 2)
            self.assertEqual(pack.shortest_distance("a", "c")["distance_km"], 2)

    def test_river_direction_and_power_node_removal_are_distinct(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "river"
            nodes = {"upper": [0, 1], "middle": [0, 0], "outlet": [0, -1]}
            links = [
                {"id": "upper-middle", "feature_id": "upper-middle", "from": "upper", "to": "middle", "directed": True},
                {"id": "middle-outlet", "feature_id": "middle-outlet", "from": "middle", "to": "outlet", "directed": True},
            ]
            write_pack(directory, pack_id="river", version="1", network_type="river", bbox=[-1, -1, 1, 1],
                       capabilities=["trace_downstream", "trace_upstream"], source={"name": "fixture"}, nodes=nodes,
                       links=links, features=[feature(item["id"]) for item in links])
            pack = Pack.open(directory)
            self.assertEqual(pack.component("upper"), set(nodes))
            self.assertEqual(pack.component("outlet"), {"outlet"})
            self.assertEqual(pack.component("upper", disabled_nodes={"middle"}), {"upper"})
            self.assertEqual(pack.trace("upper", direction="downstream")["feature_ids"],
                             ["middle-outlet", "upper-middle"])
            self.assertEqual(pack.trace("outlet", direction="upstream")["node_ids"], sorted(nodes))
            self.assertEqual(pack.trace("upper", direction="downstream", disabled_features={"upper-middle"})["node_ids"],
                             ["upper"])
            with self.assertRaisesRegex(ValueError, "distance routing"):
                pack.shortest_distance("upper", "outlet")

    def test_loader_rejects_changed_payload_and_missing_endpoint(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "bad"
            with self.assertRaisesRegex(ValueError, "Unresolved endpoint"):
                write_pack(directory, pack_id="bad", version="1", network_type="power", bbox=[0, 0, 1, 1],
                           capabilities=[], source={"name": "fixture"}, nodes={"a": [0, 0]},
                           links=[{"id": "x", "feature_id": "x", "from": "a", "to": "missing", "directed": False}],
                           features=[feature("x")])
            # Reuse the invalid directory after clearing only the fixture files.
            for path in directory.iterdir():
                path.unlink()
            write_pack(directory, pack_id="power", version="1", network_type="power", bbox=[0, 0, 1, 1],
                       capabilities=["connected_component"], source={"name": "fixture"},
                       nodes={"a": [0, 0], "b": [1, 1]},
                       links=[{"id": "x", "feature_id": "x", "from": "a", "to": "b", "directed": False}],
                       features=[feature("x")])
            payload = json.loads((directory / "nodes.json").read_text())
            payload["a"] = [9, 9]
            (directory / "nodes.json").write_text(json.dumps(payload))
            with self.assertRaisesRegex(ValueError, "differs from manifest"):
                Pack.open(directory)

    def test_scenario_save_load_is_tied_to_pack_fingerprint(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "rail"
            write_pack(directory, pack_id="rail", version="1", network_type="rail", bbox=[0, 0, 1, 1],
                       capabilities=["close_feature"], source={"name": "fixture"},
                       nodes={"a": [0, 0], "b": [1, 1]},
                       links=[{"id": "ab", "feature_id": "ab", "from": "a", "to": "b", "directed": False}],
                       features=[feature("ab")])
            pack = Pack.open(directory)
            scenarios = PackScenarioStore(root / "scenarios")
            saved = scenarios.save(pack, "test_1", ["ab", "ab"])
            self.assertEqual(saved["closed_features"], ["ab"])
            self.assertEqual(scenarios.list(pack), ["test_1"])
            self.assertEqual(scenarios.load(pack, "test_1")["closed_features"], ["ab"])
            with self.assertRaisesRegex(ValueError, "unknown feature"):
                scenarios.save(pack, "wrong", ["missing"])
            (directory / "manifest.json").write_text((directory / "manifest.json").read_text() + "\n")
            with self.assertRaisesRegex(ValueError, "different pack version"):
                scenarios.load(pack, "test_1")
            (directory / "manifest.json").write_text((directory / "manifest.json").read_text().rstrip() + "\n")
            scenarios.delete(pack, "test_1")
            self.assertEqual(scenarios.list(pack), [])


if __name__ == "__main__":
    unittest.main()
