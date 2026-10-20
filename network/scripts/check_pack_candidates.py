"""Check the locally built rail, river, and power candidates together."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.network_pack import Pack  # noqa: E402


def main() -> int:
    root = Path(__file__).resolve().parents[1] / "data/packs"
    rail = Pack.open(root / "usa-narn-rail-norcal-2024")
    river = Pack.open(root / "na-hydrorivers-norcal-v10")
    power = Pack.open(root / "usa-transmission-norcal-candidate")
    rail_closure = None
    for link in rail.links[:200]:
        before = rail.shortest_distance(link["from"], link["to"])
        after = rail.shortest_distance(link["from"], link["to"],
                                       disabled_features={link["feature_id"]})
        if before != after:
            rail_closure = {"feature_id": link["feature_id"], "before": before,
                            "after": after}
            break
    if rail_closure is None:
        raise ValueError("No changed rail route among first 200 candidate arcs")
    river_link = next(link for link in river.links if not link["boundary_exit"])
    before = river.trace(river_link["from"], direction="downstream")
    after = river.trace(river_link["from"], direction="downstream",
                        disabled_features={river_link["feature_id"]})
    if len(before["node_ids"]) <= len(after["node_ids"]):
        raise ValueError("River closure did not stop downstream trace")
    if power.manifest["capabilities"] != ["inspect"]:
        raise ValueError("Power candidate exposes unverified topology analysis")
    print(json.dumps({"rail_nodes": len(rail.nodes), "rail_links": len(rail.links),
                      "rail_closure": rail_closure,
                      "river_nodes": len(river.nodes), "river_links": len(river.links),
                      "river_trace_before": len(before["node_ids"]),
                      "river_trace_after": len(after["node_ids"]),
                      "power_nodes": len(power.nodes), "power_links": len(power.links),
                      "power_capabilities": power.manifest["capabilities"]}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
