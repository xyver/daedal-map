"""Smoke-check the running local typed-pack service, including scenario cleanup."""

from __future__ import annotations

import json
import uuid
from urllib.request import Request, urlopen


BASE = "http://127.0.0.1:8123"
PACK = "usa-narn-rail-norcal-2024"
FEATURE = "narn-323926"


def call(path: str, method: str = "GET", payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    request = Request(BASE + path, data=data, method=method,
                      headers={"Content-Type": "application/json"} if data else {})
    with urlopen(request, timeout=15) as response:
        return json.load(response)


def main() -> int:
    with urlopen(BASE + "/packs", timeout=15) as response:
        if response.status != 200:
            raise ValueError("Pack preview did not load")
    packs = call("/api/packs")["packs"]
    if sorted(item["network_type"] for item in packs) != ["power", "rail", "river"]:
        raise ValueError("Expected three typed candidates")
    scenario_id = "qa_" + uuid.uuid4().hex[:12]
    path = f"/api/packs/{PACK}/scenarios/{scenario_id}"
    created = False
    try:
        saved = call(path, "PUT", {"closed_features": [FEATURE]})
        created = True
        loaded = call(path)
        if saved != loaded or loaded["closed_features"] != [FEATURE]:
            raise ValueError("Saved scenario did not reload unchanged")
        if scenario_id not in call(f"/api/packs/{PACK}/scenarios")["scenario_ids"]:
            raise ValueError("Saved scenario was not listed")
    finally:
        if created:
            call(path, "DELETE")
    if scenario_id in call(f"/api/packs/{PACK}/scenarios")["scenario_ids"]:
        raise ValueError("Test scenario was not deleted")
    print(json.dumps({"preview": 200, "pack_types": sorted(item["network_type"] for item in packs),
                      "scenario_save_load_delete": "passed"}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
