"""Small persisted scenario overlays for immutable typed network packs."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .network_pack import Pack, digest


SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]{1,80}$")


class PackScenarioStore:
    def __init__(self, root: Path):
        self.root = root

    def _path(self, pack: Pack, scenario_id: str) -> Path:
        if not SAFE_NAME.fullmatch(scenario_id):
            raise ValueError("Scenario ID must be 1-80 letters, digits, hyphens, or underscores")
        return self.root / pack.manifest["pack_id"] / f"{scenario_id}.json"

    def list(self, pack: Pack) -> list[str]:
        folder = self.root / pack.manifest["pack_id"]
        return sorted(path.stem for path in folder.glob("*.json") if SAFE_NAME.fullmatch(path.stem)) if folder.is_dir() else []

    def load(self, pack: Pack, scenario_id: str) -> dict[str, Any]:
        path = self._path(pack, scenario_id)
        if not path.is_file():
            raise FileNotFoundError(scenario_id)
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Scenario is not a JSON object")
        if payload.get("pack_sha256") != digest(pack.directory / "manifest.json"):
            raise ValueError("Scenario belongs to a different pack version")
        valid = {feature["properties"]["feature_id"] for feature in pack.features}
        closed = payload.get("closed_features")
        if not isinstance(closed, list) or len(closed) > 500 or not all(isinstance(item, str) for item in closed) or not set(closed) <= valid:
            raise ValueError("Scenario contains unknown feature IDs")
        return payload

    def save(self, pack: Pack, scenario_id: str, closed_features: list[str]) -> dict[str, Any]:
        path = self._path(pack, scenario_id)
        if "close_feature" not in pack.manifest["capabilities"]:
            raise ValueError("Pack does not support feature changes")
        valid = {feature["properties"]["feature_id"] for feature in pack.features}
        if len(closed_features) > 500 or not set(closed_features) <= valid:
            raise ValueError("Scenario contains too many or unknown feature IDs")
        payload = {"scenario_id": scenario_id, "pack_id": pack.manifest["pack_id"],
                   "pack_sha256": digest(pack.directory / "manifest.json"),
                   "closed_features": sorted(set(closed_features))}
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
        return payload

    def delete(self, pack: Pack, scenario_id: str) -> None:
        self.load(pack, scenario_id)
        self._path(pack, scenario_id).unlink()
