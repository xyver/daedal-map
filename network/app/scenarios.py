from __future__ import annotations

import json
import os
from pathlib import Path
from threading import Lock
from datetime import datetime, timezone


STATES = ("open", "forward_only", "reverse_only", "closed")
_lock = Lock()


class ScenarioStore:
    def __init__(self, data_dir: Path) -> None:
        self.directory = data_dir / "scenarios"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.saved_directory = self.directory / "saved"
        self.saved_directory.mkdir(parents=True, exist_ok=True)

    def _path(self, scenario_id: str) -> Path:
        safe_id = "".join(char for char in scenario_id if char.isalnum() or char in "-_")
        if not safe_id:
            raise ValueError("scenario_id must contain letters, numbers, hyphens, or underscores")
        return self.directory / f"{safe_id}.json"

    def _saved_path(self, name: str) -> Path:
        safe_name = "".join(char for char in name if char.isalnum() or char in "-_").strip("-_")
        if not safe_name:
            raise ValueError("Scenario name must contain letters or numbers")
        return self.saved_directory / f"{safe_name}.json"

    def _load(self, scenario_id: str) -> dict:
        path = self._path(scenario_id)
        if not path.exists():
            return {"scenario_id": scenario_id, "segment_states": {}, "shelter_changes": {"added": [], "removed_ids": []}}
        return json.loads(path.read_text(encoding="utf-8"))

    def _write(self, scenario_id: str, payload: dict) -> None:
        path = self._path(scenario_id)
        temp_path = path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temp_path, path)

    def get(self, scenario_id: str) -> dict[str, str]:
        payload = self._load(scenario_id)
        return {str(key): str(value) for key, value in payload.get("segment_states", {}).items() if value in STATES}

    def shelter_changes(self, scenario_id: str) -> dict:
        payload = self._load(scenario_id).get("shelter_changes", {})
        added = [item for item in payload.get("added", []) if isinstance(item, dict)]
        removed_ids = [str(item) for item in payload.get("removed_ids", []) if str(item)]
        return {"added": added, "removed_ids": removed_ids}

    def cycle(self, scenario_id: str, segment_id: str) -> str:
        if not segment_id:
            raise ValueError("segment_id is required")
        with _lock:
            payload = self._load(scenario_id)
            states = {str(key): str(value) for key, value in payload.get("segment_states", {}).items() if value in STATES}
            current = states.get(segment_id, "open")
            next_state = STATES[(STATES.index(current) + 1) % len(STATES)]
            if next_state == "open":
                states.pop(segment_id, None)
            else:
                states[segment_id] = next_state
            payload["scenario_id"] = scenario_id
            payload["segment_states"] = states
            self._write(scenario_id, payload)
        return next_state

    def clear(self, scenario_id: str) -> None:
        """Reset a scenario overlay without touching the immutable baseline graph."""
        with _lock:
            payload = self._load(scenario_id)
            payload["scenario_id"] = scenario_id
            payload["segment_states"] = {}
            self._write(scenario_id, payload)

    def add_shelter(self, scenario_id: str, shelter: dict) -> dict:
        with _lock:
            payload = self._load(scenario_id)
            changes = self.shelter_changes(scenario_id)
            changes["added"] = [item for item in changes["added"] if str(item.get("id")) != str(shelter["id"])]
            changes["added"].append(shelter)
            changes["removed_ids"] = [item for item in changes["removed_ids"] if item != str(shelter["id"])]
            payload["shelter_changes"] = changes
            self._write(scenario_id, payload)
        return changes

    def remove_shelter(self, scenario_id: str, shelter_id: str) -> dict:
        if not shelter_id:
            raise ValueError("shelter_id is required")
        with _lock:
            payload = self._load(scenario_id)
            changes = self.shelter_changes(scenario_id)
            if shelter_id not in changes["removed_ids"]:
                changes["removed_ids"].append(shelter_id)
            payload["shelter_changes"] = changes
            self._write(scenario_id, payload)
        return changes

    def reopen_shelter(self, scenario_id: str, shelter_id: str) -> dict:
        if not shelter_id:
            raise ValueError("shelter_id is required")
        with _lock:
            payload = self._load(scenario_id)
            changes = self.shelter_changes(scenario_id)
            changes["removed_ids"] = [item for item in changes["removed_ids"] if item != shelter_id]
            payload["shelter_changes"] = changes
            self._write(scenario_id, payload)
        return changes

    def delete_added_shelter(self, scenario_id: str, shelter_id: str) -> dict:
        if not shelter_id:
            raise ValueError("shelter_id is required")
        with _lock:
            payload = self._load(scenario_id)
            changes = self.shelter_changes(scenario_id)
            if not any(str(item.get("id")) == shelter_id for item in changes["added"]):
                raise ValueError("Only user-added shelters can be deleted")
            changes["added"] = [item for item in changes["added"] if str(item.get("id")) != shelter_id]
            changes["removed_ids"] = [item for item in changes["removed_ids"] if item != shelter_id]
            payload["shelter_changes"] = changes
            self._write(scenario_id, payload)
        return changes

    def reset_shelters(self, scenario_id: str) -> None:
        with _lock:
            payload = self._load(scenario_id)
            payload["shelter_changes"] = {"added": [], "removed_ids": []}
            self._write(scenario_id, payload)

    def list_saved(self) -> list[dict]:
        results = []
        for path in sorted(self.saved_directory.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                results.append({"name": payload.get("name", path.stem), "saved_at": payload.get("saved_at")})
            except (OSError, json.JSONDecodeError):
                continue
        return results

    def save_snapshot(self, scenario_id: str, name: str, start_point: dict | None) -> dict:
        with _lock:
            current = self._load(scenario_id)
            saved_at = datetime.now(timezone.utc).isoformat()
            snapshot = {
                "name": name.strip(),
                "saved_at": saved_at,
                "segment_states": current.get("segment_states", {}),
                "shelter_changes": self.shelter_changes(scenario_id),
                "start_point": start_point,
            }
            path = self._saved_path(name)
            path.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
        return {"name": snapshot["name"], "saved_at": saved_at}

    def load_snapshot(self, scenario_id: str, name: str) -> dict:
        path = self._saved_path(name)
        if not path.exists():
            raise ValueError("Saved scenario was not found")
        try:
            snapshot = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("Saved scenario file is invalid") from exc
        with _lock:
            payload = self._load(scenario_id)
            payload["segment_states"] = {
                str(key): str(value) for key, value in snapshot.get("segment_states", {}).items() if value in STATES
            }
            shelter_changes = snapshot.get("shelter_changes", {})
            payload["shelter_changes"] = {
                "added": [item for item in shelter_changes.get("added", []) if isinstance(item, dict)],
                "removed_ids": [str(item) for item in shelter_changes.get("removed_ids", []) if str(item)],
            }
            self._write(scenario_id, payload)
        return {"name": snapshot.get("name", name), "start_point": snapshot.get("start_point")}
