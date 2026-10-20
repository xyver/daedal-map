"""Resolve Network's files inside the user-selected DaedalMap data library."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


def network_data_dir(environment: Mapping[str, str] | None = None) -> Path:
    values = os.environ if environment is None else environment
    explicit = str(values.get("ROAD_EDITOR_DATA_DIR") or "").strip()
    if explicit:
        return Path(explicit).expanduser()
    shared_root = str(values.get("DATA_ROOT") or "").strip()
    if shared_root:
        return Path(shared_root).expanduser() / "networks"
    return Path(__file__).resolve().parents[1] / "data"
