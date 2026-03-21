"""
Settings management for County Map.

Settings are runtime-owned state and live in the configured config directory,
not in the source tree.
"""

import json
from pathlib import Path

from .paths import SETTINGS_PATH, ensure_dir

SETTINGS_FILE = SETTINGS_PATH

DEFAULT_SETTINGS = {
    "backup_path": ""
}

BACKUP_FOLDERS = ["geometry", "data"]


def load_settings() -> dict:
    try:
        if SETTINGS_FILE.exists():
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                settings = json.load(f)
                return {**DEFAULT_SETTINGS, **settings}
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Could not load settings: {e}")
    return DEFAULT_SETTINGS.copy()


def save_settings(settings: dict) -> bool:
    try:
        ensure_dir(SETTINGS_FILE.parent)
        current = load_settings()
        current.update(settings)
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2)
        return True
    except IOError as e:
        print(f"Error saving settings: {e}")
        return False


def get_backup_path() -> str:
    settings = load_settings()
    return settings.get("backup_path", "")


def set_backup_path(path: str) -> bool:
    return save_settings({"backup_path": path})


def check_backup_folders(backup_path: str) -> dict:
    if not backup_path:
        return {}

    base_path = Path(backup_path)
    result = {}

    for folder in BACKUP_FOLDERS:
        folder_path = base_path / folder
        if folder_path.exists():
            if folder == "geometry":
                parquet_files = list(folder_path.glob("*.parquet"))
                result[folder] = len(parquet_files) > 0
            else:
                subfolders = [d for d in folder_path.iterdir() if d.is_dir()]
                result[folder] = len(subfolders) > 0
        else:
            result[folder] = False

    return result


def init_backup_folders(backup_path: str) -> list:
    if not backup_path:
        raise ValueError("Backup path is required")

    base_path = Path(backup_path)
    created = []
    base_path.mkdir(parents=True, exist_ok=True)

    for folder in BACKUP_FOLDERS:
        folder_path = base_path / folder
        if not folder_path.exists():
            folder_path.mkdir(parents=True, exist_ok=True)
            created.append(folder)

        readme_path = folder_path / "README.txt"
        if not readme_path.exists():
            readme_path.write_text(get_folder_readme(folder), encoding="utf-8")

    return created if created else BACKUP_FOLDERS


def get_folder_readme(folder_name: str) -> str:
    readmes = {
        "geometry": """Geometry Folder
===============

This folder stores geographic boundary files (GeoJSON, Shapefiles).

Recommended structure:
  admin_0/          -- Country boundaries
  admin_1/          -- State/province boundaries
  admin_2/          -- County/district boundaries
  admin_3/          -- City/municipality boundaries

Sources:
  - GADM (gadm.org)
  - Natural Earth (naturalearthdata.com)
  - geoBoundaries (geoboundaries.org)
  - US Census TIGER/Line
""",
        "data": """Data Folder
===========

This folder stores indicator datasets. Each source is self-contained in its own folder.

Structure per source:
  [source_id]/
    all_countries.parquet   -- Main data file
    metadata.json           -- Data structure, columns, coverage
    reference.json          -- Optional: conceptual context for LLM

Examples:
  owid_co2/
    all_countries.parquet
    metadata.json
  un_sdg_01/
    all_countries.parquet
    metadata.json
    reference.json          -- SDG Goal 1 description, targets

Sources:
  - World Bank Open Data
  - UN SDG Database
  - OECD.Stat
  - OWID
"""
    }
    return readmes.get(folder_name, f"{folder_name} folder for county-map data")


def get_settings_with_status() -> dict:
    settings = load_settings()
    backup_path = settings.get("backup_path", "")
    return {
        "backup_path": backup_path,
        "folders_exist": check_backup_folders(backup_path) if backup_path else {}
    }
