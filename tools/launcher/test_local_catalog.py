from __future__ import annotations

import json
from pathlib import Path

from launcher import local_folder_packs
from local_catalog import catalog_needs_refresh, refresh_catalog, scan_data_folder


def test_dropped_folder_refreshes_catalog_and_repoints_copied_sources(tmp_path: Path) -> None:
    data = tmp_path / "launcher-data" / "runtime" / "data"
    source = data / "global" / "disasters" / "earthquakes"
    source.mkdir(parents=True)
    (source / "metadata.json").write_text(json.dumps({
        "source_id": "earthquakes", "source_name": "Earthquakes", "pack_id": "earthquakes",
        "catalog_surfaces": ["explore", "research"]}), encoding="utf-8")
    (source / "events.parquet").write_bytes(b"parquet")
    detail = source / "admin2"
    detail.mkdir()
    (detail / "metadata.json").write_text('{"source_id":"earthquakes"}', encoding="utf-8")
    loose = data / "My Tables"
    loose.mkdir()
    (loose / "rows.parquet").write_bytes(b"parquet")
    (data / "catalog.json").write_text(json.dumps({
        "sources": [{"source_id": "earthquakes", "path": "global/disasters/earthquakes",
                     "runtime_data_root": "C:/old/AppData"}],
        "packs": [{"pack_id": "earthquakes", "pack_name": "Earthquakes", "pack_version": "1.0"}]
    }), encoding="utf-8")
    snapshot = tmp_path / "launcher-data" / "runtime" / "state" / "data_folder_snapshot.json"

    sources, fingerprint = scan_data_folder(data)
    assert {source["source_id"] for source in sources} == {"earthquakes", "my_tables"}
    assert catalog_needs_refresh(data, snapshot, sources, fingerprint)
    catalog = refresh_catalog(data, snapshot, sources, fingerprint)
    assert catalog["total_sources"] == 2
    assert all(source["runtime_data_root"] == str(data) for source in catalog["sources"])
    assert next(pack for pack in catalog["packs"] if pack["pack_id"] == "earthquakes")["pack_version"] == "1.0"
    assert {pack.pack_id for pack, _, _ in local_folder_packs(data, set())} == {"earthquakes", "local_my_tables"}
    assert not catalog_needs_refresh(data, snapshot, sources, fingerprint)

    (loose / "more.parquet").write_bytes(b"new")
    changed, new_fingerprint = scan_data_folder(data)
    assert catalog_needs_refresh(data, snapshot, changed, new_fingerprint)
