import hashlib
import json
import zipfile

from local_data_store import install_verified_data_zip


def test_verified_legacy_zip_imports_without_admitting_archive_catalog(tmp_path):
    archive_path = tmp_path / "pack.zip"
    source = {"source_id": "hurricanes", "pack_id": "hurricanes",
              "path": "global/disasters/hurricanes", "data_type": "events"}
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("data/catalog.json", json.dumps({
            "sources": [source], "injected_field": "must not survive"}))
        archive.writestr("data/sources/hurricanes/metadata.json", json.dumps(source))
        archive.writestr("data/sources/hurricanes/events.parquet", b"fixture")
    release = {"packageId": "hurricanes-lite", "version": "20260816.1",
               "size": archive_path.stat().st_size,
               "sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest()}

    result = install_verified_data_zip(archive_path, tmp_path / "library",
                                       pack_id="hurricanes", release=release)

    catalog = json.loads((tmp_path / "library/data/catalog.json").read_text())
    assert result["pack_id"] == "hurricanes"
    assert catalog["sources"][0]["path"] == "global/disasters/hurricanes"
    assert "injected_field" not in catalog
    assert (tmp_path / "library/data/global/disasters/hurricanes/events.parquet").read_bytes() == b"fixture"
