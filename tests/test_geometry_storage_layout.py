from pathlib import Path

from mapmover.runtime.geometry_storage_layout import (
    catalog_artifact_path,
    country_admin_spine_root,
    country_release_manifest_relative,
    global_admin0_point_path,
    released_artifact_path,
    released_artifact_paths_by_hash,
)


PROFILE = {"release_id": "usa_geometry_1_3_2", "release_version": "1.3.2"}


def test_country_paths_come_from_one_contained_layout_contract() -> None:
    assert country_admin_spine_root(Path("data"), "usa", PROFILE) == Path(
        "data/geometry/countries/USA/admin_spine/exact/usa_geometry_1_3_2"
    )


def test_catalog_artifact_path_rejects_paths_outside_geometry() -> None:
    assert catalog_artifact_path(Path("data"), {"path": "geometry/global/exact/admin_0.parquet"}) == Path(
        "data/geometry/global/exact/admin_0.parquet"
    )
    assert catalog_artifact_path(Path("data"), {"path": "../secret"}) is None
    assert catalog_artifact_path(Path("data"), {"path": "catalog.json"}) is None
    assert country_release_manifest_relative("USA", PROFILE) == (
        "geometry/countries/USA/releases/1.3.2/manifest.json"
    )


def test_embedded_legacy_path_resolves_by_release_hash() -> None:
    digest = "a" * 64
    paths = released_artifact_paths_by_hash({
        "objects": [{"sha256": digest, "source_paths": [
            "geometry/countries/USA/reference/example/1.3.2/aliases.parquet"
        ]}]
    })
    assert released_artifact_path(
        "geometry/countries/USA/relationships/old/aliases.parquet", digest, paths,
    ) == "geometry/countries/USA/reference/example/1.3.2/aliases.parquet"


def test_unknown_hash_fails_closed_to_original_path() -> None:
    original = "geometry/countries/USA/relationships/old/aliases.parquet"
    assert released_artifact_path(original, "b" * 64, {}) == original


def test_global_point_path_uses_clean_contract_in_cloud(tmp_path: Path) -> None:
    record = {"path": "geometry/global/runtime/admin_0_point.parquet"}
    assert global_admin0_point_path(tmp_path, record, cloud_mode=True) == (
        tmp_path / "geometry/global/runtime/admin_0_point.parquet"
    )
