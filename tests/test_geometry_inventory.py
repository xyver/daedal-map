from mapmover.runtime.geometry_inventory import (
    build_depth_index,
    public_geometry_inventory_payload,
)


def test_small_marker_gets_coordinates_when_published_display_omits_centroids(monkeypatch) -> None:
    import json

    import pandas as pd
    from mapmover.runtime import geometry_inventory
    from mapmover import foundation_helpers

    polygon = {"type": "Polygon", "coordinates": [[
        [12.0, 12.0], [12.1, 12.0], [12.1, 12.1], [12.0, 12.1], [12.0, 12.0]
    ]]}
    frame = pd.DataFrame([{
        "loc_id": "TST", "name": "Test island", "geometry": json.dumps(polygon)
    }])
    monkeypatch.setattr(geometry_inventory, "load_geometry_catalog", lambda: {})
    monkeypatch.setattr(foundation_helpers, "load_global_country_display_frame", lambda: frame)
    payload = geometry_inventory._build_geometry_inventory_payload_cached(-987654321)
    props = payload["features"][0]["properties"]
    assert props["is_small"] is True
    assert 12.0 <= props["centroid_lon"] <= 12.1
    assert 12.0 <= props["centroid_lat"] <= 12.1


def test_candidate_product_cannot_raise_active_depth() -> None:
    catalog = {
        "country_family_coverage": [],
        "geometry_products": [{
            "product_group": "admin_spine",
            "scope": "BRA",
            "admin_coverage": {"max_admin_level": 5},
        }],
        "global_admin_baseline": [{
            "country_code": "BRA",
            "max_admin_level": 2,
        }],
    }

    index, _global = build_depth_index(catalog)

    assert index["BRA"]["max_admin_level"] == 2
    assert index["BRA"]["depth_source"] == "shared_bank_baseline"


def test_active_and_candidate_depths_remain_separate() -> None:
    catalog = {
        "country_family_coverage": [{
            "country_code": "GBR",
            "max_admin_level": 2,
            "active_admin_depth": 2,
            "candidate_admin_depth": 3,
            "candidate_admin_status": "blocked_spatial_qa",
            "families": [{
                "family_id": "administrative",
                "max_admin_level": 2,
                "active_admin_depth": 2,
                "candidate_admin_depth": 3,
                "candidate_admin_status": "blocked_spatial_qa",
            }],
        }],
        "global_admin_baseline": [{
            "country_code": "GBR",
            "max_admin_level": 2,
        }],
    }

    index, _global = build_depth_index(catalog)

    assert index["GBR"]["max_admin_level"] == 2
    assert index["GBR"]["candidate_admin_depth"] == 3
    assert index["GBR"]["candidate_admin_status"] == "blocked_spatial_qa"
    assert index["GBR"]["program"]["active_admin_depth"] == 2
    assert index["GBR"]["program"]["candidate_admin_depth"] == 3


def test_public_payload_shows_capability_without_candidate_or_workflow_state() -> None:
    payload = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": []},
            "properties": {
                "loc_id": "FRA",
                "name": "France",
                "max_admin_level": 2,
                "candidate_admin_depth": 4,
                "candidate_admin_status": "prepared_unadmitted",
                "depth_color": "#5b8db8",
                "depth_source": "country_program",
                "coverage_matrix_status": "internal review",
                "families": [
                    {"family_id": "administrative", "label": "Administrative", "available": True, "state": "published"},
                    {"family_id": "postal", "label": "Postal", "available": False, "state": "staged", "gap_or_disposition": "WIP"},
                ],
            },
        }],
        "legend": [],
        "catalog": {"schema_version": "1", "generated_at": "now", "catalog_fingerprint": "secret"},
    }

    public = public_geometry_inventory_payload(payload)
    properties = public["features"][0]["properties"]

    assert properties["max_admin_level"] == 2
    assert properties["coverage_basis"] == "enhanced_country_coverage"
    assert properties["catalog_view"] == "public"
    assert public["view"] == "public"
    assert properties["families"] == [{
        "family_id": "administrative",
        "label": "Administrative",
        "available": True,
    }]
    serialized = str(public)
    assert "candidate" not in serialized
    assert "prepared_unadmitted" not in serialized
    assert "internal review" not in serialized
    assert "catalog_fingerprint" not in serialized
