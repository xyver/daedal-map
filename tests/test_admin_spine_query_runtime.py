from pathlib import Path
from unittest.mock import patch

import pandas as pd
from shapely.geometry import Polygon

from mapmover.runtime import admin_spine_query


def test_cloud_manifest_check_follows_the_selected_active_lane() -> None:
    payload = {
        "status": "PASS", "country": "GBR",
        "layout_policy": "national_admin_0_3_plus_admin_1_owned_deep",
    }
    with patch.object(admin_spine_query, "read_artifact_json", return_value=payload) as reader:
        admin_spine_query._published_layout_manifest_available.cache_clear()
        assert admin_spine_query._published_layout_manifest_available(
            "GBR", "geometry/countries/GBR/releases/geometry/r/runtime/admin_spine/manifest.json"
        ) is True
    reader.assert_called_once_with(
        "geometry/countries/GBR/releases/geometry/r/runtime/admin_spine/manifest.json",
        lane="active",
    )


def test_cloud_layout_availability_comes_from_published_catalog() -> None:
    catalog = {
        "country_profiles": [{
            "country_code": "NZL", "release_status": "published",
            "query_layout_manifest": "geometry/countries/NZL/releases/geometry/nzl_geometry_1/runtime/admin_spine/manifest.json",
        }],
    }
    with (
        patch.object(admin_spine_query, "is_cloud_mode", return_value=True),
        patch.object(admin_spine_query, "load_geometry_catalog", return_value=catalog),
        patch.object(admin_spine_query, "_published_layout_manifest_available", return_value=True),
    ):
        admin_spine_query.clear_admin_spine_query_cache()
        assert admin_spine_query.layout_available("NZL") is True
        assert admin_spine_query.layout_available("FRA") is False


def test_cloud_layout_availability_fails_closed_without_catalog_activation() -> None:
    with (
        patch.object(admin_spine_query, "is_cloud_mode", return_value=True),
        patch.object(admin_spine_query, "load_geometry_catalog", return_value={"country_profiles": []}),
        patch.object(admin_spine_query, "_published_layout_manifest_available", return_value=True) as fallback,
    ):
        admin_spine_query.clear_admin_spine_query_cache()
        assert admin_spine_query.layout_available("NZL") is False
    fallback.assert_not_called()


def test_cloud_point_candidates_use_object_store_uri() -> None:
    class Connection:
        def execute(self, _sql, parameters):
            self.parameters = parameters
            return self

        def fetchall(self):
            return []

    connection = Connection()
    with patch.object(admin_spine_query, "path_to_uri", return_value="s3://bucket/published/layout.parquet"):
        admin_spine_query._metadata_with_geometry(connection, Path("layout.parquet"), 1.0, 2.0)
    assert connection.parameters[0] == "s3://bucket/published/layout.parquet"


def test_exact_id_load_opens_only_shallow_and_requested_admin1_shards() -> None:
    opened_paths = []

    class Result:
        def __init__(self, frame):
            self.frame = frame

        def fetchdf(self):
            return self.frame

    class Connection:
        def execute(self, _sql, parameters=None):
            if parameters is None:
                return self
            path = parameters[0]
            opened_paths.append(path)
            loc_ids = [value for value in parameters[1:] if isinstance(value, str)]
            return Result(pd.DataFrame({"loc_id": loc_ids, "name": loc_ids}))

        def close(self):
            pass

    with (
        patch.object(admin_spine_query, "layout_available", return_value=True),
        patch.object(admin_spine_query, "layout_root", return_value=Path("layout")),
        patch.object(admin_spine_query, "is_cloud_mode", return_value=True),
        patch.object(admin_spine_query, "path_to_uri", side_effect=lambda path: path.as_posix()),
        patch.object(admin_spine_query, "_connection", return_value=Connection()),
    ):
        result = admin_spine_query.load_rows_by_loc_ids(
            "USA",
            ["USA-SD-019", "USA-SD-019-967600-1-023", "USA-CA-037-001-1-001"],
            columns=["name"],
        )

    assert opened_paths == [
        "layout/admin_0_3.parquet",
        "layout/deep/USA-SD.parquet",
        "layout/deep/USA-CA.parquet",
    ]
    assert result["loc_id"].tolist() == [
        "USA-SD-019",
        "USA-SD-019-967600-1-023",
        "USA-CA-037-001-1-001",
    ]


def test_route_index_handles_ids_whose_hyphens_do_not_encode_depth() -> None:
    opened_paths = []

    class Result:
        def __init__(self, frame=None, rows=None):
            self.frame = frame
            self.rows = rows or []

        def fetchdf(self):
            return self.frame

        def fetchall(self):
            return self.rows

    class Connection:
        def execute(self, _sql, parameters=None):
            path = parameters[0]
            opened_paths.append(path)
            if str(path).endswith("loc_id_routes.parquet"):
                return Result(rows=[("DEU-GEM-091620000000", 5, "DEU-LAN-09")])
            return Result(frame=pd.DataFrame({
                "loc_id": ["DEU-GEM-091620000000"], "name": ["Muenchen"],
            }))

        def close(self):
            pass

    with (
        patch.object(admin_spine_query, "layout_available", return_value=True),
        patch.object(admin_spine_query, "layout_root", return_value=Path("layout")),
        patch.object(admin_spine_query, "is_cloud_mode", return_value=True),
        patch.object(admin_spine_query, "path_to_uri", side_effect=lambda path: path.as_posix()),
        patch.object(admin_spine_query, "_layout_manifest", return_value={
            "route_index": {"path": "loc_id_routes.parquet"},
        }),
        patch.object(admin_spine_query, "_connection", side_effect=lambda: Connection()),
    ):
        result = admin_spine_query.load_rows_by_loc_ids(
            "DEU", ["DEU-GEM-091620000000"], columns=["name"],
        )

    assert opened_paths == [
        "layout/loc_id_routes.parquet",
        "layout/deep/DEU-LAN-09.parquet",
    ]
    assert result["loc_id"].tolist() == ["DEU-GEM-091620000000"]


def test_descendant_scope_routes_state_to_one_deep_bank() -> None:
    calls = []

    class Result:
        def __init__(self, *, row=None, frame=None):
            self.row = row
            self.frame = frame

        def fetchone(self):
            return self.row

        def fetchdf(self):
            return self.frame

    class Connection:
        def execute(self, sql, parameters):
            calls.append((sql, parameters))
            if str(parameters[0]).endswith("loc_id_routes.parquet"):
                return Result(row=(1, "USA-TX"))
            rows = []
            for loc_id in ("USA-TX-201-A", "USA-TX-201-B"):
                row = {name: None for name in admin_spine_query.META_COLUMN_NAMES}
                row.update({
                    "loc_id": loc_id,
                    "parent_id": "USA-TX-201",
                    "admin_level": 4,
                    "name": loc_id,
                    "admin_0_loc_id": "USA",
                    "admin_1_loc_id": "USA-TX",
                    "admin_2_loc_id": "USA-TX-201",
                    "__total_count": 2,
                })
                rows.append(row)
            return Result(frame=pd.DataFrame(rows))

        def close(self):
            pass

    with (
        patch.object(admin_spine_query, "layout_available", return_value=True),
        patch.object(admin_spine_query, "layout_root", return_value=Path("layout")),
        patch.object(admin_spine_query, "is_cloud_mode", return_value=True),
        patch.object(admin_spine_query, "path_to_uri", side_effect=lambda path: path.as_posix()),
        patch.object(admin_spine_query, "_connection", return_value=Connection()),
    ):
        result = admin_spine_query.query_descendant_scope(
            "USA", "USA-TX", 4, limit=10,
        )

    assert result is not None
    assert result["total_count"] == 2
    assert [row["loc_id"] for row in result["rows"]] == ["USA-TX-201-A", "USA-TX-201-B"]
    assert [parameters[0] for _, parameters in calls] == [
        "layout/loc_id_routes.parquet",
        "layout/deep/USA-TX.parquet",
    ]
    result_sql, result_parameters = calls[1]
    assert '"admin_1_loc_id" = ?' in result_sql
    assert result_parameters[:3] == ["layout/deep/USA-TX.parquet", 4, "USA-TX"]


def test_descendant_scope_refuses_country_wide_deep_multibank_scan() -> None:
    class Result:
        def fetchone(self):
            return (0, "")

    class Connection:
        def execute(self, _sql, _parameters):
            return Result()

        def close(self):
            pass

    with (
        patch.object(admin_spine_query, "layout_available", return_value=True),
        patch.object(admin_spine_query, "layout_root", return_value=Path("layout")),
        patch.object(admin_spine_query, "path_to_uri", side_effect=lambda path: path.as_posix()),
        patch.object(admin_spine_query, "_connection", return_value=Connection()),
    ):
        result = admin_spine_query.query_descendant_scope("USA", "USA", 4)

    assert result is None


def test_modern_layout_fails_closed_when_route_index_is_unreadable() -> None:
    class Connection:
        def execute(self, _sql, parameters=None):
            raise OSError("route index unavailable")

        def close(self):
            pass

    with (
        patch.object(admin_spine_query, "layout_available", return_value=True),
        patch.object(admin_spine_query, "layout_root", return_value=Path("layout")),
        patch.object(admin_spine_query, "is_cloud_mode", return_value=True),
        patch.object(admin_spine_query, "_layout_manifest", return_value={
            "route_index": {"path": "loc_id_routes.parquet"},
        }),
        patch.object(admin_spine_query, "_connection", return_value=Connection()),
    ):
        result = admin_spine_query.load_rows_by_loc_ids(
            "DEU", ["DEU-GEM-091620000000"], columns=["name"],
        )

    assert result.empty


def test_route_identity_lookup_opens_only_route_index() -> None:
    opened_paths = []

    class Result:
        def fetchdf(self):
            return pd.DataFrame([{
                "loc_id": "AUS-1-001-002-003-004",
                "admin_level": 5,
                "admin_1_loc_id": "AUS-1",
            }])

    class Connection:
        def execute(self, _sql, parameters):
            opened_paths.append(parameters[0])
            return Result()

        def close(self):
            pass

    with (
        patch.object(admin_spine_query, "layout_available", return_value=True),
        patch.object(admin_spine_query, "layout_root", return_value=Path("layout")),
        patch.object(admin_spine_query, "_layout_manifest", return_value={
            "route_index": {"path": "loc_id_routes.parquet"},
        }),
        patch.object(admin_spine_query, "path_to_uri", side_effect=lambda path: path.as_posix()),
        patch.object(admin_spine_query, "_connection", return_value=Connection()),
    ):
        result = admin_spine_query.load_route_rows_by_loc_ids(
            "AUS", ["AUS-1-001-002-003-004"],
        )

    assert result.iloc[0]["admin_level"] == 5
    assert opened_paths == ["layout/loc_id_routes.parquet"]


def test_point_resolve_reads_each_layout_file_once_with_exact_shape_check() -> None:
    """The point resolver must not reopen a remote shard for WKB by loc_id."""
    names = [part.strip() for part in admin_spine_query.META_COLUMNS.replace("\n", " ").split(",")]
    square_wkb = Polygon([(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]).wkb

    def row(loc_id: str, level: int, parent: str = "", admin3: str = "") -> tuple:
        admin3 = admin3 or (loc_id if level == 3 else "")
        values = {name: "" for name in names}
        values.update({
            "loc_id": loc_id,
            "parent_id": parent,
            "admin_level": level,
            "name": loc_id,
            "admin_0_loc_id": "USA",
            "admin_1_loc_id": "USA-CA" if level else "",
            "admin_2_loc_id": "USA-CA-037" if level >= 2 else "",
            "admin_3_loc_id": admin3,
            "bbox_min_lon": 0.0,
            "bbox_min_lat": 0.0,
            "bbox_max_lon": 10.0,
            "bbox_max_lat": 10.0,
        })
        return tuple(values[name] for name in names) + (square_wkb,)

    shallow_rows = [
        row("USA", 0),
        row("USA-CA", 1, "USA"),
        row("USA-CA-037", 2, "USA-CA"),
        row("USA-CA-037-207400", 3, "USA-CA-037"),
    ]
    deep_rows = [
        row("USA-CA-037-207400-1", 4, "USA-CA-037-207400", "USA-CA-037-207400"),
        row("USA-CA-037-207400-1-024", 5, "USA-CA-037-207400-1", "USA-CA-037-207400"),
    ]

    class Result:
        def __init__(self, rows):
            self.rows = rows

        def fetchall(self):
            return self.rows

    class Connection:
        def __init__(self):
            self.calls = []

        def execute(self, sql, parameters):
            self.calls.append((sql, parameters))
            return Result(deep_rows if "deep/USA-CA.parquet" in str(parameters[0]) else shallow_rows)

        def close(self):
            pass

    connection = Connection()
    with (
        patch.object(admin_spine_query, "layout_available", return_value=True),
        patch.object(admin_spine_query, "layout_root", return_value=Path("layout")),
        patch.object(admin_spine_query, "is_cloud_mode", return_value=True),
        patch.object(admin_spine_query, "path_to_uri", side_effect=lambda path: path.as_posix()),
        patch.object(admin_spine_query, "_connection", return_value=connection),
    ):
        result = admin_spine_query.resolve_point("USA", 5.0, 5.0, target_admin_level=5)

    assert [item["loc_id"] for item in result["stack"]] == [
        "USA", "USA-CA", "USA-CA-037", "USA-CA-037-207400",
        "USA-CA-037-207400-1", "USA-CA-037-207400-1-024",
    ]
    assert result["matched"]["loc_id"] == "USA-CA-037-207400-1-024"
    assert len(connection.calls) == 2
    assert all("ST_AsWKB(geometry)" in sql for sql, _ in connection.calls)
    assert all("WHERE loc_id IN" not in sql for sql, _ in connection.calls)


def test_batch_point_resolve_opens_shallow_bank_once_for_every_point() -> None:
    """Batch size must not multiply physical Parquet reads."""
    square_wkb = Polygon([(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]).wkb
    rows = []
    for level, loc_id, parent_id in (
        (0, "USA", ""),
        (1, "USA-CA", "USA"),
        (2, "USA-CA-001", "USA-CA"),
    ):
        row = {name: "" for name in admin_spine_query.META_COLUMN_NAMES}
        row.update({
            "loc_id": loc_id,
            "parent_id": parent_id,
            "admin_level": level,
            "name": loc_id,
            "admin_0_loc_id": "USA",
            "admin_1_loc_id": "USA-CA" if level >= 1 else "",
            "admin_2_loc_id": "USA-CA-001" if level >= 2 else "",
            "bbox_min_lon": 0.0,
            "bbox_min_lat": 0.0,
            "bbox_max_lon": 10.0,
            "bbox_max_lat": 10.0,
            "geometry": square_wkb,
        })
        rows.append(row)
    frame = pd.DataFrame(rows)

    class Connection:
        def close(self):
            pass

    with (
        patch.object(admin_spine_query, "layout_available", return_value=True),
        patch.object(admin_spine_query, "layout_root", return_value=Path("layout")),
        patch.object(admin_spine_query, "_connection", return_value=Connection()),
        patch.object(
            admin_spine_query,
            "_metadata_with_geometry_bbox",
            return_value=frame,
        ) as bank_read,
    ):
        result = admin_spine_query.resolve_points(
            "USA",
            [{"lon": 5.0, "lat": 5.0} for _ in range(100)],
            target_admin_level=2,
        )

    bank_read.assert_called_once()
    assert len(result or []) == 100
    assert all(item["matched"]["loc_id"] == "USA-CA-001" for item in result or [])


def test_point_resolve_exactly_recovers_null_ancestry_rows_from_layout_banks() -> None:
    names = admin_spine_query.META_COLUMN_NAMES
    square_wkb = Polygon([(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]).wkb

    ancestry = {
        0: "DEU", 1: "DEU-AA", 2: "DEU-AA-NULL2",
        3: "DEU-AA-NULL2-003", 4: "DEU-AA-NULL2-003-NULL4",
        5: "DEU-AA-NULL2-003-NULL4-001",
    }

    def row(loc_id: str, level: int, *, shape: bool, name: str | None = None) -> tuple:
        values = {column: None for column in names}
        values.update({
            "loc_id": loc_id,
            "parent_id": ancestry.get(level - 1, "") if level else "",
            "admin_level": level,
            "name": name or loc_id,
            **{f"admin_{item}_loc_id": value for item, value in ancestry.items()},
            "bbox_min_lon": 0.0 if shape else None,
            "bbox_min_lat": 0.0 if shape else None,
            "bbox_max_lon": 10.0 if shape else None,
            "bbox_max_lat": 10.0 if shape else None,
        })
        return tuple(values[column] for column in names) + (square_wkb if shape else None,)

    shallow_rows = [row("DEU", 0, shape=True), row("DEU-AA", 1, shape=True)]
    deep_rows = [
        row(ancestry[3], 3, shape=True),
        row(ancestry[5], 5, shape=True),
    ]
    null2 = row(ancestry[2], 2, shape=False, name="Missing Admin 2")
    null4 = row(ancestry[4], 4, shape=False, name="Missing Admin 4")

    class Result:
        def __init__(self, rows):
            self.rows = rows

        def fetchall(self):
            return self.rows

    class Connection:
        def __init__(self):
            self.calls = []

        def execute(self, sql, parameters):
            path = str(parameters[0])
            self.calls.append((sql, parameters))
            if "ST_AsWKB" in sql:
                return Result(deep_rows if "/deep/" in path else shallow_rows)
            if "/deep/" in path:
                return Result([null4])
            return Result([null2])

        def close(self):
            pass

    connection = Connection()
    with (
        patch.object(admin_spine_query, "layout_available", return_value=True),
        patch.object(admin_spine_query, "layout_root", return_value=Path("layout")),
        patch.object(admin_spine_query, "is_cloud_mode", return_value=True),
        patch.object(admin_spine_query, "path_to_uri", side_effect=lambda path: path.as_posix()),
        patch.object(admin_spine_query, "_connection", return_value=connection),
    ):
        result = admin_spine_query.resolve_point("DEU", 5.0, 5.0, target_admin_level=5)

    assert [item["loc_id"] for item in result["stack"]] == [
        ancestry[level] for level in range(6)
    ]
    assert result["stack"][2]["name"] == "Missing Admin 2"
    assert result["stack"][2]["identity_only"] is True
    assert result["stack"][4]["name"] == "Missing Admin 4"
    assert result["stack"][4]["identity_only"] is True
    assert result["matched"]["loc_id"] == ancestry[5]
    # Two spatial scans plus two exact identity lookups; NULL rows never go
    # through the WKB/point-containment query.
    assert len(connection.calls) == 4
    assert sum("ST_AsWKB" in sql for sql, _ in connection.calls) == 2
