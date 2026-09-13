from unittest.mock import patch

import pandas as pd

from mapmover.runtime import geometry_predicate_query


def test_hash_sharded_rows_use_one_duckdb_query_for_all_routed_files(tmp_path) -> None:
    values = ["candidate-a", "candidate-b"]
    while (
        geometry_predicate_query.stable_hash_shard(values[0], 4)
        == geometry_predicate_query.stable_hash_shard(values[1], 4)
    ):
        values[1] += "x"

    paths = {}
    for value in values:
        shard = geometry_predicate_query.stable_hash_shard(value, 4)
        path = tmp_path / f"{shard}.parquet"
        pd.DataFrame([{"candidate_id": value, "name": value.upper()}]).to_parquet(
            path, index=False,
        )
        paths[shard] = path

    real_run_df = geometry_predicate_query.run_df
    with patch.object(geometry_predicate_query, "run_df", wraps=real_run_df) as run:
        result = geometry_predicate_query.read_hash_sharded_rows(
            paths,
            values,
            shard_count=4,
            id_column="candidate_id",
            columns=["name"],
        )

    assert run.call_count == 1
    sql, parameters = run.call_args.args
    assert "read_parquet([?, ?], union_by_name=true)" in sql
    assert parameters[:2] == [str(paths[shard]) for shard in sorted(paths)]
    assert set(result["candidate_id"]) == set(values)
