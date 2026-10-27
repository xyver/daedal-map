from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def load_checkpoint(checkpoint_path: Path) -> tuple[set[str], pd.DataFrame]:
    if not checkpoint_path.exists():
        return set(), pd.DataFrame()
    payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    processed = set(payload.get("processed_origin_ids", []))
    results_path = payload.get("partial_results_path")
    if results_path:
        partial_path = Path(results_path)
        if partial_path.exists():
            return processed, pd.read_parquet(partial_path)
    return processed, pd.DataFrame()


def load_checkpoint_payload(checkpoint_path: Path) -> dict[str, Any]:
    if not checkpoint_path.exists():
        return {}
    return json.loads(checkpoint_path.read_text(encoding="utf-8"))


def save_checkpoint(
    checkpoint_path: Path,
    *,
    processed_origin_ids: set[str],
    partial_results_df: pd.DataFrame,
    metadata: dict[str, Any] | None = None,
    edge_chunk_paths: list[str] | None = None,
) -> None:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = checkpoint_path.with_suffix(".partial.parquet")
    partial_results_df.to_parquet(partial_path, index=False)
    payload: dict[str, Any] = {
        "processed_origin_ids": sorted(processed_origin_ids),
        "partial_results_path": str(partial_path),
    }
    if edge_chunk_paths is not None:
        payload["edge_chunk_paths"] = edge_chunk_paths
    if metadata:
        payload["metadata"] = metadata
    checkpoint_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
