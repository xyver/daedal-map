from __future__ import annotations

from pathlib import Path
import shutil
import time

import numpy as np
import pandas as pd

from .checkpoint import load_checkpoint, load_checkpoint_payload, save_checkpoint
from .schemas import AccessRunConfig


def _coerce_threshold_columns(thresholds_minutes: tuple[int, ...]) -> list[int]:
    values = sorted({int(value) for value in thresholds_minutes})
    if not values:
        raise ValueError("At least one threshold minute value is required")
    return values


def run_access_calculation(
    config: AccessRunConfig,
    *,
    provider,
) -> pd.DataFrame:
    started_at = time.perf_counter()
    origins_df = pd.read_parquet(config.origins_path)
    destinations_df = pd.read_parquet(config.destinations_path)
    if config.max_origins is not None:
        origins_df = origins_df.head(int(config.max_origins)).copy()

    thresholds = _coerce_threshold_columns(config.thresholds_minutes)
    edge_within_minutes = int(config.edge_within_minutes) if config.edge_output_path and config.edge_within_minutes else None
    checkpoint_payload = load_checkpoint_payload(config.checkpoint_path)
    processed_origin_ids, partial_results_df = load_checkpoint(config.checkpoint_path)
    pending_df = origins_df[~origins_df["origin_id"].astype(str).isin(processed_origin_ids)].copy()

    all_rows: list[dict[str, object]] = []
    if not partial_results_df.empty:
        all_rows.extend(partial_results_df.to_dict("records"))
    edge_chunk_paths: list[str] = list(checkpoint_payload.get("edge_chunk_paths") or [])
    edge_chunk_dir = config.checkpoint_path.with_suffix(".edge_chunks")
    if edge_within_minutes is not None:
        if not checkpoint_payload and edge_chunk_dir.exists():
            shutil.rmtree(edge_chunk_dir)
        edge_chunk_dir.mkdir(parents=True, exist_ok=True)
    current_edge_rows: list[dict[str, object]] = []

    def flush_edges() -> None:
        if edge_within_minutes is None or not current_edge_rows:
            return
        chunk_path = edge_chunk_dir / f"edges_{len(edge_chunk_paths) + 1:06d}.parquet"
        pd.DataFrame(current_edge_rows).to_parquet(chunk_path, index=False)
        edge_chunk_paths.append(str(chunk_path))
        current_edge_rows.clear()

    def checkpoint(index: int) -> None:
        flush_edges()
        save_checkpoint(
            config.checkpoint_path,
            processed_origin_ids=processed_origin_ids,
            partial_results_df=pd.DataFrame(all_rows),
            edge_chunk_paths=edge_chunk_paths if edge_within_minutes is not None else None,
            metadata={
                "provider_name": config.provider_name,
                "method_version": config.method_version,
                "thresholds_minutes": thresholds,
                "edge_within_minutes": edge_within_minutes,
                "origins_path": str(config.origins_path),
                "destinations_path": str(config.destinations_path),
                "processed_count": len(processed_origin_ids),
                "total_origin_count": len(origins_df),
            },
        )
        elapsed = max(0.001, time.perf_counter() - started_at)
        rate = index / elapsed
        remaining = len(pending_df) - index
        eta_minutes = (remaining / rate) / 60.0 if rate > 0 else float("inf")
        print(
            f"Processed {len(processed_origin_ids):,}/{len(origins_df):,} origins "
            f"({len(processed_origin_ids) / max(1, len(origins_df)):.1%}); "
            f"rate {rate:.2f}/s; ETA {eta_minutes:.1f} min",
            flush=True,
        )

    for index, (_, origin_row) in enumerate(pending_df.iterrows(), start=1):
        distances_km, minutes = provider.compute_batch(origin_row, destinations_df)
        finite_mask = np.isfinite(minutes)
        nearest_idx = int(minutes.argmin()) if finite_mask.any() else None
        row: dict[str, object] = {
            "origin_id": origin_row["origin_id"],
            "origin_name": origin_row.get("origin_name"),
            "origin_level": origin_row["origin_level"],
            "country_iso3": origin_row["country_iso3"],
            "state_code": origin_row.get("state_code"),
            "representative_point_method": origin_row.get("representative_point_method"),
            "nearest_destination_id": destinations_df.iloc[nearest_idx]["destination_id"] if nearest_idx is not None else None,
            "nearest_destination_name": destinations_df.iloc[nearest_idx].get("destination_name") if nearest_idx is not None else None,
            "nearest_minutes": round(float(minutes[nearest_idx]), 4) if nearest_idx is not None else float("inf"),
            "nearest_distance_km": round(float(distances_km[nearest_idx]), 4) if nearest_idx is not None else float("inf"),
            "provider_name": config.provider_name,
            "method_version": config.method_version,
        }
        if hasattr(provider, "last_candidate_count"):
            row["routed_candidate_count"] = int(provider.last_candidate_count)
        if hasattr(provider, "last_candidate_limited"):
            row["routed_candidate_limited"] = bool(provider.last_candidate_limited)
        for threshold in thresholds:
            within = minutes <= threshold
            row[f"reachable_{threshold}min"] = int(within.sum())
            if "evacuation_capacity" in destinations_df.columns:
                capacity = destinations_df.loc[within, "evacuation_capacity"].fillna(0).astype(float).sum()
                row[f"reachable_capacity_{threshold}min"] = float(capacity)
        all_rows.append(row)

        if edge_within_minutes is not None:
            edge_mask = minutes <= edge_within_minutes
            for destination_idx in np.flatnonzero(edge_mask):
                destination_row = destinations_df.iloc[int(destination_idx)]
                current_edge_rows.append(
                    {
                        "origin_id": origin_row["origin_id"],
                        "origin_level": origin_row["origin_level"],
                        "country_iso3": origin_row["country_iso3"],
                        "state_code": origin_row.get("state_code"),
                        "destination_id": destination_row["destination_id"],
                        "destination_name": destination_row.get("destination_name"),
                        "destination_family": destination_row.get("destination_family"),
                        "source_loc_id": destination_row.get("source_loc_id"),
                        "minutes": round(float(minutes[destination_idx]), 4),
                        "distance_km": round(float(distances_km[destination_idx]), 4),
                        "within_minutes": edge_within_minutes,
                        "provider_name": config.provider_name,
                        "method_version": config.method_version,
                    }
                )
        processed_origin_ids.add(str(origin_row["origin_id"]))

        if index % config.checkpoint_every == 0:
            checkpoint(index)

    results_df = pd.DataFrame(all_rows)
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_parquet(config.output_path, index=False)

    if edge_within_minutes is not None and config.edge_output_path is not None:
        flush_edges()
        edge_frames = [pd.read_parquet(path) for path in edge_chunk_paths if Path(path).exists()]
        edges_df = pd.concat(edge_frames, ignore_index=True) if edge_frames else pd.DataFrame()
        config.edge_output_path.parent.mkdir(parents=True, exist_ok=True)
        edges_df.to_parquet(config.edge_output_path, index=False)
    if config.checkpoint_path.exists():
        config.checkpoint_path.unlink()
    partial_path = config.checkpoint_path.with_suffix(".partial.parquet")
    if partial_path.exists():
        partial_path.unlink()
    if edge_within_minutes is not None and edge_chunk_dir.exists():
        shutil.rmtree(edge_chunk_dir)
    return results_df
