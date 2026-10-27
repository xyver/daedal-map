from __future__ import annotations

import argparse
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
BUILD_ROOT = Path(__file__).resolve().parents[2]
if str(BUILD_ROOT) not in sys.path:
    sys.path.insert(0, str(BUILD_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from path_bootstrap import ensure_private_build_paths

ensure_private_build_paths()

from road_access.calculator import run_access_calculation  # noqa: E402
from road_access.providers import (  # noqa: E402
    AirDistanceTravelTimeProvider,
    ValhallaMatrixTravelTimeProvider,
)
from road_access.schemas import AccessRunConfig  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run checkpointed road-access calculation.")
    parser.add_argument("--input-dir", required=True, help="Prepared input directory.")
    parser.add_argument("--output-path", required=True, help="Output parquet path.")
    parser.add_argument("--edge-output-path", help="Optional reachable origin-destination edge parquet output.")
    parser.add_argument("--edge-within-minutes", type=int, default=60)
    parser.add_argument(
        "--provider",
        default="air_distance_debug",
        choices=["air_distance_debug", "valhalla_matrix"],
        help="Travel-time provider.",
    )
    parser.add_argument(
        "--thresholds",
        default="30,60",
        help="Comma-separated minute thresholds.",
    )
    parser.add_argument("--checkpoint-every", type=int, default=100)
    parser.add_argument("--max-origins", type=int, default=None)
    parser.add_argument("--assumed-speed-kph", type=float, default=55.0)
    parser.add_argument("--detour-factor", type=float, default=1.35)
    parser.add_argument("--valhalla-url", default="http://localhost:8002", help="Valhalla service root URL.")
    parser.add_argument("--valhalla-costing", default="auto", help="Valhalla costing model.")
    parser.add_argument("--valhalla-units", default="kilometers", choices=["kilometers", "miles"])
    parser.add_argument("--valhalla-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--valhalla-target-batch-size", type=int, default=200)
    parser.add_argument("--valhalla-max-target-haversine-km", type=float, default=150.0)
    parser.add_argument("--valhalla-max-target-candidates", type=int, default=None)
    parser.add_argument("--valhalla-search-radius-meters", type=float, default=5000.0)
    parser.add_argument("--method-version", default="road_access_v1")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_path = Path(args.output_path)
    checkpoint_path = output_path.with_suffix(".checkpoint.json")
    thresholds = tuple(int(value.strip()) for value in args.thresholds.split(",") if value.strip())
    if args.provider == "air_distance_debug":
        provider = AirDistanceTravelTimeProvider(
            assumed_speed_kph=args.assumed_speed_kph,
            detour_factor=args.detour_factor,
        )
    elif args.provider == "valhalla_matrix":
        provider = ValhallaMatrixTravelTimeProvider(
            service_url=args.valhalla_url,
            costing=args.valhalla_costing,
            units=args.valhalla_units,
            timeout_seconds=args.valhalla_timeout_seconds,
            target_batch_size=args.valhalla_target_batch_size,
            max_target_haversine_km=args.valhalla_max_target_haversine_km,
            max_target_candidates=args.valhalla_max_target_candidates,
            search_radius_meters=args.valhalla_search_radius_meters,
        )
    else:
        raise ValueError(f"Unsupported provider: {args.provider}")
    config = AccessRunConfig(
        origins_path=input_dir / "origins.parquet",
        destinations_path=input_dir / "destinations.parquet",
        output_path=output_path,
        checkpoint_path=checkpoint_path,
        thresholds_minutes=thresholds,
        edge_output_path=Path(args.edge_output_path) if args.edge_output_path else None,
        edge_within_minutes=args.edge_within_minutes if args.edge_output_path else None,
        checkpoint_every=args.checkpoint_every,
        max_origins=args.max_origins,
        provider_name=args.provider,
        method_version=args.method_version,
    )
    results_df = run_access_calculation(config, provider=provider)
    print(f"Computed rows: {len(results_df):,}")
    print(f"Wrote: {output_path}")
    print(f"Provider: {args.provider}")


if __name__ == "__main__":
    main()
