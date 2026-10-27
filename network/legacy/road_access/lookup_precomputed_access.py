from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
BUILD_ROOT = Path(__file__).resolve().parents[2]
if str(BUILD_ROOT) not in sys.path:
    sys.path.insert(0, str(BUILD_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from path_bootstrap import DATA_ROOT, PUBLIC_ROOT, ensure_private_build_paths

ensure_private_build_paths()
public_root_str = str(PUBLIC_ROOT)
if public_root_str not in sys.path:
    sys.path.insert(0, public_root_str)

os.environ["RUNTIME_MODE"] = "local"
os.environ["DATA_ROOT"] = str(DATA_ROOT)

from road_access.lookup import (  # noqa: E402
    format_lookup_payload,
    lookup_precomputed_access,
    resolve_zip_to_canonical_loc_id,
)
from mapmover.runtime.loc_id_resolution import (  # noqa: E402
    resolve_place_to_loc_id_stack,
    resolve_point_to_loc_id_stack,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lookup precomputed road-access outputs through geometry normalization.")
    parser.add_argument("--results-path", required=True, help="Precomputed accessibility parquet.")
    parser.add_argument("--data-root", default=str(DATA_ROOT), help="Path to county-map-data root.")
    parser.add_argument("--loc-id", help="Direct canonical loc_id lookup.")
    parser.add_argument("--zip", dest="zip_code", help="USA ZIP code lookup via ZCTA centroid.")
    parser.add_argument("--admin-text", help="Admin text lookup such as California or Los Angeles County.")
    parser.add_argument("--lon", type=float, help="Point longitude.")
    parser.add_argument("--lat", type=float, help="Point latitude.")
    return parser.parse_args()


def _pick_normalized_loc_id(args: argparse.Namespace, data_root: Path) -> tuple[str, dict[str, object]]:
    if args.loc_id:
        return args.loc_id.strip(), {"input_kind": "loc_id", "input_value": args.loc_id.strip()}
    if args.zip_code:
        zip_payload = resolve_zip_to_canonical_loc_id(
            args.zip_code,
            data_root=data_root,
            resolve_point_to_loc_id_stack=resolve_point_to_loc_id_stack,
        )
        normalized = zip_payload["resolved"].get("deepest_resolved_loc_id")
        if not normalized:
            raise ValueError(f"ZIP {args.zip_code} did not resolve to a canonical loc_id")
        return normalized, zip_payload
    if args.admin_text:
        resolved = resolve_place_to_loc_id_stack(args.admin_text, country_hint="USA")
        normalized = resolved.get("deepest_resolved_loc_id")
        if not normalized:
            raise ValueError(f"Admin text did not resolve: {args.admin_text}")
        return normalized, {"input_kind": "admin_text", "input_value": args.admin_text, "resolved": resolved}
    if args.lon is not None or args.lat is not None:
        if args.lon is None or args.lat is None:
            raise ValueError("Both --lon and --lat are required for point lookup")
        resolved = resolve_point_to_loc_id_stack(float(args.lon), float(args.lat))
        normalized = resolved.get("deepest_resolved_loc_id")
        if not normalized:
            raise ValueError("Point did not resolve to a canonical loc_id")
        return normalized, {
            "input_kind": "point",
            "input_value": {"lon": float(args.lon), "lat": float(args.lat)},
            "resolved": resolved,
        }
    raise ValueError("Provide one of --loc-id, --zip, --admin-text, or --lon/--lat")


def main() -> None:
    args = parse_args()
    results_path = Path(args.results_path)
    data_root = Path(args.data_root)
    normalized_loc_id, query_payload = _pick_normalized_loc_id(args, data_root)
    lookup_result = lookup_precomputed_access(
        results_path=results_path,
        normalized_loc_id=normalized_loc_id,
    )
    print(format_lookup_payload(query=query_payload, lookup_result=lookup_result))


if __name__ == "__main__":
    main()
