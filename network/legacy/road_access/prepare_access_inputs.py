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

from path_bootstrap import DATA_ROOT, ensure_private_build_paths

ensure_private_build_paths()

from road_access.loaders import (  # noqa: E402
    load_nss_shelter_destinations,
    load_usa_admin_origins,
    write_prepared_inputs,
)
from road_access.schemas import PreparedInputsManifest  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare road-access origin/destination tables.")
    parser.add_argument("--country", default="USA", help="Country ISO3. Only USA is implemented initially.")
    parser.add_argument("--state", default="CA", help="State/province code for the first regional batch.")
    parser.add_argument("--origin-level", default="admin_4", choices=["admin_2", "admin_3", "admin_4", "admin_5"])
    parser.add_argument("--destination-source", default="nss_shelters", choices=["nss_shelters"])
    parser.add_argument("--destination-family", default="shelter")
    parser.add_argument("--representative-point-method", default="geometry_centroid")
    parser.add_argument(
        "--data-root",
        default=str(DATA_ROOT),
        help="Path to county-map-data root.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Output directory for prepared inputs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_root = Path(args.data_root)
    output_dir = Path(args.output_dir)
    country = args.country.upper()
    state_code = args.state.upper()

    if country != "USA":
        raise NotImplementedError("Initial prepare_access_inputs implementation supports USA only.")
    if args.destination_source != "nss_shelters":
        raise NotImplementedError("Initial prepare_access_inputs implementation supports NSS shelters only.")

    origins_df = load_usa_admin_origins(
        data_root,
        state_code=state_code,
        origin_level=args.origin_level,
        representative_point_method=args.representative_point_method,
    )
    destinations_df = load_nss_shelter_destinations(
        data_root,
        state_code=state_code,
        destination_family=args.destination_family,
    )
    manifest = PreparedInputsManifest(
        country_iso3=country,
        state_code=state_code,
        origin_level=args.origin_level,
        destination_family=args.destination_family,
        representative_point_method=args.representative_point_method,
        origins_path=str(output_dir / "origins.parquet"),
        destinations_path=str(output_dir / "destinations.parquet"),
        origin_count=len(origins_df),
        destination_count=len(destinations_df),
        notes=[
            "Initial implementation uses geometry centroids as representative points.",
            "Initial destination source is NSS shelters.",
            "Real road routing provider can be swapped in without changing prepared input schema.",
        ],
    )
    write_prepared_inputs(
        origins_df=origins_df,
        destinations_df=destinations_df,
        output_dir=output_dir,
        manifest=manifest,
    )
    print(f"Prepared origins:      {len(origins_df):,}")
    print(f"Prepared destinations: {len(destinations_df):,}")
    print(f"Wrote: {output_dir}")


if __name__ == "__main__":
    main()
