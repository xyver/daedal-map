# California reference data

`reference_data/california/` is a compact planning-data package retained in
the [original road editor repository](https://github.com/xyver/road-network-editor/tree/main/reference_data/california).
It was not copied into the DaedalMap source tree. This program can rebuild it
under `network/reference_data/california/`; that output is ignored by Git.
It is separate from the editable Lake road baseline and scenario files.

It contains:

- `population_blocks_2020.parquet`: canonical block `loc_id`, population, and
  year. This is the sole population weight for the current optimizer.
- `cejst_classification.parquet` and `cejst_burdens.parquet`: California
  tract-level disadvantaged-community context.
- `nri_counties.parquet` and `future_nri_counties.parquet`: California
  county-level baseline and future risk context.
- `nss_shelters.parquet`: California FEMA shelter inventory snapshot.
- `geometry/`: California state boundary, all county polygons, Census tract
  polygons, and Census block-group polygons. The block layer is intentionally
  excluded; it is a future release/input bundle because it is about 249 MB.

`manifest.json` records row counts, sizes, provenance, and population QA. The
population source is 2020 Census P.L. 94-171 `P1_001N`; it is block-level,
matches every current California geometry-spine block ID, has no duplicates,
missing values, or negative counts. Zero-population blocks are retained.

Smart Location is intentionally deferred: it will be a separate California
block-group context package. ClimRR is also deferred because it needs an
explicit grid-versus-aggregation design.

Smart Location must first complete the normal County Map import and QA process
(source validation, ID/vintage alignment, geometry joins, metric QA, metadata,
and release checks). Once that is complete, this project should copy only the
validated California block-group extract and its provenance metadata.

## Rebuild

Download and extract the official California 2020 Census P.L. 94-171 archive,
then run:

```powershell
python scripts/prepare_california_reference_data.py `
  --source-data-root "C:\path\to\county-map-data" `
  --pl-directory ".\data\analysis\ca2020_pl"
```

The large download and rebuilt reference package remain local to this clone.
