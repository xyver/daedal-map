"""Small exact-WKB fixture for the country-neutral compact history reader."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

from mapmover.runtime.compact_edition_history import read_compact_edition_history_rows


def _digest(data):
    return hashlib.sha256(data).hexdigest()


class CompactEditionHistoryRuntimeTests(unittest.TestCase):
    def test_reads_old_edition_and_rejects_drift(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            bank = root / "bank"
            objects = root / "history" / "objects"
            bank.joinpath("shapes").mkdir(parents=True)

            def put(rows, schema):
                staged = root / "staged.parquet"
                pq.write_table(pa.Table.from_pylist(rows, schema=schema), staged)
                digest = _digest(staged.read_bytes())
                target = objects / digest[:2] / digest
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(staged.read_bytes())
                return {"sha256": digest, "size_bytes": target.stat().st_size}

            newest = b"newest exact WKB bytes"
            oldest = b"older exact WKB bytes"
            latest_hash, old_hash = _digest(newest), _digest(oldest)
            baseline = put(
                [{"geometry_hash": latest_hash, "geometry": newest}],
                pa.schema([("geometry_hash", pa.string()), ("geometry", pa.binary())]),
            )
            patch_record = put(
                [{"old_geometry_hash": old_hash, "newer_geometry_hash": latest_hash,
                  "codec": "zstd-standalone", "payload": zstd.ZstdCompressor().compress(oldest)}],
                pa.schema([("old_geometry_hash", pa.string()), ("newer_geometry_hash", pa.string()),
                           ("codec", pa.string()), ("payload", pa.binary())]),
            )
            metadata = put([{"loc_id": "A", "geometry_sha256": old_hash}],
                           pa.schema([("loc_id", pa.string()), ("geometry_sha256", pa.string())]))
            history_path = root / "history" / "manifest.json"
            history_path.write_text(json.dumps({
                "schema_version": "0.1.0", "profile": "geometry_compact_edition_history",
                "status": "PASS", "years": [2020, 2025], "latest_year": 2025,
                "baseline": baseline, "editions": {"2020": {"metadata": metadata}},
                "reverse_steps": [{"from_year": 2025, "to_year": 2020,
                                   "patches": patch_record, "patch_count": 1}],
            }), encoding="utf-8")
            bank.joinpath("manifest.json").write_text(json.dumps({
                "storage_backend": "compact_edition_history_v1", "status": "PASS",
                "compact_history_manifest": {"path": "history/manifest.json",
                                             "sha256": _digest(history_path.read_bytes())},
                "compact_object_root": "history/objects", "source_edition_year": 2020,
            }), encoding="utf-8")
            pq.write_table(pa.Table.from_pylist([{
                "loc_id": "A", "geometry_hash": old_hash,
                "source_id": "native-A", "source_release": "2020",
            }]), bank / "shapes" / "edition_index.parquet")

            with patch("mapmover.runtime.compact_edition_history.DATA_ROOT", root):
                rows = read_compact_edition_history_rows(bank, ["A"])
                self.assertEqual(rows.iloc[0]["geometry"], oldest)
                self.assertEqual(rows.iloc[0]["source_id"], "native-A")
                (objects / patch_record["sha256"][:2] / patch_record["sha256"]).write_bytes(b"drift")
                with self.assertRaisesRegex(ValueError, "object drift"):
                    read_compact_edition_history_rows(bank, ["A"])


if __name__ == "__main__":
    unittest.main()
