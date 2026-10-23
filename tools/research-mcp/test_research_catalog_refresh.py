"""Local Research sees new launcher downloads without a service restart."""

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


PUBLIC_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PUBLIC_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import research_mcp_module as research  # noqa: E402


def test_catalog_change_refreshes_visible_packs_and_details():
    with TemporaryDirectory() as temp, patch("mapmover.storage_mode.get_runtime_mode", return_value="local"):
        root = Path(temp)
        catalog_path = root / "catalog.json"
        source_dir = root / "global" / "sample"
        source_dir.mkdir(parents=True)
        (source_dir / "metadata.json").write_text(
            json.dumps({"source_id": "sample", "source_name": "Sample", "data_type": "metrics"}),
            encoding="utf-8",
        )
        catalog_path.write_text(
            json.dumps({"sources": [{"source_id": "sample", "path": "global/sample"}]}),
            encoding="utf-8",
        )
        research.set_local_data_source(root)
        try:
            assert "local_data" in research.where_is_research_data()["pack_ids"]
            catalog_path.write_text(
                json.dumps({"sources": [{"source_id": "sample", "path": "global/sample", "pack_id": "new_download"}]}),
                encoding="utf-8",
            )
            assert "new_download" in research.where_is_research_data()["pack_ids"]
            assert research.get_research_pack(pack_id="new_download")["outcome"] == "ok"
        finally:
            research.set_local_data_source(None)
