from __future__ import annotations

import hashlib

import pytest

from local_data_store import install_data_pack


def test_verified_download_is_reused_after_install_interruption(tmp_path, monkeypatch):
    payload = b"downloaded archive bytes"
    sha = hashlib.sha256(payload).hexdigest()
    pointer = {"pack_id": "sample", "current_version": "1.0.0",
               "version_manifest_url": "file:///release.json"}
    release = {"pack_id": "sample", "version": "1.0.0",
               "artifact": {"sha256": sha, "size_bytes": len(payload),
                            "download_url": "file:///sample.zip"}}
    downloads = []
    completed = []

    def download(_url, path):
        downloads.append(path)
        path.write_bytes(payload)

    def stop_after_download(*_args, **_kwargs):
        raise RuntimeError("installation interrupted")

    monkeypatch.setattr("local_data_store._extract_verified", stop_after_download)
    args = dict(fetch_json=lambda url: pointer if url.endswith("pointer.json") else release,
                download=download, expected_version="1.0.0", expected_size=len(payload),
                selected_sha=sha, on_download_complete=lambda: completed.append(True))
    for _ in range(2):
        with pytest.raises(RuntimeError, match="installation interrupted"):
            install_data_pack("file:///pointer.json", tmp_path, **args)
    assert len(downloads) == 1
    assert len(completed) == 2
    assert (tmp_path / "cache" / "pack-downloads" / f"{sha}.zip").read_bytes() == payload


def test_selected_release_change_stops_before_download(tmp_path):
    sha = hashlib.sha256(b"archive").hexdigest()
    pointer = {"pack_id": "sample", "current_version": "1.0.0",
               "version_manifest_url": "file:///release.json"}
    release = {"pack_id": "sample", "version": "1.0.0",
               "artifact": {"sha256": sha, "size_bytes": 7,
                            "download_url": "file:///sample.zip"}}
    with pytest.raises(ValueError, match="release changed"):
        install_data_pack("file:///pointer.json", tmp_path,
                          fetch_json=lambda url: pointer if url.endswith("pointer.json") else release,
                          download=lambda *_: pytest.fail("download should not start"),
                          expected_version="2.0.0")
