"""One read-only local folder or S3-compatible prefix for a downloaded MCP."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any


def relative_member(value: str) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    path = PurePosixPath(raw)
    if (not raw or path.is_absolute() or PureWindowsPath(raw).drive
            or any(part in {"", ".", ".."} for part in path.parts)):
        raise ValueError(f"Source path must stay within the selected folder: {value!r}")
    return path.as_posix()


@dataclass(frozen=True)
class MCPSourceRoot:
    kind: str
    root: Path | None = None
    bucket: str = ""
    prefix: str = ""
    endpoint_url: str = ""
    region: str = "auto"
    cache_dir: Path | None = None

    @classmethod
    def local(cls, root: str | Path) -> "MCPSourceRoot":
        path = Path(root).expanduser().resolve()
        if not path.is_dir():
            raise ValueError(f"Local data folder does not exist: {path}")
        return cls(kind="local", root=path)

    @classmethod
    def cloud(cls, *, bucket: str, prefix: str, cache_dir: str | Path,
              endpoint_url: str = "", region: str = "auto") -> "MCPSourceRoot":
        bucket = str(bucket or "").strip()
        prefix = str(prefix or "").strip().strip("/")
        if not bucket or not prefix:
            raise ValueError("Cloud source requires an explicit bucket and prefix")
        relative_member(prefix)
        cache = Path(cache_dir).expanduser().resolve()
        return cls(kind="cloud", bucket=bucket, prefix=prefix,
                   endpoint_url=str(endpoint_url or "").strip(),
                   region=str(region or "auto").strip(), cache_dir=cache)

    def location(self) -> str:
        return str(self.root) if self.kind == "local" else f"s3://{self.bucket}/{self.prefix}/"

    def key(self, relative: str) -> str:
        return f"{self.prefix}/{relative_member(relative)}"

    def _client(self):
        import boto3
        return boto3.client("s3", endpoint_url=self.endpoint_url or None,
                            region_name=self.region)

    def read_bytes(self, relative: str) -> bytes:
        member = relative_member(relative)
        if self.kind == "local":
            path = (self.root / member).resolve()
            if not path.is_relative_to(self.root):
                raise ValueError("Source path escapes the selected folder")
            return path.read_bytes()
        return self._client().get_object(Bucket=self.bucket, Key=self.key(member))["Body"].read()

    def read_json(self, relative: str) -> dict[str, Any]:
        value = json.loads(self.read_bytes(relative).decode("utf-8-sig"))
        if not isinstance(value, dict):
            raise ValueError(f"Source catalog or metadata is not an object: {relative}")
        return value

    def local_file(self, relative: str) -> Path:
        """Return a local file; cloud objects are cached by selected source and ETag."""
        member = relative_member(relative)
        if self.kind == "local":
            path = (self.root / member).resolve()
            if not path.is_relative_to(self.root):
                raise ValueError("Source path escapes the selected folder")
            if not path.is_file():
                raise FileNotFoundError(path)
            return path
        client = self._client()
        key = self.key(member)
        try:
            identity = client.head_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                raise FileNotFoundError(f"Missing object in selected source: {key}") from exc
            raise
        stamp = f"{identity.get('ETag', '')}:{identity.get('ContentLength', '')}"
        digest = hashlib.sha256(f"{self.endpoint_url}|{self.bucket}|{key}|{stamp}".encode()).hexdigest()
        target = self.cache_dir / "objects" / digest[:2] / f"{digest}{Path(member).suffix}"
        if target.is_file() and target.stat().st_size == int(identity.get("ContentLength", -1)):
            return target
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix="download-", dir=target.parent)
        try:
            with os.fdopen(fd, "wb") as output:
                client.download_fileobj(self.bucket, key, output)
            if Path(temp_name).stat().st_size != int(identity.get("ContentLength", -1)):
                raise ValueError(f"Cloud object changed while reading: {key}")
            after = client.head_object(Bucket=self.bucket, Key=key)
            if (str(after.get("ETag")) != str(identity.get("ETag"))
                    or int(after.get("ContentLength", -1)) != int(identity.get("ContentLength", -1))):
                raise ValueError(f"Cloud object changed while reading: {key}")
            os.replace(temp_name, target)
        finally:
            Path(temp_name).unlink(missing_ok=True)
        return target
