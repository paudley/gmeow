# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Store immutable attachment and raw content objects.

The module manages compressed content-addressed storage and sidecar metadata refreshes. It connects
object persistence with attachment analysis so cached artifacts remain reproducible.
"""

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import blake3
import zstandard as zstd

from .attachment_analysis import analyze_attachment
from .config import AttachmentAnalysisConfig
from .metadata import extract_exiftool_metadata

DEFAULT_ATTACHMENT_ANALYSIS_CONFIG = cast(AttachmentAnalysisConfig, None)
DEFAULT_DICT_ANY = cast(dict[str, Any], None)

COMPRESSIBLE_TYPES = (
    "application/json",
    "application/mbox",
    "message/rfc822",
    "text/",
)

INCOMPRESSIBLE_TYPES = (
    "application/gzip",
    "application/pdf",
    "application/x-7z-compressed",
    "application/x-gzip",
    "application/zip",
    "audio/",
    "image/",
    "video/",
)


@dataclass(frozen=True, slots=True)
class StoredObject:
    """Represent StoredObject data and behavior."""

    digest: str
    path: Path
    media_type: str
    compression: str
    original_size: int
    stored_size: int


class ObjectStore:
    """Represent ObjectStore data and behavior."""

    def __init__(self, root: Path, zstd_level: int = 6) -> None:
        """Initialize ObjectStore."""
        self.root = root
        self.zstd_level = zstd_level
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for_digest(self, digest: str, compression: str = "identity") -> Path:
        """Path for digest."""
        suffix = ".zst" if compression == "zstd" else ""
        return self.root / "blake3" / digest[:2] / digest[2:4] / f"{digest}{suffix}"

    def put_json(self, value: object, media_type: str = "application/json") -> StoredObject:
        """Put json."""
        return self.put(json.dumps(value, sort_keys=True, separators=(",", ":")).encode(), media_type=media_type)

    def put_text(self, value: str, media_type: str = "text/plain; charset=utf-8") -> StoredObject:
        """Put text."""
        return self.put(value.encode(), media_type=media_type)

    def put(self, content: bytes, media_type: str = "application/octet-stream") -> StoredObject:
        """Put."""
        digest = blake3.blake3(content).hexdigest()
        compression = "zstd" if _should_compress(media_type) else "identity"
        stored = zstd.ZstdCompressor(level=self.zstd_level).compress(content) if compression == "zstd" else content
        path = self.path_for_digest(digest, compression=compression)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            self._atomic_write(path, stored)
        content = self.get(digest, compression=compression)
        if blake3.blake3(content).hexdigest() != digest:
            msg = f"CAS verification failed after write for {digest}"
            raise OSError(msg)
        return StoredObject(
            digest=digest,
            path=path,
            media_type=media_type,
            compression=compression,
            original_size=len(content),
            stored_size=len(stored),
        )

    def get(self, digest: str, compression: str = "identity") -> bytes:
        """Get."""
        path = self.path_for_digest(digest, compression=compression)
        content = path.read_bytes()
        if compression == "zstd":
            content = zstd.ZstdDecompressor().decompress(content)
        if blake3.blake3(content).hexdigest() != digest:
            msg = f"CAS digest mismatch for {digest}"
            raise OSError(msg)
        return content

    def get_text(self, digest: str, compression: str = "identity") -> str:
        """Get text."""
        return self.get(digest, compression=compression).decode("utf-8", errors="replace")

    def _atomic_write(self, path: Path, content: bytes) -> None:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as tmp:
            tmp.write(content)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)
        try:
            tmp_path.replace(path)
            dir_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()


@dataclass(frozen=True, slots=True)
class StoredAttachmentObject:
    """Represent StoredAttachmentObject data and behavior."""

    sha1: str
    digest: str
    path: Path
    sidecar_path: Path
    size: int
    metadata: dict[str, Any]
    compression: str


class CasAttachmentStore:
    """Attachment store facade backed by the BLAKE3 object store.

    The public field remains `sha1` for the existing API surface, but the value
    is the CAS digest. New storage is BLAKE3-addressed.
    """

    def __init__(self, store: ObjectStore, analysis_config: AttachmentAnalysisConfig = DEFAULT_ATTACHMENT_ANALYSIS_CONFIG) -> None:
        """Initialize CasAttachmentStore."""
        self.store = store
        self.analysis_config = analysis_config or AttachmentAnalysisConfig()

    def sidecar_for_digest(self, digest: str) -> Path:
        """Sidecar for digest."""
        return self.store.root / "sidecars" / digest[:2] / digest[2:4] / f"{digest}.json"

    def put(
        self,
        content: bytes,
        metadata: dict[str, Any],
        *,
        extract_metadata: bool = True,
        media_type: str = "application/octet-stream",
    ) -> StoredAttachmentObject:
        """Put."""
        media_type = (metadata.get("gmail", {}).get("mime_type") if isinstance(metadata.get("gmail"), dict) else None) or media_type
        stored = self.store.put(content, media_type=media_type)
        merged = self.read_metadata(stored.digest)
        merged["source"] = _deep_merge(merged.get("source", {}), metadata)
        merged.setdefault("digest", stored.digest)
        merged.setdefault("sha1", stored.digest)
        merged.setdefault("size", len(content))
        merged.setdefault("media_type", media_type)
        merged.setdefault("compression", stored.compression)
        if extract_metadata:
            merged["exiftool"] = extract_exiftool_metadata(stored.path)
            merged["exiftool"]["extracted_at"] = datetime.now(UTC).isoformat()
            merged["analysis"] = _deep_merge(
                merged.get("analysis", {}),
                analyze_attachment(stored.path, content, merged, self.analysis_config),
            )
        sidecar = self.sidecar_for_digest(stored.digest)
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        sidecar.write_text(json.dumps(merged, indent=2, sort_keys=True))
        return StoredAttachmentObject(
            sha1=stored.digest,
            digest=stored.digest,
            path=stored.path,
            sidecar_path=sidecar,
            size=len(content),
            metadata=merged,
            compression=stored.compression,
        )

    def read_metadata(self, sha1: str) -> dict[str, Any]:
        """Read metadata."""
        sidecar = self.sidecar_for_digest(sha1)
        if not sidecar.exists():
            return {}
        return cast(dict[str, Any], json.loads(sidecar.read_text()))

    def get_bytes(self, sha1: str, compression: str = "identity") -> bytes:
        """Get bytes."""
        metadata = self.read_metadata(sha1)
        return self.store.get(sha1, compression=metadata.get("compression") or compression)

    def get(self, sha1: str) -> Path:
        """Get."""
        metadata = self.read_metadata(sha1)
        compression = metadata.get("compression") or "identity"
        path = self.store.path_for_digest(sha1, compression=compression)
        if not path.exists():
            raise FileNotFoundError(sha1)
        return path

    def refresh_sidecar(self, sha1: str, source_metadata: dict[str, Any] = DEFAULT_DICT_ANY) -> StoredAttachmentObject:
        """Refresh sidecar."""
        metadata = self.read_metadata(sha1)
        compression = metadata.get("compression") or "identity"
        path = self.get(sha1)
        merged = dict(metadata)
        if source_metadata:
            merged["source"] = _deep_merge(merged.get("source", {}), source_metadata)
        content = self.store.get(sha1, compression=compression)
        extract_path = path
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            if compression != "identity":
                tmp.write(content)
                tmp.flush()
                extract_path = Path(tmp.name)
            merged["exiftool"] = extract_exiftool_metadata(extract_path)
            merged["analysis"] = _deep_merge(
                merged.get("analysis", {}),
                analyze_attachment(extract_path, content, merged, self.analysis_config),
            )
        merged["exiftool"]["extracted_at"] = datetime.now(UTC).isoformat()
        merged.setdefault("digest", sha1)
        merged.setdefault("sha1", sha1)
        merged.setdefault("size", len(content))
        merged.setdefault("compression", compression)
        sidecar = self.sidecar_for_digest(sha1)
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        sidecar.write_text(json.dumps(merged, indent=2, sort_keys=True))
        return StoredAttachmentObject(
            sha1=sha1, digest=sha1, path=path, sidecar_path=sidecar, size=len(content), metadata=merged, compression=compression
        )


def _should_compress(media_type: str) -> bool:
    value = (media_type or "").lower()
    if any(value.startswith(prefix) for prefix in INCOMPRESSIBLE_TYPES):
        return False
    return any(value.startswith(prefix) for prefix in COMPRESSIBLE_TYPES)


def _deep_merge(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(cast(dict[str, Any], merged[key]), cast(dict[str, Any], value))
        else:
            merged[key] = value
    return merged
