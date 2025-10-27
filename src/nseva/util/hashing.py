"""Hashing helpers for raw file integrity checks."""

from __future__ import annotations

import hashlib
from pathlib import Path


def sha256sum(path: Path) -> str:
    """Return the SHA-256 hex digest for `path`."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


__all__ = ["sha256sum"]
