"""Hashing helpers for raw file integrity checks."""

from __future__ import annotations

from pathlib import Path


def sha256sum(path: Path) -> str:
    """Return the SHA-256 hex digest for `path`."""
    raise NotImplementedError("Hashing helpers land in FUT-004.")


__all__ = ["sha256sum"]
