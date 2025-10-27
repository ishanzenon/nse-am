"""Path utilities centralising data layout decisions (Implementation Plan §8)."""

from __future__ import annotations

from pathlib import Path


def data_root_from_config(storage_root: str | Path) -> Path:
    """Return the resolved data root."""
    return Path(storage_root).expanduser().resolve()


__all__ = ["data_root_from_config"]
