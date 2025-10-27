"""Local cache helpers for FuData downloads."""

from __future__ import annotations

from pathlib import Path


def cache_path_for(source: str, *, trade_date: str, root: Path) -> Path:
    """Return the path where a raw file should be stored.

    Mirrors the layout in Implementation Plan §8.
    """
    raise NotImplementedError("Cache path helpers will be fleshed out in FUT-004.")


__all__ = ["cache_path_for"]
