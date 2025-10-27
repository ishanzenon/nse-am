"""Local cache helpers for FuData downloads."""

from __future__ import annotations

from datetime import date
from pathlib import Path


def cache_path_for(source: str, *, trade_date: str, root: Path) -> Path:
    """Return the path where a raw file should be stored.

    Mirrors the layout in Implementation Plan §8.
    """
    parsed = date.fromisoformat(trade_date)
    return (
        root
        / "raw"
        / source
        / f"{parsed.year:04d}"
        / f"{parsed.month:02d}"
        / f"{parsed.year:04d}-{parsed.month:02d}-{parsed.day:02d}"
    )


__all__ = ["cache_path_for"]
