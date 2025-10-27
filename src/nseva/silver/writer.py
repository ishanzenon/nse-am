"""Utilities for writing silver Parquet partitions (Implementation Plan §6.3)."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def write_partition(table: str, date: str, frame: Any, *, root: Path) -> Path:
    """Persist a silver-layer dataframe at the configured location.

    Schema enforcement and parquet IO are implemented in FUT-005/FUT-006.
    """
    raise NotImplementedError("Silver partition writer arrives in FUT-005.")


__all__ = ["write_partition"]
