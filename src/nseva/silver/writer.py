"""Utilities for writing silver Parquet partitions (Implementation Plan §6.3)."""

from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Any


def write_partition(table: str, date: str, frame: Any, *, root: Path) -> Path:
    """Persist a silver-layer dataframe at the configured location.

    Schema enforcement and parquet IO are implemented in FUT-005/FUT-006.
    """
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas DataFrame")

    partition_path = Path(root).expanduser().resolve() / "silver" / table / f"date={date}"
    partition_path.mkdir(parents=True, exist_ok=True)
    dest = partition_path / "data.parquet"

    frame.to_parquet(dest, index=False)
    return dest


__all__ = ["write_partition"]
