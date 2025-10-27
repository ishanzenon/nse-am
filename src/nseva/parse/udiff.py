"""UDiFF bhavcopy parser stubs (Implementation Plan §6.2)."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def udiff_to_silver_fo(path: Path) -> Any:
    """Convert a UDiFF FUTSTK file into the silver schema.

    Real parsing logic (alias mapping, instrument filtering) arrives in FUT-005.
    """
    raise NotImplementedError("UDiFF parsing arrives in FUT-005.")


__all__ = ["udiff_to_silver_fo"]
